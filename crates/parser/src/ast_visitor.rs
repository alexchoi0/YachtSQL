use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};

use sqlparser::ast;
use yachtsql_core::error::{Error, Result};
use yachtsql_ir::plan::{LogicalPlan, PlanNode};

struct GroupingExtension {
    columns: Vec<ast::Expr>,
    is_rollup: bool,
    is_cube: bool,
    is_grouping_sets: bool,
    explicit_sets: Vec<Vec<ast::Expr>>,
}

struct AliasScopeGuard<'a> {
    builder: &'a LogicalPlanBuilder,
}

impl<'a> AliasScopeGuard<'a> {
    fn new(builder: &'a LogicalPlanBuilder, aliases: HashSet<String>) -> Self {
        builder.push_alias_scope(aliases);
        Self { builder }
    }
}

impl Drop for AliasScopeGuard<'_> {
    fn drop(&mut self) {
        self.builder.pop_alias_scope();
    }
}

use yachtsql_core::types::{DataType, Value};

#[derive(Debug, Clone)]
pub struct SessionVariable {
    pub data_type: DataType,
    pub value: Option<Value>,
}

pub struct LogicalPlanBuilder {
    storage: Option<std::rc::Rc<RefCell<yachtsql_storage::Storage>>>,
    alias_stack: RefCell<Vec<HashSet<String>>>,
    named_windows: RefCell<HashMap<String, ast::WindowSpec>>,
    dialect: crate::DialectType,
    current_sql: RefCell<Option<String>>,
    subquery_alias_counter: Cell<usize>,
    merge_returning: RefCell<Option<String>>,
    session_variables: HashMap<String, SessionVariable>,
}

mod ddl;
mod dml;
mod expr;
mod query;

impl LogicalPlanBuilder {
    pub fn next_subquery_alias_id(&self) -> usize {
        let current = self.subquery_alias_counter.get();
        self.subquery_alias_counter.set(current + 1);
        current
    }

    pub fn new() -> Self {
        Self {
            storage: None,
            alias_stack: RefCell::new(Vec::new()),
            named_windows: RefCell::new(HashMap::new()),
            dialect: crate::DialectType::BigQuery,
            current_sql: RefCell::new(None),
            subquery_alias_counter: Cell::new(0),
            merge_returning: RefCell::new(None),
            session_variables: HashMap::new(),
        }
    }

    pub fn with_sql(self, sql: &str) -> Self {
        *self.current_sql.borrow_mut() = Some(sql.to_string());
        self
    }

    pub fn with_storage(self, storage: std::rc::Rc<RefCell<yachtsql_storage::Storage>>) -> Self {
        Self {
            storage: Some(storage),
            alias_stack: self.alias_stack,
            named_windows: self.named_windows,
            dialect: self.dialect,
            current_sql: self.current_sql,
            subquery_alias_counter: self.subquery_alias_counter,
            merge_returning: self.merge_returning,
            session_variables: self.session_variables,
        }
    }

    pub fn with_dialect(self, dialect: crate::DialectType) -> Self {
        Self {
            storage: self.storage,
            alias_stack: self.alias_stack,
            named_windows: self.named_windows,
            dialect,
            current_sql: self.current_sql,
            subquery_alias_counter: self.subquery_alias_counter,
            merge_returning: self.merge_returning,
            session_variables: self.session_variables,
        }
    }

    pub fn with_merge_returning(self, returning: Option<String>) -> Self {
        *self.merge_returning.borrow_mut() = returning;
        self
    }

    pub fn with_variables(self, variables: HashMap<String, SessionVariable>) -> Self {
        Self {
            storage: self.storage,
            alias_stack: self.alias_stack,
            named_windows: self.named_windows,
            dialect: self.dialect,
            current_sql: self.current_sql,
            subquery_alias_counter: self.subquery_alias_counter,
            merge_returning: self.merge_returning,
            session_variables: variables,
        }
    }

    pub fn get_session_variable(&self, name: &str) -> Option<&SessionVariable> {
        self.session_variables.get(name)
    }

    pub fn take_merge_returning(&self) -> Option<String> {
        self.merge_returning.borrow_mut().take()
    }

    fn push_alias_scope(&self, aliases: HashSet<String>) {
        self.alias_stack.borrow_mut().push(aliases);
    }

    fn pop_alias_scope(&self) {
        self.alias_stack.borrow_mut().pop();
    }

    fn is_current_alias(&self, name: &str) -> bool {
        self.alias_stack
            .borrow()
            .last()
            .map(|aliases| aliases.contains(name))
            .unwrap_or(false)
    }

    fn set_named_windows(&self, windows: HashMap<String, ast::WindowSpec>) {
        *self.named_windows.borrow_mut() = windows;
    }

    fn clear_named_windows(&self) {
        self.named_windows.borrow_mut().clear();
    }

    fn get_named_window(&self, name: &str) -> Option<ast::WindowSpec> {
        self.named_windows.borrow().get(name).cloned()
    }

    fn parse_frame_bound(bound: &sqlparser::ast::WindowFrameBound) -> Option<i64> {
        match bound {
            sqlparser::ast::WindowFrameBound::Preceding(Some(expr)) => {
                if let sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: sqlparser::ast::Value::Number(n, _),
                    ..
                }) = &**expr
                {
                    n.parse::<i64>().ok()
                } else {
                    None
                }
            }
            sqlparser::ast::WindowFrameBound::Following(Some(expr)) => {
                if let sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: sqlparser::ast::Value::Number(n, _),
                    ..
                }) = &**expr
                {
                    n.parse::<i64>().ok()
                } else {
                    None
                }
            }
            sqlparser::ast::WindowFrameBound::CurrentRow => Some(0),
            sqlparser::ast::WindowFrameBound::Preceding(None) => None,
            sqlparser::ast::WindowFrameBound::Following(None) => None,
        }
    }

    fn resolve_named_window_expr(
        &self,
        named_window_expr: &ast::NamedWindowExpr,
    ) -> Result<ast::WindowSpec> {
        match named_window_expr {
            ast::NamedWindowExpr::WindowSpec(spec) => {
                if let Some(window_name_ident) = &spec.window_name {
                    let window_name = window_name_ident.value.clone();
                    let base_spec = self.get_named_window(&window_name).ok_or_else(|| {
                        Error::invalid_query(format!(
                            "Window '{}' referenced in WINDOW clause definition not found",
                            window_name
                        ))
                    })?;

                    self.merge_window_specs(&base_spec, spec)
                } else {
                    Ok(spec.clone())
                }
            }
            ast::NamedWindowExpr::NamedWindow(ident) => {
                let window_name = ident.value.clone();
                self.get_named_window(&window_name).ok_or_else(|| {
                    Error::invalid_query(format!(
                        "Window '{}' referenced before definition in WINDOW clause",
                        window_name
                    ))
                })
            }
        }
    }

    fn merge_window_specs(
        &self,
        base_spec: &ast::WindowSpec,
        refinement_spec: &ast::WindowSpec,
    ) -> Result<ast::WindowSpec> {
        if !refinement_spec.partition_by.is_empty() {
            return Err(Error::invalid_query(
                "Cannot override PARTITION BY in window refinement".to_string(),
            ));
        }

        let mut merged = base_spec.clone();

        if !refinement_spec.order_by.is_empty() {
            if !merged.order_by.is_empty() {
                return Err(Error::invalid_query(
                    "Cannot override ORDER BY in window refinement when base window already has ORDER BY".to_string(),
                ));
            }
            merged.order_by = refinement_spec.order_by.clone();
        }

        if refinement_spec.window_frame.is_some() {
            if merged.window_frame.is_some() {
                return Err(Error::invalid_query(
                    "Cannot override window frame in refinement when base window already has a frame".to_string(),
                ));
            }
            merged.window_frame = refinement_spec.window_frame.clone();
        }

        Ok(merged)
    }

    fn parse_exclude_from_current_sql(&self) -> Option<yachtsql_ir::expr::ExcludeMode> {
        let sql = self.current_sql.borrow().as_ref()?.clone();
        Self::parse_exclude_mode_from_sql(&sql)
    }

    fn parse_exclude_mode_from_sql(sql: &str) -> Option<yachtsql_ir::expr::ExcludeMode> {
        let sql_upper = sql.to_uppercase();

        let exclude_pos = sql_upper.find("EXCLUDE")?;

        let after_exclude = &sql_upper[exclude_pos + "EXCLUDE".len()..].trim_start();

        Self::match_exclude_mode(after_exclude)
    }

    fn match_exclude_mode(text: &str) -> Option<yachtsql_ir::expr::ExcludeMode> {
        use yachtsql_ir::expr::ExcludeMode;

        if Self::matches_keyword_sequence(text, &["CURRENT", "ROW"]) {
            return Some(ExcludeMode::CurrentRow);
        }
        if Self::matches_keyword_sequence(text, &["NO", "OTHERS"]) {
            return Some(ExcludeMode::NoOthers);
        }

        if text.starts_with("GROUP") {
            return Some(ExcludeMode::Group);
        }
        if text.starts_with("TIES") {
            return Some(ExcludeMode::Ties);
        }

        None
    }

    fn matches_keyword_sequence(text: &str, keywords: &[&str]) -> bool {
        if keywords.is_empty() {
            return false;
        }

        if Self::matches_with_separator(text, keywords, "_") {
            return true;
        }

        Self::matches_with_separator(text, keywords, " ")
    }

    fn matches_with_separator(text: &str, keywords: &[&str], separator: &str) -> bool {
        let mut remaining = text;

        for (i, keyword) in keywords.iter().enumerate() {
            if !remaining.starts_with(keyword) {
                return false;
            }
            remaining = &remaining[keyword.len()..];

            if i < keywords.len() - 1 {
                if separator == " " {
                    let trimmed = remaining.trim_start();
                    if trimmed.len() == remaining.len() {
                        return false;
                    }
                    remaining = trimmed;
                } else {
                    if !remaining.starts_with(separator) {
                        return false;
                    }
                    remaining = &remaining[separator.len()..];
                }
            }
        }

        true
    }

    fn collect_table_aliases(&self, from: &[ast::TableWithJoins]) -> HashSet<String> {
        let mut aliases = HashSet::new();
        for table_with_joins in from {
            self.collect_alias_from_factor(&table_with_joins.relation, &mut aliases);
            for join in &table_with_joins.joins {
                self.collect_alias_from_factor(&join.relation, &mut aliases);
            }
        }
        aliases
    }

    fn collect_alias_from_factor(&self, factor: &ast::TableFactor, aliases: &mut HashSet<String>) {
        match factor {
            ast::TableFactor::Table { name, alias, .. } => {
                if let Some(alias) = alias {
                    aliases.insert(alias.name.value.clone());
                } else {
                    aliases.insert(name.to_string());
                }
            }
            ast::TableFactor::Derived {
                alias: Some(alias), ..
            }
            | ast::TableFactor::UNNEST {
                alias: Some(alias), ..
            } => {
                aliases.insert(alias.name.value.clone());
            }
            _ => {}
        }
    }

    fn collect_aliases_from_plan(&self, plan: &LogicalPlan) -> HashSet<String> {
        let mut aliases = HashSet::new();
        Self::walk_plan_for_aliases(plan.root(), &mut aliases);
        aliases
    }

    fn walk_plan_for_aliases(node: &PlanNode, aliases: &mut HashSet<String>) {
        match node {
            PlanNode::Scan {
                table_name, alias, ..
            } => {
                let name = alias.as_ref().unwrap_or(table_name);
                aliases.insert(name.clone());
            }

            PlanNode::SubqueryScan { alias, .. } => {
                aliases.insert(alias.clone());
            }

            PlanNode::TableValuedFunction {
                alias: Some(alias), ..
            } => {
                aliases.insert(alias.clone());
            }

            PlanNode::TableValuedFunction { alias: None, .. } => {}

            PlanNode::Join { left, right, .. } => {
                Self::walk_plan_for_aliases(left, aliases);
                Self::walk_plan_for_aliases(right, aliases);
            }

            PlanNode::Filter { input, .. }
            | PlanNode::Projection { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Sort { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Distinct { input }
            | PlanNode::DistinctOn { input, .. }
            | PlanNode::Window { input, .. } => {
                Self::walk_plan_for_aliases(input, aliases);
            }

            _ => {}
        }
    }

    fn plan_join_relation(
        &self,
        relation: &ast::TableFactor,
        left_plan: &LogicalPlan,
    ) -> Result<LogicalPlan> {
        let is_lateral = Self::is_lateral_derived_table(relation);

        if is_lateral {
            let left_aliases = self.collect_aliases_from_plan(left_plan);
            let _scope_guard = AliasScopeGuard::new(self, left_aliases);
            self.table_factor_to_plan(relation)
        } else {
            self.table_factor_to_plan(relation)
        }
    }

    fn is_lateral_derived_table(factor: &ast::TableFactor) -> bool {
        matches!(
            factor,
            ast::TableFactor::Derived { lateral: true, .. }
                | ast::TableFactor::Function { lateral: true, .. }
                | ast::TableFactor::UNNEST { .. }
        )
    }

    fn object_name_to_string(name: &ast::ObjectName) -> String {
        name.0
            .iter()
            .map(|part| part.to_string())
            .collect::<Vec<_>>()
            .join(".")
    }

    fn object_name_matches(name: &ast::ObjectName, target: &str) -> bool {
        name.0
            .last()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.eq_ignore_ascii_case(target))
            .unwrap_or(false)
    }

    #[allow(dead_code)]
    fn ident_list_to_strings(idents: &[ast::Ident]) -> Vec<String> {
        idents.iter().map(|ident| ident.value.clone()).collect()
    }

    #[allow(dead_code)]
    fn extract_optional_column_list(columns: &[ast::Ident]) -> Option<Vec<String>> {
        if columns.is_empty() {
            None
        } else {
            Some(Self::ident_list_to_strings(columns))
        }
    }

    fn resolve_table_name(&self, name: &str) -> String {
        name.to_string()
    }
}

impl Default for LogicalPlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let _builder = LogicalPlanBuilder::new();
        let parser = crate::parser::Parser::new();
        let sql = "SELECT * FROM users";
        let result = parser.parse_sql(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_select_with_where() {
        let _builder = LogicalPlanBuilder::new();
        let parser = crate::parser::Parser::new();
        let sql = "SELECT * FROM users WHERE age > 18";
        let result = parser.parse_sql(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_select_with_limit() {
        let _builder = LogicalPlanBuilder::new();
        let parser = crate::parser::Parser::new();
        let sql = "SELECT * FROM users LIMIT 10";
        let result = parser.parse_sql(sql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_exclude_current_row() {
        use yachtsql_ir::expr::ExcludeMode;

        let sql = "SELECT SUM(x) OVER (EXCLUDE CURRENT ROW) FROM t";
        let result = LogicalPlanBuilder::parse_exclude_mode_from_sql(sql);
        assert_eq!(result, Some(ExcludeMode::CurrentRow));
    }

    #[test]
    fn test_parse_exclude_current_row_underscore() {
        use yachtsql_ir::expr::ExcludeMode;

        let sql = "SELECT SUM(x) OVER (EXCLUDE CURRENT_ROW) FROM t";
        let result = LogicalPlanBuilder::parse_exclude_mode_from_sql(sql);
        assert_eq!(result, Some(ExcludeMode::CurrentRow));
    }

    #[test]
    fn test_parse_exclude_group() {
        use yachtsql_ir::expr::ExcludeMode;

        let sql = "SELECT SUM(x) OVER (EXCLUDE GROUP) FROM t";
        let result = LogicalPlanBuilder::parse_exclude_mode_from_sql(sql);
        assert_eq!(result, Some(ExcludeMode::Group));
    }

    #[test]
    fn test_parse_exclude_ties() {
        use yachtsql_ir::expr::ExcludeMode;

        let sql = "SELECT SUM(x) OVER (EXCLUDE TIES) FROM t";
        let result = LogicalPlanBuilder::parse_exclude_mode_from_sql(sql);
        assert_eq!(result, Some(ExcludeMode::Ties));
    }

    #[test]
    fn test_parse_exclude_no_others() {
        use yachtsql_ir::expr::ExcludeMode;

        let sql = "SELECT SUM(x) OVER (EXCLUDE NO OTHERS) FROM t";
        let result = LogicalPlanBuilder::parse_exclude_mode_from_sql(sql);
        assert_eq!(result, Some(ExcludeMode::NoOthers));
    }

    #[test]
    fn test_parse_exclude_no_others_underscore() {
        use yachtsql_ir::expr::ExcludeMode;

        let sql = "SELECT SUM(x) OVER (EXCLUDE NO_OTHERS) FROM t";
        let result = LogicalPlanBuilder::parse_exclude_mode_from_sql(sql);
        assert_eq!(result, Some(ExcludeMode::NoOthers));
    }

    #[test]
    fn test_parse_exclude_case_insensitive() {
        use yachtsql_ir::expr::ExcludeMode;

        let sql = "SELECT SUM(x) OVER (exclude current row) FROM t";
        let result = LogicalPlanBuilder::parse_exclude_mode_from_sql(sql);
        assert_eq!(result, Some(ExcludeMode::CurrentRow));
    }

    #[test]
    fn test_parse_no_exclude() {
        let sql = "SELECT SUM(x) OVER (ORDER BY y) FROM t";
        let result = LogicalPlanBuilder::parse_exclude_mode_from_sql(sql);
        assert_eq!(result, None);
    }

    #[test]
    fn test_matches_keyword_sequence_space_separated() {
        assert!(LogicalPlanBuilder::matches_keyword_sequence(
            "CURRENT ROW",
            &["CURRENT", "ROW"]
        ));
    }

    #[test]
    fn test_matches_keyword_sequence_underscore_separated() {
        assert!(LogicalPlanBuilder::matches_keyword_sequence(
            "CURRENT_ROW",
            &["CURRENT", "ROW"]
        ));
    }

    #[test]
    fn test_matches_keyword_sequence_no_separator() {
        assert!(!LogicalPlanBuilder::matches_keyword_sequence(
            "CURRENTROW",
            &["CURRENT", "ROW"]
        ));
    }

    #[test]
    fn test_matches_keyword_sequence_extra_whitespace() {
        assert!(LogicalPlanBuilder::matches_keyword_sequence(
            "CURRENT   ROW",
            &["CURRENT", "ROW"]
        ));
    }
}
