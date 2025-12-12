use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};

use sqlparser::ast;
use yachtsql_core::error::{Error, Result};
use yachtsql_ir::expr::{BinaryOp, Expr, LiteralValue};
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

#[derive(Debug, Clone)]
pub struct UdfDefinition {
    pub parameters: Vec<String>,
    pub body: ast::Expr,
    pub return_type: Option<ast::DataType>,
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
    udfs: HashMap<String, UdfDefinition>,
    final_tables: RefCell<HashSet<String>>,
    current_group_by_exprs: RefCell<Vec<yachtsql_ir::expr::Expr>>,
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
            udfs: HashMap::new(),
            final_tables: RefCell::new(HashSet::new()),
            current_group_by_exprs: RefCell::new(Vec::new()),
        }
    }

    pub fn with_sql(self, sql: &str) -> Self {
        let final_tables = Self::extract_final_tables(sql);
        *self.current_sql.borrow_mut() = Some(sql.to_string());
        *self.final_tables.borrow_mut() = final_tables;
        self
    }

    fn extract_final_tables(sql: &str) -> HashSet<String> {
        let mut final_tables = HashSet::new();
        let upper = sql.to_uppercase();
        let word_re = regex::Regex::new(r"\b([A-Za-z_][A-Za-z0-9_.]*)\s+FINAL\b").unwrap();
        for cap in word_re.captures_iter(&upper) {
            if let Some(table_name) = cap.get(1) {
                let name = table_name.as_str();
                if ![
                    "SELECT", "INSERT", "UPDATE", "DELETE", "FROM", "JOIN", "WHERE", "AND", "OR",
                    "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET",
                ]
                .contains(&name)
                {
                    final_tables.insert(name.to_lowercase());
                }
            }
        }
        final_tables
    }

    pub fn has_final_modifier(&self, table_name: &str) -> bool {
        let final_tables = self.final_tables.borrow();
        let lower = table_name.to_lowercase();
        let simple_name = lower.split('.').next_back().unwrap_or(&lower);
        final_tables.contains(simple_name) || final_tables.contains(&lower)
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
            udfs: self.udfs,
            final_tables: self.final_tables,
            current_group_by_exprs: self.current_group_by_exprs,
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
            udfs: self.udfs,
            final_tables: self.final_tables,
            current_group_by_exprs: self.current_group_by_exprs,
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
            udfs: self.udfs,
            final_tables: self.final_tables,
            current_group_by_exprs: self.current_group_by_exprs,
        }
    }

    pub fn with_udfs(self, udfs: HashMap<String, UdfDefinition>) -> Self {
        Self {
            storage: self.storage,
            alias_stack: self.alias_stack,
            named_windows: self.named_windows,
            dialect: self.dialect,
            current_sql: self.current_sql,
            subquery_alias_counter: self.subquery_alias_counter,
            merge_returning: self.merge_returning,
            session_variables: self.session_variables,
            udfs,
            final_tables: self.final_tables,
            current_group_by_exprs: self.current_group_by_exprs,
        }
    }

    pub fn get_udf(&self, name: &str) -> Option<&UdfDefinition> {
        self.udfs.get(&name.to_uppercase())
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

    fn set_current_group_by_exprs(&self, exprs: Vec<Expr>) {
        *self.current_group_by_exprs.borrow_mut() = exprs;
    }

    fn get_current_group_by_exprs(&self) -> Vec<Expr> {
        self.current_group_by_exprs.borrow().clone()
    }

    fn clear_current_group_by_exprs(&self) {
        self.current_group_by_exprs.borrow_mut().clear();
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

    fn is_paste_join_marker(factor: &ast::TableFactor) -> bool {
        match factor {
            ast::TableFactor::Table { name, .. } => {
                return name
                    .0
                    .first()
                    .and_then(|p| p.as_ident())
                    .map(|ident| ident.value == "__PASTE__")
                    .unwrap_or(false);
            }
            ast::TableFactor::Derived {
                alias: Some(table_alias),
                ..
            } => {
                return table_alias.name.value.starts_with("__PASTE_SUBQUERY_");
            }
            _ => {}
        }
        false
    }

    fn plan_paste_join_relation(
        &self,
        relation: &ast::TableFactor,
        _left_plan: &LogicalPlan,
    ) -> Result<LogicalPlan> {
        match relation {
            ast::TableFactor::Table {
                alias: Some(table_alias),
                ..
            } => {
                let real_table_name = table_alias.name.value.clone();
                let fake_table_factor = ast::TableFactor::Table {
                    name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                        &real_table_name,
                    ))]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    with_ordinality: false,
                    partitions: vec![],
                    json_path: None,
                    sample: None,
                    index_hints: vec![],
                };
                return self.table_factor_to_plan(&fake_table_factor);
            }
            ast::TableFactor::Derived { subquery, .. } => {
                let subquery_plan = self.query_to_plan(subquery)?;
                return Ok(LogicalPlan::new(PlanNode::SubqueryScan {
                    subquery: subquery_plan.root,
                    alias: "__paste_subquery__".to_string(),
                }));
            }
            _ => {}
        }
        Err(Error::parse_error(
            "Invalid PASTE JOIN: missing table name".to_string(),
        ))
    }

    fn is_asof_join_marker(factor: &ast::TableFactor) -> Option<(String, bool)> {
        match factor {
            ast::TableFactor::Table { name, .. } => {
                let table_name = name
                    .0
                    .first()
                    .and_then(|p| p.as_ident())
                    .map(|ident| &ident.value)?;

                if let Some(actual_table) = table_name.strip_prefix("__ASOF_LEFT__") {
                    return Some((actual_table.to_string(), true));
                }
                if let Some(actual_table) = table_name.strip_prefix("__ASOF__") {
                    return Some((actual_table.to_string(), false));
                }
                None
            }
            _ => None,
        }
    }

    fn is_array_join_marker(factor: &ast::TableFactor) -> Option<(String, bool)> {
        match factor {
            ast::TableFactor::Table { name, .. } => {
                let table_name = name
                    .0
                    .first()
                    .and_then(|p| p.as_ident())
                    .map(|ident| &ident.value)?;

                if let Some(rest) = table_name.strip_prefix("__LEFT_ARRAY_JOIN__") {
                    let columns = rest.strip_suffix("__").unwrap_or(rest);
                    let decoded = Self::decode_array_join_columns(columns);
                    return Some((decoded, true));
                }
                if let Some(rest) = table_name.strip_prefix("__ARRAY_JOIN__") {
                    let columns = rest.strip_suffix("__").unwrap_or(rest);
                    let decoded = Self::decode_array_join_columns(columns);
                    return Some((decoded, false));
                }
                None
            }
            _ => None,
        }
    }

    fn decode_array_join_columns(encoded: &str) -> String {
        encoded
            .replace("_SP_", " ")
            .replace("_CM_", ",")
            .replace("_LP_", "(")
            .replace("_RP_", ")")
    }

    fn parse_array_join_columns(&self, columns_str: &str) -> Result<Vec<(Expr, Option<String>)>> {
        let mut result = Vec::new();
        let trimmed = columns_str.trim();

        if trimmed.is_empty() {
            return Err(Error::parse_error(
                "ARRAY JOIN requires at least one array expression".to_string(),
            ));
        }

        let parts: Vec<&str> = Self::split_respecting_parens(trimmed);

        for part in parts {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let upper = part.to_uppercase();
            let (expr_str, alias) = if let Some(as_pos) = Self::find_as_keyword(&upper) {
                let expr_part = part[..as_pos].trim();
                let skip = if as_pos + 4 <= part.len()
                    && &upper.as_bytes()[as_pos..as_pos + 4] == b" AS "
                {
                    4
                } else {
                    3
                };
                let alias_part = if as_pos + skip < part.len() {
                    part[as_pos + skip..].trim()
                } else {
                    ""
                };
                if alias_part.is_empty() {
                    (part.to_string(), None)
                } else {
                    (expr_part.to_string(), Some(alias_part.to_string()))
                }
            } else {
                (part.to_string(), None)
            };

            let expr = Expr::Column {
                name: expr_str.clone(),
                table: None,
            };

            result.push((expr, alias));
        }

        if result.is_empty() {
            return Err(Error::parse_error(
                "ARRAY JOIN requires at least one array expression".to_string(),
            ));
        }

        Ok(result)
    }

    fn split_respecting_parens(s: &str) -> Vec<&str> {
        let mut result = Vec::new();
        let mut depth = 0;
        let mut start = 0;

        for (i, ch) in s.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    result.push(&s[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }

        if start < s.len() {
            result.push(&s[start..]);
        }

        result
    }

    fn find_as_keyword(upper_str: &str) -> Option<usize> {
        let mut idx = 0;
        let bytes = upper_str.as_bytes();

        while idx + 4 <= bytes.len() {
            if &bytes[idx..idx + 4] == b" AS " {
                return Some(idx);
            }
            idx += 1;
        }

        if bytes.len() >= 3 && &bytes[bytes.len() - 3..] == b" AS" {
            return Some(bytes.len() - 3);
        }

        None
    }

    fn plan_asof_join_relation(
        &self,
        relation: &ast::TableFactor,
        actual_table_name: &str,
    ) -> Result<LogicalPlan> {
        match relation {
            ast::TableFactor::Table { alias, .. } => {
                let table_alias = alias.as_ref().map(|a| a.name.value.clone());
                let fake_table_factor = ast::TableFactor::Table {
                    name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                        actual_table_name,
                    ))]),
                    alias: alias.clone(),
                    args: None,
                    with_hints: vec![],
                    version: None,
                    with_ordinality: false,
                    partitions: vec![],
                    json_path: None,
                    sample: None,
                    index_hints: vec![],
                };
                let plan = self.table_factor_to_plan(&fake_table_factor)?;
                if let Some(alias_name) = table_alias {
                    return Ok(LogicalPlan::new(PlanNode::SubqueryScan {
                        subquery: plan.root,
                        alias: alias_name,
                    }));
                }
                Ok(plan)
            }
            _ => Err(Error::parse_error(
                "Invalid ASOF JOIN: expected table".to_string(),
            )),
        }
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

    fn build_asof_using_condition(&self, cols: &[ast::ObjectName]) -> Result<(Expr, Expr)> {
        if cols.is_empty() {
            return Err(Error::parse_error(
                "ASOF JOIN USING requires at least one column".to_string(),
            ));
        }

        let ident_cols: Vec<String> = cols
            .iter()
            .filter_map(|name| {
                name.0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.clone())
            })
            .collect();

        if ident_cols.is_empty() {
            return Err(Error::parse_error(
                "ASOF JOIN USING requires at least one column".to_string(),
            ));
        }

        let match_col = ident_cols.last().unwrap().clone();

        let match_expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: match_col.clone(),
                table: Some("__left__".to_string()),
            }),
            op: BinaryOp::GreaterThanOrEqual,
            right: Box::new(Expr::Column {
                name: match_col,
                table: Some("__right__".to_string()),
            }),
        };

        let equality_cols = &ident_cols[..ident_cols.len() - 1];

        let equality_expr = if equality_cols.is_empty() {
            Expr::Literal(LiteralValue::Boolean(true))
        } else {
            let mut result = Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: equality_cols[0].clone(),
                    table: Some("__left__".to_string()),
                }),
                op: BinaryOp::Equal,
                right: Box::new(Expr::Column {
                    name: equality_cols[0].clone(),
                    table: Some("__right__".to_string()),
                }),
            };

            for col in equality_cols.iter().skip(1) {
                result = Expr::BinaryOp {
                    left: Box::new(result),
                    op: BinaryOp::And,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Column {
                            name: col.clone(),
                            table: Some("__left__".to_string()),
                        }),
                        op: BinaryOp::Equal,
                        right: Box::new(Expr::Column {
                            name: col.clone(),
                            table: Some("__right__".to_string()),
                        }),
                    }),
                };
            }

            result
        };

        Ok((equality_expr, match_expr))
    }

    fn split_asof_condition(&self, expr: &ast::Expr) -> Result<(Expr, Expr)> {
        let mut equality_parts = Vec::new();
        let mut match_part: Option<&ast::Expr> = None;

        Self::collect_asof_conditions(expr, &mut equality_parts, &mut match_part)?;

        let match_expr = match_part.ok_or_else(|| {
            Error::parse_error("ASOF JOIN requires a match condition (>=, <=, >, <)".to_string())
        })?;

        let equality_expr = if equality_parts.is_empty() {
            Expr::Literal(LiteralValue::Boolean(true))
        } else if equality_parts.len() == 1 {
            self.sql_expr_to_expr(equality_parts[0])?
        } else {
            let mut result = self.sql_expr_to_expr(equality_parts[0])?;
            for part in equality_parts.iter().skip(1) {
                result = Expr::BinaryOp {
                    left: Box::new(result),
                    op: BinaryOp::And,
                    right: Box::new(self.sql_expr_to_expr(part)?),
                };
            }
            result
        };

        Ok((equality_expr, self.sql_expr_to_expr(match_expr)?))
    }

    fn collect_asof_conditions<'a>(
        expr: &'a ast::Expr,
        equality_parts: &mut Vec<&'a ast::Expr>,
        match_part: &mut Option<&'a ast::Expr>,
    ) -> Result<()> {
        match expr {
            ast::Expr::BinaryOp { left, op, right } => match op {
                ast::BinaryOperator::And => {
                    Self::collect_asof_conditions(left, equality_parts, match_part)?;
                    Self::collect_asof_conditions(right, equality_parts, match_part)?;
                }
                ast::BinaryOperator::Eq => {
                    equality_parts.push(expr);
                }
                ast::BinaryOperator::Gt
                | ast::BinaryOperator::GtEq
                | ast::BinaryOperator::Lt
                | ast::BinaryOperator::LtEq => {
                    if match_part.is_some() {
                        return Err(Error::parse_error(
                            "ASOF JOIN supports only one match condition".to_string(),
                        ));
                    }
                    *match_part = Some(expr);
                }
                _ => {
                    equality_parts.push(expr);
                }
            },
            ast::Expr::Nested(inner) => {
                Self::collect_asof_conditions(inner, equality_parts, match_part)?;
            }
            _ => {
                equality_parts.push(expr);
            }
        }
        Ok(())
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
