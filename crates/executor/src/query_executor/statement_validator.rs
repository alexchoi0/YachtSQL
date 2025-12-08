use std::collections::HashSet;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Query, SelectItem, SetExpr,
    Statement as SqlStatement,
};
use yachtsql_capability::{FeatureId, FeatureRegistry};
use yachtsql_core::error::{Error, Result};
use yachtsql_parser::Statement as ParserStatement;

use super::function_validator::validate_function;

const T611_WINDOW_FUNCTIONS: FeatureId = FeatureId("T611");

pub fn validate_statement(stmt: &ParserStatement, registry: &FeatureRegistry) -> Result<()> {
    match stmt {
        ParserStatement::Standard(std_stmt) => {
            StatementValidator { registry }.validate(std_stmt.ast())
        }
        ParserStatement::Custom(_) => Ok(()),
    }
}

struct StatementValidator<'a> {
    registry: &'a FeatureRegistry,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ValidationContext {
    Select,
    Where,
    GroupBy,
    Having,
    OrderBy,
    Other,
}

impl<'a> StatementValidator<'a> {
    fn validate(&self, stmt: &SqlStatement) -> Result<()> {
        match stmt {
            SqlStatement::Query(query) => self.validate_query(query),
            SqlStatement::Insert(insert) => {
                if let Some(query) = &insert.source {
                    self.validate_set_expr(&query.body)?;
                }
                Ok(())
            }
            SqlStatement::Update {
                selection,
                assignments,
                ..
            } => {
                if let Some(expr) = selection {
                    self.validate_expr(expr)?;
                }
                for assignment in assignments {
                    self.validate_expr(&assignment.value)?;
                }
                Ok(())
            }
            SqlStatement::Delete(delete) => {
                if let Some(expr) = &delete.selection {
                    self.validate_expr(expr)?;
                }
                Ok(())
            }
            SqlStatement::CreateView { query, .. } => self.validate_query(query),
            _ => Ok(()),
        }
    }

    fn validate_query(&self, query: &Query) -> Result<()> {
        self.validate_set_expr(&query.body)?;
        Ok(())
    }

    fn validate_set_expr(&self, set_expr: &SetExpr) -> Result<()> {
        match set_expr {
            SetExpr::Select(select) => {
                let has_from = !select.from.is_empty();
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                            self.validate_expr_with_context(expr, ValidationContext::Select)?;
                        }
                        SelectItem::Wildcard(_) => {
                            if !has_from {
                                return Err(Error::InvalidQuery(
                                    "SELECT * requires a FROM clause".to_string(),
                                ));
                            }
                        }
                        SelectItem::QualifiedWildcard(_, _) => {
                            if !has_from {
                                return Err(Error::InvalidQuery(
                                    "SELECT * requires a FROM clause".to_string(),
                                ));
                            }
                        }
                    }
                }
                if let Some(selection) = &select.selection {
                    self.validate_expr_with_context(selection, ValidationContext::Where)?;
                }

                self.validate_group_by_columns(&select.projection, &select.group_by)?;

                Ok(())
            }
            SetExpr::Query(query) => self.validate_query(query),
            SetExpr::SetOperation { left, right, .. } => {
                self.validate_set_expr(left)?;
                self.validate_set_expr(right)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn validate_expr(&self, expr: &Expr) -> Result<()> {
        self.validate_expr_with_context(expr, ValidationContext::Other)
    }

    fn validate_group_by_columns(
        &self,
        projection: &[SelectItem],
        group_by: &GroupByExpr,
    ) -> Result<()> {
        let group_by_columns = self.extract_group_by_columns(group_by);

        if group_by_columns.is_empty() {
            return Ok(());
        }

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    self.validate_select_expr_against_group_by(expr, &group_by_columns)?;
                }
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
            }
        }

        Ok(())
    }

    fn extract_group_by_columns(&self, group_by: &GroupByExpr) -> HashSet<String> {
        let mut columns = HashSet::new();
        match group_by {
            GroupByExpr::All(_) => {}
            GroupByExpr::Expressions(exprs, _) => {
                for expr in exprs {
                    Self::collect_column_names(expr, &mut columns);
                }
            }
        }
        columns
    }

    fn collect_column_names(expr: &Expr, columns: &mut HashSet<String>) {
        match expr {
            Expr::Identifier(ident) => {
                columns.insert(ident.value.to_lowercase());
            }
            Expr::CompoundIdentifier(parts) => {
                if let Some(last) = parts.last() {
                    columns.insert(last.value.to_lowercase());
                }
            }
            Expr::Nested(inner) => Self::collect_column_names(inner, columns),
            Expr::Rollup(sets) | Expr::Cube(sets) => {
                for set in sets {
                    for col_expr in set {
                        Self::collect_column_names(col_expr, columns);
                    }
                }
            }
            Expr::GroupingSets(sets) => {
                for set in sets {
                    for col_expr in set {
                        Self::collect_column_names(col_expr, columns);
                    }
                }
            }
            _ => {}
        }
    }

    fn validate_select_expr_against_group_by(
        &self,
        expr: &Expr,
        group_by_columns: &HashSet<String>,
    ) -> Result<()> {
        let mut non_agg_columns = HashSet::new();
        Self::collect_non_aggregate_columns(expr, &mut non_agg_columns);

        for col in non_agg_columns {
            if !group_by_columns.contains(&col) {
                return Err(Error::InvalidQuery(format!(
                    "Column '{}' must appear in the GROUP BY clause or be used in an aggregate function",
                    col
                )));
            }
        }

        Ok(())
    }

    fn collect_non_aggregate_columns(expr: &Expr, columns: &mut HashSet<String>) {
        match expr {
            Expr::Identifier(ident) => {
                columns.insert(ident.value.to_lowercase());
            }
            Expr::CompoundIdentifier(parts) => {
                if let Some(last) = parts.last() {
                    columns.insert(last.value.to_lowercase());
                }
            }
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                if Self::is_aggregate_function(&func_name) || func.over.is_some() {
                    return;
                }
                if let FunctionArguments::List(list) = &func.args {
                    for arg in &list.args {
                        match arg {
                            FunctionArg::Unnamed(arg_expr)
                            | FunctionArg::Named { arg: arg_expr, .. }
                            | FunctionArg::ExprNamed { arg: arg_expr, .. } => {
                                if let FunctionArgExpr::Expr(e) = arg_expr {
                                    Self::collect_non_aggregate_columns(e, columns);
                                }
                            }
                        }
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_non_aggregate_columns(left, columns);
                Self::collect_non_aggregate_columns(right, columns);
            }
            Expr::UnaryOp { expr, .. } => {
                Self::collect_non_aggregate_columns(expr, columns);
            }
            Expr::Nested(inner) => {
                Self::collect_non_aggregate_columns(inner, columns);
            }
            Expr::Cast { expr, .. } => {
                Self::collect_non_aggregate_columns(expr, columns);
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    Self::collect_non_aggregate_columns(op, columns);
                }
                for case_when in conditions {
                    Self::collect_non_aggregate_columns(&case_when.condition, columns);
                    Self::collect_non_aggregate_columns(&case_when.result, columns);
                }
                if let Some(else_res) = else_result {
                    Self::collect_non_aggregate_columns(else_res, columns);
                }
            }
            _ => {}
        }
    }

    fn is_aggregate_function(name: &str) -> bool {
        matches!(
            name.to_uppercase().as_str(),
            "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "ARRAY_AGG"
                | "STRING_AGG"
                | "BOOL_AND"
                | "BOOL_OR"
                | "EVERY"
                | "BIT_AND"
                | "BIT_OR"
                | "BIT_XOR"
                | "STDDEV"
                | "STDDEV_POP"
                | "STDDEV_SAMP"
                | "VARIANCE"
                | "VAR_POP"
                | "VAR_SAMP"
                | "COVAR_POP"
                | "COVAR_SAMP"
                | "CORR"
                | "REGR_AVGX"
                | "REGR_AVGY"
                | "REGR_COUNT"
                | "REGR_INTERCEPT"
                | "REGR_R2"
                | "REGR_SLOPE"
                | "REGR_SXX"
                | "REGR_SXY"
                | "REGR_SYY"
                | "APPROX_COUNT_DISTINCT"
                | "APPROX_QUANTILES"
                | "PERCENTILE_CONT"
                | "PERCENTILE_DISC"
                | "ANY"
                | "ANYVALUE"
                | "ANY_VALUE"
                | "ANY_HEAVY"
                | "ANYHEAVY"
                | "UNIQ"
                | "UNIQ_EXACT"
                | "UNIQ_HLL12"
                | "UNIQ_COMBINED"
                | "UNIQ_COMBINED_64"
                | "UNIQ_THETA_SKETCH"
                | "QUANTILE"
                | "QUANTILE_EXACT"
                | "QUANTILE_TIMING"
                | "QUANTILE_TDIGEST"
                | "QUANTILES"
                | "QUANTILES_EXACT"
                | "QUANTILES_TIMING"
                | "QUANTILES_TDIGEST"
                | "GROUP_ARRAY"
                | "GROUP_UNIQ_ARRAY"
                | "GROUPARRAY"
                | "GROUPUNIQARRAY"
                | "GROUP_ARRAY_MOVING_AVG"
                | "GROUPARRAYMOVINGAVG"
                | "GROUP_ARRAY_MOVING_SUM"
                | "GROUPARRAYMOVINGSUM"
                | "TOP_K"
                | "TOPK"
                | "ARG_MIN"
                | "ARG_MAX"
                | "ARGMIN"
                | "ARGMAX"
                | "FIRST_VALUE"
                | "LAST_VALUE"
                | "SUM_IF"
                | "COUNT_IF"
                | "AVG_IF"
                | "SUMIF"
                | "COUNTIF"
                | "AVGIF"
                | "INTERVAL_LENGTH_SUM"
                | "INTERVALLENGTHSUM"
                | "SUM_WITH_OVERFLOW"
                | "SUMWITHOVERFLOW"
                | "WINDOW_FUNNEL"
                | "WINDOWFUNNEL"
                | "JSON_AGG"
                | "JSONB_AGG"
                | "JSON_OBJECT_AGG"
                | "JSONB_OBJECT_AGG"
        )
    }

    fn validate_expr_with_context(&self, expr: &Expr, context: ValidationContext) -> Result<()> {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string();
                let func_name_upper = func_name.to_uppercase();

                if func.over.is_some() {
                    if !self.registry.is_enabled(T611_WINDOW_FUNCTIONS) {
                        return Err(Error::unsupported_feature(
                            "Window functions (T611) are not enabled. Use CALL enable_feature('T611') to enable them.".to_string()
                        ));
                    }
                    if context == ValidationContext::Where {
                        return Err(Error::InvalidQuery(
                            "Window functions are not allowed in WHERE clause".to_string(),
                        ));
                    }
                }

                if Self::is_aggregate_function(&func_name_upper)
                    && context == ValidationContext::Where
                {
                    return Err(Error::InvalidQuery(
                        "Aggregate functions are not allowed in WHERE clause".to_string(),
                    ));
                }

                validate_function(&func_name, self.registry)?;

                if let FunctionArguments::List(list) = &func.args {
                    if func_name_upper == "COALESCE" && list.args.is_empty() {
                        return Err(Error::InvalidQuery(
                            "COALESCE requires at least one argument".to_string(),
                        ));
                    }
                    for arg in &list.args {
                        match arg {
                            FunctionArg::Unnamed(arg_expr)
                            | FunctionArg::Named { arg: arg_expr, .. }
                            | FunctionArg::ExprNamed { arg: arg_expr, .. } => match arg_expr {
                                FunctionArgExpr::Expr(e) => {
                                    self.validate_expr_with_context(e, context)?;
                                }
                                FunctionArgExpr::Wildcard
                                | FunctionArgExpr::QualifiedWildcard(_) => {}
                            },
                        }
                    }
                }
                Ok(())
            }
            Expr::BinaryOp { left, right, .. } => {
                self.validate_expr_with_context(left, context)?;
                self.validate_expr_with_context(right, context)?;
                Ok(())
            }
            Expr::UnaryOp { expr, .. } => self.validate_expr_with_context(expr, context),
            Expr::Cast { expr, .. } => self.validate_expr_with_context(expr, context),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.validate_expr_with_context(op, context)?;
                }
                for case_when in conditions {
                    self.validate_expr_with_context(&case_when.condition, context)?;
                    self.validate_expr_with_context(&case_when.result, context)?;
                }
                if let Some(else_res) = else_result {
                    self.validate_expr_with_context(else_res, context)?;
                }
                Ok(())
            }
            Expr::Nested(inner) => self.validate_expr_with_context(inner, context),
            Expr::Subquery(query) => self.validate_query(query),
            Expr::InSubquery { expr, subquery, .. } => {
                self.validate_expr_with_context(expr, context)?;
                self.validate_query(subquery)?;
                Ok(())
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.validate_expr_with_context(expr, context)?;
                self.validate_expr_with_context(low, context)?;
                self.validate_expr_with_context(high, context)?;
                Ok(())
            }
            Expr::InList { expr, list, .. } => {
                self.validate_expr_with_context(expr, context)?;
                for item in list {
                    self.validate_expr_with_context(item, context)?;
                }
                Ok(())
            }
            Expr::Ceil { expr, .. } => self.validate_expr_with_context(expr, context),
            Expr::Floor { expr, .. } => self.validate_expr_with_context(expr, context),
            Expr::Position { expr, r#in } => {
                self.validate_expr_with_context(expr, context)?;
                self.validate_expr_with_context(r#in, context)?;
                Ok(())
            }
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                self.validate_expr_with_context(expr, context)?;
                self.validate_expr_with_context(overlay_what, context)?;
                self.validate_expr_with_context(overlay_from, context)?;
                if let Some(for_expr) = overlay_for {
                    self.validate_expr_with_context(for_expr, context)?;
                }
                Ok(())
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                self.validate_expr_with_context(expr, context)?;
                if let Some(from) = substring_from {
                    self.validate_expr_with_context(from, context)?;
                }
                if let Some(for_expr) = substring_for {
                    self.validate_expr_with_context(for_expr, context)?;
                }
                Ok(())
            }
            Expr::Trim {
                expr, trim_what, ..
            } => {
                self.validate_expr_with_context(expr, context)?;
                if let Some(what) = trim_what {
                    self.validate_expr_with_context(what, context)?;
                }
                Ok(())
            }
            Expr::Extract { expr, .. } => self.validate_expr_with_context(expr, context),
            Expr::Array(array) => {
                for elem in &array.elem {
                    self.validate_expr_with_context(elem, context)?;
                }
                Ok(())
            }
            Expr::Tuple(exprs) => {
                for expr in exprs {
                    self.validate_expr_with_context(expr, context)?;
                }
                Ok(())
            }

            Expr::Like { expr, pattern, .. }
            | Expr::ILike { expr, pattern, .. }
            | Expr::SimilarTo { expr, pattern, .. } => {
                self.validate_expr_with_context(expr, context)?;
                self.validate_expr_with_context(pattern, context)?;
                Ok(())
            }
            Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr) => self.validate_expr_with_context(expr, context),
            Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
                self.validate_expr_with_context(left, context)?;
                self.validate_expr_with_context(right, context)?;
                Ok(())
            }
            Expr::Exists { subquery, .. } => self.validate_query(subquery),
            Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
                self.validate_expr_with_context(left, context)?;
                self.validate_expr_with_context(right, context)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_capability::FeatureRegistry;
    use yachtsql_parser::{DialectType, Parser, Statement};

    use super::*;

    fn parse_statement(sql: &str, dialect: DialectType) -> Statement {
        let parser = Parser::with_dialect(dialect);
        let stmts = parser.parse_sql(sql).expect("Failed to parse SQL");
        stmts.into_iter().next().expect("No statement parsed")
    }

    #[test]
    fn test_validate_core_functions() {
        let registry = FeatureRegistry::new(DialectType::PostgreSQL);
        let stmt = parse_statement(
            "SELECT UPPER(name), COUNT(*) FROM users",
            DialectType::PostgreSQL,
        );

        assert!(validate_statement(&stmt, &registry).is_ok());
    }

    #[test]
    fn test_validate_postgres_specific_function() {
        let pg_registry = FeatureRegistry::new(DialectType::PostgreSQL);
        let bq_registry = FeatureRegistry::new(DialectType::BigQuery);

        let stmt = parse_statement("SELECT JSONB(data) FROM table1", DialectType::PostgreSQL);

        assert!(validate_statement(&stmt, &pg_registry).is_ok());
        assert!(validate_statement(&stmt, &bq_registry).is_err());
    }

    #[test]
    fn test_validate_nested_functions() {
        let registry = FeatureRegistry::new(DialectType::PostgreSQL);
        let stmt = parse_statement(
            "SELECT UPPER(CONCAT(first, last)) FROM users WHERE LENGTH(name) > 5",
            DialectType::PostgreSQL,
        );

        assert!(validate_statement(&stmt, &registry).is_ok());
    }

    #[test]
    fn test_validate_update_statement() {
        let pg_registry = FeatureRegistry::new(DialectType::PostgreSQL);

        let stmt = parse_statement(
            "UPDATE users SET name = UPPER(name) WHERE id > 0",
            DialectType::PostgreSQL,
        );

        assert!(validate_statement(&stmt, &pg_registry).is_ok());
    }
}
