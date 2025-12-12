mod delete;
mod insert;
mod merge;
mod truncate;
mod update;

use std::rc::Rc;

pub use delete::DmlDeleteExecutor;
pub use insert::DmlInsertExecutor;
pub use merge::DmlMergeExecutor;
use sqlparser::ast::Expr as SqlExpr;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
pub use truncate::DmlTruncateExecutor;
pub use update::DmlUpdateExecutor;
use yachtsql_common::error::{Error, Result};
use yachtsql_storage::{CheckEvaluator, Row, Schema};

use crate::query_executor::expression_evaluator::ExpressionEvaluator;

fn build_check_evaluator() -> CheckEvaluator {
    Rc::new(|schema: &Schema, row: &Row, expr_sql: &str| {
        let parsed_expr = parse_check_expression(expr_sql)?;
        let evaluator = ExpressionEvaluator::new(schema);
        let value = evaluator.evaluate_condition_expr(&parsed_expr, row)?;

        if value.is_null() {
            return Ok(true);
        }
        if let Some(result) = value.as_bool() {
            return Ok(result);
        }
        Err(Error::InvalidQuery(format!(
            "CHECK constraint expression '{}' must evaluate to BOOLEAN",
            expr_sql
        )))
    })
}

fn parse_check_expression(expr_sql: &str) -> Result<SqlExpr> {
    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, expr_sql);

    let tokens = tokenizer.tokenize().map_err(|e| {
        Error::InvalidQuery(format!(
            "Failed to tokenize CHECK expression '{}': {}",
            expr_sql, e
        ))
    })?;

    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    parser.parse_expr().map_err(|e| {
        Error::InvalidQuery(format!(
            "Failed to parse CHECK expression '{}': {}",
            expr_sql, e
        ))
    })
}
