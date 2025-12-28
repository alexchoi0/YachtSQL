use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;

use crate::ir_evaluator::IrEvaluator;

pub(super) fn extract_agg_arg(
    evaluator: &IrEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<Value> {
    match agg_expr {
        Expr::Aggregate { args, .. } => {
            if args.is_empty() {
                Ok(Value::Null)
            } else if matches!(&args[0], Expr::Wildcard { .. }) {
                Ok(Value::Int64(1))
            } else {
                evaluator.evaluate(&args[0], record)
            }
        }
        Expr::UserDefinedAggregate { args, .. } => {
            if args.is_empty() {
                Ok(Value::Null)
            } else {
                evaluator.evaluate(&args[0], record)
            }
        }
        Expr::Alias { expr, .. } => extract_agg_arg(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => evaluator.evaluate(agg_expr, record),
    }
}

pub(super) fn extract_conditional_agg_args(
    evaluator: &IrEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<(Value, bool)> {
    match agg_expr {
        Expr::Aggregate { args, .. } => {
            if args.len() >= 2 {
                let value = evaluator.evaluate(&args[0], record)?;
                let condition_val = evaluator.evaluate(&args[1], record)?;
                let condition = condition_val.as_bool().unwrap_or(false);
                Ok((value, condition))
            } else if args.len() == 1 {
                let value = evaluator.evaluate(&args[0], record)?;
                Ok((value, true))
            } else {
                Ok((Value::Null, false))
            }
        }
        Expr::UserDefinedAggregate { args, .. } => {
            if args.len() >= 2 {
                let value = evaluator.evaluate(&args[0], record)?;
                let condition_val = evaluator.evaluate(&args[1], record)?;
                let condition = condition_val.as_bool().unwrap_or(false);
                Ok((value, condition))
            } else if args.len() == 1 {
                let value = evaluator.evaluate(&args[0], record)?;
                Ok((value, true))
            } else {
                Ok((Value::Null, false))
            }
        }
        Expr::Alias { expr, .. } => extract_conditional_agg_args(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => Ok((Value::Null, false)),
    }
}

pub(super) fn extract_bivariate_args(
    evaluator: &IrEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<(Value, Value)> {
    match agg_expr {
        Expr::Aggregate { args, .. } | Expr::UserDefinedAggregate { args, .. } => {
            if args.len() >= 2 {
                let x = evaluator.evaluate(&args[0], record)?;
                let y = evaluator.evaluate(&args[1], record)?;
                Ok((x, y))
            } else {
                Ok((Value::Null, Value::Null))
            }
        }
        Expr::Alias { expr, .. } => extract_bivariate_args(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => Ok((Value::Null, Value::Null)),
    }
}

pub(super) fn extract_order_by_keys(
    evaluator: &IrEvaluator,
    agg_expr: &Expr,
    record: &yachtsql_storage::Record,
) -> Result<Vec<(Value, bool)>> {
    match agg_expr {
        Expr::Aggregate { order_by, .. } => {
            let mut keys = Vec::new();
            for sort_expr in order_by {
                let val = evaluator.evaluate(&sort_expr.expr, record)?;
                keys.push((val, sort_expr.asc));
            }
            Ok(keys)
        }
        Expr::UserDefinedAggregate { .. } => Ok(Vec::new()),
        Expr::Alias { expr, .. } => extract_order_by_keys(evaluator, expr, record),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => Ok(Vec::new()),
    }
}

#[allow(dead_code)]
pub(super) fn has_order_by(agg_expr: &Expr) -> bool {
    match agg_expr {
        Expr::Aggregate { order_by, .. } => !order_by.is_empty(),
        Expr::UserDefinedAggregate { .. } => false,
        Expr::Alias { expr, .. } => has_order_by(expr),
        Expr::Literal(_)
        | Expr::Column { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Window { .. }
        | Expr::AggregateWindow { .. }
        | Expr::Case { .. }
        | Expr::Cast { .. }
        | Expr::IsNull { .. }
        | Expr::IsDistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::InUnnest { .. }
        | Expr::Exists { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Extract { .. }
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Position { .. }
        | Expr::Overlay { .. }
        | Expr::Array { .. }
        | Expr::ArrayAccess { .. }
        | Expr::Struct { .. }
        | Expr::StructAccess { .. }
        | Expr::TypedString { .. }
        | Expr::Interval { .. }
        | Expr::Wildcard { .. }
        | Expr::Subquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::ArraySubquery(_)
        | Expr::Parameter { .. }
        | Expr::Variable { .. }
        | Expr::Placeholder { .. }
        | Expr::Lambda { .. }
        | Expr::AtTimeZone { .. }
        | Expr::JsonAccess { .. }
        | Expr::Default => false,
    }
}
