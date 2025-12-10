use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::BinaryOp;
use yachtsql_optimizer::expr::{Expr, UnaryOp};
use yachtsql_storage::Schema;

use super::super::ProjectionWithExprExec;
use crate::storage::table::Row;

pub(super) fn resolve_column_value(
    row: &Row,
    schema: &Schema,
    column_name: &str,
    table_qualifier: Option<&str>,
) -> Value {
    if let Some(table_name) = table_qualifier {
        let qualified_name = format!("{}.{}", table_name, column_name);
        row.get_by_name(schema, &qualified_name)
            .or_else(|| row.get_by_name(schema, column_name))
            .cloned()
            .unwrap_or(Value::null())
    } else {
        row.get_by_name(schema, column_name)
            .cloned()
            .unwrap_or(Value::null())
    }
}

pub(super) fn create_combined_schema(source_schema: &Schema, target_schema: &Schema) -> Schema {
    let combined_fields: Vec<_> = source_schema
        .fields()
        .iter()
        .chain(target_schema.fields().iter())
        .cloned()
        .collect();
    Schema::from_fields(combined_fields)
}

pub(super) fn check_when_condition(
    condition: &Option<Expr>,
    row: &Row,
    source_schema: &Schema,
    target_schema: &Schema,
) -> Result<bool> {
    match condition {
        None => Ok(true),
        Some(expr) => evaluate_condition_row(expr, row, source_schema, target_schema),
    }
}

pub(super) fn evaluate_condition_row(
    expr: &Expr,
    row: &Row,
    source_schema: &Schema,
    target_schema: &Schema,
) -> Result<bool> {
    let combined_schema = create_combined_schema(source_schema, target_schema);
    let result = evaluate_expr_row(expr, row, &combined_schema)?;

    if let Some(b) = result.as_bool() {
        Ok(b)
    } else if result.is_null() {
        Ok(false)
    } else {
        Ok(false)
    }
}

pub(super) fn evaluate_expr_row(expr: &Expr, row: &Row, schema: &Schema) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => Ok(lit.to_value()),
        Expr::Column { name, table } => {
            Ok(resolve_column_value(row, schema, name, table.as_deref()))
        }
        Expr::StructFieldAccess {
            expr: inner_expr,
            field,
        } => match inner_expr.as_ref() {
            Expr::Column {
                name: table_alias, ..
            } => {
                let qualified_name = format!("{}.{}", table_alias, field);
                Ok(row
                    .get_by_name(schema, &qualified_name)
                    .cloned()
                    .or_else(|| row.get_by_name(schema, field).cloned())
                    .unwrap_or(Value::null()))
            }
            _ => {
                let val = evaluate_expr_row(inner_expr, row, schema)?;
                if let Some(fields) = val.as_struct() {
                    Ok(fields.get(field).cloned().unwrap_or(Value::null()))
                } else {
                    Ok(Value::null())
                }
            }
        },
        Expr::BinaryOp { left, op, right } => {
            let left_val = evaluate_expr_row(left, row, schema)?;
            let right_val = evaluate_expr_row(right, row, schema)?;
            apply_binary_op(&left_val, *op, &right_val)
        }
        Expr::UnaryOp {
            op,
            expr: inner_expr,
        } => {
            let val = evaluate_expr_row(inner_expr, row, schema)?;
            match op {
                UnaryOp::Not => {
                    if let Some(b) = val.as_bool() {
                        Ok(Value::bool_val(!b))
                    } else if val.is_null() {
                        Ok(Value::null())
                    } else {
                        Err(Error::InvalidOperation("NOT requires boolean".to_string()))
                    }
                }
                UnaryOp::Negate => {
                    if let Some(i) = val.as_i64() {
                        Ok(Value::int64(-i))
                    } else if let Some(f) = val.as_f64() {
                        Ok(Value::float64(-f))
                    } else if val.is_null() {
                        Ok(Value::null())
                    } else {
                        Err(Error::InvalidOperation("NEG requires numeric".to_string()))
                    }
                }
                UnaryOp::Plus => Ok(val),
                UnaryOp::IsNull => Ok(Value::bool_val(val.is_null())),
                UnaryOp::IsNotNull => Ok(Value::bool_val(!val.is_null())),
                UnaryOp::BitwiseNot => {
                    if let Some(i) = val.as_i64() {
                        Ok(Value::int64(!i))
                    } else if val.is_null() {
                        Ok(Value::null())
                    } else {
                        Err(Error::InvalidOperation(
                            "Bitwise NOT requires integer".to_string(),
                        ))
                    }
                }
                UnaryOp::TSQueryNot => {
                    if val.is_null() {
                        Ok(Value::null())
                    } else if let Some(s) = val.as_str() {
                        let result = yachtsql_functions::fulltext::tsquery_negate(s)
                            .map_err(|e| Error::InvalidOperation(e.to_string()))?;
                        Ok(Value::string(result))
                    } else {
                        Err(Error::InvalidOperation(
                            "TSQuery NOT requires string".to_string(),
                        ))
                    }
                }
            }
        }
        Expr::Cast {
            expr: inner,
            data_type,
        } => {
            let value = evaluate_expr_row(inner, row, schema)?;
            ProjectionWithExprExec::cast_value(value, data_type)
        }
        Expr::TryCast {
            expr: inner,
            data_type,
        } => {
            let value = evaluate_expr_row(inner, row, schema)?;
            Ok(ProjectionWithExprExec::try_cast_value(value, data_type))
        }
        Expr::Function { name, args, .. } => {
            use yachtsql_ir::FunctionName;
            if matches!(name, FunctionName::CurrentTimestamp | FunctionName::Now) && args.is_empty()
            {
                use chrono::Utc;
                let now = Utc::now();
                Ok(Value::timestamp(now))
            } else {
                Err(Error::unsupported_feature(format!(
                    "Function not supported in MERGE VALUES: {}",
                    name
                )))
            }
        }
        _ => Err(Error::unsupported_feature(format!(
            "Expression type not supported in MERGE: {:?}",
            expr
        ))),
    }
}

pub(super) fn apply_binary_op(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
    use rust_decimal::Decimal;

    if left.is_null() || right.is_null() {
        return Ok(Value::null());
    }

    match op {
        BinaryOp::Equal => Ok(Value::bool_val(left == right)),
        BinaryOp::NotEqual => Ok(Value::bool_val(left != right)),
        BinaryOp::LessThan => {
            if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                Ok(Value::bool_val(l < r))
            } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                Ok(Value::bool_val(l < r))
            } else {
                Ok(Value::null())
            }
        }
        BinaryOp::LessThanOrEqual => {
            if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                Ok(Value::bool_val(l <= r))
            } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                Ok(Value::bool_val(l <= r))
            } else {
                Ok(Value::null())
            }
        }
        BinaryOp::GreaterThan => {
            if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                Ok(Value::bool_val(l > r))
            } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                Ok(Value::bool_val(l > r))
            } else {
                Ok(Value::null())
            }
        }
        BinaryOp::GreaterThanOrEqual => {
            if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                Ok(Value::bool_val(l >= r))
            } else if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                Ok(Value::bool_val(l >= r))
            } else {
                Ok(Value::null())
            }
        }

        BinaryOp::And => {
            if let (Some(l), Some(r)) = (left.as_bool(), right.as_bool()) {
                Ok(Value::bool_val(l && r))
            } else {
                Ok(Value::null())
            }
        }
        BinaryOp::Or => {
            if let (Some(l), Some(r)) = (left.as_bool(), right.as_bool()) {
                Ok(Value::bool_val(l || r))
            } else {
                Ok(Value::null())
            }
        }

        BinaryOp::Add => {
            if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
                Ok(Value::int64(a + b))
            } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
                Ok(Value::float64(a + b))
            } else if let (Some(a), Some(b)) = (left.as_numeric(), right.as_numeric()) {
                Ok(Value::numeric(a + b))
            } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_numeric()) {
                Ok(Value::numeric(Decimal::from(a) + b))
            } else if let (Some(a), Some(b)) = (left.as_numeric(), right.as_i64()) {
                Ok(Value::numeric(a + Decimal::from(b)))
            } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_f64()) {
                Ok(Value::float64((a as f64) + b))
            } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_i64()) {
                Ok(Value::float64(a + (b as f64)))
            } else {
                Err(Error::TypeMismatch {
                    expected: "numeric types".to_string(),
                    actual: format!("{:?} and {:?}", left, right),
                })
            }
        }
        BinaryOp::Subtract => {
            if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
                Ok(Value::int64(a - b))
            } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
                Ok(Value::float64(a - b))
            } else if let (Some(a), Some(b)) = (left.as_numeric(), right.as_numeric()) {
                Ok(Value::numeric(a - b))
            } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_numeric()) {
                Ok(Value::numeric(Decimal::from(a) - b))
            } else if let (Some(a), Some(b)) = (left.as_numeric(), right.as_i64()) {
                Ok(Value::numeric(a - Decimal::from(b)))
            } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_f64()) {
                Ok(Value::float64((a as f64) - b))
            } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_i64()) {
                Ok(Value::float64(a - (b as f64)))
            } else {
                Err(Error::TypeMismatch {
                    expected: "numeric types".to_string(),
                    actual: format!("{:?} and {:?}", left, right),
                })
            }
        }
        BinaryOp::Multiply => {
            if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
                Ok(Value::int64(a * b))
            } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
                Ok(Value::float64(a * b))
            } else if let (Some(a), Some(b)) = (left.as_numeric(), right.as_numeric()) {
                Ok(Value::numeric(a * b))
            } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_numeric()) {
                Ok(Value::numeric(Decimal::from(a) * b))
            } else if let (Some(a), Some(b)) = (left.as_numeric(), right.as_i64()) {
                Ok(Value::numeric(a * Decimal::from(b)))
            } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_f64()) {
                Ok(Value::float64((a as f64) * b))
            } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_i64()) {
                Ok(Value::float64(a * (b as f64)))
            } else {
                Err(Error::TypeMismatch {
                    expected: "numeric types".to_string(),
                    actual: format!("{:?} and {:?}", left, right),
                })
            }
        }
        BinaryOp::Divide => {
            if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
                if b == 0 {
                    return Ok(Value::null());
                }
                Ok(Value::int64(a / b))
            } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
                Ok(Value::float64(a / b))
            } else if let (Some(a), Some(b)) = (left.as_numeric(), right.as_numeric()) {
                if b.is_zero() {
                    return Ok(Value::null());
                }
                Ok(Value::numeric(a / b))
            } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_numeric()) {
                if b.is_zero() {
                    return Ok(Value::null());
                }
                Ok(Value::numeric(Decimal::from(a) / b))
            } else if let (Some(a), Some(b)) = (left.as_numeric(), right.as_i64()) {
                if b == 0 {
                    return Ok(Value::null());
                }
                Ok(Value::numeric(a / Decimal::from(b)))
            } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_f64()) {
                Ok(Value::float64((a as f64) / b))
            } else if let (Some(a), Some(b)) = (left.as_f64(), right.as_i64()) {
                Ok(Value::float64(a / (b as f64)))
            } else {
                Err(Error::TypeMismatch {
                    expected: "numeric types".to_string(),
                    actual: format!("{:?} and {:?}", left, right),
                })
            }
        }
        BinaryOp::Modulo => {
            if let (Some(a), Some(b)) = (left.as_i64(), right.as_i64()) {
                if b == 0 {
                    return Ok(Value::null());
                }
                Ok(Value::int64(a % b))
            } else if let (Some(a), Some(b)) = (left.as_numeric(), right.as_numeric()) {
                if b.is_zero() {
                    return Ok(Value::null());
                }
                Ok(Value::numeric(a % b))
            } else if let (Some(a), Some(b)) = (left.as_i64(), right.as_numeric()) {
                if b.is_zero() {
                    return Ok(Value::null());
                }
                Ok(Value::numeric(Decimal::from(a) % b))
            } else if let (Some(a), Some(b)) = (left.as_numeric(), right.as_i64()) {
                if b == 0 {
                    return Ok(Value::null());
                }
                Ok(Value::numeric(a % Decimal::from(b)))
            } else {
                Err(Error::TypeMismatch {
                    expected: "numeric types".to_string(),
                    actual: format!("{:?} and {:?}", left, right),
                })
            }
        }
        _ => Err(Error::unsupported_feature(format!(
            "Binary operator not supported in MERGE: {:?}",
            op
        ))),
    }
}
