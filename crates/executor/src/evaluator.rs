//! Expression evaluation for WHERE clauses, projections, etc.

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_storage::{Row, Schema};
use sqlparser::ast::{Expr, BinaryOperator, UnaryOperator, Value as SqlValue};

use crate::catalog::TableData;

pub struct Evaluator<'a> {
    schema: &'a Schema,
}

impl<'a> Evaluator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }

    pub fn evaluate(&self, expr: &Expr, row: &Row) -> Result<Value> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_uppercase();
                let idx = self.schema.fields()
                    .iter()
                    .position(|f| f.name.to_uppercase() == name)
                    .ok_or_else(|| Error::ColumnNotFound(ident.value.clone()))?;
                Ok(row.values().get(idx).cloned().unwrap_or(Value::null()))
            }

            Expr::CompoundIdentifier(parts) => {
                let name = parts.last()
                    .map(|i| i.value.to_uppercase())
                    .unwrap_or_default();
                let idx = self.schema.fields()
                    .iter()
                    .position(|f| f.name.to_uppercase() == name)
                    .ok_or_else(|| Error::ColumnNotFound(name.clone()))?;
                Ok(row.values().get(idx).cloned().unwrap_or(Value::null()))
            }

            Expr::Value(val) => self.evaluate_literal(&val.value),

            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate(left, row)?;
                let right_val = self.evaluate(right, row)?;
                self.evaluate_binary_op(&left_val, op, &right_val)
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate(expr, row)?;
                self.evaluate_unary_op(op, &val)
            }

            Expr::IsNull(inner) => {
                let val = self.evaluate(inner, row)?;
                Ok(Value::bool_val(val.is_null()))
            }

            Expr::IsNotNull(inner) => {
                let val = self.evaluate(inner, row)?;
                Ok(Value::bool_val(!val.is_null()))
            }

            Expr::Nested(inner) => self.evaluate(inner, row),

            Expr::Function(func) => {
                self.evaluate_function(func, row)
            }

            _ => Err(Error::UnsupportedFeature(format!(
                "Expression type not yet supported: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_literal(&self, val: &SqlValue) -> Result<Value> {
        match val {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::float64(f))
                } else {
                    Err(Error::ParseError(format!("Invalid number: {}", n)))
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(Value::string(s.clone()))
            }
            SqlValue::Boolean(b) => Ok(Value::bool_val(*b)),
            SqlValue::Null => Ok(Value::null()),
            _ => Err(Error::UnsupportedFeature(format!(
                "Literal type not yet supported: {:?}",
                val
            ))),
        }
    }

    fn evaluate_binary_op(&self, left: &Value, op: &BinaryOperator, right: &Value) -> Result<Value> {
        if left.is_null() || right.is_null() {
            match op {
                BinaryOperator::And => {
                    if let Some(false) = left.as_bool() {
                        return Ok(Value::bool_val(false));
                    }
                    if let Some(false) = right.as_bool() {
                        return Ok(Value::bool_val(false));
                    }
                    return Ok(Value::null());
                }
                BinaryOperator::Or => {
                    if let Some(true) = left.as_bool() {
                        return Ok(Value::bool_val(true));
                    }
                    if let Some(true) = right.as_bool() {
                        return Ok(Value::bool_val(true));
                    }
                    return Ok(Value::null());
                }
                _ => return Ok(Value::null()),
            }
        }

        match op {
            BinaryOperator::Eq => Ok(Value::bool_val(left == right)),
            BinaryOperator::NotEq => Ok(Value::bool_val(left != right)),
            BinaryOperator::Lt => self.compare_values(left, right, |ord| ord.is_lt()),
            BinaryOperator::LtEq => self.compare_values(left, right, |ord| ord.is_le()),
            BinaryOperator::Gt => self.compare_values(left, right, |ord| ord.is_gt()),
            BinaryOperator::GtEq => self.compare_values(left, right, |ord| ord.is_ge()),
            BinaryOperator::And => {
                let l = left.as_bool().ok_or_else(|| Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: left.data_type().to_string(),
                })?;
                let r = right.as_bool().ok_or_else(|| Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: right.data_type().to_string(),
                })?;
                Ok(Value::bool_val(l && r))
            }
            BinaryOperator::Or => {
                let l = left.as_bool().ok_or_else(|| Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: left.data_type().to_string(),
                })?;
                let r = right.as_bool().ok_or_else(|| Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: right.data_type().to_string(),
                })?;
                Ok(Value::bool_val(l || r))
            }
            BinaryOperator::Plus => self.numeric_op(left, right, |a, b| a + b, |a, b| a + b),
            BinaryOperator::Minus => self.numeric_op(left, right, |a, b| a - b, |a, b| a - b),
            BinaryOperator::Multiply => self.numeric_op(left, right, |a, b| a * b, |a, b| a * b),
            BinaryOperator::Divide => {
                if let Some(r) = right.as_i64() {
                    if r == 0 {
                        return Err(Error::DivisionByZero);
                    }
                }
                if let Some(r) = right.as_f64() {
                    if r == 0.0 {
                        return Err(Error::DivisionByZero);
                    }
                }
                self.numeric_op(left, right, |a, b| a / b, |a, b| a / b)
            }
            BinaryOperator::Modulo => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    if r == 0 {
                        return Err(Error::DivisionByZero);
                    }
                    return Ok(Value::int64(l % r));
                }
                Err(Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", left.data_type()),
                })
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Binary operator not yet supported: {:?}",
                op
            ))),
        }
    }

    fn compare_values<F>(&self, left: &Value, right: &Value, pred: F) -> Result<Value>
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            return Ok(Value::bool_val(pred(l.cmp(&r))));
        }
        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            return Ok(Value::bool_val(pred(l.partial_cmp(&r).unwrap_or(std::cmp::Ordering::Equal))));
        }
        if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
            return Ok(Value::bool_val(pred(l.cmp(r))));
        }
        if let Some(l) = left.as_i64() {
            if let Some(r) = right.as_f64() {
                return Ok(Value::bool_val(pred((l as f64).partial_cmp(&r).unwrap_or(std::cmp::Ordering::Equal))));
            }
        }
        if let Some(l) = left.as_f64() {
            if let Some(r) = right.as_i64() {
                return Ok(Value::bool_val(pred(l.partial_cmp(&(r as f64)).unwrap_or(std::cmp::Ordering::Equal))));
            }
        }
        Err(Error::TypeMismatch {
            expected: "comparable types".to_string(),
            actual: format!("{:?} vs {:?}", left.data_type(), right.data_type()),
        })
    }

    fn numeric_op<F, G>(&self, left: &Value, right: &Value, int_op: F, float_op: G) -> Result<Value>
    where
        F: Fn(i64, i64) -> i64,
        G: Fn(f64, f64) -> f64,
    {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            return Ok(Value::int64(int_op(l, r)));
        }
        let l = left.as_f64().or_else(|| left.as_i64().map(|i| i as f64));
        let r = right.as_f64().or_else(|| right.as_i64().map(|i| i as f64));
        if let (Some(l), Some(r)) = (l, r) {
            return Ok(Value::float64(float_op(l, r)));
        }
        Err(Error::TypeMismatch {
            expected: "numeric types".to_string(),
            actual: format!("{:?} vs {:?}", left.data_type(), right.data_type()),
        })
    }

    fn evaluate_unary_op(&self, op: &UnaryOperator, val: &Value) -> Result<Value> {
        match op {
            UnaryOperator::Not => {
                if val.is_null() {
                    return Ok(Value::null());
                }
                let b = val.as_bool().ok_or_else(|| Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: val.data_type().to_string(),
                })?;
                Ok(Value::bool_val(!b))
            }
            UnaryOperator::Minus => {
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = val.as_i64() {
                    return Ok(Value::int64(-i));
                }
                if let Some(f) = val.as_f64() {
                    return Ok(Value::float64(-f));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            UnaryOperator::Plus => Ok(val.clone()),
            _ => Err(Error::UnsupportedFeature(format!(
                "Unary operator not yet supported: {:?}",
                op
            ))),
        }
    }

    fn evaluate_function(&self, func: &sqlparser::ast::Function, row: &Row) -> Result<Value> {
        let name = func.name.to_string().to_uppercase();

        match name.as_str() {
            "COALESCE" => {
                if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) = arg {
                            let val = self.evaluate(expr, row)?;
                            if !val.is_null() {
                                return Ok(val);
                            }
                        }
                    }
                }
                Ok(Value::null())
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Function not yet supported: {}",
                name
            ))),
        }
    }

    pub fn evaluate_to_bool(&self, expr: &Expr, row: &Row) -> Result<bool> {
        let val = self.evaluate(expr, row)?;
        if val.is_null() {
            return Ok(false);
        }
        val.as_bool().ok_or_else(|| Error::TypeMismatch {
            expected: "BOOL".to_string(),
            actual: val.data_type().to_string(),
        })
    }
}
