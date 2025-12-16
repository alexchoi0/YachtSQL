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

            Expr::Case { operand, conditions, else_result, .. } => {
                self.evaluate_case(operand.as_deref(), conditions, else_result.as_deref(), row)
            }

            Expr::Array(arr) => {
                self.evaluate_array(arr, row)
            }

            Expr::InList { expr, list, negated } => {
                self.evaluate_in_list(expr, list, *negated, row)
            }

            Expr::Between { expr, low, high, negated } => {
                self.evaluate_between(expr, low, high, *negated, row)
            }

            Expr::Like { expr, pattern, negated, .. } => {
                self.evaluate_like(expr, pattern, *negated, row)
            }

            Expr::ILike { expr, pattern, negated, .. } => {
                self.evaluate_ilike(expr, pattern, *negated, row)
            }

            Expr::Cast { expr, data_type, .. } => {
                self.evaluate_cast(expr, data_type, row)
            }

            Expr::Tuple(exprs) => {
                let mut values = Vec::with_capacity(exprs.len());
                for e in exprs {
                    values.push(self.evaluate(e, row)?);
                }
                Ok(Value::array(values))
            }

            _ => Err(Error::UnsupportedFeature(format!(
                "Expression type not yet supported: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_case(
        &self,
        operand: Option<&Expr>,
        conditions: &[sqlparser::ast::CaseWhen],
        else_result: Option<&Expr>,
        row: &Row,
    ) -> Result<Value> {
        match operand {
            Some(op_expr) => {
                let op_val = self.evaluate(op_expr, row)?;
                for cond in conditions {
                    let when_val = self.evaluate(&cond.condition, row)?;
                    if op_val == when_val {
                        return self.evaluate(&cond.result, row);
                    }
                }
            }
            None => {
                for cond in conditions {
                    let cond_val = self.evaluate(&cond.condition, row)?;
                    if let Some(true) = cond_val.as_bool() {
                        return self.evaluate(&cond.result, row);
                    }
                }
            }
        }
        match else_result {
            Some(else_expr) => self.evaluate(else_expr, row),
            None => Ok(Value::null()),
        }
    }

    fn evaluate_array(&self, arr: &sqlparser::ast::Array, row: &Row) -> Result<Value> {
        let mut values = Vec::with_capacity(arr.elem.len());
        for elem in &arr.elem {
            values.push(self.evaluate(elem, row)?);
        }
        Ok(Value::array(values))
    }

    fn evaluate_in_list(&self, expr: &Expr, list: &[Expr], negated: bool, row: &Row) -> Result<Value> {
        let val = self.evaluate(expr, row)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let mut found = false;
        let mut has_null = false;
        for item in list {
            let item_val = self.evaluate(item, row)?;
            if item_val.is_null() {
                has_null = true;
            } else if val == item_val {
                found = true;
                break;
            }
        }
        let result = if found {
            true
        } else if has_null {
            return Ok(Value::null());
        } else {
            false
        };
        Ok(Value::bool_val(if negated { !result } else { result }))
    }

    fn evaluate_between(&self, expr: &Expr, low: &Expr, high: &Expr, negated: bool, row: &Row) -> Result<Value> {
        let val = self.evaluate(expr, row)?;
        let low_val = self.evaluate(low, row)?;
        let high_val = self.evaluate(high, row)?;

        if val.is_null() || low_val.is_null() || high_val.is_null() {
            return Ok(Value::null());
        }

        let ge_low = self.compare_values(&val, &low_val, |ord| ord.is_ge())?;
        let le_high = self.compare_values(&val, &high_val, |ord| ord.is_le())?;

        let in_range = ge_low.as_bool().unwrap_or(false) && le_high.as_bool().unwrap_or(false);
        Ok(Value::bool_val(if negated { !in_range } else { in_range }))
    }

    fn evaluate_like(&self, expr: &Expr, pattern: &Expr, negated: bool, row: &Row) -> Result<Value> {
        let val = self.evaluate(expr, row)?;
        let pat = self.evaluate(pattern, row)?;

        if val.is_null() || pat.is_null() {
            return Ok(Value::null());
        }

        let val_str = val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: val.data_type().to_string(),
        })?;
        let pat_str = pat.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: pat.data_type().to_string(),
        })?;

        let matches = self.like_match(val_str, pat_str, false);
        Ok(Value::bool_val(if negated { !matches } else { matches }))
    }

    fn evaluate_ilike(&self, expr: &Expr, pattern: &Expr, negated: bool, row: &Row) -> Result<Value> {
        let val = self.evaluate(expr, row)?;
        let pat = self.evaluate(pattern, row)?;

        if val.is_null() || pat.is_null() {
            return Ok(Value::null());
        }

        let val_str = val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: val.data_type().to_string(),
        })?;
        let pat_str = pat.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: pat.data_type().to_string(),
        })?;

        let matches = self.like_match(val_str, pat_str, true);
        Ok(Value::bool_val(if negated { !matches } else { matches }))
    }

    fn like_match(&self, text: &str, pattern: &str, case_insensitive: bool) -> bool {
        let (text, pattern) = if case_insensitive {
            (text.to_lowercase(), pattern.to_lowercase())
        } else {
            (text.to_string(), pattern.to_string())
        };

        let regex_pattern = pattern
            .replace('%', ".*")
            .replace('_', ".");
        let regex_pattern = format!("^{}$", regex_pattern);

        regex::Regex::new(&regex_pattern)
            .map(|re| re.is_match(&text))
            .unwrap_or(false)
    }

    fn evaluate_cast(&self, expr: &Expr, target_type: &sqlparser::ast::DataType, row: &Row) -> Result<Value> {
        let val = self.evaluate(expr, row)?;
        if val.is_null() {
            return Ok(Value::null());
        }

        match target_type {
            sqlparser::ast::DataType::Int64 | sqlparser::ast::DataType::BigInt(_) | sqlparser::ast::DataType::Integer(_) => {
                if let Some(i) = val.as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(f) = val.as_f64() {
                    return Ok(Value::int64(f as i64));
                }
                if let Some(s) = val.as_str() {
                    if let Ok(i) = s.parse::<i64>() {
                        return Ok(Value::int64(i));
                    }
                }
                if let Some(b) = val.as_bool() {
                    return Ok(Value::int64(if b { 1 } else { 0 }));
                }
                Err(Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Float64 | sqlparser::ast::DataType::Double(_) => {
                if let Some(f) = val.as_f64() {
                    return Ok(Value::float64(f));
                }
                if let Some(i) = val.as_i64() {
                    return Ok(Value::float64(i as f64));
                }
                if let Some(s) = val.as_str() {
                    if let Ok(f) = s.parse::<f64>() {
                        return Ok(Value::float64(f));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::String(_) | sqlparser::ast::DataType::Varchar(_) | sqlparser::ast::DataType::Text => {
                Ok(Value::string(val.to_string()))
            }
            sqlparser::ast::DataType::Boolean | sqlparser::ast::DataType::Bool => {
                if let Some(b) = val.as_bool() {
                    return Ok(Value::bool_val(b));
                }
                if let Some(i) = val.as_i64() {
                    return Ok(Value::bool_val(i != 0));
                }
                if let Some(s) = val.as_str() {
                    let lower = s.to_lowercase();
                    if lower == "true" || lower == "1" || lower == "yes" {
                        return Ok(Value::bool_val(true));
                    }
                    if lower == "false" || lower == "0" || lower == "no" {
                        return Ok(Value::bool_val(false));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "CAST to {:?} not yet supported",
                target_type
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
            BinaryOperator::StringConcat => {
                let l_str = left.to_string();
                let r_str = right.to_string();
                Ok(Value::string(format!("{}{}", l_str, r_str)))
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
        let args = self.extract_function_args(func, row)?;

        match name.as_str() {
            "COALESCE" => {
                for val in args {
                    if !val.is_null() {
                        return Ok(val);
                    }
                }
                Ok(Value::null())
            }
            "NULLIF" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery("NULLIF requires 2 arguments".to_string()));
                }
                if args[0] == args[1] {
                    Ok(Value::null())
                } else {
                    Ok(args[0].clone())
                }
            }
            "IFNULL" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery("IFNULL requires 2 arguments".to_string()));
                }
                if args[0].is_null() {
                    Ok(args[1].clone())
                } else {
                    Ok(args[0].clone())
                }
            }
            "IF" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery("IF requires 3 arguments".to_string()));
                }
                if let Some(true) = args[0].as_bool() {
                    Ok(args[1].clone())
                } else {
                    Ok(args[2].clone())
                }
            }
            "UPPER" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("UPPER requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.to_uppercase()))
            }
            "LOWER" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("LOWER requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.to_lowercase()))
            }
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("LENGTH requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::int64(s.chars().count() as i64))
            }
            "TRIM" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("TRIM requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.trim().to_string()))
            }
            "LTRIM" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("LTRIM requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.trim_start().to_string()))
            }
            "RTRIM" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("RTRIM requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.trim_end().to_string()))
            }
            "CONCAT" => {
                let mut result = String::new();
                for val in &args {
                    if !val.is_null() {
                        result.push_str(&val.to_string());
                    }
                }
                Ok(Value::string(result))
            }
            "SUBSTR" | "SUBSTRING" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery("SUBSTR requires 2 or 3 arguments".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let start = args[1].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[1].data_type().to_string(),
                })? as usize;
                let start = if start > 0 { start - 1 } else { 0 };
                let chars: Vec<char> = s.chars().collect();
                if start >= chars.len() {
                    return Ok(Value::string(String::new()));
                }
                let result = if args.len() == 3 {
                    let len = args[2].as_i64().ok_or_else(|| Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: args[2].data_type().to_string(),
                    })? as usize;
                    chars[start..].iter().take(len).collect()
                } else {
                    chars[start..].iter().collect()
                };
                Ok(Value::string(result))
            }
            "REPLACE" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery("REPLACE requires 3 arguments".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let from = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let to = args[2].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[2].data_type().to_string(),
                })?;
                Ok(Value::string(s.replace(from, to)))
            }
            "ABS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("ABS requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i.abs()));
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::float64(f.abs()));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "CEIL" | "CEILING" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("CEIL requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::float64(f.ceil()));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "FLOOR" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("FLOOR requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::float64(f.floor()));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "ROUND" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery("ROUND requires 1 or 2 arguments".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let decimals = if args.len() == 2 {
                    args[1].as_i64().unwrap_or(0)
                } else {
                    0
                };
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(f) = args[0].as_f64() {
                    let multiplier = 10f64.powi(decimals as i32);
                    return Ok(Value::float64((f * multiplier).round() / multiplier));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "MOD" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery("MOD requires 2 arguments".to_string()));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let a = args[0].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let b = args[1].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                if b == 0 {
                    return Err(Error::DivisionByZero);
                }
                Ok(Value::int64(a % b))
            }
            "GREATEST" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery("GREATEST requires at least 1 argument".to_string()));
                }
                let mut max: Option<Value> = None;
                for val in args {
                    if val.is_null() {
                        continue;
                    }
                    match &max {
                        None => max = Some(val),
                        Some(m) => {
                            if self.compare_for_ordering(&val, m) == std::cmp::Ordering::Greater {
                                max = Some(val);
                            }
                        }
                    }
                }
                Ok(max.unwrap_or_else(Value::null))
            }
            "LEAST" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery("LEAST requires at least 1 argument".to_string()));
                }
                let mut min: Option<Value> = None;
                for val in args {
                    if val.is_null() {
                        continue;
                    }
                    match &min {
                        None => min = Some(val),
                        Some(m) => {
                            if self.compare_for_ordering(&val, m) == std::cmp::Ordering::Less {
                                min = Some(val);
                            }
                        }
                    }
                }
                Ok(min.unwrap_or_else(Value::null))
            }
            "ARRAY_LENGTH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("ARRAY_LENGTH requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(arr) = args[0].as_array() {
                    return Ok(Value::int64(arr.len() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "ARRAY".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Function not yet supported: {}",
                name
            ))),
        }
    }

    fn extract_function_args(&self, func: &sqlparser::ast::Function, row: &Row) -> Result<Vec<Value>> {
        let mut args = Vec::new();
        if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
            for arg in &arg_list.args {
                if let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) = arg {
                    args.push(self.evaluate(expr, row)?);
                }
            }
        }
        Ok(args)
    }

    fn compare_for_ordering(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
            return ai.cmp(&bi);
        }
        if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
            return af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal);
        }
        if let (Some(as_), Some(bs)) = (a.as_str(), b.as_str()) {
            return as_.cmp(bs);
        }
        std::cmp::Ordering::Equal
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
