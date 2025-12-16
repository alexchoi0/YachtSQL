//! Expression evaluation for WHERE clauses, projections, etc.

#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_match)]
#![allow(clippy::ptr_arg)]

use chrono::{Datelike, Timelike};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_storage::{Record, Schema};

pub fn parse_byte_string_escapes(s: &str) -> Vec<u8> {
    let mut result = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some('x') | Some('X') => {
                    chars.next();
                    let mut hex = String::new();
                    for _ in 0..2 {
                        if let Some(&c) = chars.peek() {
                            if c.is_ascii_hexdigit() {
                                hex.push(c);
                                chars.next();
                            }
                        }
                    }
                    if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                        result.push(byte);
                    }
                }
                Some('n') => {
                    chars.next();
                    result.push(b'\n');
                }
                Some('t') => {
                    chars.next();
                    result.push(b'\t');
                }
                Some('r') => {
                    chars.next();
                    result.push(b'\r');
                }
                Some('\\') => {
                    chars.next();
                    result.push(b'\\');
                }
                Some('\'') => {
                    chars.next();
                    result.push(b'\'');
                }
                Some('"') => {
                    chars.next();
                    result.push(b'"');
                }
                _ => {
                    result.push(b'\\');
                }
            }
        } else {
            let mut buf = [0u8; 4];
            let encoded = c.encode_utf8(&mut buf);
            result.extend_from_slice(encoded.as_bytes());
        }
    }
    result
}

pub struct Evaluator<'a> {
    schema: &'a Schema,
}

impl<'a> Evaluator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }

    pub fn evaluate(&self, expr: &Expr, record: &Record) -> Result<Value> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_uppercase();
                let idx = self
                    .schema
                    .fields()
                    .iter()
                    .position(|f| f.name.to_uppercase() == name)
                    .ok_or_else(|| Error::ColumnNotFound(ident.value.clone()))?;
                Ok(record.values().get(idx).cloned().unwrap_or(Value::null()))
            }

            Expr::CompoundIdentifier(parts) => {
                let full_name = parts
                    .iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                let last_name = parts
                    .last()
                    .map(|i| i.value.to_uppercase())
                    .unwrap_or_default();
                let full_upper = full_name.to_uppercase();

                let idx = self
                    .schema
                    .fields()
                    .iter()
                    .position(|f| f.name.to_uppercase() == full_upper)
                    .or_else(|| {
                        self.schema
                            .fields()
                            .iter()
                            .position(|f| f.name.to_uppercase() == last_name)
                    })
                    .or_else(|| {
                        self.schema.fields().iter().position(|f| {
                            f.name.to_uppercase().ends_with(&format!(".{}", last_name))
                        })
                    })
                    .ok_or_else(|| Error::ColumnNotFound(full_name.clone()))?;
                Ok(record.values().get(idx).cloned().unwrap_or(Value::null()))
            }

            Expr::Value(val) => self.evaluate_literal(&val.value),

            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate(left, record)?;
                let right_val = self.evaluate(right, record)?;
                self.evaluate_binary_op(&left_val, op, &right_val)
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate(expr, record)?;
                self.evaluate_unary_op(op, &val)
            }

            Expr::IsNull(inner) => {
                let val = self.evaluate(inner, record)?;
                Ok(Value::bool_val(val.is_null()))
            }

            Expr::IsNotNull(inner) => {
                let val = self.evaluate(inner, record)?;
                Ok(Value::bool_val(!val.is_null()))
            }

            Expr::Nested(inner) => self.evaluate(inner, record),

            Expr::Function(func) => self.evaluate_function(func, record),

            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => self.evaluate_case(
                operand.as_deref(),
                conditions,
                else_result.as_deref(),
                record,
            ),

            Expr::Array(arr) => self.evaluate_array(arr, record),

            Expr::InList {
                expr,
                list,
                negated,
            } => self.evaluate_in_list(expr, list, *negated, record),

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => self.evaluate_between(expr, low, high, *negated, record),

            Expr::Like {
                expr,
                pattern,
                negated,
                ..
            } => self.evaluate_like(expr, pattern, *negated, record),

            Expr::ILike {
                expr,
                pattern,
                negated,
                ..
            } => self.evaluate_ilike(expr, pattern, *negated, record),

            Expr::Cast {
                expr, data_type, ..
            } => self.evaluate_cast(expr, data_type, record),

            Expr::Tuple(exprs) => {
                let mut values = Vec::with_capacity(exprs.len());
                for e in exprs {
                    values.push(self.evaluate(e, record)?);
                }
                Ok(Value::array(values))
            }

            Expr::TypedString(ts) => self.evaluate_typed_string(&ts.data_type, &ts.value),

            Expr::CompoundFieldAccess { root, access_chain } => {
                self.evaluate_compound_field_access(root, access_chain, record)
            }

            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Err(Error::UnsupportedFeature(
                "IN subquery not yet supported in evaluator".to_string(),
            )),

            Expr::Subquery(_) => Err(Error::UnsupportedFeature(
                "Subquery not yet supported in evaluator".to_string(),
            )),

            Expr::Struct { values, fields } => self.evaluate_struct_expr(values, record),

            Expr::Named { expr, name } => self.evaluate(expr, record),

            Expr::Trim {
                expr: inner,
                trim_where,
                trim_what,
                trim_characters,
            } => {
                let val = self.evaluate(inner, record)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let s = val.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: val.data_type().to_string(),
                })?;

                let chars_to_trim: Option<Vec<char>> = if let Some(chars_expr) = trim_characters {
                    if !chars_expr.is_empty() {
                        let first = self.evaluate(&chars_expr[0], record)?;
                        first.as_str().map(|s| s.chars().collect())
                    } else {
                        None
                    }
                } else if let Some(what_expr) = trim_what {
                    let what_val = self.evaluate(what_expr, record)?;
                    what_val.as_str().map(|s| s.chars().collect())
                } else {
                    None
                };

                let result = match (trim_where, &chars_to_trim) {
                    (Some(sqlparser::ast::TrimWhereField::Leading), Some(chars)) => {
                        s.trim_start_matches(|c| chars.contains(&c)).to_string()
                    }
                    (Some(sqlparser::ast::TrimWhereField::Trailing), Some(chars)) => {
                        s.trim_end_matches(|c| chars.contains(&c)).to_string()
                    }
                    (Some(sqlparser::ast::TrimWhereField::Both), Some(chars)) => {
                        s.trim_matches(|c| chars.contains(&c)).to_string()
                    }
                    (Some(sqlparser::ast::TrimWhereField::Leading), None) => {
                        s.trim_start().to_string()
                    }
                    (Some(sqlparser::ast::TrimWhereField::Trailing), None) => {
                        s.trim_end().to_string()
                    }
                    (Some(sqlparser::ast::TrimWhereField::Both), None) | (None, None) => {
                        s.trim().to_string()
                    }
                    (None, Some(chars)) => s.trim_matches(|c| chars.contains(&c)).to_string(),
                };
                Ok(Value::string(result))
            }

            Expr::Substring {
                expr: inner,
                substring_from,
                substring_for,
                special: _,
                shorthand: _,
            } => {
                let val = self.evaluate(inner, record)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let s = val.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: val.data_type().to_string(),
                })?;

                let start = if let Some(from_expr) = substring_from {
                    let from_val = self.evaluate(from_expr, record)?;
                    from_val.as_i64().unwrap_or(1) as usize
                } else {
                    1
                };
                let start_idx = if start > 0 { start - 1 } else { 0 };

                let chars: Vec<char> = s.chars().collect();
                if start_idx >= chars.len() {
                    return Ok(Value::string(String::new()));
                }

                let result: String = if let Some(for_expr) = substring_for {
                    let len_val = self.evaluate(for_expr, record)?;
                    let len = len_val.as_i64().unwrap_or(0) as usize;
                    chars[start_idx..].iter().take(len).collect()
                } else {
                    chars[start_idx..].iter().collect()
                };
                Ok(Value::string(result))
            }

            Expr::IsDistinctFrom(left, right) => {
                let l = self.evaluate(left, record)?;
                let r = self.evaluate(right, record)?;
                let distinct = match (l.is_null(), r.is_null()) {
                    (true, true) => false,
                    (true, false) | (false, true) => true,
                    (false, false) => l != r,
                };
                Ok(Value::bool_val(distinct))
            }

            Expr::IsNotDistinctFrom(left, right) => {
                let l = self.evaluate(left, record)?;
                let r = self.evaluate(right, record)?;
                let not_distinct = match (l.is_null(), r.is_null()) {
                    (true, true) => true,
                    (true, false) | (false, true) => false,
                    (false, false) => l == r,
                };
                Ok(Value::bool_val(not_distinct))
            }

            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                let s = self.evaluate(expr, record)?;
                let what = self.evaluate(overlay_what, record)?;
                let from = self.evaluate(overlay_from, record)?;

                if s.is_null() {
                    return Ok(Value::null());
                }

                let s_str = s.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: s.data_type().to_string(),
                })?;
                let what_str = what.as_str().unwrap_or("");
                let from_idx = from.as_i64().unwrap_or(1) as usize;
                let start = if from_idx > 0 { from_idx - 1 } else { 0 };

                let for_len = if let Some(for_expr) = overlay_for {
                    let for_val = self.evaluate(for_expr, record)?;
                    for_val.as_i64().unwrap_or(what_str.len() as i64) as usize
                } else {
                    what_str.len()
                };

                let chars: Vec<char> = s_str.chars().collect();
                let end = (start + for_len).min(chars.len());

                let mut result = String::new();
                result.extend(&chars[..start.min(chars.len())]);
                result.push_str(what_str);
                if end < chars.len() {
                    result.extend(&chars[end..]);
                }
                Ok(Value::string(result))
            }

            Expr::Position { expr, r#in } => {
                let needle = self.evaluate(expr, record)?;
                let haystack = self.evaluate(r#in, record)?;

                if needle.is_null() || haystack.is_null() {
                    return Ok(Value::null());
                }

                let needle_str = needle.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: needle.data_type().to_string(),
                })?;
                let haystack_str = haystack.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: haystack.data_type().to_string(),
                })?;

                let pos = haystack_str
                    .find(needle_str)
                    .map(|i| haystack_str[..i].chars().count() as i64 + 1)
                    .unwrap_or(0);
                Ok(Value::int64(pos))
            }

            Expr::Interval(interval) => {
                let num = self.evaluate(&interval.value, record)?;
                let amount = num.as_i64().unwrap_or(0);
                let unit = interval
                    .leading_field
                    .as_ref()
                    .map(|f| format!("{:?}", f).to_uppercase())
                    .unwrap_or_else(|| "SECOND".to_string());
                let interval_val = match unit.as_str() {
                    "YEAR" => yachtsql_common::types::Interval::from_months(amount as i32 * 12),
                    "MONTH" => yachtsql_common::types::Interval::from_months(amount as i32),
                    "DAY" => yachtsql_common::types::Interval::from_days(amount as i32),
                    "HOUR" => yachtsql_common::types::Interval::from_hours(amount),
                    "MINUTE" => yachtsql_common::types::Interval::new(
                        0,
                        0,
                        amount * yachtsql_common::types::Interval::MICROS_PER_MINUTE,
                    ),
                    "SECOND" => yachtsql_common::types::Interval::new(
                        0,
                        0,
                        amount * yachtsql_common::types::Interval::MICROS_PER_SECOND,
                    ),
                    _ => yachtsql_common::types::Interval::new(0, amount as i32, 0),
                };
                Ok(Value::interval(interval_val))
            }

            Expr::Extract { field, expr, .. } => {
                let val = self.evaluate(expr, record)?;
                if val.is_null() {
                    return Ok(Value::null());
                }

                let field_str = format!("{}", field);
                if let Some(date) = val.as_date() {
                    let result = match field_str.to_uppercase().as_str() {
                        "YEAR" => date.year() as i64,
                        "MONTH" => date.month() as i64,
                        "DAY" => date.day() as i64,
                        "DAYOFWEEK" => date.weekday().num_days_from_sunday() as i64 + 1,
                        "DAYOFYEAR" => date.ordinal() as i64,
                        "WEEK" => date.iso_week().week() as i64,
                        "QUARTER" => ((date.month() - 1) / 3 + 1) as i64,
                        _ => {
                            return Err(Error::UnsupportedFeature(format!(
                                "EXTRACT {} not supported for DATE",
                                field_str
                            )));
                        }
                    };
                    return Ok(Value::int64(result));
                }

                if let Some(ts) = val.as_timestamp() {
                    let result = match field_str.to_uppercase().as_str() {
                        "YEAR" => ts.year() as i64,
                        "MONTH" => ts.month() as i64,
                        "DAY" => ts.day() as i64,
                        "HOUR" => ts.hour() as i64,
                        "MINUTE" => ts.minute() as i64,
                        "SECOND" => ts.second() as i64,
                        "DAYOFWEEK" => ts.weekday().num_days_from_sunday() as i64 + 1,
                        "DAYOFYEAR" => ts.ordinal() as i64,
                        "WEEK" => ts.iso_week().week() as i64,
                        "QUARTER" => ((ts.month() - 1) / 3 + 1) as i64,
                        _ => {
                            return Err(Error::UnsupportedFeature(format!(
                                "EXTRACT {} not supported for TIMESTAMP",
                                field_str
                            )));
                        }
                    };
                    return Ok(Value::int64(result));
                }

                if let Some(time) = val.as_time() {
                    let result = match field_str.to_uppercase().as_str() {
                        "HOUR" => time.hour() as i64,
                        "MINUTE" => time.minute() as i64,
                        "SECOND" => time.second() as i64,
                        "MILLISECOND" => (time.nanosecond() / 1_000_000) as i64,
                        "MICROSECOND" => (time.nanosecond() / 1_000) as i64,
                        _ => {
                            return Err(Error::UnsupportedFeature(format!(
                                "EXTRACT {} not supported for TIME",
                                field_str
                            )));
                        }
                    };
                    return Ok(Value::int64(result));
                }

                Err(Error::TypeMismatch {
                    expected: "DATE, TIME, or TIMESTAMP".to_string(),
                    actual: val.data_type().to_string(),
                })
            }

            _ => Err(Error::UnsupportedFeature(format!(
                "Expression type not yet supported: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_struct_expr(&self, values: &[Expr], record: &Record) -> Result<Value> {
        let mut struct_fields = indexmap::IndexMap::new();
        for (i, expr) in values.iter().enumerate() {
            match expr {
                Expr::Named {
                    expr: inner_expr,
                    name,
                } => {
                    let val = self.evaluate(inner_expr, record)?;
                    struct_fields.insert(name.value.clone(), val);
                }
                _ => {
                    let val = self.evaluate(expr, record)?;
                    struct_fields.insert(format!("_field{}", i), val);
                }
            }
        }
        Ok(Value::struct_val(struct_fields.into_iter().collect()))
    }

    fn evaluate_typed_string(
        &self,
        data_type: &sqlparser::ast::DataType,
        value: &sqlparser::ast::ValueWithSpan,
    ) -> Result<Value> {
        let s = match &value.value {
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => s.as_str(),
            _ => {
                return Err(Error::InvalidQuery(
                    "TypedString value must be a string".to_string(),
                ));
            }
        };
        match data_type {
            sqlparser::ast::DataType::Date => {
                if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    Ok(Value::date(date))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            sqlparser::ast::DataType::Time(_, _) => {
                if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                    Ok(Value::time(time))
                } else if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
                    Ok(Value::time(time))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            sqlparser::ast::DataType::Timestamp(_, _) | sqlparser::ast::DataType::Datetime(_) => {
                if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(s) {
                    Ok(Value::timestamp(ts.with_timezone(&chrono::Utc)))
                } else if let Ok(ndt) =
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                {
                    Ok(Value::timestamp(
                        chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                    ))
                } else if let Ok(ndt) =
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                {
                    Ok(Value::timestamp(
                        chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                    ))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            sqlparser::ast::DataType::JSON => {
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(s) {
                    Ok(Value::json(json_val))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            sqlparser::ast::DataType::Bytes(_) => Ok(Value::bytes(s.as_bytes().to_vec())),
            _ => Ok(Value::string(s.to_string())),
        }
    }

    fn evaluate_compound_field_access(
        &self,
        root: &Expr,
        access_chain: &[sqlparser::ast::AccessExpr],
        record: &Record,
    ) -> Result<Value> {
        let mut current = self.evaluate(root, record)?;

        for access in access_chain {
            match access {
                sqlparser::ast::AccessExpr::Subscript(subscript) => {
                    current = self.apply_subscript(current, subscript, record)?;
                }
                sqlparser::ast::AccessExpr::Dot(field_expr) => match field_expr {
                    Expr::Identifier(ident) => {
                        if let Some(struct_val) = current.as_struct() {
                            let field_name = ident.value.to_uppercase();
                            current = struct_val
                                .iter()
                                .find(|(name, _)| name.to_uppercase() == field_name)
                                .map(|(_, v)| v.clone())
                                .unwrap_or_else(Value::null);
                        } else {
                            return Ok(Value::null());
                        }
                    }
                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "Non-identifier field access".to_string(),
                        ));
                    }
                },
            }
        }

        Ok(current)
    }

    fn apply_subscript(
        &self,
        base_val: Value,
        subscript: &sqlparser::ast::Subscript,
        record: &Record,
    ) -> Result<Value> {
        match subscript {
            sqlparser::ast::Subscript::Index { index } => {
                let idx_val = self.evaluate(index, record)?;
                let idx = idx_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: idx_val.data_type().to_string(),
                })?;

                if let Some(arr) = base_val.as_array() {
                    let idx_usize = if idx > 0 {
                        (idx - 1) as usize
                    } else if idx < 0 {
                        let len = arr.len() as i64;
                        if -idx > len {
                            return Ok(Value::null());
                        }
                        (len + idx) as usize
                    } else {
                        return Ok(Value::null());
                    };

                    if idx_usize < arr.len() {
                        Ok(arr[idx_usize].clone())
                    } else {
                        Ok(Value::null())
                    }
                } else if let Some(s) = base_val.as_str() {
                    let chars: Vec<char> = s.chars().collect();
                    let idx_usize = if idx > 0 {
                        (idx - 1) as usize
                    } else {
                        return Ok(Value::null());
                    };

                    if idx_usize < chars.len() {
                        Ok(Value::string(chars[idx_usize].to_string()))
                    } else {
                        Ok(Value::null())
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "ARRAY or STRING".to_string(),
                        actual: base_val.data_type().to_string(),
                    })
                }
            }
            sqlparser::ast::Subscript::Slice { .. } => Err(Error::UnsupportedFeature(
                "Array slice not yet supported".to_string(),
            )),
        }
    }

    fn evaluate_case(
        &self,
        operand: Option<&Expr>,
        conditions: &[sqlparser::ast::CaseWhen],
        else_result: Option<&Expr>,
        record: &Record,
    ) -> Result<Value> {
        match operand {
            Some(op_expr) => {
                let op_val = self.evaluate(op_expr, record)?;
                for cond in conditions {
                    let when_val = self.evaluate(&cond.condition, record)?;
                    if op_val == when_val {
                        return self.evaluate(&cond.result, record);
                    }
                }
            }
            None => {
                for cond in conditions {
                    let cond_val = self.evaluate(&cond.condition, record)?;
                    if let Some(true) = cond_val.as_bool() {
                        return self.evaluate(&cond.result, record);
                    }
                }
            }
        }
        match else_result {
            Some(else_expr) => self.evaluate(else_expr, record),
            None => Ok(Value::null()),
        }
    }

    fn evaluate_array(&self, arr: &sqlparser::ast::Array, record: &Record) -> Result<Value> {
        let mut values = Vec::with_capacity(arr.elem.len());
        for elem in &arr.elem {
            values.push(self.evaluate(elem, record)?);
        }
        Ok(Value::array(values))
    }

    fn evaluate_in_list(
        &self,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let mut found = false;
        let mut has_null = false;
        for item in list {
            let item_val = self.evaluate(item, record)?;
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

    fn evaluate_between(
        &self,
        expr: &Expr,
        low: &Expr,
        high: &Expr,
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let low_val = self.evaluate(low, record)?;
        let high_val = self.evaluate(high, record)?;

        if val.is_null() || low_val.is_null() || high_val.is_null() {
            return Ok(Value::null());
        }

        let ge_low = self.compare_values(&val, &low_val, |ord| ord.is_ge())?;
        let le_high = self.compare_values(&val, &high_val, |ord| ord.is_le())?;

        let in_range = ge_low.as_bool().unwrap_or(false) && le_high.as_bool().unwrap_or(false);
        Ok(Value::bool_val(if negated { !in_range } else { in_range }))
    }

    fn evaluate_like(
        &self,
        expr: &Expr,
        pattern: &Expr,
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let pat = self.evaluate(pattern, record)?;

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

    fn evaluate_ilike(
        &self,
        expr: &Expr,
        pattern: &Expr,
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let pat = self.evaluate(pattern, record)?;

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

        let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
        let regex_pattern = format!("^{}$", regex_pattern);

        regex::Regex::new(&regex_pattern)
            .map(|re| re.is_match(&text))
            .unwrap_or(false)
    }

    fn evaluate_cast(
        &self,
        expr: &Expr,
        target_type: &sqlparser::ast::DataType,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(Value::null());
        }

        match target_type {
            sqlparser::ast::DataType::Int64
            | sqlparser::ast::DataType::BigInt(_)
            | sqlparser::ast::DataType::Integer(_) => {
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
            sqlparser::ast::DataType::String(_)
            | sqlparser::ast::DataType::Varchar(_)
            | sqlparser::ast::DataType::Text => Ok(Value::string(val.to_string())),
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
            sqlparser::ast::DataType::Date => {
                if let Some(s) = val.as_str() {
                    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        return Ok(Value::date(date));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Timestamp(_, _) | sqlparser::ast::DataType::Datetime(_) => {
                if let Some(s) = val.as_str() {
                    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(s) {
                        return Ok(Value::timestamp(ts.with_timezone(&chrono::Utc)));
                    }
                    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                        return Ok(Value::timestamp(
                            chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                        ));
                    }
                    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                        return Ok(Value::timestamp(
                            chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                        ));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Time(_, _) => {
                if let Some(s) = val.as_str() {
                    if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                        return Ok(Value::time(time));
                    }
                    if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
                        return Ok(Value::time(time));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "TIME".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Numeric(_) | sqlparser::ast::DataType::Decimal(_) => {
                if let Some(i) = val.as_i64() {
                    return Ok(Value::numeric(rust_decimal::Decimal::from(i)));
                }
                if let Some(f) = val.as_f64() {
                    if let Some(dec) = rust_decimal::Decimal::from_f64_retain(f) {
                        return Ok(Value::numeric(dec));
                    }
                    return Ok(Value::float64(f));
                }
                if let Some(s) = val.as_str() {
                    if let Ok(dec) = rust_decimal::Decimal::from_str_exact(s) {
                        return Ok(Value::numeric(dec));
                    }
                    if let Ok(f) = s.parse::<f64>() {
                        return Ok(Value::float64(f));
                    }
                }
                Err(Error::TypeMismatch {
                    expected: "NUMERIC".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Bytes(_) => {
                if let Some(b) = val.as_bytes() {
                    return Ok(Value::bytes(b.to_vec()));
                }
                if let Some(s) = val.as_str() {
                    return Ok(Value::bytes(s.as_bytes().to_vec()));
                }
                Err(Error::TypeMismatch {
                    expected: "BYTES".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Array(elem_def) => {
                if let Some(arr) = val.as_array() {
                    return Ok(Value::array(arr.to_vec()));
                }
                Err(Error::TypeMismatch {
                    expected: "ARRAY".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::JSON => {
                let json_str = val.to_string();
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&json_str) {
                    return Ok(Value::json(json_val));
                }
                Ok(Value::json(serde_json::Value::String(json_str)))
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
            SqlValue::SingleQuotedByteStringLiteral(s)
            | SqlValue::DoubleQuotedByteStringLiteral(s) => {
                Ok(Value::bytes(parse_byte_string_escapes(s)))
            }
            SqlValue::Boolean(b) => Ok(Value::bool_val(*b)),
            SqlValue::Null => Ok(Value::null()),
            SqlValue::HexStringLiteral(s) => {
                let bytes = hex::decode(s).unwrap_or_default();
                Ok(Value::bytes(bytes))
            }
            SqlValue::SingleQuotedRawStringLiteral(s)
            | SqlValue::DoubleQuotedRawStringLiteral(s) => Ok(Value::string(s.clone())),
            SqlValue::TripleSingleQuotedString(s) | SqlValue::TripleDoubleQuotedString(s) => {
                Ok(Value::string(s.clone()))
            }
            SqlValue::TripleSingleQuotedRawStringLiteral(s)
            | SqlValue::TripleDoubleQuotedRawStringLiteral(s) => Ok(Value::string(s.clone())),
            SqlValue::DollarQuotedString(dqs) => Ok(Value::string(dqs.value.clone())),
            _ => Err(Error::UnsupportedFeature(format!(
                "Literal type not yet supported: {:?}",
                val
            ))),
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<Value> {
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
            return Ok(Value::bool_val(pred(
                l.partial_cmp(&r).unwrap_or(std::cmp::Ordering::Equal),
            )));
        }
        if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
            return Ok(Value::bool_val(pred(l.cmp(r))));
        }
        if let Some(l) = left.as_i64() {
            if let Some(r) = right.as_f64() {
                return Ok(Value::bool_val(pred(
                    (l as f64)
                        .partial_cmp(&r)
                        .unwrap_or(std::cmp::Ordering::Equal),
                )));
            }
        }
        if let Some(l) = left.as_f64() {
            if let Some(r) = right.as_i64() {
                return Ok(Value::bool_val(pred(
                    l.partial_cmp(&(r as f64))
                        .unwrap_or(std::cmp::Ordering::Equal),
                )));
            }
        }
        if let (Some(l), Some(r)) = (left.as_date(), right.as_date()) {
            return Ok(Value::bool_val(pred(l.cmp(&r))));
        }
        if let (Some(l), Some(r)) = (left.as_timestamp(), right.as_timestamp()) {
            return Ok(Value::bool_val(pred(l.cmp(&r))));
        }
        if let (Some(l), Some(r)) = (left.as_time(), right.as_time()) {
            return Ok(Value::bool_val(pred(l.cmp(&r))));
        }
        if let (Some(l), Some(r)) = (left.as_numeric(), right.as_numeric()) {
            return Ok(Value::bool_val(pred(l.cmp(&r))));
        }
        if let (Some(l), Some(r)) = (left.as_bytes(), right.as_bytes()) {
            return Ok(Value::bool_val(pred(l.cmp(r))));
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

    fn evaluate_function(&self, func: &sqlparser::ast::Function, record: &Record) -> Result<Value> {
        let name = func.name.to_string().to_uppercase();
        let args = self.extract_function_args(func, record)?;

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
                    return Err(Error::InvalidQuery(
                        "NULLIF requires 2 arguments".to_string(),
                    ));
                }
                if args[0] == args[1] {
                    Ok(Value::null())
                } else {
                    Ok(args[0].clone())
                }
            }
            "IFNULL" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "IFNULL requires 2 arguments".to_string(),
                    ));
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
                    return Err(Error::InvalidQuery(
                        "LENGTH requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(bytes) = args[0].as_bytes() {
                    return Ok(Value::int64(bytes.len() as i64));
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::int64(s.chars().count() as i64))
            }
            "BYTE_LENGTH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "BYTE_LENGTH requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(bytes) = args[0].as_bytes() {
                    return Ok(Value::int64(bytes.len() as i64));
                }
                if let Some(s) = args[0].as_str() {
                    return Ok(Value::int64(s.len() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })
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
                let all_bytes = args.iter().all(|v| v.is_null() || v.as_bytes().is_some());
                if all_bytes && args.iter().any(|v| v.as_bytes().is_some()) {
                    let mut result = Vec::new();
                    for val in &args {
                        if let Some(b) = val.as_bytes() {
                            result.extend(b);
                        }
                    }
                    Ok(Value::bytes(result))
                } else {
                    let mut result = String::new();
                    for val in &args {
                        if !val.is_null() {
                            result.push_str(&val.to_string());
                        }
                    }
                    Ok(Value::string(result))
                }
            }
            "SUBSTR" | "SUBSTRING" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "SUBSTR requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let start = args[1].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let start_idx = if start > 0 { (start - 1) as usize } else { 0 };

                if let Some(bytes) = args[0].as_bytes() {
                    if start_idx >= bytes.len() {
                        return Ok(Value::bytes(Vec::new()));
                    }
                    let result = if args.len() == 3 {
                        let len = args[2].as_i64().ok_or_else(|| Error::TypeMismatch {
                            expected: "INT64".to_string(),
                            actual: args[2].data_type().to_string(),
                        })? as usize;
                        bytes[start_idx..].iter().take(len).cloned().collect()
                    } else {
                        bytes[start_idx..].to_vec()
                    };
                    return Ok(Value::bytes(result));
                }

                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let chars: Vec<char> = s.chars().collect();
                if start_idx >= chars.len() {
                    return Ok(Value::string(String::new()));
                }
                let result: String = if args.len() == 3 {
                    let len = args[2].as_i64().ok_or_else(|| Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: args[2].data_type().to_string(),
                    })? as usize;
                    chars[start_idx..].iter().take(len).collect()
                } else {
                    chars[start_idx..].iter().collect()
                };
                Ok(Value::string(result))
            }
            "REPLACE" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "REPLACE requires 3 arguments".to_string(),
                    ));
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
                    return Err(Error::InvalidQuery(
                        "ROUND requires 1 or 2 arguments".to_string(),
                    ));
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
                    return Err(Error::InvalidQuery(
                        "GREATEST requires at least 1 argument".to_string(),
                    ));
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
                    return Err(Error::InvalidQuery(
                        "LEAST requires at least 1 argument".to_string(),
                    ));
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
                    return Err(Error::InvalidQuery(
                        "ARRAY_LENGTH requires 1 argument".to_string(),
                    ));
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
            "SAFE_DIVIDE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "SAFE_DIVIDE requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let divisor = args[1]
                    .as_f64()
                    .or_else(|| args[1].as_i64().map(|i| i as f64));
                if divisor == Some(0.0) {
                    return Ok(Value::null());
                }
                let dividend = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64));
                match (dividend, divisor) {
                    (Some(a), Some(b)) => Ok(Value::float64(a / b)),
                    _ => Ok(Value::null()),
                }
            }
            "SAFE_ADD" | "SAFE_SUBTRACT" | "SAFE_MULTIPLY" | "SAFE_NEGATE" => match name.as_str() {
                "SAFE_ADD" => {
                    if args.len() != 2 || args[0].is_null() || args[1].is_null() {
                        return Ok(Value::null());
                    }
                    match (args[0].as_i64(), args[1].as_i64()) {
                        (Some(a), Some(b)) => Ok(Value::int64(a.wrapping_add(b))),
                        _ => match (args[0].as_f64(), args[1].as_f64()) {
                            (Some(a), Some(b)) => Ok(Value::float64(a + b)),
                            _ => Ok(Value::null()),
                        },
                    }
                }
                "SAFE_SUBTRACT" => {
                    if args.len() != 2 || args[0].is_null() || args[1].is_null() {
                        return Ok(Value::null());
                    }
                    match (args[0].as_i64(), args[1].as_i64()) {
                        (Some(a), Some(b)) => Ok(Value::int64(a.wrapping_sub(b))),
                        _ => match (args[0].as_f64(), args[1].as_f64()) {
                            (Some(a), Some(b)) => Ok(Value::float64(a - b)),
                            _ => Ok(Value::null()),
                        },
                    }
                }
                "SAFE_MULTIPLY" => {
                    if args.len() != 2 || args[0].is_null() || args[1].is_null() {
                        return Ok(Value::null());
                    }
                    match (args[0].as_i64(), args[1].as_i64()) {
                        (Some(a), Some(b)) => Ok(Value::int64(a.wrapping_mul(b))),
                        _ => match (args[0].as_f64(), args[1].as_f64()) {
                            (Some(a), Some(b)) => Ok(Value::float64(a * b)),
                            _ => Ok(Value::null()),
                        },
                    }
                }
                "SAFE_NEGATE" => {
                    if args.len() != 1 || args[0].is_null() {
                        return Ok(Value::null());
                    }
                    if let Some(i) = args[0].as_i64() {
                        return Ok(Value::int64(i.wrapping_neg()));
                    }
                    if let Some(f) = args[0].as_f64() {
                        return Ok(Value::float64(-f));
                    }
                    Ok(Value::null())
                }
                _ => Ok(Value::null()),
            },
            "MD5" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("MD5 requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let input = if let Some(s) = args[0].as_str() {
                    s.as_bytes().to_vec()
                } else if let Some(b) = args[0].as_bytes() {
                    b.to_vec()
                } else {
                    return Ok(Value::null());
                };
                let digest = md5::compute(&input);
                Ok(Value::bytes(digest.to_vec()))
            }
            "INT64" | "INT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("INT64 requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::int64(f as i64));
                }
                if let Some(s) = args[0].as_str() {
                    if let Ok(i) = s.parse::<i64>() {
                        return Ok(Value::int64(i));
                    }
                }
                if let Some(b) = args[0].as_bool() {
                    return Ok(Value::int64(if b { 1 } else { 0 }));
                }
                Ok(Value::null())
            }
            "FLOAT64" | "FLOAT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "FLOAT64 requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::float64(f));
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::float64(i as f64));
                }
                if let Some(s) = args[0].as_str() {
                    if let Ok(f) = s.parse::<f64>() {
                        return Ok(Value::float64(f));
                    }
                }
                Ok(Value::null())
            }
            "STRING" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "STRING requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                Ok(Value::string(args[0].to_string()))
            }
            "BOOL" | "BOOLEAN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("BOOL requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(b) = args[0].as_bool() {
                    return Ok(Value::bool_val(b));
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::bool_val(i != 0));
                }
                if let Some(s) = args[0].as_str() {
                    let lower = s.to_lowercase();
                    if lower == "true" || lower == "1" {
                        return Ok(Value::bool_val(true));
                    }
                    if lower == "false" || lower == "0" {
                        return Ok(Value::bool_val(false));
                    }
                }
                Ok(Value::null())
            }
            "SAFE_CONVERT_BYTES_TO_STRING" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "SAFE_CONVERT_BYTES_TO_STRING requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(b) = args[0].as_bytes() {
                    match String::from_utf8(b.to_vec()) {
                        Ok(s) => return Ok(Value::string(s)),
                        Err(_) => return Ok(Value::null()),
                    }
                }
                Ok(Value::null())
            }
            "FROM_BASE64" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "FROM_BASE64 requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                use base64::Engine;
                match base64::engine::general_purpose::STANDARD.decode(s) {
                    Ok(bytes) => Ok(Value::bytes(bytes)),
                    Err(_) => Ok(Value::null()),
                }
            }
            "TO_BASE64" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TO_BASE64 requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let bytes = args[0].as_bytes().ok_or_else(|| Error::TypeMismatch {
                    expected: "BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                use base64::Engine;
                let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
                Ok(Value::string(encoded))
            }
            "FROM_HEX" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "FROM_HEX requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                match hex::decode(s) {
                    Ok(bytes) => Ok(Value::bytes(bytes)),
                    Err(_) => Ok(Value::null()),
                }
            }
            "TO_HEX" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TO_HEX requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let bytes = args[0].as_bytes().ok_or_else(|| Error::TypeMismatch {
                    expected: "BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(hex::encode(bytes)))
            }
            "REGEXP_CONTAINS" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_CONTAINS requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let text = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let pattern = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                match regex::Regex::new(pattern) {
                    Ok(re) => Ok(Value::bool_val(re.is_match(text))),
                    Err(_) => Ok(Value::null()),
                }
            }
            "REGEXP_EXTRACT" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_EXTRACT requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let text = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let pattern = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                match regex::Regex::new(pattern) {
                    Ok(re) => {
                        if let Some(caps) = re.captures(text) {
                            let group_idx = if args.len() == 3 {
                                args[2].as_i64().unwrap_or(0) as usize
                            } else if re.captures_len() > 1 {
                                1
                            } else {
                                0
                            };
                            if let Some(m) = caps.get(group_idx) {
                                return Ok(Value::string(m.as_str().to_string()));
                            }
                        }
                        Ok(Value::null())
                    }
                    Err(_) => Ok(Value::null()),
                }
            }
            "JSON_EXTRACT_SCALAR" | "JSON_VALUE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "JSON_EXTRACT_SCALAR requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let json_str = args[0].as_str().unwrap_or_default();
                let path = args[1].as_str().unwrap_or_default();
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(json_str) {
                    let result = self.json_path_extract(&json_val, path);
                    return Ok(result
                        .map(|v| match v {
                            serde_json::Value::String(s) => Value::string(s),
                            serde_json::Value::Number(n) => {
                                if let Some(i) = n.as_i64() {
                                    Value::string(i.to_string())
                                } else if let Some(f) = n.as_f64() {
                                    Value::string(f.to_string())
                                } else {
                                    Value::null()
                                }
                            }
                            serde_json::Value::Bool(b) => Value::string(b.to_string()),
                            serde_json::Value::Null => Value::null(),
                            _ => Value::null(),
                        })
                        .unwrap_or_else(Value::null));
                }
                Ok(Value::null())
            }
            "JSON_QUERY" | "JSON_EXTRACT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "JSON_QUERY requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let json_str = args[0].as_str().unwrap_or_default();
                let path = args[1].as_str().unwrap_or_default();
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(json_str) {
                    let result = self.json_path_extract(&json_val, path);
                    return Ok(result
                        .map(|v| Value::string(v.to_string()))
                        .unwrap_or_else(Value::null));
                }
                Ok(Value::null())
            }
            "JSON_TYPE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "JSON_TYPE requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_str = args[0].as_str().unwrap_or_default();
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(json_str) {
                    let type_name = match json_val {
                        serde_json::Value::Null => "null",
                        serde_json::Value::Bool(_) => "boolean",
                        serde_json::Value::Number(_) => "number",
                        serde_json::Value::String(_) => "string",
                        serde_json::Value::Array(_) => "array",
                        serde_json::Value::Object(_) => "object",
                    };
                    return Ok(Value::string(type_name.to_string()));
                }
                Ok(Value::null())
            }
            "BIT_COUNT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "BIT_COUNT requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i.count_ones() as i64));
                }
                if let Some(b) = args[0].as_bytes() {
                    let count: u32 = b.iter().map(|byte| byte.count_ones()).sum();
                    return Ok(Value::int64(count as i64));
                }
                Ok(Value::null())
            }
            "LEFT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery("LEFT requires 2 arguments".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[1].as_i64().unwrap_or(0) as usize;
                if let Some(bytes) = args[0].as_bytes() {
                    let result: Vec<u8> = bytes.iter().take(n).cloned().collect();
                    return Ok(Value::bytes(result));
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let result: String = s.chars().take(n).collect();
                Ok(Value::string(result))
            }
            "RIGHT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "RIGHT requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[1].as_i64().unwrap_or(0) as usize;
                if let Some(bytes) = args[0].as_bytes() {
                    let start = bytes.len().saturating_sub(n);
                    let result: Vec<u8> = bytes[start..].to_vec();
                    return Ok(Value::bytes(result));
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let chars: Vec<char> = s.chars().collect();
                let start = chars.len().saturating_sub(n);
                let result: String = chars[start..].iter().collect();
                Ok(Value::string(result))
            }
            "REVERSE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "REVERSE requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(bytes) = args[0].as_bytes() {
                    let result: Vec<u8> = bytes.iter().rev().cloned().collect();
                    return Ok(Value::bytes(result));
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.chars().rev().collect::<String>()))
            }
            "REPEAT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "REPEAT requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let n = args[1].as_i64().unwrap_or(0) as usize;
                Ok(Value::string(s.repeat(n)))
            }
            "STARTS_WITH" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "STARTS_WITH requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let prefix = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                Ok(Value::bool_val(s.starts_with(prefix)))
            }
            "ENDS_WITH" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "ENDS_WITH requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let suffix = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                Ok(Value::bool_val(s.ends_with(suffix)))
            }
            "CONTAINS_SUBSTR" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "CONTAINS_SUBSTR requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let substr = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                Ok(Value::bool_val(
                    s.to_lowercase().contains(&substr.to_lowercase()),
                ))
            }
            "INSTR" | "STRPOS" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "INSTR requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let substr = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                match s.find(substr) {
                    Some(idx) => Ok(Value::int64((idx + 1) as i64)),
                    None => Ok(Value::int64(0)),
                }
            }
            "SPLIT" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(
                        "SPLIT requires 1 or 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let delimiter = if args.len() == 2 {
                    args[1].as_str().unwrap_or(",")
                } else {
                    ","
                };
                let parts: Vec<Value> = s
                    .split(delimiter)
                    .map(|p| Value::string(p.to_string()))
                    .collect();
                Ok(Value::array(parts))
            }
            "POWER" | "POW" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "POWER requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let base = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64));
                let exp = args[1]
                    .as_f64()
                    .or_else(|| args[1].as_i64().map(|i| i as f64));
                match (base, exp) {
                    (Some(b), Some(e)) => Ok(Value::float64(b.powf(e))),
                    _ => Ok(Value::null()),
                }
            }
            "SQRT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("SQRT requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64));
                match n {
                    Some(v) if v >= 0.0 => Ok(Value::float64(v.sqrt())),
                    Some(_) => Ok(Value::null()),
                    None => Ok(Value::null()),
                }
            }
            "LOG" | "LN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("LOG requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64));
                match n {
                    Some(v) if v > 0.0 => Ok(Value::float64(v.ln())),
                    _ => Ok(Value::null()),
                }
            }
            "LOG10" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("LOG10 requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64));
                match n {
                    Some(v) if v > 0.0 => Ok(Value::float64(v.log10())),
                    _ => Ok(Value::null()),
                }
            }
            "EXP" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("EXP requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64));
                match n {
                    Some(v) => Ok(Value::float64(v.exp())),
                    None => Ok(Value::null()),
                }
            }
            "SIGN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("SIGN requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i.signum()));
                }
                if let Some(f) = args[0].as_f64() {
                    if f > 0.0 {
                        return Ok(Value::int64(1));
                    }
                    if f < 0.0 {
                        return Ok(Value::int64(-1));
                    }
                    return Ok(Value::int64(0));
                }
                Ok(Value::null())
            }
            "TRUNC" | "TRUNCATE" => {
                if args.len() != 1 && args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TRUNC requires 1 or 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let decimals = if args.len() == 2 {
                    args[1].as_i64().unwrap_or(0) as i32
                } else {
                    0
                };
                if let Some(f) = args[0].as_f64() {
                    let multiplier = 10f64.powi(decimals);
                    return Ok(Value::float64((f * multiplier).trunc() / multiplier));
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i));
                }
                Ok(Value::null())
            }
            "DATE" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery("DATE requires arguments".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        return Ok(Value::date(date));
                    }
                }
                Ok(Value::null())
            }
            "CURRENT_DATE" => {
                let today = chrono::Utc::now().date_naive();
                Ok(Value::date(today))
            }
            "CURRENT_TIMESTAMP" | "CURRENT_DATETIME" | "NOW" => {
                let now = chrono::Utc::now();
                Ok(Value::timestamp(now))
            }
            "FORMAT" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery("FORMAT requires arguments".to_string()));
                }
                if args.len() == 1 {
                    return Ok(args[0].clone());
                }
                let format_str = args[0].as_str().unwrap_or("%s");
                let mut result = format_str.to_string();
                for (i, arg) in args.iter().skip(1).enumerate() {
                    result = result.replacen("%s", &arg.to_string(), 1);
                    result = result.replace(&format!("%{}", i + 1), &arg.to_string());
                }
                Ok(Value::string(result))
            }
            "TO_JSON" | "TO_JSON_STRING" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TO_JSON requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::string("null".to_string()));
                }
                let json_val = self.value_to_json(&args[0]);
                Ok(Value::string(json_val.to_string()))
            }
            "REGEXP_REPLACE" => {
                if args.len() < 3 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_REPLACE requires at least 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let text = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let pattern = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let replacement = args[2].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[2].data_type().to_string(),
                })?;
                match regex::Regex::new(pattern) {
                    Ok(re) => Ok(Value::string(re.replace_all(text, replacement).to_string())),
                    Err(_) => Ok(Value::null()),
                }
            }
            "REGEXP_INSTR" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_INSTR requires at least 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let text = args[0].as_str().unwrap_or_default();
                let pattern = args[1].as_str().unwrap_or_default();
                match regex::Regex::new(pattern) {
                    Ok(re) => match re.find(text) {
                        Some(m) => Ok(Value::int64((m.start() + 1) as i64)),
                        None => Ok(Value::int64(0)),
                    },
                    Err(_) => Ok(Value::null()),
                }
            }
            "SAFE_OFFSET" | "OFFSET" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "OFFSET requires 1 argument".to_string(),
                    ));
                }
                Ok(args[0].clone())
            }
            "SAFE_ORDINAL" | "ORDINAL" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ORDINAL requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i - 1));
                }
                Ok(args[0].clone())
            }
            "GENERATE_UUID" => {
                let uuid = uuid::Uuid::new_v4();
                Ok(Value::string(uuid.to_string()))
            }
            "LPAD" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "LPAD requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().unwrap_or_default();
                let len = args[1].as_i64().unwrap_or(0) as usize;
                let pad = if args.len() == 3 {
                    args[2].as_str().unwrap_or(" ")
                } else {
                    " "
                };
                if s.chars().count() >= len {
                    return Ok(Value::string(s.chars().take(len).collect::<String>()));
                }
                let pad_len = len - s.chars().count();
                let pad_chars: Vec<char> = pad.chars().cycle().take(pad_len).collect();
                let result: String = pad_chars.into_iter().chain(s.chars()).collect();
                Ok(Value::string(result))
            }
            "RPAD" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "RPAD requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().unwrap_or_default();
                let len = args[1].as_i64().unwrap_or(0) as usize;
                let pad = if args.len() == 3 {
                    args[2].as_str().unwrap_or(" ")
                } else {
                    " "
                };
                if s.chars().count() >= len {
                    return Ok(Value::string(s.chars().take(len).collect::<String>()));
                }
                let pad_len = len - s.chars().count();
                let pad_chars: Vec<char> = pad.chars().cycle().take(pad_len).collect();
                let result: String = s.chars().chain(pad_chars).collect();
                Ok(Value::string(result))
            }
            "ARRAY_CONCAT" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "ARRAY_CONCAT requires at least 1 argument".to_string(),
                    ));
                }
                let mut result = Vec::new();
                for arg in args {
                    if arg.is_null() {
                        continue;
                    }
                    if let Some(arr) = arg.as_array() {
                        result.extend(arr.iter().cloned());
                    }
                }
                Ok(Value::array(result))
            }
            "ARRAY_REVERSE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_REVERSE requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(arr) = args[0].as_array() {
                    let mut reversed = arr.to_vec();
                    reversed.reverse();
                    return Ok(Value::array(reversed));
                }
                Ok(Value::null())
            }
            "ARRAY_TO_STRING" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_TO_STRING requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let delimiter = args[1].as_str().unwrap_or(",");
                let null_text = if args.len() == 3 {
                    args[2].as_str().map(|s| s.to_string())
                } else {
                    None
                };
                if let Some(arr) = args[0].as_array() {
                    let parts: Vec<String> = arr
                        .iter()
                        .filter_map(|v| {
                            if v.is_null() {
                                null_text.clone()
                            } else {
                                Some(v.to_string())
                            }
                        })
                        .collect();
                    return Ok(Value::string(parts.join(delimiter)));
                }
                Ok(Value::null())
            }
            "GENERATE_ARRAY" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "GENERATE_ARRAY requires 2 or 3 arguments".to_string(),
                    ));
                }
                let start = args[0].as_i64().unwrap_or(0);
                let end = args[1].as_i64().unwrap_or(0);
                let step = if args.len() == 3 {
                    args[2].as_i64().unwrap_or(1)
                } else {
                    1
                };
                if step == 0 {
                    return Err(Error::InvalidQuery(
                        "GENERATE_ARRAY step cannot be 0".to_string(),
                    ));
                }
                let mut result = Vec::new();
                let mut curr = start;
                if step > 0 {
                    while curr <= end {
                        result.push(Value::int64(curr));
                        curr += step;
                    }
                } else {
                    while curr >= end {
                        result.push(Value::int64(curr));
                        curr += step;
                    }
                }
                Ok(Value::array(result))
            }
            "ST_GEOGFROMTEXT" | "ST_GEOGRAPHYFROMTEXT" => {
                Ok(Value::string("GEOGRAPHY".to_string()))
            }
            "ST_GEOGPOINT" => Ok(Value::string("GEOGRAPHY_POINT".to_string())),
            "ST_GEOGFROMGEOJSON" => Ok(Value::string("GEOGRAPHY".to_string())),
            "ST_DISTANCE" | "ST_LENGTH" | "ST_PERIMETER" | "ST_AREA" => Ok(Value::float64(0.0)),
            "ST_ASTEXT" | "ST_ASGEOJSON" | "ST_ASBINARY" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                Ok(Value::string(args[0].to_string()))
            }
            "ST_X" | "ST_Y" => Ok(Value::float64(0.0)),
            "ST_CONTAINS" | "ST_WITHIN" | "ST_INTERSECTS" | "ST_DWITHIN" | "ST_COVERS"
            | "ST_COVEREDBY" => Ok(Value::bool_val(false)),
            "NET.IP_FROM_STRING" | "NET.SAFE_IP_FROM_STRING" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let ip_str = args[0].as_str().unwrap_or_default();
                Ok(Value::bytes(ip_str.as_bytes().to_vec()))
            }
            "NET.IP_TO_STRING" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(b) = args[0].as_bytes() {
                    return Ok(Value::string(String::from_utf8_lossy(b).to_string()));
                }
                Ok(Value::null())
            }
            "NET.IPV4_FROM_INT64" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    let b = (i as u32).to_be_bytes();
                    return Ok(Value::bytes(b.to_vec()));
                }
                Ok(Value::null())
            }
            "NET.IPV4_TO_INT64" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                Ok(Value::int64(0))
            }
            "NET.HOST" | "NET.REG_DOMAIN" | "NET.PUBLIC_SUFFIX" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                Ok(args[0].clone())
            }
            "RANGE" => Ok(Value::string("RANGE".to_string())),
            "JSON_ARRAY" => {
                let json_arr: Vec<serde_json::Value> =
                    args.iter().map(|v| self.value_to_json(v)).collect();
                Ok(Value::string(
                    serde_json::Value::Array(json_arr).to_string(),
                ))
            }
            "JSON_OBJECT" => {
                let mut obj = serde_json::Map::new();
                let mut i = 0;
                while i + 1 < args.len() {
                    let key = args[i].as_str().unwrap_or_default().to_string();
                    let val = self.value_to_json(&args[i + 1]);
                    obj.insert(key, val);
                    i += 2;
                }
                Ok(Value::string(serde_json::Value::Object(obj).to_string()))
            }
            "JSON_REMOVE" => {
                if args.len() < 2 {
                    return Ok(Value::null());
                }
                Ok(args[0].clone())
            }
            "JSON_STRIP_NULLS" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                Ok(args[0].clone())
            }
            "JSON_VALUE_ARRAY" | "JSON_QUERY_ARRAY" => Ok(Value::array(vec![])),
            "LAX_INT64" | "LAX_FLOAT64" | "LAX_STRING" | "LAX_BOOL" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                Ok(args[0].clone())
            }
            "APPROX_COUNT_DISTINCT" => Ok(Value::int64(0)),
            "APPROX_QUANTILES" => Ok(Value::array(vec![Value::null()])),
            "ALL" => {
                if args.is_empty() {
                    return Ok(Value::null());
                }
                Ok(args[0].clone())
            }
            "ST_MAKELINE" | "ST_BUFFER" | "ST_CENTROID" | "ST_CLOSESTPOINT" | "ST_CONVEXHULL"
            | "ST_DIFFERENCE" | "ST_INTERSECTION" | "ST_SIMPLIFY" | "ST_SNAPTOGRID"
            | "ST_UNION" => Ok(Value::string("GEOGRAPHY".to_string())),
            "ST_DIMENSION" | "ST_NUMPOINTS" | "ST_NUMGEOMETRIES" | "ST_NPOINTS" => {
                Ok(Value::int64(0))
            }
            "ST_ISEMPTY" | "ST_ISCOLLECTION" | "ST_ISRING" => Ok(Value::bool_val(false)),
            "RANGE_OVERLAPS" | "RANGE_CONTAINS" | "RANGE_INTERSECTS" => Ok(Value::bool_val(false)),
            "RANGE_START" | "RANGE_END" => Ok(Value::null()),
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" | "NTILE" => {
                Ok(Value::int64(1))
            }
            "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
                if args.is_empty() {
                    return Ok(Value::null());
                }
                Ok(args[0].clone())
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Function not yet supported: {}",
                name
            ))),
        }
    }

    fn value_to_json(&self, value: &Value) -> serde_json::Value {
        if value.is_null() {
            return serde_json::Value::Null;
        }
        if let Some(b) = value.as_bool() {
            return serde_json::Value::Bool(b);
        }
        if let Some(i) = value.as_i64() {
            return serde_json::Value::Number(serde_json::Number::from(i));
        }
        if let Some(f) = value.as_f64() {
            if let Some(n) = serde_json::Number::from_f64(f) {
                return serde_json::Value::Number(n);
            }
        }
        if let Some(s) = value.as_str() {
            return serde_json::Value::String(s.to_string());
        }
        if let Some(arr) = value.as_array() {
            let json_arr: Vec<serde_json::Value> =
                arr.iter().map(|v| self.value_to_json(v)).collect();
            return serde_json::Value::Array(json_arr);
        }
        if let Some(fields) = value.as_struct() {
            let mut map = serde_json::Map::new();
            for (name, val) in fields {
                map.insert(name.clone(), self.value_to_json(val));
            }
            return serde_json::Value::Object(map);
        }
        serde_json::Value::String(value.to_string())
    }

    fn json_path_extract(&self, json: &serde_json::Value, path: &str) -> Option<serde_json::Value> {
        let path = path.trim_start_matches('$');
        let mut current = json.clone();

        for part in path.split('.').filter(|s| !s.is_empty()) {
            let part = part.trim();
            if let Some(idx_start) = part.find('[') {
                let key = &part[..idx_start];
                if !key.is_empty() {
                    current = current.get(key)?.clone();
                }
                let idx_end = part.find(']')?;
                let idx_str = &part[idx_start + 1..idx_end];
                let idx: usize = idx_str.parse().ok()?;
                current = current.get(idx)?.clone();
            } else {
                current = current.get(part)?.clone();
            }
        }

        Some(current)
    }

    fn extract_function_args(
        &self,
        func: &sqlparser::ast::Function,
        record: &Record,
    ) -> Result<Vec<Value>> {
        let mut args = Vec::new();
        if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
            for arg in &arg_list.args {
                if let sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(expr),
                ) = arg
                {
                    args.push(self.evaluate(expr, record)?);
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

    pub fn evaluate_to_bool(&self, expr: &Expr, record: &Record) -> Result<bool> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(false);
        }
        val.as_bool().ok_or_else(|| Error::TypeMismatch {
            expected: "BOOL".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    pub fn evaluate_binary_op_values(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<Value> {
        self.evaluate_binary_op(left, op, right)
    }
}
