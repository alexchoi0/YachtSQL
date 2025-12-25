#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::collapsible_if)]

use std::collections::HashMap;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use geo::{
    BooleanOps, BoundingRect, Centroid, Contains, ConvexHull, GeodesicArea, GeodesicDistance,
    GeodesicLength, Intersects, SimplifyVw,
};
use geo_types::{Coord, Geometry, LineString, MultiPolygon, Point, Polygon};
use ordered_float::OrderedFloat;
use rust_decimal::prelude::ToPrimitive;
use wkt::TryFromWkt;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, IntervalValue, RangeValue, Value};
use yachtsql_ir::{
    AggregateFunction, BinaryOp, DateTimeField, Expr, FunctionArg, FunctionBody, Literal,
    ScalarFunction, TrimWhere, UnaryOp, WeekStartDay, WhenClause,
};
use yachtsql_storage::{Record, Schema};

pub struct UserFunctionDef {
    pub parameters: Vec<FunctionArg>,
    pub body: FunctionBody,
}

pub struct IrEvaluator<'a> {
    schema: &'a Schema,
    variables: Option<&'a HashMap<String, Value>>,
    user_functions: Option<&'a HashMap<String, UserFunctionDef>>,
}

impl<'a> IrEvaluator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            variables: None,
            user_functions: None,
        }
    }

    pub fn with_variables(mut self, variables: &'a HashMap<String, Value>) -> Self {
        self.variables = Some(variables);
        self
    }

    pub fn with_user_functions(
        mut self,
        user_functions: &'a HashMap<String, UserFunctionDef>,
    ) -> Self {
        self.user_functions = Some(user_functions);
        self
    }

    pub fn eval_scalar_function_with_values(
        &self,
        func: &ScalarFunction,
        arg_values: &[Value],
    ) -> Result<Value> {
        let vec_values: Vec<Value> = arg_values.to_vec();
        match func {
            ScalarFunction::Upper => self.fn_upper(&vec_values),
            ScalarFunction::Lower => self.fn_lower(&vec_values),
            ScalarFunction::Length => self.fn_length(&vec_values),
            ScalarFunction::Coalesce => self.fn_coalesce(&vec_values),
            ScalarFunction::IfNull | ScalarFunction::Ifnull => self.fn_ifnull(&vec_values),
            ScalarFunction::NullIf => self.fn_nullif(&vec_values),
            ScalarFunction::If => self.fn_if(&vec_values),
            ScalarFunction::Abs => self.fn_abs(&vec_values),
            ScalarFunction::Floor => self.fn_floor(&vec_values),
            ScalarFunction::Ceil => self.fn_ceil(&vec_values),
            ScalarFunction::Round => self.fn_round(&vec_values),
            ScalarFunction::Concat => self.fn_concat(&vec_values),
            ScalarFunction::Greatest => self.fn_greatest(&vec_values),
            ScalarFunction::Least => self.fn_least(&vec_values),
            _ => Err(Error::unsupported(format!(
                "Scalar function {:?} not yet supported in eval_scalar_function_with_values",
                func
            ))),
        }
    }

    pub fn cast_value(val: Value, target_type: &DataType, safe: bool) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::Null);
        }
        let result = match (&val, target_type) {
            (v, DataType::Unknown) => Ok(v.clone()),
            (Value::Int64(n), DataType::String) => Ok(Value::String(n.to_string())),
            (Value::Float64(f), DataType::String) => Ok(Value::String(f.0.to_string())),
            (Value::Bool(b), DataType::String) => Ok(Value::String(b.to_string())),
            (Value::String(s), DataType::Int64) => s
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| Error::InvalidQuery(format!("Cannot cast '{}' to INT64", s))),
            (Value::Float64(f), DataType::Int64) => {
                if f.0.is_nan()
                    || f.0.is_infinite()
                    || f.0 > i64::MAX as f64
                    || f.0 < i64::MIN as f64
                {
                    Err(Error::InvalidQuery(format!(
                        "Cannot cast {} to INT64: value out of range",
                        f.0
                    )))
                } else {
                    Ok(Value::Int64(f.0 as i64))
                }
            }
            (Value::Int64(n), DataType::Float64) => Ok(Value::Float64(OrderedFloat(*n as f64))),
            (Value::String(s), DataType::Float64) => s
                .parse::<f64>()
                .map(|f| Value::Float64(OrderedFloat(f)))
                .map_err(|_| Error::InvalidQuery(format!("Cannot cast '{}' to FLOAT64", s))),
            _ => Ok(val.clone()),
        };
        if safe {
            Ok(result.unwrap_or(Value::Null))
        } else {
            result
        }
    }

    pub fn evaluate(&self, expr: &Expr, record: &Record) -> Result<Value> {
        match expr {
            Expr::Literal(lit) => self.eval_literal(lit),
            Expr::Column { table, name, index } => {
                self.eval_column(table.as_deref(), name, *index, record)
            }
            Expr::BinaryOp { left, op, right } => self.eval_binary_op(left, *op, right, record),
            Expr::UnaryOp { op, expr } => self.eval_unary_op(*op, expr, record),
            Expr::ScalarFunction { name, args } => self.eval_scalar_function(name, args, record),
            Expr::Aggregate { .. } => Err(Error::InvalidQuery(
                "Aggregates should be evaluated by aggregate executor".into(),
            )),
            Expr::UserDefinedAggregate { .. } => Err(Error::InvalidQuery(
                "User-defined aggregates should be evaluated by aggregate executor".into(),
            )),
            Expr::Window { .. } => Err(Error::InvalidQuery(
                "Window functions should be evaluated by window executor".into(),
            )),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => self.eval_case(
                operand.as_deref(),
                when_clauses,
                else_result.as_deref(),
                record,
            ),
            Expr::Cast {
                expr,
                data_type,
                safe,
            } => self.eval_cast(expr, data_type, *safe, record),
            Expr::IsNull { expr, negated } => self.eval_is_null(expr, *negated, record),
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => self.eval_is_distinct_from(left, right, *negated, record),
            Expr::InList {
                expr,
                list,
                negated,
            } => self.eval_in_list(expr, list, *negated, record),
            Expr::InSubquery { .. } => Err(Error::InvalidQuery(
                "Subqueries should be evaluated by plan executor".into(),
            )),
            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => self.eval_in_unnest(expr, array_expr, *negated, record),
            Expr::Exists { .. } => Err(Error::InvalidQuery(
                "EXISTS should be evaluated by plan executor".into(),
            )),
            Expr::ArraySubquery(_) => Err(Error::InvalidQuery(
                "ArraySubquery should be evaluated by plan executor".into(),
            )),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => self.eval_between(expr, low, high, *negated, record),
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => self.eval_like(expr, pattern, *negated, *case_insensitive, record),
            Expr::Extract { field, expr } => self.eval_extract(*field, expr, record),
            Expr::Substring {
                expr,
                start,
                length,
            } => self.eval_substring(expr, start.as_deref(), length.as_deref(), record),
            Expr::Trim {
                expr,
                trim_what,
                trim_where,
            } => self.eval_trim(expr, trim_what.as_deref(), *trim_where, record),
            Expr::Position { substr, string } => self.eval_position(substr, string, record),
            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => self.eval_overlay(
                expr,
                overlay_what,
                overlay_from,
                overlay_for.as_deref(),
                record,
            ),
            Expr::Array { elements, .. } => self.eval_array(elements, record),
            Expr::ArrayAccess { array, index } => self.eval_array_access(array, index, record),
            Expr::Struct { fields } => self.eval_struct(fields, record),
            Expr::StructAccess { expr, field } => self.eval_struct_access(expr, field, record),
            Expr::TypedString { data_type, value } => self.eval_typed_string(data_type, value),
            Expr::Interval {
                value,
                leading_field,
            } => self.eval_interval(value, *leading_field, record),
            Expr::Alias { expr, .. } => self.evaluate(expr, record),
            Expr::Wildcard { .. } => Err(Error::InvalidQuery(
                "Wildcard should be expanded before evaluation".into(),
            )),
            Expr::Subquery(_) => Err(Error::InvalidQuery(
                "Subqueries should be evaluated by plan executor".into(),
            )),
            Expr::AggregateWindow { .. } => Err(Error::InvalidQuery(
                "Aggregate window functions should be evaluated by plan executor".into(),
            )),
            Expr::ScalarSubquery(_) => Err(Error::InvalidQuery(
                "Scalar subqueries should be evaluated by plan executor".into(),
            )),
            Expr::Parameter { name } => {
                Err(Error::InvalidQuery(format!("Unbound parameter: {}", name)))
            }
            Expr::Variable { name } => {
                if let Some(vars) = self.variables {
                    let upper_name = name.to_uppercase();
                    if let Some(val) = vars.get(&upper_name) {
                        return Ok(val.clone());
                    }
                }
                Err(Error::InvalidQuery(format!(
                    "Variable '{}' not in scope",
                    name
                )))
            }
            Expr::Placeholder { id } => {
                Err(Error::InvalidQuery(format!("Unbound placeholder: {}", id)))
            }
            Expr::Lambda { .. } => Err(Error::InvalidQuery(
                "Lambda expressions should be evaluated inline".into(),
            )),
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => self.eval_at_time_zone(timestamp, time_zone, record),
            Expr::JsonAccess { expr, path } => self.eval_json_access(expr, path, record),
            Expr::Default => Ok(Value::Default),
        }
    }

    fn eval_at_time_zone(
        &self,
        timestamp: &Expr,
        time_zone: &Expr,
        record: &Record,
    ) -> Result<Value> {
        let ts_val = self.evaluate(timestamp, record)?;
        let tz_val = self.evaluate(time_zone, record)?;
        match (ts_val, tz_val) {
            (Value::Timestamp(ts), Value::String(tz_name)) => Ok(Value::Timestamp(ts)),
            (Value::DateTime(dt), Value::String(_)) => Ok(Value::DateTime(dt)),
            (v, _) => Ok(v),
        }
    }

    fn eval_json_access(
        &self,
        expr: &Expr,
        path: &[yachtsql_ir::JsonPathElement],
        record: &Record,
    ) -> Result<Value> {
        let base = self.evaluate(expr, record)?;
        let mut current = match base {
            Value::Json(j) => j,
            Value::String(s) => {
                serde_json::from_str(&s).map_err(|_| Error::InvalidQuery("Invalid JSON".into()))?
            }
            _ => return Err(Error::InvalidQuery("Cannot access non-JSON value".into())),
        };
        for element in path {
            match element {
                yachtsql_ir::JsonPathElement::Key(key) => {
                    current = current.get(key).cloned().unwrap_or(serde_json::Value::Null);
                }
                yachtsql_ir::JsonPathElement::Index(idx) => {
                    current = current
                        .get(*idx as usize)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                }
            }
        }
        Ok(Value::Json(current))
    }

    fn eval_literal(&self, lit: &Literal) -> Result<Value> {
        match lit {
            Literal::Null => Ok(Value::Null),
            Literal::Bool(b) => Ok(Value::Bool(*b)),
            Literal::Int64(n) => Ok(Value::Int64(*n)),
            Literal::Float64(f) => Ok(Value::Float64(OrderedFloat(f.0))),
            Literal::Numeric(d) => Ok(Value::Numeric(*d)),
            Literal::String(s) => Ok(Value::String(s.clone())),
            Literal::Bytes(b) => Ok(Value::Bytes(b.clone())),
            Literal::Date(days) => {
                let date = NaiveDate::from_num_days_from_ce_opt(*days + 719163)
                    .ok_or_else(|| Error::InvalidQuery("Invalid date".into()))?;
                Ok(Value::Date(date))
            }
            Literal::Time(micros) => {
                let secs = (*micros / 1_000_000) as u32;
                let nanos = ((*micros % 1_000_000) * 1000) as u32;
                let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .ok_or_else(|| Error::InvalidQuery("Invalid time".into()))?;
                Ok(Value::Time(time))
            }
            Literal::Timestamp(micros) => {
                let dt = DateTime::from_timestamp_micros(*micros)
                    .ok_or_else(|| Error::InvalidQuery("Invalid timestamp".into()))?;
                Ok(Value::Timestamp(dt))
            }
            Literal::Interval {
                months,
                days,
                nanos,
            } => Ok(Value::Interval(IntervalValue {
                months: *months,
                days: *days,
                nanos: *nanos,
            })),
            Literal::Array(elements) => {
                let values: Result<Vec<_>> =
                    elements.iter().map(|e| self.eval_literal(e)).collect();
                Ok(Value::Array(values?))
            }
            Literal::Struct(fields) => {
                let struct_fields: Result<Vec<_>> = fields
                    .iter()
                    .map(|(name, lit)| {
                        let val = self.eval_literal(lit)?;
                        Ok((name.clone(), val))
                    })
                    .collect();
                Ok(Value::Struct(struct_fields?))
            }
            Literal::Json(json) => Ok(Value::Json(json.clone())),
            Literal::BigNumeric(d) => Ok(Value::BigNumeric(*d)),
            Literal::Datetime(micros) => {
                let secs = *micros / 1_000_000;
                let nanos = ((*micros % 1_000_000) * 1000) as u32;
                let dt = chrono::DateTime::from_timestamp(secs, nanos)
                    .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))?
                    .naive_utc();
                Ok(Value::DateTime(dt))
            }
        }
    }

    fn eval_column(
        &self,
        table: Option<&str>,
        name: &str,
        index: Option<usize>,
        record: &Record,
    ) -> Result<Value> {
        if let Some(idx) = index {
            if idx < record.values().len() {
                return Ok(record.values()[idx].clone());
            }
        }

        if let Some(vars) = self.variables {
            let upper_name = name.to_uppercase();
            if let Some(val) = vars.get(&upper_name) {
                return Ok(val.clone());
            }
            if let Some(table_name) = table {
                let upper_table = table_name.to_uppercase();
                if let Some(Value::Struct(fields)) = vars.get(&upper_table) {
                    let field_name_lower = name.to_lowercase();
                    for (field_name, field_value) in fields {
                        if field_name.to_lowercase() == field_name_lower {
                            return Ok(field_value.clone());
                        }
                    }
                }
            }
        }

        let idx = self.schema.field_index_qualified(name, table);

        match idx {
            Some(i) if i < record.values().len() => Ok(record.values()[i].clone()),
            _ => Err(Error::ColumnNotFound(name.to_string())),
        }
    }

    fn get_collation_for_expr(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column { name, table, .. } => {
                let upper_col = name.to_uppercase();
                for field in self.schema.fields() {
                    if field.name.to_uppercase() == upper_col {
                        if let Some(src) = &field.source_table {
                            if let Some(prefix) = table {
                                if prefix.to_uppercase() != src.to_uppercase() {
                                    continue;
                                }
                            }
                        }
                        return field.collation.clone();
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn eval_binary_op(
        &self,
        left: &Expr,
        op: BinaryOp,
        right: &Expr,
        record: &Record,
    ) -> Result<Value> {
        let left_val = self.evaluate(left, record)?;
        let right_val = self.evaluate(right, record)?;

        match op {
            BinaryOp::Add => self.add_values(&left_val, &right_val),
            BinaryOp::Sub => self.sub_values(&left_val, &right_val),
            BinaryOp::Mul => self.mul_values(&left_val, &right_val),
            BinaryOp::Div => self.div_values(&left_val, &right_val),
            BinaryOp::Mod => self.mod_values(&left_val, &right_val),
            BinaryOp::Eq => {
                let collation = self
                    .get_collation_for_expr(left)
                    .or_else(|| self.get_collation_for_expr(right));
                self.eq_values_with_collation(&left_val, &right_val, collation.as_deref())
            }
            BinaryOp::NotEq => {
                let collation = self
                    .get_collation_for_expr(left)
                    .or_else(|| self.get_collation_for_expr(right));
                self.neq_values_with_collation(&left_val, &right_val, collation.as_deref())
            }
            BinaryOp::Lt => self.compare_values(&left_val, &right_val, |ord| ord.is_lt()),
            BinaryOp::LtEq => self.compare_values(&left_val, &right_val, |ord| ord.is_le()),
            BinaryOp::Gt => self.compare_values(&left_val, &right_val, |ord| ord.is_gt()),
            BinaryOp::GtEq => self.compare_values(&left_val, &right_val, |ord| ord.is_ge()),
            BinaryOp::And => self.and_values(&left_val, &right_val),
            BinaryOp::Or => self.or_values(&left_val, &right_val),
            BinaryOp::Concat => self.concat_values(&left_val, &right_val),
            BinaryOp::BitwiseAnd => self.bitwise_and(&left_val, &right_val),
            BinaryOp::BitwiseOr => self.bitwise_or(&left_val, &right_val),
            BinaryOp::BitwiseXor => self.bitwise_xor(&left_val, &right_val),
            BinaryOp::ShiftLeft => self.shift_left(&left_val, &right_val),
            BinaryOp::ShiftRight => self.shift_right(&left_val, &right_val),
        }
    }

    fn eval_unary_op(&self, op: UnaryOp, expr: &Expr, record: &Record) -> Result<Value> {
        let val = self.evaluate(expr, record)?;

        match op {
            UnaryOp::Not => match val {
                Value::Null => Ok(Value::Null),
                Value::Bool(b) => Ok(Value::Bool(!b)),
                _ => Err(Error::InvalidQuery("NOT requires boolean operand".into())),
            },
            UnaryOp::Minus => match val {
                Value::Null => Ok(Value::Null),
                Value::Int64(n) => Ok(Value::Int64(-n)),
                Value::Float64(f) => Ok(Value::Float64(OrderedFloat(-f.0))),
                Value::Numeric(d) => Ok(Value::Numeric(-d)),
                _ => Err(Error::InvalidQuery(
                    "Unary minus requires numeric operand".into(),
                )),
            },
            UnaryOp::Plus => Ok(val),
            UnaryOp::BitwiseNot => match val {
                Value::Null => Ok(Value::Null),
                Value::Int64(n) => Ok(Value::Int64(!n)),
                _ => Err(Error::InvalidQuery(
                    "Bitwise NOT requires integer operand".into(),
                )),
            },
        }
    }

    fn eval_scalar_function(
        &self,
        func: &ScalarFunction,
        args: &[Expr],
        record: &Record,
    ) -> Result<Value> {
        let arg_values: Vec<Value> = args
            .iter()
            .map(|a| self.evaluate(a, record))
            .collect::<Result<_>>()?;

        match func {
            ScalarFunction::Upper => self.fn_upper(&arg_values),
            ScalarFunction::Lower => self.fn_lower(&arg_values),
            ScalarFunction::Length => self.fn_length(&arg_values),
            ScalarFunction::Coalesce => self.fn_coalesce(&arg_values),
            ScalarFunction::IfNull | ScalarFunction::Ifnull => self.fn_ifnull(&arg_values),
            ScalarFunction::NullIf => self.fn_nullif(&arg_values),
            ScalarFunction::If => self.fn_if(&arg_values),
            ScalarFunction::Abs => self.fn_abs(&arg_values),
            ScalarFunction::Floor => self.fn_floor(&arg_values),
            ScalarFunction::Ceil => self.fn_ceil(&arg_values),
            ScalarFunction::Round => self.fn_round(&arg_values),
            ScalarFunction::Sqrt => self.fn_sqrt(&arg_values),
            ScalarFunction::Cbrt => self.fn_cbrt(&arg_values),
            ScalarFunction::Power | ScalarFunction::Pow => self.fn_power(&arg_values),
            ScalarFunction::Mod => self.fn_mod(&arg_values),
            ScalarFunction::Sign => self.fn_sign(&arg_values),
            ScalarFunction::Exp => self.fn_exp(&arg_values),
            ScalarFunction::Ln => self.fn_ln(&arg_values),
            ScalarFunction::Log => self.fn_log(&arg_values),
            ScalarFunction::Log10 => self.fn_log10(&arg_values),
            ScalarFunction::Greatest => self.fn_greatest(&arg_values),
            ScalarFunction::Least => self.fn_least(&arg_values),
            ScalarFunction::Trunc => self.fn_trunc(&arg_values),
            ScalarFunction::Div => self.fn_div(&arg_values),
            ScalarFunction::SafeDivide => self.fn_safe_divide(&arg_values),
            ScalarFunction::IeeeDivide => self.fn_ieee_divide(&arg_values),
            ScalarFunction::SafeMultiply => self.fn_safe_multiply(&arg_values),
            ScalarFunction::SafeAdd => self.fn_safe_add(&arg_values),
            ScalarFunction::SafeSubtract => self.fn_safe_subtract(&arg_values),
            ScalarFunction::SafeNegate => self.fn_safe_negate(&arg_values),
            ScalarFunction::Concat => self.fn_concat(&arg_values),
            ScalarFunction::Trim => self.fn_trim(&arg_values),
            ScalarFunction::LTrim => self.fn_ltrim(&arg_values),
            ScalarFunction::RTrim => self.fn_rtrim(&arg_values),
            ScalarFunction::Substr => self.fn_substr(&arg_values),
            ScalarFunction::Replace => self.fn_replace(&arg_values),
            ScalarFunction::Reverse => self.fn_reverse(&arg_values),
            ScalarFunction::Left => self.fn_left(&arg_values),
            ScalarFunction::Right => self.fn_right(&arg_values),
            ScalarFunction::Repeat => self.fn_repeat(&arg_values),
            ScalarFunction::StartsWith => self.fn_starts_with(&arg_values),
            ScalarFunction::EndsWith => self.fn_ends_with(&arg_values),
            ScalarFunction::Contains => self.fn_contains(&arg_values),
            ScalarFunction::Strpos => self.fn_strpos(&arg_values),
            ScalarFunction::Lpad => self.fn_lpad(&arg_values),
            ScalarFunction::Rpad => self.fn_rpad(&arg_values),
            ScalarFunction::Initcap => self.fn_initcap(&arg_values),
            ScalarFunction::Datetime => self.fn_datetime(&arg_values),
            ScalarFunction::Date => self.fn_date(&arg_values),
            ScalarFunction::Time => self.fn_time(&arg_values),
            ScalarFunction::Timestamp => self.fn_timestamp(&arg_values),
            ScalarFunction::ArrayLength => self.fn_array_length(&arg_values),
            ScalarFunction::ArrayToString => self.fn_array_to_string(&arg_values),
            ScalarFunction::GenerateArray => self.fn_generate_array(&arg_values),
            ScalarFunction::GenerateDateArray => self.fn_generate_date_array(&arg_values),
            ScalarFunction::GenerateTimestampArray => self.fn_generate_timestamp_array(&arg_values),
            ScalarFunction::Md5 => self.fn_md5(&arg_values),
            ScalarFunction::Sha256 => self.fn_sha256(&arg_values),
            ScalarFunction::Sha512 => self.fn_sha512(&arg_values),
            ScalarFunction::FarmFingerprint => self.fn_farm_fingerprint(&arg_values),
            ScalarFunction::GenerateUuid => self.fn_generate_uuid(&arg_values),
            ScalarFunction::CurrentDate => self.fn_current_date(&arg_values),
            ScalarFunction::CurrentTimestamp => self.fn_current_timestamp(&arg_values),
            ScalarFunction::CurrentTime => self.fn_current_time(&arg_values),
            ScalarFunction::CurrentDatetime => self.fn_current_datetime(&arg_values),
            ScalarFunction::Zeroifnull => self.fn_zeroifnull(&arg_values),
            ScalarFunction::String => self.fn_string(&arg_values),
            ScalarFunction::RegexpContains => self.fn_regexp_contains(&arg_values),
            ScalarFunction::RegexpReplace => self.fn_regexp_replace(&arg_values),
            ScalarFunction::DateAdd => self.fn_date_add(&arg_values),
            ScalarFunction::DateSub => self.fn_date_sub(&arg_values),
            ScalarFunction::DateDiff => self.fn_date_diff(&arg_values),
            ScalarFunction::DateTrunc => self.fn_date_trunc(&arg_values),
            ScalarFunction::DateBucket => self.fn_date_bucket(&arg_values),
            ScalarFunction::DatetimeTrunc => self.fn_datetime_trunc(&arg_values),
            ScalarFunction::TimestampTrunc => self.fn_timestamp_trunc(&arg_values),
            ScalarFunction::TimeTrunc => self.fn_time_trunc(&arg_values),
            ScalarFunction::DateFromUnixDate => self.fn_date_from_unix_date(&arg_values),
            ScalarFunction::UnixDate => self.fn_unix_date(&arg_values),
            ScalarFunction::UnixMicros => self.fn_unix_micros(&arg_values),
            ScalarFunction::UnixMillis => self.fn_unix_millis(&arg_values),
            ScalarFunction::UnixSeconds => self.fn_unix_seconds(&arg_values),
            ScalarFunction::TimestampMicros => self.fn_timestamp_micros(&arg_values),
            ScalarFunction::TimestampMillis => self.fn_timestamp_millis(&arg_values),
            ScalarFunction::TimestampSeconds => self.fn_timestamp_seconds(&arg_values),
            ScalarFunction::FormatDate => self.fn_format_date(&arg_values),
            ScalarFunction::FormatDatetime => self.fn_format_datetime(&arg_values),
            ScalarFunction::FormatTimestamp => self.fn_format_timestamp(&arg_values),
            ScalarFunction::FormatTime => self.fn_format_time(&arg_values),
            ScalarFunction::ParseDate => self.fn_parse_date(&arg_values),
            ScalarFunction::ParseDatetime => self.fn_parse_datetime(&arg_values),
            ScalarFunction::ParseTimestamp => self.fn_parse_timestamp(&arg_values),
            ScalarFunction::ParseTime => self.fn_parse_time(&arg_values),
            ScalarFunction::LastDay => self.fn_last_day(&arg_values),
            ScalarFunction::DatetimeBucket => self.fn_datetime_bucket(&arg_values),
            ScalarFunction::TimestampBucket => self.fn_timestamp_bucket(&arg_values),
            ScalarFunction::Extract => self.fn_extract_from_args(args, record),
            ScalarFunction::Sin => self.fn_sin(&arg_values),
            ScalarFunction::Cos => self.fn_cos(&arg_values),
            ScalarFunction::Tan => self.fn_tan(&arg_values),
            ScalarFunction::Asin => self.fn_asin(&arg_values),
            ScalarFunction::Acos => self.fn_acos(&arg_values),
            ScalarFunction::Atan => self.fn_atan(&arg_values),
            ScalarFunction::Atan2 => self.fn_atan2(&arg_values),
            ScalarFunction::Sinh => self.fn_sinh(&arg_values),
            ScalarFunction::Cosh => self.fn_cosh(&arg_values),
            ScalarFunction::Tanh => self.fn_tanh(&arg_values),
            ScalarFunction::Asinh => self.fn_asinh(&arg_values),
            ScalarFunction::Acosh => self.fn_acosh(&arg_values),
            ScalarFunction::Atanh => self.fn_atanh(&arg_values),
            ScalarFunction::Cot => self.fn_cot(&arg_values),
            ScalarFunction::Csc => self.fn_csc(&arg_values),
            ScalarFunction::Sec => self.fn_sec(&arg_values),
            ScalarFunction::Coth => self.fn_coth(&arg_values),
            ScalarFunction::Csch => self.fn_csch(&arg_values),
            ScalarFunction::Sech => self.fn_sech(&arg_values),
            ScalarFunction::Pi => Ok(Value::Float64(OrderedFloat(std::f64::consts::PI))),
            ScalarFunction::IsNan => self.fn_is_nan(&arg_values),
            ScalarFunction::IsInf => self.fn_is_inf(&arg_values),
            ScalarFunction::CosineDistance => self.fn_cosine_distance(&arg_values),
            ScalarFunction::EuclideanDistance => self.fn_euclidean_distance(&arg_values),
            ScalarFunction::RangeBucket => self.fn_range_bucket(&arg_values),
            ScalarFunction::Rand | ScalarFunction::RandCanonical => self.fn_rand(&arg_values),
            ScalarFunction::Split => self.fn_split(&arg_values),
            ScalarFunction::ArrayConcat => self.fn_array_concat(&arg_values),
            ScalarFunction::ArrayReverse => self.fn_array_reverse(&arg_values),
            ScalarFunction::Instr => self.fn_instr(&arg_values),
            ScalarFunction::Format => self.fn_format(&arg_values),
            ScalarFunction::Cast | ScalarFunction::SafeCast => self.fn_cast(&arg_values),
            ScalarFunction::RegexpExtract => self.fn_regexp_extract(&arg_values),
            ScalarFunction::RegexpExtractAll => self.fn_regexp_extract_all(&arg_values),
            ScalarFunction::ByteLength | ScalarFunction::CharLength => {
                self.fn_byte_length(&arg_values)
            }
            ScalarFunction::Ascii => self.fn_ascii(&arg_values),
            ScalarFunction::Chr => self.fn_chr(&arg_values),
            ScalarFunction::Unicode => self.fn_unicode(&arg_values),
            ScalarFunction::ToCodePoints => self.fn_to_code_points(&arg_values),
            ScalarFunction::CodePointsToString => self.fn_code_points_to_string(&arg_values),
            ScalarFunction::CodePointsToBytes => self.fn_code_points_to_bytes(&arg_values),
            ScalarFunction::Translate => self.fn_translate(&arg_values),
            ScalarFunction::Soundex => self.fn_soundex(&arg_values),
            ScalarFunction::Normalize => self.fn_normalize(&arg_values),
            ScalarFunction::NormalizeAndCasefold => self.fn_normalize_and_casefold(&arg_values),
            ScalarFunction::EditDistance => self.fn_edit_distance(&arg_values),
            ScalarFunction::ContainsSubstr => self.fn_contains_substr(&arg_values),
            ScalarFunction::ToBase64 => self.fn_to_base64(&arg_values),
            ScalarFunction::FromBase64 => self.fn_from_base64(&arg_values),
            ScalarFunction::ToBase32 => self.fn_to_base32(&arg_values),
            ScalarFunction::FromBase32 => self.fn_from_base32(&arg_values),
            ScalarFunction::ToHex => self.fn_to_hex(&arg_values),
            ScalarFunction::FromHex => self.fn_from_hex(&arg_values),
            ScalarFunction::ToJson => self.fn_to_json(&arg_values),
            ScalarFunction::ToJsonString => self.fn_to_json_string(&arg_values),
            ScalarFunction::ParseJson => self.fn_parse_json(&arg_values),
            ScalarFunction::JsonValue => self.fn_json_value(&arg_values),
            ScalarFunction::JsonQuery => self.fn_json_query(&arg_values),
            ScalarFunction::JsonExtract | ScalarFunction::JsonExtractScalar => {
                self.fn_json_extract(&arg_values)
            }
            ScalarFunction::JsonExtractArray => self.fn_json_extract_array(&arg_values),
            ScalarFunction::TypeOf => self.fn_typeof(&arg_values),
            ScalarFunction::Nvl => self.fn_ifnull(&arg_values),
            ScalarFunction::Nvl2 => self.fn_nvl2(&arg_values),
            ScalarFunction::SafeConvertBytesToString => {
                self.fn_safe_convert_bytes_to_string(&arg_values)
            }
            ScalarFunction::ConvertBytesToString => self.fn_convert_bytes_to_string(&arg_values),
            ScalarFunction::Range => self.fn_range(&arg_values),
            ScalarFunction::JsonType => self.fn_json_type(&arg_values),
            ScalarFunction::BitCount => self.fn_bit_count(&arg_values),
            ScalarFunction::Int64FromJson => self.fn_int64_from_json(&arg_values),
            ScalarFunction::Float64FromJson => self.fn_float64_from_json(&arg_values),
            ScalarFunction::BoolFromJson => self.fn_bool_from_json(&arg_values),
            ScalarFunction::StringFromJson => self.fn_string_from_json(&arg_values),
            ScalarFunction::MakeInterval => self.fn_make_interval(&arg_values),
            ScalarFunction::JustifyDays => self.fn_justify_days(&arg_values),
            ScalarFunction::JustifyHours => self.fn_justify_hours(&arg_values),
            ScalarFunction::JustifyInterval => self.fn_justify_interval(&arg_values),
            ScalarFunction::SafeOffset | ScalarFunction::SafeOrdinal => {
                self.fn_safe_array_access(&arg_values, matches!(func, ScalarFunction::SafeOrdinal))
            }
            ScalarFunction::Custom(name) => self.eval_custom_function(name, &arg_values),
            _ => Err(Error::UnsupportedFeature(format!(
                "Scalar function {:?} not yet implemented in IR evaluator",
                func
            ))),
        }
    }

    fn eval_case(
        &self,
        operand: Option<&Expr>,
        when_clauses: &[WhenClause],
        else_result: Option<&Expr>,
        record: &Record,
    ) -> Result<Value> {
        match operand {
            Some(op_expr) => {
                let op_val = self.evaluate(op_expr, record)?;
                for clause in when_clauses {
                    let when_val = self.evaluate(&clause.condition, record)?;
                    if self.values_equal(&op_val, &when_val) {
                        return self.evaluate(&clause.result, record);
                    }
                }
            }
            None => {
                for clause in when_clauses {
                    let cond_val = self.evaluate(&clause.condition, record)?;
                    if cond_val.as_bool().unwrap_or(false) {
                        return self.evaluate(&clause.result, record);
                    }
                }
            }
        }

        match else_result {
            Some(e) => self.evaluate(e, record),
            None => Ok(Value::Null),
        }
    }

    fn eval_cast(
        &self,
        expr: &Expr,
        target_type: &DataType,
        safe: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(Value::Null);
        }
        let result = match (val, target_type) {
            (v, DataType::Unknown) => Ok(v),

            (Value::Int64(n), DataType::String) => Ok(Value::String(n.to_string())),
            (Value::Float64(f), DataType::String) => Ok(Value::String(f.0.to_string())),
            (Value::Bool(b), DataType::String) => Ok(Value::String(b.to_string())),
            (Value::Date(d), DataType::String) => Ok(Value::String(d.to_string())),
            (Value::Time(t), DataType::String) => Ok(Value::String(t.to_string())),
            (Value::DateTime(dt), DataType::String) => Ok(Value::String(dt.to_string())),
            (Value::Timestamp(ts), DataType::String) => Ok(Value::String(ts.to_rfc3339())),
            (Value::Numeric(n), DataType::String) => Ok(Value::String(n.to_string())),

            (Value::String(s), DataType::Int64) => s
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| Error::InvalidQuery(format!("Cannot cast '{}' to INT64", s))),
            (Value::Float64(f), DataType::Int64) => {
                if f.0.is_nan()
                    || f.0.is_infinite()
                    || f.0 > i64::MAX as f64
                    || f.0 < i64::MIN as f64
                {
                    Err(Error::InvalidQuery(format!(
                        "Cannot cast {} to INT64: value out of range",
                        f.0
                    )))
                } else {
                    Ok(Value::Int64(f.0 as i64))
                }
            }
            (Value::Bool(b), DataType::Int64) => Ok(Value::Int64(if b { 1 } else { 0 })),
            (Value::Numeric(n), DataType::Int64) => n
                .to_i64()
                .map(Value::Int64)
                .ok_or_else(|| Error::InvalidQuery("Cannot cast NUMERIC to INT64".into())),

            (Value::String(s), DataType::Float64) => s
                .parse::<f64>()
                .map(|f| Value::Float64(OrderedFloat(f)))
                .map_err(|_| Error::InvalidQuery(format!("Cannot cast '{}' to FLOAT64", s))),
            (Value::Int64(n), DataType::Float64) => Ok(Value::Float64(OrderedFloat(n as f64))),
            (Value::Numeric(n), DataType::Float64) => n
                .to_f64()
                .map(|f| Value::Float64(OrderedFloat(f)))
                .ok_or_else(|| Error::InvalidQuery("Cannot cast NUMERIC to FLOAT64".into())),

            (Value::String(s), DataType::Bool) => match s.to_lowercase().as_str() {
                "true" | "1" => Ok(Value::Bool(true)),
                "false" | "0" => Ok(Value::Bool(false)),
                _ => Err(Error::InvalidQuery(format!("Cannot cast '{}' to BOOL", s))),
            },
            (Value::Int64(n), DataType::Bool) => Ok(Value::Bool(n != 0)),

            (Value::String(s), DataType::Date) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .map(Value::Date)
                .map_err(|e| Error::InvalidQuery(format!("Cannot cast to DATE: {}", e))),
            (Value::DateTime(dt), DataType::Date) => Ok(Value::Date(dt.date())),
            (Value::Timestamp(ts), DataType::Date) => Ok(Value::Date(ts.date_naive())),

            (Value::String(s), DataType::Time) => NaiveTime::parse_from_str(&s, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(&s, "%H:%M:%S%.f"))
                .map(Value::Time)
                .map_err(|e| Error::InvalidQuery(format!("Cannot cast to TIME: {}", e))),
            (Value::DateTime(dt), DataType::Time) => Ok(Value::Time(dt.time())),
            (Value::Timestamp(ts), DataType::Time) => Ok(Value::Time(ts.time())),

            (Value::String(s), DataType::DateTime) => {
                NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f"))
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S"))
                    .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f"))
                    .map(Value::DateTime)
                    .map_err(|e| Error::InvalidQuery(format!("Cannot cast to DATETIME: {}", e)))
            }
            (Value::Date(d), DataType::DateTime) => d
                .and_hms_opt(0, 0, 0)
                .map(Value::DateTime)
                .ok_or_else(|| Error::InvalidQuery("Cannot convert DATE to DATETIME".into())),
            (Value::Timestamp(ts), DataType::DateTime) => Ok(Value::DateTime(ts.naive_utc())),

            (Value::String(s), DataType::Timestamp) => DateTime::parse_from_rfc3339(&s)
                .map(|d| Value::Timestamp(d.with_timezone(&Utc)))
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f"))
                        .or_else(|_| NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S"))
                        .map(|ndt| Value::Timestamp(ndt.and_utc()))
                })
                .map_err(|e| Error::InvalidQuery(format!("Cannot cast to TIMESTAMP: {}", e))),
            (Value::Date(d), DataType::Timestamp) => d
                .and_hms_opt(0, 0, 0)
                .map(|dt| Value::Timestamp(dt.and_utc()))
                .ok_or_else(|| Error::InvalidQuery("Cannot convert DATE to TIMESTAMP".into())),
            (Value::DateTime(dt), DataType::Timestamp) => Ok(Value::Timestamp(dt.and_utc())),

            (Value::Int64(n), DataType::Numeric(_) | DataType::BigNumeric) => {
                Ok(Value::Numeric(rust_decimal::Decimal::from(n)))
            }
            (Value::Float64(f), DataType::Numeric(_) | DataType::BigNumeric) => {
                rust_decimal::Decimal::try_from(f.0)
                    .map(Value::Numeric)
                    .map_err(|e| Error::InvalidQuery(format!("Cannot cast to NUMERIC: {}", e)))
            }
            (Value::String(s), DataType::Numeric(_) | DataType::BigNumeric) => s
                .parse::<rust_decimal::Decimal>()
                .map(Value::Numeric)
                .map_err(|e| Error::InvalidQuery(format!("Cannot cast to NUMERIC: {}", e))),

            (Value::String(s), DataType::Bytes) => Ok(Value::Bytes(s.as_bytes().to_vec())),
            (Value::Bytes(b), DataType::String) => String::from_utf8(b)
                .map(Value::String)
                .map_err(|e| Error::InvalidQuery(format!("Cannot cast BYTES to STRING: {}", e))),

            (Value::Array(arr), DataType::Array(elem_type)) => {
                let mut new_arr = Vec::with_capacity(arr.len());
                for elem in arr {
                    let casted = Self::cast_value(elem, elem_type, safe)?;
                    new_arr.push(casted);
                }
                Ok(Value::Array(new_arr))
            }

            (v, _) if v.data_type() == *target_type => Ok(v),

            (v, t) => Err(Error::InvalidQuery(format!(
                "Cannot cast {:?} to {:?}",
                v.data_type(),
                t
            ))),
        };

        if safe {
            Ok(result.unwrap_or(Value::Null))
        } else {
            result
        }
    }

    fn eval_is_null(&self, expr: &Expr, negated: bool, record: &Record) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let is_null = val.is_null();
        Ok(Value::Bool(if negated { !is_null } else { is_null }))
    }

    fn eval_is_distinct_from(
        &self,
        left: &Expr,
        right: &Expr,
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let left_val = self.evaluate(left, record)?;
        let right_val = self.evaluate(right, record)?;

        let both_null = left_val.is_null() && right_val.is_null();
        let either_null = left_val.is_null() || right_val.is_null();

        let is_distinct = if both_null {
            false
        } else if either_null {
            true
        } else {
            !self.values_equal(&left_val, &right_val)
        };

        Ok(Value::Bool(if negated {
            !is_distinct
        } else {
            is_distinct
        }))
    }

    fn eval_in_list(
        &self,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(Value::Null);
        }

        let mut has_null = false;
        for item in list {
            let item_val = self.evaluate(item, record)?;
            if item_val.is_null() {
                has_null = true;
            } else if self.values_equal(&val, &item_val) {
                return Ok(Value::Bool(!negated));
            }
        }

        if has_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Bool(negated))
        }
    }

    fn eval_in_unnest(
        &self,
        expr: &Expr,
        array_expr: &Expr,
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let array_val = self.evaluate(array_expr, record)?;

        match array_val {
            Value::Array(elements) => {
                if val.is_null() {
                    return Ok(Value::Null);
                }
                let mut has_null = false;
                for elem in &elements {
                    if elem.is_null() {
                        has_null = true;
                    } else if self.values_equal(&val, elem) {
                        return Ok(Value::Bool(!negated));
                    }
                }
                if has_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(negated))
                }
            }
            Value::Null => Ok(Value::Null),
            _ => Err(Error::InvalidQuery(
                "IN UNNEST requires array argument".into(),
            )),
        }
    }

    fn eval_between(
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
            return Ok(Value::Null);
        }

        let ge_low = self.compare_values(&val, &low_val, |ord| ord.is_ge())?;
        let le_high = self.compare_values(&val, &high_val, |ord| ord.is_le())?;

        match (ge_low, le_high) {
            (Value::Bool(g), Value::Bool(l)) => {
                Ok(Value::Bool(if negated { !(g && l) } else { g && l }))
            }
            _ => Ok(Value::Null),
        }
    }

    fn eval_like(
        &self,
        expr: &Expr,
        pattern: &Expr,
        negated: bool,
        case_insensitive: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let pat = self.evaluate(pattern, record)?;

        match (&val, &pat) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(p)) => {
                let regex_pattern = like_pattern_to_regex(p);
                let regex = if case_insensitive {
                    regex::Regex::new(&format!("(?i)^{}$", regex_pattern))
                } else {
                    regex::Regex::new(&format!("^{}$", regex_pattern))
                }
                .map_err(|e| Error::InvalidQuery(format!("Invalid LIKE pattern: {}", e)))?;
                let matches = regex.is_match(s);
                Ok(Value::Bool(if negated { !matches } else { matches }))
            }
            _ => Err(Error::InvalidQuery("LIKE requires string arguments".into())),
        }
    }

    fn eval_extract(&self, field: DateTimeField, expr: &Expr, record: &Record) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        extract_datetime_field(&val, field)
    }

    fn eval_substring(
        &self,
        expr: &Expr,
        start: Option<&Expr>,
        length: Option<&Expr>,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let start_val = start.map(|e| self.evaluate(e, record)).transpose()?;
        let len_val = length.map(|e| self.evaluate(e, record)).transpose()?;

        match val {
            Value::Null => Ok(Value::Null),
            Value::String(s) => {
                let chars: Vec<char> = s.chars().collect();
                let char_len = chars.len();
                let start_raw = start_val.and_then(|v| v.as_i64()).unwrap_or(1);

                let start_idx = if start_raw < 0 {
                    char_len.saturating_sub((-start_raw) as usize)
                } else if start_raw == 0 {
                    0
                } else {
                    (start_raw as usize).saturating_sub(1).min(char_len)
                };

                let len = len_val
                    .and_then(|v| v.as_i64())
                    .map(|n| n.max(0) as usize)
                    .unwrap_or(char_len.saturating_sub(start_idx));
                let result: String = chars.into_iter().skip(start_idx).take(len).collect();
                Ok(Value::String(result))
            }
            Value::Bytes(b) => {
                let byte_len = b.len();
                let start_raw = start_val.and_then(|v| v.as_i64()).unwrap_or(1);

                let start_idx = if start_raw < 0 {
                    byte_len.saturating_sub((-start_raw) as usize)
                } else if start_raw == 0 {
                    0
                } else {
                    (start_raw as usize).saturating_sub(1).min(byte_len)
                };

                let len = len_val
                    .and_then(|v| v.as_i64())
                    .map(|n| n.max(0) as usize)
                    .unwrap_or(byte_len.saturating_sub(start_idx));
                let end_idx = (start_idx + len).min(byte_len);
                let result: Vec<u8> = b[start_idx..end_idx].to_vec();
                Ok(Value::Bytes(result))
            }
            _ => Err(Error::InvalidQuery(
                "SUBSTRING requires string or bytes argument".into(),
            )),
        }
    }

    fn eval_trim(
        &self,
        expr: &Expr,
        trim_what: Option<&Expr>,
        trim_where: TrimWhere,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let trim_chars = trim_what
            .map(|e| self.evaluate(e, record))
            .transpose()?
            .and_then(|v| v.as_str().map(|s| s.to_string()));

        match val {
            Value::Null => Ok(Value::Null),
            Value::String(s) => {
                let chars: Vec<char> = trim_chars
                    .as_ref()
                    .map(|t| t.chars().collect())
                    .unwrap_or_else(|| vec![' ']);
                let result = match trim_where {
                    TrimWhere::Both => s.trim_matches(|c| chars.contains(&c)).to_string(),
                    TrimWhere::Leading => s.trim_start_matches(|c| chars.contains(&c)).to_string(),
                    TrimWhere::Trailing => s.trim_end_matches(|c| chars.contains(&c)).to_string(),
                };
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery("TRIM requires string argument".into())),
        }
    }

    fn eval_position(&self, substr: &Expr, string: &Expr, record: &Record) -> Result<Value> {
        let substr_val = self.evaluate(substr, record)?;
        let string_val = self.evaluate(string, record)?;

        match (&substr_val, &string_val) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(sub), Value::String(s)) => {
                let pos = s.find(sub).map(|i| i + 1).unwrap_or(0);
                Ok(Value::Int64(pos as i64))
            }
            _ => Err(Error::InvalidQuery(
                "POSITION requires string arguments".into(),
            )),
        }
    }

    fn eval_overlay(
        &self,
        expr: &Expr,
        overlay_what: &Expr,
        overlay_from: &Expr,
        overlay_for: Option<&Expr>,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let replacement = self.evaluate(overlay_what, record)?;
        let start = self.evaluate(overlay_from, record)?;
        let len = overlay_for.map(|e| self.evaluate(e, record)).transpose()?;

        match (&val, &replacement, &start) {
            (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(r), Value::Int64(start_pos)) => {
                let start_idx = (*start_pos - 1).max(0) as usize;
                let replace_len = len
                    .and_then(|v| v.as_i64())
                    .map(|n| n.max(0) as usize)
                    .unwrap_or(r.len());
                let chars: Vec<char> = s.chars().collect();
                let mut result = String::new();
                result.extend(chars.iter().take(start_idx));
                result.push_str(r);
                result.extend(chars.iter().skip(start_idx + replace_len));
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery(
                "OVERLAY requires string arguments".into(),
            )),
        }
    }

    fn eval_array(&self, elements: &[Expr], record: &Record) -> Result<Value> {
        let values: Result<Vec<_>> = elements.iter().map(|e| self.evaluate(e, record)).collect();
        Ok(Value::Array(values?))
    }

    fn eval_array_access(&self, array: &Expr, index: &Expr, record: &Record) -> Result<Value> {
        let array_val = self.evaluate(array, record)?;

        match index {
            Expr::ScalarFunction {
                name: ScalarFunction::SafeOffset,
                args,
            } if args.len() == 1 => {
                let idx_val = self.evaluate(&args[0], record)?;
                match (&array_val, &idx_val) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Array(elements), Value::Int64(idx)) => {
                        if *idx < 0 || *idx as usize >= elements.len() {
                            Ok(Value::Null)
                        } else {
                            Ok(elements[*idx as usize].clone())
                        }
                    }
                    _ => Err(Error::InvalidQuery(
                        "Array access requires array and integer index".into(),
                    )),
                }
            }
            Expr::ScalarFunction {
                name: ScalarFunction::SafeOrdinal,
                args,
            } if args.len() == 1 => {
                let idx_val = self.evaluate(&args[0], record)?;
                match (&array_val, &idx_val) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Array(elements), Value::Int64(idx)) => {
                        let zero_based = idx - 1;
                        if zero_based < 0 || zero_based as usize >= elements.len() {
                            Ok(Value::Null)
                        } else {
                            Ok(elements[zero_based as usize].clone())
                        }
                    }
                    _ => Err(Error::InvalidQuery(
                        "Array access requires array and integer index".into(),
                    )),
                }
            }
            Expr::ScalarFunction {
                name: ScalarFunction::ArrayOffset,
                args,
            } if args.len() == 1 => {
                let idx_val = self.evaluate(&args[0], record)?;
                match (&array_val, &idx_val) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Array(elements), Value::Int64(idx)) => {
                        if *idx < 0 || *idx as usize >= elements.len() {
                            Err(Error::InvalidQuery(format!(
                                "Array index {} out of bounds for array of length {}",
                                idx,
                                elements.len()
                            )))
                        } else {
                            Ok(elements[*idx as usize].clone())
                        }
                    }
                    _ => Err(Error::InvalidQuery(
                        "Array access requires array and integer index".into(),
                    )),
                }
            }
            Expr::ScalarFunction {
                name: ScalarFunction::ArrayOrdinal,
                args,
            } if args.len() == 1 => {
                let idx_val = self.evaluate(&args[0], record)?;
                match (&array_val, &idx_val) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Array(elements), Value::Int64(idx)) => {
                        let zero_based = idx - 1;
                        if zero_based < 0 || zero_based as usize >= elements.len() {
                            Err(Error::InvalidQuery(format!(
                                "Array index {} out of bounds for array of length {}",
                                idx,
                                elements.len()
                            )))
                        } else {
                            Ok(elements[zero_based as usize].clone())
                        }
                    }
                    _ => Err(Error::InvalidQuery(
                        "Array access requires array and integer index".into(),
                    )),
                }
            }
            _ => {
                let index_val = self.evaluate(index, record)?;
                match (&array_val, &index_val) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Array(elements), Value::Int64(idx)) => {
                        let actual_idx = if *idx < 0 {
                            (elements.len() as i64 + idx) as usize
                        } else if *idx == 0 {
                            return Err(Error::InvalidQuery(
                                "Array index 0 is not valid, arrays are 1-indexed".into(),
                            ));
                        } else {
                            (*idx - 1) as usize
                        };
                        Ok(elements.get(actual_idx).cloned().unwrap_or(Value::Null))
                    }
                    (Value::Json(json), Value::Int64(idx)) => {
                        let result = json
                            .get(*idx as usize)
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        Ok(Value::Json(result))
                    }
                    (Value::Json(json), Value::String(key)) => {
                        let result = json.get(key).cloned().unwrap_or(serde_json::Value::Null);
                        Ok(Value::Json(result))
                    }
                    _ => Err(Error::InvalidQuery(
                        "Array access requires array and integer index".into(),
                    )),
                }
            }
        }
    }

    fn eval_struct(&self, fields: &[(Option<String>, Expr)], record: &Record) -> Result<Value> {
        let struct_fields: Result<Vec<_>> = fields
            .iter()
            .enumerate()
            .map(|(i, (name, expr))| {
                let val = self.evaluate(expr, record)?;
                let field_name = name.clone().unwrap_or_else(|| format!("_field{}", i));
                Ok((field_name, val))
            })
            .collect();
        Ok(Value::Struct(struct_fields?))
    }

    fn eval_struct_access(&self, expr: &Expr, field: &str, record: &Record) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        match val {
            Value::Null => Ok(Value::Null),
            Value::Struct(fields) => {
                let field_lower = field.to_lowercase();
                for (name, value) in &fields {
                    if name.to_lowercase() == field_lower {
                        return Ok(value.clone());
                    }
                }
                Ok(Value::Null)
            }
            _ => Err(Error::InvalidQuery(
                "Struct access requires struct value".into(),
            )),
        }
    }

    fn eval_typed_string(&self, data_type: &DataType, value: &str) -> Result<Value> {
        match data_type {
            DataType::Date => parse_date_string(value),
            DataType::Time => parse_time_string(value),
            DataType::Timestamp => parse_timestamp_string(value),
            DataType::DateTime => parse_datetime_string(value),
            DataType::String => Ok(Value::String(value.to_string())),
            DataType::Bytes => Ok(Value::Bytes(value.as_bytes().to_vec())),
            DataType::Json => {
                let json_val: serde_json::Value = serde_json::from_str(value)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
                Ok(Value::Json(json_val))
            }
            DataType::Numeric(_) | DataType::BigNumeric => {
                let decimal: rust_decimal::Decimal = value
                    .parse()
                    .map_err(|e| Error::InvalidQuery(format!("Invalid NUMERIC: {}", e)))?;
                Ok(Value::Numeric(decimal))
            }
            DataType::Int64 => {
                let i: i64 = value
                    .parse()
                    .map_err(|e| Error::InvalidQuery(format!("Invalid INT64: {}", e)))?;
                Ok(Value::Int64(i))
            }
            DataType::Float64 => {
                let f: f64 = value
                    .parse()
                    .map_err(|e| Error::InvalidQuery(format!("Invalid FLOAT64: {}", e)))?;
                Ok(Value::Float64(OrderedFloat(f)))
            }
            DataType::Bool => {
                let b: bool = value
                    .parse()
                    .map_err(|e| Error::InvalidQuery(format!("Invalid BOOL: {}", e)))?;
                Ok(Value::Bool(b))
            }
            DataType::Range(inner_type) => parse_range_string(value, inner_type),
            DataType::Unknown
            | DataType::Geography
            | DataType::Struct(_)
            | DataType::Array(_)
            | DataType::Interval => {
                panic!(
                    "[ir_evaluator::eval_typed_string] Unsupported TypedString for data type: {:?}",
                    data_type
                )
            }
        }
    }

    fn eval_interval(
        &self,
        value: &Expr,
        leading_field: Option<DateTimeField>,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(value, record)?;
        match val {
            Value::Int64(n) => {
                let field = leading_field.unwrap_or(DateTimeField::Second);
                interval_from_field(n, field)
            }
            Value::String(s) => parse_interval_string(&s),
            _ => Err(Error::InvalidQuery(
                "INTERVAL requires integer or string value".into(),
            )),
        }
    }

    fn add_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a + b)),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 + b.0))),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 + b.0)))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(a.0 + *b as f64)))
            }
            (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Numeric(*a + *b)),
            (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
            (Value::Date(d), Value::Interval(interval)) => {
                let new_date = add_interval_to_date(d, interval)?;
                Ok(Value::Date(new_date))
            }
            (Value::Interval(interval), Value::Date(d)) => {
                let new_date = add_interval_to_date(d, interval)?;
                Ok(Value::Date(new_date))
            }
            (Value::DateTime(dt), Value::Interval(interval)) => {
                let new_dt = add_interval_to_datetime(dt, interval)?;
                Ok(Value::DateTime(new_dt))
            }
            (Value::Interval(interval), Value::DateTime(dt)) => {
                let new_dt = add_interval_to_datetime(dt, interval)?;
                Ok(Value::DateTime(new_dt))
            }
            (Value::Timestamp(ts), Value::Interval(interval)) => {
                let new_dt = add_interval_to_datetime(&ts.naive_utc(), interval)?;
                Ok(Value::Timestamp(new_dt.and_utc()))
            }
            (Value::Interval(interval), Value::Timestamp(ts)) => {
                let new_dt = add_interval_to_datetime(&ts.naive_utc(), interval)?;
                Ok(Value::Timestamp(new_dt.and_utc()))
            }
            (Value::Interval(a), Value::Interval(b)) => Ok(Value::Interval(IntervalValue {
                months: a.months + b.months,
                days: a.days + b.days,
                nanos: a.nanos + b.nanos,
            })),
            _ => Err(Error::InvalidQuery(format!(
                "Cannot add {:?} and {:?}",
                left, right
            ))),
        }
    }

    fn sub_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a - b)),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 - b.0))),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 - b.0)))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(a.0 - *b as f64)))
            }
            (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Numeric(*a - *b)),
            (Value::Date(d), Value::Interval(interval)) => {
                let neg_interval = negate_interval(interval);
                let new_date = add_interval_to_date(d, &neg_interval)?;
                Ok(Value::Date(new_date))
            }
            (Value::DateTime(dt), Value::Interval(interval)) => {
                let neg_interval = negate_interval(interval);
                let new_dt = add_interval_to_datetime(dt, &neg_interval)?;
                Ok(Value::DateTime(new_dt))
            }
            (Value::Timestamp(ts), Value::Interval(interval)) => {
                let neg_interval = negate_interval(interval);
                let new_dt = add_interval_to_datetime(&ts.naive_utc(), &neg_interval)?;
                Ok(Value::Timestamp(new_dt.and_utc()))
            }
            (Value::Interval(a), Value::Interval(b)) => Ok(Value::Interval(IntervalValue {
                months: a.months - b.months,
                days: a.days - b.days,
                nanos: a.nanos - b.nanos,
            })),
            (Value::Float64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a.0 - b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot subtract {:?} from {:?}",
                        right, left
                    )))
                }
            }
            (Value::String(s), Value::Float64(b)) => {
                if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a - b.0)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot subtract {:?} from {:?}",
                        right, left
                    )))
                }
            }
            (Value::Int64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<i64>() {
                    Ok(Value::Int64(a - b))
                } else if let Ok(b) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(*a as f64 - b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot subtract {:?} from {:?}",
                        right, left
                    )))
                }
            }
            (Value::String(s), Value::Int64(b)) => {
                if let Ok(a) = s.parse::<i64>() {
                    Ok(Value::Int64(a - b))
                } else if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a - *b as f64)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot subtract {:?} from {:?}",
                        right, left
                    )))
                }
            }
            (Value::String(s1), Value::String(s2)) => {
                if let (Ok(a), Ok(b)) = (s1.parse::<f64>(), s2.parse::<f64>()) {
                    Ok(Value::Float64(OrderedFloat(a - b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot subtract {:?} from {:?}",
                        right, left
                    )))
                }
            }
            _ => Err(Error::InvalidQuery(format!(
                "Cannot subtract {:?} from {:?}",
                right, left
            ))),
        }
    }

    fn mul_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a * b)),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 * b.0))),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 * b.0)))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(a.0 * *b as f64)))
            }
            (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Numeric(*a * *b)),
            (Value::Interval(iv), Value::Int64(n)) => Ok(Value::Interval(IntervalValue {
                months: iv.months * (*n as i32),
                days: iv.days * (*n as i32),
                nanos: iv.nanos * *n,
            })),
            (Value::Int64(n), Value::Interval(iv)) => Ok(Value::Interval(IntervalValue {
                months: iv.months * (*n as i32),
                days: iv.days * (*n as i32),
                nanos: iv.nanos * *n,
            })),
            (Value::String(s1), Value::String(s2)) => {
                if let (Ok(a), Ok(b)) = (s1.parse::<f64>(), s2.parse::<f64>()) {
                    Ok(Value::Float64(OrderedFloat(a * b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot multiply {:?} and {:?}",
                        left, right
                    )))
                }
            }
            (Value::String(s), Value::Int64(b)) => {
                if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a * *b as f64)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot multiply {:?} and {:?}",
                        left, right
                    )))
                }
            }
            (Value::Int64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(*a as f64 * b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot multiply {:?} and {:?}",
                        left, right
                    )))
                }
            }
            (Value::String(s), Value::Float64(b)) => {
                if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a * b.0)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot multiply {:?} and {:?}",
                        left, right
                    )))
                }
            }
            (Value::Float64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a.0 * b)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot multiply {:?} and {:?}",
                        left, right
                    )))
                }
            }
            _ => Err(Error::InvalidQuery(format!(
                "Cannot multiply {:?} and {:?}",
                left, right
            ))),
        }
    }

    fn div_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (_, Value::Int64(0)) => Err(Error::InvalidQuery("Division by zero".into())),
            (_, Value::Float64(f)) if f.0 == 0.0 => {
                Err(Error::InvalidQuery("Division by zero".into()))
            }
            (Value::Int64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 / *b as f64)))
            }
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 / b.0))),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 / b.0)))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(a.0 / *b as f64)))
            }
            (Value::Numeric(a), Value::Numeric(b)) => {
                if b.is_zero() {
                    Err(Error::InvalidQuery("Division by zero".into()))
                } else {
                    Ok(Value::Numeric(*a / *b))
                }
            }
            (Value::Int64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<f64>() {
                    if b == 0.0 {
                        Err(Error::InvalidQuery("Division by zero".into()))
                    } else {
                        Ok(Value::Float64(OrderedFloat(*a as f64 / b)))
                    }
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot divide {:?} by {:?}",
                        left, right
                    )))
                }
            }
            (Value::String(s), Value::Int64(b)) => {
                if *b == 0 {
                    return Err(Error::InvalidQuery("Division by zero".into()));
                }
                if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a / *b as f64)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot divide {:?} by {:?}",
                        left, right
                    )))
                }
            }
            (Value::Float64(a), Value::String(s)) => {
                if let Ok(b) = s.parse::<f64>() {
                    if b == 0.0 {
                        Err(Error::InvalidQuery("Division by zero".into()))
                    } else {
                        Ok(Value::Float64(OrderedFloat(a.0 / b)))
                    }
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot divide {:?} by {:?}",
                        left, right
                    )))
                }
            }
            (Value::String(s), Value::Float64(b)) => {
                if b.0 == 0.0 {
                    return Err(Error::InvalidQuery("Division by zero".into()));
                }
                if let Ok(a) = s.parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(a / b.0)))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot divide {:?} by {:?}",
                        left, right
                    )))
                }
            }
            (Value::String(s1), Value::String(s2)) => {
                if let (Ok(a), Ok(b)) = (s1.parse::<f64>(), s2.parse::<f64>()) {
                    if b == 0.0 {
                        Err(Error::InvalidQuery("Division by zero".into()))
                    } else {
                        Ok(Value::Float64(OrderedFloat(a / b)))
                    }
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Cannot divide {:?} by {:?}",
                        left, right
                    )))
                }
            }
            _ => Err(Error::InvalidQuery(format!(
                "Cannot divide {:?} by {:?}",
                left, right
            ))),
        }
    }

    fn mod_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (_, Value::Int64(0)) => Err(Error::InvalidQuery("Modulo by zero".into())),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a % b)),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 % b.0))),
            (Value::Numeric(a), Value::Numeric(b)) => {
                if b.is_zero() {
                    Err(Error::InvalidQuery("Modulo by zero".into()))
                } else {
                    Ok(Value::Numeric(*a % *b))
                }
            }
            _ => Err(Error::InvalidQuery(format!(
                "Cannot compute modulo of {:?} and {:?}",
                left, right
            ))),
        }
    }

    fn values_equal(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => false,
            (Value::Int64(a), Value::Float64(b)) => (*a as f64 - b.0).abs() < f64::EPSILON,
            (Value::Float64(a), Value::Int64(b)) => (a.0 - *b as f64).abs() < f64::EPSILON,
            _ => left == right,
        }
    }

    fn eq_values(&self, left: &Value, right: &Value) -> Result<Value> {
        self.eq_values_with_collation(left, right, None)
    }

    fn eq_values_with_collation(
        &self,
        left: &Value,
        right: &Value,
        collation: Option<&str>,
    ) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Bool((*a as f64 - b.0).abs() < f64::EPSILON))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Bool((a.0 - *b as f64).abs() < f64::EPSILON))
            }
            (Value::String(a), Value::String(b)) if matches!(collation, Some("und:ci")) => {
                Ok(Value::Bool(a.to_lowercase() == b.to_lowercase()))
            }
            _ => Ok(Value::Bool(left == right)),
        }
    }

    fn neq_values(&self, left: &Value, right: &Value) -> Result<Value> {
        self.neq_values_with_collation(left, right, None)
    }

    fn neq_values_with_collation(
        &self,
        left: &Value,
        right: &Value,
        collation: Option<&str>,
    ) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Bool((*a as f64 - b.0).abs() >= f64::EPSILON))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Bool((a.0 - *b as f64).abs() >= f64::EPSILON))
            }
            (Value::String(a), Value::String(b)) if matches!(collation, Some("und:ci")) => {
                Ok(Value::Bool(a.to_lowercase() != b.to_lowercase()))
            }
            _ => Ok(Value::Bool(left != right)),
        }
    }

    fn compare_values<F>(&self, left: &Value, right: &Value, cmp: F) -> Result<Value>
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Bool(cmp(a
                .partial_cmp(b)
                .unwrap_or(std::cmp::Ordering::Equal)))),
            (Value::Int64(a), Value::Float64(b)) => {
                let af = *a as f64;
                Ok(Value::Bool(cmp(af
                    .partial_cmp(&b.0)
                    .unwrap_or(std::cmp::Ordering::Equal))))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                let bf = *b as f64;
                Ok(Value::Bool(cmp(a
                    .0
                    .partial_cmp(&bf)
                    .unwrap_or(std::cmp::Ordering::Equal))))
            }
            (Value::String(a), Value::String(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Bytes(a), Value::Bytes(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Date(a), Value::Date(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Time(a), Value::Time(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::DateTime(a), Value::DateTime(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Timestamp(a), Value::Timestamp(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Numeric(a), Value::Numeric(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Numeric(a), Value::Int64(b)) => {
                let b_decimal = rust_decimal::Decimal::from(*b);
                Ok(Value::Bool(cmp(a.cmp(&b_decimal))))
            }
            (Value::Int64(a), Value::Numeric(b)) => {
                let a_decimal = rust_decimal::Decimal::from(*a);
                Ok(Value::Bool(cmp(a_decimal.cmp(b))))
            }
            (Value::Numeric(a), Value::Float64(b)) => {
                if let Some(a_f64) = a.to_f64() {
                    Ok(Value::Bool(cmp(a_f64
                        .partial_cmp(&b.0)
                        .unwrap_or(std::cmp::Ordering::Equal))))
                } else {
                    Ok(Value::Null)
                }
            }
            (Value::Float64(a), Value::Numeric(b)) => {
                if let Some(b_f64) = b.to_f64() {
                    Ok(Value::Bool(cmp(a
                        .0
                        .partial_cmp(&b_f64)
                        .unwrap_or(std::cmp::Ordering::Equal))))
                } else {
                    Ok(Value::Null)
                }
            }
            (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(cmp(a.cmp(b)))),
            (Value::Interval(a), Value::Interval(b)) => {
                let a_total_nanos = a.nanos
                    + a.days as i64 * 86_400_000_000_000
                    + a.months as i64 * 30 * 86_400_000_000_000;
                let b_total_nanos = b.nanos
                    + b.days as i64 * 86_400_000_000_000
                    + b.months as i64 * 30 * 86_400_000_000_000;
                Ok(Value::Bool(cmp(a_total_nanos.cmp(&b_total_nanos))))
            }
            _ => Err(Error::InvalidQuery(format!(
                "Cannot compare {:?} with {:?}",
                left, right
            ))),
        }
    }

    fn and_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
            (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
            (Value::Bool(true), Value::Null) | (Value::Null, Value::Bool(true)) => Ok(Value::Null),
            (Value::Null, Value::Null) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery("AND requires boolean operands".into())),
        }
    }

    fn or_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
            (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
            (Value::Bool(false), Value::Null) | (Value::Null, Value::Bool(false)) => {
                Ok(Value::Null)
            }
            (Value::Null, Value::Null) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery("OR requires boolean operands".into())),
        }
    }

    fn concat_values(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
            (Value::Array(a), Value::Array(b)) => {
                let mut result = a.clone();
                result.extend(b.clone());
                Ok(Value::Array(result))
            }
            _ => Err(Error::InvalidQuery(
                "Concat requires string or array operands".into(),
            )),
        }
    }

    fn bitwise_and(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a & b)),
            _ => Err(Error::InvalidQuery(
                "Bitwise AND requires integer operands".into(),
            )),
        }
    }

    fn bitwise_or(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a | b)),
            _ => Err(Error::InvalidQuery(
                "Bitwise OR requires integer operands".into(),
            )),
        }
    }

    fn bitwise_xor(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a ^ b)),
            _ => Err(Error::InvalidQuery(
                "Bitwise XOR requires integer operands".into(),
            )),
        }
    }

    fn shift_left(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a << b)),
            _ => Err(Error::InvalidQuery(
                "Shift left requires integer operands".into(),
            )),
        }
    }

    fn shift_right(&self, left: &Value, right: &Value) -> Result<Value> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a >> b)),
            _ => Err(Error::InvalidQuery(
                "Shift right requires integer operands".into(),
            )),
        }
    }

    fn fn_upper(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::String(s.to_uppercase())),
            _ => Err(Error::InvalidQuery("UPPER requires string argument".into())),
        }
    }

    fn fn_lower(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::String(s.to_lowercase())),
            _ => Err(Error::InvalidQuery("LOWER requires string argument".into())),
        }
    }

    fn fn_length(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::Int64(s.chars().count() as i64)),
            Some(Value::Bytes(b)) => Ok(Value::Int64(b.len() as i64)),
            Some(Value::Array(a)) => Ok(Value::Int64(a.len() as i64)),
            _ => Err(Error::InvalidQuery(
                "LENGTH requires string, bytes, or array argument".into(),
            )),
        }
    }

    fn fn_coalesce(&self, args: &[Value]) -> Result<Value> {
        for arg in args {
            if !arg.is_null() {
                return Ok(arg.clone());
            }
        }
        Ok(Value::Null)
    }

    fn fn_ifnull(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("IFNULL requires 2 arguments".into()));
        }
        if args[0].is_null() {
            Ok(args[1].clone())
        } else {
            Ok(args[0].clone())
        }
    }

    fn fn_nullif(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("NULLIF requires 2 arguments".into()));
        }
        if self.values_equal(&args[0], &args[1]) {
            Ok(Value::Null)
        } else {
            Ok(args[0].clone())
        }
    }

    fn fn_if(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery("IF requires 3 arguments".into()));
        }
        match &args[0] {
            Value::Bool(true) => Ok(args[1].clone()),
            Value::Bool(false) | Value::Null => Ok(args[2].clone()),
            _ => Err(Error::InvalidQuery("IF requires boolean condition".into())),
        }
    }

    fn fn_abs(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Int64(n.abs())),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.abs()))),
            Some(Value::Numeric(d)) => Ok(Value::Numeric(d.abs())),
            _ => Err(Error::InvalidQuery("ABS requires numeric argument".into())),
        }
    }

    fn fn_floor(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Int64(*n)),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.floor()))),
            Some(Value::Numeric(d)) => Ok(Value::Numeric(d.floor())),
            _ => Err(Error::InvalidQuery(
                "FLOOR requires numeric argument".into(),
            )),
        }
    }

    fn fn_ceil(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Int64(*n)),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.ceil()))),
            Some(Value::Numeric(d)) => Ok(Value::Numeric(d.ceil())),
            _ => Err(Error::InvalidQuery("CEIL requires numeric argument".into())),
        }
    }

    fn fn_round(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "ROUND requires at least 1 argument".into(),
            ));
        }
        let precision = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Int64(n) => Ok(Value::Int64(*n)),
            Value::Float64(f) => {
                let mult = 10f64.powi(precision as i32);
                Ok(Value::Float64(OrderedFloat((f.0 * mult).round() / mult)))
            }
            Value::Numeric(d) => Ok(Value::Numeric(d.round_dp(precision.max(0) as u32))),
            _ => Err(Error::InvalidQuery(
                "ROUND requires numeric argument".into(),
            )),
        }
    }

    fn fn_sqrt(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).sqrt()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.sqrt()))),
            _ => Err(Error::InvalidQuery("SQRT requires numeric argument".into())),
        }
    }

    fn fn_cbrt(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).cbrt()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.cbrt()))),
            _ => Err(Error::InvalidQuery("CBRT requires numeric argument".into())),
        }
    }

    fn fn_power(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("POWER requires 2 arguments".into()));
        }
        let base = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Int64(n) => *n as f64,
            Value::Float64(f) => f.0,
            _ => {
                return Err(Error::InvalidQuery(
                    "POWER requires numeric arguments".into(),
                ));
            }
        };
        let exp = match &args[1] {
            Value::Null => return Ok(Value::Null),
            Value::Int64(n) => *n as f64,
            Value::Float64(f) => f.0,
            _ => {
                return Err(Error::InvalidQuery(
                    "POWER requires numeric arguments".into(),
                ));
            }
        };
        Ok(Value::Float64(OrderedFloat(base.powf(exp))))
    }

    fn fn_mod(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("MOD requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => {
                if *b == 0 {
                    return Err(Error::InvalidQuery("Division by zero".into()));
                }
                Ok(Value::Int64(a % b))
            }
            (Value::Float64(a), Value::Float64(b)) => {
                if b.0 == 0.0 {
                    return Err(Error::InvalidQuery("Division by zero".into()));
                }
                Ok(Value::Float64(OrderedFloat(a.0 % b.0)))
            }
            _ => Err(Error::InvalidQuery("MOD requires numeric arguments".into())),
        }
    }

    fn fn_sign(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Int64(n.signum())),
            Some(Value::Float64(f)) => Ok(Value::Int64(if f.0 > 0.0 {
                1
            } else if f.0 < 0.0 {
                -1
            } else {
                0
            })),
            _ => Err(Error::InvalidQuery("SIGN requires numeric argument".into())),
        }
    }

    fn fn_exp(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).exp()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.exp()))),
            _ => Err(Error::InvalidQuery("EXP requires numeric argument".into())),
        }
    }

    fn fn_ln(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).ln()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.ln()))),
            _ => Err(Error::InvalidQuery("LN requires numeric argument".into())),
        }
    }

    fn fn_log(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "LOG requires at least 1 argument".into(),
            ));
        }
        let val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Int64(n) => *n as f64,
            Value::Float64(f) => f.0,
            _ => return Err(Error::InvalidQuery("LOG requires numeric argument".into())),
        };
        let base = args
            .get(1)
            .map(|v| match v {
                Value::Int64(n) => *n as f64,
                Value::Float64(f) => f.0,
                _ => 10.0,
            })
            .unwrap_or(10.0);
        Ok(Value::Float64(OrderedFloat(val.log(base))))
    }

    fn fn_log10(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).log10()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.log10()))),
            _ => Err(Error::InvalidQuery(
                "LOG10 requires numeric argument".into(),
            )),
        }
    }

    fn fn_greatest(&self, args: &[Value]) -> Result<Value> {
        if args.iter().any(|v| v.is_null()) {
            return Ok(Value::Null);
        }
        let mut max: Option<Value> = None;
        for arg in args {
            max = Some(match max {
                None => arg.clone(),
                Some(m) => {
                    if arg > &m {
                        arg.clone()
                    } else {
                        m
                    }
                }
            });
        }
        Ok(max.unwrap_or(Value::Null))
    }

    fn fn_least(&self, args: &[Value]) -> Result<Value> {
        if args.iter().any(|v| v.is_null()) {
            return Ok(Value::Null);
        }
        let mut min: Option<Value> = None;
        for arg in args {
            min = Some(match min {
                None => arg.clone(),
                Some(m) => {
                    if arg < &m {
                        arg.clone()
                    } else {
                        m
                    }
                }
            });
        }
        Ok(min.unwrap_or(Value::Null))
    }

    fn fn_trunc(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "TRUNC requires at least 1 argument".into(),
            ));
        }
        let precision = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Int64(n) => Ok(Value::Int64(*n)),
            Value::Float64(f) => {
                let mult = 10f64.powi(precision as i32);
                Ok(Value::Float64(OrderedFloat((f.0 * mult).trunc() / mult)))
            }
            Value::Numeric(d) => Ok(Value::Numeric(d.trunc_with_scale(precision.max(0) as u32))),
            _ => Err(Error::InvalidQuery(
                "TRUNC requires numeric argument".into(),
            )),
        }
    }

    fn fn_div(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("DIV requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => {
                if *b == 0 {
                    return Err(Error::InvalidQuery("Division by zero".into()));
                }
                Ok(Value::Int64(a / b))
            }
            (Value::Float64(a), Value::Float64(b)) => {
                if b.0 == 0.0 {
                    return Err(Error::InvalidQuery("Division by zero".into()));
                }
                Ok(Value::Int64((a.0 / b.0).trunc() as i64))
            }
            _ => Err(Error::InvalidQuery("DIV requires numeric arguments".into())),
        }
    }

    fn fn_safe_divide(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "SAFE_DIVIDE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(_), Value::Int64(0)) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 / *b as f64)))
            }
            (Value::Float64(_), Value::Float64(b)) if b.0 == 0.0 => Ok(Value::Null),
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 / b.0))),
            (Value::Int64(a), Value::Float64(b)) if b.0 == 0.0 => Ok(Value::Null),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 / b.0)))
            }
            (Value::Float64(a), Value::Int64(0)) => Ok(Value::Null),
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(a.0 / *b as f64)))
            }
            _ => Err(Error::InvalidQuery(
                "SAFE_DIVIDE requires numeric arguments".into(),
            )),
        }
    }

    fn fn_ieee_divide(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "IEEE_DIVIDE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => {
                let a = *a as f64;
                let b = *b as f64;
                Ok(Value::Float64(OrderedFloat(a / b)))
            }
            (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(OrderedFloat(a.0 / b.0))),
            (Value::Int64(a), Value::Float64(b)) => {
                Ok(Value::Float64(OrderedFloat(*a as f64 / b.0)))
            }
            (Value::Float64(a), Value::Int64(b)) => {
                Ok(Value::Float64(OrderedFloat(a.0 / *b as f64)))
            }
            _ => Err(Error::InvalidQuery(
                "IEEE_DIVIDE requires numeric arguments".into(),
            )),
        }
    }

    fn fn_safe_multiply(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "SAFE_MULTIPLY requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => {
                Ok(a.checked_mul(*b).map(Value::Int64).unwrap_or(Value::Null))
            }
            (Value::Float64(a), Value::Float64(b)) => {
                let result = a.0 * b.0;
                if result.is_finite() {
                    Ok(Value::Float64(OrderedFloat(result)))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(Error::InvalidQuery(
                "SAFE_MULTIPLY requires numeric arguments".into(),
            )),
        }
    }

    fn fn_safe_add(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("SAFE_ADD requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => {
                Ok(a.checked_add(*b).map(Value::Int64).unwrap_or(Value::Null))
            }
            (Value::Float64(a), Value::Float64(b)) => {
                let result = a.0 + b.0;
                if result.is_finite() {
                    Ok(Value::Float64(OrderedFloat(result)))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(Error::InvalidQuery(
                "SAFE_ADD requires numeric arguments".into(),
            )),
        }
    }

    fn fn_safe_subtract(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "SAFE_SUBTRACT requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => {
                Ok(a.checked_sub(*b).map(Value::Int64).unwrap_or(Value::Null))
            }
            (Value::Float64(a), Value::Float64(b)) => {
                let result = a.0 - b.0;
                if result.is_finite() {
                    Ok(Value::Float64(OrderedFloat(result)))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(Error::InvalidQuery(
                "SAFE_SUBTRACT requires numeric arguments".into(),
            )),
        }
    }

    fn fn_safe_negate(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(n.checked_neg().map(Value::Int64).unwrap_or(Value::Null)),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(-f.0))),
            _ => Err(Error::InvalidQuery(
                "SAFE_NEGATE requires numeric argument".into(),
            )),
        }
    }

    fn fn_concat(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::String(String::new()));
        }
        let first = &args[0];
        let is_bytes = matches!(first, Value::Bytes(_));

        if is_bytes {
            let mut result: Vec<u8> = Vec::new();
            for arg in args {
                match arg {
                    Value::Null => return Ok(Value::Null),
                    Value::Bytes(b) => result.extend(b),
                    Value::Bool(_)
                    | Value::Int64(_)
                    | Value::Float64(_)
                    | Value::Numeric(_)
                    | Value::BigNumeric(_)
                    | Value::String(_)
                    | Value::Date(_)
                    | Value::Time(_)
                    | Value::DateTime(_)
                    | Value::Timestamp(_)
                    | Value::Json(_)
                    | Value::Array(_)
                    | Value::Struct(_)
                    | Value::Geography(_)
                    | Value::Interval(_)
                    | Value::Range(_)
                    | Value::Default => {
                        return Err(Error::InvalidQuery(
                            "CONCAT with BYTES requires all arguments to be BYTES".into(),
                        ));
                    }
                }
            }
            Ok(Value::Bytes(result))
        } else {
            let mut result = String::new();
            for arg in args {
                match arg {
                    Value::Null => return Ok(Value::Null),
                    Value::String(s) => result.push_str(s),
                    Value::Bool(_)
                    | Value::Int64(_)
                    | Value::Float64(_)
                    | Value::Numeric(_)
                    | Value::BigNumeric(_)
                    | Value::Bytes(_)
                    | Value::Date(_)
                    | Value::Time(_)
                    | Value::DateTime(_)
                    | Value::Timestamp(_)
                    | Value::Json(_)
                    | Value::Array(_)
                    | Value::Struct(_)
                    | Value::Geography(_)
                    | Value::Interval(_)
                    | Value::Range(_)
                    | Value::Default => {
                        return Err(Error::InvalidQuery(
                            "CONCAT requires STRING arguments".into(),
                        ));
                    }
                }
            }
            Ok(Value::String(result))
        }
    }

    fn fn_trim(&self, args: &[Value]) -> Result<Value> {
        match args {
            [Value::Null, ..] => Ok(Value::Null),
            [Value::String(s)] => Ok(Value::String(s.trim().to_string())),
            [Value::String(s), Value::String(chars)] => {
                let char_set: std::collections::HashSet<char> = chars.chars().collect();
                let result = s
                    .trim_start_matches(|c| char_set.contains(&c))
                    .trim_end_matches(|c| char_set.contains(&c))
                    .to_string();
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery("TRIM requires string argument".into())),
        }
    }

    fn fn_ltrim(&self, args: &[Value]) -> Result<Value> {
        match args {
            [Value::Null, ..] => Ok(Value::Null),
            [Value::String(s)] => Ok(Value::String(s.trim_start().to_string())),
            [Value::String(s), Value::String(chars)] => {
                let char_set: std::collections::HashSet<char> = chars.chars().collect();
                let result = s.trim_start_matches(|c| char_set.contains(&c)).to_string();
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery("LTRIM requires string argument".into())),
        }
    }

    fn fn_rtrim(&self, args: &[Value]) -> Result<Value> {
        match args {
            [Value::Null, ..] => Ok(Value::Null),
            [Value::String(s)] => Ok(Value::String(s.trim_end().to_string())),
            [Value::String(s), Value::String(chars)] => {
                let char_set: std::collections::HashSet<char> = chars.chars().collect();
                let result = s.trim_end_matches(|c| char_set.contains(&c)).to_string();
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery("RTRIM requires string argument".into())),
        }
    }

    fn fn_substr(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "SUBSTR requires at least 1 argument".into(),
            ));
        }
        let s = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::String(s) => s,
            _ => {
                return Err(Error::InvalidQuery(
                    "SUBSTR requires string argument".into(),
                ));
            }
        };
        let start_raw = args.get(1).and_then(|v| v.as_i64()).unwrap_or(1);
        let len = args.get(2).and_then(|v| v.as_i64()).map(|l| l as usize);
        let chars: Vec<char> = s.chars().collect();
        let char_len = chars.len();

        let start_idx = if start_raw < 0 {
            char_len.saturating_sub((-start_raw) as usize)
        } else if start_raw == 0 {
            0
        } else {
            (start_raw as usize).saturating_sub(1).min(char_len)
        };

        let end_idx = len
            .map(|l| (start_idx + l).min(char_len))
            .unwrap_or(char_len);
        Ok(Value::String(chars[start_idx..end_idx].iter().collect()))
    }

    fn fn_replace(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery("REPLACE requires 3 arguments".into()));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) => Ok(Value::Null),
            (Value::String(s), Value::String(from), Value::String(to)) => {
                if from.is_empty() {
                    Ok(Value::String(s.clone()))
                } else {
                    Ok(Value::String(s.replace(from.as_str(), to.as_str())))
                }
            }
            _ => Err(Error::InvalidQuery(
                "REPLACE requires string arguments".into(),
            )),
        }
    }

    fn fn_reverse(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::String(s.chars().rev().collect())),
            Some(Value::Bytes(b)) => Ok(Value::Bytes(b.iter().rev().cloned().collect())),
            Some(Value::Array(a)) => Ok(Value::Array(a.iter().rev().cloned().collect())),
            _ => Err(Error::InvalidQuery(
                "REVERSE requires string/bytes/array argument".into(),
            )),
        }
    }

    fn fn_left(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("LEFT requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::Int64(n)) => {
                let chars: Vec<char> = s.chars().collect();
                let n = (*n as usize).min(chars.len());
                Ok(Value::String(chars[..n].iter().collect()))
            }
            (Value::Bytes(b), Value::Int64(n)) => {
                let n = (*n as usize).min(b.len());
                Ok(Value::Bytes(b[..n].to_vec()))
            }
            _ => Err(Error::InvalidQuery(
                "LEFT requires string/bytes and int arguments".into(),
            )),
        }
    }

    fn fn_right(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("RIGHT requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::Int64(n)) => {
                let chars: Vec<char> = s.chars().collect();
                let n = (*n as usize).min(chars.len());
                let start = chars.len().saturating_sub(n);
                Ok(Value::String(chars[start..].iter().collect()))
            }
            (Value::Bytes(b), Value::Int64(n)) => {
                let n = (*n as usize).min(b.len());
                let start = b.len().saturating_sub(n);
                Ok(Value::Bytes(b[start..].to_vec()))
            }
            _ => Err(Error::InvalidQuery(
                "RIGHT requires string/bytes and int arguments".into(),
            )),
        }
    }

    fn fn_repeat(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("REPEAT requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::Int64(n)) => Ok(Value::String(s.repeat(*n as usize))),
            _ => Err(Error::InvalidQuery(
                "REPEAT requires string and int arguments".into(),
            )),
        }
    }

    fn fn_starts_with(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "STARTS_WITH requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(prefix)) => {
                Ok(Value::Bool(s.starts_with(prefix.as_str())))
            }
            _ => Err(Error::InvalidQuery(
                "STARTS_WITH requires string arguments".into(),
            )),
        }
    }

    fn fn_ends_with(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("ENDS_WITH requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(suffix)) => {
                Ok(Value::Bool(s.ends_with(suffix.as_str())))
            }
            _ => Err(Error::InvalidQuery(
                "ENDS_WITH requires string arguments".into(),
            )),
        }
    }

    fn fn_contains(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("CONTAINS requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(substr)) => {
                Ok(Value::Bool(s.contains(substr.as_str())))
            }
            _ => Err(Error::InvalidQuery(
                "CONTAINS requires string arguments".into(),
            )),
        }
    }

    fn fn_strpos(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("STRPOS requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(substr)) => Ok(Value::Int64(
                s.find(substr.as_str()).map(|i| i as i64 + 1).unwrap_or(0),
            )),
            _ => Err(Error::InvalidQuery(
                "STRPOS requires string arguments".into(),
            )),
        }
    }

    fn fn_lpad(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "LPAD requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::Int64(n)) => {
                let pad_str = args.get(2).and_then(|v| v.as_str()).unwrap_or(" ");
                let n = *n as usize;
                let s_chars: Vec<char> = s.chars().collect();
                if s_chars.len() >= n {
                    Ok(Value::String(s_chars[..n].iter().collect()))
                } else {
                    let pad_len = n - s_chars.len();
                    let pad_chars: Vec<char> = pad_str.chars().collect();
                    let padded: String = pad_chars
                        .iter()
                        .cycle()
                        .take(pad_len)
                        .chain(s_chars.iter())
                        .collect();
                    Ok(Value::String(padded))
                }
            }
            _ => Err(Error::InvalidQuery(
                "LPAD requires string and int arguments".into(),
            )),
        }
    }

    fn fn_rpad(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "RPAD requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::Int64(n)) => {
                let pad_str = args.get(2).and_then(|v| v.as_str()).unwrap_or(" ");
                let n = *n as usize;
                let s_chars: Vec<char> = s.chars().collect();
                if s_chars.len() >= n {
                    Ok(Value::String(s_chars[..n].iter().collect()))
                } else {
                    let pad_len = n - s_chars.len();
                    let pad_chars: Vec<char> = pad_str.chars().collect();
                    let padded: String = s_chars
                        .iter()
                        .chain(pad_chars.iter().cycle().take(pad_len))
                        .collect();
                    Ok(Value::String(padded))
                }
            }
            _ => Err(Error::InvalidQuery(
                "RPAD requires string and int arguments".into(),
            )),
        }
    }

    fn fn_initcap(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let delimiters: std::collections::HashSet<char> = args
                    .get(1)
                    .and_then(|v| v.as_str())
                    .map(|d| d.chars().collect())
                    .unwrap_or_else(|| " \t\n\r-_!@#$%^&*()+=[]{}|;:',.<>?/~`".chars().collect());

                let mut result = String::new();
                let mut capitalize_next = true;
                for c in s.chars() {
                    if delimiters.contains(&c) {
                        result.push(c);
                        capitalize_next = true;
                    } else if capitalize_next {
                        result.extend(c.to_uppercase());
                        capitalize_next = false;
                    } else {
                        result.extend(c.to_lowercase());
                    }
                }
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery(
                "INITCAP requires string argument".into(),
            )),
        }
    }

    fn fn_datetime(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "DATETIME requires at least 1 argument".into(),
            ));
        }
        if args.len() == 6 {
            let year = args[0]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME year must be int".into()))?;
            let month = args[1]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME month must be int".into()))?;
            let day = args[2]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME day must be int".into()))?;
            let hour = args[3]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME hour must be int".into()))?;
            let minute = args[4]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME minute must be int".into()))?;
            let second = args[5]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("DATETIME second must be int".into()))?;
            let dt = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                .and_then(|d| d.and_hms_opt(hour as u32, minute as u32, second as u32))
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime components".into()))?;
            return Ok(Value::DateTime(dt));
        }
        if args.len() == 2 {
            if let (Value::Date(d), Value::Time(t)) = (&args[0], &args[1]) {
                let dt = chrono::NaiveDateTime::new(*d, *t);
                return Ok(Value::DateTime(dt));
            }
        }
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d"))
                    .map_err(|e| Error::InvalidQuery(format!("Invalid datetime: {}", e)))?;
                Ok(Value::DateTime(dt))
            }
            Some(Value::Date(d)) => Ok(Value::DateTime(d.and_hms_opt(0, 0, 0).unwrap())),
            Some(Value::Timestamp(ts)) => Ok(Value::DateTime(ts.naive_utc())),
            _ => Err(Error::InvalidQuery(
                "DATETIME requires date/string argument".into(),
            )),
        }
    }

    fn fn_date(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|e| Error::InvalidQuery(format!("Invalid date: {}", e)))?;
                Ok(Value::Date(date))
            }
            Some(Value::Timestamp(ts)) => Ok(Value::Date(ts.date_naive())),
            Some(Value::DateTime(dt)) => Ok(Value::Date(dt.date())),
            _ => Err(Error::InvalidQuery(
                "DATE requires date/string argument".into(),
            )),
        }
    }

    fn fn_time(&self, args: &[Value]) -> Result<Value> {
        if args.len() == 3 {
            if args.iter().any(|a| a.is_null()) {
                return Ok(Value::Null);
            }
            let hour = args[0]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("TIME hour must be int".into()))?;
            let minute = args[1]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("TIME minute must be int".into()))?;
            let second = args[2]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("TIME second must be int".into()))?;
            let time = NaiveTime::from_hms_opt(hour as u32, minute as u32, second as u32)
                .ok_or_else(|| Error::InvalidQuery("Invalid time components".into()))?;
            return Ok(Value::Time(time));
        }
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let time = NaiveTime::parse_from_str(s, "%H:%M:%S")
                    .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
                    .map_err(|e| Error::InvalidQuery(format!("Invalid time: {}", e)))?;
                Ok(Value::Time(time))
            }
            Some(Value::Timestamp(ts)) => Ok(Value::Time(ts.time())),
            Some(Value::DateTime(dt)) => Ok(Value::Time(dt.time())),
            _ => Err(Error::InvalidQuery(
                "TIME requires time/string argument".into(),
            )),
        }
    }

    fn fn_timestamp(&self, args: &[Value]) -> Result<Value> {
        if args.len() == 2 {
            match (&args[0], &args[1]) {
                (Value::Null, _) | (_, Value::Null) => return Ok(Value::Null),
                (Value::String(s), Value::String(tz_name)) => {
                    let ndt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                        .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp: {}", e)))?;
                    let tz: chrono_tz::Tz = tz_name.parse().map_err(|_| {
                        Error::InvalidQuery(format!("Invalid timezone: {}", tz_name))
                    })?;
                    let local_dt = ndt.and_local_timezone(tz).single().ok_or_else(|| {
                        Error::InvalidQuery("Ambiguous or invalid local time".into())
                    })?;
                    return Ok(Value::Timestamp(local_dt.with_timezone(&Utc)));
                }
                _ => {
                    return Err(Error::InvalidQuery(
                        "TIMESTAMP with timezone requires (string, string) arguments".into(),
                    ));
                }
            }
        }
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let normalized = if s.ends_with("+00") || s.ends_with("-00") {
                    format!("{}:00", s)
                } else {
                    s.to_string()
                };
                let dt = DateTime::parse_from_rfc3339(&normalized)
                    .map(|d| d.with_timezone(&Utc))
                    .or_else(|_| {
                        DateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%:z")
                            .map(|d| d.with_timezone(&Utc))
                    })
                    .or_else(|_| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                            .or_else(|_| {
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                            })
                            .map(|ndt| ndt.and_utc())
                    })
                    .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp: {}", e)))?;
                Ok(Value::Timestamp(dt))
            }
            Some(Value::DateTime(dt)) => Ok(Value::Timestamp(dt.and_utc())),
            Some(Value::Date(d)) => {
                let ndt = d.and_hms_opt(0, 0, 0).ok_or_else(|| {
                    Error::InvalidQuery("Failed to create timestamp from date".into())
                })?;
                Ok(Value::Timestamp(ndt.and_utc()))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP requires timestamp/string/date argument".into(),
            )),
        }
    }

    fn fn_array_length(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Array(arr)) => Ok(Value::Int64(arr.len() as i64)),
            _ => Err(Error::InvalidQuery(
                "ARRAY_LENGTH requires array argument".into(),
            )),
        }
    }

    fn fn_array_to_string(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ARRAY_TO_STRING requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Array(arr), Value::String(sep)) => {
                let strs: Vec<String> = arr
                    .iter()
                    .filter(|v| !v.is_null())
                    .map(|v| format!("{}", v))
                    .collect();
                Ok(Value::String(strs.join(sep)))
            }
            _ => Err(Error::InvalidQuery(
                "ARRAY_TO_STRING requires array and string arguments".into(),
            )),
        }
    }

    fn fn_generate_array(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "GENERATE_ARRAY requires at least 2 arguments".into(),
            ));
        }
        let start = match &args[0] {
            Value::Int64(n) => *n,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_ARRAY requires integer arguments".into(),
                ));
            }
        };
        let end = match &args[1] {
            Value::Int64(n) => *n,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_ARRAY requires integer arguments".into(),
                ));
            }
        };
        let step = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
        if step == 0 {
            return Err(Error::InvalidQuery(
                "GENERATE_ARRAY step cannot be zero".into(),
            ));
        }
        let mut result = Vec::new();
        let mut i = start;
        if step > 0 {
            while i <= end {
                result.push(Value::Int64(i));
                i += step;
            }
        } else {
            while i >= end {
                result.push(Value::Int64(i));
                i += step;
            }
        }
        Ok(Value::Array(result))
    }

    fn fn_generate_date_array(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "GENERATE_DATE_ARRAY requires at least 2 arguments".into(),
            ));
        }
        let start_date = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Date(d) => *d,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_DATE_ARRAY requires DATE arguments".into(),
                ));
            }
        };
        let end_date = match &args[1] {
            Value::Null => return Ok(Value::Null),
            Value::Date(d) => *d,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_DATE_ARRAY requires DATE arguments".into(),
                ));
            }
        };
        let step_days = match args.get(2) {
            Some(Value::Null) => return Ok(Value::Null),
            Some(Value::Interval(iv)) => iv.days as i64 + iv.months as i64 * 30,
            Some(Value::Int64(n)) => *n,
            None => 1,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_DATE_ARRAY step must be INTERVAL or INT64".into(),
                ));
            }
        };
        if step_days == 0 {
            return Err(Error::InvalidQuery(
                "GENERATE_DATE_ARRAY step cannot be zero".into(),
            ));
        }
        let mut result = Vec::new();
        let mut current = start_date;
        if step_days > 0 {
            while current <= end_date {
                result.push(Value::Date(current));
                current = current + chrono::Days::new(step_days as u64);
            }
        } else {
            let neg_step = (-step_days) as u64;
            while current >= end_date {
                result.push(Value::Date(current));
                current = current - chrono::Days::new(neg_step);
            }
        }
        Ok(Value::Array(result))
    }

    fn fn_generate_timestamp_array(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "GENERATE_TIMESTAMP_ARRAY requires 3 arguments".into(),
            ));
        }
        let start_ts = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Timestamp(ts) => *ts,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_TIMESTAMP_ARRAY requires TIMESTAMP arguments".into(),
                ));
            }
        };
        let end_ts = match &args[1] {
            Value::Null => return Ok(Value::Null),
            Value::Timestamp(ts) => *ts,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_TIMESTAMP_ARRAY requires TIMESTAMP arguments".into(),
                ));
            }
        };
        let step = match &args[2] {
            Value::Null => return Ok(Value::Null),
            Value::Interval(iv) => iv,
            _ => {
                return Err(Error::InvalidQuery(
                    "GENERATE_TIMESTAMP_ARRAY step must be INTERVAL".into(),
                ));
            }
        };
        let step_nanos = step.nanos
            + step.days as i64 * 86_400_000_000_000
            + step.months as i64 * 30 * 86_400_000_000_000;
        if step_nanos == 0 {
            return Err(Error::InvalidQuery(
                "GENERATE_TIMESTAMP_ARRAY step cannot be zero".into(),
            ));
        }
        let mut result = Vec::new();
        let mut current = start_ts;
        if step_nanos > 0 {
            while current <= end_ts {
                result.push(Value::Timestamp(current));
                current += chrono::Duration::nanoseconds(step_nanos);
            }
        } else {
            while current >= end_ts {
                result.push(Value::Timestamp(current));
                current += chrono::Duration::nanoseconds(step_nanos);
            }
        }
        Ok(Value::Array(result))
    }

    fn fn_md5(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let digest = md5::compute(s.as_bytes());
                Ok(Value::Bytes(digest.to_vec()))
            }
            Some(Value::Bytes(b)) => {
                let digest = md5::compute(b);
                Ok(Value::Bytes(digest.to_vec()))
            }
            _ => Err(Error::InvalidQuery("MD5 requires string argument".into())),
        }
    }

    fn fn_sha256(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(s.as_bytes());
                let result = hasher.finalize();
                Ok(Value::Bytes(result.to_vec()))
            }
            Some(Value::Bytes(b)) => {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(b);
                let result = hasher.finalize();
                Ok(Value::Bytes(result.to_vec()))
            }
            _ => Err(Error::InvalidQuery(
                "SHA256 requires string or bytes argument".into(),
            )),
        }
    }

    fn fn_sha512(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                use sha2::{Digest, Sha512};
                let mut hasher = Sha512::new();
                hasher.update(s.as_bytes());
                let result = hasher.finalize();
                Ok(Value::Bytes(result.to_vec()))
            }
            Some(Value::Bytes(b)) => {
                use sha2::{Digest, Sha512};
                let mut hasher = Sha512::new();
                hasher.update(b);
                let result = hasher.finalize();
                Ok(Value::Bytes(result.to_vec()))
            }
            _ => Err(Error::InvalidQuery(
                "SHA512 requires string or bytes argument".into(),
            )),
        }
    }

    fn fn_farm_fingerprint(&self, args: &[Value]) -> Result<Value> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let mut hasher = DefaultHasher::new();
                s.hash(&mut hasher);
                let hash = hasher.finish() as i64;
                Ok(Value::Int64(hash))
            }
            Some(Value::Bytes(b)) => {
                let mut hasher = DefaultHasher::new();
                b.hash(&mut hasher);
                let hash = hasher.finish() as i64;
                Ok(Value::Int64(hash))
            }
            _ => Err(Error::InvalidQuery(
                "FARM_FINGERPRINT requires STRING or BYTES argument".into(),
            )),
        }
    }

    fn fn_generate_uuid(&self, _args: &[Value]) -> Result<Value> {
        Ok(Value::String(uuid::Uuid::new_v4().to_string()))
    }

    fn fn_current_date(&self, _args: &[Value]) -> Result<Value> {
        Ok(Value::Date(Utc::now().date_naive()))
    }

    fn fn_current_timestamp(&self, _args: &[Value]) -> Result<Value> {
        Ok(Value::Timestamp(Utc::now()))
    }

    fn fn_current_time(&self, _args: &[Value]) -> Result<Value> {
        Ok(Value::Time(Utc::now().time()))
    }

    fn fn_current_datetime(&self, _args: &[Value]) -> Result<Value> {
        Ok(Value::DateTime(Utc::now().naive_utc()))
    }

    fn fn_zeroifnull(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) | None => Ok(Value::Int64(0)),
            Some(v) => Ok(v.clone()),
        }
    }

    fn fn_string(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Json(j)) => {
                if let Some(s) = j.as_str() {
                    Ok(Value::String(s.to_string()))
                } else {
                    Ok(Value::String(j.to_string()))
                }
            }
            Some(v) => Ok(Value::String(format!("{}", v))),
            None => Err(Error::InvalidQuery("STRING requires an argument".into())),
        }
    }

    fn fn_regexp_contains(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "REGEXP_CONTAINS requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(pattern)) => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
                Ok(Value::Bool(re.is_match(s)))
            }
            _ => Err(Error::InvalidQuery(
                "REGEXP_CONTAINS requires string arguments".into(),
            )),
        }
    }

    fn fn_regexp_replace(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "REGEXP_REPLACE requires 3 arguments".into(),
            ));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) => Ok(Value::Null),
            (Value::String(s), Value::String(pattern), Value::String(replacement)) => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
                let rust_replacement = replacement
                    .replace("\\1", "$1")
                    .replace("\\2", "$2")
                    .replace("\\3", "$3")
                    .replace("\\4", "$4")
                    .replace("\\5", "$5")
                    .replace("\\6", "$6")
                    .replace("\\7", "$7")
                    .replace("\\8", "$8")
                    .replace("\\9", "$9");
                Ok(Value::String(
                    re.replace_all(s, rust_replacement.as_str()).to_string(),
                ))
            }
            _ => Err(Error::InvalidQuery(
                "REGEXP_REPLACE requires string arguments".into(),
            )),
        }
    }

    fn fn_date_add(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("DATE_ADD requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Date(d), Value::Interval(interval)) => {
                let new_date = add_interval_to_date(d, interval)?;
                Ok(Value::Date(new_date))
            }
            (Value::DateTime(dt), Value::Interval(interval)) => {
                let new_dt = add_interval_to_datetime(dt, interval)?;
                Ok(Value::DateTime(new_dt))
            }
            (Value::Timestamp(ts), Value::Interval(interval)) => {
                let new_dt = add_interval_to_datetime(&ts.naive_utc(), interval)?;
                Ok(Value::Timestamp(new_dt.and_utc()))
            }
            _ => Err(Error::InvalidQuery(
                "DATE_ADD requires date/datetime/timestamp and interval".into(),
            )),
        }
    }

    fn fn_date_sub(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("DATE_SUB requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Date(d), Value::Interval(interval)) => {
                let neg_interval = negate_interval(interval);
                let new_date = add_interval_to_date(d, &neg_interval)?;
                Ok(Value::Date(new_date))
            }
            (Value::DateTime(dt), Value::Interval(interval)) => {
                let neg_interval = negate_interval(interval);
                let new_dt = add_interval_to_datetime(dt, &neg_interval)?;
                Ok(Value::DateTime(new_dt))
            }
            (Value::Timestamp(ts), Value::Interval(interval)) => {
                let neg_interval = negate_interval(interval);
                let new_dt = add_interval_to_datetime(&ts.naive_utc(), &neg_interval)?;
                Ok(Value::Timestamp(new_dt.and_utc()))
            }
            _ => Err(Error::InvalidQuery(
                "DATE_SUB requires date/datetime/timestamp and interval".into(),
            )),
        }
    }

    fn fn_date_diff(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery("DATE_DIFF requires 3 arguments".into()));
        }
        let part = args[2].as_str().unwrap_or("DAY").to_uppercase();
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Date(d1), Value::Date(d2)) => {
                let result = date_diff_by_part(d1, d2, &part)?;
                Ok(Value::Int64(result))
            }
            (Value::DateTime(dt1), Value::DateTime(dt2)) => {
                let d1 = dt1.date();
                let d2 = dt2.date();
                let result = date_diff_by_part(&d1, &d2, &part)?;
                Ok(Value::Int64(result))
            }
            (Value::Timestamp(ts1), Value::Timestamp(ts2)) => {
                let d1 = ts1.naive_utc().date();
                let d2 = ts2.naive_utc().date();
                let result = date_diff_by_part(&d1, &d2, &part)?;
                Ok(Value::Int64(result))
            }
            _ => Err(Error::InvalidQuery(
                "DATE_DIFF requires date/datetime/timestamp arguments".into(),
            )),
        }
    }

    fn fn_date_trunc(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATE_TRUNC requires 2 arguments".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Date(d) => {
                let part = args[1].as_str().unwrap_or("DAY").to_uppercase();
                let truncated = trunc_date(d, &part)?;
                Ok(Value::Date(truncated))
            }
            _ => Err(Error::InvalidQuery(
                "DATE_TRUNC requires a date argument".into(),
            )),
        }
    }

    fn fn_date_bucket(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATE_BUCKET requires at least 2 arguments".into(),
            ));
        }
        let date = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Date(d) => *d,
            _ => {
                return Err(Error::InvalidQuery(
                    "DATE_BUCKET requires a date as first argument".into(),
                ));
            }
        };
        let interval = match &args[1] {
            Value::Null => return Ok(Value::Null),
            Value::Interval(i) => i,
            _ => {
                return Err(Error::InvalidQuery(
                    "DATE_BUCKET requires an interval as second argument".into(),
                ));
            }
        };
        let origin = if args.len() >= 3 {
            match &args[2] {
                Value::Null => return Ok(Value::Null),
                Value::Date(d) => *d,
                _ => {
                    return Err(Error::InvalidQuery(
                        "DATE_BUCKET origin must be a date".into(),
                    ));
                }
            }
        } else {
            NaiveDate::from_ymd_opt(1950, 1, 1).unwrap()
        };
        if interval.months != 0 {
            let date_months = date.year() * 12 + date.month() as i32 - 1;
            let origin_months = origin.year() * 12 + origin.month() as i32 - 1;
            let diff_months = date_months - origin_months;
            let bucket_months = interval.months;
            let bucket_idx = if diff_months >= 0 {
                diff_months / bucket_months
            } else {
                (diff_months - bucket_months + 1) / bucket_months
            };
            let bucket_start_months = origin_months + bucket_idx * bucket_months;
            let year = bucket_start_months / 12;
            let month = (bucket_start_months % 12) as u32 + 1;
            let bucketed = NaiveDate::from_ymd_opt(year, month, 1).ok_or_else(|| {
                Error::InvalidQuery("Invalid date in DATE_BUCKET calculation".into())
            })?;
            Ok(Value::Date(bucketed))
        } else if interval.days != 0 {
            let diff_days = date.signed_duration_since(origin).num_days();
            let bucket_days = interval.days as i64;
            let bucket_idx = if diff_days >= 0 {
                diff_days / bucket_days
            } else {
                (diff_days - bucket_days + 1) / bucket_days
            };
            let bucketed = origin + chrono::Duration::days(bucket_idx * bucket_days);
            Ok(Value::Date(bucketed))
        } else {
            Err(Error::InvalidQuery(
                "DATE_BUCKET interval must have days or months".into(),
            ))
        }
    }

    fn fn_datetime_trunc(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATETIME_TRUNC requires 2 arguments".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::DateTime(dt) => {
                let part = args[1].as_str().unwrap_or("DAY").to_uppercase();
                let truncated = trunc_datetime(dt, &part)?;
                Ok(Value::DateTime(truncated))
            }
            _ => Err(Error::InvalidQuery(
                "DATETIME_TRUNC requires a datetime argument".into(),
            )),
        }
    }

    fn fn_timestamp_trunc(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIMESTAMP_TRUNC requires 2 arguments".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Timestamp(ts) => {
                let part = args[1].as_str().unwrap_or("DAY").to_uppercase();
                let truncated = trunc_datetime(&ts.naive_utc(), &part)?;
                Ok(Value::Timestamp(truncated.and_utc()))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_TRUNC requires a timestamp argument".into(),
            )),
        }
    }

    fn fn_time_trunc(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIME_TRUNC requires 2 arguments".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Time(t) => {
                let part = args[1].as_str().unwrap_or("SECOND").to_uppercase();
                let truncated = trunc_time(t, &part)?;
                Ok(Value::Time(truncated))
            }
            _ => Err(Error::InvalidQuery(
                "TIME_TRUNC requires a time argument".into(),
            )),
        }
    }

    fn fn_date_from_unix_date(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(days)) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = epoch + chrono::Duration::days(*days);
                Ok(Value::Date(date))
            }
            _ => Err(Error::InvalidQuery(
                "DATE_FROM_UNIX_DATE requires integer argument".into(),
            )),
        }
    }

    fn fn_unix_date(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Date(d)) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = d.signed_duration_since(epoch).num_days();
                Ok(Value::Int64(days))
            }
            _ => Err(Error::InvalidQuery(
                "UNIX_DATE requires date argument".into(),
            )),
        }
    }

    fn fn_unix_micros(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Timestamp(ts)) => {
                let micros = ts.timestamp_micros();
                Ok(Value::Int64(micros))
            }
            _ => Err(Error::InvalidQuery(
                "UNIX_MICROS requires timestamp argument".into(),
            )),
        }
    }

    fn fn_unix_millis(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Timestamp(ts)) => {
                let millis = ts.timestamp_millis();
                Ok(Value::Int64(millis))
            }
            _ => Err(Error::InvalidQuery(
                "UNIX_MILLIS requires timestamp argument".into(),
            )),
        }
    }

    fn fn_unix_seconds(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Timestamp(ts)) => {
                let secs = ts.timestamp();
                Ok(Value::Int64(secs))
            }
            _ => Err(Error::InvalidQuery(
                "UNIX_SECONDS requires timestamp argument".into(),
            )),
        }
    }

    fn fn_timestamp_micros(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(micros)) => {
                let ts = DateTime::from_timestamp_micros(*micros)
                    .ok_or_else(|| Error::InvalidQuery("Invalid microseconds".into()))?;
                Ok(Value::Timestamp(ts))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_MICROS requires integer argument".into(),
            )),
        }
    }

    fn fn_timestamp_millis(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(millis)) => {
                let ts = DateTime::from_timestamp_millis(*millis)
                    .ok_or_else(|| Error::InvalidQuery("Invalid milliseconds".into()))?;
                Ok(Value::Timestamp(ts))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_MILLIS requires integer argument".into(),
            )),
        }
    }

    fn fn_timestamp_seconds(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(secs)) => {
                let ts = DateTime::from_timestamp(*secs, 0)
                    .ok_or_else(|| Error::InvalidQuery("Invalid seconds".into()))?;
                Ok(Value::Timestamp(ts))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_SECONDS requires integer argument".into(),
            )),
        }
    }

    fn fn_format_date(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "FORMAT_DATE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::Date(d)) => {
                let formatted = format_date_with_pattern(d, fmt)?;
                Ok(Value::String(formatted))
            }
            _ => Err(Error::InvalidQuery(
                "FORMAT_DATE requires format string and date".into(),
            )),
        }
    }

    fn fn_format_datetime(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "FORMAT_DATETIME requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::DateTime(dt)) => {
                let formatted = format_datetime_with_pattern(dt, fmt)?;
                Ok(Value::String(formatted))
            }
            _ => Err(Error::InvalidQuery(
                "FORMAT_DATETIME requires format string and datetime".into(),
            )),
        }
    }

    fn fn_format_timestamp(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "FORMAT_TIMESTAMP requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::Timestamp(ts)) => {
                let formatted = format_datetime_with_pattern(&ts.naive_utc(), fmt)?;
                Ok(Value::String(formatted))
            }
            _ => Err(Error::InvalidQuery(
                "FORMAT_TIMESTAMP requires format string and timestamp".into(),
            )),
        }
    }

    fn fn_format_time(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "FORMAT_TIME requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::Time(t)) => {
                let formatted = format_time_with_pattern(t, fmt)?;
                Ok(Value::String(formatted))
            }
            _ => Err(Error::InvalidQuery(
                "FORMAT_TIME requires format string and time".into(),
            )),
        }
    }

    fn fn_parse_date(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "PARSE_DATE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::String(s)) => {
                let date = parse_date_with_pattern(s, fmt)?;
                Ok(Value::Date(date))
            }
            _ => Err(Error::InvalidQuery(
                "PARSE_DATE requires format and date strings".into(),
            )),
        }
    }

    fn fn_parse_datetime(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "PARSE_DATETIME requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::String(s)) => {
                let dt = parse_datetime_with_pattern(s, fmt)?;
                Ok(Value::DateTime(dt))
            }
            _ => Err(Error::InvalidQuery(
                "PARSE_DATETIME requires format and datetime strings".into(),
            )),
        }
    }

    fn fn_parse_timestamp(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "PARSE_TIMESTAMP requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::String(s)) => {
                let dt = parse_datetime_with_pattern(s, fmt)?;
                Ok(Value::Timestamp(dt.and_utc()))
            }
            _ => Err(Error::InvalidQuery(
                "PARSE_TIMESTAMP requires format and timestamp strings".into(),
            )),
        }
    }

    fn fn_parse_time(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "PARSE_TIME requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(fmt), Value::String(s)) => {
                let time = parse_time_with_pattern(s, fmt)?;
                Ok(Value::Time(time))
            }
            _ => Err(Error::InvalidQuery(
                "PARSE_TIME requires format and time strings".into(),
            )),
        }
    }

    fn fn_last_day(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Date(d)) => {
                let year = d.year();
                let month = d.month();
                let next_month = if month == 12 { 1 } else { month + 1 };
                let next_year = if month == 12 { year + 1 } else { year };
                let first_of_next = NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap();
                let last_day = first_of_next - chrono::Duration::days(1);
                Ok(Value::Date(last_day))
            }
            Some(Value::DateTime(dt)) => {
                let year = dt.date().year();
                let month = dt.date().month();
                let next_month = if month == 12 { 1 } else { month + 1 };
                let next_year = if month == 12 { year + 1 } else { year };
                let first_of_next = NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap();
                let last_day = first_of_next - chrono::Duration::days(1);
                Ok(Value::Date(last_day))
            }
            _ => Err(Error::InvalidQuery(
                "LAST_DAY requires date argument".into(),
            )),
        }
    }

    fn fn_datetime_bucket(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATETIME_BUCKET requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::DateTime(dt), Value::Interval(interval)) => {
                let origin = if args.len() > 2 {
                    match &args[2] {
                        Value::DateTime(o) => *o,
                        _ => NaiveDate::from_ymd_opt(1950, 1, 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                    }
                } else {
                    NaiveDate::from_ymd_opt(1950, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                };
                let bucket = bucket_datetime(dt, interval, &origin)?;
                Ok(Value::DateTime(bucket))
            }
            _ => Err(Error::InvalidQuery(
                "DATETIME_BUCKET requires datetime and interval arguments".into(),
            )),
        }
    }

    fn fn_timestamp_bucket(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIMESTAMP_BUCKET requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Timestamp(ts), Value::Interval(interval)) => {
                let origin = if args.len() > 2 {
                    match &args[2] {
                        Value::Timestamp(o) => o.naive_utc(),
                        _ => NaiveDate::from_ymd_opt(1950, 1, 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                    }
                } else {
                    NaiveDate::from_ymd_opt(1950, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                };
                let bucket = bucket_datetime(&ts.naive_utc(), interval, &origin)?;
                Ok(Value::Timestamp(bucket.and_utc()))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_BUCKET requires timestamp and interval arguments".into(),
            )),
        }
    }

    fn fn_extract_from_args(&self, args: &[Expr], record: &Record) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "EXTRACT requires field and value".into(),
            ));
        }
        let val = self.evaluate(&args[1], record)?;
        let field = match &args[0] {
            Expr::Column { name, .. } => name.to_uppercase(),
            _ => return Err(Error::InvalidQuery("EXTRACT requires field name".into())),
        };
        let datetime_field = match field.as_str() {
            "YEAR" => DateTimeField::Year,
            "MONTH" => DateTimeField::Month,
            "DAY" => DateTimeField::Day,
            "HOUR" => DateTimeField::Hour,
            "MINUTE" => DateTimeField::Minute,
            "SECOND" => DateTimeField::Second,
            "DAYOFWEEK" => DateTimeField::DayOfWeek,
            "DAYOFYEAR" => DateTimeField::DayOfYear,
            "QUARTER" => DateTimeField::Quarter,
            "WEEK" => DateTimeField::Week(WeekStartDay::Sunday),
            _ => {
                return Err(Error::InvalidQuery(format!(
                    "Unknown EXTRACT field: {}",
                    field
                )));
            }
        };
        extract_datetime_field(&val, datetime_field)
    }

    fn fn_sin(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).sin()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.sin()))),
            _ => Err(Error::InvalidQuery("SIN requires numeric argument".into())),
        }
    }

    fn fn_cos(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).cos()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.cos()))),
            _ => Err(Error::InvalidQuery("COS requires numeric argument".into())),
        }
    }

    fn fn_tan(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).tan()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.tan()))),
            _ => Err(Error::InvalidQuery("TAN requires numeric argument".into())),
        }
    }

    fn fn_asin(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).asin()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.asin()))),
            _ => Err(Error::InvalidQuery("ASIN requires numeric argument".into())),
        }
    }

    fn fn_acos(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).acos()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.acos()))),
            _ => Err(Error::InvalidQuery("ACOS requires numeric argument".into())),
        }
    }

    fn fn_atan(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).atan()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.atan()))),
            _ => Err(Error::InvalidQuery("ATAN requires numeric argument".into())),
        }
    }

    fn fn_atan2(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("ATAN2 requires 2 arguments".into()));
        }
        let y = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Int64(n) => *n as f64,
            Value::Float64(f) => f.0,
            _ => {
                return Err(Error::InvalidQuery(
                    "ATAN2 requires numeric arguments".into(),
                ));
            }
        };
        let x = match &args[1] {
            Value::Null => return Ok(Value::Null),
            Value::Int64(n) => *n as f64,
            Value::Float64(f) => f.0,
            _ => {
                return Err(Error::InvalidQuery(
                    "ATAN2 requires numeric arguments".into(),
                ));
            }
        };
        Ok(Value::Float64(OrderedFloat(y.atan2(x))))
    }

    fn fn_sinh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).sinh()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.sinh()))),
            _ => Err(Error::InvalidQuery("SINH requires numeric argument".into())),
        }
    }

    fn fn_cosh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).cosh()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.cosh()))),
            _ => Err(Error::InvalidQuery("COSH requires numeric argument".into())),
        }
    }

    fn fn_tanh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).tanh()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.tanh()))),
            _ => Err(Error::InvalidQuery("TANH requires numeric argument".into())),
        }
    }

    fn fn_asinh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => Ok(Value::Float64(OrderedFloat((*n as f64).asinh()))),
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.asinh()))),
            _ => Err(Error::InvalidQuery(
                "ASINH requires numeric argument".into(),
            )),
        }
    }

    fn fn_acosh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let val = *n as f64;
                if val < 1.0 {
                    Err(Error::InvalidQuery("ACOSH argument must be >= 1".into()))
                } else {
                    Ok(Value::Float64(OrderedFloat(val.acosh())))
                }
            }
            Some(Value::Float64(f)) => {
                if f.0 < 1.0 {
                    Err(Error::InvalidQuery("ACOSH argument must be >= 1".into()))
                } else {
                    Ok(Value::Float64(OrderedFloat(f.0.acosh())))
                }
            }
            _ => Err(Error::InvalidQuery(
                "ACOSH requires numeric argument".into(),
            )),
        }
    }

    fn fn_atanh(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let val = *n as f64;
                if val <= -1.0 || val >= 1.0 {
                    Err(Error::InvalidQuery(
                        "ATANH argument must be in (-1, 1)".into(),
                    ))
                } else {
                    Ok(Value::Float64(OrderedFloat(val.atanh())))
                }
            }
            Some(Value::Float64(f)) => {
                if f.0 <= -1.0 || f.0 >= 1.0 {
                    Err(Error::InvalidQuery(
                        "ATANH argument must be in (-1, 1)".into(),
                    ))
                } else {
                    Ok(Value::Float64(OrderedFloat(f.0.atanh())))
                }
            }
            _ => Err(Error::InvalidQuery(
                "ATANH requires numeric argument".into(),
            )),
        }
    }

    fn fn_cot(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(x.cos() / x.sin())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(f.0.cos() / f.0.sin()))),
            _ => Err(Error::InvalidQuery("COT requires numeric argument".into())),
        }
    }

    fn fn_csc(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(1.0 / x.sin())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.sin()))),
            _ => Err(Error::InvalidQuery("CSC requires numeric argument".into())),
        }
    }

    fn fn_sec(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(1.0 / x.cos())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.cos()))),
            _ => Err(Error::InvalidQuery("SEC requires numeric argument".into())),
        }
    }

    fn fn_coth(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(1.0 / x.tanh())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.tanh()))),
            _ => Err(Error::InvalidQuery("COTH requires numeric argument".into())),
        }
    }

    fn fn_csch(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(1.0 / x.sinh())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.sinh()))),
            _ => Err(Error::InvalidQuery("CSCH requires numeric argument".into())),
        }
    }

    fn fn_sech(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                let x = *n as f64;
                Ok(Value::Float64(OrderedFloat(1.0 / x.cosh())))
            }
            Some(Value::Float64(f)) => Ok(Value::Float64(OrderedFloat(1.0 / f.0.cosh()))),
            _ => Err(Error::InvalidQuery("SECH requires numeric argument".into())),
        }
    }

    fn fn_is_nan(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Float64(f)) => Ok(Value::Bool(f.0.is_nan())),
            Some(Value::Int64(_)) => Ok(Value::Bool(false)),
            _ => Err(Error::InvalidQuery(
                "IS_NAN requires numeric argument".into(),
            )),
        }
    }

    fn fn_is_inf(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Float64(f)) => Ok(Value::Bool(f.0.is_infinite())),
            Some(Value::Int64(_)) => Ok(Value::Bool(false)),
            _ => Err(Error::InvalidQuery(
                "IS_INF requires numeric argument".into(),
            )),
        }
    }

    fn fn_cosine_distance(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::InvalidQuery(
                "COSINE_DISTANCE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(a), Value::Array(b)) => {
                if a.len() != b.len() {
                    return Err(Error::InvalidQuery(
                        "COSINE_DISTANCE: arrays must have same length".into(),
                    ));
                }
                let mut dot_product = 0.0;
                let mut norm_a = 0.0;
                let mut norm_b = 0.0;
                for (va, vb) in a.iter().zip(b.iter()) {
                    let fa = match va {
                        Value::Float64(f) => f.0,
                        Value::Int64(n) => *n as f64,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(Error::InvalidQuery(
                                "COSINE_DISTANCE: array elements must be numeric".into(),
                            ));
                        }
                    };
                    let fb = match vb {
                        Value::Float64(f) => f.0,
                        Value::Int64(n) => *n as f64,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(Error::InvalidQuery(
                                "COSINE_DISTANCE: array elements must be numeric".into(),
                            ));
                        }
                    };
                    dot_product += fa * fb;
                    norm_a += fa * fa;
                    norm_b += fb * fb;
                }
                let similarity = dot_product / (norm_a.sqrt() * norm_b.sqrt());
                Ok(Value::Float64(OrderedFloat(1.0 - similarity)))
            }
            _ => Err(Error::InvalidQuery(
                "COSINE_DISTANCE requires array arguments".into(),
            )),
        }
    }

    fn fn_euclidean_distance(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::InvalidQuery(
                "EUCLIDEAN_DISTANCE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(a), Value::Array(b)) => {
                if a.len() != b.len() {
                    return Err(Error::InvalidQuery(
                        "EUCLIDEAN_DISTANCE: arrays must have same length".into(),
                    ));
                }
                let mut sum_sq = 0.0;
                for (va, vb) in a.iter().zip(b.iter()) {
                    let fa = match va {
                        Value::Float64(f) => f.0,
                        Value::Int64(n) => *n as f64,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(Error::InvalidQuery(
                                "EUCLIDEAN_DISTANCE: array elements must be numeric".into(),
                            ));
                        }
                    };
                    let fb = match vb {
                        Value::Float64(f) => f.0,
                        Value::Int64(n) => *n as f64,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(Error::InvalidQuery(
                                "EUCLIDEAN_DISTANCE: array elements must be numeric".into(),
                            ));
                        }
                    };
                    let diff = fa - fb;
                    sum_sq += diff * diff;
                }
                Ok(Value::Float64(OrderedFloat(sum_sq.sqrt())))
            }
            _ => Err(Error::InvalidQuery(
                "EUCLIDEAN_DISTANCE requires array arguments".into(),
            )),
        }
    }

    fn fn_range_bucket(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::InvalidQuery(
                "RANGE_BUCKET requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) => Ok(Value::Null),
            (_, Value::Null) => Ok(Value::Null),
            (point, Value::Array(arr)) => {
                if arr.is_empty() {
                    return Ok(Value::Int64(0));
                }
                let point_val = match point {
                    Value::Int64(n) => *n as f64,
                    Value::Float64(f) => f.0,
                    _ => {
                        return Err(Error::InvalidQuery(
                            "RANGE_BUCKET: point must be numeric".into(),
                        ));
                    }
                };
                let mut bucket = 0i64;
                for v in arr {
                    let boundary = match v {
                        Value::Int64(n) => *n as f64,
                        Value::Float64(f) => f.0,
                        Value::Null => return Ok(Value::Null),
                        _ => {
                            return Err(Error::InvalidQuery(
                                "RANGE_BUCKET: array elements must be numeric".into(),
                            ));
                        }
                    };
                    if point_val < boundary {
                        break;
                    }
                    bucket += 1;
                }
                Ok(Value::Int64(bucket))
            }
            _ => Err(Error::InvalidQuery(
                "RANGE_BUCKET requires point and array arguments".into(),
            )),
        }
    }

    fn fn_rand(&self, _args: &[Value]) -> Result<Value> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let random = (seed as f64 / u64::MAX as f64).fract();
        Ok(Value::Float64(OrderedFloat(random)))
    }

    fn fn_split(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "SPLIT requires at least 1 argument".into(),
            ));
        }
        let delimiter = args.get(1).and_then(|v| v.as_str()).unwrap_or(",");
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(s) => {
                let parts: Vec<Value> = if delimiter.is_empty() {
                    s.chars().map(|c| Value::String(c.to_string())).collect()
                } else {
                    s.split(delimiter)
                        .map(|p| Value::String(p.to_string()))
                        .collect()
                };
                Ok(Value::Array(parts))
            }
            _ => Err(Error::InvalidQuery("SPLIT requires string argument".into())),
        }
    }

    fn fn_array_concat(&self, args: &[Value]) -> Result<Value> {
        let mut result = Vec::new();
        for arg in args {
            match arg {
                Value::Null => continue,
                Value::Array(arr) => result.extend(arr.clone()),
                _ => {
                    return Err(Error::InvalidQuery(
                        "ARRAY_CONCAT requires array arguments".into(),
                    ));
                }
            }
        }
        Ok(Value::Array(result))
    }

    fn fn_array_reverse(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Array(arr)) => {
                let mut reversed = arr.clone();
                reversed.reverse();
                Ok(Value::Array(reversed))
            }
            _ => Err(Error::InvalidQuery(
                "ARRAY_REVERSE requires array argument".into(),
            )),
        }
    }

    fn fn_instr(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery("INSTR requires 2 arguments".into()));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(substr)) => {
                if substr.is_empty() {
                    return Ok(Value::Int64(0));
                }
                let position = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
                let occurrence = args.get(3).and_then(|v| v.as_i64()).unwrap_or(1) as usize;

                let chars: Vec<char> = s.chars().collect();
                let substr_chars: Vec<char> = substr.chars().collect();
                let char_len = chars.len();

                if position >= 0 {
                    let start_idx = if position == 0 {
                        0
                    } else {
                        (position as usize).saturating_sub(1).min(char_len)
                    };
                    let mut found_count = 0;
                    let mut idx = start_idx;
                    while idx + substr_chars.len() <= char_len {
                        if chars[idx..idx + substr_chars.len()] == substr_chars[..] {
                            found_count += 1;
                            if found_count == occurrence {
                                return Ok(Value::Int64((idx + 1) as i64));
                            }
                        }
                        idx += 1;
                    }
                } else {
                    let start_idx = char_len.saturating_sub((-position) as usize);
                    let mut found_count = 0;
                    let mut idx = start_idx.min(char_len.saturating_sub(substr_chars.len()));
                    loop {
                        if chars[idx..idx + substr_chars.len()] == substr_chars[..] {
                            found_count += 1;
                            if found_count == occurrence {
                                return Ok(Value::Int64((idx + 1) as i64));
                            }
                        }
                        if idx == 0 {
                            break;
                        }
                        idx -= 1;
                    }
                }
                Ok(Value::Int64(0))
            }
            _ => Err(Error::InvalidQuery(
                "INSTR requires string arguments".into(),
            )),
        }
    }

    fn fn_format(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "FORMAT requires at least 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(fmt) => {
                let format_args = &args[1..];
                let mut arg_index = 0;
                let mut result = String::new();
                let mut chars = fmt.chars().peekable();

                while let Some(c) = chars.next() {
                    if c == '%' {
                        if chars.peek() == Some(&'%') {
                            chars.next();
                            result.push('%');
                            continue;
                        }

                        let mut zero_pad = false;
                        let mut use_grouping = false;
                        let mut width: Option<usize> = None;
                        let mut precision: Option<usize> = None;

                        while let Some(&ch) = chars.peek() {
                            if ch == '0' && width.is_none() {
                                zero_pad = true;
                                chars.next();
                            } else if ch == '\'' {
                                use_grouping = true;
                                chars.next();
                            } else {
                                break;
                            }
                        }

                        let mut width_str = String::new();
                        while let Some(&ch) = chars.peek() {
                            if ch.is_ascii_digit() {
                                width_str.push(ch);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        if !width_str.is_empty() {
                            width = width_str.parse().ok();
                        }

                        if chars.peek() == Some(&'.') {
                            chars.next();
                            let mut prec_str = String::new();
                            while let Some(&ch) = chars.peek() {
                                if ch.is_ascii_digit() {
                                    prec_str.push(ch);
                                    chars.next();
                                } else {
                                    break;
                                }
                            }
                            if !prec_str.is_empty() {
                                precision = prec_str.parse().ok();
                            }
                        }

                        if let Some(&spec) = chars.peek() {
                            chars.next();
                            let val = format_args.get(arg_index);
                            arg_index += 1;

                            let formatted = match spec {
                                's' => val
                                    .map(|v| self.format_value_for_format(v))
                                    .unwrap_or_default(),
                                'd' | 'i' => {
                                    if let Some(n) = val.and_then(|v| v.as_i64()) {
                                        let mut s = if use_grouping {
                                            Self::format_with_grouping(n)
                                        } else {
                                            n.to_string()
                                        };
                                        if let Some(w) = width {
                                            if s.len() < w {
                                                let pad = w - s.len();
                                                if zero_pad {
                                                    if n < 0 {
                                                        s = format!(
                                                            "-{}{}",
                                                            "0".repeat(pad),
                                                            &s[1..]
                                                        );
                                                    } else {
                                                        s = format!("{}{}", "0".repeat(pad), s);
                                                    }
                                                } else {
                                                    s = format!("{}{}", " ".repeat(pad), s);
                                                }
                                            }
                                        }
                                        s
                                    } else {
                                        String::new()
                                    }
                                }
                                'f' => {
                                    if let Some(v) = val {
                                        let f_val = match v {
                                            Value::Float64(f) => Some(f.0),
                                            Value::Int64(n) => Some(*n as f64),
                                            Value::Numeric(n) => n.to_string().parse().ok(),
                                            _ => None,
                                        };
                                        if let Some(f) = f_val {
                                            let prec = precision.unwrap_or(6);
                                            format!("{:.prec$}", f, prec = prec)
                                        } else {
                                            String::new()
                                        }
                                    } else {
                                        String::new()
                                    }
                                }
                                'e' | 'E' => {
                                    if let Some(v) = val {
                                        let f_val = match v {
                                            Value::Float64(f) => Some(f.0),
                                            Value::Int64(n) => Some(*n as f64),
                                            Value::Numeric(n) => n.to_string().parse().ok(),
                                            _ => None,
                                        };
                                        if let Some(f) = f_val {
                                            let prec = precision.unwrap_or(6);
                                            Self::format_scientific(f, prec, spec == 'E')
                                        } else {
                                            String::new()
                                        }
                                    } else {
                                        String::new()
                                    }
                                }
                                'g' => {
                                    if let Some(v) = val {
                                        let f_val = match v {
                                            Value::Float64(f) => Some(f.0),
                                            Value::Int64(n) => Some(*n as f64),
                                            Value::Numeric(n) => n.to_string().parse().ok(),
                                            _ => None,
                                        };
                                        if let Some(f) = f_val {
                                            if let Some(prec) = precision {
                                                format!("{:.prec$}", f, prec = prec)
                                            } else {
                                                f.to_string()
                                            }
                                        } else {
                                            String::new()
                                        }
                                    } else {
                                        String::new()
                                    }
                                }
                                't' | 'T' => val
                                    .map(|v| self.format_value_for_format(v))
                                    .unwrap_or_default(),
                                _ => format!("%{}", spec),
                            };
                            result.push_str(&formatted);
                        }
                    } else {
                        result.push(c);
                    }
                }
                Ok(Value::String(result))
            }
            Value::Bool(_)
            | Value::Int64(_)
            | Value::Float64(_)
            | Value::Numeric(_)
            | Value::BigNumeric(_)
            | Value::Bytes(_)
            | Value::Date(_)
            | Value::Time(_)
            | Value::DateTime(_)
            | Value::Timestamp(_)
            | Value::Json(_)
            | Value::Array(_)
            | Value::Struct(_)
            | Value::Geography(_)
            | Value::Interval(_)
            | Value::Range(_)
            | Value::Default => Err(Error::InvalidQuery(
                "FORMAT requires a format string as first argument".into(),
            )),
        }
    }

    fn format_with_grouping(n: i64) -> String {
        let sign = if n < 0 { "-" } else { "" };
        let abs_n = n.abs();
        let s = abs_n.to_string();
        let mut result = String::new();
        for (i, c) in s.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 {
                result.push(',');
            }
            result.push(c);
        }
        format!("{}{}", sign, result.chars().rev().collect::<String>())
    }

    fn format_scientific(f: f64, precision: usize, uppercase: bool) -> String {
        if f == 0.0 {
            let e_char = if uppercase { 'E' } else { 'e' };
            return format!("{:.prec$}{}+00", 0.0, e_char, prec = precision);
        }
        let exp = f.abs().log10().floor() as i32;
        let mantissa = f / 10_f64.powi(exp);
        let e_char = if uppercase { 'E' } else { 'e' };
        let sign = if exp >= 0 { '+' } else { '-' };
        format!(
            "{:.prec$}{}{}{:02}",
            mantissa,
            e_char,
            sign,
            exp.abs(),
            prec = precision
        )
    }

    fn format_value_for_format(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int64(n) => n.to_string(),
            Value::Float64(f) => f.0.to_string(),
            Value::Numeric(n) => n.to_string(),
            Value::BigNumeric(n) => n.to_string(),
            Value::String(s) => s.clone(),
            Value::Bytes(b) => format!("{:?}", b),
            Value::Date(d) => d.to_string(),
            Value::Time(t) => t.to_string(),
            Value::DateTime(dt) => dt.to_string(),
            Value::Timestamp(ts) => ts.to_rfc3339(),
            Value::Json(j) => j.to_string(),
            Value::Array(arr) => format!("{:?}", arr),
            Value::Struct(fields) => format!("{:?}", fields),
            Value::Geography(g) => g.clone(),
            Value::Interval(i) => format!("{:?}", i),
            Value::Range(r) => format!("{:?}", r),
            Value::Default => "DEFAULT".to_string(),
        }
    }

    fn fn_cast(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("CAST requires an argument".into()));
        }
        Ok(args[0].clone())
    }

    fn fn_regexp_extract(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "REGEXP_EXTRACT requires 2 arguments".into(),
            ));
        }
        let group_num = args.get(2).and_then(|v| v.as_i64()).unwrap_or(1) as usize;
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(pattern)) => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
                match re.captures(s) {
                    Some(caps) => {
                        let matched = caps
                            .get(group_num)
                            .or_else(|| caps.get(0))
                            .map(|m| m.as_str().to_string());
                        Ok(matched.map(Value::String).unwrap_or(Value::Null))
                    }
                    None => Ok(Value::Null),
                }
            }
            _ => Err(Error::InvalidQuery(
                "REGEXP_EXTRACT requires string arguments".into(),
            )),
        }
    }

    fn fn_regexp_extract_all(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "REGEXP_EXTRACT_ALL requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(s), Value::String(pattern)) => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid regex: {}", e)))?;
                let matches: Vec<Value> = re
                    .captures_iter(s)
                    .filter_map(|caps| {
                        caps.get(1)
                            .or_else(|| caps.get(0))
                            .map(|m| Value::String(m.as_str().to_string()))
                    })
                    .collect();
                Ok(Value::Array(matches))
            }
            _ => Err(Error::InvalidQuery(
                "REGEXP_EXTRACT_ALL requires string arguments".into(),
            )),
        }
    }

    fn fn_byte_length(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::Int64(s.len() as i64)),
            Some(Value::Bytes(b)) => Ok(Value::Int64(b.len() as i64)),
            _ => Err(Error::InvalidQuery(
                "BYTE_LENGTH requires string or bytes argument".into(),
            )),
        }
    }

    fn fn_ascii(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let code = s.chars().next().map(|c| c as i64).unwrap_or(0);
                Ok(Value::Int64(code))
            }
            _ => Err(Error::InvalidQuery("ASCII requires string argument".into())),
        }
    }

    fn fn_chr(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Int64(n)) => {
                if *n == 0 {
                    return Ok(Value::String(String::new()));
                }
                let c = char::from_u32(*n as u32).unwrap_or('\0');
                Ok(Value::String(c.to_string()))
            }
            _ => Err(Error::InvalidQuery("CHR requires integer argument".into())),
        }
    }

    fn fn_unicode(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let code = s.chars().next().map(|c| c as i64).unwrap_or(0);
                Ok(Value::Int64(code))
            }
            _ => Err(Error::InvalidQuery(
                "UNICODE requires string argument".into(),
            )),
        }
    }

    fn fn_to_code_points(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let code_points: Vec<Value> = s.chars().map(|c| Value::Int64(c as i64)).collect();
                Ok(Value::Array(code_points))
            }
            _ => Err(Error::InvalidQuery(
                "TO_CODE_POINTS requires string argument".into(),
            )),
        }
    }

    fn fn_code_points_to_string(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Array(arr)) => {
                let mut result = String::new();
                for v in arr {
                    match v {
                        Value::Null => return Ok(Value::Null),
                        Value::Int64(n) => {
                            if let Some(c) = char::from_u32(*n as u32) {
                                result.push(c);
                            }
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "CODE_POINTS_TO_STRING requires array of integers".into(),
                            ));
                        }
                    }
                }
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery(
                "CODE_POINTS_TO_STRING requires array argument".into(),
            )),
        }
    }

    fn fn_code_points_to_bytes(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Array(arr)) => {
                let mut result = Vec::new();
                for v in arr {
                    match v {
                        Value::Null => return Ok(Value::Null),
                        Value::Int64(n) => {
                            if *n < 0 || *n > 255 {
                                return Err(Error::InvalidQuery(format!(
                                    "CODE_POINTS_TO_BYTES: value {} out of range 0-255",
                                    n
                                )));
                            }
                            result.push(*n as u8);
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "CODE_POINTS_TO_BYTES requires array of integers".into(),
                            ));
                        }
                    }
                }
                Ok(Value::Bytes(result))
            }
            _ => Err(Error::InvalidQuery(
                "CODE_POINTS_TO_BYTES requires array argument".into(),
            )),
        }
    }

    fn fn_translate(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery("TRANSLATE requires 3 arguments".into()));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
            (Value::String(source), Value::String(from_chars), Value::String(to_chars)) => {
                let from: Vec<char> = from_chars.chars().collect();
                let to: Vec<char> = to_chars.chars().collect();
                let result: String = source
                    .chars()
                    .filter_map(|c| {
                        if let Some(pos) = from.iter().position(|&fc| fc == c) {
                            to.get(pos).copied()
                        } else {
                            Some(c)
                        }
                    })
                    .collect();
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery(
                "TRANSLATE requires string arguments".into(),
            )),
        }
    }

    fn fn_soundex(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                if s.is_empty() {
                    return Ok(Value::String(String::new()));
                }
                let mut result = String::new();
                let mut chars = s.chars().filter(|c| c.is_ascii_alphabetic());
                if let Some(first) = chars.next() {
                    result.push(first.to_ascii_uppercase());
                }
                let get_code = |c: char| -> Option<char> {
                    match c.to_ascii_lowercase() {
                        'b' | 'f' | 'p' | 'v' => Some('1'),
                        'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some('2'),
                        'd' | 't' => Some('3'),
                        'l' => Some('4'),
                        'm' | 'n' => Some('5'),
                        'r' => Some('6'),
                        _ => None,
                    }
                };
                let is_hw = |c: char| matches!(c.to_ascii_lowercase(), 'h' | 'w');
                let mut last_code: Option<char> = None;
                for c in chars {
                    if result.len() >= 4 {
                        break;
                    }
                    if is_hw(c) {
                        continue;
                    }
                    if let Some(code) = get_code(c) {
                        if Some(code) != last_code {
                            result.push(code);
                            last_code = Some(code);
                        }
                    } else {
                        last_code = None;
                    }
                }
                while result.len() < 4 {
                    result.push('0');
                }
                Ok(Value::String(result))
            }
            _ => Err(Error::InvalidQuery(
                "SOUNDEX requires string argument".into(),
            )),
        }
    }

    fn fn_normalize(&self, args: &[Value]) -> Result<Value> {
        use unicode_normalization::UnicodeNormalization;

        let mode = args
            .get(1)
            .and_then(|v| {
                if let Value::String(s) = v {
                    Some(s.to_uppercase())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "NFC".to_string());

        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let normalized: String = match mode.as_str() {
                    "NFC" => s.nfc().collect(),
                    "NFKC" => s.nfkc().collect(),
                    "NFD" => s.nfd().collect(),
                    "NFKD" => s.nfkd().collect(),
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid normalization mode: {}. Expected NFC, NFKC, NFD, or NFKD",
                            mode
                        )));
                    }
                };
                Ok(Value::String(normalized))
            }
            _ => Err(Error::InvalidQuery(
                "NORMALIZE requires string argument".into(),
            )),
        }
    }

    fn fn_normalize_and_casefold(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                use unicode_normalization::UnicodeNormalization;
                let normalized: String = s.nfkc().collect();
                let casefolded = normalized.to_lowercase();
                Ok(Value::String(casefolded))
            }
            _ => Err(Error::InvalidQuery(
                "NORMALIZE_AND_CASEFOLD requires string argument".into(),
            )),
        }
    }

    fn fn_edit_distance(&self, args: &[Value]) -> Result<Value> {
        let (s1, s2, max_distance) = match args {
            [Value::Null, ..] | [_, Value::Null, ..] => return Ok(Value::Null),
            [Value::String(a), Value::String(b)] => (a.as_str(), b.as_str(), None),
            [Value::String(a), Value::String(b), Value::Int64(max)] => {
                (a.as_str(), b.as_str(), Some(*max as usize))
            }
            _ => {
                return Err(Error::InvalidQuery(
                    "EDIT_DISTANCE requires two string arguments".into(),
                ));
            }
        };

        let len1 = s1.chars().count();
        let len2 = s2.chars().count();

        if let Some(max) = max_distance {
            if len1.abs_diff(len2) > max {
                return Ok(Value::Int64(max as i64));
            }
        }

        let mut prev_row: Vec<usize> = (0..=len2).collect();
        let mut curr_row = vec![0; len2 + 1];

        for (i, c1) in s1.chars().enumerate() {
            curr_row[0] = i + 1;
            for (j, c2) in s2.chars().enumerate() {
                let cost = if c1 == c2 { 0 } else { 1 };
                curr_row[j + 1] = (prev_row[j + 1] + 1)
                    .min(curr_row[j] + 1)
                    .min(prev_row[j] + cost);
            }
            std::mem::swap(&mut prev_row, &mut curr_row);
        }

        let distance = prev_row[len2];
        let result = match max_distance {
            Some(max) if distance > max => max,
            _ => distance,
        };
        Ok(Value::Int64(result as i64))
    }

    fn fn_contains_substr(&self, args: &[Value]) -> Result<Value> {
        match args {
            [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
            [haystack_val, Value::String(needle)] => {
                use unicode_normalization::UnicodeNormalization;
                let haystack = self.value_to_contains_substr_string(haystack_val);
                let normalized_haystack: String = haystack.nfkc().collect();
                let normalized_needle: String = needle.nfkc().collect();
                let result = normalized_haystack
                    .to_lowercase()
                    .contains(&normalized_needle.to_lowercase());
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "CONTAINS_SUBSTR requires string second argument".into(),
            )),
        }
    }

    fn value_to_contains_substr_string(&self, val: &Value) -> String {
        match val {
            Value::Null => "".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Int64(n) => n.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::Numeric(d) | Value::BigNumeric(d) => d.to_string(),
            Value::String(s) => s.clone(),
            Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
            Value::Date(d) => d.to_string(),
            Value::Time(t) => t.to_string(),
            Value::DateTime(dt) => dt.to_string(),
            Value::Timestamp(ts) => ts.to_string(),
            Value::Json(j) => j.to_string(),
            Value::Array(arr) => {
                let elements: Vec<String> = arr
                    .iter()
                    .map(|v| self.value_to_contains_substr_string(v))
                    .collect();
                format!("[{}]", elements.join(", "))
            }
            Value::Struct(fields) => {
                let elements: Vec<String> = fields
                    .iter()
                    .map(|(_, v)| self.value_to_contains_substr_string(v))
                    .collect();
                format!("({})", elements.join(", "))
            }
            Value::Geography(g) => g.clone(),
            Value::Interval(i) => format!("{:?}", i),
            Value::Range(r) => format!("{:?}", r),
            Value::Default => "DEFAULT".to_string(),
        }
    }

    fn fn_to_base64(&self, args: &[Value]) -> Result<Value> {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => Ok(Value::String(STANDARD.encode(s.as_bytes()))),
            Some(Value::Bytes(b)) => Ok(Value::String(STANDARD.encode(b))),
            _ => Err(Error::InvalidQuery(
                "TO_BASE64 requires string or bytes argument".into(),
            )),
        }
    }

    fn fn_from_base64(&self, args: &[Value]) -> Result<Value> {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let decoded = STANDARD
                    .decode(s)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid base64: {}", e)))?;
                Ok(Value::Bytes(decoded))
            }
            _ => Err(Error::InvalidQuery(
                "FROM_BASE64 requires string argument".into(),
            )),
        }
    }

    fn fn_to_base32(&self, args: &[Value]) -> Result<Value> {
        use data_encoding::BASE32;
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Bytes(b)) => Ok(Value::String(BASE32.encode(b))),
            Some(Value::String(s)) => Ok(Value::String(BASE32.encode(s.as_bytes()))),
            _ => Err(Error::InvalidQuery(
                "TO_BASE32 requires bytes or string argument".into(),
            )),
        }
    }

    fn fn_from_base32(&self, args: &[Value]) -> Result<Value> {
        use data_encoding::BASE32;
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let decoded = BASE32
                    .decode(s.as_bytes())
                    .map_err(|e| Error::InvalidQuery(format!("Invalid base32: {}", e)))?;
                Ok(Value::Bytes(decoded))
            }
            _ => Err(Error::InvalidQuery(
                "FROM_BASE32 requires string argument".into(),
            )),
        }
    }

    fn fn_to_hex(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::Bytes(b)) => Ok(Value::String(hex::encode(b))),
            Some(Value::String(s)) => Ok(Value::String(hex::encode(s.as_bytes()))),
            _ => Err(Error::InvalidQuery("TO_HEX requires bytes argument".into())),
        }
    }

    fn fn_from_hex(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let normalized = if s.len() % 2 == 1 {
                    format!("0{}", s)
                } else {
                    s.clone()
                };
                let decoded = hex::decode(&normalized)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid hex: {}", e)))?;
                Ok(Value::Bytes(decoded))
            }
            _ => Err(Error::InvalidQuery(
                "FROM_HEX requires string argument".into(),
            )),
        }
    }

    fn fn_safe_convert_bytes_to_string(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "SAFE_CONVERT_BYTES_TO_STRING requires 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(b) => match String::from_utf8(b.clone()) {
                Ok(s) => Ok(Value::String(s)),
                Err(_) => Ok(Value::Null),
            },
            Value::Bool(_)
            | Value::Int64(_)
            | Value::Float64(_)
            | Value::Numeric(_)
            | Value::BigNumeric(_)
            | Value::String(_)
            | Value::Date(_)
            | Value::Time(_)
            | Value::DateTime(_)
            | Value::Timestamp(_)
            | Value::Json(_)
            | Value::Array(_)
            | Value::Struct(_)
            | Value::Geography(_)
            | Value::Interval(_)
            | Value::Range(_)
            | Value::Default => Err(Error::InvalidQuery(
                "SAFE_CONVERT_BYTES_TO_STRING requires BYTES argument".into(),
            )),
        }
    }

    fn fn_convert_bytes_to_string(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "CONVERT_BYTES_TO_STRING requires 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(b) => {
                let s = String::from_utf8(b.clone())
                    .map_err(|e| Error::InvalidQuery(format!("Invalid UTF-8 in bytes: {}", e)))?;
                Ok(Value::String(s))
            }
            Value::Bool(_)
            | Value::Int64(_)
            | Value::Float64(_)
            | Value::Numeric(_)
            | Value::BigNumeric(_)
            | Value::String(_)
            | Value::Date(_)
            | Value::Time(_)
            | Value::DateTime(_)
            | Value::Timestamp(_)
            | Value::Json(_)
            | Value::Array(_)
            | Value::Struct(_)
            | Value::Geography(_)
            | Value::Interval(_)
            | Value::Range(_)
            | Value::Default => Err(Error::InvalidQuery(
                "CONVERT_BYTES_TO_STRING requires BYTES argument".into(),
            )),
        }
    }

    fn fn_to_json(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(v) => {
                let json = value_to_json(v)?;
                Ok(Value::Json(json))
            }
            None => Err(Error::InvalidQuery("TO_JSON requires an argument".into())),
        }
    }

    fn fn_to_json_string(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(v) => {
                let json = value_to_json(v)?;
                Ok(Value::String(json.to_string()))
            }
            None => Err(Error::InvalidQuery(
                "TO_JSON_STRING requires an argument".into(),
            )),
        }
    }

    fn fn_parse_json(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::Null),
            Some(Value::String(s)) => {
                let json: serde_json::Value = serde_json::from_str(s)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
                Ok(Value::Json(json))
            }
            _ => Err(Error::InvalidQuery(
                "PARSE_JSON requires string argument".into(),
            )),
        }
    }

    fn fn_json_value(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "JSON_VALUE requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Json(json), Value::String(path)) => {
                let value = extract_json_path(json, path)?;
                match value {
                    Some(serde_json::Value::String(s)) => Ok(Value::String(s)),
                    Some(serde_json::Value::Number(n)) => {
                        if let Some(i) = n.as_i64() {
                            Ok(Value::String(i.to_string()))
                        } else if let Some(f) = n.as_f64() {
                            Ok(Value::String(f.to_string()))
                        } else {
                            Ok(Value::String(n.to_string()))
                        }
                    }
                    Some(serde_json::Value::Bool(b)) => Ok(Value::String(b.to_string())),
                    Some(serde_json::Value::Null) => Ok(Value::Null),
                    Some(_) => Ok(Value::Null),
                    None => Ok(Value::Null),
                }
            }
            (Value::String(s), Value::String(path)) => {
                let json: serde_json::Value = serde_json::from_str(s)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
                let value = extract_json_path(&json, path)?;
                match value {
                    Some(serde_json::Value::String(s)) => Ok(Value::String(s)),
                    Some(serde_json::Value::Number(n)) => Ok(Value::String(n.to_string())),
                    Some(serde_json::Value::Bool(b)) => Ok(Value::String(b.to_string())),
                    Some(serde_json::Value::Null) => Ok(Value::Null),
                    Some(_) => Ok(Value::Null),
                    None => Ok(Value::Null),
                }
            }
            _ => Err(Error::InvalidQuery(
                "JSON_VALUE requires JSON and path arguments".into(),
            )),
        }
    }

    fn fn_json_query(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "JSON_QUERY requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Json(json), Value::String(path)) => {
                let value = extract_json_path(json, path)?;
                match value {
                    Some(v) => Ok(Value::Json(v)),
                    None => Ok(Value::Null),
                }
            }
            (Value::String(s), Value::String(path)) => {
                let json: serde_json::Value = serde_json::from_str(s)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid JSON: {}", e)))?;
                let value = extract_json_path(&json, path)?;
                match value {
                    Some(v) => Ok(Value::Json(v)),
                    None => Ok(Value::Null),
                }
            }
            _ => Err(Error::InvalidQuery(
                "JSON_QUERY requires JSON and path arguments".into(),
            )),
        }
    }

    fn fn_json_extract(&self, args: &[Value]) -> Result<Value> {
        self.fn_json_value(args)
    }

    fn fn_json_extract_array(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(j) => j,
                Err(_) => return Ok(Value::Null),
            },
            _ => return Ok(Value::Null),
        };
        let json_to_extract = if args.len() > 1 {
            if let Value::String(path) = &args[1] {
                let path = path.trim_start_matches('$');
                let path = path.trim_start_matches('.');
                if path.is_empty() {
                    json_val
                } else {
                    let mut current = &json_val;
                    for part in path.split('.') {
                        let part = part.trim_start_matches('[').trim_end_matches(']');
                        if let Ok(idx) = part.parse::<usize>() {
                            if let Some(arr) = current.as_array() {
                                if idx < arr.len() {
                                    current = &arr[idx];
                                } else {
                                    return Ok(Value::Null);
                                }
                            } else {
                                return Ok(Value::Null);
                            }
                        } else if let Some(obj) = current.as_object() {
                            if let Some(val) = obj.get(part) {
                                current = val;
                            } else {
                                return Ok(Value::Null);
                            }
                        } else {
                            return Ok(Value::Null);
                        }
                    }
                    current.clone()
                }
            } else {
                json_val
            }
        } else {
            json_val
        };
        if let Some(arr) = json_to_extract.as_array() {
            let result: Vec<Value> = arr.iter().map(|v| Value::Json(v.clone())).collect();
            Ok(Value::Array(result))
        } else {
            Ok(Value::Array(vec![]))
        }
    }

    fn fn_typeof(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) => Ok(Value::String("NULL".to_string())),
            Some(Value::Bool(_)) => Ok(Value::String("BOOL".to_string())),
            Some(Value::Int64(_)) => Ok(Value::String("INT64".to_string())),
            Some(Value::Float64(_)) => Ok(Value::String("FLOAT64".to_string())),
            Some(Value::String(_)) => Ok(Value::String("STRING".to_string())),
            Some(Value::Bytes(_)) => Ok(Value::String("BYTES".to_string())),
            Some(Value::Date(_)) => Ok(Value::String("DATE".to_string())),
            Some(Value::Time(_)) => Ok(Value::String("TIME".to_string())),
            Some(Value::DateTime(_)) => Ok(Value::String("DATETIME".to_string())),
            Some(Value::Timestamp(_)) => Ok(Value::String("TIMESTAMP".to_string())),
            Some(Value::Array(_)) => Ok(Value::String("ARRAY".to_string())),
            Some(Value::Struct(_)) => Ok(Value::String("STRUCT".to_string())),
            Some(Value::Json(_)) => Ok(Value::String("JSON".to_string())),
            Some(Value::Numeric(_)) => Ok(Value::String("NUMERIC".to_string())),
            Some(Value::BigNumeric(_)) => Ok(Value::String("BIGNUMERIC".to_string())),
            Some(Value::Interval(_)) => Ok(Value::String("INTERVAL".to_string())),
            Some(Value::Geography(_)) => Ok(Value::String("GEOGRAPHY".to_string())),
            Some(Value::Range(_)) => Ok(Value::String("RANGE".to_string())),
            Some(Value::Default) => Ok(Value::String("DEFAULT".to_string())),
            None => Ok(Value::Null),
        }
    }

    fn fn_nvl2(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery("NVL2 requires 3 arguments".into()));
        }
        if args[0].is_null() {
            Ok(args[2].clone())
        } else {
            Ok(args[1].clone())
        }
    }

    fn fn_range(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::InvalidQuery("RANGE requires 2 arguments".into()));
        }
        let start = if args[0].is_null() {
            None
        } else {
            Some(args[0].clone())
        };
        let end = if args[1].is_null() {
            None
        } else {
            Some(args[1].clone())
        };
        Ok(Value::range(start, end))
    }

    fn fn_json_type(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("JSON_TYPE requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(json) => {
                let type_name = match json {
                    serde_json::Value::Null => "null",
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::Object(_) => "object",
                };
                Ok(Value::String(type_name.to_string()))
            }
            _ => Err(Error::InvalidQuery(
                "JSON_TYPE requires a JSON argument".into(),
            )),
        }
    }

    fn fn_bit_count(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("BIT_COUNT requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Int64(n) => {
                let count = n.count_ones() as i64;
                Ok(Value::Int64(count))
            }
            Value::Bytes(bytes) => {
                let count: u32 = bytes.iter().map(|b| b.count_ones()).sum();
                Ok(Value::Int64(count as i64))
            }
            _ => Err(Error::InvalidQuery(
                "BIT_COUNT requires an integer or bytes argument".into(),
            )),
        }
    }

    fn fn_int64_from_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("INT64 requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(json) => match json {
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Ok(Value::Int64(i))
                    } else if let Some(f) = n.as_f64() {
                        Ok(Value::Int64(f as i64))
                    } else {
                        Err(Error::InvalidQuery(
                            "Cannot convert JSON number to INT64".into(),
                        ))
                    }
                }
                serde_json::Value::String(s) => s
                    .parse::<i64>()
                    .map(Value::Int64)
                    .map_err(|_| Error::InvalidQuery("Cannot parse JSON string as INT64".into())),
                serde_json::Value::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
                _ => Err(Error::InvalidQuery("Cannot convert JSON to INT64".into())),
            },
            Value::Int64(n) => Ok(Value::Int64(*n)),
            Value::Float64(f) => Ok(Value::Int64(f.0 as i64)),
            Value::String(s) => s
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|_| Error::InvalidQuery("Cannot parse string as INT64".into())),
            _ => Err(Error::InvalidQuery("INT64 requires a JSON argument".into())),
        }
    }

    fn fn_float64_from_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("FLOAT64 requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(json) => match json {
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        Ok(Value::Float64(OrderedFloat(f)))
                    } else {
                        Err(Error::InvalidQuery(
                            "Cannot convert JSON number to FLOAT64".into(),
                        ))
                    }
                }
                serde_json::Value::String(s) => s
                    .parse::<f64>()
                    .map(|f| Value::Float64(OrderedFloat(f)))
                    .map_err(|_| Error::InvalidQuery("Cannot parse JSON string as FLOAT64".into())),
                _ => Err(Error::InvalidQuery("Cannot convert JSON to FLOAT64".into())),
            },
            Value::Float64(f) => Ok(Value::Float64(*f)),
            Value::Int64(n) => Ok(Value::Float64(OrderedFloat(*n as f64))),
            Value::String(s) => s
                .parse::<f64>()
                .map(|f| Value::Float64(OrderedFloat(f)))
                .map_err(|_| Error::InvalidQuery("Cannot parse string as FLOAT64".into())),
            _ => Err(Error::InvalidQuery(
                "FLOAT64 requires a JSON argument".into(),
            )),
        }
    }

    fn fn_bool_from_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("BOOL requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(json) => match json {
                serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
                serde_json::Value::String(s) => match s.to_lowercase().as_str() {
                    "true" => Ok(Value::Bool(true)),
                    "false" => Ok(Value::Bool(false)),
                    _ => Err(Error::InvalidQuery(
                        "Cannot parse JSON string as BOOL".into(),
                    )),
                },
                _ => Err(Error::InvalidQuery("Cannot convert JSON to BOOL".into())),
            },
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                _ => Err(Error::InvalidQuery("Cannot parse string as BOOL".into())),
            },
            _ => Err(Error::InvalidQuery("BOOL requires a JSON argument".into())),
        }
    }

    fn fn_string_from_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery("STRING requires 1 argument".into()));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(json) => match json {
                serde_json::Value::String(s) => Ok(Value::String(s.clone())),
                serde_json::Value::Number(n) => Ok(Value::String(n.to_string())),
                serde_json::Value::Bool(b) => Ok(Value::String(b.to_string())),
                _ => Ok(Value::String(json.to_string())),
            },
            Value::String(s) => Ok(Value::String(s.clone())),
            Value::Int64(n) => Ok(Value::String(n.to_string())),
            Value::Float64(f) => Ok(Value::String(f.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            other => Ok(Value::String(format!("{}", other))),
        }
    }

    fn fn_make_interval(&self, args: &[Value]) -> Result<Value> {
        use yachtsql_common::types::IntervalValue;

        let mut years = 0i32;
        let mut months = 0i32;
        let mut days = 0i32;
        let mut hours = 0i64;
        let mut minutes = 0i64;
        let mut seconds = 0i64;

        for (i, arg) in args.iter().enumerate() {
            let val = match arg {
                Value::Null => continue,
                Value::Int64(n) => *n,
                _ => {
                    return Err(Error::InvalidQuery(
                        "MAKE_INTERVAL arguments must be integers".into(),
                    ));
                }
            };
            match i {
                0 => years = val as i32,
                1 => months = val as i32,
                2 => days = val as i32,
                3 => hours = val,
                4 => minutes = val,
                5 => seconds = val,
                _ => {}
            }
        }

        let total_months = years * 12 + months;
        let total_nanos = (hours * 3600 + minutes * 60 + seconds) * 1_000_000_000;
        Ok(Value::Interval(IntervalValue {
            months: total_months,
            days,
            nanos: total_nanos,
        }))
    }

    fn fn_justify_days(&self, args: &[Value]) -> Result<Value> {
        use yachtsql_common::types::IntervalValue;
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "JUSTIFY_DAYS requires 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Interval(iv) => {
                let extra_months = iv.days / 30;
                let remaining_days = iv.days % 30;
                Ok(Value::Interval(IntervalValue {
                    months: iv.months + extra_months,
                    days: remaining_days,
                    nanos: iv.nanos,
                }))
            }
            _ => Err(Error::InvalidQuery(
                "JUSTIFY_DAYS requires an interval argument".into(),
            )),
        }
    }

    fn fn_justify_hours(&self, args: &[Value]) -> Result<Value> {
        use yachtsql_common::types::IntervalValue;
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "JUSTIFY_HOURS requires 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Interval(iv) => {
                const NANOS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000_000;
                let extra_days = (iv.nanos / NANOS_PER_DAY) as i32;
                let remaining_nanos = iv.nanos % NANOS_PER_DAY;
                Ok(Value::Interval(IntervalValue {
                    months: iv.months,
                    days: iv.days + extra_days,
                    nanos: remaining_nanos,
                }))
            }
            _ => Err(Error::InvalidQuery(
                "JUSTIFY_HOURS requires an interval argument".into(),
            )),
        }
    }

    fn fn_justify_interval(&self, args: &[Value]) -> Result<Value> {
        use yachtsql_common::types::IntervalValue;
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "JUSTIFY_INTERVAL requires 1 argument".into(),
            ));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Interval(iv) => {
                const NANOS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000_000;
                let extra_days_from_nanos = (iv.nanos / NANOS_PER_DAY) as i32;
                let remaining_nanos = iv.nanos % NANOS_PER_DAY;
                let total_days = iv.days + extra_days_from_nanos;
                let extra_months_from_days = total_days / 30;
                let remaining_days = total_days % 30;
                Ok(Value::Interval(IntervalValue {
                    months: iv.months + extra_months_from_days,
                    days: remaining_days,
                    nanos: remaining_nanos,
                }))
            }
            _ => Err(Error::InvalidQuery(
                "JUSTIFY_INTERVAL requires an interval argument".into(),
            )),
        }
    }

    fn fn_safe_array_access(&self, args: &[Value], one_based: bool) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "SAFE_OFFSET/SAFE_ORDINAL requires 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(arr), Value::Int64(idx)) => {
                let index = if one_based { *idx - 1 } else { *idx };
                if index < 0 || index as usize >= arr.len() {
                    Ok(Value::Null)
                } else {
                    Ok(arr[index as usize].clone())
                }
            }
            _ => Err(Error::InvalidQuery(
                "SAFE_OFFSET/SAFE_ORDINAL requires array and integer arguments".into(),
            )),
        }
    }

    fn eval_custom_function(&self, name: &str, args: &[Value]) -> Result<Value> {
        if let Some(result) = self.try_eval_user_function(name, args)? {
            return Ok(result);
        }
        match name.to_uppercase().as_str() {
            "ST_GEOGFROMTEXT" | "ST_GEOGRAPHYFROMTEXT" => self.fn_st_geogfromtext(args),
            "ST_GEOGPOINT" | "ST_GEOGRAPHYPOINT" => self.fn_st_geogpoint(args),
            "ST_GEOGFROMGEOJSON" => self.fn_st_geogfromgeojson(args),
            "ST_ASTEXT" => self.fn_st_astext(args),
            "ST_ASGEOJSON" => self.fn_st_asgeojson(args),
            "ST_ASBINARY" => self.fn_st_asbinary(args),
            "ST_X" => self.fn_st_x(args),
            "ST_Y" => self.fn_st_y(args),
            "ST_AREA" => self.fn_st_area(args),
            "ST_LENGTH" => self.fn_st_length(args),
            "ST_PERIMETER" => self.fn_st_perimeter(args),
            "ST_DISTANCE" => self.fn_st_distance(args),
            "ST_CENTROID" => self.fn_st_centroid(args),
            "ST_BUFFER" => self.fn_st_buffer(args),
            "ST_BOUNDINGBOX" => self.fn_st_boundingbox(args),
            "ST_CLOSESTPOINT" => self.fn_st_closestpoint(args),
            "ST_CONTAINS" => self.fn_st_contains(args),
            "ST_INTERSECTS" => self.fn_st_intersects(args),
            "ST_UNION" => self.fn_st_union(args),
            "ST_INTERSECTION" => self.fn_st_intersection(args),
            "ST_DIFFERENCE" => self.fn_st_difference(args),
            "ST_MAKELINE" => self.fn_st_makeline(args),
            "ST_MAKEPOLYGON" => self.fn_st_makepolygon(args),
            "ST_NUMPOINTS" => self.fn_st_numpoints(args),
            "ST_ISCLOSED" => self.fn_st_isclosed(args),
            "ST_ISEMPTY" => self.fn_st_isempty(args),
            "NET.IP_FROM_STRING" => self.fn_net_ip_from_string(args),
            "NET.SAFE_IP_FROM_STRING" => self.fn_net_safe_ip_from_string(args),
            "NET.IP_TO_STRING" => self.fn_net_ip_to_string(args),
            "NET.IPV4_FROM_INT64" => self.fn_net_ipv4_from_int64(args),
            "NET.IPV4_TO_INT64" => self.fn_net_ipv4_to_int64(args),
            "NET.HOST" => self.fn_net_host(args),
            "NET.PUBLIC_SUFFIX" => self.fn_net_public_suffix(args),
            "NET.REG_DOMAIN" => self.fn_net_reg_domain(args),
            "ST_GEOMETRYTYPE" => self.fn_st_geometrytype(args),
            "ST_DIMENSION" => self.fn_st_dimension(args),
            "ST_WITHIN" => self.fn_st_within(args),
            "ST_DWITHIN" => self.fn_st_dwithin(args),
            "ST_COVERS" => self.fn_st_covers(args),
            "ST_COVEREDBY" => self.fn_st_coveredby(args),
            "ST_TOUCHES" => self.fn_st_touches(args),
            "ST_DISJOINT" => self.fn_st_disjoint(args),
            "ST_EQUALS" => self.fn_st_equals(args),
            "ST_CONVEXHULL" => self.fn_st_convexhull(args),
            "ST_SIMPLIFY" => self.fn_st_simplify(args),
            "ST_SNAPTOGRID" => self.fn_st_snaptogrid(args),
            "ST_BOUNDARY" => self.fn_st_boundary(args),
            "ST_STARTPOINT" => self.fn_st_startpoint(args),
            "ST_ENDPOINT" => self.fn_st_endpoint(args),
            "ST_POINTN" => self.fn_st_pointn(args),
            "ST_ISCOLLECTION" => self.fn_st_iscollection(args),
            "ST_ISRING" => self.fn_st_isring(args),
            "ST_MAXDISTANCE" => self.fn_st_maxdistance(args),
            "ST_GEOHASH" => self.fn_st_geohash(args),
            "ST_GEOGPOINTFROMGEOHASH" => self.fn_st_geogpointfromgeohash(args),
            "ST_BUFFERWITHTOLERANCE" => self.fn_st_bufferwithtolerance(args),
            "TIMESTAMP_DIFF" => self.fn_timestamp_diff(args),
            "DATETIME_ADD" => self.fn_datetime_add(args),
            "DATETIME_SUB" => self.fn_datetime_sub(args),
            "DATETIME_DIFF" => self.fn_datetime_diff(args),
            "TIMESTAMP_ADD" => self.fn_timestamp_add(args),
            "TIMESTAMP_SUB" => self.fn_timestamp_sub(args),
            "TIME_ADD" => self.fn_time_add(args),
            "TIME_SUB" => self.fn_time_sub(args),
            "TIME_DIFF" => self.fn_time_diff(args),
            "ALL" => self.fn_all(args),
            "ANY" | "SOME" => self.fn_any(args),
            "OFFSET" => self.fn_offset(args),
            "ORDINAL" => self.fn_ordinal(args),
            "JSON_ARRAY" => self.fn_json_array(args),
            "JSON_OBJECT" => self.fn_json_object(args),
            "JSON_SET" => self.fn_json_set(args),
            "JSON_REMOVE" => self.fn_json_remove(args),
            "JSON_STRIP_NULLS" => self.fn_json_strip_nulls(args),
            "JSON_QUERY_ARRAY" => self.fn_json_query_array(args),
            "JSON_VALUE_ARRAY" => self.fn_json_value_array(args),
            "JSON_EXTRACT_STRING_ARRAY" => self.fn_json_extract_string_array(args),
            "JSON_KEYS" => self.fn_json_keys(args),
            "REGEXP_INSTR" => self.fn_regexp_instr(args),
            "NET.IP_IN_NET" => self.fn_net_ip_in_net(args),
            "NET.IP_IS_PRIVATE" => self.fn_net_ip_is_private(args),
            "NET.IP_TRUNC" => self.fn_net_ip_trunc(args),
            "NET.IP_NET_MASK" => self.fn_net_ip_net_mask(args),
            "NET.MAKE_NET" => self.fn_net_make_net(args),
            "RANGE_CONTAINS" => self.fn_range_contains(args),
            "RANGE_OVERLAPS" => self.fn_range_overlaps(args),
            "RANGE_START" => self.fn_range_start(args),
            "RANGE_END" => self.fn_range_end(args),
            "RANGE_INTERSECT" => self.fn_range_intersect(args),
            "GENERATE_RANGE_ARRAY" => self.fn_generate_range_array(args),
            "LAX_INT64" => self.fn_lax_int64(args),
            "LAX_FLOAT64" => self.fn_lax_float64(args),
            "LAX_BOOL" => self.fn_lax_bool(args),
            "LAX_STRING" => self.fn_lax_string(args),
            "SESSION_USER" | "CURRENT_USER" => Ok(Value::String("anonymous".to_string())),
            "NULLIFZERO" => self.fn_nullifzero(args),
            "REGEXP_SUBSTR" => self.fn_regexp_substr(args),
            "ARRAY_SLICE" => self.fn_array_slice(args),
            "ARRAYENUMERATE" => self.fn_array_enumerate(args),
            "HLL_COUNT_EXTRACT" => self.fn_hll_count_extract(args),
            "MAP" => self.fn_map(args),
            "MAPKEYS" => self.fn_map_keys(args),
            "MAPVALUES" => self.fn_map_values(args),
            "KEYS.NEW_KEYSET" => self.fn_keys_new_keyset(args),
            "KEYS.ADD_KEY_FROM_RAW_BYTES" => self.fn_keys_add_key_from_raw_bytes(args),
            "KEYS.KEYSET_CHAIN" => self.fn_keys_keyset_chain(args),
            "KEYS.KEYSET_FROM_JSON" => self.fn_keys_keyset_from_json(args),
            "KEYS.KEYSET_TO_JSON" => self.fn_keys_keyset_to_json(args),
            "KEYS.KEYSET_LENGTH" => self.fn_keys_keyset_length(args),
            "KEYS.ROTATE_KEYSET" => self.fn_keys_rotate_keyset(args),
            "AEAD.ENCRYPT" => self.fn_aead_encrypt(args),
            "AEAD.DECRYPT_STRING" => self.fn_aead_decrypt_string(args),
            "AEAD.DECRYPT_BYTES" => self.fn_aead_decrypt_bytes(args),
            "DETERMINISTIC_ENCRYPT" => self.fn_deterministic_encrypt(args),
            "DETERMINISTIC_DECRYPT_STRING" => self.fn_deterministic_decrypt_string(args),
            "DETERMINISTIC_DECRYPT_BYTES" => self.fn_deterministic_decrypt_bytes(args),
            "COLLATE" => self.fn_collate(args),
            _ => Err(Error::UnsupportedFeature(format!(
                "Scalar function Custom(\"{}\") not yet implemented in IR evaluator",
                name
            ))),
        }
    }

    fn fn_collate(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::InvalidQuery(
                "COLLATE requires exactly 2 arguments".into(),
            ));
        }
        let Value::String(s) = &args[0] else {
            return Ok(Value::null());
        };
        let Value::String(collation) = &args[1] else {
            return Err(Error::InvalidQuery(
                "COLLATE second argument must be a string".into(),
            ));
        };
        let collation_lower = collation.to_lowercase();
        if collation_lower.contains(":ci") {
            Ok(Value::String(s.to_lowercase()))
        } else {
            Ok(Value::String(s.clone()))
        }
    }

    fn fn_map(&self, args: &[Value]) -> Result<Value> {
        if !args.len().is_multiple_of(2) {
            return Err(Error::InvalidQuery(
                "MAP requires an even number of arguments (alternating key, value pairs)".into(),
            ));
        }
        if args.is_empty() {
            return Ok(Value::Array(vec![]));
        }
        let map_entries: Vec<Value> = args
            .chunks(2)
            .map(|pair| {
                Value::Struct(vec![
                    ("key".to_string(), pair[0].clone()),
                    ("value".to_string(), pair[1].clone()),
                ])
            })
            .collect();
        Ok(Value::Array(map_entries))
    }

    fn fn_map_keys(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "MAPKEYS requires exactly 1 argument".into(),
            ));
        }
        let map_array = match &args[0] {
            Value::Array(arr) => arr,
            Value::Null => return Ok(Value::Null),
            _ => {
                return Err(Error::InvalidQuery(
                    "MAPKEYS argument must be a MAP (array of key-value structs)".into(),
                ));
            }
        };
        let keys: Vec<Value> = map_array
            .iter()
            .filter_map(|entry| {
                if let Value::Struct(fields) = entry {
                    fields
                        .iter()
                        .find(|(name, _)| name == "key")
                        .map(|(_, v)| v.clone())
                } else {
                    None
                }
            })
            .collect();
        Ok(Value::Array(keys))
    }

    fn fn_map_values(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "MAPVALUES requires exactly 1 argument".into(),
            ));
        }
        let map_array = match &args[0] {
            Value::Array(arr) => arr,
            Value::Null => return Ok(Value::Null),
            _ => {
                return Err(Error::InvalidQuery(
                    "MAPVALUES argument must be a MAP (array of key-value structs)".into(),
                ));
            }
        };
        let values: Vec<Value> = map_array
            .iter()
            .filter_map(|entry| {
                if let Value::Struct(fields) = entry {
                    fields
                        .iter()
                        .find(|(name, _)| name == "value")
                        .map(|(_, v)| v.clone())
                } else {
                    None
                }
            })
            .collect();
        Ok(Value::Array(values))
    }

    fn fn_st_geogfromtext(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(wkt) => Ok(Value::Geography(wkt.clone())),
            _ => Err(Error::InvalidQuery(
                "ST_GEOGFROMTEXT expects a string argument".into(),
            )),
        }
    }

    fn fn_st_geogpoint(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_GEOGPOINT requires longitude and latitude".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Float64(lon), Value::Float64(lat)) => {
                Ok(Value::Geography(format!("POINT({} {})", lon.0, lat.0)))
            }
            (Value::Int64(lon), Value::Int64(lat)) => {
                Ok(Value::Geography(format!("POINT({} {})", lon, lat)))
            }
            (Value::Float64(lon), Value::Int64(lat)) => {
                Ok(Value::Geography(format!("POINT({} {})", lon.0, lat)))
            }
            (Value::Int64(lon), Value::Float64(lat)) => {
                Ok(Value::Geography(format!("POINT({} {})", lon, lat.0)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_GEOGPOINT expects numeric arguments".into(),
            )),
        }
    }

    fn fn_st_geogfromgeojson(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(geojson) => {
                let parsed: serde_json::Value = serde_json::from_str(geojson)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid GeoJSON: {}", e)))?;
                let geom_type = parsed.get("type").and_then(|t| t.as_str());
                let coords = parsed.get("coordinates");
                match (geom_type, coords) {
                    (Some("Point"), Some(serde_json::Value::Array(c))) if c.len() >= 2 => {
                        let x = c[0].as_f64().unwrap_or(0.0);
                        let y = c[1].as_f64().unwrap_or(0.0);
                        Ok(Value::Geography(format!("POINT({} {})", x, y)))
                    }
                    _ => Ok(Value::Geography(geojson.clone())),
                }
            }
            _ => Err(Error::InvalidQuery(
                "ST_GEOGFROMGEOJSON expects a string argument".into(),
            )),
        }
    }

    fn fn_st_astext(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => Ok(Value::String(wkt.clone())),
            _ => Err(Error::InvalidQuery(
                "ST_ASTEXT expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_asgeojson(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                if wkt.starts_with("POINT(") && wkt.ends_with(")") {
                    let inner = &wkt[6..wkt.len() - 1];
                    let parts: Vec<&str> = inner.split_whitespace().collect();
                    if parts.len() >= 2 {
                        let x: f64 = parts[0].parse().unwrap_or(0.0);
                        let y: f64 = parts[1].parse().unwrap_or(0.0);
                        return Ok(Value::String(format!(
                            "{{\"type\":\"Point\",\"coordinates\":[{},{}]}}",
                            x, y
                        )));
                    }
                }
                Ok(Value::String(format!("{{\"wkt\":\"{}\"}}", wkt)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_ASGEOJSON expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_asbinary(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => Ok(Value::Bytes(wkt.as_bytes().to_vec())),
            _ => Err(Error::InvalidQuery(
                "ST_ASBINARY expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_x(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                if wkt.starts_with("POINT(") && wkt.ends_with(")") {
                    let inner = &wkt[6..wkt.len() - 1];
                    let parts: Vec<&str> = inner.split_whitespace().collect();
                    if let Some(x_str) = parts.first() {
                        if let Ok(x) = x_str.parse::<f64>() {
                            return Ok(Value::Float64(OrderedFloat(x)));
                        }
                    }
                }
                Ok(Value::Null)
            }
            _ => Err(Error::InvalidQuery(
                "ST_X expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_y(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                if wkt.starts_with("POINT(") && wkt.ends_with(")") {
                    let inner = &wkt[6..wkt.len() - 1];
                    let parts: Vec<&str> = inner.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(y) = parts[1].parse::<f64>() {
                            return Ok(Value::Float64(OrderedFloat(y)));
                        }
                    }
                }
                Ok(Value::Null)
            }
            _ => Err(Error::InvalidQuery(
                "ST_Y expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_area(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let area = geom.geodesic_area_signed().abs();
                Ok(Value::Float64(OrderedFloat(area)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_AREA expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_length(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let length = match &geom {
                    Geometry::LineString(ls) => ls.geodesic_length(),
                    Geometry::MultiLineString(mls) => {
                        mls.0.iter().map(|ls| ls.geodesic_length()).sum()
                    }
                    _ => 0.0,
                };
                Ok(Value::Float64(OrderedFloat(length)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_LENGTH expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_perimeter(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let perimeter = match &geom {
                    Geometry::Polygon(poly) => poly.exterior().geodesic_length(),
                    Geometry::MultiPolygon(mp) => {
                        mp.0.iter().map(|p| p.exterior().geodesic_length()).sum()
                    }
                    _ => 0.0,
                };
                Ok(Value::Float64(OrderedFloat(perimeter)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_PERIMETER expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_distance(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_DISTANCE requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let distance = Self::geodesic_distance_between_geometries(&geom1, &geom2);
                Ok(Value::Float64(OrderedFloat(distance)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_DISTANCE expects geography arguments".into(),
            )),
        }
    }

    fn geodesic_distance_between_geometries(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> f64 {
        match (geom1, geom2) {
            (Geometry::Point(p1), Geometry::Point(p2)) => p1.geodesic_distance(p2),
            (Geometry::Point(p), Geometry::LineString(l))
            | (Geometry::LineString(l), Geometry::Point(p)) => l
                .points()
                .map(|lp| p.geodesic_distance(&lp))
                .fold(f64::INFINITY, f64::min),
            (Geometry::Point(p), Geometry::Polygon(poly))
            | (Geometry::Polygon(poly), Geometry::Point(p)) => poly
                .exterior()
                .points()
                .map(|pp| p.geodesic_distance(&pp))
                .fold(f64::INFINITY, f64::min),
            (Geometry::LineString(l1), Geometry::LineString(l2)) => l1
                .points()
                .flat_map(|p1| l2.points().map(move |p2| p1.geodesic_distance(&p2)))
                .fold(f64::INFINITY, f64::min),
            (Geometry::Polygon(poly1), Geometry::Polygon(poly2)) => poly1
                .exterior()
                .points()
                .flat_map(|p1| {
                    poly2
                        .exterior()
                        .points()
                        .map(move |p2| p1.geodesic_distance(&p2))
                })
                .fold(f64::INFINITY, f64::min),
            _ => 0.0,
        }
    }

    fn fn_st_centroid(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let centroid = geom.centroid();
                match centroid {
                    Some(p) => Ok(Value::Geography(format!("POINT({} {})", p.x(), p.y()))),
                    None => Ok(Value::Null),
                }
            }
            _ => Err(Error::InvalidQuery(
                "ST_CENTROID expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_buffer(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::Null);
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt), Value::Float64(_))
            | (Value::Geography(wkt), Value::Int64(_)) => {
                let distance_meters = match &args[1] {
                    Value::Float64(f) => f.0,
                    Value::Int64(i) => *i as f64,
                    _ => 0.0,
                };
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let buffered = Self::create_buffer(&geom, distance_meters);
                Ok(Value::Geography(Self::geometry_to_wkt(&buffered)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_BUFFER expects a geography argument".into(),
            )),
        }
    }

    fn create_buffer(geom: &Geometry<f64>, distance_meters: f64) -> Geometry<f64> {
        match geom {
            Geometry::Point(p) => {
                let num_segments = 32;
                let deg_per_meter_lat = 1.0 / 111_320.0;
                let deg_per_meter_lon = 1.0 / (111_320.0 * p.y().to_radians().cos());
                let mut coords = Vec::with_capacity(num_segments + 1);
                for i in 0..num_segments {
                    let angle = 2.0 * std::f64::consts::PI * (i as f64) / (num_segments as f64);
                    let dx = distance_meters * angle.cos() * deg_per_meter_lon;
                    let dy = distance_meters * angle.sin() * deg_per_meter_lat;
                    coords.push(Coord {
                        x: p.x() + dx,
                        y: p.y() + dy,
                    });
                }
                coords.push(coords[0]);
                Geometry::Polygon(Polygon::new(LineString::new(coords), vec![]))
            }
            _ => geom.clone(),
        }
    }

    fn geometry_to_wkt(geom: &Geometry<f64>) -> String {
        use std::fmt::Write;
        match geom {
            Geometry::Point(p) => format!("POINT({} {})", p.x(), p.y()),
            Geometry::LineString(ls) => {
                let coords: Vec<String> = ls.coords().map(|c| format!("{} {}", c.x, c.y)).collect();
                format!("LINESTRING({})", coords.join(", "))
            }
            Geometry::Polygon(poly) => {
                let exterior: Vec<String> = poly
                    .exterior()
                    .coords()
                    .map(|c| format!("{} {}", c.x, c.y))
                    .collect();
                if poly.interiors().is_empty() {
                    format!("POLYGON(({}))", exterior.join(", "))
                } else {
                    let mut result = format!("POLYGON(({})", exterior.join(", "));
                    for interior in poly.interiors() {
                        let interior_coords: Vec<String> = interior
                            .coords()
                            .map(|c| format!("{} {}", c.x, c.y))
                            .collect();
                        let _ = write!(result, ", ({})", interior_coords.join(", "));
                    }
                    result.push(')');
                    result
                }
            }
            Geometry::MultiPoint(mp) => {
                let points: Vec<String> =
                    mp.0.iter()
                        .map(|p| format!("{} {}", p.x(), p.y()))
                        .collect();
                format!("MULTIPOINT({})", points.join(", "))
            }
            Geometry::MultiLineString(mls) => {
                let lines: Vec<String> = mls
                    .0
                    .iter()
                    .map(|ls| {
                        let coords: Vec<String> =
                            ls.coords().map(|c| format!("{} {}", c.x, c.y)).collect();
                        format!("({})", coords.join(", "))
                    })
                    .collect();
                format!("MULTILINESTRING({})", lines.join(", "))
            }
            Geometry::MultiPolygon(mp) => {
                let polys: Vec<String> =
                    mp.0.iter()
                        .map(|poly| {
                            let exterior: Vec<String> = poly
                                .exterior()
                                .coords()
                                .map(|c| format!("{} {}", c.x, c.y))
                                .collect();
                            format!("(({}))", exterior.join(", "))
                        })
                        .collect();
                format!("MULTIPOLYGON({})", polys.join(", "))
            }
            Geometry::GeometryCollection(gc) => {
                if gc.0.is_empty() {
                    "GEOMETRYCOLLECTION EMPTY".to_string()
                } else {
                    let geoms: Vec<String> = gc.0.iter().map(Self::geometry_to_wkt).collect();
                    format!("GEOMETRYCOLLECTION({})", geoms.join(", "))
                }
            }
            _ => "GEOMETRYCOLLECTION EMPTY".to_string(),
        }
    }

    fn fn_st_boundingbox(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let rect = geom.bounding_rect();
                match rect {
                    Some(r) => {
                        let min = r.min();
                        let max = r.max();
                        let bbox_wkt = format!(
                            "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))",
                            min.x, min.y, min.x, max.y, max.x, max.y, max.x, min.y, min.x, min.y
                        );
                        Ok(Value::Geography(bbox_wkt))
                    }
                    None => Ok(Value::Null),
                }
            }
            _ => Err(Error::InvalidQuery(
                "ST_BOUNDINGBOX expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_closestpoint(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_CLOSESTPOINT requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let closest = Self::find_closest_point(&geom1, &geom2);
                Ok(Value::Geography(format!(
                    "POINT({} {})",
                    closest.x(),
                    closest.y()
                )))
            }
            _ => Err(Error::InvalidQuery(
                "ST_CLOSESTPOINT expects geography arguments".into(),
            )),
        }
    }

    fn find_closest_point(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> Point<f64> {
        let target_point = match geom2 {
            Geometry::Point(p) => *p,
            _ => geom2.centroid().unwrap_or(Point::new(0.0, 0.0)),
        };

        let points = Self::extract_points(geom1);
        if points.is_empty() {
            return Point::new(0.0, 0.0);
        }

        points
            .into_iter()
            .min_by(|a, b| {
                let da = a.geodesic_distance(&target_point);
                let db = b.geodesic_distance(&target_point);
                da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap_or(Point::new(0.0, 0.0))
    }

    fn extract_points(geom: &Geometry<f64>) -> Vec<Point<f64>> {
        match geom {
            Geometry::Point(p) => vec![*p],
            Geometry::LineString(ls) => ls.points().collect(),
            Geometry::Polygon(poly) => poly.exterior().points().collect(),
            Geometry::MultiPoint(mp) => mp.0.clone(),
            Geometry::MultiLineString(mls) => mls.0.iter().flat_map(|ls| ls.points()).collect(),
            Geometry::MultiPolygon(mp) => {
                mp.0.iter()
                    .flat_map(|poly| poly.exterior().points())
                    .collect()
            }
            _ => vec![],
        }
    }

    fn fn_st_contains(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_CONTAINS requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_contains(&geom1, &geom2);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_CONTAINS expects geography arguments".into(),
            )),
        }
    }

    fn geometry_contains(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> bool {
        match (geom1, geom2) {
            (Geometry::Polygon(poly), Geometry::Point(p)) => poly.contains(p),
            (Geometry::Polygon(poly1), Geometry::Polygon(poly2)) => {
                poly2.exterior().points().all(|p| poly1.contains(&p))
            }
            (Geometry::Polygon(poly), Geometry::LineString(ls)) => {
                ls.points().all(|p| poly.contains(&p))
            }
            (Geometry::MultiPolygon(mp), Geometry::Point(p)) => {
                mp.0.iter().any(|poly| poly.contains(p))
            }
            _ => false,
        }
    }

    fn fn_st_intersects(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_INTERSECTS requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = geom1.intersects(&geom2);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_INTERSECTS expects geography arguments".into(),
            )),
        }
    }

    fn fn_st_union(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_UNION requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_union(&geom1, &geom2);
                Ok(Value::Geography(Self::geometry_to_wkt(&result)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_UNION expects geography arguments".into(),
            )),
        }
    }

    fn geometry_union(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> Geometry<f64> {
        match (geom1, geom2) {
            (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.union(&mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::Polygon(p2)) => {
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.union(&mp2))
            }
            (Geometry::Polygon(p1), Geometry::MultiPolygon(mp2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                Geometry::MultiPolygon(mp1.union(mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::MultiPolygon(mp2)) => {
                Geometry::MultiPolygon(mp1.union(mp2))
            }
            _ => geom1.clone(),
        }
    }

    fn fn_st_intersection(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_INTERSECTION requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_intersection(&geom1, &geom2);
                Ok(Value::Geography(Self::geometry_to_wkt(&result)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_INTERSECTION expects geography arguments".into(),
            )),
        }
    }

    fn geometry_intersection(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> Geometry<f64> {
        match (geom1, geom2) {
            (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.intersection(&mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::Polygon(p2)) => {
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.intersection(&mp2))
            }
            (Geometry::Polygon(p1), Geometry::MultiPolygon(mp2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                Geometry::MultiPolygon(mp1.intersection(mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::MultiPolygon(mp2)) => {
                Geometry::MultiPolygon(mp1.intersection(mp2))
            }
            _ => Geometry::GeometryCollection(geo_types::GeometryCollection::new_from(vec![])),
        }
    }

    fn fn_st_difference(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_DIFFERENCE requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_difference(&geom1, &geom2);
                Ok(Value::Geography(Self::geometry_to_wkt(&result)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_DIFFERENCE expects geography arguments".into(),
            )),
        }
    }

    fn geometry_difference(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> Geometry<f64> {
        match (geom1, geom2) {
            (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.difference(&mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::Polygon(p2)) => {
                let mp2 = MultiPolygon::new(vec![p2.clone()]);
                Geometry::MultiPolygon(mp1.difference(&mp2))
            }
            (Geometry::Polygon(p1), Geometry::MultiPolygon(mp2)) => {
                let mp1 = MultiPolygon::new(vec![p1.clone()]);
                Geometry::MultiPolygon(mp1.difference(mp2))
            }
            (Geometry::MultiPolygon(mp1), Geometry::MultiPolygon(mp2)) => {
                Geometry::MultiPolygon(mp1.difference(mp2))
            }
            _ => geom1.clone(),
        }
    }

    fn fn_st_makeline(&self, args: &[Value]) -> Result<Value> {
        let mut points = Vec::new();

        if args.len() == 1 {
            if let Value::Array(arr) = &args[0] {
                for elem in arr {
                    if let Value::Geography(wkt) = elem {
                        if wkt.starts_with("POINT(") {
                            let inner = &wkt[6..wkt.len() - 1];
                            points.push(inner.to_string());
                        }
                    }
                }
            }
        } else {
            for arg in args {
                match arg {
                    Value::Null => continue,
                    Value::Geography(wkt) if wkt.starts_with("POINT(") => {
                        let inner = &wkt[6..wkt.len() - 1];
                        points.push(inner.to_string());
                    }
                    _ => {}
                }
            }
        }

        if points.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_MAKELINE requires at least two geography points".into(),
            ));
        }
        Ok(Value::Geography(format!(
            "LINESTRING({})",
            points.join(", ")
        )))
    }

    fn fn_st_makepolygon(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
                let inner = &wkt[11..wkt.len() - 1];
                Ok(Value::Geography(format!("POLYGON(({}))", inner)))
            }
            Value::Geography(wkt) => Ok(Value::Geography(wkt.clone())),
            _ => Err(Error::InvalidQuery(
                "ST_MAKEPOLYGON expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_numpoints(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                if wkt.starts_with("POINT(") {
                    Ok(Value::Int64(1))
                } else if wkt.starts_with("LINESTRING(") || wkt.starts_with("POLYGON(") {
                    let count = wkt.matches(',').count() + 1;
                    Ok(Value::Int64(count as i64))
                } else {
                    Ok(Value::Int64(0))
                }
            }
            _ => Err(Error::InvalidQuery(
                "ST_NUMPOINTS expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_isclosed(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let is_closed = match &geom {
                    Geometry::Point(_) => true,
                    Geometry::LineString(ls) => {
                        if ls.0.len() < 2 {
                            false
                        } else {
                            ls.0.first() == ls.0.last()
                        }
                    }
                    Geometry::Polygon(_) => true,
                    _ => false,
                };
                Ok(Value::Bool(is_closed))
            }
            _ => Err(Error::InvalidQuery(
                "ST_ISCLOSED expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_isempty(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let is_empty = wkt.contains("EMPTY") || wkt.is_empty();
                Ok(Value::Bool(is_empty))
            }
            _ => Err(Error::InvalidQuery(
                "ST_ISEMPTY expects a geography argument".into(),
            )),
        }
    }

    fn fn_net_ip_from_string(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(s) => {
                use std::net::IpAddr;
                match s.parse::<IpAddr>() {
                    Ok(IpAddr::V4(ipv4)) => Ok(Value::Bytes(ipv4.octets().to_vec())),
                    Ok(IpAddr::V6(ipv6)) => Ok(Value::Bytes(ipv6.octets().to_vec())),
                    Err(_) => Err(Error::InvalidQuery(format!("Invalid IP address: {}", s))),
                }
            }
            _ => Err(Error::InvalidQuery(
                "NET.IP_FROM_STRING expects a string argument".into(),
            )),
        }
    }

    fn fn_net_safe_ip_from_string(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(s) => {
                use std::net::IpAddr;
                match s.parse::<IpAddr>() {
                    Ok(IpAddr::V4(ipv4)) => Ok(Value::Bytes(ipv4.octets().to_vec())),
                    Ok(IpAddr::V6(ipv6)) => Ok(Value::Bytes(ipv6.octets().to_vec())),
                    Err(_) => Ok(Value::Null),
                }
            }
            _ => Ok(Value::Null),
        }
    }

    fn fn_net_ip_to_string(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(bytes) => {
                use std::net::{Ipv4Addr, Ipv6Addr};
                if bytes.len() == 4 {
                    let arr: [u8; 4] = bytes[..4].try_into().unwrap();
                    Ok(Value::String(Ipv4Addr::from(arr).to_string()))
                } else if bytes.len() == 16 {
                    let arr: [u8; 16] = bytes[..16].try_into().unwrap();
                    Ok(Value::String(Ipv6Addr::from(arr).to_string()))
                } else {
                    Err(Error::InvalidQuery(
                        "NET.IP_TO_STRING expects 4 or 16 bytes".into(),
                    ))
                }
            }
            _ => Err(Error::InvalidQuery(
                "NET.IP_TO_STRING expects a bytes argument".into(),
            )),
        }
    }

    fn fn_net_ipv4_from_int64(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Int64(n) => {
                let bytes = (*n as u32).to_be_bytes();
                Ok(Value::Bytes(bytes.to_vec()))
            }
            _ => Err(Error::InvalidQuery(
                "NET.IPV4_FROM_INT64 expects an integer argument".into(),
            )),
        }
    }

    fn fn_net_ipv4_to_int64(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(bytes) if bytes.len() == 4 => {
                let arr: [u8; 4] = bytes[..4].try_into().unwrap();
                let n = u32::from_be_bytes(arr);
                Ok(Value::Int64(n as i64))
            }
            _ => Err(Error::InvalidQuery(
                "NET.IPV4_TO_INT64 expects 4 bytes".into(),
            )),
        }
    }

    fn fn_net_host(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(url) => {
                if let Some(host_start) = url.find("://").map(|i| i + 3) {
                    let rest = &url[host_start..];
                    let host_end = rest
                        .find('/')
                        .or_else(|| rest.find('?'))
                        .or_else(|| rest.find('#'))
                        .unwrap_or(rest.len());
                    let host_port = &rest[..host_end];
                    let host = host_port.split(':').next().unwrap_or(host_port);
                    Ok(Value::String(host.to_lowercase()))
                } else {
                    let host = url.split('/').next().unwrap_or(url);
                    let host = host.split(':').next().unwrap_or(host);
                    Ok(Value::String(host.to_lowercase()))
                }
            }
            _ => Err(Error::InvalidQuery(
                "NET.HOST expects a string argument".into(),
            )),
        }
    }

    fn fn_net_public_suffix(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(url) => {
                let host_result = self.fn_net_host(args)?;
                if let Value::String(host) = host_result {
                    let parts: Vec<&str> = host.split('.').collect();
                    if parts.len() >= 2 {
                        Ok(Value::String(parts[parts.len() - 1].to_string()))
                    } else if parts.len() == 1 {
                        Ok(Value::String(parts[0].to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(Error::InvalidQuery(
                "NET.PUBLIC_SUFFIX expects a string argument".into(),
            )),
        }
    }

    fn fn_net_reg_domain(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(_) => {
                let host_result = self.fn_net_host(args)?;
                if let Value::String(host) = host_result {
                    let parts: Vec<&str> = host.split('.').collect();
                    if parts.len() >= 2 {
                        Ok(Value::String(format!(
                            "{}.{}",
                            parts[parts.len() - 2],
                            parts[parts.len() - 1]
                        )))
                    } else {
                        Ok(Value::String(host))
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(Error::InvalidQuery(
                "NET.REG_DOMAIN expects a string argument".into(),
            )),
        }
    }

    fn fn_st_geometrytype(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom_type = if wkt.starts_with("POINT") {
                    "Point"
                } else if wkt.starts_with("LINESTRING") {
                    "LineString"
                } else if wkt.starts_with("POLYGON") {
                    "Polygon"
                } else if wkt.starts_with("MULTIPOINT") {
                    "MultiPoint"
                } else if wkt.starts_with("MULTILINESTRING") {
                    "MultiLineString"
                } else if wkt.starts_with("MULTIPOLYGON") {
                    "MultiPolygon"
                } else if wkt.starts_with("GEOMETRYCOLLECTION") {
                    "GeometryCollection"
                } else {
                    "Unknown"
                };
                Ok(Value::String(geom_type.to_string()))
            }
            _ => Err(Error::InvalidQuery(
                "ST_GEOMETRYTYPE expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_dimension(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let dim = if wkt.starts_with("POINT") {
                    0
                } else if wkt.starts_with("LINESTRING") || wkt.starts_with("MULTILINESTRING") {
                    1
                } else if wkt.starts_with("POLYGON") || wkt.starts_with("MULTIPOLYGON") {
                    2
                } else {
                    -1
                };
                Ok(Value::Int64(dim))
            }
            _ => Err(Error::InvalidQuery(
                "ST_DIMENSION expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_within(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_WITHIN requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_contains(&geom2, &geom1);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_WITHIN expects geography arguments".into(),
            )),
        }
    }

    fn fn_st_dwithin(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "ST_DWITHIN requires two geography arguments and a distance".into(),
            ));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2), distance_val) => {
                let distance_limit = match distance_val {
                    Value::Float64(f) => f.0,
                    Value::Int64(i) => *i as f64,
                    _ => return Ok(Value::Bool(false)),
                };
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let distance = Self::geodesic_distance_between_geometries(&geom1, &geom2);
                Ok(Value::Bool(distance <= distance_limit))
            }
            _ => Err(Error::InvalidQuery(
                "ST_DWITHIN expects geography arguments".into(),
            )),
        }
    }

    fn fn_st_covers(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_COVERS requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_contains(&geom1, &geom2);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_COVERS expects geography arguments".into(),
            )),
        }
    }

    fn fn_st_coveredby(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_COVEREDBY requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_contains(&geom2, &geom1);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_COVEREDBY expects geography arguments".into(),
            )),
        }
    }

    fn fn_st_touches(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_TOUCHES requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = Self::geometry_touches(&geom1, &geom2);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_TOUCHES expects geography arguments".into(),
            )),
        }
    }

    fn geometry_touches(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> bool {
        match (geom1, geom2) {
            (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
                let points1: Vec<Point<f64>> = p1.exterior().points().collect();
                let points2: Vec<Point<f64>> = p2.exterior().points().collect();
                for p in &points1 {
                    if points2
                        .iter()
                        .any(|p2| (p.x() - p2.x()).abs() < 1e-10 && (p.y() - p2.y()).abs() < 1e-10)
                    {
                        return true;
                    }
                }
                false
            }
            _ => false,
        }
    }

    fn fn_st_disjoint(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_DISJOINT requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let result = !geom1.intersects(&geom2);
                Ok(Value::Bool(result))
            }
            _ => Err(Error::InvalidQuery(
                "ST_DISJOINT expects geography arguments".into(),
            )),
        }
    }

    fn fn_st_equals(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_EQUALS requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(a), Value::Geography(b)) => Ok(Value::Bool(a == b)),
            _ => Err(Error::InvalidQuery(
                "ST_EQUALS expects geography arguments".into(),
            )),
        }
    }

    fn fn_st_convexhull(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let hull = geom.convex_hull();
                Ok(Value::Geography(Self::geometry_to_wkt(&Geometry::Polygon(
                    hull,
                ))))
            }
            _ => Err(Error::InvalidQuery(
                "ST_CONVEXHULL expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_simplify(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::Null);
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Geography(wkt), tolerance_val) => {
                let epsilon = match tolerance_val {
                    Value::Float64(f) => f.0,
                    Value::Int64(i) => *i as f64,
                    _ => 0.0,
                };
                let epsilon_degrees = epsilon / 111_320.0;
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let simplified = match geom {
                    Geometry::LineString(ls) => {
                        let simplified = ls.simplify_vw(&epsilon_degrees);
                        Geometry::LineString(simplified)
                    }
                    Geometry::Polygon(poly) => {
                        let simplified_exterior = poly.exterior().simplify_vw(&epsilon_degrees);
                        Geometry::Polygon(Polygon::new(simplified_exterior, vec![]))
                    }
                    other => other,
                };
                Ok(Value::Geography(Self::geometry_to_wkt(&simplified)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_SIMPLIFY expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_snaptogrid(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::Null);
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Geography(wkt), grid_val) => {
                let grid_size = match grid_val {
                    Value::Float64(f) => f.0,
                    Value::Int64(i) => *i as f64,
                    _ => 1.0,
                };
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let snapped = Self::snap_geometry_to_grid(&geom, grid_size);
                Ok(Value::Geography(Self::geometry_to_wkt(&snapped)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_SNAPTOGRID expects a geography argument".into(),
            )),
        }
    }

    fn snap_geometry_to_grid(geom: &Geometry<f64>, grid_size: f64) -> Geometry<f64> {
        let snap = |v: f64| -> f64 { (v / grid_size).round() * grid_size };
        match geom {
            Geometry::Point(p) => Geometry::Point(Point::new(snap(p.x()), snap(p.y()))),
            Geometry::LineString(ls) => {
                let coords: Vec<Coord<f64>> = ls
                    .coords()
                    .map(|c| Coord {
                        x: snap(c.x),
                        y: snap(c.y),
                    })
                    .collect();
                Geometry::LineString(LineString::new(coords))
            }
            Geometry::Polygon(poly) => {
                let exterior: Vec<Coord<f64>> = poly
                    .exterior()
                    .coords()
                    .map(|c| Coord {
                        x: snap(c.x),
                        y: snap(c.y),
                    })
                    .collect();
                Geometry::Polygon(Polygon::new(LineString::new(exterior), vec![]))
            }
            other => other.clone(),
        }
    }

    fn fn_st_boundary(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let boundary = match geom {
                    Geometry::Polygon(poly) => Geometry::LineString(poly.exterior().clone()),
                    Geometry::LineString(ls) => {
                        if ls.0.len() >= 2 {
                            let start = ls.0.first().unwrap();
                            let end = ls.0.last().unwrap();
                            if start == end {
                                Geometry::GeometryCollection(
                                    geo_types::GeometryCollection::new_from(vec![]),
                                )
                            } else {
                                Geometry::MultiPoint(geo_types::MultiPoint::new(vec![
                                    Point::new(start.x, start.y),
                                    Point::new(end.x, end.y),
                                ]))
                            }
                        } else {
                            Geometry::GeometryCollection(geo_types::GeometryCollection::new_from(
                                vec![],
                            ))
                        }
                    }
                    _ => Geometry::GeometryCollection(geo_types::GeometryCollection::new_from(
                        vec![],
                    )),
                };
                Ok(Value::Geography(Self::geometry_to_wkt(&boundary)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_BOUNDARY expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_startpoint(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
                let inner = &wkt[11..wkt.len() - 1];
                if let Some(first_comma) = inner.find(',') {
                    let first_point = inner[..first_comma].trim();
                    Ok(Value::Geography(format!("POINT({})", first_point)))
                } else {
                    Ok(Value::Geography(format!("POINT({})", inner.trim())))
                }
            }
            Value::Geography(_) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery(
                "ST_STARTPOINT expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_endpoint(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
                let inner = &wkt[11..wkt.len() - 1];
                if let Some(last_comma) = inner.rfind(',') {
                    let last_point = inner[last_comma + 1..].trim();
                    Ok(Value::Geography(format!("POINT({})", last_point)))
                } else {
                    Ok(Value::Geography(format!("POINT({})", inner.trim())))
                }
            }
            Value::Geography(_) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery(
                "ST_ENDPOINT expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_pointn(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_POINTN requires a geography and an index".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt), Value::Int64(n)) if wkt.starts_with("LINESTRING(") => {
                let inner = &wkt[11..wkt.len() - 1];
                let points: Vec<&str> = inner.split(',').collect();
                let idx = (*n - 1) as usize;
                if idx < points.len() {
                    Ok(Value::Geography(format!("POINT({})", points[idx].trim())))
                } else {
                    Ok(Value::Null)
                }
            }
            (Value::Geography(_), Value::Int64(_)) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery(
                "ST_POINTN expects geography and integer arguments".into(),
            )),
        }
    }

    fn fn_st_iscollection(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) => {
                let is_collection =
                    wkt.starts_with("MULTI") || wkt.starts_with("GEOMETRYCOLLECTION");
                Ok(Value::Bool(is_collection))
            }
            _ => Err(Error::InvalidQuery(
                "ST_ISCOLLECTION expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_isring(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) if wkt.starts_with("LINESTRING(") => {
                let inner = &wkt[11..wkt.len() - 1];
                let points: Vec<&str> = inner.split(',').collect();
                if points.len() >= 4 {
                    let first = points.first().map(|s| s.trim());
                    let last = points.last().map(|s| s.trim());
                    Ok(Value::Bool(first == last))
                } else {
                    Ok(Value::Bool(false))
                }
            }
            Value::Geography(_) => Ok(Value::Bool(false)),
            _ => Err(Error::InvalidQuery(
                "ST_ISRING expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_maxdistance(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ST_MAXDISTANCE requires two geography arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt1), Value::Geography(wkt2)) => {
                let geom1: Geometry<f64> = Geometry::try_from_wkt_str(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let geom2: Geometry<f64> = Geometry::try_from_wkt_str(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let points1 = Self::extract_points(&geom1);
                let points2 = Self::extract_points(&geom2);
                let mut max_distance = 0.0_f64;
                for p1 in &points1 {
                    for p2 in &points2 {
                        let dist = p1.geodesic_distance(p2);
                        if dist > max_distance {
                            max_distance = dist;
                        }
                    }
                }
                Ok(Value::Float64(OrderedFloat(max_distance)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_MAXDISTANCE expects geography arguments".into(),
            )),
        }
    }

    fn fn_st_geohash(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        let max_len = if args.len() > 1 {
            match &args[1] {
                Value::Int64(i) => *i as usize,
                _ => 12,
            }
        } else {
            12
        };
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Geography(wkt) if wkt.starts_with("POINT(") => {
                let inner = &wkt[6..wkt.len() - 1];
                let parts: Vec<&str> = inner.split_whitespace().collect();
                if parts.len() >= 2 {
                    let lon: f64 = parts[0].parse().unwrap_or(0.0);
                    let lat: f64 = parts[1].parse().unwrap_or(0.0);
                    let hash = geohash::encode(geohash::Coord { x: lon, y: lat }, max_len)
                        .unwrap_or_else(|_| "".to_string());
                    Ok(Value::String(hash))
                } else {
                    Ok(Value::Null)
                }
            }
            Value::Geography(_) => Ok(Value::Null),
            _ => Err(Error::InvalidQuery(
                "ST_GEOHASH expects a geography argument".into(),
            )),
        }
    }

    fn fn_st_geogpointfromgeohash(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(hash) => match geohash::decode(hash) {
                Ok((coord, _, _)) => {
                    Ok(Value::Geography(format!("POINT({} {})", coord.x, coord.y)))
                }
                Err(_) => Ok(Value::Null),
            },
            _ => Err(Error::InvalidQuery(
                "ST_GEOGPOINTFROMGEOHASH expects a string argument".into(),
            )),
        }
    }

    fn fn_st_bufferwithtolerance(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::Null);
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Geography(wkt), Value::Float64(_))
            | (Value::Geography(wkt), Value::Int64(_)) => {
                let distance_meters = match &args[1] {
                    Value::Float64(f) => f.0,
                    Value::Int64(i) => *i as f64,
                    _ => 0.0,
                };
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid WKT: {}", e)))?;
                let buffered = Self::create_buffer(&geom, distance_meters);
                Ok(Value::Geography(Self::geometry_to_wkt(&buffered)))
            }
            _ => Err(Error::InvalidQuery(
                "ST_BUFFERWITHTOLERANCE expects a geography argument".into(),
            )),
        }
    }

    fn fn_timestamp_diff(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "TIMESTAMP_DIFF requires timestamp1, timestamp2, and date_part".into(),
            ));
        }
        let date_part = args[2].as_str().unwrap_or("SECOND").to_uppercase();
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Timestamp(ts1), Value::Timestamp(ts2)) => {
                let diff_micros = ts1.timestamp_micros() - ts2.timestamp_micros();
                let result = match date_part.as_str() {
                    "MICROSECOND" => diff_micros,
                    "MILLISECOND" => diff_micros / 1_000,
                    "SECOND" => diff_micros / 1_000_000,
                    "MINUTE" => diff_micros / 60_000_000,
                    "HOUR" => diff_micros / 3_600_000_000,
                    "DAY" => diff_micros / 86_400_000_000,
                    _ => diff_micros / 1_000_000,
                };
                Ok(Value::Int64(result))
            }
            (Value::DateTime(dt1), Value::DateTime(dt2)) => {
                let diff_micros =
                    dt1.and_utc().timestamp_micros() - dt2.and_utc().timestamp_micros();
                let result = match date_part.as_str() {
                    "MICROSECOND" => diff_micros,
                    "MILLISECOND" => diff_micros / 1_000,
                    "SECOND" => diff_micros / 1_000_000,
                    "MINUTE" => diff_micros / 60_000_000,
                    "HOUR" => diff_micros / 3_600_000_000,
                    "DAY" => diff_micros / 86_400_000_000,
                    _ => diff_micros / 1_000_000,
                };
                Ok(Value::Int64(result))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_DIFF expects timestamp arguments".into(),
            )),
        }
    }

    fn fn_datetime_add(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATETIME_ADD requires datetime and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::DateTime(dt), Value::Interval(iv)) => {
                let new_dt = add_interval_to_datetime(dt, iv)?;
                Ok(Value::DateTime(new_dt))
            }
            _ => Err(Error::InvalidQuery(
                "DATETIME_ADD expects datetime and interval arguments".into(),
            )),
        }
    }

    fn fn_datetime_sub(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "DATETIME_SUB requires datetime and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::DateTime(dt), Value::Interval(iv)) => {
                let neg_iv = negate_interval(iv);
                let new_dt = add_interval_to_datetime(dt, &neg_iv)?;
                Ok(Value::DateTime(new_dt))
            }
            _ => Err(Error::InvalidQuery(
                "DATETIME_SUB expects datetime and interval arguments".into(),
            )),
        }
    }

    fn fn_datetime_diff(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "DATETIME_DIFF requires datetime1, datetime2, and date_part".into(),
            ));
        }
        let date_part = args[2].as_str().unwrap_or("SECOND").to_uppercase();
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::DateTime(dt1), Value::DateTime(dt2)) => {
                let diff_micros =
                    dt1.and_utc().timestamp_micros() - dt2.and_utc().timestamp_micros();
                let result = match date_part.as_str() {
                    "MICROSECOND" => diff_micros,
                    "MILLISECOND" => diff_micros / 1_000,
                    "SECOND" => diff_micros / 1_000_000,
                    "MINUTE" => diff_micros / 60_000_000,
                    "HOUR" => diff_micros / 3_600_000_000,
                    "DAY" => diff_micros / 86_400_000_000,
                    _ => diff_micros / 1_000_000,
                };
                Ok(Value::Int64(result))
            }
            _ => Err(Error::InvalidQuery(
                "DATETIME_DIFF expects datetime arguments".into(),
            )),
        }
    }

    fn fn_timestamp_add(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIMESTAMP_ADD requires timestamp and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Timestamp(ts), Value::Interval(iv)) => {
                let nanos_to_add = iv.nanos
                    + iv.days as i64 * 86_400_000_000_000
                    + iv.months as i64 * 30 * 86_400_000_000_000;
                let new_ts = *ts + chrono::Duration::nanoseconds(nanos_to_add);
                Ok(Value::Timestamp(new_ts))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_ADD expects timestamp and interval arguments".into(),
            )),
        }
    }

    fn fn_timestamp_sub(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIMESTAMP_SUB requires timestamp and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Timestamp(ts), Value::Interval(iv)) => {
                let nanos_to_sub = iv.nanos
                    + iv.days as i64 * 86_400_000_000_000
                    + iv.months as i64 * 30 * 86_400_000_000_000;
                let new_ts = *ts - chrono::Duration::nanoseconds(nanos_to_sub);
                Ok(Value::Timestamp(new_ts))
            }
            _ => Err(Error::InvalidQuery(
                "TIMESTAMP_SUB expects timestamp and interval arguments".into(),
            )),
        }
    }

    fn fn_time_add(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIME_ADD requires time and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Time(t), Value::Interval(iv)) => {
                let nanos_to_add = iv.nanos;
                let total_nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                    + t.nanosecond() as i64
                    + nanos_to_add;
                let total_nanos = total_nanos.rem_euclid(86_400_000_000_000);
                let secs = (total_nanos / 1_000_000_000) as u32;
                let nanos = (total_nanos % 1_000_000_000) as u32;
                let new_time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .unwrap_or(*t);
                Ok(Value::Time(new_time))
            }
            _ => Err(Error::InvalidQuery(
                "TIME_ADD expects time and interval arguments".into(),
            )),
        }
    }

    fn fn_time_sub(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "TIME_SUB requires time and interval arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Time(t), Value::Interval(iv)) => {
                let nanos_to_sub = iv.nanos;
                let total_nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                    + t.nanosecond() as i64
                    - nanos_to_sub;
                let total_nanos = total_nanos.rem_euclid(86_400_000_000_000);
                let secs = (total_nanos / 1_000_000_000) as u32;
                let nanos = (total_nanos % 1_000_000_000) as u32;
                let new_time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .unwrap_or(*t);
                Ok(Value::Time(new_time))
            }
            _ => Err(Error::InvalidQuery(
                "TIME_SUB expects time and interval arguments".into(),
            )),
        }
    }

    fn fn_time_diff(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "TIME_DIFF requires time1, time2, and date_part arguments".into(),
            ));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) => Ok(Value::Null),
            (Value::Time(t1), Value::Time(t2), Value::String(part)) => {
                let t1_nanos =
                    t1.num_seconds_from_midnight() as i64 * 1_000_000_000 + t1.nanosecond() as i64;
                let t2_nanos =
                    t2.num_seconds_from_midnight() as i64 * 1_000_000_000 + t2.nanosecond() as i64;
                let diff_nanos = t1_nanos - t2_nanos;
                let result = match part.to_uppercase().as_str() {
                    "HOUR" => diff_nanos / 3_600_000_000_000,
                    "MINUTE" => diff_nanos / 60_000_000_000,
                    "SECOND" => diff_nanos / 1_000_000_000,
                    "MILLISECOND" => diff_nanos / 1_000_000,
                    "MICROSECOND" => diff_nanos / 1_000,
                    "NANOSECOND" => diff_nanos,
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid date part for TIME_DIFF: {}",
                            part
                        )));
                    }
                };
                Ok(Value::Int64(result))
            }
            _ => Err(Error::InvalidQuery(
                "TIME_DIFF expects time arguments".into(),
            )),
        }
    }

    fn fn_all(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Bool(true));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Array(arr) => {
                for v in arr {
                    match v {
                        Value::Null => return Ok(Value::Null),
                        Value::Bool(false) => return Ok(Value::Bool(false)),
                        Value::Bool(true) => {}
                        _ => return Ok(Value::Bool(false)),
                    }
                }
                Ok(Value::Bool(true))
            }
            Value::Bool(b) => Ok(Value::Bool(*b)),
            _ => Ok(Value::Bool(true)),
        }
    }

    fn fn_any(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Bool(false));
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Array(arr) => {
                let mut has_null = false;
                for v in arr {
                    match v {
                        Value::Null => has_null = true,
                        Value::Bool(true) => return Ok(Value::Bool(true)),
                        _ => {}
                    }
                }
                if has_null {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Bool(false))
                }
            }
            Value::Bool(b) => Ok(Value::Bool(*b)),
            _ => Ok(Value::Bool(false)),
        }
    }

    fn fn_offset(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "OFFSET requires array and index arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(arr), Value::Int64(idx)) => {
                if *idx < 0 || *idx as usize >= arr.len() {
                    Err(Error::InvalidQuery(format!(
                        "Array index {} out of bounds for array of length {}",
                        idx,
                        arr.len()
                    )))
                } else {
                    Ok(arr[*idx as usize].clone())
                }
            }
            _ => Err(Error::InvalidQuery(
                "OFFSET requires array and integer arguments".into(),
            )),
        }
    }

    fn fn_ordinal(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "ORDINAL requires array and index arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Array(arr), Value::Int64(idx)) => {
                let zero_based = *idx - 1;
                if zero_based < 0 || zero_based as usize >= arr.len() {
                    Err(Error::InvalidQuery(format!(
                        "Array ordinal {} out of bounds for array of length {}",
                        idx,
                        arr.len()
                    )))
                } else {
                    Ok(arr[zero_based as usize].clone())
                }
            }
            _ => Err(Error::InvalidQuery(
                "ORDINAL requires array and integer arguments".into(),
            )),
        }
    }

    fn fn_json_array(&self, args: &[Value]) -> Result<Value> {
        let json_values: Vec<serde_json::Value> = args
            .iter()
            .map(|v| match v {
                Value::Null => serde_json::Value::Null,
                Value::Bool(b) => serde_json::Value::Bool(*b),
                Value::Int64(n) => serde_json::Value::Number((*n).into()),
                Value::Float64(f) => serde_json::Number::from_f64(f.0)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
                Value::String(s) => serde_json::Value::String(s.clone()),
                _ => serde_json::Value::Null,
            })
            .collect();
        Ok(Value::Json(serde_json::Value::Array(json_values)))
    }

    fn fn_json_object(&self, args: &[Value]) -> Result<Value> {
        let mut obj = serde_json::Map::new();
        for chunk in args.chunks(2) {
            if chunk.len() == 2 {
                if let Value::String(key) = &chunk[0] {
                    let val = match &chunk[1] {
                        Value::Null => serde_json::Value::Null,
                        Value::Bool(b) => serde_json::Value::Bool(*b),
                        Value::Int64(n) => serde_json::Value::Number((*n).into()),
                        Value::Float64(f) => serde_json::Number::from_f64(f.0)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null),
                        Value::String(s) => serde_json::Value::String(s.clone()),
                        _ => serde_json::Value::Null,
                    };
                    obj.insert(key.clone(), val);
                }
            }
        }
        Ok(Value::Json(serde_json::Value::Object(obj)))
    }

    fn fn_json_set(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => serde_json::from_str(s).unwrap_or(serde_json::Value::Null),
            _ => return Ok(Value::Null),
        };
        let path = match &args[1] {
            Value::String(p) => p.clone(),
            _ => return Ok(Value::Json(json_val)),
        };
        let new_value = self.value_to_json(&args[2]);

        let key = if path.starts_with("$.") {
            path[2..].to_string()
        } else if path.starts_with("$[") || path.starts_with('$') {
            path[1..].to_string()
        } else {
            path.clone()
        };

        let mut result = json_val;
        if let serde_json::Value::Object(ref mut obj) = result {
            obj.insert(key, new_value);
        }
        Ok(Value::Json(result))
    }

    fn value_to_json(&self, val: &Value) -> serde_json::Value {
        match val {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int64(n) => serde_json::json!(*n),
            Value::Float64(f) => serde_json::json!(f.0),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Json(j) => j.clone(),
            Value::Numeric(n) => serde_json::json!(n.to_string()),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| self.value_to_json(v)).collect())
            }
            _ => serde_json::Value::Null,
        }
    }

    fn fn_json_remove(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => serde_json::from_str(s).unwrap_or(serde_json::Value::Null),
            _ => return Ok(Value::Null),
        };
        let path = match &args[1] {
            Value::String(p) => p.clone(),
            _ => return Ok(Value::Json(json_val)),
        };

        let key = if path.starts_with("$.") {
            path[2..].to_string()
        } else if path.starts_with("$[") || path.starts_with('$') {
            path[1..].to_string()
        } else {
            path.clone()
        };

        let mut result = json_val;
        if let serde_json::Value::Object(ref mut obj) = result {
            obj.remove(&key);
        }
        Ok(Value::Json(result))
    }

    fn fn_json_strip_nulls(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(j) => {
                fn strip_nulls(v: &serde_json::Value) -> serde_json::Value {
                    match v {
                        serde_json::Value::Object(obj) => {
                            let filtered: serde_json::Map<String, serde_json::Value> = obj
                                .iter()
                                .filter(|(_, val)| !val.is_null())
                                .map(|(k, val)| (k.clone(), strip_nulls(val)))
                                .collect();
                            serde_json::Value::Object(filtered)
                        }
                        serde_json::Value::Array(arr) => {
                            serde_json::Value::Array(arr.iter().map(strip_nulls).collect())
                        }
                        other => other.clone(),
                    }
                }
                Ok(Value::Json(strip_nulls(j)))
            }
            Value::String(s) => {
                let parsed: serde_json::Value =
                    serde_json::from_str(s).unwrap_or(serde_json::Value::Null);
                fn strip_nulls(v: &serde_json::Value) -> serde_json::Value {
                    match v {
                        serde_json::Value::Object(obj) => {
                            let filtered: serde_json::Map<String, serde_json::Value> = obj
                                .iter()
                                .filter(|(_, val)| !val.is_null())
                                .map(|(k, val)| (k.clone(), strip_nulls(val)))
                                .collect();
                            serde_json::Value::Object(filtered)
                        }
                        serde_json::Value::Array(arr) => {
                            serde_json::Value::Array(arr.iter().map(strip_nulls).collect())
                        }
                        other => other.clone(),
                    }
                }
                Ok(Value::Json(strip_nulls(&parsed)))
            }
            _ => Ok(Value::Null),
        }
    }

    fn fn_json_query_array(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(j) => j,
                Err(_) => return Ok(Value::Array(vec![])),
            },
            _ => return Ok(Value::Null),
        };
        let json_to_extract = if args.len() > 1 {
            if let Value::String(path) = &args[1] {
                match extract_json_path(&json_val, path)? {
                    Some(v) => v,
                    None => return Ok(Value::Array(vec![])),
                }
            } else {
                json_val
            }
        } else {
            json_val
        };
        if let Some(arr) = json_to_extract.as_array() {
            let values: Vec<Value> = arr.iter().map(|v| Value::Json(v.clone())).collect();
            Ok(Value::Array(values))
        } else {
            Ok(Value::Array(vec![]))
        }
    }

    fn fn_json_value_array(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(j) => j,
                Err(_) => return Ok(Value::Array(vec![])),
            },
            _ => return Ok(Value::Null),
        };
        let json_to_extract = if args.len() > 1 {
            if let Value::String(path) = &args[1] {
                match extract_json_path(&json_val, path)? {
                    Some(v) => v,
                    None => return Ok(Value::Array(vec![])),
                }
            } else {
                json_val
            }
        } else {
            json_val
        };
        if let Some(arr) = json_to_extract.as_array() {
            let values: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => Value::String(s.clone()),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Value::Int64(i)
                        } else if let Some(f) = n.as_f64() {
                            Value::Float64(OrderedFloat(f))
                        } else {
                            Value::Null
                        }
                    }
                    serde_json::Value::Bool(b) => Value::Bool(*b),
                    _ => Value::Null,
                })
                .collect();
            Ok(Value::Array(values))
        } else {
            Ok(Value::Array(vec![]))
        }
    }

    fn fn_json_extract_string_array(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        let json_val = match &args[0] {
            Value::Null => return Ok(Value::Null),
            Value::Json(j) => j.clone(),
            Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(j) => j,
                Err(_) => return Ok(Value::Array(vec![])),
            },
            _ => return Ok(Value::Null),
        };
        let json_to_extract = if args.len() > 1 {
            if let Value::String(path) = &args[1] {
                match extract_json_path(&json_val, path)? {
                    Some(v) => v,
                    None => return Ok(Value::Array(vec![])),
                }
            } else {
                json_val
            }
        } else {
            json_val
        };
        if let Some(arr) = json_to_extract.as_array() {
            let values: Vec<Value> = arr
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => Value::String(s.clone()),
                    serde_json::Value::Number(n) => Value::String(n.to_string()),
                    serde_json::Value::Bool(b) => Value::String(b.to_string()),
                    serde_json::Value::Null => Value::Null,
                    _ => Value::String(v.to_string()),
                })
                .collect();
            Ok(Value::Array(values))
        } else {
            Ok(Value::Array(vec![]))
        }
    }

    fn fn_json_keys(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Json(j) => {
                if let Some(obj) = j.as_object() {
                    let keys: Vec<Value> = obj.keys().map(|k| Value::String(k.clone())).collect();
                    Ok(Value::Array(keys))
                } else {
                    Ok(Value::Array(vec![]))
                }
            }
            Value::String(s) => {
                if let Ok(j) = serde_json::from_str::<serde_json::Value>(s) {
                    if let Some(obj) = j.as_object() {
                        let keys: Vec<Value> =
                            obj.keys().map(|k| Value::String(k.clone())).collect();
                        return Ok(Value::Array(keys));
                    }
                }
                Ok(Value::Array(vec![]))
            }
            _ => Ok(Value::Null),
        }
    }

    fn fn_regexp_instr(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "REGEXP_INSTR requires source and pattern arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(source), Value::String(pattern)) => match regex::Regex::new(pattern) {
                Ok(re) => {
                    if let Some(m) = re.find(source) {
                        Ok(Value::Int64((m.start() + 1) as i64))
                    } else {
                        Ok(Value::Int64(0))
                    }
                }
                Err(_) => Ok(Value::Int64(0)),
            },
            _ => Err(Error::InvalidQuery(
                "REGEXP_INSTR expects string arguments".into(),
            )),
        }
    }

    fn fn_net_ip_in_net(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "NET.IP_IN_NET requires IP and network arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Bytes(ip_bytes), Value::String(network)) => {
                let parts: Vec<&str> = network.split('/').collect();
                if parts.len() != 2 {
                    return Ok(Value::Bool(false));
                }
                let prefix_len: u8 = match parts[1].parse() {
                    Ok(p) => p,
                    Err(_) => return Ok(Value::Bool(false)),
                };
                let network_octets: Vec<u8> =
                    parts[0].split('.').filter_map(|s| s.parse().ok()).collect();
                if network_octets.len() != 4 || ip_bytes.len() != 4 {
                    return Ok(Value::Bool(false));
                }
                let full_bytes = (prefix_len / 8) as usize;
                let remaining_bits = prefix_len % 8;
                for i in 0..full_bytes {
                    if ip_bytes[i] != network_octets[i] {
                        return Ok(Value::Bool(false));
                    }
                }
                if remaining_bits > 0 && full_bytes < 4 {
                    let mask = !((1u8 << (8 - remaining_bits)) - 1);
                    if (ip_bytes[full_bytes] & mask) != (network_octets[full_bytes] & mask) {
                        return Ok(Value::Bool(false));
                    }
                }
                Ok(Value::Bool(true))
            }
            _ => Err(Error::InvalidQuery(
                "NET.IP_IN_NET expects bytes and string arguments".into(),
            )),
        }
    }

    fn fn_net_ip_is_private(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(bytes) if bytes.len() == 4 => {
                let is_private = (bytes[0] == 10)
                    || (bytes[0] == 172 && (16..=31).contains(&bytes[1]))
                    || (bytes[0] == 192 && bytes[1] == 168);
                Ok(Value::Bool(is_private))
            }
            Value::Bytes(_) => Ok(Value::Bool(false)),
            _ => Err(Error::InvalidQuery(
                "NET.IP_IS_PRIVATE expects bytes argument".into(),
            )),
        }
    }

    fn fn_net_ip_trunc(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "NET.IP_TRUNC requires IP and prefix length arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Bytes(bytes), Value::Int64(prefix_len)) => {
                let mut result = bytes.clone();
                let full_bytes = (*prefix_len / 8) as usize;
                let remaining_bits = (*prefix_len % 8) as u8;

                for (i, byte) in result.iter_mut().enumerate() {
                    if i < full_bytes {
                        continue;
                    } else if i == full_bytes && remaining_bits > 0 {
                        let mask = !((1u8 << (8 - remaining_bits)) - 1);
                        *byte &= mask;
                    } else {
                        *byte = 0;
                    }
                }
                Ok(Value::Bytes(result))
            }
            _ => Err(Error::InvalidQuery(
                "NET.IP_TRUNC expects bytes and integer arguments".into(),
            )),
        }
    }

    fn fn_net_ip_net_mask(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "NET.IP_NET_MASK requires num_bytes and prefix_length arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Int64(num_bytes), Value::Int64(prefix_len)) => {
                let mut mask = vec![0u8; *num_bytes as usize];
                let full_bytes = (*prefix_len / 8) as usize;
                let remaining_bits = (*prefix_len % 8) as u8;

                for (i, byte) in mask.iter_mut().enumerate() {
                    if i < full_bytes {
                        *byte = 0xFF;
                    } else if i == full_bytes && remaining_bits > 0 {
                        *byte = !((1u8 << (8 - remaining_bits)) - 1);
                    }
                }
                Ok(Value::Bytes(mask))
            }
            _ => Err(Error::InvalidQuery(
                "NET.IP_NET_MASK expects integer arguments".into(),
            )),
        }
    }

    fn fn_net_make_net(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "NET.MAKE_NET requires IP and prefix length arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Bytes(bytes), Value::Int64(prefix_len)) => {
                let ip_str = if bytes.len() == 4 {
                    format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3])
                } else if bytes.len() == 16 {
                    let parts: Vec<String> = bytes
                        .chunks(2)
                        .map(|chunk| format!("{:x}{:02x}", chunk[0], chunk[1]))
                        .collect();
                    parts.join(":")
                } else {
                    return Err(Error::InvalidQuery(
                        "NET.MAKE_NET expects 4 or 16 byte IP address".into(),
                    ));
                };
                Ok(Value::String(format!("{}/{}", ip_str, prefix_len)))
            }
            _ => Err(Error::InvalidQuery(
                "NET.MAKE_NET expects bytes and integer arguments".into(),
            )),
        }
    }

    fn fn_range_contains(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "RANGE_CONTAINS requires range and value arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Range(r), val) => {
                let in_range = match (&r.start, &r.end) {
                    (Some(start), Some(end)) => val >= start && val < end,
                    (Some(start), None) => val >= start,
                    (None, Some(end)) => val < end,
                    (None, None) => true,
                };
                Ok(Value::Bool(in_range))
            }
            _ => Ok(Value::Bool(false)),
        }
    }

    fn fn_range_overlaps(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "RANGE_OVERLAPS requires two range arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Range(r1), Value::Range(r2)) => {
                let (start1, end1) = (r1.start.as_deref(), r1.end.as_deref());
                let (start2, end2) = (r2.start.as_deref(), r2.end.as_deref());
                let overlaps = match (start1, end1, start2, end2) {
                    (Some(s1), Some(e1), Some(s2), Some(e2)) => {
                        self.value_less_than(s1, e2) && self.value_less_than(s2, e1)
                    }
                    (None, Some(e1), Some(s2), _) => self.value_less_than(s2, e1),
                    (Some(s1), None, _, Some(e2)) => self.value_less_than(s1, e2),
                    (None, _, _, None) | (_, None, None, _) => true,
                    _ => false,
                };
                Ok(Value::Bool(overlaps))
            }
            _ => Err(Error::InvalidQuery(
                "RANGE_OVERLAPS expects range arguments".into(),
            )),
        }
    }

    fn value_less_than(&self, a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Int64(x), Value::Int64(y)) => x < y,
            (Value::Float64(x), Value::Float64(y)) => x < y,
            (Value::Date(x), Value::Date(y)) => x < y,
            (Value::DateTime(x), Value::DateTime(y)) => x < y,
            (Value::Timestamp(x), Value::Timestamp(y)) => x < y,
            (Value::Time(x), Value::Time(y)) => x < y,
            (Value::String(x), Value::String(y)) => x < y,
            _ => false,
        }
    }

    fn fn_range_start(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Range(range) => match &range.start {
                Some(v) => Ok((**v).clone()),
                None => Ok(Value::Null),
            },
            _ => Err(Error::InvalidQuery(
                "RANGE_START expects a range argument".into(),
            )),
        }
    }

    fn fn_range_end(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Range(range) => match &range.end {
                Some(v) => Ok((**v).clone()),
                None => Ok(Value::Null),
            },
            _ => Err(Error::InvalidQuery(
                "RANGE_END expects a range argument".into(),
            )),
        }
    }

    fn fn_range_intersect(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "RANGE_INTERSECT requires two range arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Range(r1), Value::Range(r2)) => {
                use yachtsql_common::types::RangeValue;
                let start = match (&r1.start, &r2.start) {
                    (Some(a), Some(b)) if **a > **b => Some(Box::new((**a).clone())),
                    (_, Some(b)) => Some(Box::new((**b).clone())),
                    (Some(a), None) => Some(Box::new((**a).clone())),
                    (None, None) => None,
                };
                let end = match (&r1.end, &r2.end) {
                    (Some(a), Some(b)) if **a < **b => Some(Box::new((**a).clone())),
                    (_, Some(b)) => Some(Box::new((**b).clone())),
                    (Some(a), None) => Some(Box::new((**a).clone())),
                    (None, None) => None,
                };
                if let (Some(s), Some(e)) = (&start, &end) {
                    if **s > **e {
                        return Ok(Value::Null);
                    }
                }
                Ok(Value::Range(RangeValue { start, end }))
            }
            _ => Err(Error::InvalidQuery(
                "RANGE_INTERSECT expects range arguments".into(),
            )),
        }
    }

    fn fn_generate_range_array(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "GENERATE_RANGE_ARRAY requires at least 2 arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Range(range), Value::Interval(interval)) => {
                use yachtsql_common::types::RangeValue;
                let mut results = vec![];
                let start = range.start.as_deref();
                let end = range.end.as_deref();
                match (start, end) {
                    (Some(Value::Date(start_date)), Some(Value::Date(end_date))) => {
                        let mut current = *start_date;
                        while current < *end_date {
                            let mut next = current;
                            if interval.days != 0 {
                                next += chrono::Duration::days(interval.days as i64);
                            }
                            if interval.months != 0 {
                                let year = next.year();
                                let month = next.month() as i32 + interval.months;
                                let new_year = year + (month - 1) / 12;
                                let new_month = ((month - 1) % 12 + 12) % 12 + 1;
                                next = NaiveDate::from_ymd_opt(
                                    new_year,
                                    new_month as u32,
                                    next.day().min(28),
                                )
                                .unwrap_or(next);
                            }
                            let range_end = if next > *end_date { *end_date } else { next };
                            results.push(Value::Range(RangeValue {
                                start: Some(Box::new(Value::Date(current))),
                                end: Some(Box::new(Value::Date(range_end))),
                            }));
                            current = next;
                        }
                    }
                    (Some(Value::Int64(start_val)), Some(Value::Int64(end_val))) => {
                        let step = if interval.days != 0 {
                            interval.days as i64
                        } else {
                            1
                        };
                        let mut current = *start_val;
                        while current < *end_val {
                            let next = current + step;
                            let range_end = if next > *end_val { *end_val } else { next };
                            results.push(Value::Range(RangeValue {
                                start: Some(Box::new(Value::Int64(current))),
                                end: Some(Box::new(Value::Int64(range_end))),
                            }));
                            current = next;
                        }
                    }
                    _ => return Ok(Value::Array(vec![])),
                }
                Ok(Value::Array(results))
            }
            _ => Ok(Value::Array(vec![])),
        }
    }

    fn fn_lax_int64(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Int64(n) => Ok(Value::Int64(*n)),
            Value::Float64(f) => Ok(Value::Int64(f.0 as i64)),
            Value::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
            Value::String(s) => {
                if let Ok(n) = s.trim().parse::<i64>() {
                    Ok(Value::Int64(n))
                } else {
                    Ok(Value::Null)
                }
            }
            Value::Json(j) => {
                if let Some(n) = j.as_i64() {
                    Ok(Value::Int64(n))
                } else if let Some(f) = j.as_f64() {
                    Ok(Value::Int64(f as i64))
                } else if let Some(s) = j.as_str() {
                    if let Ok(n) = s.trim().parse::<i64>() {
                        Ok(Value::Int64(n))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Ok(Value::Null),
        }
    }

    fn fn_lax_float64(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Float64(f) => Ok(Value::Float64(*f)),
            Value::Int64(n) => Ok(Value::Float64(OrderedFloat(*n as f64))),
            Value::Bool(b) => Ok(Value::Float64(OrderedFloat(if *b { 1.0 } else { 0.0 }))),
            Value::String(s) => {
                if let Ok(f) = s.trim().parse::<f64>() {
                    Ok(Value::Float64(OrderedFloat(f)))
                } else {
                    Ok(Value::Null)
                }
            }
            Value::Json(j) => {
                if let Some(f) = j.as_f64() {
                    Ok(Value::Float64(OrderedFloat(f)))
                } else if let Some(s) = j.as_str() {
                    if let Ok(f) = s.trim().parse::<f64>() {
                        Ok(Value::Float64(OrderedFloat(f)))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Ok(Value::Null),
        }
    }

    fn fn_lax_bool(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::Int64(n) => Ok(Value::Bool(*n != 0)),
            Value::Float64(f) => Ok(Value::Bool(f.0 != 0.0)),
            Value::String(s) => {
                let lower = s.trim().to_lowercase();
                if lower == "true" || lower == "1" {
                    Ok(Value::Bool(true))
                } else if lower == "false" || lower == "0" {
                    Ok(Value::Bool(false))
                } else {
                    Ok(Value::Null)
                }
            }
            Value::Json(j) => {
                if let Some(b) = j.as_bool() {
                    Ok(Value::Bool(b))
                } else if let Some(s) = j.as_str() {
                    let lower = s.trim().to_lowercase();
                    if lower == "true" || lower == "1" {
                        Ok(Value::Bool(true))
                    } else if lower == "false" || lower == "0" {
                        Ok(Value::Bool(false))
                    } else {
                        Ok(Value::Null)
                    }
                } else if let Some(n) = j.as_i64() {
                    Ok(Value::Bool(n != 0))
                } else if let Some(n) = j.as_f64() {
                    Ok(Value::Bool(n != 0.0))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Ok(Value::Null),
        }
    }

    fn fn_lax_string(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(s) => Ok(Value::String(s.clone())),
            Value::Int64(n) => Ok(Value::String(n.to_string())),
            Value::Float64(f) => Ok(Value::String(f.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            Value::Json(j) => {
                if let Some(s) = j.as_str() {
                    Ok(Value::String(s.to_string()))
                } else {
                    Ok(Value::String(j.to_string()))
                }
            }
            _ => Ok(Value::Null),
        }
    }

    fn fn_nullifzero(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Ok(Value::Null);
        }
        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Int64(0) => Ok(Value::Null),
            Value::Int64(n) => Ok(Value::Int64(*n)),
            Value::Float64(f) if f.0 == 0.0 => Ok(Value::Null),
            Value::Float64(f) => Ok(Value::Float64(*f)),
            v => Ok(v.clone()),
        }
    }

    fn fn_regexp_substr(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "REGEXP_SUBSTR requires source and pattern arguments".into(),
            ));
        }
        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(source), Value::String(pattern)) => match regex::Regex::new(pattern) {
                Ok(re) => {
                    if let Some(m) = re.find(source) {
                        Ok(Value::String(m.as_str().to_string()))
                    } else {
                        Ok(Value::Null)
                    }
                }
                Err(_) => Ok(Value::Null),
            },
            _ => Err(Error::InvalidQuery(
                "REGEXP_SUBSTR expects string arguments".into(),
            )),
        }
    }

    fn fn_array_slice(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "ARRAY_SLICE requires array, start, and end arguments".into(),
            ));
        }
        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
            (Value::Array(arr), Value::Int64(start), Value::Int64(end)) => {
                let start_idx = if *start < 0 {
                    (arr.len() as i64 + start).max(0) as usize
                } else if *start == 0 {
                    0
                } else {
                    ((*start - 1) as usize).min(arr.len())
                };
                let end_idx = if *end < 0 {
                    (arr.len() as i64 + end).max(0) as usize
                } else {
                    (*end as usize).min(arr.len())
                };
                if start_idx >= end_idx {
                    Ok(Value::Array(vec![]))
                } else {
                    Ok(Value::Array(arr[start_idx..end_idx].to_vec()))
                }
            }
            _ => Err(Error::InvalidQuery(
                "ARRAY_SLICE expects array and integer arguments".into(),
            )),
        }
    }

    fn fn_array_enumerate(&self, args: &[Value]) -> Result<Value> {
        match args.first() {
            Some(Value::Null) | None => Ok(Value::Null),
            Some(Value::Array(arr)) => {
                let indices: Vec<Value> = (1..=arr.len() as i64).map(Value::Int64).collect();
                Ok(Value::Array(indices))
            }
            _ => Err(Error::InvalidQuery(
                "arrayEnumerate expects an array argument".into(),
            )),
        }
    }

    fn try_eval_user_function(&self, name: &str, args: &[Value]) -> Result<Option<Value>> {
        let user_functions = match self.user_functions {
            Some(funcs) => funcs,
            None => return Ok(None),
        };

        let upper_name = name.to_uppercase();
        let udf = match user_functions.get(&upper_name) {
            Some(f) => f,
            None => return Ok(None),
        };

        if args.len() != udf.parameters.len() {
            return Err(Error::InvalidQuery(format!(
                "Function {} expects {} arguments, got {}",
                name,
                udf.parameters.len(),
                args.len()
            )));
        }

        match &udf.body {
            FunctionBody::Sql(body_expr) => {
                let substituted_body = self.substitute_udf_params(body_expr, &udf.parameters, args);
                Ok(Some(self.evaluate(&substituted_body, &Record::new())?))
            }
            FunctionBody::JavaScript(js_code) => {
                let param_names: Vec<String> =
                    udf.parameters.iter().map(|p| p.name.clone()).collect();
                let result = crate::js_udf::evaluate_js_function(js_code, &param_names, args)
                    .map_err(|e| Error::InvalidQuery(format!("JavaScript UDF error: {}", e)))?;
                Ok(Some(result))
            }
            FunctionBody::Language { name: lang, code } => {
                if lang.to_uppercase() == "PYTHON" {
                    let param_names: Vec<String> =
                        udf.parameters.iter().map(|p| p.name.clone()).collect();
                    let result = crate::py_udf::evaluate_py_function(code, &param_names, args)
                        .map_err(|e| Error::InvalidQuery(format!("Python UDF error: {}", e)))?;
                    Ok(Some(result))
                } else {
                    Err(Error::UnsupportedFeature(format!(
                        "Language {} is not supported for UDFs",
                        lang
                    )))
                }
            }
            FunctionBody::SqlQuery(_) => Err(Error::InvalidQuery(
                "Table functions cannot be called as scalar functions".to_string(),
            )),
        }
    }

    fn substitute_udf_params(&self, expr: &Expr, params: &[FunctionArg], args: &[Value]) -> Expr {
        match expr {
            Expr::Column { name, .. } => {
                let col_upper = name.to_uppercase();
                for (i, param) in params.iter().enumerate() {
                    if param.name.to_uppercase() == col_upper {
                        return self.value_to_literal_expr(&args[i]);
                    }
                }
                expr.clone()
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.substitute_udf_params(left, params, args)),
                op: *op,
                right: Box::new(self.substitute_udf_params(right, params, args)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.substitute_udf_params(inner, params, args)),
            },
            Expr::ScalarFunction {
                name,
                args: fn_args,
            } => Expr::ScalarFunction {
                name: name.clone(),
                args: fn_args
                    .iter()
                    .map(|a| self.substitute_udf_params(a, params, args))
                    .collect(),
            },
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|o| Box::new(self.substitute_udf_params(o, params, args))),
                when_clauses: when_clauses
                    .iter()
                    .map(|wc| WhenClause {
                        condition: self.substitute_udf_params(&wc.condition, params, args),
                        result: self.substitute_udf_params(&wc.result, params, args),
                    })
                    .collect(),
                else_result: else_result
                    .as_ref()
                    .map(|e| Box::new(self.substitute_udf_params(e, params, args))),
            },
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => Expr::Cast {
                expr: Box::new(self.substitute_udf_params(inner, params, args)),
                data_type: data_type.clone(),
                safe: *safe,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(self.substitute_udf_params(expr, params, args)),
                list: list
                    .iter()
                    .map(|l| self.substitute_udf_params(l, params, args))
                    .collect(),
                negated: *negated,
            },
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: Box::new(self.substitute_udf_params(expr, params, args)),
                low: Box::new(self.substitute_udf_params(low, params, args)),
                high: Box::new(self.substitute_udf_params(high, params, args)),
                negated: *negated,
            },
            _ => expr.clone(),
        }
    }

    fn value_to_literal_expr(&self, value: &Value) -> Expr {
        match value {
            Value::Null => Expr::Literal(Literal::Null),
            Value::Bool(b) => Expr::Literal(Literal::Bool(*b)),
            Value::Int64(n) => Expr::Literal(Literal::Int64(*n)),
            Value::Float64(f) => Expr::Literal(Literal::Float64(OrderedFloat(f.0))),
            Value::String(s) => Expr::Literal(Literal::String(s.clone())),
            Value::Numeric(d) => Expr::Literal(Literal::Numeric(*d)),
            Value::Date(d) => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = d.signed_duration_since(epoch).num_days() as i32;
                Expr::Literal(Literal::Date(days))
            }
            Value::Time(t) => {
                let nanos =
                    t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64;
                Expr::Literal(Literal::Time(nanos))
            }
            Value::Timestamp(ts) => {
                let micros = ts.timestamp_micros();
                Expr::Literal(Literal::Timestamp(micros))
            }
            Value::DateTime(dt) => {
                let micros = dt.and_utc().timestamp_micros();
                Expr::Literal(Literal::Datetime(micros))
            }
            _ => Expr::Literal(Literal::Null),
        }
    }

    fn fn_hll_count_extract(&self, args: &[Value]) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::InvalidQuery(
                "HLL_COUNT_EXTRACT requires 1 argument".to_string(),
            ));
        }
        if args[0].is_null() {
            return Ok(Value::Null);
        }
        let sketch_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: args[0].data_type().to_string(),
        })?;
        if let Some(encoded) = sketch_str.strip_prefix("HLL_SKETCH:p14:") {
            if let Ok(registers) =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
            {
                let m = registers.len() as f64;
                let mut sum: f64 = 0.0;
                let mut zeros = 0i32;
                for &r in &registers {
                    sum += 2.0_f64.powf(-(r as f64));
                    if r == 0 {
                        zeros += 1;
                    }
                }
                let alpha_m = 0.7213 / (1.0 + 1.079 / m);
                let raw_est = alpha_m * m * m / sum;
                let estimate = if raw_est <= 2.5 * m && zeros > 0 {
                    m * (m / zeros as f64).ln()
                } else {
                    raw_est
                };
                return Ok(Value::Int64(estimate.round() as i64));
            }
        } else if let Some(rest) = sketch_str.strip_prefix("HLL_SKETCH:p15:n") {
            if let Ok(count) = rest.parse::<i64>() {
                return Ok(Value::Int64(count));
            }
        }
        Ok(Value::Int64(0))
    }

    fn fn_keys_new_keyset(&self, args: &[Value]) -> Result<Value> {
        use aes_gcm::aead::OsRng;
        use aes_gcm::aead::rand_core::RngCore;

        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "KEYS.NEW_KEYSET requires key type argument".into(),
            ));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(key_type) => {
                let key_size = match key_type.to_uppercase().as_str() {
                    "AEAD_AES_GCM_256" => 32,
                    "AEAD_AES_GCM_128" => 16,
                    "DETERMINISTIC_AEAD_AES_SIV_CMAC_256" => 64,
                    _ => 32,
                };

                let mut key = vec![0u8; key_size];
                OsRng.fill_bytes(&mut key);

                let keyset = serde_json::json!({
                    "primaryKeyId": OsRng.next_u32(),
                    "keyType": key_type,
                    "key": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &key)
                });

                let keyset_bytes = serde_json::to_vec(&keyset).map_err(|e| {
                    Error::InvalidQuery(format!("Failed to serialize keyset: {}", e))
                })?;

                Ok(Value::Bytes(keyset_bytes))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.NEW_KEYSET expects a string key type argument".into(),
            )),
        }
    }

    fn fn_keys_add_key_from_raw_bytes(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "KEYS.ADD_KEY_FROM_RAW_BYTES requires keyset, key_type, and raw_bytes arguments"
                    .into(),
            ));
        }

        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
            (Value::Bytes(keyset), Value::String(_key_type), Value::Bytes(raw_bytes)) => {
                let mut keyset_json: serde_json::Value = serde_json::from_slice(keyset)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid keyset: {}", e)))?;

                if let Some(obj) = keyset_json.as_object_mut() {
                    obj.insert(
                        "additionalKey".to_string(),
                        serde_json::json!(base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            raw_bytes
                        )),
                    );
                }

                let new_keyset = serde_json::to_vec(&keyset_json).map_err(|e| {
                    Error::InvalidQuery(format!("Failed to serialize keyset: {}", e))
                })?;

                Ok(Value::Bytes(new_keyset))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.ADD_KEY_FROM_RAW_BYTES expects bytes arguments".into(),
            )),
        }
    }

    fn fn_keys_keyset_chain(&self, args: &[Value]) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "KEYS.KEYSET_CHAIN requires kms_resource and keyset arguments".into(),
            ));
        }

        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::String(_kms_resource), Value::Bytes(keyset)) => {
                Ok(Value::Bytes(keyset.clone()))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.KEYSET_CHAIN expects string and bytes arguments".into(),
            )),
        }
    }

    fn fn_keys_keyset_from_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "KEYS.KEYSET_FROM_JSON requires json_string argument".into(),
            ));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::String(json_str) => {
                let keyset_bytes = json_str.as_bytes().to_vec();
                Ok(Value::Bytes(keyset_bytes))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.KEYSET_FROM_JSON expects a string argument".into(),
            )),
        }
    }

    fn fn_keys_keyset_to_json(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "KEYS.KEYSET_TO_JSON requires keyset argument".into(),
            ));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(keyset) => {
                let json_str = String::from_utf8_lossy(keyset).to_string();
                Ok(Value::String(json_str))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.KEYSET_TO_JSON expects a bytes argument".into(),
            )),
        }
    }

    fn fn_keys_keyset_length(&self, args: &[Value]) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::InvalidQuery(
                "KEYS.KEYSET_LENGTH requires keyset argument".into(),
            ));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            Value::Bytes(keyset) => {
                let keyset_json: serde_json::Value =
                    serde_json::from_slice(keyset).unwrap_or(serde_json::json!({"keys": []}));

                let count = keyset_json
                    .get("keys")
                    .and_then(|k| k.as_array())
                    .map(|arr| arr.len())
                    .unwrap_or(1);

                Ok(Value::Int64(count as i64))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.KEYSET_LENGTH expects a bytes argument".into(),
            )),
        }
    }

    fn fn_keys_rotate_keyset(&self, args: &[Value]) -> Result<Value> {
        use aes_gcm::aead::OsRng;
        use aes_gcm::aead::rand_core::RngCore;

        if args.len() < 2 {
            return Err(Error::InvalidQuery(
                "KEYS.ROTATE_KEYSET requires keyset and key_type arguments".into(),
            ));
        }

        match (&args[0], &args[1]) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Bytes(keyset), Value::String(key_type)) => {
                let mut keyset_json: serde_json::Value = serde_json::from_slice(keyset)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid keyset: {}", e)))?;

                let key_size = match key_type.to_uppercase().as_str() {
                    "AEAD_AES_GCM_256" => 32,
                    "AEAD_AES_GCM_128" => 16,
                    "DETERMINISTIC_AEAD_AES_SIV_CMAC_256" => 64,
                    _ => 32,
                };

                let mut new_key = vec![0u8; key_size];
                OsRng.fill_bytes(&mut new_key);

                if let Some(obj) = keyset_json.as_object_mut() {
                    obj.insert(
                        "primaryKeyId".to_string(),
                        serde_json::json!(OsRng.next_u32()),
                    );
                    obj.insert(
                        "key".to_string(),
                        serde_json::json!(base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            &new_key
                        )),
                    );
                }

                let new_keyset = serde_json::to_vec(&keyset_json).map_err(|e| {
                    Error::InvalidQuery(format!("Failed to serialize keyset: {}", e))
                })?;

                Ok(Value::Bytes(new_keyset))
            }
            _ => Err(Error::InvalidQuery(
                "KEYS.ROTATE_KEYSET expects bytes and string arguments".into(),
            )),
        }
    }

    fn fn_aead_encrypt(&self, args: &[Value]) -> Result<Value> {
        use aes_gcm::aead::rand_core::RngCore;
        use aes_gcm::aead::{Aead, KeyInit, OsRng};
        use aes_gcm::{Aes256Gcm, Nonce};

        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "AEAD.ENCRYPT requires keyset, plaintext, and additional_data arguments".into(),
            ));
        }

        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) => Ok(Value::Null),
            (Value::Bytes(keyset), Value::Bytes(plaintext), aad) => {
                let keyset_json: serde_json::Value = serde_json::from_slice(keyset)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid keyset: {}", e)))?;

                let key_b64 = keyset_json
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| Error::InvalidQuery("Keyset missing key".into()))?;

                let key_bytes =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64)
                        .map_err(|e| Error::InvalidQuery(format!("Invalid key encoding: {}", e)))?;

                let key: [u8; 32] = key_bytes.try_into().map_err(|_| {
                    Error::InvalidQuery("Key must be 32 bytes for AES-256-GCM".into())
                })?;

                let cipher = Aes256Gcm::new_from_slice(&key)
                    .map_err(|e| Error::InvalidQuery(format!("Failed to create cipher: {}", e)))?;

                let mut nonce_bytes = [0u8; 12];
                OsRng.fill_bytes(&mut nonce_bytes);
                let nonce = Nonce::from_slice(&nonce_bytes);

                let aad_bytes = match aad {
                    Value::Bytes(b) => b.clone(),
                    Value::String(s) => s.as_bytes().to_vec(),
                    Value::Null => vec![],
                    _ => vec![],
                };

                let ciphertext = cipher
                    .encrypt(
                        nonce,
                        aes_gcm::aead::Payload {
                            msg: plaintext,
                            aad: &aad_bytes,
                        },
                    )
                    .map_err(|e| Error::InvalidQuery(format!("Encryption failed: {}", e)))?;

                let mut result = nonce_bytes.to_vec();
                result.extend(ciphertext);

                Ok(Value::Bytes(result))
            }
            _ => Err(Error::InvalidQuery(
                "AEAD.ENCRYPT expects bytes arguments".into(),
            )),
        }
    }

    fn fn_aead_decrypt_string(&self, args: &[Value]) -> Result<Value> {
        let decrypted = self.fn_aead_decrypt_bytes(args)?;
        match decrypted {
            Value::Null => Ok(Value::Null),
            Value::Bytes(b) => {
                let s = String::from_utf8(b).map_err(|e| {
                    Error::InvalidQuery(format!("Decrypted data is not valid UTF-8: {}", e))
                })?;
                Ok(Value::String(s))
            }
            _ => Ok(decrypted),
        }
    }

    fn fn_aead_decrypt_bytes(&self, args: &[Value]) -> Result<Value> {
        use aes_gcm::aead::{Aead, KeyInit};
        use aes_gcm::{Aes256Gcm, Nonce};

        if args.len() < 3 {
            return Err(Error::InvalidQuery(
                "AEAD.DECRYPT requires keyset, ciphertext, and additional_data arguments".into(),
            ));
        }

        match (&args[0], &args[1], &args[2]) {
            (Value::Null, _, _) | (_, Value::Null, _) => Ok(Value::Null),
            (Value::Bytes(keyset), Value::Bytes(ciphertext), aad) => {
                if ciphertext.len() < 12 {
                    return Err(Error::InvalidQuery("Ciphertext too short".into()));
                }

                let keyset_json: serde_json::Value = serde_json::from_slice(keyset)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid keyset: {}", e)))?;

                let key_b64 = keyset_json
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| Error::InvalidQuery("Keyset missing key".into()))?;

                let key_bytes =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64)
                        .map_err(|e| Error::InvalidQuery(format!("Invalid key encoding: {}", e)))?;

                let key: [u8; 32] = key_bytes.try_into().map_err(|_| {
                    Error::InvalidQuery("Key must be 32 bytes for AES-256-GCM".into())
                })?;

                let cipher = Aes256Gcm::new_from_slice(&key)
                    .map_err(|e| Error::InvalidQuery(format!("Failed to create cipher: {}", e)))?;

                let nonce = Nonce::from_slice(&ciphertext[..12]);
                let encrypted_data = &ciphertext[12..];

                let aad_bytes = match aad {
                    Value::Bytes(b) => b.clone(),
                    Value::String(s) => s.as_bytes().to_vec(),
                    Value::Null => vec![],
                    _ => vec![],
                };

                let plaintext = cipher
                    .decrypt(
                        nonce,
                        aes_gcm::aead::Payload {
                            msg: encrypted_data,
                            aad: &aad_bytes,
                        },
                    )
                    .map_err(|e| Error::InvalidQuery(format!("Decryption failed: {}", e)))?;

                Ok(Value::Bytes(plaintext))
            }
            _ => Err(Error::InvalidQuery(
                "AEAD.DECRYPT expects bytes arguments".into(),
            )),
        }
    }

    fn fn_deterministic_encrypt(&self, args: &[Value]) -> Result<Value> {
        self.fn_aead_encrypt(args)
    }

    fn fn_deterministic_decrypt_string(&self, args: &[Value]) -> Result<Value> {
        self.fn_aead_decrypt_string(args)
    }

    fn fn_deterministic_decrypt_bytes(&self, args: &[Value]) -> Result<Value> {
        self.fn_aead_decrypt_bytes(args)
    }
}

fn like_pattern_to_regex(pattern: &str) -> String {
    let mut regex = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => {
                if let Some(&next) = chars.peek() {
                    regex.push_str(&regex::escape(&next.to_string()));
                    chars.next();
                }
            }
            _ => regex.push_str(&regex::escape(&c.to_string())),
        }
    }
    regex
}

fn extract_datetime_field(val: &Value, field: DateTimeField) -> Result<Value> {
    match val {
        Value::Null => Ok(Value::Null),
        Value::Date(date) => extract_from_date(date, field),
        Value::Timestamp(dt) => extract_from_datetime(&dt.naive_utc(), field),
        Value::Time(time) => extract_from_time(time, field),
        Value::DateTime(dt) => extract_from_datetime(dt, field),
        Value::Interval(iv) => extract_from_interval(iv, field),
        _ => Err(Error::InvalidQuery(
            "EXTRACT requires date/time/timestamp/interval argument".into(),
        )),
    }
}

fn week_number_from_date(date: &NaiveDate, start_day: WeekStartDay) -> i64 {
    let year_start = NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap();
    let start_weekday = match start_day {
        WeekStartDay::Sunday => chrono::Weekday::Sun,
        WeekStartDay::Monday => chrono::Weekday::Mon,
        WeekStartDay::Tuesday => chrono::Weekday::Tue,
        WeekStartDay::Wednesday => chrono::Weekday::Wed,
        WeekStartDay::Thursday => chrono::Weekday::Thu,
        WeekStartDay::Friday => chrono::Weekday::Fri,
        WeekStartDay::Saturday => chrono::Weekday::Sat,
    };
    let days_until_first_start_day = (start_weekday.num_days_from_sunday() as i32
        - year_start.weekday().num_days_from_sunday() as i32
        + 7)
        % 7;
    let first_week_start = year_start + chrono::Duration::days(days_until_first_start_day as i64);
    if *date < first_week_start {
        0
    } else {
        let days_since_first_week = (*date - first_week_start).num_days();
        days_since_first_week / 7 + 1
    }
}

fn extract_from_date(date: &NaiveDate, field: DateTimeField) -> Result<Value> {
    match field {
        DateTimeField::Year => Ok(Value::Int64(date.year() as i64)),
        DateTimeField::Month => Ok(Value::Int64(date.month() as i64)),
        DateTimeField::Day => Ok(Value::Int64(date.day() as i64)),
        DateTimeField::Week(start_day) => Ok(Value::Int64(week_number_from_date(date, start_day))),
        DateTimeField::IsoWeek => Ok(Value::Int64(date.iso_week().week() as i64)),
        DateTimeField::IsoYear => Ok(Value::Int64(date.iso_week().year() as i64)),
        DateTimeField::DayOfWeek => Ok(Value::Int64(
            date.weekday().num_days_from_sunday() as i64 + 1,
        )),
        DateTimeField::DayOfYear => Ok(Value::Int64(date.ordinal() as i64)),
        DateTimeField::Quarter => Ok(Value::Int64(((date.month() - 1) / 3 + 1) as i64)),
        _ => Err(Error::InvalidQuery(format!(
            "Cannot extract {:?} from date",
            field
        ))),
    }
}

fn extract_from_datetime(dt: &chrono::NaiveDateTime, field: DateTimeField) -> Result<Value> {
    match field {
        DateTimeField::Year => Ok(Value::Int64(dt.year() as i64)),
        DateTimeField::Month => Ok(Value::Int64(dt.month() as i64)),
        DateTimeField::Day => Ok(Value::Int64(dt.day() as i64)),
        DateTimeField::Hour => Ok(Value::Int64(dt.hour() as i64)),
        DateTimeField::Minute => Ok(Value::Int64(dt.minute() as i64)),
        DateTimeField::Second => Ok(Value::Int64(dt.second() as i64)),
        DateTimeField::Millisecond => Ok(Value::Int64((dt.nanosecond() / 1_000_000) as i64)),
        DateTimeField::Microsecond => Ok(Value::Int64((dt.nanosecond() / 1000) as i64)),
        DateTimeField::Nanosecond => Ok(Value::Int64(dt.nanosecond() as i64)),
        DateTimeField::Week(start_day) => {
            Ok(Value::Int64(week_number_from_date(&dt.date(), start_day)))
        }
        DateTimeField::IsoWeek => Ok(Value::Int64(dt.iso_week().week() as i64)),
        DateTimeField::IsoYear => Ok(Value::Int64(dt.iso_week().year() as i64)),
        DateTimeField::DayOfWeek => {
            Ok(Value::Int64(dt.weekday().num_days_from_sunday() as i64 + 1))
        }
        DateTimeField::DayOfYear => Ok(Value::Int64(dt.ordinal() as i64)),
        DateTimeField::Quarter => Ok(Value::Int64(((dt.month() - 1) / 3 + 1) as i64)),
        DateTimeField::Date => Ok(Value::Date(dt.date())),
        DateTimeField::Time => Ok(Value::Time(dt.time())),
        _ => Err(Error::InvalidQuery(format!(
            "Cannot extract {:?} from timestamp",
            field
        ))),
    }
}

fn extract_from_time(time: &NaiveTime, field: DateTimeField) -> Result<Value> {
    match field {
        DateTimeField::Hour => Ok(Value::Int64(time.hour() as i64)),
        DateTimeField::Minute => Ok(Value::Int64(time.minute() as i64)),
        DateTimeField::Second => Ok(Value::Int64(time.second() as i64)),
        DateTimeField::Millisecond => Ok(Value::Int64((time.nanosecond() / 1_000_000) as i64)),
        DateTimeField::Microsecond => Ok(Value::Int64((time.nanosecond() / 1000) as i64)),
        DateTimeField::Nanosecond => Ok(Value::Int64(time.nanosecond() as i64)),
        _ => Err(Error::InvalidQuery(format!(
            "Cannot extract {:?} from time",
            field
        ))),
    }
}

fn extract_from_interval(
    iv: &yachtsql_common::types::IntervalValue,
    field: DateTimeField,
) -> Result<Value> {
    match field {
        DateTimeField::Year => Ok(Value::Int64((iv.months / 12) as i64)),
        DateTimeField::Month => Ok(Value::Int64((iv.months % 12) as i64)),
        DateTimeField::Day => Ok(Value::Int64(iv.days as i64)),
        DateTimeField::Hour => {
            const NANOS_PER_HOUR: i64 = 60 * 60 * 1_000_000_000;
            Ok(Value::Int64(iv.nanos / NANOS_PER_HOUR))
        }
        DateTimeField::Minute => {
            const NANOS_PER_MINUTE: i64 = 60 * 1_000_000_000;
            const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MINUTE;
            Ok(Value::Int64((iv.nanos % NANOS_PER_HOUR) / NANOS_PER_MINUTE))
        }
        DateTimeField::Second => {
            const NANOS_PER_SECOND: i64 = 1_000_000_000;
            const NANOS_PER_MINUTE: i64 = 60 * NANOS_PER_SECOND;
            Ok(Value::Int64(
                (iv.nanos % NANOS_PER_MINUTE) / NANOS_PER_SECOND,
            ))
        }
        DateTimeField::Millisecond => {
            const NANOS_PER_MS: i64 = 1_000_000;
            const NANOS_PER_SECOND: i64 = 1_000_000_000;
            Ok(Value::Int64((iv.nanos % NANOS_PER_SECOND) / NANOS_PER_MS))
        }
        DateTimeField::Microsecond => {
            const NANOS_PER_US: i64 = 1_000;
            const NANOS_PER_MS: i64 = 1_000_000;
            Ok(Value::Int64((iv.nanos % NANOS_PER_MS) / NANOS_PER_US))
        }
        _ => Err(Error::InvalidQuery(format!(
            "Cannot extract {:?} from interval",
            field
        ))),
    }
}

fn parse_date_string(s: &str) -> Result<Value> {
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| Error::InvalidQuery(format!("Invalid date string: {}", e)))?;
    Ok(Value::Date(date))
}

fn parse_time_string(s: &str) -> Result<Value> {
    let time = NaiveTime::parse_from_str(s, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
        .map_err(|e| Error::InvalidQuery(format!("Invalid time string: {}", e)))?;
    Ok(Value::Time(time))
}

fn parse_timestamp_string(s: &str) -> Result<Value> {
    use chrono::{FixedOffset, NaiveDateTime, TimeZone};

    let s_trimmed = s.trim();

    if let Ok(dt) = DateTime::parse_from_rfc3339(s_trimmed) {
        return Ok(Value::Timestamp(dt.with_timezone(&Utc)));
    }
    if let Ok(dt) = DateTime::parse_from_str(s_trimmed, "%Y-%m-%d %H:%M:%S%:z") {
        return Ok(Value::Timestamp(dt.with_timezone(&Utc)));
    }
    if let Ok(dt) = DateTime::parse_from_str(s_trimmed, "%Y-%m-%d %H:%M:%S%z") {
        return Ok(Value::Timestamp(dt.with_timezone(&Utc)));
    }
    if let Ok(dt) = DateTime::parse_from_str(s_trimmed, "%Y-%m-%d %H:%M:%S%.f%:z") {
        return Ok(Value::Timestamp(dt.with_timezone(&Utc)));
    }
    if let Ok(dt) = DateTime::parse_from_str(s_trimmed, "%Y-%m-%d %H:%M:%S%.f%z") {
        return Ok(Value::Timestamp(dt.with_timezone(&Utc)));
    }

    let re = regex::Regex::new(r"^(.+?)([+-])(\d{1,2})$").unwrap();
    if let Some(caps) = re.captures(s_trimmed) {
        let datetime_part = caps.get(1).unwrap().as_str();
        let sign = caps.get(2).unwrap().as_str();
        let hours: i32 = caps.get(3).unwrap().as_str().parse().unwrap_or(0);
        let offset_secs = hours * 3600 * if sign == "-" { -1 } else { 1 };
        if let Some(offset) = FixedOffset::east_opt(offset_secs) {
            if let Ok(ndt) = NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S") {
                let dt = offset.from_local_datetime(&ndt).single();
                if let Some(dt) = dt {
                    return Ok(Value::Timestamp(dt.with_timezone(&Utc)));
                }
            }
            if let Ok(ndt) = NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%d %H:%M:%S%.f") {
                let dt = offset.from_local_datetime(&ndt).single();
                if let Some(dt) = dt {
                    return Ok(Value::Timestamp(dt.with_timezone(&Utc)));
                }
            }
        }
    }

    let s_no_tz = if s_trimmed.ends_with(" UTC") {
        &s_trimmed[..s_trimmed.len() - 4]
    } else if s_trimmed.ends_with("+00:00") {
        &s_trimmed[..s_trimmed.len() - 6]
    } else if s_trimmed.ends_with("+00") {
        &s_trimmed[..s_trimmed.len() - 3]
    } else if s_trimmed.ends_with("Z") {
        &s_trimmed[..s_trimmed.len() - 1]
    } else {
        s_trimmed
    };

    let dt = NaiveDateTime::parse_from_str(s_no_tz, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s_no_tz, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s_no_tz, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s_no_tz, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| {
            NaiveDate::parse_from_str(s_no_tz, "%Y-%m-%d").map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .map(|ndt| ndt.and_utc())
        .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp string: {}", e)))?;
    Ok(Value::Timestamp(dt))
}

fn parse_datetime_string(s: &str) -> Result<Value> {
    use chrono::NaiveDateTime;
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .map_err(|e| Error::InvalidQuery(format!("Invalid datetime string: {}", e)))?;
    Ok(Value::DateTime(dt))
}

fn parse_range_string(s: &str, inner_type: &DataType) -> Result<Value> {
    let s = s.trim();
    if s.len() < 2 {
        return Err(Error::InvalidQuery(format!(
            "Invalid range string: too short: {}",
            s
        )));
    }
    let first_char = s.chars().next().unwrap();
    let last_char = s.chars().last().unwrap();
    if first_char != '[' && first_char != '(' {
        return Err(Error::InvalidQuery(format!(
            "Invalid range string: must start with '[' or '(': {}",
            s
        )));
    }
    if last_char != ']' && last_char != ')' {
        return Err(Error::InvalidQuery(format!(
            "Invalid range string: must end with ']' or ')': {}",
            s
        )));
    }
    let inner = &s[1..s.len() - 1];
    let parts: Vec<&str> = inner.splitn(2, ',').collect();
    if parts.len() != 2 {
        return Err(Error::InvalidQuery(format!(
            "Invalid range string: must contain exactly one comma: {}",
            s
        )));
    }
    let start_str = parts[0].trim();
    let end_str = parts[1].trim();
    let start = if start_str.is_empty()
        || start_str.eq_ignore_ascii_case("NULL")
        || start_str.eq_ignore_ascii_case("UNBOUNDED")
    {
        None
    } else {
        Some(parse_range_value(start_str, inner_type)?)
    };
    let end = if end_str.is_empty()
        || end_str.eq_ignore_ascii_case("NULL")
        || end_str.eq_ignore_ascii_case("UNBOUNDED")
    {
        None
    } else {
        Some(parse_range_value(end_str, inner_type)?)
    };
    Ok(Value::Range(RangeValue::new(start, end)))
}

fn parse_range_value(s: &str, inner_type: &DataType) -> Result<Value> {
    match inner_type {
        DataType::Date => parse_date_string(s),
        DataType::DateTime => parse_datetime_string(s),
        DataType::Timestamp => parse_timestamp_string(s),
        _ => Err(Error::InvalidQuery(format!(
            "Unsupported range inner type: {:?}",
            inner_type
        ))),
    }
}

fn parse_interval_string(_s: &str) -> Result<Value> {
    Ok(Value::Interval(IntervalValue {
        months: 0,
        days: 0,
        nanos: 0,
    }))
}

fn interval_from_field(n: i64, field: DateTimeField) -> Result<Value> {
    match field {
        DateTimeField::Year => Ok(Value::Interval(IntervalValue {
            months: n as i32 * 12,
            days: 0,
            nanos: 0,
        })),
        DateTimeField::Month => Ok(Value::Interval(IntervalValue {
            months: n as i32,
            days: 0,
            nanos: 0,
        })),
        DateTimeField::Day => Ok(Value::Interval(IntervalValue {
            months: 0,
            days: n as i32,
            nanos: 0,
        })),
        DateTimeField::Hour => Ok(Value::Interval(IntervalValue {
            months: 0,
            days: 0,
            nanos: n * 3_600_000_000_000,
        })),
        DateTimeField::Minute => Ok(Value::Interval(IntervalValue {
            months: 0,
            days: 0,
            nanos: n * 60_000_000_000,
        })),
        DateTimeField::Second => Ok(Value::Interval(IntervalValue {
            months: 0,
            days: 0,
            nanos: n * 1_000_000_000,
        })),
        _ => Err(Error::InvalidQuery(format!(
            "Invalid interval field: {:?}",
            field
        ))),
    }
}

fn add_interval_to_date(date: &NaiveDate, interval: &IntervalValue) -> Result<NaiveDate> {
    use chrono::Months;
    let mut result = *date;
    if interval.months != 0 {
        result = if interval.months > 0 {
            result
                .checked_add_months(Months::new(interval.months as u32))
                .ok_or_else(|| Error::InvalidQuery("Date overflow".into()))?
        } else {
            result
                .checked_sub_months(Months::new((-interval.months) as u32))
                .ok_or_else(|| Error::InvalidQuery("Date overflow".into()))?
        };
    }
    if interval.days != 0 {
        result += chrono::Duration::days(interval.days as i64);
    }
    Ok(result)
}

fn add_interval_to_datetime(dt: &NaiveDateTime, interval: &IntervalValue) -> Result<NaiveDateTime> {
    use chrono::Months;
    let mut result = *dt;
    if interval.months != 0 {
        result = if interval.months > 0 {
            result
                .checked_add_months(Months::new(interval.months as u32))
                .ok_or_else(|| Error::InvalidQuery("DateTime overflow".into()))?
        } else {
            result
                .checked_sub_months(Months::new((-interval.months) as u32))
                .ok_or_else(|| Error::InvalidQuery("DateTime overflow".into()))?
        };
    }
    if interval.days != 0 {
        result += chrono::Duration::days(interval.days as i64);
    }
    if interval.nanos != 0 {
        result += chrono::Duration::nanoseconds(interval.nanos);
    }
    Ok(result)
}

fn negate_interval(interval: &IntervalValue) -> IntervalValue {
    IntervalValue {
        months: -interval.months,
        days: -interval.days,
        nanos: -interval.nanos,
    }
}

fn date_diff_by_part(d1: &NaiveDate, d2: &NaiveDate, part: &str) -> Result<i64> {
    match part {
        "DAY" => Ok(d1.signed_duration_since(*d2).num_days()),
        "WEEK" => Ok(d1.signed_duration_since(*d2).num_weeks()),
        "MONTH" => {
            let months1 = d1.year() as i64 * 12 + d1.month() as i64;
            let months2 = d2.year() as i64 * 12 + d2.month() as i64;
            Ok(months1 - months2)
        }
        "QUARTER" => {
            let q1 = d1.year() as i64 * 4 + ((d1.month() - 1) / 3) as i64;
            let q2 = d2.year() as i64 * 4 + ((d2.month() - 1) / 3) as i64;
            Ok(q1 - q2)
        }
        "YEAR" => Ok((d1.year() - d2.year()) as i64),
        _ => Ok(d1.signed_duration_since(*d2).num_days()),
    }
}

fn trunc_date(date: &NaiveDate, part: &str) -> Result<NaiveDate> {
    match part {
        "YEAR" => NaiveDate::from_ymd_opt(date.year(), 1, 1)
            .ok_or_else(|| Error::InvalidQuery("Invalid date".into())),
        "QUARTER" => {
            let month = ((date.month() - 1) / 3) * 3 + 1;
            NaiveDate::from_ymd_opt(date.year(), month, 1)
                .ok_or_else(|| Error::InvalidQuery("Invalid date".into()))
        }
        "MONTH" => NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
            .ok_or_else(|| Error::InvalidQuery("Invalid date".into())),
        "WEEK" => {
            let days_from_monday = date.weekday().num_days_from_monday();
            Ok(*date - chrono::Duration::days(days_from_monday as i64))
        }
        _ => Ok(*date),
    }
}

fn trunc_datetime(dt: &NaiveDateTime, part: &str) -> Result<NaiveDateTime> {
    match part {
        "YEAR" => NaiveDate::from_ymd_opt(dt.year(), 1, 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        "QUARTER" => {
            let month = ((dt.month() - 1) / 3) * 3 + 1;
            NaiveDate::from_ymd_opt(dt.year(), month, 1)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "MONTH" => NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        "WEEK" | "WEEK_SUNDAY" => {
            let days_from_sunday = dt.weekday().num_days_from_sunday();
            let date = dt.date() - chrono::Duration::days(days_from_sunday as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_MONDAY" => {
            let days_from_monday = dt.weekday().num_days_from_monday();
            let date = dt.date() - chrono::Duration::days(days_from_monday as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_TUESDAY" => {
            let days = (dt.weekday().num_days_from_sunday() + 5) % 7;
            let date = dt.date() - chrono::Duration::days(days as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_WEDNESDAY" => {
            let days = (dt.weekday().num_days_from_sunday() + 4) % 7;
            let date = dt.date() - chrono::Duration::days(days as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_THURSDAY" => {
            let days = (dt.weekday().num_days_from_sunday() + 3) % 7;
            let date = dt.date() - chrono::Duration::days(days as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_FRIDAY" => {
            let days = (dt.weekday().num_days_from_sunday() + 2) % 7;
            let date = dt.date() - chrono::Duration::days(days as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "WEEK_SATURDAY" => {
            let days = (dt.weekday().num_days_from_sunday() + 1) % 7;
            let date = dt.date() - chrono::Duration::days(days as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "ISOWEEK" => {
            let days_from_monday = dt.weekday().num_days_from_monday();
            let date = dt.date() - chrono::Duration::days(days_from_monday as i64);
            date.and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "ISOYEAR" => {
            let iso_year = dt.date().iso_week().year();
            let first_day_of_iso_year =
                NaiveDate::from_isoywd_opt(iso_year, 1, chrono::Weekday::Mon)
                    .ok_or_else(|| Error::InvalidQuery("Invalid ISO year".into()))?;
            first_day_of_iso_year
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into()))
        }
        "DAY" => dt
            .date()
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        "HOUR" => NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
            .and_then(|d| d.and_hms_opt(dt.hour(), 0, 0))
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        "MINUTE" => NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
            .and_then(|d| d.and_hms_opt(dt.hour(), dt.minute(), 0))
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        "SECOND" => NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
            .and_then(|d| d.and_hms_opt(dt.hour(), dt.minute(), dt.second()))
            .ok_or_else(|| Error::InvalidQuery("Invalid datetime".into())),
        _ => Ok(*dt),
    }
}

fn trunc_time(time: &NaiveTime, part: &str) -> Result<NaiveTime> {
    match part {
        "HOUR" => NaiveTime::from_hms_opt(time.hour(), 0, 0)
            .ok_or_else(|| Error::InvalidQuery("Invalid time".into())),
        "MINUTE" => NaiveTime::from_hms_opt(time.hour(), time.minute(), 0)
            .ok_or_else(|| Error::InvalidQuery("Invalid time".into())),
        _ => Ok(*time),
    }
}

fn bucket_date(
    date: &NaiveDate,
    interval: &IntervalValue,
    origin: &NaiveDate,
) -> Result<NaiveDate> {
    let days_since_origin = (*date - *origin).num_days();
    let bucket_days = if interval.days > 0 {
        interval.days as i64
    } else if interval.months > 0 {
        let months_since =
            (date.year() - origin.year()) * 12 + (date.month() as i32 - origin.month() as i32);
        let bucket_count = months_since / interval.months;
        let bucket_start_month = origin.month() as i32 + (bucket_count * interval.months);
        let years_to_add = (bucket_start_month - 1) / 12;
        let month = ((bucket_start_month - 1) % 12) + 1;
        return NaiveDate::from_ymd_opt(origin.year() + years_to_add, month as u32, origin.day())
            .ok_or_else(|| Error::InvalidQuery("Invalid bucket date".into()));
    } else {
        1
    };
    let bucket_count = if days_since_origin >= 0 {
        days_since_origin / bucket_days
    } else {
        (days_since_origin - bucket_days + 1) / bucket_days
    };
    let bucket_start_days = bucket_count * bucket_days;
    Ok(*origin + chrono::Duration::days(bucket_start_days))
}

fn bucket_datetime(
    dt: &NaiveDateTime,
    interval: &IntervalValue,
    origin: &NaiveDateTime,
) -> Result<NaiveDateTime> {
    if interval.months > 0 {
        let months_since =
            (dt.year() - origin.year()) * 12 + (dt.month() as i32 - origin.month() as i32);
        let bucket_count = months_since / interval.months;
        let bucket_start_month = origin.month() as i32 + (bucket_count * interval.months);
        let years_to_add = (bucket_start_month - 1) / 12;
        let month = ((bucket_start_month - 1) % 12) + 1;
        return NaiveDate::from_ymd_opt(origin.year() + years_to_add, month as u32, origin.day())
            .and_then(|d| d.and_hms_opt(origin.hour(), origin.minute(), origin.second()))
            .ok_or_else(|| Error::InvalidQuery("Invalid bucket datetime".into()));
    }
    let total_nanos_in_interval =
        interval.days as i64 * 24 * 60 * 60 * 1_000_000_000 + interval.nanos;
    if total_nanos_in_interval == 0 {
        return Ok(*dt);
    }
    let diff = *dt - *origin;
    let diff_nanos = diff.num_nanoseconds().unwrap_or(0);
    let bucket_count = if diff_nanos >= 0 {
        diff_nanos / total_nanos_in_interval
    } else {
        (diff_nanos - total_nanos_in_interval + 1) / total_nanos_in_interval
    };
    let bucket_start_nanos = bucket_count * total_nanos_in_interval;
    Ok(*origin + chrono::Duration::nanoseconds(bucket_start_nanos))
}

fn format_date_with_pattern(date: &NaiveDate, pattern: &str) -> Result<String> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    Ok(date.format(&chrono_pattern).to_string())
}

fn format_datetime_with_pattern(dt: &NaiveDateTime, pattern: &str) -> Result<String> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    Ok(dt.format(&chrono_pattern).to_string())
}

fn format_time_with_pattern(time: &NaiveTime, pattern: &str) -> Result<String> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    Ok(time.format(&chrono_pattern).to_string())
}

fn bq_format_to_chrono(bq_format: &str) -> String {
    bq_format
        .replace("%F", "%Y-%m-%d")
        .replace("%T", "%H:%M:%S")
        .replace("%R", "%H:%M")
        .replace("%D", "%m/%d/%y")
        .replace("%Q", "Q%Q")
}

fn parse_date_with_pattern(s: &str, pattern: &str) -> Result<NaiveDate> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    NaiveDate::parse_from_str(s, &chrono_pattern).map_err(|e| {
        Error::InvalidQuery(format!(
            "Failed to parse date '{}' with pattern '{}': {}",
            s, pattern, e
        ))
    })
}

fn parse_datetime_with_pattern(s: &str, pattern: &str) -> Result<NaiveDateTime> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, &chrono_pattern) {
        return Ok(dt);
    }
    let has_date = pattern.contains("%Y") || pattern.contains("%m") || pattern.contains("%d");
    let has_time = pattern.contains("%H")
        || pattern.contains("%I")
        || pattern.contains("%M")
        || pattern.contains("%S");
    if has_date && !has_time {
        if let Ok(date) = NaiveDate::parse_from_str(s, &chrono_pattern) {
            return Ok(date.and_hms_opt(0, 0, 0).unwrap());
        }
    }
    Err(Error::InvalidQuery(format!(
        "Failed to parse datetime '{}' with pattern '{}'",
        s, pattern
    )))
}

fn parse_time_with_pattern(s: &str, pattern: &str) -> Result<NaiveTime> {
    let chrono_pattern = bq_format_to_chrono(pattern);
    if let Ok(time) = NaiveTime::parse_from_str(s, &chrono_pattern) {
        return Ok(time);
    }
    let has_hour = pattern.contains("%H") || pattern.contains("%I");
    let has_minute = pattern.contains("%M");
    let has_second = pattern.contains("%S");
    if has_hour && !has_minute && !has_second {
        let extended_pattern = format!("{} %M %S", chrono_pattern);
        let extended_input = format!("{} 00 00", s);
        if let Ok(time) = NaiveTime::parse_from_str(&extended_input, &extended_pattern) {
            return Ok(time);
        }
    } else if has_hour && has_minute && !has_second {
        let extended_pattern = format!("{} %S", chrono_pattern);
        let extended_input = format!("{} 00", s);
        if let Ok(time) = NaiveTime::parse_from_str(&extended_input, &extended_pattern) {
            return Ok(time);
        }
    }
    Err(Error::InvalidQuery(format!(
        "Failed to parse time '{}' with pattern '{}'",
        s, pattern
    )))
}

fn value_to_json(value: &Value) -> Result<serde_json::Value> {
    match value {
        Value::Null => Ok(serde_json::Value::Null),
        Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Int64(n) => Ok(serde_json::Value::Number((*n).into())),
        Value::Float64(f) => {
            let n = serde_json::Number::from_f64(f.0)
                .ok_or_else(|| Error::InvalidQuery("Cannot convert float to JSON".into()))?;
            Ok(serde_json::Value::Number(n))
        }
        Value::String(s) => Ok(serde_json::Value::String(s.clone())),
        Value::Array(arr) => {
            let json_arr: Result<Vec<serde_json::Value>> = arr.iter().map(value_to_json).collect();
            Ok(serde_json::Value::Array(json_arr?))
        }
        Value::Struct(fields) => {
            let mut map = serde_json::Map::new();
            for (name, val) in fields {
                map.insert(name.clone(), value_to_json(val)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        Value::Json(j) => Ok(j.clone()),
        Value::Date(d) => Ok(serde_json::Value::String(d.to_string())),
        Value::Time(t) => Ok(serde_json::Value::String(t.to_string())),
        Value::DateTime(dt) => Ok(serde_json::Value::String(dt.to_string())),
        Value::Timestamp(ts) => Ok(serde_json::Value::String(ts.to_rfc3339())),
        Value::Numeric(n) => {
            if let Some(f) = n.to_f64() {
                let num = serde_json::Number::from_f64(f)
                    .ok_or_else(|| Error::InvalidQuery("Cannot convert numeric to JSON".into()))?;
                Ok(serde_json::Value::Number(num))
            } else {
                Ok(serde_json::Value::String(n.to_string()))
            }
        }
        Value::BigNumeric(n) => {
            if let Some(f) = n.to_f64() {
                let num = serde_json::Number::from_f64(f).ok_or_else(|| {
                    Error::InvalidQuery("Cannot convert bignumeric to JSON".into())
                })?;
                Ok(serde_json::Value::Number(num))
            } else {
                Ok(serde_json::Value::String(n.to_string()))
            }
        }
        Value::Bytes(b) => {
            use base64::Engine;
            use base64::engine::general_purpose::STANDARD;
            Ok(serde_json::Value::String(STANDARD.encode(b)))
        }
        Value::Interval(i) => Ok(serde_json::Value::String(format!(
            "{} months, {} days, {} nanos",
            i.months, i.days, i.nanos
        ))),
        Value::Geography(g) => Ok(serde_json::Value::String(g.clone())),
        Value::Range(r) => Ok(serde_json::Value::String(format!(
            "[{:?}, {:?})",
            r.start, r.end
        ))),
        Value::Default => Ok(serde_json::Value::Null),
    }
}

fn extract_json_path(json: &serde_json::Value, path: &str) -> Result<Option<serde_json::Value>> {
    let path = path.trim_start_matches('$');
    let mut current = json.clone();

    for segment in path.split('.').filter(|s| !s.is_empty()) {
        let (key, index) = if segment.contains('[') {
            let parts: Vec<&str> = segment.split('[').collect();
            let key = parts[0];
            let idx_str = parts[1].trim_end_matches(']');
            let idx: usize = idx_str.parse().map_err(|_| {
                Error::InvalidQuery(format!("Invalid JSON path index: {}", idx_str))
            })?;
            (key, Some(idx))
        } else {
            (segment, None)
        };

        if !key.is_empty() {
            current = match current {
                serde_json::Value::Object(map) => {
                    map.get(key).cloned().unwrap_or(serde_json::Value::Null)
                }
                _ => return Ok(None),
            };
        }

        if let Some(idx) = index {
            current = match current {
                serde_json::Value::Array(arr) => {
                    arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                }
                _ => return Ok(None),
            };
        }
    }

    if current == serde_json::Value::Null {
        Ok(None)
    } else {
        Ok(Some(current))
    }
}
