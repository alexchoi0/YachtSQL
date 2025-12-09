use std::num::IntErrorKind;
use std::str::FromStr;

use chrono::{Datelike, Days, Months, NaiveDate};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Interval, Value};
use yachtsql_optimizer::expr::Expr;
use yachtsql_optimizer::plan::PlanNode;
use yachtsql_storage::Schema;

pub fn infer_scalar_subquery_type_static(
    subquery: &PlanNode,
    _schema: Option<&Schema>,
) -> Option<DataType> {
    match subquery {
        PlanNode::Projection { expressions, .. } => {
            if expressions.len() == 1 {
                infer_expr_type_basic(&expressions[0].0)
            } else {
                None
            }
        }
        PlanNode::Aggregate {
            aggregates, input, ..
        } => {
            if aggregates.len() == 1 {
                infer_aggregate_type_with_input_plan(&aggregates[0], input)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn infer_aggregate_type_with_input_plan(agg_expr: &Expr, input: &PlanNode) -> Option<DataType> {
    match agg_expr {
        Expr::Aggregate { name, args, .. } => {
            use yachtsql_ir::FunctionName;
            match name {
                FunctionName::Count => Some(DataType::Int64),
                FunctionName::Avg | FunctionName::Average | FunctionName::Sum => {
                    if let Some(arg) = args.first() {
                        if let Some(col_type) = infer_column_type_from_plan(arg, input) {
                            return match col_type {
                                DataType::Numeric(prec) => Some(DataType::Numeric(prec)),
                                DataType::Int64 => Some(DataType::Float64),
                                _ => Some(DataType::Float64),
                            };
                        }
                    }

                    Some(DataType::Numeric(None))
                }
                FunctionName::Min
                | FunctionName::Minimum
                | FunctionName::Max
                | FunctionName::Maximum => {
                    if let Some(arg) = args.first() {
                        if let Some(col_type) = infer_column_type_from_plan(arg, input) {
                            return Some(col_type);
                        }
                    }

                    Some(DataType::Float64)
                }
                FunctionName::StringAgg | FunctionName::ArrayAgg => Some(DataType::String),
                FunctionName::ApproxQuantiles => Some(DataType::Array(Box::new(DataType::Float64))),
                _ => None,
            }
        }
        _ => infer_expr_type_basic(agg_expr),
    }
}

fn infer_column_type_from_plan(expr: &Expr, plan: &PlanNode) -> Option<DataType> {
    match expr {
        Expr::Column { name, .. } => find_column_type_in_plan(name, plan),
        _ => infer_input_column_type(expr),
    }
}

fn find_column_type_in_plan(col_name: &str, plan: &PlanNode) -> Option<DataType> {
    match plan {
        PlanNode::Scan { .. } => None,
        PlanNode::Filter { input, .. } => find_column_type_in_plan(col_name, input),
        PlanNode::Projection { expressions, input } => {
            for (expr, alias) in expressions {
                let alias_name = alias.as_deref().unwrap_or("");
                if alias_name.eq_ignore_ascii_case(col_name) {
                    return infer_input_column_type(expr);
                }
                if let Expr::Column { name, .. } = expr {
                    if name.eq_ignore_ascii_case(col_name) {
                        return find_column_type_in_plan(col_name, input);
                    }
                }
            }
            find_column_type_in_plan(col_name, input)
        }
        _ => None,
    }
}

fn infer_expr_type_basic(expr: &Expr) -> Option<DataType> {
    match expr {
        Expr::Literal(lit) => Some(literal_type(lit)),
        Expr::Function { name, .. } => function_return_type(name),
        Expr::Aggregate { name, args, .. } => aggregate_return_type_with_input(name, args.first()),
        Expr::Cast { data_type, .. } | Expr::TryCast { data_type, .. } => {
            Some(cast_data_type_to_data_type(data_type))
        }
        _ => None,
    }
}

fn literal_type(lit: &yachtsql_optimizer::expr::LiteralValue) -> DataType {
    match lit {
        yachtsql_optimizer::expr::LiteralValue::Null => DataType::Unknown,
        yachtsql_optimizer::expr::LiteralValue::Boolean(_) => DataType::Bool,
        yachtsql_optimizer::expr::LiteralValue::Int64(_) => DataType::Int64,
        yachtsql_optimizer::expr::LiteralValue::Float64(_) => DataType::Float64,
        yachtsql_optimizer::expr::LiteralValue::Numeric(_) => DataType::Numeric(None),
        yachtsql_optimizer::expr::LiteralValue::String(_) => DataType::String,
        yachtsql_optimizer::expr::LiteralValue::Bytes(_) => DataType::Bytes,
        yachtsql_optimizer::expr::LiteralValue::Date(_) => DataType::Date,
        yachtsql_optimizer::expr::LiteralValue::Time(_) => DataType::Time,
        yachtsql_optimizer::expr::LiteralValue::DateTime(_) => DataType::DateTime,
        yachtsql_optimizer::expr::LiteralValue::Timestamp(_) => DataType::Timestamp,
        yachtsql_optimizer::expr::LiteralValue::Uuid(_) => DataType::String,
        yachtsql_optimizer::expr::LiteralValue::Json(_) => DataType::Json,
        yachtsql_optimizer::expr::LiteralValue::Array(_) => {
            DataType::Array(Box::new(DataType::Unknown))
        }
        yachtsql_optimizer::expr::LiteralValue::Vector(vec) => DataType::Vector(vec.len()),
        yachtsql_optimizer::expr::LiteralValue::Interval(_) => DataType::Interval,
        yachtsql_optimizer::expr::LiteralValue::Range(_) => {
            DataType::Range(yachtsql_core::types::RangeType::Int4Range)
        }
        yachtsql_optimizer::expr::LiteralValue::Point(_) => DataType::Point,
        yachtsql_optimizer::expr::LiteralValue::PgBox(_) => DataType::PgBox,
        yachtsql_optimizer::expr::LiteralValue::Circle(_) => DataType::Circle,
        yachtsql_optimizer::expr::LiteralValue::MacAddr(_) => DataType::MacAddr,
        yachtsql_optimizer::expr::LiteralValue::MacAddr8(_) => DataType::MacAddr8,
    }
}

fn function_return_type(name: &yachtsql_ir::FunctionName) -> Option<DataType> {
    use yachtsql_ir::FunctionName;
    match name {
        FunctionName::CurrentDate | FunctionName::Curdate | FunctionName::Today => {
            Some(DataType::Date)
        }
        FunctionName::CurrentTimestamp
        | FunctionName::Getdate
        | FunctionName::Sysdate
        | FunctionName::Systimestamp
        | FunctionName::Now => Some(DataType::Timestamp),
        FunctionName::Length
        | FunctionName::Len
        | FunctionName::CharLength
        | FunctionName::CharacterLength
        | FunctionName::OctetLength
        | FunctionName::ByteLength
        | FunctionName::Lengthb => Some(DataType::Int64),
        FunctionName::Upper
        | FunctionName::Ucase
        | FunctionName::Lower
        | FunctionName::Lcase
        | FunctionName::Trim
        | FunctionName::Btrim
        | FunctionName::Ltrim
        | FunctionName::TrimLeft
        | FunctionName::Rtrim
        | FunctionName::TrimRight => Some(DataType::String),
        FunctionName::UnixDate
        | FunctionName::UnixSeconds
        | FunctionName::UnixMillis
        | FunctionName::UnixMicros
        | FunctionName::DateDiff
        | FunctionName::Datediff
        | FunctionName::TimestampDiff
        | FunctionName::DatetimeDiff
        | FunctionName::TimeDiff => Some(DataType::Int64),
        FunctionName::DateFromUnixDate
        | FunctionName::DateAdd
        | FunctionName::Dateadd
        | FunctionName::Adddate
        | FunctionName::DateSub
        | FunctionName::Datesub
        | FunctionName::Subdate
        | FunctionName::DateTrunc
        | FunctionName::LastDay => Some(DataType::Date),
        FunctionName::TimestampSeconds
        | FunctionName::TimestampMillis
        | FunctionName::TimestampMicros
        | FunctionName::TimestampAdd
        | FunctionName::TimestampSub
        | FunctionName::TimestampTrunc => Some(DataType::Timestamp),
        FunctionName::DatetimeAdd | FunctionName::DatetimeSub | FunctionName::DatetimeTrunc => {
            Some(DataType::DateTime)
        }
        FunctionName::TimeAdd | FunctionName::TimeSub | FunctionName::TimeTrunc => {
            Some(DataType::Time)
        }
        _ => None,
    }
}

fn aggregate_return_type(name: &yachtsql_ir::FunctionName) -> Option<DataType> {
    use yachtsql_ir::FunctionName;
    match name {
        FunctionName::Count => Some(DataType::Int64),
        FunctionName::Avg
        | FunctionName::Average
        | FunctionName::Sum
        | FunctionName::Min
        | FunctionName::Minimum
        | FunctionName::Max
        | FunctionName::Maximum => Some(DataType::Float64),
        FunctionName::StringAgg | FunctionName::ArrayAgg => Some(DataType::String),
        _ => None,
    }
}

fn aggregate_return_type_with_input(
    name: &yachtsql_ir::FunctionName,
    input: Option<&Expr>,
) -> Option<DataType> {
    use yachtsql_ir::FunctionName;
    match name {
        FunctionName::Count => Some(DataType::Int64),
        FunctionName::Avg
        | FunctionName::Average
        | FunctionName::Sum
        | FunctionName::Min
        | FunctionName::Minimum
        | FunctionName::Max
        | FunctionName::Maximum => {
            if let Some(input_expr) = input {
                if let Some(input_type) = infer_input_column_type(input_expr) {
                    return match input_type {
                        DataType::Numeric(prec) => Some(DataType::Numeric(prec)),
                        DataType::Int64 | DataType::Float64 => Some(DataType::Float64),
                        other => Some(other),
                    };
                }
            }

            Some(DataType::Float64)
        }
        FunctionName::StringAgg | FunctionName::ArrayAgg => Some(DataType::String),
        FunctionName::ApproxQuantiles => Some(DataType::Array(Box::new(DataType::Float64))),
        _ => None,
    }
}

fn infer_input_column_type(expr: &Expr) -> Option<DataType> {
    match expr {
        Expr::Literal(lit) => Some(literal_type(lit)),

        Expr::Cast { data_type, .. } | Expr::TryCast { data_type, .. } => {
            Some(cast_data_type_to_data_type(data_type))
        }

        Expr::Column { .. } => None,

        Expr::Function { name, args } => {
            if let Some(ret_type) = function_return_type(name) {
                return Some(ret_type);
            }

            args.first().and_then(|arg| infer_input_column_type(arg))
        }
        _ => None,
    }
}

fn cast_data_type_to_data_type(cast_type: &yachtsql_optimizer::expr::CastDataType) -> DataType {
    match cast_type {
        yachtsql_optimizer::expr::CastDataType::Bool => DataType::Bool,
        yachtsql_optimizer::expr::CastDataType::Int64 => DataType::Int64,
        yachtsql_optimizer::expr::CastDataType::Float64 => DataType::Float64,
        yachtsql_optimizer::expr::CastDataType::Numeric(precision) => DataType::Numeric(*precision),
        yachtsql_optimizer::expr::CastDataType::String => DataType::String,
        yachtsql_optimizer::expr::CastDataType::Bytes => DataType::Bytes,
        yachtsql_optimizer::expr::CastDataType::Date => DataType::Date,
        yachtsql_optimizer::expr::CastDataType::Time => DataType::Time,
        yachtsql_optimizer::expr::CastDataType::DateTime => DataType::DateTime,
        yachtsql_optimizer::expr::CastDataType::Timestamp => DataType::Timestamp,
        yachtsql_optimizer::expr::CastDataType::Geography => DataType::Geography,
        yachtsql_optimizer::expr::CastDataType::Json => DataType::Json,
        yachtsql_optimizer::expr::CastDataType::Array(inner) => {
            DataType::Array(Box::new(cast_data_type_to_data_type(inner)))
        }
        yachtsql_optimizer::expr::CastDataType::TimestampTz => DataType::TimestampTz,
        yachtsql_optimizer::expr::CastDataType::Vector(dims) => DataType::Vector(*dims),
        yachtsql_optimizer::expr::CastDataType::Interval => DataType::Interval,
        yachtsql_optimizer::expr::CastDataType::Uuid => DataType::Uuid,
        yachtsql_optimizer::expr::CastDataType::Hstore => DataType::Hstore,
        yachtsql_optimizer::expr::CastDataType::MacAddr => DataType::MacAddr,
        yachtsql_optimizer::expr::CastDataType::MacAddr8 => DataType::MacAddr8,
        yachtsql_optimizer::expr::CastDataType::Inet => DataType::Inet,
        yachtsql_optimizer::expr::CastDataType::Cidr => DataType::Cidr,
        yachtsql_optimizer::expr::CastDataType::Int4Range => {
            DataType::Range(yachtsql_core::types::RangeType::Int4Range)
        }
        yachtsql_optimizer::expr::CastDataType::Int8Range => {
            DataType::Range(yachtsql_core::types::RangeType::Int8Range)
        }
        yachtsql_optimizer::expr::CastDataType::NumRange => {
            DataType::Range(yachtsql_core::types::RangeType::NumRange)
        }
        yachtsql_optimizer::expr::CastDataType::TsRange => {
            DataType::Range(yachtsql_core::types::RangeType::TsRange)
        }
        yachtsql_optimizer::expr::CastDataType::TsTzRange => {
            DataType::Range(yachtsql_core::types::RangeType::TsTzRange)
        }
        yachtsql_optimizer::expr::CastDataType::DateRange => {
            DataType::Range(yachtsql_core::types::RangeType::DateRange)
        }
        yachtsql_optimizer::expr::CastDataType::Point => DataType::Point,
        yachtsql_optimizer::expr::CastDataType::PgBox => DataType::PgBox,
        yachtsql_optimizer::expr::CastDataType::Circle => DataType::Circle,
        yachtsql_optimizer::expr::CastDataType::Custom(name, _) => DataType::Custom(name.clone()),
    }
}

pub fn format_interval(interval: &yachtsql_core::types::Interval) -> String {
    let mut parts = Vec::new();

    if interval.months != 0 {
        let years = interval.months / 12;
        let months = interval.months % 12;
        if years != 0 {
            parts.push(format!(
                "{} year{}",
                years,
                if years.abs() != 1 { "s" } else { "" }
            ));
        }
        if months != 0 {
            parts.push(format!(
                "{} mon{}",
                months,
                if months.abs() != 1 { "s" } else { "" }
            ));
        }
    }

    if interval.days != 0 {
        parts.push(format!(
            "{} day{}",
            interval.days,
            if interval.days.abs() != 1 { "s" } else { "" }
        ));
    }

    if interval.micros != 0 || parts.is_empty() {
        let total_seconds = interval.micros.abs() / 1_000_000;
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        let micros = interval.micros.abs() % 1_000_000;

        let sign = if interval.micros < 0 && (hours > 0 || minutes > 0 || seconds > 0 || micros > 0)
        {
            "-"
        } else {
            ""
        };

        if hours > 0 || minutes > 0 || seconds > 0 || micros > 0 {
            if micros > 0 {
                parts.push(format!(
                    "{}{}:{:02}:{:02}.{:06}",
                    sign, hours, minutes, seconds, micros
                ));
            } else {
                parts.push(format!("{}{}:{:02}:{:02}", sign, hours, minutes, seconds));
            }
        } else if parts.is_empty() {
            parts.push("00:00:00".to_string());
        }
    }

    parts.join(" ")
}

pub fn parse_interval_from_string(s: &str) -> Result<yachtsql_core::types::Interval> {
    use yachtsql_core::types::Interval;

    let s = s.trim().to_lowercase();
    let mut months = 0i32;
    let mut days = 0i32;
    let mut micros = 0i64;

    let parts: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;
    while i < parts.len() {
        let value_str = parts[i];
        let value: i64 = value_str.parse().map_err(|_| {
            Error::InvalidOperation(format!("Cannot parse '{}' as interval value", value_str))
        })?;

        if i + 1 < parts.len() {
            let unit = parts[i + 1];
            match unit {
                "year" | "years" => months += (value as i32) * 12,
                "month" | "months" | "mon" | "mons" => months += value as i32,
                "day" | "days" => days += value as i32,
                "hour" | "hours" => micros += value * 3_600_000_000,
                "minute" | "minutes" | "min" | "mins" => micros += value * 60_000_000,
                "second" | "seconds" | "sec" | "secs" => micros += value * 1_000_000,
                _ => {
                    return Err(Error::InvalidOperation(format!(
                        "Unknown interval unit: '{}'",
                        unit
                    )));
                }
            }
            i += 2;
        } else {
            return Err(Error::InvalidOperation(format!(
                "Interval value '{}' without unit",
                value_str
            )));
        }
    }

    Ok(Interval::new(months, days, micros))
}

pub fn apply_interval_to_date(
    date: NaiveDate,
    interval_value: i64,
    interval_unit: &str,
) -> Result<NaiveDate> {
    match interval_unit.to_uppercase().as_str() {
        "DAY" => if interval_value >= 0 {
            date.checked_add_days(Days::new(interval_value as u64))
        } else {
            date.checked_sub_days(Days::new((-interval_value) as u64))
        }
        .ok_or_else(|| {
            Error::ExecutionError(format!(
                "Date arithmetic overflow: {} + {} {}",
                date, interval_value, interval_unit
            ))
        }),
        "MONTH" => if interval_value >= 0 {
            date.checked_add_months(Months::new(interval_value as u32))
        } else {
            date.checked_sub_months(Months::new((-interval_value) as u32))
        }
        .ok_or_else(|| {
            Error::ExecutionError(format!(
                "Date arithmetic overflow: {} + {} {}",
                date, interval_value, interval_unit
            ))
        }),
        "YEAR" => {
            let months = interval_value.checked_mul(12).ok_or_else(|| {
                Error::ExecutionError("Year to month conversion overflow".to_string())
            })?;
            if months >= 0 {
                date.checked_add_months(Months::new(months as u32))
            } else {
                date.checked_sub_months(Months::new((-months) as u32))
            }
            .ok_or_else(|| {
                Error::ExecutionError(format!(
                    "Date arithmetic overflow: {} + {} {}",
                    date, interval_value, interval_unit
                ))
            })
        }
        _ => Err(Error::InvalidOperation(format!(
            "Unsupported interval unit: {}",
            interval_unit
        ))),
    }
}

pub fn add_interval_to_date(date: NaiveDate, interval: &Interval) -> Result<NaiveDate> {
    let mut result = date;

    if interval.months != 0 {
        result = if interval.months > 0 {
            result.checked_add_months(Months::new(interval.months as u32))
        } else {
            result.checked_sub_months(Months::new((-interval.months) as u32))
        }
        .ok_or_else(|| {
            Error::ExecutionError(format!(
                "Date arithmetic overflow: {} + {} months",
                date, interval.months
            ))
        })?;
    }

    if interval.days != 0 {
        result = if interval.days > 0 {
            result.checked_add_days(Days::new(interval.days as u64))
        } else {
            result.checked_sub_days(Days::new((-interval.days) as u64))
        }
        .ok_or_else(|| {
            Error::ExecutionError(format!(
                "Date arithmetic overflow: {} + {} days",
                date, interval.days
            ))
        })?;
    }

    Ok(result)
}

pub fn sub_interval_from_date(date: NaiveDate, interval: &Interval) -> Result<NaiveDate> {
    let negated = Interval::new(-interval.months, -interval.days, -interval.micros);
    add_interval_to_date(date, &negated)
}

pub fn calculate_date_diff(date1: NaiveDate, date2: NaiveDate, unit: &str) -> Result<i64> {
    match unit.to_uppercase().as_str() {
        "DAY" => {
            let duration = date1.signed_duration_since(date2);
            Ok(duration.num_days())
        }
        "MONTH" => {
            let months1 = date1.year() * 12 + date1.month() as i32;
            let months2 = date2.year() * 12 + date2.month() as i32;
            Ok((months1 - months2) as i64)
        }
        "YEAR" => Ok((date1.year() - date2.year()) as i64),
        "WEEK" => {
            let duration = date1.signed_duration_since(date2);
            Ok(duration.num_weeks())
        }
        _ => Err(Error::InvalidOperation(format!(
            "Unsupported date diff unit: {}",
            unit
        ))),
    }
}

pub(crate) fn check_float_to_int64_overflow(f: f64) -> Result<()> {
    if f.is_nan() {
        return Err(Error::InvalidOperation(
            "Cannot cast NaN to INT64".to_string(),
        ));
    }

    if f.is_infinite() {
        let inf = if f.is_sign_positive() {
            "Infinity"
        } else {
            "-Infinity"
        };
        return Err(Error::InvalidOperation(format!(
            "INT64 overflow: cannot cast {} to INT64",
            inf
        )));
    }

    const MAX_ACCEPTABLE: f64 = 9_223_372_036_854_775_807.0;
    const MIN_ACCEPTABLE: f64 = -9_223_372_036_854_775_808.0;

    if !(MIN_ACCEPTABLE..=MAX_ACCEPTABLE).contains(&f) {
        return Err(Error::InvalidOperation(format!(
            "INT64 overflow: {} is out of range for INT64 (min: {}, max: {})",
            f,
            i64::MIN,
            i64::MAX
        )));
    }

    Ok(())
}

pub fn apply_numeric_precision_scale(
    decimal: Decimal,
    precision_scale: &Option<(u8, u8)>,
) -> Result<Decimal> {
    if let Some((precision, scale)) = precision_scale {
        if *precision == 0 {
            return Err(Error::InvalidQuery(
                "NUMERIC precision must be at least 1".to_string(),
            ));
        }
        if *scale > *precision {
            return Err(Error::InvalidQuery(format!(
                "NUMERIC scale ({}) cannot exceed precision ({})",
                scale, precision
            )));
        }

        let rounded = decimal.round_dp(*scale as u32);
        let abs_value = rounded.abs();
        let integer_part = abs_value.trunc();
        let integer_digits = if integer_part.is_zero() {
            0
        } else {
            let int_str = integer_part.to_string();
            int_str.trim_start_matches('-').len()
        };

        let max_integer_digits = precision.saturating_sub(*scale) as usize;
        if integer_digits > max_integer_digits {
            return Err(Error::InvalidQuery(format!(
                "NUMERIC value {} exceeds precision ({}, {}): has {} integer digits, max {} allowed",
                decimal, precision, scale, integer_digits, max_integer_digits
            )));
        }

        Ok(rounded)
    } else {
        Ok(decimal)
    }
}

pub(crate) fn parse_bool_string(input: &str) -> Option<bool> {
    match input.trim().to_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "1" | "on" => Some(true),
        "false" | "f" | "no" | "n" | "0" | "off" => Some(false),
        _ => None,
    }
}

pub fn safe_divide(a: &Value, b: &Value) -> Result<Value> {
    yachtsql_functions::scalar::eval_safe_divide(a, b)
}

pub fn safe_multiply(a: &Value, b: &Value) -> Result<Value> {
    yachtsql_functions::scalar::eval_safe_multiply(a, b)
}

pub fn safe_add(a: &Value, b: &Value) -> Result<Value> {
    yachtsql_functions::scalar::eval_safe_add(a, b)
}

pub fn safe_subtract(a: &Value, b: &Value) -> Result<Value> {
    yachtsql_functions::scalar::eval_safe_subtract(a, b)
}

pub fn safe_negate(val: &Value) -> Result<Value> {
    yachtsql_functions::scalar::eval_safe_negate(val)
}

pub fn evaluate_condition_as_bool(condition: &Value) -> Result<bool> {
    if let Some(b) = condition.as_bool() {
        Ok(b)
    } else if condition.is_null() {
        Ok(false)
    } else {
        Err(Error::TypeMismatch {
            expected: "BOOL".to_string(),
            actual: format!("{:?}", condition.data_type()),
        })
    }
}

pub fn decode_values_match(left: &Value, right: &Value) -> bool {
    if left.is_null() && right.is_null() {
        true
    } else if left.is_null() || right.is_null() {
        false
    } else {
        left == right
    }
}

pub fn perform_cast(
    value: &Value,
    target_type: &yachtsql_optimizer::expr::CastDataType,
) -> Result<Value> {
    use yachtsql_optimizer::expr::CastDataType;

    if value.is_null() {
        return Ok(Value::null());
    }

    match target_type {
        CastDataType::Int64 => {
            if let Some(i) = value.as_i64() {
                Ok(Value::int64(i))
            } else if let Some(f) = value.as_f64() {
                check_float_to_int64_overflow(f)?;
                Ok(Value::int64(f as i64))
            } else if let Some(n) = value.as_numeric() {
                n.to_i64().map(Value::int64).ok_or_else(|| {
                    Error::InvalidOperation(format!(
                        "NUMERIC value {} is out of range for INT64",
                        n
                    ))
                })
            } else if let Some(s) = value.as_str() {
                let trimmed = s.trim();
                match trimmed.parse::<i64>() {
                    Ok(v) => Ok(Value::int64(v)),
                    Err(e) => {
                        let msg = if matches!(
                            e.kind(),
                            IntErrorKind::PosOverflow | IntErrorKind::NegOverflow
                        ) {
                            format!(
                                "INT64 overflow: '{}' is out of range for INT64 (min: {}, max: {})",
                                s,
                                i64::MIN,
                                i64::MAX
                            )
                        } else {
                            format!("Invalid integer: cannot cast '{}' to INT64", s)
                        };
                        Err(Error::InvalidOperation(msg))
                    }
                }
            } else if let Some(b) = value.as_bool() {
                Ok(Value::int64(if b { 1 } else { 0 }))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Int64".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Float64 => {
            if let Some(i) = value.as_i64() {
                Ok(Value::float64(i as f64))
            } else if let Some(f) = value.as_f64() {
                Ok(Value::float64(f))
            } else if let Some(n) = value.as_numeric() {
                n.to_f64().map(Value::float64).ok_or_else(|| {
                    Error::ExecutionError("Numeric to Float64 conversion failed".to_string())
                })
            } else if let Some(s) = value.as_str() {
                s.trim().parse::<f64>().map(Value::float64).map_err(|_| {
                    Error::InvalidOperation(format!(
                        "Invalid float parse: cannot cast '{}' to FLOAT64",
                        s
                    ))
                })
            } else if let Some(b) = value.as_bool() {
                Ok(Value::float64(if b { 1.0 } else { 0.0 }))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Float64".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Numeric(precision_scale) => {
            if let Some(n) = value.as_numeric() {
                apply_numeric_precision_scale(n, precision_scale).map(Value::numeric)
            } else if let Some(i) = value.as_i64() {
                let decimal = Decimal::from(i);
                apply_numeric_precision_scale(decimal, precision_scale).map(Value::numeric)
            } else if let Some(f) = value.as_f64() {
                let decimal = Decimal::from_f64_retain(f).ok_or_else(|| {
                    Error::InvalidOperation(format!(
                        "Cannot convert FLOAT64 {} to NUMERIC (NaN or Infinity)",
                        f
                    ))
                })?;
                apply_numeric_precision_scale(decimal, precision_scale).map(Value::numeric)
            } else if let Some(s) = value.as_str() {
                let trimmed = s.trim();
                let decimal = Decimal::from_str(trimmed).map_err(|_| {
                    Error::InvalidOperation(format!(
                        "Invalid numeric value: cannot cast '{}' to NUMERIC",
                        s
                    ))
                })?;
                apply_numeric_precision_scale(decimal, precision_scale).map(Value::numeric)
            } else if let Some(b) = value.as_bool() {
                let decimal = if b { Decimal::ONE } else { Decimal::ZERO };
                apply_numeric_precision_scale(decimal, precision_scale).map(Value::numeric)
            } else {
                Err(Error::TypeMismatch {
                    expected: "Numeric".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::String => {
            if let Some(s) = value.as_str() {
                Ok(Value::string(s.to_string()))
            } else if let Some(bytes) = value.as_bytes() {
                String::from_utf8(bytes.to_vec())
                    .map(Value::string)
                    .map_err(|_| {
                        Error::InvalidOperation(
                            "Cannot cast BYTES to STRING: invalid UTF-8 sequence".to_string(),
                        )
                    })
            } else if let Some(i) = value.as_i64() {
                Ok(Value::string(i.to_string()))
            } else if let Some(f) = value.as_f64() {
                Ok(Value::string(f.to_string()))
            } else if let Some(b) = value.as_bool() {
                Ok(Value::string(if b { "true" } else { "false" }.to_string()))
            } else if let Some(n) = value.as_numeric() {
                Ok(Value::string(n.normalize().to_string()))
            } else if let Some(d) = value.as_date() {
                Ok(Value::string(d.to_string()))
            } else if let Some(t) = value.as_time() {
                Ok(Value::string(t.to_string()))
            } else if let Some(ts) = value.as_timestamp() {
                Ok(Value::string(ts.to_string()))
            } else if let Some(dt) = value.as_datetime() {
                Ok(Value::string(dt.to_string()))
            } else if let Some(u) = value.as_uuid() {
                Ok(Value::string(u.to_string()))
            } else if let Some(j) = value.as_json() {
                Ok(Value::string(j.to_string()))
            } else if let Some(p) = value.as_point() {
                Ok(Value::string(p.to_string()))
            } else if let Some(b) = value.as_pgbox() {
                Ok(Value::string(b.to_string()))
            } else if let Some(c) = value.as_circle() {
                Ok(Value::string(c.to_string()))
            } else if let Some(interval) = value.as_interval() {
                Ok(Value::string(format_interval(interval)))
            } else {
                Err(Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Bool => {
            if let Some(b) = value.as_bool() {
                Ok(Value::bool_val(b))
            } else if let Some(i) = value.as_i64() {
                Ok(Value::bool_val(i != 0))
            } else if let Some(f) = value.as_f64() {
                Ok(Value::bool_val(f != 0.0))
            } else if let Some(n) = value.as_numeric() {
                Ok(Value::bool_val(!n.is_zero()))
            } else if let Some(s) = value.as_str() {
                parse_bool_string(s)
                    .map(Value::bool_val)
                    .ok_or_else(|| Error::InvalidOperation(format!("Cannot cast '{}' to BOOL", s)))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Bool".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Json => {
            if let Some(j) = value.as_json() {
                Ok(Value::json(j.clone()))
            } else if let Some(s) = value.as_str() {
                yachtsql_functions::json::parse_json(&Value::string(s.to_string()))
            } else {
                serde_json::to_value(value.clone())
                    .map(Value::json)
                    .map_err(|e| Error::ExecutionError(format!("Cannot convert to JSON: {}", e)))
            }
        }
        CastDataType::Array(inner) => {
            if let Some(elements) = value.as_array() {
                let mut casted = Vec::with_capacity(elements.len());
                for elem in elements {
                    casted.push(perform_cast(elem, inner)?);
                }
                Ok(Value::array(casted))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Array".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Bytes => {
            if let Some(b) = value.as_bytes() {
                Ok(Value::bytes(b.to_vec()))
            } else if let Some(s) = value.as_str() {
                Ok(Value::bytes(s.as_bytes().to_vec()))
            } else if let Some(i) = value.as_i64() {
                Ok(Value::bytes(i.to_be_bytes().to_vec()))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Bytes".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Geography => {
            if let Some(g) = value.as_geography() {
                Ok(Value::geography(g.to_string()))
            } else if let Some(s) = value.as_str() {
                let geom = yachtsql_functions::geography::parse_wkt(s)?;
                Ok(Value::geography(geom.to_wkt()))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Geography".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Date => {
            if let Some(d) = value.as_date() {
                Ok(Value::date(d))
            } else if let Some(s) = value.as_str() {
                use chrono::NaiveDate;
                NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                    .map(Value::date)
                    .map_err(|_| Error::InvalidOperation(format!("Cannot cast '{}' to DATE", s)))
            } else if let Some(ts) = value.as_timestamp() {
                Ok(Value::date(ts.date_naive()))
            } else if let Some(dt) = value.as_datetime() {
                Ok(Value::date(dt.date_naive()))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Date".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Timestamp => {
            if let Some(ts) = value.as_timestamp() {
                Ok(Value::timestamp(ts))
            } else if let Some(s) = value.as_str() {
                use yachtsql_core::types::parse_timestamp_to_utc;
                parse_timestamp_to_utc(s.trim())
                    .map(Value::timestamp)
                    .ok_or_else(|| {
                        Error::InvalidOperation(format!("Cannot cast '{}' to TIMESTAMP", s))
                    })
            } else if let Some(d) = value.as_date() {
                use chrono::NaiveTime;

                let ndt =
                    d.and_time(NaiveTime::from_hms_opt(0, 0, 0).expect("valid midnight time"));
                use chrono::{DateTime, Utc};
                Ok(Value::timestamp(
                    DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc),
                ))
            } else if let Some(dt) = value.as_datetime() {
                Ok(Value::timestamp(dt))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Timestamp".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::DateTime => {
            if let Some(dt) = value.as_datetime() {
                Ok(Value::datetime(dt))
            } else if let Some(ts) = value.as_timestamp() {
                Ok(Value::datetime(ts))
            } else if let Some(d) = value.as_date() {
                use chrono::NaiveTime;

                let ndt =
                    d.and_time(NaiveTime::from_hms_opt(0, 0, 0).expect("valid midnight time"));
                use chrono::{DateTime, Utc};
                Ok(Value::datetime(DateTime::<Utc>::from_naive_utc_and_offset(
                    ndt, Utc,
                )))
            } else if let Some(s) = value.as_str() {
                use yachtsql_core::types::parse_timestamp_to_utc;
                parse_timestamp_to_utc(s.trim())
                    .map(Value::datetime)
                    .ok_or_else(|| {
                        Error::InvalidOperation(format!("Cannot cast '{}' to DATETIME", s))
                    })
            } else {
                Err(Error::TypeMismatch {
                    expected: "DateTime".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Time => {
            if let Some(t) = value.as_time() {
                Ok(Value::time(t))
            } else if let Some(s) = value.as_str() {
                use chrono::NaiveTime;
                NaiveTime::parse_from_str(s.trim(), "%H:%M:%S")
                    .or_else(|_| NaiveTime::parse_from_str(s.trim(), "%H:%M:%S%.f"))
                    .map(Value::time)
                    .map_err(|_| Error::InvalidOperation(format!("Cannot cast '{}' to TIME", s)))
            } else if let Some(ts) = value.as_timestamp() {
                Ok(Value::time(ts.time()))
            } else if let Some(dt) = value.as_datetime() {
                Ok(Value::time(dt.time()))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Time".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::TimestampTz => {
            if let Some(ts) = value.as_timestamp() {
                Ok(Value::timestamp(ts))
            } else if let Some(s) = value.as_str() {
                use yachtsql_core::types::parse_timestamp_to_utc;
                parse_timestamp_to_utc(s.trim())
                    .map(Value::timestamp)
                    .ok_or_else(|| {
                        Error::InvalidOperation(format!(
                            "Cannot cast '{}' to TIMESTAMP WITH TIME ZONE",
                            s
                        ))
                    })
            } else {
                Err(Error::TypeMismatch {
                    expected: "TimestampTz".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Vector(_dims) => {
            if let Some(vec) = value.as_vector() {
                Ok(Value::vector(vec.clone()))
            } else if let Some(arr) = value.as_array() {
                let values: Result<Vec<f64>> = arr
                    .iter()
                    .map(|v| {
                        v.as_f64()
                            .or_else(|| v.as_i64().map(|i| i as f64))
                            .ok_or_else(|| {
                                Error::InvalidOperation(format!(
                                    "Cannot convert {:?} to vector element",
                                    v
                                ))
                            })
                    })
                    .collect();
                Ok(Value::vector(values?))
            } else if let Some(s) = value.as_str() {
                parse_vector_from_string(s)
            } else {
                Err(Error::TypeMismatch {
                    expected: "Vector".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Interval => {
            if let Some(_interval) = value.as_interval() {
                Ok(value.clone())
            } else if let Some(s) = value.as_str() {
                parse_interval_from_string(s).map(Value::interval)
            } else {
                Err(Error::TypeMismatch {
                    expected: "Interval".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Uuid => {
            if let Some(s) = value.as_str() {
                crate::types::parse_uuid_strict(s)
            } else {
                Err(Error::TypeMismatch {
                    expected: "Uuid".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Hstore => {
            if let Some(h) = value.as_hstore() {
                Ok(Value::hstore(h.clone()))
            } else if let Some(s) = value.as_str() {
                Value::hstore_from_str(s)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid HSTORE format: {}", e)))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Hstore".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::MacAddr => {
            if let Some(mac) = value.as_macaddr() {
                Ok(Value::macaddr(mac.clone()))
            } else if let Some(s) = value.as_str() {
                use yachtsql_core::types::MacAddress;
                match MacAddress::parse(s, false) {
                    Some(mac) => Ok(Value::macaddr(mac)),
                    None => Err(Error::InvalidOperation(format!(
                        "Invalid MAC address: '{}'",
                        s
                    ))),
                }
            } else {
                Err(Error::TypeMismatch {
                    expected: "MacAddr".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::MacAddr8 => {
            if let Some(mac) = value.as_macaddr8() {
                Ok(Value::macaddr8(mac.clone()))
            } else if let Some(mac) = value.as_macaddr() {
                Ok(Value::macaddr8(mac.to_eui64()))
            } else if let Some(s) = value.as_str() {
                use yachtsql_core::types::MacAddress;

                match MacAddress::parse(s, true) {
                    Some(mac) => Ok(Value::macaddr8(mac)),
                    None => match MacAddress::parse(s, false) {
                        Some(mac) => Ok(Value::macaddr8(mac.to_eui64())),
                        None => Err(Error::InvalidOperation(format!(
                            "Invalid MAC address: '{}'",
                            s
                        ))),
                    },
                }
            } else {
                Err(Error::TypeMismatch {
                    expected: "MacAddr8".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Inet => {
            if value.as_inet().is_some() {
                Ok(value.clone())
            } else if let Some(s) = value.as_str() {
                Value::inet_from_str(s).map_err(|e| {
                    Error::InvalidOperation(format!("Invalid INET address '{}': {}", s, e))
                })
            } else {
                Err(Error::TypeMismatch {
                    expected: "Inet".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Cidr => {
            if value.as_cidr().is_some() {
                Ok(value.clone())
            } else if let Some(s) = value.as_str() {
                Value::cidr_from_str(s).map_err(|e| {
                    Error::InvalidOperation(format!("Invalid CIDR address '{}': {}", s, e))
                })
            } else {
                Err(Error::TypeMismatch {
                    expected: "Cidr".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Int4Range => {
            if let Some(range) = value.as_range() {
                Ok(Value::range(range.clone()))
            } else if let Some(s) = value.as_str() {
                parse_range_from_string(s, yachtsql_core::types::RangeType::Int4Range)
            } else {
                Err(Error::TypeMismatch {
                    expected: "INT4RANGE".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Int8Range => {
            if let Some(range) = value.as_range() {
                Ok(Value::range(range.clone()))
            } else if let Some(s) = value.as_str() {
                parse_range_from_string(s, yachtsql_core::types::RangeType::Int8Range)
            } else {
                Err(Error::TypeMismatch {
                    expected: "INT8RANGE".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::NumRange => {
            if let Some(range) = value.as_range() {
                Ok(Value::range(range.clone()))
            } else if let Some(s) = value.as_str() {
                parse_range_from_string(s, yachtsql_core::types::RangeType::NumRange)
            } else {
                Err(Error::TypeMismatch {
                    expected: "NUMRANGE".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::TsRange => {
            if let Some(range) = value.as_range() {
                Ok(Value::range(range.clone()))
            } else if let Some(s) = value.as_str() {
                parse_range_from_string(s, yachtsql_core::types::RangeType::TsRange)
            } else {
                Err(Error::TypeMismatch {
                    expected: "TSRANGE".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::TsTzRange => {
            if let Some(range) = value.as_range() {
                Ok(Value::range(range.clone()))
            } else if let Some(s) = value.as_str() {
                parse_range_from_string(s, yachtsql_core::types::RangeType::TsTzRange)
            } else {
                Err(Error::TypeMismatch {
                    expected: "TSTZRANGE".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::DateRange => {
            if let Some(range) = value.as_range() {
                Ok(Value::range(range.clone()))
            } else if let Some(s) = value.as_str() {
                parse_range_from_string(s, yachtsql_core::types::RangeType::DateRange)
            } else {
                Err(Error::TypeMismatch {
                    expected: "DATERANGE".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Point => {
            if value.as_point().is_some() {
                Ok(value.clone())
            } else if let Some(s) = value.as_str() {
                use yachtsql_core::types::PgPoint;
                PgPoint::parse(s)
                    .map(Value::point)
                    .ok_or_else(|| Error::InvalidOperation(format!("Invalid POINT '{}'", s)))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Point".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::PgBox => {
            if value.as_pgbox().is_some() {
                Ok(value.clone())
            } else if let Some(s) = value.as_str() {
                use yachtsql_core::types::PgBox;
                PgBox::parse(s)
                    .map(Value::pgbox)
                    .ok_or_else(|| Error::InvalidOperation(format!("Invalid BOX '{}'", s)))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Box".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Circle => {
            if value.as_circle().is_some() {
                Ok(value.clone())
            } else if let Some(s) = value.as_str() {
                use yachtsql_core::types::PgCircle;
                PgCircle::parse(s)
                    .map(Value::circle)
                    .ok_or_else(|| Error::InvalidOperation(format!("Invalid CIRCLE '{}'", s)))
            } else {
                Err(Error::TypeMismatch {
                    expected: "Circle".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
        CastDataType::Custom(_name, struct_fields) => {
            if let Some(struct_val) = value.as_struct() {
                if struct_fields.is_empty() {
                    Ok(value.clone())
                } else {
                    let old_values: Vec<_> = struct_val.values().cloned().collect();
                    if old_values.len() != struct_fields.len() {
                        return Err(Error::TypeMismatch {
                            expected: format!("Struct with {} fields", struct_fields.len()),
                            actual: format!("Struct with {} fields", old_values.len()),
                        });
                    }
                    let new_fields: Vec<_> = struct_fields
                        .iter()
                        .zip(old_values)
                        .map(|(field, val)| (field.name.clone(), val))
                        .collect();
                    Ok(Value::struct_val(new_fields.into_iter().collect()))
                }
            } else {
                Err(Error::TypeMismatch {
                    expected: "Composite type".to_string(),
                    actual: format!("{:?}", value.data_type()),
                })
            }
        }
    }
}

fn parse_range_from_string(s: &str, range_type: yachtsql_core::types::RangeType) -> Result<Value> {
    use yachtsql_core::types::{Range, RangeType};

    let s = s.trim();

    if s == "empty" || s.is_empty() {
        return Ok(Value::range(Range {
            range_type,
            lower: None,
            upper: None,
            lower_inclusive: false,
            upper_inclusive: false,
        }));
    }

    if s.len() < 3 {
        return Err(Error::InvalidOperation(format!(
            "Invalid range format: '{}'",
            s
        )));
    }

    let lower_inclusive = s.starts_with('[');
    let upper_inclusive = s.ends_with(']');

    let inner = &s[1..s.len() - 1];
    let comma_pos = inner.find(',').ok_or_else(|| {
        Error::InvalidOperation(format!("Invalid range format (no comma): '{}'", s))
    })?;

    let lower_str = inner[..comma_pos].trim();
    let upper_str = inner[comma_pos + 1..].trim();

    let lower = if lower_str.is_empty() {
        None
    } else {
        Some(parse_range_bound(lower_str, &range_type)?)
    };

    let upper = if upper_str.is_empty() {
        None
    } else {
        Some(parse_range_bound(upper_str, &range_type)?)
    };

    Ok(Value::range(Range {
        range_type,
        lower,
        upper,
        lower_inclusive,
        upper_inclusive,
    }))
}

fn parse_range_bound(s: &str, range_type: &yachtsql_core::types::RangeType) -> Result<Value> {
    use yachtsql_core::types::RangeType;

    match range_type {
        RangeType::Int4Range | RangeType::Int8Range => {
            let val: i64 = s.parse().map_err(|_| {
                Error::InvalidOperation(format!("Invalid integer in range: '{}'", s))
            })?;
            Ok(Value::int64(val))
        }
        RangeType::NumRange => {
            let val: f64 = s.parse().map_err(|_| {
                Error::InvalidOperation(format!("Invalid number in range: '{}'", s))
            })?;
            Ok(Value::float64(val))
        }
        RangeType::DateRange => {
            use chrono::NaiveDate;
            let date = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                .map_err(|_| Error::InvalidOperation(format!("Invalid date in range: '{}'", s)))?;
            Ok(Value::date(date))
        }
        RangeType::TsRange | RangeType::TsTzRange => {
            use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
            let s = s.trim();
            let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| {
                    NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                })
                .or_else(|_| {
                    DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%z").map(|dt| dt.naive_utc())
                })
                .or_else(|_| {
                    let s_no_tz =
                        s.trim_end_matches(|c: char| c == '+' || c == '-' || c.is_numeric());
                    NaiveDateTime::parse_from_str(s_no_tz.trim(), "%Y-%m-%d %H:%M:%S")
                })
                .map_err(|_| {
                    Error::InvalidOperation(format!("Invalid timestamp in range: '{}'", s))
                })?;
            Ok(Value::timestamp(Utc.from_utc_datetime(&dt)))
        }
    }
}

pub fn parse_vector_from_string(s: &str) -> Result<Value> {
    let s = s.trim();

    let inner = if s.starts_with('[') && s.ends_with(']') {
        &s[1..s.len() - 1]
    } else {
        s
    };

    let values: Result<Vec<f64>> = inner
        .split(',')
        .map(|part| {
            part.trim()
                .parse::<f64>()
                .map_err(|_| Error::InvalidOperation(format!("Invalid vector element: '{}'", part)))
        })
        .collect();

    Ok(Value::vector(values?))
}

pub fn evaluate_numeric_op(
    left: &rust_decimal::Decimal,
    op: &yachtsql_optimizer::expr::BinaryOp,
    right: &rust_decimal::Decimal,
) -> Result<Value> {
    use yachtsql_optimizer::expr::BinaryOp;

    let result = match op {
        BinaryOp::Add => left
            .checked_add(*right)
            .ok_or_else(|| Error::ExecutionError("Numeric addition overflow".to_string()))?,
        BinaryOp::Subtract => left
            .checked_sub(*right)
            .ok_or_else(|| Error::ExecutionError("Numeric subtraction overflow".to_string()))?,
        BinaryOp::Multiply => left
            .checked_mul(*right)
            .ok_or_else(|| Error::ExecutionError("Numeric multiplication overflow".to_string()))?,
        BinaryOp::Divide => {
            if right.is_zero() {
                return Ok(Value::null());
            }
            left.checked_div(*right)
                .ok_or_else(|| Error::ExecutionError("Numeric division overflow".to_string()))?
        }
        BinaryOp::Modulo => {
            if right.is_zero() {
                return Ok(Value::null());
            }
            left.checked_rem(*right)
                .ok_or_else(|| Error::ExecutionError("Numeric modulo overflow".to_string()))?
        }
        _ => {
            return Err(Error::ExecutionError(format!(
                "Unsupported numeric operation: {:?}",
                op
            )));
        }
    };

    Ok(Value::numeric(result))
}

pub fn evaluate_vector_l2_distance(left: &Value, right: &Value) -> Result<Value> {
    let left_vec = extract_f64_vector(left)?;
    let right_vec = extract_f64_vector(right)?;

    if left_vec.len() != right_vec.len() {
        return Err(Error::ExecutionError(format!(
            "Vector dimension mismatch: {} vs {}",
            left_vec.len(),
            right_vec.len()
        )));
    }

    let sum_squares: f64 = left_vec
        .iter()
        .zip(right_vec.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum();

    Ok(Value::float64(sum_squares.sqrt()))
}

pub fn evaluate_vector_inner_product(left: &Value, right: &Value) -> Result<Value> {
    let left_vec = extract_f64_vector(left)?;
    let right_vec = extract_f64_vector(right)?;

    if left_vec.len() != right_vec.len() {
        return Err(Error::ExecutionError(format!(
            "Vector dimension mismatch: {} vs {}",
            left_vec.len(),
            right_vec.len()
        )));
    }

    let dot_product: f64 = left_vec
        .iter()
        .zip(right_vec.iter())
        .map(|(a, b)| a * b)
        .sum();

    Ok(Value::float64(dot_product))
}

pub fn evaluate_vector_cosine_distance(left: &Value, right: &Value) -> Result<Value> {
    let left_vec = extract_f64_vector(left)?;
    let right_vec = extract_f64_vector(right)?;

    if left_vec.len() != right_vec.len() {
        return Err(Error::ExecutionError(format!(
            "Vector dimension mismatch: {} vs {}",
            left_vec.len(),
            right_vec.len()
        )));
    }

    let dot_product: f64 = left_vec
        .iter()
        .zip(right_vec.iter())
        .map(|(a, b)| a * b)
        .sum();

    let magnitude_left: f64 = left_vec.iter().map(|x| x * x).sum::<f64>().sqrt();
    let magnitude_right: f64 = right_vec.iter().map(|x| x * x).sum::<f64>().sqrt();

    if magnitude_left == 0.0 || magnitude_right == 0.0 {
        return Ok(Value::null());
    }

    let cosine_similarity = dot_product / (magnitude_left * magnitude_right);

    Ok(Value::float64(1.0 - cosine_similarity))
}

fn extract_f64_vector(value: &Value) -> Result<Vec<f64>> {
    if let Some(vec) = value.as_vector() {
        return Ok(vec.clone());
    }

    if let Some(arr) = value.as_array() {
        let mut result = Vec::with_capacity(arr.len());
        for elem in arr {
            if let Some(f) = elem.as_f64() {
                result.push(f);
            } else if let Some(i) = elem.as_i64() {
                result.push(i as f64);
            } else {
                return Err(Error::TypeMismatch {
                    expected: "Float64 or Int64".to_string(),
                    actual: format!("{:?}", elem.data_type()),
                });
            }
        }
        return Ok(result);
    }

    if let Some(s) = value.as_str() {
        return parse_vector_string(s);
    }

    Err(Error::TypeMismatch {
        expected: "Vector, Array, or vector string".to_string(),
        actual: format!("{:?}", value.data_type()),
    })
}

fn parse_vector_string(s: &str) -> Result<Vec<f64>> {
    let trimmed = s.trim();
    let inner = if trimmed.starts_with('[') && trimmed.ends_with(']') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };

    if inner.is_empty() {
        return Ok(vec![]);
    }

    let mut result = Vec::new();
    for part in inner.split(',') {
        let val: f64 = part.trim().parse().map_err(|_| Error::TypeMismatch {
            expected: "valid float in vector string".to_string(),
            actual: part.trim().to_string(),
        })?;
        result.push(val);
    }
    Ok(result)
}
