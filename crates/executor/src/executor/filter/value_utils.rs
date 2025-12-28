use chrono::{NaiveDate, Timelike};

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::Literal;

use super::super::PlanExecutor;

const UNIX_EPOCH_DATE: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
    Some(d) => d,
    None => panic!("Invalid date"),
};

impl PlanExecutor<'_> {
    pub(super) fn value_to_literal(value: Value) -> Literal {
        match value {
            Value::Null => Literal::Null,
            Value::Bool(b) => Literal::Bool(b),
            Value::Int64(n) => Literal::Int64(n),
            Value::Float64(f) => Literal::Float64(f),
            Value::Numeric(d) => Literal::Numeric(d),
            Value::BigNumeric(d) => Literal::BigNumeric(d),
            Value::String(s) => Literal::String(s),
            Value::Bytes(b) => Literal::Bytes(b),
            Value::Date(d) => {
                let days = d.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i32;
                Literal::Date(days)
            }
            Value::Time(t) => {
                let nanos =
                    t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64;
                Literal::Time(nanos)
            }
            Value::Timestamp(ts) => {
                let micros = ts.timestamp_micros();
                Literal::Timestamp(micros)
            }
            Value::DateTime(dt) => {
                let micros = dt.and_utc().timestamp_micros();
                Literal::Datetime(micros)
            }
            Value::Interval(iv) => Literal::Interval {
                months: iv.months,
                days: iv.days,
                nanos: iv.nanos,
            },
            Value::Array(arr) => {
                Literal::Array(arr.into_iter().map(Self::value_to_literal).collect())
            }
            Value::Struct(fields) => Literal::Struct(
                fields
                    .into_iter()
                    .map(|(k, v)| (k, Self::value_to_literal(v)))
                    .collect(),
            ),
            Value::Json(j) => Literal::Json(j),
            Value::Geography(_) => Literal::Null,
            Value::Range(_) => Literal::Null,
            Value::Default => Literal::Null,
        }
    }

    pub(super) fn arithmetic_op<F>(left: &Value, right: &Value, op: F) -> Result<Value>
    where
        F: Fn(f64, f64) -> f64,
    {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(op(*a as f64, *b as f64) as i64)),
            (Value::Float64(a), Value::Float64(b)) => {
                Ok(Value::Float64(ordered_float::OrderedFloat(op(a.0, b.0))))
            }
            (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(
                ordered_float::OrderedFloat(op(*a as f64, b.0)),
            )),
            (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(
                ordered_float::OrderedFloat(op(a.0, *b as f64)),
            )),
            _ => Err(Error::InvalidQuery(format!(
                "Cannot perform arithmetic on {:?} and {:?}",
                left, right
            ))),
        }
    }

    pub(super) fn values_equal(left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Null, Value::Null) => true,
            (Value::Null, _) | (_, Value::Null) => false,
            (Value::Int64(a), Value::Float64(b)) => (*a as f64) == b.0,
            (Value::Float64(a), Value::Int64(b)) => a.0 == (*b as f64),
            _ => left == right,
        }
    }

    pub(super) fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            (Value::Int64(a), Value::Float64(b)) => {
                let a_f64 = *a as f64;
                a_f64.partial_cmp(&b.0).unwrap_or(Ordering::Equal)
            }
            (Value::Float64(a), Value::Int64(b)) => {
                let b_f64 = *b as f64;
                a.0.partial_cmp(&b_f64).unwrap_or(Ordering::Equal)
            }
            _ => left.cmp(right),
        }
    }
}
