#![allow(clippy::approx_constant)]

use yachtsql::{DataType, Table, Value};
use yachtsql_storage::{Field, Schema};

pub trait IntoValue {
    fn into_value(self) -> Value;
}

impl IntoValue for Value {
    fn into_value(self) -> Value {
        self
    }
}

impl IntoValue for i64 {
    fn into_value(self) -> Value {
        Value::int64(self)
    }
}

impl IntoValue for i32 {
    fn into_value(self) -> Value {
        Value::int64(self as i64)
    }
}

impl IntoValue for u64 {
    fn into_value(self) -> Value {
        Value::int64(self as i64)
    }
}

impl IntoValue for f64 {
    fn into_value(self) -> Value {
        Value::float64(self)
    }
}

impl IntoValue for &str {
    fn into_value(self) -> Value {
        Value::string(self.to_string())
    }
}

impl IntoValue for bool {
    fn into_value(self) -> Value {
        Value::bool_val(self)
    }
}

#[macro_export]
macro_rules! val {
    ([]) => { $crate::common::array(vec![]) };
    ([$($elem:tt)*]) => {
        $crate::common::array($crate::vals![$($elem)*])
    };
    ({}) => { $crate::common::stv(vec![]) };
    ({{ $($val:tt)* }}) => {
        $crate::common::stv_numeric($crate::vals![$($val)*])
    };
    ({ $($val:tt)* }) => {
        $crate::common::stv($crate::vals![$($val)*])
    };
    (( $($val:tt),+ $(,)? )) => {
        $crate::common::tuple($crate::vals![$($val),+])
    };
    (null) => { $crate::common::null() };
    (true) => { $crate::common::IntoValue::into_value(true) };
    (false) => { $crate::common::IntoValue::into_value(false) };
    ($($e:tt)+) => { $crate::common::IntoValue::into_value($($e)+) };
}

#[macro_export]
macro_rules! vals {
    () => { vec![] };
    ($e:tt $(,)?) => { vec![$crate::val!($e)] };
    ($id:ident $args:tt $(, $($rest:tt)*)?) => {
        {
            let mut v = vec![$crate::val!($id $args)];
            v.extend($crate::vals![$($($rest)*)?]);
            v
        }
    };
    (- $e:tt $(, $($rest:tt)*)?) => {
        {
            let mut v = vec![$crate::val!(- $e)];
            v.extend($crate::vals![$($($rest)*)?]);
            v
        }
    };
    ($e:tt $(, $($rest:tt)*)?) => {
        {
            let mut v = vec![$crate::val!($e)];
            v.extend($crate::vals![$($($rest)*)?]);
            v
        }
    };
}

#[macro_export]
macro_rules! table {
    [$([$($val:tt)*]),* $(,)?] => {
        $crate::common::table(vec![$($crate::vals![$($val)*]),*])
    };
}

#[macro_export]
macro_rules! assert_table_eq {
    ($actual:expr, []) => {{
        let actual_table = &$actual;
        assert_eq!(actual_table.num_rows(), 0, "Expected empty table but got {} rows", actual_table.num_rows());
    }};
    ($actual:expr, [$([$($val:tt)*]),* $(,)?]) => {{
        let actual_table = &$actual;
        let expected_rows: Vec<Vec<yachtsql::Value>> = vec![$($crate::vals![$($val)*]),*];
        let expected_table = yachtsql::Table::from_values(
            actual_table.schema().clone(),
            expected_rows,
        ).expect("Failed to create expected table");
        assert_eq!(*actual_table, expected_table);
    }};
}

pub fn table(rows: Vec<Vec<Value>>) -> Table {
    if rows.is_empty() {
        return Table::from_values(Schema::new(), vec![]).unwrap();
    }

    let num_cols = rows[0].len();
    let fields: Vec<Field> = (0..num_cols)
        .map(|i| {
            let data_type = rows
                .iter()
                .map(|row| row.get(i).map(|v| v.data_type()))
                .find(|dt| {
                    dt.as_ref()
                        .map(|t| *t != DataType::Unknown)
                        .unwrap_or(false)
                })
                .flatten()
                .unwrap_or(DataType::String);
            Field::nullable(format!("col{}", i), data_type)
        })
        .collect();

    let schema = Schema::from_fields(fields);
    Table::from_values(schema, rows).unwrap()
}

pub fn str(val: &str) -> Value {
    Value::string(val.to_string())
}

pub fn bool(val: bool) -> Value {
    Value::bool_val(val)
}

pub fn numeric(val: &str) -> Value {
    use std::str::FromStr;

    use rust_decimal::Decimal;
    Value::numeric(Decimal::from_str(val).unwrap())
}

pub fn i64(val: i64) -> Value {
    Value::int64(val)
}

pub fn f64(val: f64) -> Value {
    Value::float64(val)
}

pub fn array(vals: Vec<Value>) -> Value {
    Value::array(vals)
}

pub fn st(fields: Vec<(&str, Value)>) -> Value {
    Value::struct_val(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

pub fn stv(vals: Vec<Value>) -> Value {
    Value::struct_val(
        vals.into_iter()
            .enumerate()
            .map(|(i, v)| (format!("_field{}", i), v))
            .collect(),
    )
}

pub fn stv_numeric(vals: Vec<Value>) -> Value {
    Value::struct_val(
        vals.into_iter()
            .enumerate()
            .map(|(i, v)| {
                let coerced = match v {
                    Value::Float64(f) => {
                        let d = rust_decimal::Decimal::from_f64_retain(f.0)
                            .unwrap_or(rust_decimal::Decimal::ZERO);
                        Value::numeric(d)
                    }
                    Value::Null
                    | Value::Bool(_)
                    | Value::Int64(_)
                    | Value::Numeric(_)
                    | Value::String(_)
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
                    | Value::Range(_) => v,
                };
                (format!("_field{}", i), coerced)
            })
            .collect(),
    )
}

pub fn tuple(vals: Vec<Value>) -> Value {
    Value::struct_val(
        vals.into_iter()
            .enumerate()
            .map(|(i, v)| ((i + 1).to_string(), v))
            .collect(),
    )
}

pub fn null() -> Value {
    Value::null()
}

pub fn date(year: i32, month: u32, day: u32) -> Value {
    Value::date(chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap())
}

pub fn ip(addr: &str) -> Value {
    let octets: Vec<u8> = addr.split('.').map(|s| s.parse().unwrap()).collect();
    Value::bytes(octets)
}

pub fn timestamp(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> Value {
    use chrono::{TimeZone, Utc};
    let dt = Utc
        .with_ymd_and_hms(year, month, day, hour, min, sec)
        .unwrap();
    Value::timestamp(dt)
}

pub fn d(year: i32, month: u32, day: u32) -> Value {
    date(year, month, day)
}

pub fn ts(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> Value {
    timestamp(year, month, day, hour, min, sec)
}

pub fn ts_ms(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32, millis: u32) -> Value {
    use chrono::{TimeZone, Timelike, Utc};
    let dt = Utc
        .with_ymd_and_hms(year, month, day, hour, min, sec)
        .unwrap()
        .with_nanosecond(millis * 1_000_000)
        .unwrap();
    Value::timestamp(dt)
}

pub fn n(val: &str) -> Value {
    numeric(val)
}

pub fn dt(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> Value {
    datetime(year, month, day, hour, min, sec)
}

pub fn datetime(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> Value {
    use chrono::NaiveDate;
    let dt = NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .and_hms_opt(hour, min, sec)
        .unwrap();
    Value::datetime(dt)
}

pub fn time(hour: u32, min: u32, sec: u32) -> Value {
    use chrono::NaiveTime;
    let t = NaiveTime::from_hms_opt(hour, min, sec).unwrap();
    Value::time(t)
}

pub fn tm(hour: u32, min: u32, sec: u32) -> Value {
    time(hour, min, sec)
}
