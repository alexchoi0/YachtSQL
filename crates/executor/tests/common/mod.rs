#[allow(unused_imports)]
pub use yachtsql_test_utils::*;

pub mod assertions;

use yachtsql::{DataType, Table, Value};
use yachtsql_storage::{Field, Schema};

#[allow(dead_code)]
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

impl IntoValue for () {
    fn into_value(self) -> Value {
        Value::null()
    }
}

#[macro_export]
macro_rules! val {
    ([]) => { $crate::common::array(vec![]) };
    ([$($elem:tt)*]) => {
        $crate::common::array($crate::vals![$($elem)*])
    };
    ({}) => { $crate::common::stv(vec![]) };
    ({ $($val:tt)* }) => {
        $crate::common::stv($crate::vals![$($val)*])
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
macro_rules! batch {
    [$([$($val:tt)*]),* $(,)?] => {
        $crate::common::batch(vec![$($crate::vals![$($val)*]),*])
    };
}

#[macro_export]
macro_rules! assert_batch_eq {
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

#[allow(dead_code)]
pub fn batch(rows: Vec<Vec<Value>>) -> Table {
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

#[allow(dead_code)]
pub fn array(vals: Vec<Value>) -> Value {
    Value::array(vals)
}

#[allow(dead_code)]
pub fn stv(vals: Vec<Value>) -> Value {
    Value::struct_val(
        vals.into_iter()
            .enumerate()
            .map(|(i, v)| (format!("f{}", i), v))
            .collect(),
    )
}

#[allow(dead_code)]
pub fn null() -> Value {
    Value::null()
}

#[allow(dead_code)]
pub fn numeric(val: i64) -> Value {
    use rust_decimal::Decimal;
    Value::numeric(Decimal::from(val))
}
