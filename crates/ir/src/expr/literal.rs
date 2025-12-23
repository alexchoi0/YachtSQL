use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use yachtsql_common::types::DataType;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(ordered_float::OrderedFloat<f64>),
    Numeric(Decimal),
    BigNumeric(Decimal),
    String(String),
    Bytes(Vec<u8>),
    Date(i32),
    Time(i64),
    Timestamp(i64),
    Datetime(i64),
    Interval { months: i32, days: i32, nanos: i64 },
    Array(Vec<Literal>),
    Struct(Vec<(String, Literal)>),
    Json(serde_json::Value),
}

impl Literal {
    pub fn data_type(&self) -> DataType {
        match self {
            Literal::Null => DataType::Unknown,
            Literal::Bool(_) => DataType::Bool,
            Literal::Int64(_) => DataType::Int64,
            Literal::Float64(_) => DataType::Float64,
            Literal::Numeric(_) => DataType::Numeric(None),
            Literal::BigNumeric(_) => DataType::BigNumeric,
            Literal::String(_) => DataType::String,
            Literal::Bytes(_) => DataType::Bytes,
            Literal::Date(_) => DataType::Date,
            Literal::Time(_) => DataType::Time,
            Literal::Timestamp(_) => DataType::Timestamp,
            Literal::Datetime(_) => DataType::DateTime,
            Literal::Interval { .. } => DataType::Interval,
            Literal::Array(elements) => {
                let elem_type = elements
                    .first()
                    .map(|e| e.data_type())
                    .unwrap_or(DataType::Unknown);
                DataType::Array(Box::new(elem_type))
            }
            Literal::Struct(fields) => {
                let struct_fields = fields
                    .iter()
                    .map(|(name, lit)| yachtsql_common::types::StructField {
                        name: name.clone(),
                        data_type: lit.data_type(),
                    })
                    .collect();
                DataType::Struct(struct_fields)
            }
            Literal::Json(_) => DataType::Json,
        }
    }
}
