use std::rc::Rc;

use yachtsql_core::error::Result;
use yachtsql_core::types::{DataType, Value};
use yachtsql_ir::expr::{BinaryOp, Expr, LiteralValue, UnaryOp};
use yachtsql_storage::{Column, Schema};

use super::ExecutionPlan;
use crate::Table;

#[derive(Debug)]
pub struct ValuesExec {
    schema: Schema,
    rows: Vec<Vec<Expr>>,
}

impl ValuesExec {
    pub fn new(schema: Schema, rows: Vec<Vec<Expr>>) -> Self {
        Self { schema, rows }
    }

    fn evaluate_expr(&self, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::Literal(lit) => self.evaluate_literal(lit),
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expr(left)?;
                let right_val = self.evaluate_expr(right)?;
                self.evaluate_binary_op(&left_val, *op, &right_val)
            }
            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate_expr(expr)?;
                self.evaluate_unary_op(*op, &val)
            }
            Expr::Cast { expr, data_type } => {
                let val = self.evaluate_expr(expr)?;
                self.cast_value(&val, data_type)
            }
            _ => Ok(Value::null()),
        }
    }

    fn evaluate_literal(&self, lit: &LiteralValue) -> Result<Value> {
        Ok(match lit {
            LiteralValue::Null => Value::null(),
            LiteralValue::Boolean(b) => Value::bool_val(*b),
            LiteralValue::Int64(i) => Value::int64(*i),
            LiteralValue::Float64(f) => Value::float64(*f),
            LiteralValue::Numeric(d) => Value::numeric(*d),
            LiteralValue::String(s) => Value::string(s.clone()),
            LiteralValue::Bytes(b) => Value::bytes(b.clone()),
            LiteralValue::Date(d) => {
                use chrono::NaiveDate;
                match NaiveDate::parse_from_str(d, "%Y-%m-%d") {
                    Ok(date) => Value::date(date),
                    Err(_) => Value::null(),
                }
            }
            LiteralValue::Time(t) => {
                use chrono::NaiveTime;
                NaiveTime::parse_from_str(t, "%H:%M:%S")
                    .or_else(|_| NaiveTime::parse_from_str(t, "%H:%M:%S%.f"))
                    .or_else(|_| NaiveTime::parse_from_str(t, "%H:%M"))
                    .map(Value::time)
                    .unwrap_or(Value::null())
            }
            LiteralValue::DateTime(dt) => match crate::types::parse_timestamp_to_utc(dt) {
                Some(ts) => Value::datetime(ts),
                None => Value::null(),
            },
            LiteralValue::Timestamp(t) => match crate::types::parse_timestamp_to_utc(t) {
                Some(ts) => Value::timestamp(ts),
                None => Value::null(),
            },
            LiteralValue::Json(s) => match serde_json::from_str(s) {
                Ok(json_val) => Value::json(json_val),
                Err(_) => Value::null(),
            },
            LiteralValue::Uuid(s) => crate::types::parse_uuid_strict(s).unwrap_or(Value::null()),
            LiteralValue::Array(elements) => {
                let values: Result<Vec<_>> =
                    elements.iter().map(|e| self.evaluate_expr(e)).collect();
                Value::array(values?)
            }
            LiteralValue::Vector(vec) => Value::vector(vec.clone()),
            LiteralValue::Interval(_s) => Value::null(),
            LiteralValue::Range(_s) => Value::null(),
            LiteralValue::Point(s) => yachtsql_core::types::parse_point_literal(s),
            LiteralValue::PgBox(s) => yachtsql_core::types::parse_pgbox_literal(s),
            LiteralValue::Circle(s) => yachtsql_core::types::parse_circle_literal(s),
            LiteralValue::MacAddr(s) => {
                use yachtsql_core::types::MacAddress;
                match MacAddress::parse(s, false) {
                    Some(mac) => Value::macaddr(mac),
                    None => Value::null(),
                }
            }
            LiteralValue::MacAddr8(s) => {
                use yachtsql_core::types::MacAddress;
                match MacAddress::parse(s, true) {
                    Some(mac) => Value::macaddr8(mac),
                    None => match MacAddress::parse(s, false) {
                        Some(mac) => Value::macaddr8(mac.to_eui64()),
                        None => Value::null(),
                    },
                }
            }
        })
    }

    fn evaluate_binary_op(&self, left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
        match op {
            BinaryOp::Add => self.numeric_op(left, right, |a, b| a + b),
            BinaryOp::Subtract => self.numeric_op(left, right, |a, b| a - b),
            BinaryOp::Multiply => self.numeric_op(left, right, |a, b| a * b),
            BinaryOp::Divide => {
                self.numeric_op(left, right, |a, b| if b == 0.0 { f64::NAN } else { a / b })
            }
            BinaryOp::Modulo => {
                self.numeric_op(left, right, |a, b| if b == 0.0 { f64::NAN } else { a % b })
            }
            _ => Ok(Value::null()),
        }
    }

    fn numeric_op<F>(&self, left: &Value, right: &Value, f: F) -> Result<Value>
    where
        F: Fn(f64, f64) -> f64,
    {
        let left_num = self.value_to_f64(left);
        let right_num = self.value_to_f64(right);

        match (left_num, right_num) {
            (Some(l), Some(r)) => {
                let result = f(l, r);
                if left.as_i64().is_some() && right.as_i64().is_some() {
                    Ok(Value::int64(result as i64))
                } else {
                    Ok(Value::float64(result))
                }
            }
            _ => Ok(Value::null()),
        }
    }

    fn value_to_f64(&self, value: &Value) -> Option<f64> {
        if let Some(i) = value.as_i64() {
            return Some(i as f64);
        }
        if let Some(f) = value.as_f64() {
            return Some(f);
        }
        if let Some(d) = value.as_numeric() {
            return Some(d.to_string().parse::<f64>().unwrap_or(0.0));
        }
        None
    }

    fn evaluate_unary_op(&self, op: UnaryOp, val: &Value) -> Result<Value> {
        match op {
            UnaryOp::Negate => {
                if let Some(i) = val.as_i64() {
                    Ok(Value::int64(-i))
                } else if let Some(f) = val.as_f64() {
                    Ok(Value::float64(-f))
                } else {
                    Ok(Value::null())
                }
            }
            UnaryOp::Plus => Ok(val.clone()),
            UnaryOp::Not => {
                if let Some(b) = val.as_bool() {
                    Ok(Value::bool_val(!b))
                } else {
                    Ok(Value::null())
                }
            }
            _ => Ok(Value::null()),
        }
    }

    fn cast_value(
        &self,
        val: &Value,
        data_type: &yachtsql_ir::expr::CastDataType,
    ) -> Result<Value> {
        use yachtsql_ir::expr::CastDataType;

        match data_type {
            CastDataType::Int64 => {
                if let Some(s) = val.as_str() {
                    if let Ok(i) = s.parse::<i64>() {
                        return Ok(Value::int64(i));
                    }
                }
                if let Some(f) = val.as_f64() {
                    return Ok(Value::int64(f as i64));
                }
                Ok(val.clone())
            }
            CastDataType::Float64 => {
                if let Some(s) = val.as_str() {
                    if let Ok(f) = s.parse::<f64>() {
                        return Ok(Value::float64(f));
                    }
                }
                if let Some(i) = val.as_i64() {
                    return Ok(Value::float64(i as f64));
                }
                Ok(val.clone())
            }
            CastDataType::String => Ok(Value::string(format!("{}", val))),
            _ => Ok(val.clone()),
        }
    }
}

impl ExecutionPlan for ValuesExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        if self.rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let num_rows = self.rows.len();
        let num_cols = self.schema.fields().len();

        let mut evaluated_rows: Vec<Vec<Value>> = Vec::with_capacity(num_rows);
        for row in &self.rows {
            let mut evaluated_row = Vec::with_capacity(num_cols);
            for expr in row {
                evaluated_row.push(self.evaluate_expr(expr)?);
            }
            evaluated_rows.push(evaluated_row);
        }

        let mut columns: Vec<Column> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_rows);
            for row in &evaluated_rows {
                let value = row.get(col_idx).cloned().unwrap_or(Value::null());
                column.push(value)?;
            }
            columns.push(column);
        }

        Ok(vec![Table::new(self.schema.clone(), columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![]
    }

    fn describe(&self) -> String {
        format!("Values: {} rows", self.rows.len())
    }
}

pub fn infer_values_schema(rows: &[Vec<Expr>]) -> Schema {
    if rows.is_empty() {
        return Schema::from_fields(vec![]);
    }

    let num_cols = rows[0].len();
    let mut fields = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let mut inferred_type: Option<DataType> = None;

        for row in rows {
            if col_idx < row.len() {
                let expr = &row[col_idx];
                if let Some(expr_type) = infer_expr_type(expr) {
                    match &inferred_type {
                        None => inferred_type = Some(expr_type),
                        Some(current) => {
                            inferred_type = Some(promote_types(current, &expr_type));
                        }
                    }
                }
            }
        }

        let data_type = inferred_type.unwrap_or(DataType::String);
        let field_name = format!("column{}", col_idx + 1);
        fields.push(yachtsql_storage::Field::nullable(field_name, data_type));
    }

    Schema::from_fields(fields)
}

fn infer_expr_type(expr: &Expr) -> Option<DataType> {
    match expr {
        Expr::Literal(lit) => match lit {
            LiteralValue::Null => None,
            LiteralValue::Boolean(_) => Some(DataType::Bool),
            LiteralValue::Int64(_) => Some(DataType::Int64),
            LiteralValue::Float64(_) => Some(DataType::Float64),
            LiteralValue::Numeric(_) => Some(DataType::Numeric(None)),
            LiteralValue::String(_) => Some(DataType::String),
            LiteralValue::Bytes(_) => Some(DataType::Bytes),
            LiteralValue::Date(_) => Some(DataType::Date),
            LiteralValue::Time(_) => Some(DataType::Time),
            LiteralValue::DateTime(_) => Some(DataType::DateTime),
            LiteralValue::Timestamp(_) => Some(DataType::Timestamp),
            LiteralValue::Json(_) => Some(DataType::Json),
            LiteralValue::Uuid(_) => Some(DataType::Uuid),
            LiteralValue::Array(_) => Some(DataType::Array(Box::new(DataType::String))),
            LiteralValue::Vector(v) => Some(DataType::Vector(v.len())),
            LiteralValue::Interval(_) => Some(DataType::Interval),
            LiteralValue::Range(_) => Some(DataType::String),
            LiteralValue::Point(_) => Some(DataType::Point),
            LiteralValue::PgBox(_) => Some(DataType::PgBox),
            LiteralValue::Circle(_) => Some(DataType::Circle),
            LiteralValue::MacAddr(_) => Some(DataType::MacAddr),
            LiteralValue::MacAddr8(_) => Some(DataType::MacAddr8),
        },
        Expr::BinaryOp { left, right, .. } => {
            let left_type = infer_expr_type(left);
            let right_type = infer_expr_type(right);
            match (left_type, right_type) {
                (Some(l), Some(r)) => Some(promote_types(&l, &r)),
                (Some(t), None) | (None, Some(t)) => Some(t),
                (None, None) => None,
            }
        }
        Expr::UnaryOp { expr, .. } => infer_expr_type(expr),
        Expr::Cast { data_type, .. } => {
            use yachtsql_ir::expr::CastDataType;
            Some(match data_type {
                CastDataType::Int64 => DataType::Int64,
                CastDataType::Float64 => DataType::Float64,
                CastDataType::String => DataType::String,
                CastDataType::Bool => DataType::Bool,
                CastDataType::Date => DataType::Date,
                CastDataType::Time => DataType::Time,
                CastDataType::Timestamp => DataType::Timestamp,
                CastDataType::TimestampTz => DataType::TimestampTz,
                CastDataType::Interval => DataType::Interval,
                CastDataType::Json => DataType::Json,
                CastDataType::Uuid => DataType::Uuid,
                CastDataType::Bytes => DataType::Bytes,
                CastDataType::Numeric(_) => DataType::Numeric(None),
                _ => DataType::String,
            })
        }
        _ => None,
    }
}

fn promote_types(left: &DataType, right: &DataType) -> DataType {
    if left == right {
        return left.clone();
    }

    match (left, right) {
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
            DataType::Float64
        }
        (DataType::Int64, DataType::Numeric(_)) | (DataType::Numeric(_), DataType::Int64) => {
            DataType::Numeric(None)
        }
        (DataType::Float64, DataType::Numeric(_)) | (DataType::Numeric(_), DataType::Float64) => {
            DataType::Float64
        }
        _ => left.clone(),
    }
}
