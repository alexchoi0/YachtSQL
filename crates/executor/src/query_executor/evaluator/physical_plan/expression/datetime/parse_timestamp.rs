use chrono::{NaiveDateTime, TimeZone, Utc};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_parse_timestamp(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "PARSE_TIMESTAMP requires exactly 2 arguments (format_string, timestamp_string)"
                    .to_string(),
            ));
        }

        let format_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let ts_str_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if format_val.is_null() || ts_str_val.is_null() {
            return Ok(Value::null());
        }

        let format_str = format_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: format_val.data_type().to_string(),
        })?;

        let ts_str = ts_str_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: ts_str_val.data_type().to_string(),
        })?;

        let chrono_format = bigquery_format_to_chrono(format_str);
        match NaiveDateTime::parse_from_str(ts_str, &chrono_format) {
            Ok(ndt) => Ok(Value::timestamp(Utc.from_utc_datetime(&ndt))),
            Err(_) => Err(Error::invalid_query(format!(
                "Cannot parse '{}' with format '{}'",
                ts_str, format_str
            ))),
        }
    }
}

fn bigquery_format_to_chrono(format: &str) -> String {
    format.to_string()
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn schema_with_format_and_ts_str() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("format", DataType::String),
            Field::nullable("ts_str", DataType::String),
        ])
    }

    #[test]
    fn parses_timestamp_with_standard_format() {
        let batch = create_batch(
            schema_with_format_and_ts_str(),
            vec![vec![
                Value::string("%Y-%m-%d %H:%M:%S".into()),
                Value::string("2024-06-15 14:30:00".into()),
            ]],
        );
        let args = vec![Expr::column("format"), Expr::column("ts_str")];
        let result =
            ProjectionWithExprExec::eval_parse_timestamp(&args, &batch, 0).expect("success");
        let expected = chrono::DateTime::parse_from_rfc3339("2024-06-15T14:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(result, Value::timestamp(expected));
    }

    #[test]
    fn propagates_null_format() {
        let batch = create_batch(
            schema_with_format_and_ts_str(),
            vec![vec![
                Value::null(),
                Value::string("2024-06-15 14:30:00".into()),
            ]],
        );
        let args = vec![Expr::column("format"), Expr::column("ts_str")];
        let result =
            ProjectionWithExprExec::eval_parse_timestamp(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_timestamp_str() {
        let batch = create_batch(
            schema_with_format_and_ts_str(),
            vec![vec![
                Value::string("%Y-%m-%d %H:%M:%S".into()),
                Value::null(),
            ]],
        );
        let args = vec![Expr::column("format"), Expr::column("ts_str")];
        let result =
            ProjectionWithExprExec::eval_parse_timestamp(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }
}
