use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_timestamp_trunc(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "TIMESTAMP_TRUNC requires exactly 2 arguments (timestamp, precision)".to_string(),
            ));
        }
        let ts = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let precision = Self::evaluate_expr(&args[1], batch, row_idx)?;
        crate::functions::datetime::eval_timestamp_trunc(&ts, &precision)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    fn schema_with_timestamp_and_string() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("ts", DataType::Timestamp),
            Field::nullable("precision", DataType::String),
        ])
    }

    #[test]
    fn truncates_timestamp_to_year() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_timestamp_and_string(),
            vec![vec![Value::timestamp(ts), Value::string("YEAR".into())]],
        );
        let args = vec![Expr::column("ts"), Expr::column("precision")];
        let result =
            ProjectionWithExprExec::eval_timestamp_trunc(&args, &batch, 0).expect("success");

        let expected = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(result, Value::timestamp(expected));
    }

    #[test]
    fn truncates_timestamp_to_month() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_timestamp_and_string(),
            vec![vec![Value::timestamp(ts), Value::string("MONTH".into())]],
        );
        let args = vec![Expr::column("ts"), Expr::column("precision")];
        let result =
            ProjectionWithExprExec::eval_timestamp_trunc(&args, &batch, 0).expect("success");

        let expected = chrono::DateTime::parse_from_rfc3339("2024-08-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(result, Value::timestamp(expected));
    }

    #[test]
    fn truncates_timestamp_to_day() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_timestamp_and_string(),
            vec![vec![Value::timestamp(ts), Value::string("DAY".into())]],
        );
        let args = vec![Expr::column("ts"), Expr::column("precision")];
        let result =
            ProjectionWithExprExec::eval_timestamp_trunc(&args, &batch, 0).expect("success");

        let expected = chrono::DateTime::parse_from_rfc3339("2024-08-15T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(result, Value::timestamp(expected));
    }

    #[test]
    fn truncates_timestamp_to_hour() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_timestamp_and_string(),
            vec![vec![Value::timestamp(ts), Value::string("HOUR".into())]],
        );
        let args = vec![Expr::column("ts"), Expr::column("precision")];
        let result =
            ProjectionWithExprExec::eval_timestamp_trunc(&args, &batch, 0).expect("success");

        let expected = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(result, Value::timestamp(expected));
    }

    #[test]
    fn truncates_timestamp_to_minute() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_timestamp_and_string(),
            vec![vec![Value::timestamp(ts), Value::string("MINUTE".into())]],
        );
        let args = vec![Expr::column("ts"), Expr::column("precision")];
        let result =
            ProjectionWithExprExec::eval_timestamp_trunc(&args, &batch, 0).expect("success");

        let expected = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(result, Value::timestamp(expected));
    }

    #[test]
    fn truncates_timestamp_to_second() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:45.123456789Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_timestamp_and_string(),
            vec![vec![Value::timestamp(ts), Value::string("SECOND".into())]],
        );
        let args = vec![Expr::column("ts"), Expr::column("precision")];
        let result =
            ProjectionWithExprExec::eval_timestamp_trunc(&args, &batch, 0).expect("success");

        let expected = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(result, Value::timestamp(expected));
    }

    #[test]
    fn propagates_null_timestamp() {
        let batch = create_batch(
            schema_with_timestamp_and_string(),
            vec![vec![Value::null(), Value::string("HOUR".into())]],
        );
        let args = vec![Expr::column("ts"), Expr::column("precision")];
        let result =
            ProjectionWithExprExec::eval_timestamp_trunc(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_precision() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-08-15T14:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_timestamp_and_string(),
            vec![vec![Value::timestamp(ts), Value::null()]],
        );
        let args = vec![Expr::column("ts"), Expr::column("precision")];
        let result =
            ProjectionWithExprExec::eval_timestamp_trunc(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(
            Schema::from_fields(vec![Field::nullable("ts", DataType::Timestamp)]),
            vec![vec![Value::timestamp(chrono::Utc::now())]],
        );
        let err = ProjectionWithExprExec::eval_timestamp_trunc(&[Expr::column("ts")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "TIMESTAMP_TRUNC");
        assert_error_contains(&err, "2 arguments");
    }

    #[test]
    fn errors_on_invalid_type() {
        let schema = Schema::from_fields(vec![
            Field::nullable("not_ts", DataType::String),
            Field::nullable("precision", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("not-a-timestamp".into()),
                Value::string("HOUR".into()),
            ]],
        );
        let args = vec![Expr::column("not_ts"), Expr::column("precision")];
        let err =
            ProjectionWithExprExec::eval_timestamp_trunc(&args, &batch, 0).expect_err("type error");
        assert_error_contains(&err, "TIMESTAMP");
    }
}
