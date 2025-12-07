use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_timestamp_diff(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 3 {
            return Err(Error::invalid_query(
                "TIMESTAMP_DIFF requires exactly 3 arguments (timestamp1, timestamp2, unit)"
                    .to_string(),
            ));
        }
        let ts1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let ts2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let unit = Self::evaluate_expr(&args[2], batch, row_idx)?;
        crate::functions::datetime::eval_timestamp_diff(&ts1, &ts2, &unit)
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

    fn schema_with_two_timestamps_and_string() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("ts1", DataType::Timestamp),
            Field::nullable("ts2", DataType::Timestamp),
            Field::nullable("unit", DataType::String),
        ])
    }

    #[test]
    fn calculates_difference_in_seconds() {
        let ts1 = chrono::DateTime::parse_from_rfc3339("2024-03-15T10:30:45Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let ts2 = chrono::DateTime::parse_from_rfc3339("2024-03-15T10:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_two_timestamps_and_string(),
            vec![vec![
                Value::timestamp(ts1),
                Value::timestamp(ts2),
                Value::string("SECOND".into()),
            ]],
        );
        let args = vec![
            Expr::column("ts1"),
            Expr::column("ts2"),
            Expr::column("unit"),
        ];
        let result =
            ProjectionWithExprExec::eval_timestamp_diff(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(45));
    }

    #[test]
    fn calculates_difference_in_minutes() {
        let ts1 = chrono::DateTime::parse_from_rfc3339("2024-03-15T10:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let ts2 = chrono::DateTime::parse_from_rfc3339("2024-03-15T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_two_timestamps_and_string(),
            vec![vec![
                Value::timestamp(ts1),
                Value::timestamp(ts2),
                Value::string("MINUTE".into()),
            ]],
        );
        let args = vec![
            Expr::column("ts1"),
            Expr::column("ts2"),
            Expr::column("unit"),
        ];
        let result =
            ProjectionWithExprExec::eval_timestamp_diff(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(30));
    }

    #[test]
    fn calculates_difference_in_days() {
        let ts1 = chrono::DateTime::parse_from_rfc3339("2024-03-15T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let ts2 = chrono::DateTime::parse_from_rfc3339("2024-03-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_two_timestamps_and_string(),
            vec![vec![
                Value::timestamp(ts1),
                Value::timestamp(ts2),
                Value::string("DAY".into()),
            ]],
        );
        let args = vec![
            Expr::column("ts1"),
            Expr::column("ts2"),
            Expr::column("unit"),
        ];
        let result =
            ProjectionWithExprExec::eval_timestamp_diff(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(14));
    }

    #[test]
    fn returns_negative_when_first_before_second() {
        let ts1 = chrono::DateTime::parse_from_rfc3339("2024-03-15T09:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let ts2 = chrono::DateTime::parse_from_rfc3339("2024-03-15T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_two_timestamps_and_string(),
            vec![vec![
                Value::timestamp(ts1),
                Value::timestamp(ts2),
                Value::string("HOUR".into()),
            ]],
        );
        let args = vec![
            Expr::column("ts1"),
            Expr::column("ts2"),
            Expr::column("unit"),
        ];
        let result =
            ProjectionWithExprExec::eval_timestamp_diff(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(-1));
    }

    #[test]
    fn propagates_null_timestamp1() {
        let ts2 = chrono::DateTime::parse_from_rfc3339("2024-03-15T10:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_two_timestamps_and_string(),
            vec![vec![
                Value::null(),
                Value::timestamp(ts2),
                Value::string("SECOND".into()),
            ]],
        );
        let args = vec![
            Expr::column("ts1"),
            Expr::column("ts2"),
            Expr::column("unit"),
        ];
        let result =
            ProjectionWithExprExec::eval_timestamp_diff(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_timestamp2() {
        let ts1 = chrono::DateTime::parse_from_rfc3339("2024-03-15T10:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_two_timestamps_and_string(),
            vec![vec![
                Value::timestamp(ts1),
                Value::null(),
                Value::string("SECOND".into()),
            ]],
        );
        let args = vec![
            Expr::column("ts1"),
            Expr::column("ts2"),
            Expr::column("unit"),
        ];
        let result =
            ProjectionWithExprExec::eval_timestamp_diff(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_unit() {
        let ts1 = chrono::DateTime::parse_from_rfc3339("2024-03-15T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let ts2 = chrono::DateTime::parse_from_rfc3339("2024-03-15T10:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(
            schema_with_two_timestamps_and_string(),
            vec![vec![
                Value::timestamp(ts1),
                Value::timestamp(ts2),
                Value::null(),
            ]],
        );
        let args = vec![
            Expr::column("ts1"),
            Expr::column("ts2"),
            Expr::column("unit"),
        ];
        let result =
            ProjectionWithExprExec::eval_timestamp_diff(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(
            Schema::from_fields(vec![Field::nullable("ts", DataType::Timestamp)]),
            vec![vec![Value::timestamp(chrono::Utc::now())]],
        );
        let err = ProjectionWithExprExec::eval_timestamp_diff(&[Expr::column("ts")], &batch, 0)
            .expect_err("missing arguments");
        assert_error_contains(&err, "TIMESTAMP_DIFF");
        assert_error_contains(&err, "3 arguments");
    }

    #[test]
    fn errors_on_invalid_type() {
        let schema = Schema::from_fields(vec![
            Field::nullable("val1", DataType::String),
            Field::nullable("val2", DataType::String),
            Field::nullable("unit", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("not-a-timestamp".into()),
                Value::string("also-not-a-timestamp".into()),
                Value::string("SECOND".into()),
            ]],
        );
        let args = vec![
            Expr::column("val1"),
            Expr::column("val2"),
            Expr::column("unit"),
        ];
        let err =
            ProjectionWithExprExec::eval_timestamp_diff(&args, &batch, 0).expect_err("type error");
        assert_error_contains(&err, "TIMESTAMP");
    }
}
