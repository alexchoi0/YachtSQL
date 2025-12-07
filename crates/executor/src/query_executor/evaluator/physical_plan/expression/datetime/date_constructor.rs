use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_date_constructor(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "DATE requires exactly 1 argument".to_string(),
            ));
        }
        let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
        crate::functions::datetime::date(&value)
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

    fn schema_with_timestamp() -> Schema {
        Schema::from_fields(vec![Field::nullable("ts", DataType::Timestamp)])
    }

    #[test]
    fn extracts_date_from_timestamp() {
        let ts = chrono::DateTime::parse_from_rfc3339("2024-03-15T14:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let batch = create_batch(schema_with_timestamp(), vec![vec![Value::timestamp(ts)]]);
        let args = vec![Expr::column("ts")];
        let result =
            ProjectionWithExprExec::eval_date_constructor(&args, &batch, 0).expect("success");

        let expected_date = chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        assert_eq!(result, Value::date(expected_date));
    }

    #[test]
    fn propagates_null() {
        let batch = create_batch(schema_with_timestamp(), vec![vec![Value::null()]]);
        let args = vec![Expr::column("ts")];
        let result =
            ProjectionWithExprExec::eval_date_constructor(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count_too_few() {
        let batch = create_batch(schema_with_timestamp(), vec![vec![Value::null()]]);
        let err = ProjectionWithExprExec::eval_date_constructor(&[], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "DATE");
        assert_error_contains(&err, "1 argument");
    }

    #[test]
    fn validates_argument_count_too_many() {
        let schema = Schema::from_fields(vec![
            Field::nullable("ts1", DataType::Timestamp),
            Field::nullable("ts2", DataType::Timestamp),
        ]);
        let ts = chrono::Utc::now();
        let batch = create_batch(
            schema,
            vec![vec![Value::timestamp(ts), Value::timestamp(ts)]],
        );
        let args = vec![Expr::column("ts1"), Expr::column("ts2")];
        let err = ProjectionWithExprExec::eval_date_constructor(&args, &batch, 0)
            .expect_err("too many arguments");
        assert_error_contains(&err, "DATE");
        assert_error_contains(&err, "1 argument");
    }
}
