use chrono::Datelike;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_age(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "AGE requires exactly 2 arguments (end_date, start_date)".to_string(),
            ));
        }

        let end_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let start_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if end_val.is_null() || start_val.is_null() {
            return Ok(Value::null());
        }

        let end_days = if let Some(d) = end_val.as_date() {
            d.num_days_from_ce()
        } else if let Some(ts) = end_val.as_timestamp() {
            ts.date_naive().num_days_from_ce()
        } else {
            return Err(Error::TypeMismatch {
                expected: "DATE or TIMESTAMP".to_string(),
                actual: end_val.data_type().to_string(),
            });
        };

        let start_days = if let Some(d) = start_val.as_date() {
            d.num_days_from_ce()
        } else if let Some(ts) = start_val.as_timestamp() {
            ts.date_naive().num_days_from_ce()
        } else {
            return Err(Error::TypeMismatch {
                expected: "DATE or TIMESTAMP".to_string(),
                actual: start_val.data_type().to_string(),
            });
        };

        Ok(Value::int64((end_days - start_days) as i64))
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

    fn schema_with_two_dates() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("end_date", DataType::Date),
            Field::nullable("start_date", DataType::Date),
        ])
    }

    #[test]
    fn calculates_age_between_dates() {
        let end_date = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let start_date = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let batch = create_batch(
            schema_with_two_dates(),
            vec![vec![Value::date(end_date), Value::date(start_date)]],
        );
        let args = vec![Expr::column("end_date"), Expr::column("start_date")];
        let result = ProjectionWithExprExec::eval_age(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(14));
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(
            Schema::from_fields(vec![Field::nullable("ts", DataType::Date)]),
            vec![vec![Value::date(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            )]],
        );
        let err = ProjectionWithExprExec::eval_age(&[Expr::column("ts")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "AGE");
        assert_error_contains(&err, "2 arguments");
    }

    #[test]
    fn propagates_null() {
        let end_date = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let batch = create_batch(
            schema_with_two_dates(),
            vec![vec![Value::date(end_date), Value::null()]],
        );
        let args = vec![Expr::column("end_date"), Expr::column("start_date")];
        let result = ProjectionWithExprExec::eval_age(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }
}
