use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_make_date(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 3 {
            return Err(Error::invalid_query(
                "MAKE_DATE requires exactly 3 arguments (year, month, day)".to_string(),
            ));
        }

        let year_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let month_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let day_val = Self::evaluate_expr(&args[2], batch, row_idx)?;

        if year_val.is_null() || month_val.is_null() || day_val.is_null() {
            return Ok(Value::null());
        }

        let year = year_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: year_val.data_type().to_string(),
        })?;

        let month = month_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: month_val.data_type().to_string(),
        })?;

        let day = day_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: day_val.data_type().to_string(),
        })?;

        use chrono::NaiveDate;
        match NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
            Some(date) => Ok(Value::date(date)),
            None => Err(Error::invalid_query(format!(
                "Invalid date: year={}, month={}, day={}",
                year, month, day
            ))),
        }
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

    #[test]
    fn test_make_date_success() {
        let schema = Schema::from_fields(vec![
            Field::nullable("year", DataType::Int64),
            Field::nullable("month", DataType::Int64),
            Field::nullable("day", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::int64(2024), Value::int64(3), Value::int64(15)]],
        );
        let args = vec![
            Expr::column("year"),
            Expr::column("month"),
            Expr::column("day"),
        ];
        let result =
            ProjectionWithExprExec::eval_make_date(&args, &batch, 0).expect("should succeed");

        assert!(result.as_date().is_some(), "Expected a date value");
    }
}
