use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_date_add(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "DATE_ADD requires exactly 2 arguments (date, interval)".to_string(),
            ));
        }

        let date_val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        let (interval_value, interval_unit) = match &args[1] {
            Expr::Function {
                name,
                args: interval_args,
            } if matches!(name, yachtsql_ir::FunctionName::Custom(s) if s == "INTERVAL_LITERAL") => {
                if interval_args.len() != 2 {
                    return Err(Error::invalid_query(
                        "INTERVAL_LITERAL requires exactly 2 arguments".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&interval_args[0], batch, row_idx)?;
                let unit = Self::evaluate_expr(&interval_args[1], batch, row_idx)?;
                match (val.as_i64(), unit.as_str()) {
                    (Some(v), Some(u)) => (v, u.to_string()),
                    _ => {
                        return Err(Error::invalid_query(
                            "INTERVAL must have INT64 value and STRING unit".to_string(),
                        ));
                    }
                }
            }
            _ => {
                let days_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if days_val.is_null() {
                    return Ok(Value::null());
                }
                match days_val.as_i64() {
                    Some(days) => (days, "DAY".to_string()),
                    None => {
                        return Err(Error::TypeMismatch {
                            expected: "INTERVAL or INT64".to_string(),
                            actual: days_val.data_type().to_string(),
                        });
                    }
                }
            }
        };

        if date_val.is_null() {
            return Ok(Value::null());
        }

        match date_val.as_date() {
            Some(d) => {
                let new_date = crate::query_executor::execution::apply_interval_to_date(
                    d,
                    interval_value,
                    &interval_unit,
                )?;
                Ok(Value::date(new_date))
            }
            None => Err(Error::TypeMismatch {
                expected: "DATE".to_string(),
                actual: date_val.data_type().to_string(),
            }),
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

    fn schema_with_date_and_int() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("date_col", DataType::Date),
            Field::nullable("days", DataType::Int64),
        ])
    }

    #[test]
    fn adds_days_to_date() {
        let base_date = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let batch = create_batch(
            schema_with_date_and_int(),
            vec![vec![Value::date(base_date), Value::int64(10)]],
        );
        let args = vec![Expr::column("date_col"), Expr::column("days")];
        let result = ProjectionWithExprExec::eval_date_add(&args, &batch, 0).expect("success");

        let expected = chrono::NaiveDate::from_ymd_opt(2024, 1, 25).unwrap();
        assert_eq!(result, Value::date(expected));
    }

    #[test]
    fn propagates_null_date() {
        let batch = create_batch(
            schema_with_date_and_int(),
            vec![vec![Value::null(), Value::int64(5)]],
        );
        let args = vec![Expr::column("date_col"), Expr::column("days")];
        let result = ProjectionWithExprExec::eval_date_add(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_interval() {
        let base_date = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let batch = create_batch(
            schema_with_date_and_int(),
            vec![vec![Value::date(base_date), Value::null()]],
        );
        let args = vec![Expr::column("date_col"), Expr::column("days")];
        let result = ProjectionWithExprExec::eval_date_add(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(
            Schema::from_fields(vec![Field::nullable("date_col", DataType::Date)]),
            vec![vec![Value::date(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            )]],
        );
        let err = ProjectionWithExprExec::eval_date_add(&[Expr::column("date_col")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "DATE_ADD");
        assert_error_contains(&err, "2 arguments");
    }

    #[test]
    fn errors_on_non_date_first_argument() {
        let schema = Schema::from_fields(vec![
            Field::nullable("not_date", DataType::String),
            Field::nullable("days", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("2024-01-01".into()), Value::int64(5)]],
        );
        let args = vec![Expr::column("not_date"), Expr::column("days")];
        let err =
            ProjectionWithExprExec::eval_date_add(&args, &batch, 0).expect_err("type mismatch");
        assert_error_contains(&err, "DATE");
    }
}
