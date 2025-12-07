use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_date_diff(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 || args.len() > 3 {
            return Err(Error::invalid_query(
                "DATE_DIFF requires 2 or 3 arguments (date1, date2, [unit])".to_string(),
            ));
        }

        let date1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let date2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        let unit_str = if args.len() == 3 {
            let unit_value = match &args[2] {
                Expr::Column { name, .. } => {
                    if batch.schema().field(name).is_some() {
                        Self::evaluate_expr(&args[2], batch, row_idx)?
                    } else {
                        Value::string(name.clone())
                    }
                }
                _ => Self::evaluate_expr(&args[2], batch, row_idx)?,
            };

            if unit_value.is_null() {
                return Ok(Value::null());
            }

            match unit_value.as_str() {
                Some(s) => s.to_uppercase(),
                None => {
                    return Err(Error::TypeMismatch {
                        expected: "STRING or identifier (unit)".to_string(),
                        actual: unit_value.data_type().to_string(),
                    });
                }
            }
        } else {
            "DAY".to_string()
        };

        if date1_val.is_null() || date2_val.is_null() {
            return Ok(Value::null());
        }

        match (date1_val.as_date(), date2_val.as_date()) {
            (Some(d1), Some(d2)) => {
                let diff =
                    crate::query_executor::execution::calculate_date_diff(d1, d2, &unit_str)?;
                Ok(Value::int64(diff))
            }
            _ => Err(Error::TypeMismatch {
                expected: "DATE, DATE".to_string(),
                actual: format!("{}, {}", date1_val.data_type(), date2_val.data_type()),
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

    fn schema_with_dates_and_string() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("date1", DataType::Date),
            Field::nullable("date2", DataType::Date),
            Field::nullable("unit", DataType::String),
        ])
    }

    #[test]
    fn calculates_day_difference() {
        let date1 = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let date2 = chrono::NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        let batch = create_batch(
            schema_with_dates_and_string(),
            vec![vec![
                Value::date(date1),
                Value::date(date2),
                Value::string("DAY".into()),
            ]],
        );
        let args = vec![
            Expr::column("date1"),
            Expr::column("date2"),
            Expr::column("unit"),
        ];
        let result = ProjectionWithExprExec::eval_date_diff(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(5));
    }

    #[test]
    fn propagates_null_first_date() {
        let date2 = chrono::NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        let batch = create_batch(
            schema_with_dates_and_string(),
            vec![vec![
                Value::null(),
                Value::date(date2),
                Value::string("DAY".into()),
            ]],
        );
        let args = vec![
            Expr::column("date1"),
            Expr::column("date2"),
            Expr::column("unit"),
        ];
        let result = ProjectionWithExprExec::eval_date_diff(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_second_date() {
        let date1 = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let batch = create_batch(
            schema_with_dates_and_string(),
            vec![vec![
                Value::date(date1),
                Value::null(),
                Value::string("DAY".into()),
            ]],
        );
        let args = vec![
            Expr::column("date1"),
            Expr::column("date2"),
            Expr::column("unit"),
        ];
        let result = ProjectionWithExprExec::eval_date_diff(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(
            Schema::from_fields(vec![Field::nullable("date1", DataType::Date)]),
            vec![vec![Value::date(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            )]],
        );
        let err = ProjectionWithExprExec::eval_date_diff(&[Expr::column("date1")], &batch, 0)
            .expect_err("missing arguments");
        assert_error_contains(&err, "DATE_DIFF");
        assert_error_contains(&err, "2 or 3 arguments");
    }

    #[test]
    fn errors_on_non_date_arguments() {
        let schema = Schema::from_fields(vec![
            Field::nullable("not_date", DataType::String),
            Field::nullable("date2", DataType::Date),
            Field::nullable("unit", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("2024-01-15".into()),
                Value::date(chrono::NaiveDate::from_ymd_opt(2024, 1, 10).unwrap()),
                Value::string("DAY".into()),
            ]],
        );
        let args = vec![
            Expr::column("not_date"),
            Expr::column("date2"),
            Expr::column("unit"),
        ];
        let err =
            ProjectionWithExprExec::eval_date_diff(&args, &batch, 0).expect_err("type mismatch");
        assert_error_contains(&err, "DATE");
    }
}
