use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_parse_date(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "PARSE_DATE requires exactly 2 arguments (format_string, date_string)".to_string(),
            ));
        }

        let format_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let date_str_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if format_val.is_null() || date_str_val.is_null() {
            return Ok(Value::null());
        }

        let format_str = format_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: format_val.data_type().to_string(),
        })?;

        let date_str = date_str_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: date_str_val.data_type().to_string(),
        })?;

        let chrono_format = bigquery_format_to_chrono(format_str);
        match chrono::NaiveDate::parse_from_str(date_str, &chrono_format) {
            Ok(date) => Ok(Value::date(date)),
            Err(_) => Err(Error::invalid_query(format!(
                "Cannot parse '{}' with format '{}'",
                date_str, format_str
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

    fn schema_with_format_and_date_str() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("format", DataType::String),
            Field::nullable("date_str", DataType::String),
        ])
    }

    #[test]
    fn parses_date_with_standard_format() {
        let batch = create_batch(
            schema_with_format_and_date_str(),
            vec![vec![
                Value::string("%Y-%m-%d".into()),
                Value::string("2024-06-15".into()),
            ]],
        );
        let args = vec![Expr::column("format"), Expr::column("date_str")];
        let result = ProjectionWithExprExec::eval_parse_date(&args, &batch, 0).expect("success");
        assert_eq!(
            result,
            Value::date(chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap())
        );
    }

    #[test]
    fn propagates_null_format() {
        let batch = create_batch(
            schema_with_format_and_date_str(),
            vec![vec![Value::null(), Value::string("2024-06-15".into())]],
        );
        let args = vec![Expr::column("format"), Expr::column("date_str")];
        let result = ProjectionWithExprExec::eval_parse_date(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_date_str() {
        let batch = create_batch(
            schema_with_format_and_date_str(),
            vec![vec![Value::string("%Y-%m-%d".into()), Value::null()]],
        );
        let args = vec![Expr::column("format"), Expr::column("date_str")];
        let result = ProjectionWithExprExec::eval_parse_date(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }
}
