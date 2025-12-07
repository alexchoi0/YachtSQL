use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_number(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(crate::error::Error::invalid_query(
                "TO_NUMBER requires 1 or 2 arguments".to_string(),
            ));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if args.len() == 2 {
            let format = Self::evaluate_expr(&args[1], batch, row_idx)?;
            if let Some(fmt) = format.as_str() {
                if fmt.to_uppercase() == "RN" {
                    if val.is_null() {
                        return Ok(Value::null());
                    }
                    if let Some(s) = val.as_str() {
                        return parse_roman_numeral(s).map(Value::int64);
                    }
                    return Err(crate::error::Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: val.data_type().to_string(),
                    });
                }
            }
        }

        crate::functions::scalar::eval_to_number(&val)
    }
}

fn parse_roman_numeral(s: &str) -> Result<i64> {
    let s = s.trim().to_uppercase();
    let mut total: i64 = 0;
    let mut prev: i64 = 0;

    for c in s.chars().rev() {
        let value = match c {
            'I' => 1,
            'V' => 5,
            'X' => 10,
            'L' => 50,
            'C' => 100,
            'D' => 500,
            'M' => 1000,
            _ => {
                return Err(crate::error::Error::invalid_query(format!(
                    "Invalid Roman numeral character: {}",
                    c
                )));
            }
        };

        if value < prev {
            total -= value;
        } else {
            total += value;
        }
        prev = value;
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    fn schema_with_string() -> Schema {
        Schema::from_fields(vec![Field::nullable("val", DataType::String)])
    }

    #[test]
    fn converts_valid_string_to_number() {
        let batch = create_batch(
            schema_with_string(),
            vec![vec![Value::string("123.45".to_string())]],
        );
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_to_number(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::float64(123.45));
    }

    #[test]
    fn propagates_null() {
        let batch = create_batch(schema_with_string(), vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_to_number(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(schema_with_string(), vec![vec![Value::string("1".into())]]);
        let err =
            ProjectionWithExprExec::eval_to_number(&[], &batch, 0).expect_err("missing arguments");
        assert_error_contains(&err, "TO_NUMBER");
        assert_error_contains(&err, "1 or 2 arguments");
    }

    #[test]
    fn errors_on_invalid_number_string() {
        let batch = create_batch(
            schema_with_string(),
            vec![vec![Value::string("not-a-number".to_string())]],
        );
        let args = vec![Expr::column("val")];
        let err =
            ProjectionWithExprExec::eval_to_number(&args, &batch, 0).expect_err("invalid number");
        let msg = err.to_string();
        assert!(
            msg.contains("parse") || msg.contains("invalid") || msg.contains("number"),
            "expected parse error, got: {}",
            msg
        );
    }
}
