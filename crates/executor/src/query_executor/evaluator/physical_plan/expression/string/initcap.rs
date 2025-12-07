use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_initcap(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("INITCAP", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if val.is_null() {
            return Ok(Value::null());
        }

        if let Some(s) = val.as_str() {
            let mut result = String::new();
            let mut capitalize_next = true;

            for c in s.chars() {
                if c.is_alphabetic() {
                    if capitalize_next {
                        result.push_str(&c.to_uppercase().to_string());
                        capitalize_next = false;
                    } else {
                        result.push_str(&c.to_lowercase().to_string());
                    }
                } else {
                    result.push(c);
                    capitalize_next = true;
                }
            }
            return Ok(Value::string(result));
        }

        Err(crate::error::Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: val.data_type().to_string(),
        })
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
    fn capitalizes_first_letter_of_each_word() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello world".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_initcap(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("Hello World".into()));
    }

    #[test]
    fn lowercases_non_initial_letters() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("HELLO WORLD".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_initcap(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("Hello World".into()));
    }

    #[test]
    fn handles_mixed_case() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hElLo WoRlD".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_initcap(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("Hello World".into()));
    }

    #[test]
    fn preserves_non_alphabetic_characters() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello-world_123".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_initcap(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("Hello-World_123".into()));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_initcap(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::evaluate_initcap(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "INITCAP");
    }
}
