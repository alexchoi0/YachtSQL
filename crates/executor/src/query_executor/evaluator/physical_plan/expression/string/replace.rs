use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_replace(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("REPLACE", args, 3)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;

        if let (Some(s), Some(from), Some(to)) =
            (values[0].as_str(), values[1].as_str(), values[2].as_str())
        {
            Ok(Value::string(s.replace(from, to)))
        } else if values[0].is_null() || values[1].is_null() || values[2].is_null() {
            Ok(Value::null())
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING, STRING, STRING".to_string(),
                actual: format!(
                    "{}, {}, {}",
                    values[0].data_type(),
                    values[1].data_type(),
                    values[2].data_type()
                ),
            })
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

    fn schema_with_three_strings() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("from", DataType::String),
            Field::nullable("to", DataType::String),
        ])
    }

    #[test]
    fn replaces_all_occurrences() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello world hello".into()),
                Value::string("hello".into()),
                Value::string("hi".into()),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("from"),
            Expr::column("to"),
        ];
        let result = ProjectionWithExprExec::evaluate_replace(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hi world hi".into()));
    }

    #[test]
    fn returns_original_when_substring_not_found() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello world".into()),
                Value::string("xyz".into()),
                Value::string("abc".into()),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("from"),
            Expr::column("to"),
        ];
        let result = ProjectionWithExprExec::evaluate_replace(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hello world".into()));
    }

    #[test]
    fn handles_empty_replacement() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::string("hello world".into()),
                Value::string(" ".into()),
                Value::string("".into()),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("from"),
            Expr::column("to"),
        ];
        let result = ProjectionWithExprExec::evaluate_replace(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("helloworld".into()));
    }

    #[test]
    fn propagates_null() {
        let batch = create_batch(
            schema_with_three_strings(),
            vec![vec![
                Value::null(),
                Value::string("a".into()),
                Value::string("b".into()),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("from"),
            Expr::column("to"),
        ];
        let result = ProjectionWithExprExec::evaluate_replace(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let err = ProjectionWithExprExec::evaluate_replace(&[Expr::column("str")], &batch, 0)
            .expect_err("missing arguments");
        assert_error_contains(&err, "REPLACE");
    }
}
