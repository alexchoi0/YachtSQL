use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_position(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("POSITION", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        if let (Some(substring), Some(string)) = (values[0].as_str(), values[1].as_str()) {
            Ok(Value::int64(Self::find_substring_position(
                string, substring,
            )))
        } else if let (Some(pattern), Some(haystack)) = (values[0].as_bytes(), values[1].as_bytes())
        {
            Ok(Value::int64(Self::find_bytes_position(haystack, pattern)))
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING, STRING or BYTES, BYTES".to_string(),
                actual: format!("{}, {}", values[0].data_type(), values[1].data_type()),
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

    fn schema_with_two_strings() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("substring", DataType::String),
            Field::nullable("string", DataType::String),
        ])
    }

    #[test]
    fn finds_substring_at_beginning() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("hello".into()),
                Value::string("hello world".into()),
            ]],
        );
        let args = vec![Expr::column("substring"), Expr::column("string")];
        let result = ProjectionWithExprExec::evaluate_position(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(1));
    }

    #[test]
    fn finds_substring_in_middle() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("world".into()),
                Value::string("hello world".into()),
            ]],
        );
        let args = vec![Expr::column("substring"), Expr::column("string")];
        let result = ProjectionWithExprExec::evaluate_position(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(7));
    }

    #[test]
    fn returns_zero_when_not_found() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("xyz".into()),
                Value::string("hello world".into()),
            ]],
        );
        let args = vec![Expr::column("substring"), Expr::column("string")];
        let result = ProjectionWithExprExec::evaluate_position(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(0));
    }

    #[test]
    fn propagates_null() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![Value::null(), Value::string("hello".into())]],
        );
        let args = vec![Expr::column("substring"), Expr::column("string")];
        let result = ProjectionWithExprExec::evaluate_position(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let err = ProjectionWithExprExec::evaluate_position(&[Expr::column("str")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "POSITION");
    }
}
