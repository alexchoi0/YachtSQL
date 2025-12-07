use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_strpos(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("STRPOS", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        if let (Some(string), Some(substring)) = (values[0].as_str(), values[1].as_str()) {
            Ok(Value::int64(Self::find_substring_position(
                string, substring,
            )))
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING, STRING".to_string(),
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
            Field::nullable("str", DataType::String),
            Field::nullable("substr", DataType::String),
        ])
    }

    #[test]
    fn returns_position_at_beginning() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("hello world".into()),
                Value::string("hello".into()),
            ]],
        );
        let args = vec![Expr::column("str"), Expr::column("substr")];
        let result = ProjectionWithExprExec::evaluate_strpos(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(1));
    }

    #[test]
    fn returns_position_in_middle() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("hello world".into()),
                Value::string("world".into()),
            ]],
        );
        let args = vec![Expr::column("str"), Expr::column("substr")];
        let result = ProjectionWithExprExec::evaluate_strpos(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(7));
    }

    #[test]
    fn returns_zero_when_not_found() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("hello world".into()),
                Value::string("xyz".into()),
            ]],
        );
        let args = vec![Expr::column("str"), Expr::column("substr")];
        let result = ProjectionWithExprExec::evaluate_strpos(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(0));
    }

    #[test]
    fn propagates_null_string() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![Value::null(), Value::string("hello".into())]],
        );
        let args = vec![Expr::column("str"), Expr::column("substr")];
        let result = ProjectionWithExprExec::evaluate_strpos(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_substring() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![Value::string("hello".into()), Value::null()]],
        );
        let args = vec![Expr::column("str"), Expr::column("substr")];
        let result = ProjectionWithExprExec::evaluate_strpos(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let err = ProjectionWithExprExec::evaluate_strpos(&[Expr::column("str")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "STRPOS");
    }
}
