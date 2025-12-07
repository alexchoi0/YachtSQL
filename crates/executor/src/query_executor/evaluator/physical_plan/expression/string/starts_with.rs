use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_starts_with(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("STARTS_WITH", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;

        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        if let (Some(s), Some(prefix)) = (values[0].as_str(), values[1].as_str()) {
            return Ok(Value::bool_val(s.starts_with(prefix)));
        }

        Err(crate::error::Error::TypeMismatch {
            expected: "STRING, STRING".to_string(),
            actual: format!("{}, {}", values[0].data_type(), values[1].data_type()),
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

    fn schema_with_two_strings() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("prefix", DataType::String),
        ])
    }

    #[test]
    fn returns_true_when_starts_with() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("hello world".into()),
                Value::string("hello".into()),
            ]],
        );
        let args = vec![Expr::column("str"), Expr::column("prefix")];
        let result =
            ProjectionWithExprExec::evaluate_starts_with(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::bool_val(true));
    }

    #[test]
    fn returns_false_when_does_not_start_with() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("hello world".into()),
                Value::string("world".into()),
            ]],
        );
        let args = vec![Expr::column("str"), Expr::column("prefix")];
        let result =
            ProjectionWithExprExec::evaluate_starts_with(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::bool_val(false));
    }

    #[test]
    fn empty_prefix_returns_true() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("hello".into()),
                Value::string("".into()),
            ]],
        );
        let args = vec![Expr::column("str"), Expr::column("prefix")];
        let result =
            ProjectionWithExprExec::evaluate_starts_with(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::bool_val(true));
    }

    #[test]
    fn propagates_null_string() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![Value::null(), Value::string("hello".into())]],
        );
        let args = vec![Expr::column("str"), Expr::column("prefix")];
        let result =
            ProjectionWithExprExec::evaluate_starts_with(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_prefix() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![Value::string("hello".into()), Value::null()]],
        );
        let args = vec![Expr::column("str"), Expr::column("prefix")];
        let result =
            ProjectionWithExprExec::evaluate_starts_with(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let err = ProjectionWithExprExec::evaluate_starts_with(&[Expr::column("str")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "STARTS_WITH");
    }
}
