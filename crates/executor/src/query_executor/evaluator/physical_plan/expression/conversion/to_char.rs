use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_to_char(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::invalid_query(
                "TO_CHAR requires 1 or 2 arguments (value, [format])".to_string(),
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        crate::functions::scalar::eval_to_char(&val)
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

    fn schema_with_int() -> Schema {
        Schema::from_fields(vec![Field::nullable("val", DataType::Int64)])
    }

    #[test]
    fn converts_integer_to_string() {
        let batch = create_batch(schema_with_int(), vec![vec![Value::int64(42)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_to_char(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("42".to_string()));
    }

    #[test]
    fn propagates_null() {
        let batch = create_batch(schema_with_int(), vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_to_char(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count_too_few() {
        let batch = create_batch(schema_with_int(), vec![vec![Value::int64(1)]]);
        let err =
            ProjectionWithExprExec::eval_to_char(&[], &batch, 0).expect_err("missing arguments");
        assert_error_contains(&err, "TO_CHAR");
    }

    #[test]
    fn validates_argument_count_too_many() {
        let schema = Schema::from_fields(vec![
            Field::nullable("val", DataType::Int64),
            Field::nullable("fmt", DataType::String),
            Field::nullable("extra", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::int64(1),
                Value::string("FM999".into()),
                Value::string("extra".into()),
            ]],
        );
        let args = vec![
            Expr::column("val"),
            Expr::column("fmt"),
            Expr::column("extra"),
        ];
        let err =
            ProjectionWithExprExec::eval_to_char(&args, &batch, 0).expect_err("too many arguments");
        assert_error_contains(&err, "TO_CHAR");
    }
}
