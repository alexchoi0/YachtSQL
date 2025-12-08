use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_upper(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("UPPER", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.as_range().is_some() {
            return yachtsql_functions::range::range_upper(&val);
        }
        Self::apply_string_unary("UPPER", &args[0], batch, row_idx, |s| s.to_uppercase())
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
    fn converts_lowercase_to_uppercase() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_upper(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("HELLO".into()));
    }

    #[test]
    fn preserves_already_uppercase() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("HELLO".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_upper(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("HELLO".into()));
    }

    #[test]
    fn handles_mixed_case() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("HeLLo WoRLd".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_upper(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("HELLO WORLD".into()));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_upper(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::evaluate_upper(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "UPPER");
    }
}
