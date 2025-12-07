use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_to_json(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "TO_JSON requires exactly 1 argument".to_string(),
            ));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::to_json(&val)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    #[test]
    fn converts_string_to_json() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_to_json(&args, &batch, 0).expect("success");
        if let Some(val) = result.as_json() {
            assert_eq!(*val, json!("hello"))
        } else {
            panic!("Expected JSON string")
        }
    }

    #[test]
    fn converts_integer_to_json() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Int64)]);
        let batch = create_batch(schema, vec![vec![Value::int64(42)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_to_json(&args, &batch, 0).expect("success");
        if let Some(val) = result.as_json() {
            assert_eq!(*val, json!(42))
        } else {
            panic!("Expected JSON number")
        }
    }

    #[test]
    fn converts_boolean_to_json() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Bool)]);
        let batch = create_batch(schema, vec![vec![Value::bool_val(true)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_to_json(&args, &batch, 0).expect("success");
        if let Some(val) = result.as_json() {
            assert_eq!(*val, json!(true))
        } else {
            panic!("Expected JSON bool")
        }
    }

    #[test]
    fn converts_array_to_json() {
        let schema = Schema::from_fields(vec![Field::nullable(
            "val",
            DataType::Array(Box::new(DataType::Int64)),
        )]);
        let batch = create_batch(
            schema,
            vec![vec![Value::array(vec![
                Value::int64(1),
                Value::int64(2),
                Value::int64(3),
            ])]],
        );
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_to_json(&args, &batch, 0).expect("success");
        if let Some(val) = result.as_json() {
            assert_eq!(*val, json!([1, 2, 3]))
        } else {
            panic!("Expected JSON array")
        }
    }

    #[test]
    fn converts_null_to_json() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::evaluate_to_json(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::evaluate_to_json(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "TO_JSON requires exactly 1 argument");
    }
}
