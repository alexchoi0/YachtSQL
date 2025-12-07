use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_array(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let mut values = Vec::new();
        for arg in args {
            let val = Self::evaluate_expr(arg, batch, row_idx)?;
            values.push(val);
        }
        yachtsql_functions::json::builder::json_array(values)
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

    #[test]
    fn constructs_empty_array() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let args = vec![];
        let result =
            ProjectionWithExprExec::evaluate_json_array(&args, &batch, 0).expect("success");
        if let Some(v) = result.as_json() {
            assert_eq!(*v, json!([]))
        } else {
            panic!("Expected JSON value")
        }
    }

    #[test]
    fn constructs_array_with_integers() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::Int64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::int64(1), Value::int64(2)]]);
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result =
            ProjectionWithExprExec::evaluate_json_array(&args, &batch, 0).expect("success");
        if let Some(v) = result.as_json() {
            assert_eq!(*v, json!([1, 2]))
        } else {
            panic!("Expected JSON value")
        }
    }

    #[test]
    fn constructs_array_with_strings() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::String),
            Field::nullable("b", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("hello".into()),
                Value::string("world".into()),
            ]],
        );
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result =
            ProjectionWithExprExec::evaluate_json_array(&args, &batch, 0).expect("success");
        if let Some(v) = result.as_json() {
            assert_eq!(*v, json!(["hello", "world"]))
        } else {
            panic!("Expected JSON value")
        }
    }

    #[test]
    fn constructs_array_with_mixed_types() {
        let schema = Schema::from_fields(vec![
            Field::nullable("num", DataType::Int64),
            Field::nullable("str", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::int64(42), Value::string("test".into())]],
        );
        let args = vec![Expr::column("num"), Expr::column("str")];
        let result =
            ProjectionWithExprExec::evaluate_json_array(&args, &batch, 0).expect("success");
        if let Some(v) = result.as_json() {
            assert_eq!(*v, json!([42, "test"]))
        } else {
            panic!("Expected JSON value")
        }
    }

    #[test]
    fn handles_null_values() {
        let schema = Schema::from_fields(vec![
            Field::nullable("a", DataType::Int64),
            Field::nullable("b", DataType::Int64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::int64(1), Value::null()]]);
        let args = vec![Expr::column("a"), Expr::column("b")];
        let result =
            ProjectionWithExprExec::evaluate_json_array(&args, &batch, 0).expect("success");
        if let Some(v) = result.as_json() {
            assert_eq!(*v, json!([1, null]))
        } else {
            panic!("Expected JSON value")
        }
    }
}
