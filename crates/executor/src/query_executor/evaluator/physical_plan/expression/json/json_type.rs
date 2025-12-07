use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_type(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "JSON_TYPE requires exactly 1 argument".to_string(),
            ));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::json_type(&val)
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
    fn returns_string_for_string_value() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string(r#""hello""#.into())]]);
        let args = vec![Expr::column("json")];
        let result = ProjectionWithExprExec::evaluate_json_type(&args, &batch, 0).expect("success");
        if let Some(s) = result.as_str() {
            assert_eq!(s.to_lowercase(), "string")
        } else {
            panic!("Expected String")
        }
    }

    #[test]
    fn returns_number_for_numeric_value() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("42".into())]]);
        let args = vec![Expr::column("json")];
        let result = ProjectionWithExprExec::evaluate_json_type(&args, &batch, 0).expect("success");
        if let Some(s) = result.as_str() {
            assert_eq!(s.to_lowercase(), "number")
        } else {
            panic!("Expected String")
        }
    }

    #[test]
    fn returns_array_for_array_value() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("[1,2,3]".into())]]);
        let args = vec![Expr::column("json")];
        let result = ProjectionWithExprExec::evaluate_json_type(&args, &batch, 0).expect("success");
        if let Some(s) = result.as_str() {
            assert_eq!(s.to_lowercase(), "array")
        } else {
            panic!("Expected String")
        }
    }

    #[test]
    fn returns_object_for_object_value() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string(r#"{"key":"value"}"#.into())]],
        );
        let args = vec![Expr::column("json")];
        let result = ProjectionWithExprExec::evaluate_json_type(&args, &batch, 0).expect("success");
        if let Some(s) = result.as_str() {
            assert_eq!(s.to_lowercase(), "object")
        } else {
            panic!("Expected String")
        }
    }

    #[test]
    fn returns_boolean_for_boolean_value() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("true".into())]]);
        let args = vec![Expr::column("json")];
        let result = ProjectionWithExprExec::evaluate_json_type(&args, &batch, 0).expect("success");
        if let Some(s) = result.as_str() {
            assert_eq!(s.to_lowercase(), "boolean")
        } else {
            panic!("Expected String")
        }
    }

    #[test]
    fn returns_null_for_null_value() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("null".into())]]);
        let args = vec![Expr::column("json")];
        let result = ProjectionWithExprExec::evaluate_json_type(&args, &batch, 0).expect("success");
        if let Some(s) = result.as_str() {
            assert_eq!(s.to_lowercase(), "null")
        } else {
            panic!("Expected String")
        }
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::evaluate_json_type(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "JSON_TYPE requires exactly 1 argument");
    }
}
