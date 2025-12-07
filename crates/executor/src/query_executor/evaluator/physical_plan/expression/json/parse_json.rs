use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_parse_json(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "PARSE_JSON requires exactly 1 argument".to_string(),
            ));
        }

        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::parse_json(&val)
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
    fn parses_json_object() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string(r#"{"name":"John","age":30}"#.into())]],
        );
        let args = vec![Expr::column("json")];
        let result =
            ProjectionWithExprExec::evaluate_parse_json(&args, &batch, 0).expect("success");
        if let Some(val) = result.as_json() {
            assert_eq!(*val, json!({"name": "John", "age": 30}))
        } else {
            panic!("Expected JSON")
        }
    }

    #[test]
    fn parses_json_array() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("[1,2,3,4,5]".into())]]);
        let args = vec![Expr::column("json")];
        let result =
            ProjectionWithExprExec::evaluate_parse_json(&args, &batch, 0).expect("success");
        if let Some(val) = result.as_json() {
            assert_eq!(*val, json!([1, 2, 3, 4, 5]))
        } else {
            panic!("Expected JSON")
        }
    }

    #[test]
    fn parses_json_string() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string(r#""hello""#.into())]]);
        let args = vec![Expr::column("json")];
        let result =
            ProjectionWithExprExec::evaluate_parse_json(&args, &batch, 0).expect("success");
        if let Some(val) = result.as_json() {
            assert_eq!(*val, json!("hello"))
        } else {
            panic!("Expected JSON")
        }
    }

    #[test]
    fn parses_json_number() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("42".into())]]);
        let args = vec![Expr::column("json")];
        let result =
            ProjectionWithExprExec::evaluate_parse_json(&args, &batch, 0).expect("success");
        if let Some(val) = result.as_json() {
            assert_eq!(*val, json!(42))
        } else {
            panic!("Expected JSON")
        }
    }

    #[test]
    fn errors_on_invalid_json() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("invalid json{".into())]]);
        let args = vec![Expr::column("json")];
        let err = ProjectionWithExprExec::evaluate_parse_json(&args, &batch, 0)
            .expect_err("invalid json");
        assert_error_contains(&err, "JSON");
    }

    #[test]
    fn propagates_null_argument() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("json")];
        let result =
            ProjectionWithExprExec::evaluate_parse_json(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::evaluate_parse_json(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "PARSE_JSON requires exactly 1 argument");
    }
}
