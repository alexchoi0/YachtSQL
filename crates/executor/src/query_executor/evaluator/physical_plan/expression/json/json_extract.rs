use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_extract(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "JSON_EXTRACT requires exactly 2 arguments".to_string(),
            ));
        }

        let left_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let right_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if left_val.as_hstore().is_some() {
            let key = right_val
                .as_str()
                .ok_or_else(|| Error::invalid_query("hstore key must be a string"))?;
            return yachtsql_functions::hstore::hstore_get(
                &left_val,
                &Value::string(key.to_string()),
            );
        }

        let path_str = right_val
            .as_str()
            .ok_or_else(|| Error::invalid_query("JSON path must be a string"))?;

        yachtsql_functions::json::json_extract_json(&left_val, path_str)
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

    fn schema_with_json_and_path() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("json", DataType::String),
            Field::nullable("path", DataType::String),
        ])
    }

    #[test]
    fn extracts_string_value() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![
                Value::string(r#"{"name":"John","age":30}"#.into()),
                Value::string("$.name".into()),
            ]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let result =
            ProjectionWithExprExec::evaluate_json_extract(&args, &batch, 0).expect("success");
        if let Some(v) = result.as_json() {
            assert_eq!(*v, json!("John"))
        } else {
            panic!("Expected JSON")
        }
    }

    #[test]
    fn extracts_number_value() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![
                Value::string(r#"{"name":"John","age":30}"#.into()),
                Value::string("$.age".into()),
            ]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let result =
            ProjectionWithExprExec::evaluate_json_extract(&args, &batch, 0).expect("success");
        if let Some(v) = result.as_json() {
            assert_eq!(*v, json!(30))
        } else {
            panic!("Expected JSON")
        }
    }

    #[test]
    fn extracts_nested_value() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![
                Value::string(r#"{"person":{"name":"John","age":30}}"#.into()),
                Value::string("$.person.name".into()),
            ]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let result =
            ProjectionWithExprExec::evaluate_json_extract(&args, &batch, 0).expect("success");
        if let Some(v) = result.as_json() {
            assert_eq!(*v, json!("John"))
        } else {
            panic!("Expected JSON")
        }
    }

    #[test]
    fn returns_null_for_missing_path() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![
                Value::string(r#"{"name":"John"}"#.into()),
                Value::string("$.missing".into()),
            ]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let result =
            ProjectionWithExprExec::evaluate_json_extract(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string(r#"{"key":"value"}"#.into())]],
        );
        let err = ProjectionWithExprExec::evaluate_json_extract(&[Expr::column("json")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "JSON_EXTRACT");
    }
}
