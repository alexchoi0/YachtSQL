use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_exists(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "JSON_EXISTS requires exactly 2 arguments".to_string(),
            ));
        }

        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let path_str = path_val
            .as_str()
            .ok_or_else(|| Error::invalid_query("JSON path must be a string"))?;

        yachtsql_functions::json::predicates::json_exists(&json_val, path_str)
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

    fn schema_with_json_and_path() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("json", DataType::String),
            Field::nullable("path", DataType::String),
        ])
    }

    #[test]
    fn returns_true_when_path_exists() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![
                Value::string(r#"{"name":"John","age":30}"#.into()),
                Value::string("$.name".into()),
            ]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let result =
            ProjectionWithExprExec::evaluate_json_exists(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::bool_val(true));
    }

    #[test]
    fn returns_false_when_path_does_not_exist() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![
                Value::string(r#"{"name":"John","age":30}"#.into()),
                Value::string("$.missing".into()),
            ]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let result =
            ProjectionWithExprExec::evaluate_json_exists(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::bool_val(false));
    }

    #[test]
    fn handles_nested_paths() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![
                Value::string(r#"{"person":{"name":"John"}}"#.into()),
                Value::string("$.person.name".into()),
            ]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let result =
            ProjectionWithExprExec::evaluate_json_exists(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::bool_val(true));
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string(r#"{"key":"value"}"#.into())]],
        );
        let err = ProjectionWithExprExec::evaluate_json_exists(&[Expr::column("json")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "JSON_EXISTS");
    }
}
