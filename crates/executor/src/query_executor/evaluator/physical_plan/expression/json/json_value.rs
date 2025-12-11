use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_functions::json::JsonValueEvalOptions;
use yachtsql_functions::json::extract::json_value_with_options;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_value(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if !(2..=3).contains(&args.len()) {
            return Err(Error::invalid_query(
                "JSON_VALUE requires 2 or 3 arguments".to_string(),
            ));
        }

        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let path_str = path_val
            .as_str()
            .ok_or_else(|| Error::invalid_query("JSON path must be a string"))?;

        let options = if args.len() == 3 {
            let literal = Self::evaluate_expr(&args[2], batch, row_idx)?;
            JsonValueEvalOptions::from_literal(&literal)?
        } else {
            JsonValueEvalOptions::default()
        };

        json_value_with_options(&json_val, path_str, &options)
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
    fn extracts_string_scalar() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![
                Value::string(r#"{"name":"John","age":30}"#.into()),
                Value::string("$.name".into()),
            ]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let result =
            ProjectionWithExprExec::evaluate_json_value(&args, &batch, 0).expect("success");
        if let Some(s) = result.as_str() {
            assert_eq!(s, "John")
        } else {
            panic!("Expected String")
        }
    }

    #[test]
    fn extracts_numeric_scalar() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![
                Value::string(r#"{"name":"John","age":30}"#.into()),
                Value::string("$.age".into()),
            ]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let result =
            ProjectionWithExprExec::evaluate_json_value(&args, &batch, 0).expect("success");
        if let Some(s) = result.as_str() {
            assert_eq!(s, "30")
        } else {
            panic!("Expected String")
        }
    }

    #[test]
    fn extracts_nested_scalar() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![
                Value::string(r#"{"person":{"name":"Alice"}}"#.into()),
                Value::string("$.person.name".into()),
            ]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let result =
            ProjectionWithExprExec::evaluate_json_value(&args, &batch, 0).expect("success");
        if let Some(s) = result.as_str() {
            assert_eq!(s, "Alice")
        } else {
            panic!("Expected String")
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
            ProjectionWithExprExec::evaluate_json_value(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_json_argument() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![Value::null(), Value::string("$.path".into())]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let result =
            ProjectionWithExprExec::evaluate_json_value(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_too_few_arguments() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string(r#"{"key":"value"}"#.into())]],
        );
        let err = ProjectionWithExprExec::evaluate_json_value(&[Expr::column("json")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "JSON_VALUE requires 2 or 3 arguments");
    }

    #[test]
    fn validates_too_many_arguments() {
        let schema = Schema::from_fields(vec![
            Field::nullable("json", DataType::String),
            Field::nullable("p1", DataType::String),
            Field::nullable("p2", DataType::String),
            Field::nullable("p3", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string(r#"{"key":"value"}"#.into()),
                Value::string("$.key".into()),
                Value::string("opt1".into()),
                Value::string("opt2".into()),
            ]],
        );
        let args = vec![
            Expr::column("json"),
            Expr::column("p1"),
            Expr::column("p2"),
            Expr::column("p3"),
        ];
        let err = ProjectionWithExprExec::evaluate_json_value(&args, &batch, 0)
            .expect_err("too many arguments");
        assert_error_contains(&err, "JSON_VALUE requires 2 or 3 arguments");
    }
}
