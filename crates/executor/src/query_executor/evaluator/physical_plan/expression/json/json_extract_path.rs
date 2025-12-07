use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_extract_path(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(format!(
                "{} requires exactly 2 arguments",
                name
            )));
        }

        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path_array_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let path_array_str = path_array_val
            .as_str()
            .ok_or_else(|| Error::invalid_query("JSON path array must be a string"))?;

        match name {
            "JSON_EXTRACT_PATH_ARRAY" => {
                yachtsql_functions::json::json_extract_path_array(&json_val, path_array_str)
            }
            "JSON_EXTRACT_PATH_ARRAY_TEXT" => {
                yachtsql_functions::json::json_extract_path_array_text(&json_val, path_array_str)
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unsupported JSON_EXTRACT_PATH variant: {}",
                name
            ))),
        }
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
    fn errors_on_unsupported_variant() {
        let batch = create_batch(
            schema_with_json_and_path(),
            vec![vec![
                Value::string(r#"{"items":[1,2,3]}"#.into()),
                Value::string("items".into()),
            ]],
        );
        let args = vec![Expr::column("json"), Expr::column("path")];
        let err = ProjectionWithExprExec::evaluate_json_extract_path(
            "UNSUPPORTED_VARIANT",
            &args,
            &batch,
            0,
        )
        .expect_err("unsupported");
        assert_error_contains(&err, "Unsupported JSON_EXTRACT_PATH variant");
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string(r#"{"items":[1,2,3]}"#.into())]],
        );
        let err = ProjectionWithExprExec::evaluate_json_extract_path(
            "JSON_EXTRACT_PATH_ARRAY",
            &[Expr::column("json")],
            &batch,
            0,
        )
        .expect_err("missing argument");
        assert_error_contains(&err, "JSON_EXTRACT_PATH_ARRAY");
    }
}
