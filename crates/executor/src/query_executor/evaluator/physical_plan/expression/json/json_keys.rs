use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_keys(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::invalid_query(format!(
                "JSON_KEYS requires 1 or 2 arguments, got {}",
                args.len()
            )));
        }

        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path = if args.len() == 2 {
            let path_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
            Some(
                path_val
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("JSON path must be a string"))?
                    .to_string(),
            )
        } else {
            None
        };

        yachtsql_functions::json::postgres::json_keys(&json_val, path)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_object_keys(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(format!(
                "JSON_OBJECT_KEYS requires exactly 1 argument, got {}",
                args.len()
            )));
        }

        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if json_val.is_null() {
            return Ok(Value::null());
        }

        let json = json_val
            .as_json()
            .ok_or_else(|| Error::type_mismatch("JSON", json_val.data_type().to_string()))?;

        match json {
            serde_json::Value::Object(map) => {
                let keys: Vec<Value> = map.keys().map(|k| Value::string(k.clone())).collect();
                Ok(Value::array(keys))
            }
            _ => Ok(Value::null()),
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

    #[test]
    fn validates_argument_count_zero() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::evaluate_json_keys(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "JSON_KEYS requires 1 or 2 arguments");
    }

    #[test]
    fn validates_argument_count_too_many() {
        let schema = Schema::from_fields(vec![
            Field::nullable("json", DataType::String),
            Field::nullable("path1", DataType::String),
            Field::nullable("path2", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string(r#"{"key":"value"}"#.into()),
                Value::string("a".into()),
                Value::string("b".into()),
            ]],
        );
        let args = vec![
            Expr::column("json"),
            Expr::column("path1"),
            Expr::column("path2"),
        ];
        let err =
            ProjectionWithExprExec::evaluate_json_keys(&args, &batch, 0).expect_err("too many");
        assert_error_contains(&err, "JSON_KEYS requires 1 or 2 arguments");
    }
}
