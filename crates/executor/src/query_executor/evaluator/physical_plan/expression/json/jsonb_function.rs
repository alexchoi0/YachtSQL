use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_jsonb_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "JSONB_ARRAY_LENGTH" => Self::evaluate_json_length(args, batch, row_idx),
            "JSONB_OBJECT_KEYS" => Self::evaluate_json_object_keys(args, batch, row_idx),
            "JSONB_TYPEOF" => Self::evaluate_json_type(args, batch, row_idx),
            "JSONB_BUILD_ARRAY" => Self::evaluate_json_array(args, batch, row_idx),
            "JSONB_BUILD_OBJECT" => Self::evaluate_json_object(args, batch, row_idx),
            "JSONB_CONTAINS" => Self::evaluate_jsonb_contains(args, batch, row_idx),
            "JSONB_CONCAT" => Self::evaluate_jsonb_concat(args, batch, row_idx),
            "JSONB_DELETE" => Self::evaluate_jsonb_delete(args, batch, row_idx),
            "JSONB_DELETE_PATH" => Self::evaluate_jsonb_delete_path(args, batch, row_idx),
            "JSONB_SET" => Self::evaluate_jsonb_set(args, batch, row_idx),
            "JSONB_INSERT" => Self::evaluate_jsonb_insert(args, batch, row_idx),
            "JSONB_PRETTY" => Self::evaluate_jsonb_pretty(args, batch, row_idx),
            "JSONB_STRIP_NULLS" => Self::evaluate_json_strip_nulls(args, batch, row_idx),
            "JSONB_PATH_EXISTS" => Self::evaluate_jsonb_path_exists(args, batch, row_idx),
            "JSONB_PATH_QUERY_FIRST" => Self::evaluate_jsonb_path_query_first(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown JSONB function: {}",
                name
            ))),
        }
    }

    fn evaluate_jsonb_contains(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONB_CONTAINS requires 2 arguments".to_string(),
            ));
        }
        let left = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let right = Self::evaluate_expr(&args[1], batch, row_idx)?;
        yachtsql_functions::json::jsonb_contains(&left, &right)
    }

    fn evaluate_jsonb_concat(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONB_CONCAT requires 2 arguments".to_string(),
            ));
        }
        let left = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let right = Self::evaluate_expr(&args[1], batch, row_idx)?;
        yachtsql_functions::json::jsonb_concat(&left, &right)
    }

    fn evaluate_jsonb_delete(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONB_DELETE requires 2 arguments".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let key = Self::evaluate_expr(&args[1], batch, row_idx)?;
        yachtsql_functions::json::jsonb_delete(&json_val, &key)
    }

    fn evaluate_jsonb_delete_path(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONB_DELETE_PATH requires 2 arguments".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path = Self::evaluate_expr(&args[1], batch, row_idx)?;
        yachtsql_functions::json::jsonb_delete_path(&json_val, &path)
    }

    fn evaluate_jsonb_set(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "JSONB_SET requires at least 3 arguments".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let new_value = Self::evaluate_expr(&args[2], batch, row_idx)?;
        let create_missing = if args.len() > 3 {
            Self::evaluate_expr(&args[3], batch, row_idx)?.as_bool()
        } else {
            None
        };
        yachtsql_functions::json::jsonb_set(&json_val, &path, &new_value, create_missing)
    }

    fn evaluate_jsonb_insert(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "JSONB_INSERT requires at least 3 arguments".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let new_value = Self::evaluate_expr(&args[2], batch, row_idx)?;
        let insert_after = if args.len() > 3 {
            Self::evaluate_expr(&args[3], batch, row_idx)?
                .as_bool()
                .unwrap_or(false)
        } else {
            false
        };
        yachtsql_functions::json::jsonb_insert(&json_val, &path, &new_value, insert_after)
    }

    fn evaluate_jsonb_pretty(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "JSONB_PRETTY requires 1 argument".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::jsonb_pretty(&json_val)
    }

    fn evaluate_jsonb_path_exists(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONB_PATH_EXISTS requires 2 arguments".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let path_str = path.as_str().unwrap_or("");
        yachtsql_functions::json::jsonb_path_exists(&json_val, path_str)
    }

    fn evaluate_jsonb_path_query_first(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "JSONB_PATH_QUERY_FIRST requires 2 arguments".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let path = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let path_str = path.as_str().unwrap_or("");
        yachtsql_functions::json::jsonb_path_query_first(&json_val, path_str)
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
    fn returns_unsupported_error_for_unknown_function() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("data".into())]]);
        let args = vec![Expr::column("val")];
        let err =
            ProjectionWithExprExec::evaluate_jsonb_function("JSONB_UNKNOWN_FUNC", &args, &batch, 0)
                .expect_err("unsupported");
        assert_error_contains(&err, "Unknown JSONB function");
    }

    #[test]
    fn jsonb_build_object_requires_even_args() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("data".into())]]);
        let args = vec![Expr::column("val")];
        let err =
            ProjectionWithExprExec::evaluate_jsonb_function("JSONB_BUILD_OBJECT", &args, &batch, 0)
                .expect_err("should fail with odd args");
        assert_error_contains(&err, "even number");
    }
}
