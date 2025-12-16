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
            "JSONB_STRIP_NULLS" => Self::evaluate_json_strip_nulls(args, batch, row_idx),
            "JSONB_CONTAINS"
            | "JSONB_CONCAT"
            | "JSONB_DELETE"
            | "JSONB_DELETE_PATH"
            | "JSONB_SET"
            | "JSONB_INSERT"
            | "JSONB_PRETTY"
            | "JSONB_PATH_EXISTS"
            | "JSONB_PATH_QUERY_FIRST" => Err(Error::unsupported_feature(format!(
                "PostgreSQL-specific function {} is not supported",
                name
            ))),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown JSONB function: {}",
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
