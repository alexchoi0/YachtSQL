use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_is_not_json_predicate(
        _name: &str,
        _args: &[Expr],
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        Err(Error::unsupported_feature(
            "IS_NOT_JSON_* predicates need migration from core.rs".to_string(),
        ))
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
    fn returns_unsupported_feature_error() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string(r#"{"key": "value"}"#.into())]],
        );
        let args = vec![Expr::column("json")];
        let err =
            ProjectionWithExprExec::evaluate_is_not_json_predicate("IS_NOT_JSON", &args, &batch, 0)
                .expect_err("not yet implemented");
        assert_error_contains(&err, "IS_NOT_JSON");
        assert_error_contains(&err, "migration");
    }
}
