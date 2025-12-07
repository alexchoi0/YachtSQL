use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_deterministic_function(
        _name: &str,
        _args: &[Expr],
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        Err(Error::unsupported_feature(
            "DETERMINISTIC_* functions need migration from core.rs".to_string(),
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
        let schema = Schema::from_fields(vec![
            Field::nullable("keyset", DataType::Bytes),
            Field::nullable("plaintext", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::bytes(vec![0, 1, 2, 3]),
                Value::string("secret data".into()),
            ]],
        );
        let args = vec![Expr::column("keyset"), Expr::column("plaintext")];
        let err =
            ProjectionWithExprExec::evaluate_deterministic_function("ENCRYPT", &args, &batch, 0)
                .expect_err("not yet implemented");
        assert_error_contains(&err, "DETERMINISTIC");
        assert_error_contains(&err, "migration");
    }
}
