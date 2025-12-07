use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_aead_function(
        _name: &str,
        _args: &[Expr],
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        Err(Error::unsupported_feature(
            "AEAD.* functions need migration from core.rs".to_string(),
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
            Field::nullable("additional_data", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::bytes(vec![0, 1, 2, 3]),
                Value::string("secret data".into()),
                Value::string("context".into()),
            ]],
        );
        let args = vec![
            Expr::column("keyset"),
            Expr::column("plaintext"),
            Expr::column("additional_data"),
        ];
        let err = ProjectionWithExprExec::evaluate_aead_function("ENCRYPT", &args, &batch, 0)
            .expect_err("not yet implemented");
        assert_error_contains(&err, "AEAD");
        assert_error_contains(&err, "migration");
    }
}
