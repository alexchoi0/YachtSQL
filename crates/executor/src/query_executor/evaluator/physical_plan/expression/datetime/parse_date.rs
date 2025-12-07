use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_parse_date(
        _args: &[Expr],
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        Err(Error::unsupported_feature(
            "PARSE_DATE not yet fully implemented - requires complex date formatting logic"
                .to_string(),
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
            Field::nullable("format", DataType::String),
            Field::nullable("date_str", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("%Y-%m-%d".into()),
                Value::string("2024-03-15".into()),
            ]],
        );
        let args = vec![Expr::column("format"), Expr::column("date_str")];
        let err = ProjectionWithExprExec::eval_parse_date(&args, &batch, 0)
            .expect_err("not yet implemented");
        assert_error_contains(&err, "PARSE_DATE");
        assert_error_contains(&err, "not yet");
    }
}
