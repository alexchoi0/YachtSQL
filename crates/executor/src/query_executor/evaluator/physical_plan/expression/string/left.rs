use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_left(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::apply_string_int_binary("LEFT", args, batch, row_idx, |s, n| {
            if n <= 0 {
                String::new()
            } else {
                let chars: Vec<char> = s.chars().collect();
                let take = (n as usize).min(chars.len());
                chars[..take].iter().collect()
            }
        })
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
    fn returns_leftmost_characters() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello world".into()), Value::int64(5)]],
        );
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_left(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hello".into()));
    }

    #[test]
    fn returns_entire_string_when_n_exceeds_length() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello".into()), Value::int64(100)]],
        );
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_left(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hello".into()));
    }

    #[test]
    fn returns_empty_string_for_zero_or_negative() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello".into()), Value::int64(0)]],
        );
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_left(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("".into()));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::null(), Value::int64(5)]]);
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_left(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let err = ProjectionWithExprExec::evaluate_left(&[Expr::column("str")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "LEFT");
    }
}
