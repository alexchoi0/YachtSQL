use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_repeat(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::apply_string_int_binary("REPEAT", args, batch, row_idx, |s, n| {
            if n <= 0 {
                String::new()
            } else {
                s.repeat(n as usize)
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
    fn repeats_string_n_times() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("ab".into()), Value::int64(3)]],
        );
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_repeat(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("ababab".into()));
    }

    #[test]
    fn returns_empty_for_zero_repeats() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello".into()), Value::int64(0)]],
        );
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_repeat(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("".into()));
    }

    #[test]
    fn returns_empty_for_negative_repeats() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello".into()), Value::int64(-1)]],
        );
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_repeat(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("".into()));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::null(), Value::int64(3)]]);
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_repeat(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hi".into())]]);
        let err = ProjectionWithExprExec::evaluate_repeat(&[Expr::column("str")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "REPEAT");
    }
}
