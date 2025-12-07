use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_generate_array(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 || args.len() > 3 {
            return Err(crate::error::Error::invalid_query(
                "GENERATE_ARRAY requires 2 or 3 arguments (start, end, [step])".to_string(),
            ));
        }
        let start_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let end_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let step_val = if args.len() == 3 {
            Some(Self::evaluate_expr(&args[2], batch, row_idx)?)
        } else {
            None
        };
        crate::functions::array::generate_array(&start_val, &end_val, step_val.as_ref())
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

    fn schema_two_args() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("start", DataType::Int64),
            Field::nullable("end", DataType::Int64),
        ])
    }

    fn schema_three_args() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("start", DataType::Int64),
            Field::nullable("end", DataType::Int64),
            Field::nullable("step", DataType::Int64),
        ])
    }

    #[test]
    fn generates_sequence_with_default_step() {
        let batch = create_batch(
            schema_two_args(),
            vec![vec![Value::int64(1), Value::int64(3)]],
        );

        let args = vec![Expr::column("start"), Expr::column("end")];
        let result =
            ProjectionWithExprExec::evaluate_generate_array(&args, &batch, 0).expect("success");

        assert_eq!(
            result,
            Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)])
        );
    }

    #[test]
    fn supports_explicit_step_argument() {
        let batch = create_batch(
            schema_three_args(),
            vec![vec![Value::int64(0), Value::int64(6), Value::int64(3)]],
        );

        let args = vec![
            Expr::column("start"),
            Expr::column("end"),
            Expr::column("step"),
        ];
        let result =
            ProjectionWithExprExec::evaluate_generate_array(&args, &batch, 0).expect("success");

        assert_eq!(
            result,
            Value::array(vec![Value::int64(0), Value::int64(3), Value::int64(6)])
        );
    }

    #[test]
    fn propagates_null_inputs() {
        let batch = create_batch(
            schema_three_args(),
            vec![vec![Value::null(), Value::int64(3), Value::int64(1)]],
        );

        let args = vec![
            Expr::column("start"),
            Expr::column("end"),
            Expr::column("step"),
        ];
        let result =
            ProjectionWithExprExec::evaluate_generate_array(&args, &batch, 0).expect("success");

        assert_eq!(result, Value::null());
    }

    #[test]
    fn errors_when_step_is_zero() {
        let batch = create_batch(
            schema_three_args(),
            vec![vec![Value::int64(0), Value::int64(1), Value::int64(0)]],
        );

        let args = vec![
            Expr::column("start"),
            Expr::column("end"),
            Expr::column("step"),
        ];
        let err = ProjectionWithExprExec::evaluate_generate_array(&args, &batch, 0)
            .expect_err("step validation should fail");
        assert_error_contains(&err, "step cannot be 0");
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(
            schema_two_args(),
            vec![vec![Value::int64(1), Value::int64(2)]],
        );

        let err =
            ProjectionWithExprExec::evaluate_generate_array(&[Expr::column("start")], &batch, 0)
                .expect_err("invalid arg count");
        assert_error_contains(&err, "GENERATE_ARRAY");
    }
}
