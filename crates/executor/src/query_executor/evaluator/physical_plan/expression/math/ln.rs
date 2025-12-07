use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_ln(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("LN", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        Self::calculate_natural_log(val)
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
    fn calculates_ln_of_e() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(std::f64::consts::E)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_ln(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 1.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn calculates_ln_of_one() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::float64(1.0)]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_ln(&args, &batch, 0).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - 0.0).abs() < 0.001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![Field::nullable("val", DataType::Float64)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("val")];
        let result = ProjectionWithExprExec::eval_ln(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::eval_ln(&[], &batch, 0).expect_err("missing argument");
        assert_error_contains(&err, "LN");
    }
}
