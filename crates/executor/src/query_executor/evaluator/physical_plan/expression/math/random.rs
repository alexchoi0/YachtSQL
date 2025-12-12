use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_random(
        name: &str,
        args: &[Expr],
    ) -> Result<Value> {
        Self::validate_zero_args(name, args)?;

        use rand::Rng;
        let mut rng = rand::thread_rng();
        Ok(Value::float64(rng.r#gen::<f64>()))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::Value;
    use yachtsql_optimizer::expr::Expr;

    use super::*;
    use crate::tests::support::assert_error_contains;

    #[test]
    fn returns_float_in_valid_range() {
        let result = ProjectionWithExprExec::eval_random("RANDOM", &[]).expect("success");
        if let Some(f) = result.as_f64() {
            assert!(f >= 0.0 && f < 1.0, "Random value should be in [0.0, 1.0)");
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn returns_different_values_on_multiple_calls() {
        let result1 = ProjectionWithExprExec::eval_random("RANDOM", &[]).expect("success");
        let result2 = ProjectionWithExprExec::eval_random("RANDOM", &[]).expect("success");

        if let (Some(f1), Some(f2)) = (result1.as_f64(), result2.as_f64()) {
            assert_ne!(f1, f2, "Random should return different values");
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn validates_no_arguments() {
        let err = ProjectionWithExprExec::eval_random("RANDOM", &[Expr::column("x")])
            .expect_err("has argument");
        assert_error_contains(&err, "RANDOM");
    }
}
