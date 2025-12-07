use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_pi(
        args: &[Expr],
    ) -> Result<Value> {
        Self::validate_zero_args("PI", args)?;
        Ok(Value::float64(std::f64::consts::PI))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_optimizer::expr::Expr;

    use super::*;
    use crate::tests::support::assert_error_contains;

    #[test]
    fn returns_pi_constant() {
        let result = ProjectionWithExprExec::eval_pi(&[]).expect("success");
        if let Some(f) = result.as_f64() {
            assert!((f - std::f64::consts::PI).abs() < 0.00001)
        } else {
            panic!("Expected Float64")
        }
    }

    #[test]
    fn validates_no_arguments() {
        let err = ProjectionWithExprExec::eval_pi(&[Expr::column("x")]).expect_err("has argument");
        assert_error_contains(&err, "PI");
    }
}
