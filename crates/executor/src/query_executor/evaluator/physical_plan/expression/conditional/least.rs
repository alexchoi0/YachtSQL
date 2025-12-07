use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_least(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_min_arg_count("LEAST", args, 1)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        let non_null_vals: Vec<Value> = values.into_iter().filter(|v| !v.is_null()).collect();
        if non_null_vals.is_empty() {
            return Ok(Value::null());
        }
        let mut min_val = non_null_vals[0].clone();
        for val in &non_null_vals[1..] {
            if let (Some(a), Some(b)) = (min_val.as_i64(), val.as_i64()) {
                if b < a {
                    min_val = val.clone();
                }
            } else if let (Some(a), Some(b)) = (min_val.as_f64(), val.as_f64()) {
                if b < a {
                    min_val = val.clone();
                }
            } else if let (Some(a), Some(b)) = (min_val.as_i64(), val.as_f64()) {
                if b < a as f64 {
                    min_val = val.clone();
                }
            } else if let (Some(a), Some(b)) = (min_val.as_f64(), val.as_i64()) {
                if (b as f64) < a {
                    min_val = val.clone();
                }
            } else if let (Some(a), Some(b)) = (min_val.as_str(), val.as_str()) {
                if b < a {
                    min_val = val.clone();
                }
            }
        }
        Ok(min_val)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_optimizer::expr::LiteralValue;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn create_test_batch() -> Table {
        create_single_row_batch(vec![("col1", DataType::Int64)], vec![Value::int64(42)])
    }

    #[test]
    fn test_least_integers() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Int64(5)),
            Expr::Literal(LiteralValue::Int64(12)),
            Expr::Literal(LiteralValue::Int64(3)),
        ];
        let result = ProjectionWithExprExec::eval_least(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(3));
    }

    #[test]
    fn test_least_floats() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Float64(3.14)),
            Expr::Literal(LiteralValue::Float64(2.71)),
            Expr::Literal(LiteralValue::Float64(9.81)),
        ];
        let result = ProjectionWithExprExec::eval_least(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::float64(2.71));
    }

    #[test]
    fn test_least_strings() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::String("apple".to_string())),
            Expr::Literal(LiteralValue::String("banana".to_string())),
            Expr::Literal(LiteralValue::String("cherry".to_string())),
        ];
        let result = ProjectionWithExprExec::eval_least(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("apple".to_string()));
    }

    #[test]
    fn test_least_with_nulls() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Null),
            Expr::Literal(LiteralValue::Int64(10)),
            Expr::Literal(LiteralValue::Null),
            Expr::Literal(LiteralValue::Int64(5)),
        ];
        let result = ProjectionWithExprExec::eval_least(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(5));
    }

    #[test]
    fn test_least_all_nulls() {
        let batch = create_test_batch();
        let args = vec![
            Expr::Literal(LiteralValue::Null),
            Expr::Literal(LiteralValue::Null),
        ];
        let result = ProjectionWithExprExec::eval_least(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_least_single_value() {
        let batch = create_test_batch();
        let args = vec![Expr::Literal(LiteralValue::Int64(42))];
        let result = ProjectionWithExprExec::eval_least(&args, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(42));
    }
}
