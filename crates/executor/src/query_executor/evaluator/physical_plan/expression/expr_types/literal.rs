use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::LiteralValue;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_literal(
        lit: &LiteralValue,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match lit {
            LiteralValue::Array(elements) => {
                let mut evaluated_elements = Vec::with_capacity(elements.len());
                for element_expr in elements {
                    let element_value = Self::evaluate_expr(element_expr, batch, row_idx)?;
                    evaluated_elements.push(element_value);
                }
                Ok(Value::array(evaluated_elements))
            }
            _ => Ok(lit.to_value()),
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_optimizer::expr::Expr;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn create_test_batch() -> Table {
        create_batch(
            vec![("col1", DataType::Int64)],
            vec![vec![Value::int64(1)], vec![Value::int64(2)]],
        )
    }

    #[test]
    fn test_evaluate_literal_int64() {
        let batch = create_test_batch();
        let lit = LiteralValue::Int64(42);
        let result = ProjectionWithExprExec::evaluate_literal(&lit, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(42));
    }

    #[test]
    fn test_evaluate_literal_string() {
        let batch = create_test_batch();
        let lit = LiteralValue::String("hello".to_string());
        let result = ProjectionWithExprExec::evaluate_literal(&lit, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("hello".to_string()));
    }

    #[test]
    fn test_evaluate_literal_bool() {
        let batch = create_test_batch();
        let lit = LiteralValue::Boolean(true);
        let result = ProjectionWithExprExec::evaluate_literal(&lit, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::bool_val(true));
    }

    #[test]
    fn test_evaluate_literal_null() {
        let batch = create_test_batch();
        let lit = LiteralValue::Null;
        let result = ProjectionWithExprExec::evaluate_literal(&lit, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_evaluate_literal_float64() {
        let batch = create_test_batch();
        let lit = LiteralValue::Float64(3.14);
        let result = ProjectionWithExprExec::evaluate_literal(&lit, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::float64(3.14));
    }

    #[test]
    fn test_evaluate_literal_array_simple() {
        let batch = create_test_batch();
        let lit = LiteralValue::Array(vec![
            Expr::Literal(LiteralValue::Int64(1)),
            Expr::Literal(LiteralValue::Int64(2)),
            Expr::Literal(LiteralValue::Int64(3)),
        ]);
        let result = ProjectionWithExprExec::evaluate_literal(&lit, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)])
        );
    }

    #[test]
    fn test_evaluate_literal_array_mixed_types() {
        let batch = create_test_batch();
        let lit = LiteralValue::Array(vec![
            Expr::Literal(LiteralValue::Int64(1)),
            Expr::Literal(LiteralValue::String("two".to_string())),
            Expr::Literal(LiteralValue::Null),
        ]);
        let result = ProjectionWithExprExec::evaluate_literal(&lit, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Value::array(vec![
                Value::int64(1),
                Value::string("two".to_string()),
                Value::null()
            ])
        );
    }

    #[test]
    fn test_evaluate_literal_empty_array() {
        let batch = create_test_batch();
        let lit = LiteralValue::Array(vec![]);
        let result = ProjectionWithExprExec::evaluate_literal(&lit, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::array(vec![]));
    }
}
