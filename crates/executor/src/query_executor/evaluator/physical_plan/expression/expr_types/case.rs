use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_case(
        operand: &Option<Box<Expr>>,
        when_then: &[(Expr, Expr)],
        else_expr: &Option<Box<Expr>>,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if let Some(case_operand) = operand {
            let operand_value = Self::evaluate_expr(case_operand, batch, row_idx)?;
            for (when_expr, then_expr) in when_then {
                let when_value = Self::evaluate_expr(when_expr, batch, row_idx)?;
                if Self::values_equal(&operand_value, &when_value) {
                    return Self::evaluate_expr(then_expr, batch, row_idx);
                }
            }
        } else {
            for (when_expr, then_expr) in when_then {
                let condition = Self::evaluate_expr(when_expr, batch, row_idx)?;
                if condition.as_bool() == Some(true) {
                    return Self::evaluate_expr(then_expr, batch, row_idx);
                }
            }
        }

        if let Some(else_e) = else_expr {
            Self::evaluate_expr(else_e, batch, row_idx)
        } else {
            Ok(Value::null())
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_optimizer::expr::LiteralValue;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn create_test_batch() -> Table {
        create_batch(
            vec![("col1", DataType::Int64), ("col2", DataType::String)],
            vec![
                vec![Value::int64(1), Value::string("a".to_string())],
                vec![Value::int64(2), Value::string("b".to_string())],
                vec![Value::int64(3), Value::null()],
            ],
        )
    }

    #[test]
    fn test_case_searched_simple() {
        let batch = create_test_batch();

        let when_then = vec![
            (
                Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        name: "col1".to_string(),
                        table: None,
                    }),
                    op: yachtsql_optimizer::BinaryOp::Equal,
                    right: Box::new(Expr::Literal(LiteralValue::Int64(1))),
                },
                Expr::Literal(LiteralValue::String("one".to_string())),
            ),
            (
                Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        name: "col1".to_string(),
                        table: None,
                    }),
                    op: yachtsql_optimizer::BinaryOp::Equal,
                    right: Box::new(Expr::Literal(LiteralValue::Int64(2))),
                },
                Expr::Literal(LiteralValue::String("two".to_string())),
            ),
        ];

        let else_expr = Some(Box::new(Expr::Literal(LiteralValue::String(
            "other".to_string(),
        ))));

        let result =
            ProjectionWithExprExec::evaluate_case(&None, &when_then, &else_expr, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("one".to_string()));

        let result =
            ProjectionWithExprExec::evaluate_case(&None, &when_then, &else_expr, &batch, 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("two".to_string()));

        let result =
            ProjectionWithExprExec::evaluate_case(&None, &when_then, &else_expr, &batch, 2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("other".to_string()));
    }

    #[test]
    fn test_case_searched_no_else() {
        let batch = create_test_batch();

        let when_then = vec![(
            Expr::BinaryOp {
                left: Box::new(Expr::Column {
                    name: "col1".to_string(),
                    table: None,
                }),
                op: yachtsql_optimizer::BinaryOp::Equal,
                right: Box::new(Expr::Literal(LiteralValue::Int64(1))),
            },
            Expr::Literal(LiteralValue::String("matched".to_string())),
        )];

        let result = ProjectionWithExprExec::evaluate_case(&None, &when_then, &None, &batch, 2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_case_simple() {
        let batch = create_test_batch();

        let operand = Some(Box::new(Expr::Column {
            name: "col1".to_string(),
            table: None,
        }));
        let when_then = vec![
            (
                Expr::Literal(LiteralValue::Int64(1)),
                Expr::Literal(LiteralValue::String("one".to_string())),
            ),
            (
                Expr::Literal(LiteralValue::Int64(2)),
                Expr::Literal(LiteralValue::String("two".to_string())),
            ),
        ];

        let result = ProjectionWithExprExec::evaluate_case(&operand, &when_then, &None, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("one".to_string()));

        let result = ProjectionWithExprExec::evaluate_case(&operand, &when_then, &None, &batch, 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("two".to_string()));
    }
}
