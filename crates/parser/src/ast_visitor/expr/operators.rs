use sqlparser::ast;
use yachtsql_core::error::{Error, Result};
use yachtsql_ir::expr::{BinaryOp, Expr, LiteralValue, UnaryOp};

use super::super::LogicalPlanBuilder;

impl LogicalPlanBuilder {
    pub(super) fn convert_any_op(
        &self,
        left: &ast::Expr,
        compare_op: &ast::BinaryOperator,
        right: &ast::Expr,
    ) -> Result<Expr> {
        let lhs_expr = self.sql_expr_to_expr(left)?;

        let rhs_expr = match right {
            ast::Expr::Subquery(query) => {
                let subquery_plan = self.query_to_plan(query)?;
                Expr::Subquery {
                    plan: Box::new(subquery_plan.root().clone()),
                }
            }
            ast::Expr::Tuple(elements) => {
                let array_elements = elements
                    .iter()
                    .map(|e| self.sql_expr_to_expr(e))
                    .collect::<Result<Vec<_>>>()?;
                Expr::Literal(LiteralValue::Array(array_elements))
            }
            _ => self.sql_expr_to_expr(right)?,
        };

        let op = self.sql_binary_op_to_op(compare_op)?;

        Ok(Expr::AnyOp {
            left: Box::new(lhs_expr),
            compare_op: op,
            right: Box::new(rhs_expr),
        })
    }

    pub(super) fn convert_all_op(
        &self,
        left: &ast::Expr,
        compare_op: &ast::BinaryOperator,
        right: &ast::Expr,
    ) -> Result<Expr> {
        let lhs_expr = self.sql_expr_to_expr(left)?;

        let rhs_expr = match right {
            ast::Expr::Subquery(query) => {
                let subquery_plan = self.query_to_plan(query)?;
                Expr::Subquery {
                    plan: Box::new(subquery_plan.root().clone()),
                }
            }
            ast::Expr::Tuple(elements) => {
                let array_elements = elements
                    .iter()
                    .map(|e| self.sql_expr_to_expr(e))
                    .collect::<Result<Vec<_>>>()?;
                Expr::Literal(LiteralValue::Array(array_elements))
            }
            _ => self.sql_expr_to_expr(right)?,
        };

        let op = self.sql_binary_op_to_op(compare_op)?;

        Ok(Expr::AllOp {
            left: Box::new(lhs_expr),
            compare_op: op,
            right: Box::new(rhs_expr),
        })
    }

    pub(super) fn sql_binary_op_to_op(&self, op: &ast::BinaryOperator) -> Result<BinaryOp> {
        match op {
            ast::BinaryOperator::Plus => Ok(BinaryOp::Add),
            ast::BinaryOperator::Minus => Ok(BinaryOp::Subtract),
            ast::BinaryOperator::Multiply => Ok(BinaryOp::Multiply),
            ast::BinaryOperator::Divide => Ok(BinaryOp::Divide),
            ast::BinaryOperator::Modulo => Ok(BinaryOp::Modulo),
            ast::BinaryOperator::Eq => Ok(BinaryOp::Equal),
            ast::BinaryOperator::NotEq => Ok(BinaryOp::NotEqual),
            ast::BinaryOperator::Lt => Ok(BinaryOp::LessThan),
            ast::BinaryOperator::LtEq => Ok(BinaryOp::LessThanOrEqual),
            ast::BinaryOperator::Gt => Ok(BinaryOp::GreaterThan),
            ast::BinaryOperator::GtEq => Ok(BinaryOp::GreaterThanOrEqual),
            ast::BinaryOperator::And => Ok(BinaryOp::And),
            ast::BinaryOperator::Or => Ok(BinaryOp::Or),
            ast::BinaryOperator::BitwiseAnd => Ok(BinaryOp::BitwiseAnd),
            ast::BinaryOperator::BitwiseOr => Ok(BinaryOp::BitwiseOr),
            ast::BinaryOperator::BitwiseXor => Ok(BinaryOp::BitwiseXor),
            ast::BinaryOperator::PGBitwiseXor => Ok(BinaryOp::BitwiseXor),
            ast::BinaryOperator::StringConcat => Ok(BinaryOp::Concat),
            ast::BinaryOperator::PGRegexMatch => Ok(BinaryOp::RegexMatch),
            ast::BinaryOperator::PGRegexNotMatch => Ok(BinaryOp::RegexNotMatch),
            ast::BinaryOperator::PGRegexIMatch => Ok(BinaryOp::RegexMatchI),
            ast::BinaryOperator::PGRegexNotIMatch => Ok(BinaryOp::RegexNotMatchI),
            ast::BinaryOperator::Spaceship => Ok(BinaryOp::VectorCosineDistance),
            ast::BinaryOperator::Custom(op_str) => match op_str.as_str() {
                "<->" => Ok(BinaryOp::VectorL2Distance),
                "<#>" => Ok(BinaryOp::VectorInnerProduct),
                "@>" => Ok(BinaryOp::ArrayContains),
                "<@" => Ok(BinaryOp::ArrayContainedBy),
                "&&" => Ok(BinaryOp::ArrayOverlap),
                "-|-" => Ok(BinaryOp::RangeAdjacent),
                "<<=" => Ok(BinaryOp::InetContainedByOrEqual),
                ">>=" => Ok(BinaryOp::InetContainsOrEqual),
                _ => Err(Error::unsupported_feature(format!(
                    "Custom binary operator not supported: {:?}",
                    op_str
                ))),
            },

            ast::BinaryOperator::PGCustomBinaryOperator(parts) => {
                let op_name = parts
                    .iter()
                    .map(|p| p.as_str())
                    .collect::<Vec<_>>()
                    .join("");
                match op_name.as_str() {
                    "-|-" => Ok(BinaryOp::RangeAdjacent),
                    _ => Err(Error::unsupported_feature(format!(
                        "PG custom binary operator not supported: {:?}",
                        op_name
                    ))),
                }
            }

            ast::BinaryOperator::PGOverlap => Ok(BinaryOp::ArrayOverlap),
            ast::BinaryOperator::ArrowAt => Ok(BinaryOp::ArrayContainedBy),
            ast::BinaryOperator::LtDashGt => Ok(BinaryOp::VectorL2Distance),
            ast::BinaryOperator::HashMinus => Ok(BinaryOp::HashMinus),
            ast::BinaryOperator::PGBitwiseShiftLeft => Ok(BinaryOp::ShiftLeft),
            ast::BinaryOperator::PGBitwiseShiftRight => Ok(BinaryOp::ShiftRight),
            _ => Err(Error::unsupported_feature(format!(
                "Binary operator not supported: {:?}",
                op
            ))),
        }
    }

    pub(super) fn sql_unary_op_to_op(&self, op: &ast::UnaryOperator) -> Result<UnaryOp> {
        match op {
            ast::UnaryOperator::Not => Ok(UnaryOp::Not),
            ast::UnaryOperator::Minus => Ok(UnaryOp::Negate),
            ast::UnaryOperator::Plus => Ok(UnaryOp::Plus),
            ast::UnaryOperator::PGBitwiseNot => Ok(UnaryOp::BitwiseNot),
            _ => Err(Error::unsupported_feature(format!(
                "Unary operator not supported: {:?}",
                op
            ))),
        }
    }
}
