use yachtsql_core::error::Result;

use crate::optimizer::expr::{BinaryOp, Expr, LiteralValue, UnaryOp};
use crate::optimizer::plan::{LogicalPlan, PlanNode};
use crate::optimizer::rule::OptimizationRule;

pub struct ConstantFolding;

impl ConstantFolding {
    pub fn new() -> Self {
        Self
    }

    fn try_fold_zero_arg_constant(func_name: &str) -> Option<Expr> {
        match func_name {
            "PI" => Some(Expr::Literal(LiteralValue::Float64(std::f64::consts::PI))),
            _ => None,
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn fold_expr(&self, expr: &Expr) -> Expr {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let folded_left = self.fold_expr(left);
                let folded_right = self.fold_expr(right);

                if let (Expr::Literal(left_val), Expr::Literal(right_val)) =
                    (&folded_left, &folded_right)
                    && let Some(result) = self.evaluate_binary_op(left_val, *op, right_val)
                {
                    return Expr::Literal(result);
                }

                Expr::BinaryOp {
                    left: Box::new(folded_left),
                    op: *op,
                    right: Box::new(folded_right),
                }
            }

            Expr::UnaryOp { op, expr: inner } => {
                let folded_inner = self.fold_expr(inner);

                if let Expr::Literal(val) = &folded_inner
                    && let Some(result) = self.evaluate_unary_op(*op, val)
                {
                    return Expr::Literal(result);
                }

                Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(folded_inner),
                }
            }

            Expr::Function { name, args } => {
                let folded_args: Vec<_> = args.iter().map(|arg| self.fold_expr(arg)).collect();

                if folded_args.is_empty()
                    && let Some(constant_expr) = Self::try_fold_zero_arg_constant(name.as_str())
                {
                    return constant_expr;
                }

                Expr::Function {
                    name: name.clone(),
                    args: folded_args,
                }
            }

            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => Expr::Case {
                operand: operand.as_ref().map(|o| Box::new(self.fold_expr(o))),
                when_then: when_then
                    .iter()
                    .map(|(w, t)| (self.fold_expr(w), self.fold_expr(t)))
                    .collect(),
                else_expr: else_expr.as_ref().map(|e| Box::new(self.fold_expr(e))),
            },

            _ => expr.clone(),
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &LiteralValue,
        op: BinaryOp,
        right: &LiteralValue,
    ) -> Option<LiteralValue> {
        match (left, op, right) {
            (LiteralValue::Int64(l), BinaryOp::Add, LiteralValue::Int64(r)) => {
                Some(LiteralValue::Int64(l.wrapping_add(*r)))
            }
            (LiteralValue::Int64(l), BinaryOp::Subtract, LiteralValue::Int64(r)) => {
                Some(LiteralValue::Int64(l.wrapping_sub(*r)))
            }
            (LiteralValue::Int64(l), BinaryOp::Multiply, LiteralValue::Int64(r)) => {
                Some(LiteralValue::Int64(l.wrapping_mul(*r)))
            }
            (LiteralValue::Int64(l), BinaryOp::Divide, LiteralValue::Int64(r)) if *r != 0 => {
                Some(LiteralValue::Int64(l / r))
            }

            (LiteralValue::Float64(l), BinaryOp::Add, LiteralValue::Float64(r)) => {
                Some(LiteralValue::Float64(l + r))
            }
            (LiteralValue::Float64(l), BinaryOp::Subtract, LiteralValue::Float64(r)) => {
                Some(LiteralValue::Float64(l - r))
            }
            (LiteralValue::Float64(l), BinaryOp::Multiply, LiteralValue::Float64(r)) => {
                Some(LiteralValue::Float64(l * r))
            }
            (LiteralValue::Float64(l), BinaryOp::Divide, LiteralValue::Float64(r)) if *r != 0.0 => {
                Some(LiteralValue::Float64(l / r))
            }

            (LiteralValue::Boolean(l), BinaryOp::And, LiteralValue::Boolean(r)) => {
                Some(LiteralValue::Boolean(*l && *r))
            }
            (LiteralValue::Boolean(l), BinaryOp::Or, LiteralValue::Boolean(r)) => {
                Some(LiteralValue::Boolean(*l || *r))
            }

            (LiteralValue::Int64(l), BinaryOp::Equal, LiteralValue::Int64(r)) => {
                Some(LiteralValue::Boolean(l == r))
            }
            (LiteralValue::Int64(l), BinaryOp::NotEqual, LiteralValue::Int64(r)) => {
                Some(LiteralValue::Boolean(l != r))
            }
            (LiteralValue::Int64(l), BinaryOp::LessThan, LiteralValue::Int64(r)) => {
                Some(LiteralValue::Boolean(l < r))
            }
            (LiteralValue::Int64(l), BinaryOp::GreaterThan, LiteralValue::Int64(r)) => {
                Some(LiteralValue::Boolean(l > r))
            }
            _ => None,
        }
    }

    fn evaluate_unary_op(&self, op: UnaryOp, val: &LiteralValue) -> Option<LiteralValue> {
        match (op, val) {
            (UnaryOp::Not, LiteralValue::Boolean(b)) => Some(LiteralValue::Boolean(!b)),
            (UnaryOp::Negate, LiteralValue::Int64(i)) => Some(LiteralValue::Int64(-i)),
            (UnaryOp::Negate, LiteralValue::Float64(f)) => Some(LiteralValue::Float64(-f)),
            (UnaryOp::IsNull, _) => Some(LiteralValue::Boolean(matches!(val, LiteralValue::Null))),
            (UnaryOp::IsNotNull, _) => {
                Some(LiteralValue::Boolean(!matches!(val, LiteralValue::Null)))
            }
            _ => None,
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn optimize_node(&self, node: &PlanNode) -> Option<PlanNode> {
        match node {
            PlanNode::Filter { predicate, input } => {
                let folded_predicate = self.fold_expr(predicate);
                let optimized_input = self.optimize_node(input);

                if folded_predicate != *predicate || optimized_input.is_some() {
                    Some(PlanNode::Filter {
                        predicate: folded_predicate,
                        input: Box::new(optimized_input.unwrap_or_else(|| input.as_ref().clone())),
                    })
                } else {
                    None
                }
            }
            PlanNode::Projection { expressions, input } => {
                let folded_expressions: Vec<_> = expressions
                    .iter()
                    .map(|(expr, alias)| (self.fold_expr(expr), alias.clone()))
                    .collect();
                let optimized_input = self.optimize_node(input);

                if folded_expressions != *expressions || optimized_input.is_some() {
                    Some(PlanNode::Projection {
                        expressions: folded_expressions,
                        input: Box::new(optimized_input.unwrap_or_else(|| input.as_ref().clone())),
                    })
                } else {
                    None
                }
            }
            PlanNode::Join {
                left,
                right,
                on,
                join_type,
            } => {
                let folded_on = self.fold_expr(on);
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if folded_on != *on || left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::Join {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        on: folded_on,
                        join_type: *join_type,
                    })
                } else {
                    None
                }
            }
            PlanNode::LateralJoin {
                left,
                right,
                on,
                join_type,
            } => {
                let folded_on = self.fold_expr(on);
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if folded_on != *on || left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::LateralJoin {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        on: folded_on,
                        join_type: *join_type,
                    })
                } else {
                    None
                }
            }
            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                grouping_metadata,
            } => {
                let folded_group_by: Vec<_> =
                    group_by.iter().map(|expr| self.fold_expr(expr)).collect();
                let folded_aggregates: Vec<_> =
                    aggregates.iter().map(|expr| self.fold_expr(expr)).collect();
                let optimized_input = self.optimize_node(input);

                if folded_group_by != *group_by
                    || folded_aggregates != *aggregates
                    || optimized_input.is_some()
                {
                    Some(PlanNode::Aggregate {
                        group_by: folded_group_by,
                        aggregates: folded_aggregates,
                        input: Box::new(optimized_input.unwrap_or_else(|| input.as_ref().clone())),
                        grouping_metadata: grouping_metadata.clone(),
                    })
                } else {
                    None
                }
            }
            PlanNode::Sort { order_by, input } => {
                let folded_expressions: Vec<_> = order_by
                    .iter()
                    .map(|order_expr| crate::optimizer::expr::OrderByExpr {
                        expr: self.fold_expr(&order_expr.expr),
                        asc: order_expr.asc,
                        nulls_first: order_expr.nulls_first,
                        collation: order_expr.collation.clone(),
                    })
                    .collect();
                let optimized_input = self.optimize_node(input);

                if folded_expressions != *order_by || optimized_input.is_some() {
                    Some(PlanNode::Sort {
                        order_by: folded_expressions,
                        input: Box::new(optimized_input.unwrap_or_else(|| input.as_ref().clone())),
                    })
                } else {
                    None
                }
            }
            PlanNode::Limit {
                limit,
                offset,
                input,
            } => self
                .optimize_node(input)
                .map(|optimized_input| PlanNode::Limit {
                    limit: *limit,
                    offset: *offset,
                    input: Box::new(optimized_input),
                }),
            PlanNode::Distinct { input } => {
                self.optimize_node(input)
                    .map(|optimized_input| PlanNode::Distinct {
                        input: Box::new(optimized_input),
                    })
            }
            PlanNode::SubqueryScan { subquery, alias } => {
                self.optimize_node(subquery)
                    .map(|optimized_subquery| PlanNode::SubqueryScan {
                        subquery: Box::new(optimized_subquery),
                        alias: alias.clone(),
                    })
            }
            PlanNode::Union { left, right, all } => {
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::Union {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        all: *all,
                    })
                } else {
                    None
                }
            }
            PlanNode::Intersect { left, right, all } => {
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::Intersect {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        all: *all,
                    })
                } else {
                    None
                }
            }
            PlanNode::Except { left, right, all } => {
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::Except {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        all: *all,
                    })
                } else {
                    None
                }
            }
            PlanNode::Cte {
                name,
                cte_plan,
                input,
                recursive,
                use_union_all,
                materialization_hint,
            } => {
                let cte_opt = self.optimize_node(cte_plan);
                let input_opt = self.optimize_node(input);

                if cte_opt.is_some() || input_opt.is_some() {
                    Some(PlanNode::Cte {
                        name: name.clone(),
                        cte_plan: Box::new(cte_opt.unwrap_or_else(|| cte_plan.as_ref().clone())),
                        input: Box::new(input_opt.unwrap_or_else(|| input.as_ref().clone())),
                        recursive: *recursive,
                        use_union_all: *use_union_all,
                        materialization_hint: materialization_hint.clone(),
                    })
                } else {
                    None
                }
            }
            PlanNode::TableSample {
                input,
                method,
                size,
                seed,
            } => self
                .optimize_node(input)
                .map(|optimized_input| PlanNode::TableSample {
                    input: Box::new(optimized_input),
                    method: method.clone(),
                    size: size.clone(),
                    seed: *seed,
                }),
            PlanNode::Pivot {
                input,
                aggregate_expr,
                aggregate_function,
                pivot_column,
                pivot_values,
                group_by_columns,
            } => {
                let folded_expr = self.fold_expr(aggregate_expr);
                let optimized_input = self.optimize_node(input);

                if folded_expr != *aggregate_expr || optimized_input.is_some() {
                    Some(PlanNode::Pivot {
                        input: Box::new(optimized_input.unwrap_or_else(|| input.as_ref().clone())),
                        aggregate_expr: folded_expr,
                        aggregate_function: aggregate_function.clone(),
                        pivot_column: pivot_column.clone(),
                        pivot_values: pivot_values.clone(),
                        group_by_columns: group_by_columns.clone(),
                    })
                } else {
                    None
                }
            }
            PlanNode::Unpivot {
                input,
                value_column,
                name_column,
                unpivot_columns,
            } => self
                .optimize_node(input)
                .map(|optimized_input| PlanNode::Unpivot {
                    input: Box::new(optimized_input),
                    value_column: value_column.clone(),
                    name_column: name_column.clone(),
                    unpivot_columns: unpivot_columns.clone(),
                }),
            PlanNode::Scan { .. }
            | PlanNode::IndexScan { .. }
            | PlanNode::Update { .. }
            | PlanNode::Delete { .. }
            | PlanNode::Truncate { .. }
            | PlanNode::Unnest { .. }
            | PlanNode::TableValuedFunction { .. }
            | PlanNode::Window { .. }
            | PlanNode::AlterTable { .. }
            | PlanNode::DistinctOn { .. }
            | PlanNode::ArrayJoin { .. } => None,
            PlanNode::EmptyRelation
            | PlanNode::Values { .. }
            | PlanNode::InsertOnConflict { .. }
            | PlanNode::Insert { .. }
            | PlanNode::Merge { .. } => None,
        }
    }
}

impl Default for ConstantFolding {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for ConstantFolding {
    fn name(&self) -> &str {
        "constant_folding"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(self.optimize_node(plan.root()).map(LogicalPlan::new))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::expr::{Expr, LiteralValue, UnaryOp};

    #[test]
    fn test_fold_integer_addition() {
        let rule = ConstantFolding::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(1))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(LiteralValue::Int64(2))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Int64(3)));
    }

    #[test]
    fn test_fold_integer_subtraction() {
        let rule = ConstantFolding::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(10))),
            op: BinaryOp::Subtract,
            right: Box::new(Expr::Literal(LiteralValue::Int64(3))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Int64(7)));
    }

    #[test]
    fn test_fold_integer_multiplication() {
        let rule = ConstantFolding::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(5))),
            op: BinaryOp::Multiply,
            right: Box::new(Expr::Literal(LiteralValue::Int64(4))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Int64(20)));
    }

    #[test]
    fn test_fold_integer_division() {
        let rule = ConstantFolding::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(20))),
            op: BinaryOp::Divide,
            right: Box::new(Expr::Literal(LiteralValue::Int64(4))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Int64(5)));
    }

    #[test]
    fn test_fold_float_arithmetic() {
        let rule = ConstantFolding::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Float64(2.5))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(LiteralValue::Float64(1.5))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Float64(4.0)));
    }

    #[test]
    fn test_fold_boolean_and() {
        let rule = ConstantFolding::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
            op: BinaryOp::And,
            right: Box::new(Expr::Literal(LiteralValue::Boolean(false))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Boolean(false)));
    }

    #[test]
    fn test_fold_boolean_or() {
        let rule = ConstantFolding::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
            op: BinaryOp::Or,
            right: Box::new(Expr::Literal(LiteralValue::Boolean(false))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Boolean(true)));
    }

    #[test]
    fn test_fold_comparison_equal() {
        let rule = ConstantFolding::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(5))),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Boolean(true)));
    }

    #[test]
    fn test_fold_comparison_less_than() {
        let rule = ConstantFolding::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(3))),
            op: BinaryOp::LessThan,
            right: Box::new(Expr::Literal(LiteralValue::Int64(10))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Boolean(true)));
    }

    #[test]
    fn test_fold_unary_not() {
        let rule = ConstantFolding::new();

        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Boolean(false)));
    }

    #[test]
    fn test_fold_unary_negate_int() {
        let rule = ConstantFolding::new();

        let expr = Expr::UnaryOp {
            op: UnaryOp::Negate,
            expr: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Int64(-5)));
    }

    #[test]
    fn test_fold_unary_is_null() {
        let rule = ConstantFolding::new();

        let expr = Expr::UnaryOp {
            op: UnaryOp::IsNull,
            expr: Box::new(Expr::Literal(LiteralValue::Null)),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Boolean(true)));
    }

    #[test]
    fn test_fold_nested_expression() {
        let rule = ConstantFolding::new();

        let inner = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(1))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(LiteralValue::Int64(2))),
        };

        let expr = Expr::BinaryOp {
            left: Box::new(inner),
            op: BinaryOp::Multiply,
            right: Box::new(Expr::Literal(LiteralValue::Int64(3))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(LiteralValue::Int64(9)));
    }

    #[test]
    fn test_no_fold_with_column_reference() {
        let rule = ConstantFolding::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::column("x")),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };

        let folded = rule.fold_expr(&expr);

        match folded {
            Expr::BinaryOp { left, op, right } => {
                assert!(matches!(left.as_ref(), Expr::Column { .. }));
                assert_eq!(op, BinaryOp::Add);
                assert_eq!(*right, Expr::Literal(LiteralValue::Int64(5)));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_fold_completely_constant_comparison() {
        let rule = ConstantFolding::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let filter = PlanNode::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Literal(LiteralValue::Int64(1))),
                    op: BinaryOp::Add,
                    right: Box::new(Expr::Literal(LiteralValue::Int64(2))),
                }),
                op: BinaryOp::Equal,
                right: Box::new(Expr::Literal(LiteralValue::Int64(3))),
            },
            input: Box::new(scan),
        };

        let plan = LogicalPlan::new(filter);
        let optimized = rule
            .optimize(&plan)
            .expect("constant folding optimization should not fail");
        let optimized_plan = optimized.expect("optimization should produce a result");

        match optimized_plan.root() {
            PlanNode::Filter { predicate, .. } => {
                assert_eq!(*predicate, Expr::Literal(LiteralValue::Boolean(true)));
            }
            _ => panic!("Expected Filter node"),
        }
    }

    #[test]
    fn test_division_by_zero_not_folded() {
        let rule = ConstantFolding::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(10))),
            op: BinaryOp::Divide,
            right: Box::new(Expr::Literal(LiteralValue::Int64(0))),
        };

        let folded = rule.fold_expr(&expr);

        assert!(matches!(folded, Expr::BinaryOp { .. }));
    }
}
