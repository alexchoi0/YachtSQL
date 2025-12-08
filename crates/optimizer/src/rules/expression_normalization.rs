use yachtsql_core::error::Result;

use crate::optimizer::expr::{BinaryOp, Expr};
use crate::optimizer::plan::{LogicalPlan, PlanNode};
use crate::optimizer::rule::OptimizationRule;

pub struct ExpressionNormalization;

impl ExpressionNormalization {
    pub fn new() -> Self {
        Self
    }

    #[allow(clippy::only_used_in_recursion)]
    fn normalize_expr(&self, expr: &Expr) -> Expr {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let normalized_left = self.normalize_expr(left);
                let normalized_right = self.normalize_expr(right);

                match op {
                    BinaryOp::Add | BinaryOp::Multiply | BinaryOp::Equal => {
                        if matches!(normalized_left, Expr::Literal(_))
                            && !matches!(normalized_right, Expr::Literal(_))
                        {
                            return Expr::BinaryOp {
                                left: Box::new(normalized_right),
                                op: *op,
                                right: Box::new(normalized_left),
                            };
                        }
                    }

                    BinaryOp::GreaterThan => {
                        if matches!(normalized_left, Expr::Literal(_))
                            && matches!(normalized_right, Expr::Column { .. })
                        {
                            return Expr::BinaryOp {
                                left: Box::new(normalized_right),
                                op: BinaryOp::LessThan,
                                right: Box::new(normalized_left),
                            };
                        }
                    }
                    BinaryOp::GreaterThanOrEqual => {
                        if matches!(normalized_left, Expr::Literal(_))
                            && matches!(normalized_right, Expr::Column { .. })
                        {
                            return Expr::BinaryOp {
                                left: Box::new(normalized_right),
                                op: BinaryOp::LessThanOrEqual,
                                right: Box::new(normalized_left),
                            };
                        }
                    }
                    BinaryOp::LessThan | BinaryOp::LessThanOrEqual => {}
                    _ => {}
                }

                if matches!(op, BinaryOp::Add | BinaryOp::Multiply)
                    && let Expr::BinaryOp {
                        left: inner_left,
                        op: inner_op,
                        right: inner_right,
                    } = &normalized_left
                    && inner_op == op
                {
                    return self.normalize_expr(&Expr::BinaryOp {
                        left: inner_left.clone(),
                        op: *op,
                        right: Box::new(Expr::BinaryOp {
                            left: inner_right.clone(),
                            op: *op,
                            right: Box::new(normalized_right),
                        }),
                    });
                }

                Expr::BinaryOp {
                    left: Box::new(normalized_left),
                    op: *op,
                    right: Box::new(normalized_right),
                }
            }
            Expr::UnaryOp { op, expr: inner } => {
                let normalized_inner = self.normalize_expr(inner);
                Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(normalized_inner),
                }
            }
            Expr::Function { name, args } => {
                let normalized_args = args.iter().map(|arg| self.normalize_expr(arg)).collect();
                Expr::Function {
                    name: name.clone(),
                    args: normalized_args,
                }
            }
            Expr::Aggregate {
                name,
                args,
                distinct,
                order_by,
                filter,
                ..
            } => {
                let normalized_args = args.iter().map(|arg| self.normalize_expr(arg)).collect();
                let normalized_order_by = order_by.as_ref().map(|orders| {
                    orders
                        .iter()
                        .map(|o| crate::optimizer::expr::OrderByExpr {
                            expr: self.normalize_expr(&o.expr),
                            asc: o.asc,
                            nulls_first: o.nulls_first,
                            collation: o.collation.clone(),
                        })
                        .collect()
                });
                let normalized_filter = filter.as_ref().map(|f| Box::new(self.normalize_expr(f)));
                Expr::Aggregate {
                    name: name.clone(),
                    args: normalized_args,
                    distinct: *distinct,
                    order_by: normalized_order_by,
                    filter: normalized_filter,
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let normalized_operand =
                    operand.as_ref().map(|op| Box::new(self.normalize_expr(op)));
                let normalized_when_then = when_then
                    .iter()
                    .map(|(when, then)| (self.normalize_expr(when), self.normalize_expr(then)))
                    .collect();
                let normalized_else = else_expr.as_ref().map(|e| Box::new(self.normalize_expr(e)));
                Expr::Case {
                    operand: normalized_operand,
                    when_then: normalized_when_then,
                    else_expr: normalized_else,
                }
            }

            _ => expr.clone(),
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn optimize_node(&self, node: &PlanNode) -> Option<PlanNode> {
        match node {
            PlanNode::Filter { predicate, input } => {
                let normalized_predicate = self.normalize_expr(predicate);
                let changed = normalized_predicate != *predicate;

                let optimized_input = self.optimize_node(input);

                if changed || optimized_input.is_some() {
                    Some(PlanNode::Filter {
                        predicate: normalized_predicate,
                        input: Box::new(optimized_input.unwrap_or_else(|| input.as_ref().clone())),
                    })
                } else {
                    None
                }
            }
            PlanNode::Projection { expressions, input } => {
                let mut changed = false;
                let normalized_expressions: Vec<_> = expressions
                    .iter()
                    .map(|(expr, alias)| {
                        let normalized = self.normalize_expr(expr);
                        if normalized != *expr {
                            changed = true;
                        }
                        (normalized, alias.clone())
                    })
                    .collect();

                let optimized_input = self.optimize_node(input);

                if changed || optimized_input.is_some() {
                    Some(PlanNode::Projection {
                        expressions: normalized_expressions,
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
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::Join {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        on: on.clone(),
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
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::LateralJoin {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        on: on.clone(),
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
                ..
            } => {
                let mut changed = false;
                let normalized_group_by: Vec<_> = group_by
                    .iter()
                    .map(|expr| {
                        let normalized = self.normalize_expr(expr);
                        if normalized != *expr {
                            changed = true;
                        }
                        normalized
                    })
                    .collect();

                let normalized_aggregates: Vec<_> = aggregates
                    .iter()
                    .map(|expr| {
                        let normalized = self.normalize_expr(expr);
                        if normalized != *expr {
                            changed = true;
                        }
                        normalized
                    })
                    .collect();

                let optimized_input = self.optimize_node(input);

                if changed || optimized_input.is_some() {
                    Some(PlanNode::Aggregate {
                        group_by: normalized_group_by,
                        aggregates: normalized_aggregates,
                        input: Box::new(optimized_input.unwrap_or_else(|| input.as_ref().clone())),
                        grouping_metadata: None,
                    })
                } else {
                    None
                }
            }
            PlanNode::Sort { order_by, input } => {
                self.optimize_node(input)
                    .map(|optimized_input| PlanNode::Sort {
                        order_by: order_by.clone(),
                        input: Box::new(optimized_input),
                    })
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
            } => self
                .optimize_node(input)
                .map(|optimized_input| PlanNode::Pivot {
                    input: Box::new(optimized_input),
                    aggregate_expr: aggregate_expr.clone(),
                    aggregate_function: aggregate_function.clone(),
                    pivot_column: pivot_column.clone(),
                    pivot_values: pivot_values.clone(),
                    group_by_columns: group_by_columns.clone(),
                }),
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
            | PlanNode::AlterTable { .. }
            | PlanNode::Unnest { .. }
            | PlanNode::TableValuedFunction { .. }
            | PlanNode::Window { .. }
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

impl Default for ExpressionNormalization {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for ExpressionNormalization {
    fn name(&self) -> &str {
        "expression_normalization"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(self.optimize_node(plan.root()).map(LogicalPlan::new))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::expr::LiteralValue;

    #[test]
    fn test_normalize_commutative_add() {
        let rule = ExpressionNormalization::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(5))),
            op: BinaryOp::Add,
            right: Box::new(Expr::column("x")),
        };

        let normalized = rule.normalize_expr(&expr);

        match normalized {
            Expr::BinaryOp { left, op, right } => {
                assert_eq!(op, BinaryOp::Add);
                assert!(matches!(left.as_ref(), Expr::Column { .. }));
                assert!(matches!(right.as_ref(), Expr::Literal(_)));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_normalize_comparison_flip() {
        let rule = ExpressionNormalization::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(5))),
            op: BinaryOp::GreaterThan,
            right: Box::new(Expr::column("x")),
        };

        let normalized = rule.normalize_expr(&expr);

        match normalized {
            Expr::BinaryOp { left, op, right } => {
                assert_eq!(op, BinaryOp::LessThan);
                assert!(matches!(left.as_ref(), Expr::Column { .. }));
                assert!(matches!(right.as_ref(), Expr::Literal(_)));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_normalize_in_filter() {
        let rule = ExpressionNormalization::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let filter = PlanNode::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Literal(LiteralValue::Int64(5))),
                op: BinaryOp::GreaterThan,
                right: Box::new(Expr::column("x")),
            },
            input: Box::new(scan),
        };

        let plan = LogicalPlan::new(filter);
        let optimized = rule
            .optimize(&plan)
            .expect("expression normalization optimization should not fail");
        let optimized_plan = optimized.expect("optimization should produce a result");

        match optimized_plan.root() {
            PlanNode::Filter { predicate, .. } => match predicate {
                Expr::BinaryOp { op, .. } => {
                    assert_eq!(*op, BinaryOp::LessThan);
                }
                _ => panic!("Expected BinaryOp"),
            },
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn test_normalize_multiply_commutative() {
        let rule = ExpressionNormalization::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Int64(3))),
            op: BinaryOp::Multiply,
            right: Box::new(Expr::column("y")),
        };

        let normalized = rule.normalize_expr(&expr);

        match normalized {
            Expr::BinaryOp { left, op, right } => {
                assert_eq!(op, BinaryOp::Multiply);
                assert!(matches!(left.as_ref(), Expr::Column { .. }));
                assert!(matches!(right.as_ref(), Expr::Literal(_)));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_no_change_for_canonical_form() {
        let rule = ExpressionNormalization::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::column("x")),
            op: BinaryOp::GreaterThan,
            right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };

        let normalized = rule.normalize_expr(&expr);

        assert_eq!(expr, normalized);
    }
}
