use yachtsql_core::error::Result;

use crate::optimizer::expr::{BinaryOp, Expr, LiteralValue, UnaryOp};
use crate::optimizer::plan::{LogicalPlan, PlanNode};
use crate::optimizer::rule::OptimizationRule;

pub struct BooleanSimplification;

impl BooleanSimplification {
    pub fn new() -> Self {
        Self
    }

    #[allow(clippy::only_used_in_recursion)]
    fn simplify_expr(&self, expr: &Expr) -> Expr {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let simplified_left = self.simplify_expr(left);
                let simplified_right = self.simplify_expr(right);

                match op {
                    BinaryOp::And => {
                        if matches!(simplified_left, Expr::Literal(LiteralValue::Boolean(true))) {
                            return simplified_right;
                        }

                        if matches!(simplified_right, Expr::Literal(LiteralValue::Boolean(true))) {
                            return simplified_left;
                        }

                        if matches!(simplified_left, Expr::Literal(LiteralValue::Boolean(false))) {
                            return Expr::Literal(LiteralValue::Boolean(false));
                        }

                        if matches!(
                            simplified_right,
                            Expr::Literal(LiteralValue::Boolean(false))
                        ) {
                            return Expr::Literal(LiteralValue::Boolean(false));
                        }

                        if simplified_left == simplified_right {
                            return simplified_left;
                        }
                    }

                    BinaryOp::Or => {
                        if matches!(simplified_left, Expr::Literal(LiteralValue::Boolean(true))) {
                            return Expr::Literal(LiteralValue::Boolean(true));
                        }

                        if matches!(simplified_right, Expr::Literal(LiteralValue::Boolean(true))) {
                            return Expr::Literal(LiteralValue::Boolean(true));
                        }

                        if matches!(simplified_left, Expr::Literal(LiteralValue::Boolean(false))) {
                            return simplified_right;
                        }

                        if matches!(
                            simplified_right,
                            Expr::Literal(LiteralValue::Boolean(false))
                        ) {
                            return simplified_left;
                        }

                        if simplified_left == simplified_right {
                            return simplified_left;
                        }
                    }
                    _ => {}
                }

                Expr::BinaryOp {
                    left: Box::new(simplified_left),
                    op: *op,
                    right: Box::new(simplified_right),
                }
            }
            Expr::UnaryOp { op, expr: inner } => {
                let simplified_inner = self.simplify_expr(inner);

                if op == &UnaryOp::Not {
                    if let Expr::UnaryOp {
                        op: UnaryOp::Not,
                        expr: inner_inner,
                    } = &simplified_inner
                    {
                        return inner_inner.as_ref().clone();
                    }

                    if let Expr::BinaryOp {
                        left,
                        op: BinaryOp::And,
                        right,
                    } = &simplified_inner
                    {
                        return Expr::BinaryOp {
                            left: Box::new(Expr::UnaryOp {
                                op: UnaryOp::Not,
                                expr: left.clone(),
                            }),
                            op: BinaryOp::Or,
                            right: Box::new(Expr::UnaryOp {
                                op: UnaryOp::Not,
                                expr: right.clone(),
                            }),
                        };
                    }

                    if let Expr::BinaryOp {
                        left,
                        op: BinaryOp::Or,
                        right,
                    } = &simplified_inner
                    {
                        return Expr::BinaryOp {
                            left: Box::new(Expr::UnaryOp {
                                op: UnaryOp::Not,
                                expr: left.clone(),
                            }),
                            op: BinaryOp::And,
                            right: Box::new(Expr::UnaryOp {
                                op: UnaryOp::Not,
                                expr: right.clone(),
                            }),
                        };
                    }

                    if let Expr::Literal(LiteralValue::Boolean(b)) = simplified_inner {
                        return Expr::Literal(LiteralValue::Boolean(!b));
                    }
                }

                Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(simplified_inner),
                }
            }

            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args.iter().map(|arg| self.simplify_expr(arg)).collect(),
            },

            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => Expr::Case {
                operand: operand.as_ref().map(|o| Box::new(self.simplify_expr(o))),
                when_then: when_then
                    .iter()
                    .map(|(w, t)| (self.simplify_expr(w), self.simplify_expr(t)))
                    .collect(),
                else_expr: else_expr.as_ref().map(|e| Box::new(self.simplify_expr(e))),
            },

            _ => expr.clone(),
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn optimize_node(&self, node: &PlanNode) -> Option<PlanNode> {
        match node {
            PlanNode::Filter { predicate, input } => {
                let simplified_predicate = self.simplify_expr(predicate);
                let optimized_input = self.optimize_node(input);

                if simplified_predicate != *predicate || optimized_input.is_some() {
                    Some(PlanNode::Filter {
                        predicate: simplified_predicate,
                        input: Box::new(optimized_input.unwrap_or_else(|| input.as_ref().clone())),
                    })
                } else {
                    None
                }
            }
            PlanNode::Projection { expressions, input } => {
                let simplified_expressions: Vec<_> = expressions
                    .iter()
                    .map(|(expr, alias)| (self.simplify_expr(expr), alias.clone()))
                    .collect();
                let optimized_input = self.optimize_node(input);

                if simplified_expressions != *expressions || optimized_input.is_some() {
                    Some(PlanNode::Projection {
                        expressions: simplified_expressions,
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
                let simplified_on = self.simplify_expr(on);
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if simplified_on != *on || left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::Join {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        on: simplified_on,
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
                let simplified_on = self.simplify_expr(on);
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

                if simplified_on != *on || left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::LateralJoin {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        on: simplified_on,
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
                let simplified_group_by: Vec<_> = group_by
                    .iter()
                    .map(|expr| self.simplify_expr(expr))
                    .collect();
                let simplified_aggregates: Vec<_> = aggregates
                    .iter()
                    .map(|expr| self.simplify_expr(expr))
                    .collect();
                let optimized_input = self.optimize_node(input);

                if simplified_group_by != *group_by
                    || simplified_aggregates != *aggregates
                    || optimized_input.is_some()
                {
                    Some(PlanNode::Aggregate {
                        group_by: simplified_group_by,
                        aggregates: simplified_aggregates,
                        input: Box::new(optimized_input.unwrap_or_else(|| input.as_ref().clone())),
                        grouping_metadata: None,
                    })
                } else {
                    None
                }
            }
            PlanNode::Sort { order_by, input } => {
                let simplified_expressions: Vec<_> = order_by
                    .iter()
                    .map(|order_expr| crate::optimizer::expr::OrderByExpr {
                        expr: self.simplify_expr(&order_expr.expr),
                        asc: order_expr.asc,
                        nulls_first: order_expr.nulls_first,
                        collation: order_expr.collation.clone(),
                    })
                    .collect();
                let optimized_input = self.optimize_node(input);

                if simplified_expressions != *order_by || optimized_input.is_some() {
                    Some(PlanNode::Sort {
                        order_by: simplified_expressions,
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
            | PlanNode::ArrayJoin { .. }
            | PlanNode::EmptyRelation
            | PlanNode::Values { .. }
            | PlanNode::InsertOnConflict { .. }
            | PlanNode::Insert { .. }
            | PlanNode::Merge { .. } => None,
        }
    }
}

impl Default for BooleanSimplification {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for BooleanSimplification {
    fn name(&self) -> &str {
        "boolean_simplification"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(self.optimize_node(plan.root()).map(LogicalPlan::new))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::expr::{Expr, LiteralValue};

    #[test]
    fn test_simplify_true_and_x() {
        let rule = BooleanSimplification::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
            op: BinaryOp::And,
            right: Box::new(Expr::column("x")),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::column("x"));
    }

    #[test]
    fn test_simplify_x_and_true() {
        let rule = BooleanSimplification::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::column("x")),
            op: BinaryOp::And,
            right: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::column("x"));
    }

    #[test]
    fn test_simplify_false_and_x() {
        let rule = BooleanSimplification::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Boolean(false))),
            op: BinaryOp::And,
            right: Box::new(Expr::column("x")),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Boolean(false)));
    }

    #[test]
    fn test_simplify_x_and_false() {
        let rule = BooleanSimplification::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::column("x")),
            op: BinaryOp::And,
            right: Box::new(Expr::Literal(LiteralValue::Boolean(false))),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Boolean(false)));
    }

    #[test]
    fn test_simplify_true_or_x() {
        let rule = BooleanSimplification::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
            op: BinaryOp::Or,
            right: Box::new(Expr::column("x")),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Boolean(true)));
    }

    #[test]
    fn test_simplify_false_or_x() {
        let rule = BooleanSimplification::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Boolean(false))),
            op: BinaryOp::Or,
            right: Box::new(Expr::column("x")),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::column("x"));
    }

    #[test]
    fn test_simplify_x_and_x_idempotence() {
        let rule = BooleanSimplification::new();

        let x = Expr::column("x");
        let expr = Expr::BinaryOp {
            left: Box::new(x.clone()),
            op: BinaryOp::And,
            right: Box::new(x.clone()),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, x);
    }

    #[test]
    fn test_simplify_x_or_x_idempotence() {
        let rule = BooleanSimplification::new();

        let x = Expr::column("x");
        let expr = Expr::BinaryOp {
            left: Box::new(x.clone()),
            op: BinaryOp::Or,
            right: Box::new(x.clone()),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, x);
    }

    #[test]
    fn test_simplify_double_negation() {
        let rule = BooleanSimplification::new();

        let inner = Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(Expr::column("x")),
        };

        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(inner),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::column("x"));
    }

    #[test]
    fn test_de_morgan_not_and() {
        let rule = BooleanSimplification::new();

        let and_expr = Expr::BinaryOp {
            left: Box::new(Expr::column("x")),
            op: BinaryOp::And,
            right: Box::new(Expr::column("y")),
        };

        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(and_expr),
        };

        let simplified = rule.simplify_expr(&expr);

        match simplified {
            Expr::BinaryOp { left, op, right } => {
                assert_eq!(op, BinaryOp::Or);
                assert!(matches!(
                    left.as_ref(),
                    Expr::UnaryOp {
                        op: UnaryOp::Not,
                        ..
                    }
                ));
                assert!(matches!(
                    right.as_ref(),
                    Expr::UnaryOp {
                        op: UnaryOp::Not,
                        ..
                    }
                ));
            }
            _ => panic!("Expected BinaryOp with OR"),
        }
    }

    #[test]
    fn test_de_morgan_not_or() {
        let rule = BooleanSimplification::new();

        let or_expr = Expr::BinaryOp {
            left: Box::new(Expr::column("x")),
            op: BinaryOp::Or,
            right: Box::new(Expr::column("y")),
        };

        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(or_expr),
        };

        let simplified = rule.simplify_expr(&expr);

        match simplified {
            Expr::BinaryOp { left, op, right } => {
                assert_eq!(op, BinaryOp::And);
                assert!(matches!(
                    left.as_ref(),
                    Expr::UnaryOp {
                        op: UnaryOp::Not,
                        ..
                    }
                ));
                assert!(matches!(
                    right.as_ref(),
                    Expr::UnaryOp {
                        op: UnaryOp::Not,
                        ..
                    }
                ));
            }
            _ => panic!("Expected BinaryOp with AND"),
        }
    }

    #[test]
    fn test_simplify_not_true() {
        let rule = BooleanSimplification::new();

        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Boolean(false)));
    }

    #[test]
    fn test_simplify_not_false() {
        let rule = BooleanSimplification::new();

        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Literal(LiteralValue::Boolean(false))),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Boolean(true)));
    }

    #[test]
    fn test_simplify_complex_nested_expression() {
        let rule = BooleanSimplification::new();

        let and_expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
            op: BinaryOp::And,
            right: Box::new(Expr::column("x")),
        };

        let or_expr = Expr::BinaryOp {
            left: Box::new(and_expr),
            op: BinaryOp::Or,
            right: Box::new(Expr::Literal(LiteralValue::Boolean(false))),
        };

        let simplified = rule.simplify_expr(&or_expr);
        assert_eq!(simplified, Expr::column("x"));
    }

    #[test]
    fn test_simplify_in_filter() {
        let rule = BooleanSimplification::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let filter = PlanNode::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
                op: BinaryOp::And,
                right: Box::new(Expr::column("x")),
            },
            input: Box::new(scan),
        };

        let plan = LogicalPlan::new(filter);
        let optimized = rule
            .optimize(&plan)
            .expect("boolean simplification optimization should not fail");
        let optimized_plan = optimized.expect("optimization should produce a result");

        match optimized_plan.root() {
            PlanNode::Filter { predicate, .. } => {
                assert_eq!(*predicate, Expr::column("x"));
            }
            _ => panic!("Expected Filter node"),
        }
    }
}
