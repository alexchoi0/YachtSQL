use yachtsql_core::error::Result;

use crate::optimizer::expr::Expr;
#[cfg(test)]
use crate::optimizer::expr::{BinaryOp, LiteralValue};
use crate::optimizer::plan::{LogicalPlan, PlanNode};
use crate::optimizer::rule::OptimizationRule;

pub struct InListConversion;

impl InListConversion {
    pub fn new() -> Self {
        Self
    }

    #[cfg(test)]
    fn is_continuous_range(values: &[i64]) -> Option<(i64, i64)> {
        if values.len() < 3 {
            return None;
        }

        let mut sorted = values.to_vec();
        sorted.sort_unstable();

        let min = sorted[0];
        let max = sorted[sorted.len() - 1];

        if (max - min + 1) as usize == sorted.len() {
            for (i, &val) in sorted.iter().enumerate() {
                if val != min + i as i64 {
                    return None;
                }
            }
            Some((min, max))
        } else {
            None
        }
    }

    #[cfg(test)]
    fn convert_small_in_to_or(column: Expr, values: Vec<LiteralValue>) -> Option<Expr> {
        if values.is_empty() {
            return Some(Expr::Literal(LiteralValue::Boolean(false)));
        }

        if values.len() == 1 {
            return Some(Expr::BinaryOp {
                left: Box::new(column),
                op: BinaryOp::Equal,
                right: Box::new(Expr::Literal(values[0].clone())),
            });
        }

        if values.len() <= 5 {
            let mut result = Expr::BinaryOp {
                left: Box::new(column.clone()),
                op: BinaryOp::Equal,
                right: Box::new(Expr::Literal(values[0].clone())),
            };

            for value in values.iter().skip(1) {
                result = Expr::BinaryOp {
                    left: Box::new(result),
                    op: BinaryOp::Or,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(column.clone()),
                        op: BinaryOp::Equal,
                        right: Box::new(Expr::Literal(value.clone())),
                    }),
                };
            }

            return Some(result);
        }

        None
    }

    #[allow(clippy::only_used_in_recursion)]
    fn simplify_expr(&self, expr: &Expr) -> Expr {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let simplified_left = self.simplify_expr(left);
                let simplified_right = self.simplify_expr(right);

                Expr::BinaryOp {
                    left: Box::new(simplified_left),
                    op: *op,
                    right: Box::new(simplified_right),
                }
            }
            Expr::UnaryOp { op, expr: inner } => {
                let simplified_inner = self.simplify_expr(inner);
                Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(simplified_inner),
                }
            }
            Expr::Function { name, args } => {
                let simplified_args: Vec<_> =
                    args.iter().map(|arg| self.simplify_expr(arg)).collect();
                Expr::Function {
                    name: name.clone(),
                    args: simplified_args,
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
                let simplified_args: Vec<_> =
                    args.iter().map(|arg| self.simplify_expr(arg)).collect();
                let simplified_order_by = order_by.as_ref().map(|orders| {
                    orders
                        .iter()
                        .map(|o| crate::optimizer::expr::OrderByExpr {
                            expr: self.simplify_expr(&o.expr),
                            asc: o.asc,
                            nulls_first: o.nulls_first,
                            collation: o.collation.clone(),
                        })
                        .collect()
                });
                let simplified_filter = filter.as_ref().map(|f| Box::new(self.simplify_expr(f)));
                Expr::Aggregate {
                    name: name.clone(),
                    args: simplified_args,
                    distinct: *distinct,
                    order_by: simplified_order_by,
                    filter: simplified_filter,
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let simplified_operand =
                    operand.as_ref().map(|op| Box::new(self.simplify_expr(op)));
                let simplified_when_then: Vec<_> = when_then
                    .iter()
                    .map(|(when, then)| (self.simplify_expr(when), self.simplify_expr(then)))
                    .collect();
                let simplified_else = else_expr.as_ref().map(|e| Box::new(self.simplify_expr(e)));

                Expr::Case {
                    operand: simplified_operand,
                    when_then: simplified_when_then,
                    else_expr: simplified_else,
                }
            }
            _ => expr.clone(),
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn optimize_node(&self, node: &PlanNode) -> Option<PlanNode> {
        match node {
            PlanNode::Filter { predicate, input } => {
                let simplified_predicate = self.simplify_expr(predicate);
                let changed = simplified_predicate != *predicate;

                let optimized_input = self.optimize_node(input);

                if changed || optimized_input.is_some() {
                    Some(PlanNode::Filter {
                        predicate: simplified_predicate,
                        input: Box::new(optimized_input.unwrap_or_else(|| input.as_ref().clone())),
                    })
                } else {
                    None
                }
            }
            PlanNode::Projection { expressions, input } => {
                let mut changed = false;
                let simplified_expressions: Vec<_> = expressions
                    .iter()
                    .map(|(expr, alias)| {
                        let simplified = self.simplify_expr(expr);
                        if simplified != *expr {
                            changed = true;
                        }
                        (simplified, alias.clone())
                    })
                    .collect();

                let optimized_input = self.optimize_node(input);

                if changed || optimized_input.is_some() {
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

                let simplified_group_by: Vec<_> = group_by
                    .iter()
                    .map(|expr| {
                        let simplified = self.simplify_expr(expr);
                        if simplified != *expr {
                            changed = true;
                        }
                        simplified
                    })
                    .collect();

                let simplified_aggregates: Vec<_> = aggregates
                    .iter()
                    .map(|expr| {
                        let simplified = self.simplify_expr(expr);
                        if simplified != *expr {
                            changed = true;
                        }
                        simplified
                    })
                    .collect();

                let optimized_input = self.optimize_node(input);

                if changed || optimized_input.is_some() {
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
                column_aliases,
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
                        column_aliases: column_aliases.clone(),
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

impl Default for InListConversion {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for InListConversion {
    fn name(&self) -> &str {
        "in_list_conversion"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(self.optimize_node(plan.root()).map(LogicalPlan::new))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_continuous_range_detection() {
        assert_eq!(
            InListConversion::is_continuous_range(&[1, 2, 3, 4, 5]),
            Some((1, 5))
        );

        assert_eq!(
            InListConversion::is_continuous_range(&[5, 3, 1, 4, 2]),
            Some((1, 5))
        );

        assert_eq!(
            InListConversion::is_continuous_range(&[1, 3, 5, 7, 9]),
            None
        );

        assert_eq!(InListConversion::is_continuous_range(&[1, 2]), None);
    }

    #[test]
    fn test_convert_single_value_in() {
        let column = Expr::column("x");
        let values = vec![LiteralValue::Int64(1)];

        let result = InListConversion::convert_small_in_to_or(column, values);
        assert!(result.is_some());

        match result.unwrap() {
            Expr::BinaryOp { op, .. } => {
                assert_eq!(op, BinaryOp::Equal);
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_convert_empty_in() {
        let column = Expr::column("x");
        let values = vec![];

        let result = InListConversion::convert_small_in_to_or(column, values);
        assert!(result.is_some());

        assert_eq!(result.unwrap(), Expr::Literal(LiteralValue::Boolean(false)));
    }

    #[test]
    fn test_convert_small_in_to_or() {
        let column = Expr::column("x");
        let values = vec![
            LiteralValue::Int64(1),
            LiteralValue::Int64(2),
            LiteralValue::Int64(3),
        ];

        let result = InListConversion::convert_small_in_to_or(column, values);
        assert!(result.is_some());

        match result.unwrap() {
            Expr::BinaryOp {
                op: BinaryOp::Or, ..
            } => {}
            _ => panic!("Expected OR chain"),
        }
    }

    #[test]
    fn test_rule_application() {
        let rule = InListConversion::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
            only: false,
        };

        let filter = PlanNode::Filter {
            predicate: Expr::column("x"),
            input: Box::new(scan),
        };

        let plan = LogicalPlan::new(filter);
        let optimized = rule.optimize(&plan).unwrap();

        assert!(optimized.is_none());
    }
}
