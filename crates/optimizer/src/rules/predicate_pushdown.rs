use yachtsql_core::error::Result;
use yachtsql_ir::expr::{BinaryOp, Expr};
use yachtsql_ir::plan::{JoinType, LogicalPlan, PlanNode};

use crate::properties::infer_output_columns;
use crate::rule::OptimizationRule;

pub struct PredicatePushdown;

impl PredicatePushdown {
    pub fn new() -> Self {
        Self
    }

    fn split_conjunction(expr: &Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let mut left_predicates = Self::split_conjunction(left);
                let mut right_predicates = Self::split_conjunction(right);
                left_predicates.append(&mut right_predicates);
                left_predicates
            }
            _ => vec![expr.clone()],
        }
    }

    fn combine_predicates(predicates: Vec<Expr>) -> Option<Expr> {
        if predicates.is_empty() {
            return None;
        }

        predicates.into_iter().reduce(|acc, pred| Expr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOp::And,
            right: Box::new(pred),
        })
    }

    fn predicate_references_left(expr: &Expr, left_columns: &[String]) -> bool {
        Self::get_column_references(expr)
            .iter()
            .all(|col| left_columns.contains(col))
    }

    fn predicate_references_right(expr: &Expr, right_columns: &[String]) -> bool {
        Self::get_column_references(expr)
            .iter()
            .all(|col| right_columns.contains(col))
    }

    fn get_column_references(expr: &Expr) -> Vec<String> {
        match expr {
            Expr::Column { name, .. } => vec![name.clone()],
            Expr::BinaryOp { left, right, .. } => {
                let mut refs = Self::get_column_references(left);
                refs.extend(Self::get_column_references(right));
                refs
            }
            Expr::UnaryOp { expr, .. } => Self::get_column_references(expr),
            Expr::Literal(_) => vec![],
            Expr::Function { args, .. } => {
                let mut refs = vec![];
                for arg in args {
                    refs.extend(Self::get_column_references(arg));
                }
                refs
            }
            Expr::Aggregate { args, .. } => {
                let mut refs = vec![];
                for arg in args {
                    refs.extend(Self::get_column_references(arg));
                }
                refs
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let mut refs = vec![];
                if let Some(op) = operand {
                    refs.extend(Self::get_column_references(op));
                }
                for (when_expr, then_expr) in when_then {
                    refs.extend(Self::get_column_references(when_expr));
                    refs.extend(Self::get_column_references(then_expr));
                }
                if let Some(else_e) = else_expr {
                    refs.extend(Self::get_column_references(else_e));
                }
                refs
            }
            Expr::InList { expr, list, .. } => {
                let mut refs = Self::get_column_references(expr);
                for item in list {
                    refs.extend(Self::get_column_references(item));
                }
                refs
            }
            Expr::TupleInList { tuple, list, .. } => {
                let mut refs = vec![];
                for t_expr in tuple {
                    refs.extend(Self::get_column_references(t_expr));
                }
                for tuple_list in list {
                    for item in tuple_list {
                        refs.extend(Self::get_column_references(item));
                    }
                }
                refs
            }
            Expr::TupleInSubquery { tuple, .. } => {
                tuple.iter().flat_map(Self::get_column_references).collect()
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                let mut refs = Self::get_column_references(expr);
                refs.extend(Self::get_column_references(low));
                refs.extend(Self::get_column_references(high));
                refs
            }
            Expr::Wildcard | Expr::QualifiedWildcard { .. } | Expr::Excluded { .. } => vec![],
            Expr::Tuple(exprs) => exprs.iter().flat_map(Self::get_column_references).collect(),
            Expr::Grouping { column } => vec![column.clone()],
            Expr::Cast { expr, .. } => Self::get_column_references(expr),
            Expr::TryCast { expr, .. } => Self::get_column_references(expr),
            Expr::Subquery { .. } => vec![],
            Expr::Exists { .. } => vec![],
            Expr::InSubquery { expr, .. } => Self::get_column_references(expr),
            Expr::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                let mut refs = vec![];
                for arg in args {
                    refs.extend(Self::get_column_references(arg));
                }
                for part in partition_by {
                    refs.extend(Self::get_column_references(part));
                }
                for order in order_by {
                    refs.extend(Self::get_column_references(&order.expr));
                }
                refs
            }
            Expr::ArrayIndex { array, index, .. } => {
                let mut refs = Self::get_column_references(array);
                refs.extend(Self::get_column_references(index));
                refs
            }
            Expr::AnyOp { left, .. } | Expr::AllOp { left, .. } => {
                Self::get_column_references(left)
            }
            Expr::ScalarSubquery { .. } => vec![],
            Expr::ArraySlice { array, start, end } => {
                let mut cols = Self::get_column_references(array);
                if let Some(s) = start {
                    cols.extend(Self::get_column_references(s));
                }
                if let Some(e) = end {
                    cols.extend(Self::get_column_references(e));
                }
                cols
            }
            Expr::StructLiteral { fields } => {
                let mut refs = Vec::new();
                for field in fields {
                    refs.extend(Self::get_column_references(&field.expr));
                }
                refs
            }
            Expr::StructFieldAccess { expr, .. } => Self::get_column_references(expr),
            Expr::IsDistinctFrom { left, right, .. } => {
                let mut refs = Self::get_column_references(left);
                refs.extend(Self::get_column_references(right));
                refs
            }
            Expr::Lambda { body, .. } => Self::get_column_references(body),
        }
    }

    fn get_plan_columns(node: &PlanNode) -> Option<Vec<String>> {
        infer_output_columns(node).map(|set| set.into_iter().collect())
    }

    #[allow(clippy::only_used_in_recursion)]
    fn optimize_node(&self, node: &PlanNode) -> Option<PlanNode> {
        match node {
            PlanNode::Filter { predicate, input } => {
                if let PlanNode::Projection {
                    expressions,
                    input: proj_input,
                } = input.as_ref()
                {
                    let new_filter = PlanNode::Filter {
                        predicate: predicate.clone(),
                        input: proj_input.clone(),
                    };
                    return Some(PlanNode::Projection {
                        expressions: expressions.clone(),
                        input: Box::new(new_filter),
                    });
                }

                if let PlanNode::Join {
                    left,
                    right,
                    on,
                    join_type,
                } = input.as_ref()
                {
                    let predicates = Self::split_conjunction(predicate);

                    let left_cols = Self::get_plan_columns(left);
                    let right_cols = Self::get_plan_columns(right);

                    let mut left_predicates = Vec::new();
                    let mut right_predicates = Vec::new();
                    let mut join_predicates = Vec::new();

                    for pred in predicates {
                        if left_cols
                            .as_ref()
                            .is_some_and(|cols| Self::predicate_references_left(&pred, cols))
                        {
                            left_predicates.push(pred);
                        } else if right_cols
                            .as_ref()
                            .is_some_and(|cols| Self::predicate_references_right(&pred, cols))
                        {
                            right_predicates.push(pred);
                        } else {
                            join_predicates.push(pred);
                        }
                    }

                    let can_push_left = matches!(join_type, JoinType::Inner);
                    let can_push_right = matches!(join_type, JoinType::Inner);

                    let new_left = if can_push_left && !left_predicates.is_empty() {
                        if let Some(pred) = Self::combine_predicates(left_predicates) {
                            Box::new(PlanNode::Filter {
                                predicate: pred,
                                input: left.clone(),
                            })
                        } else {
                            left.clone()
                        }
                    } else {
                        join_predicates.extend(left_predicates);
                        left.clone()
                    };

                    let new_right = if can_push_right && !right_predicates.is_empty() {
                        if let Some(pred) = Self::combine_predicates(right_predicates) {
                            Box::new(PlanNode::Filter {
                                predicate: pred,
                                input: right.clone(),
                            })
                        } else {
                            right.clone()
                        }
                    } else {
                        join_predicates.extend(right_predicates);
                        right.clone()
                    };

                    let new_join = PlanNode::Join {
                        left: new_left,
                        right: new_right,
                        on: on.clone(),
                        join_type: *join_type,
                    };

                    return if let Some(remaining_pred) = Self::combine_predicates(join_predicates) {
                        Some(PlanNode::Filter {
                            predicate: remaining_pred,
                            input: Box::new(new_join),
                        })
                    } else {
                        Some(new_join)
                    };
                }

                self.optimize_node(input)
                    .map(|optimized_input| PlanNode::Filter {
                        predicate: predicate.clone(),
                        input: Box::new(optimized_input),
                    })
            }
            PlanNode::Projection { expressions, input } => {
                self.optimize_node(input)
                    .map(|optimized_input| PlanNode::Projection {
                        expressions: expressions.clone(),
                        input: Box::new(optimized_input),
                    })
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
                grouping_metadata,
            } => self
                .optimize_node(input)
                .map(|optimized_input| PlanNode::Aggregate {
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    input: Box::new(optimized_input),
                    grouping_metadata: grouping_metadata.clone(),
                }),
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
            | PlanNode::Unnest { .. }
            | PlanNode::TableValuedFunction { .. }
            | PlanNode::AlterTable { .. }
            | PlanNode::DistinctOn { .. }
            | PlanNode::ArrayJoin { .. } => None,
            PlanNode::EmptyRelation
            | PlanNode::Values { .. }
            | PlanNode::InsertOnConflict { .. }
            | PlanNode::Insert { .. }
            | PlanNode::Merge { .. } => None,
            PlanNode::Window {
                input,
                window_exprs,
            } => self.optimize_node(input).map(|new_input| PlanNode::Window {
                input: Box::new(new_input),
                window_exprs: window_exprs.clone(),
            }),
        }
    }
}

impl Default for PredicatePushdown {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for PredicatePushdown {
    fn name(&self) -> &str {
        "predicate_pushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(self.optimize_node(plan.root()).map(LogicalPlan::new))
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::error::Result;

    use super::*;
    use crate::optimizer::expr::{BinaryOp, Expr, LiteralValue};

    #[test]
    fn test_pushdown_through_projection() -> Result<()> {
        let rule = PredicatePushdown::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let projection = PlanNode::Projection {
            expressions: vec![(Expr::column("x"), None), (Expr::column("y"), None)],
            input: Box::new(scan),
        };

        let filter = PlanNode::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::column("x")),
                op: BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
            },
            input: Box::new(projection),
        };

        let plan = LogicalPlan::new(filter);
        let optimized = rule.optimize(&plan)?;

        assert!(optimized.is_some());
        let optimized_plan = optimized.expect("optimization should produce a plan");

        match optimized_plan.root() {
            PlanNode::Projection { input, .. } => {
                assert!(matches!(input.as_ref(), PlanNode::Filter { .. }));
            }
            _ => panic!("Expected Projection at root"),
        }
        Ok(())
    }

    #[test]
    fn test_split_conjunction() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::column("x")),
                    op: BinaryOp::GreaterThan,
                    right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
                }),
                op: BinaryOp::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::column("y")),
                    op: BinaryOp::LessThan,
                    right: Box::new(Expr::Literal(LiteralValue::Int64(10))),
                }),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::column("z")),
                op: BinaryOp::Equal,
                right: Box::new(Expr::Literal(LiteralValue::Int64(3))),
            }),
        };

        let predicates = PredicatePushdown::split_conjunction(&expr);
        assert_eq!(predicates.len(), 3);
    }

    #[test]
    fn test_combine_predicates() {
        let pred1 = Expr::BinaryOp {
            left: Box::new(Expr::column("x")),
            op: BinaryOp::GreaterThan,
            right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };

        let pred2 = Expr::BinaryOp {
            left: Box::new(Expr::column("y")),
            op: BinaryOp::LessThan,
            right: Box::new(Expr::Literal(LiteralValue::Int64(10))),
        };

        let combined = PredicatePushdown::combine_predicates(vec![pred1, pred2]);
        assert!(combined.is_some());
        let combined_expr = combined.expect("should have combined predicates");
        assert!(matches!(
            combined_expr,
            Expr::BinaryOp {
                op: BinaryOp::And,
                ..
            }
        ));
    }

    #[test]
    fn test_combine_empty_predicates() {
        let combined = PredicatePushdown::combine_predicates(vec![]);
        assert!(combined.is_none());
    }

    #[test]
    fn test_get_column_references() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::column("x")),
                op: BinaryOp::Add,
                right: Box::new(Expr::column("y")),
            }),
            op: BinaryOp::GreaterThan,
            right: Box::new(Expr::column("z")),
        };

        let refs = PredicatePushdown::get_column_references(&expr);
        assert_eq!(refs.len(), 3);
        assert!(refs.contains(&"x".to_string()));
        assert!(refs.contains(&"y".to_string()));
        assert!(refs.contains(&"z".to_string()));
    }

    #[test]
    fn test_no_pushdown_on_scan() -> Result<()> {
        let rule = PredicatePushdown::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let filter = PlanNode::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::column("x")),
                op: BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
            },
            input: Box::new(scan),
        };

        let plan = LogicalPlan::new(filter);
        let optimized = rule.optimize(&plan)?;

        assert!(optimized.is_none());
        Ok(())
    }

    #[test]
    fn test_pushdown_through_multiple_projections() -> Result<()> {
        let rule = PredicatePushdown::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let proj1 = PlanNode::Projection {
            expressions: vec![(Expr::column("x"), None), (Expr::column("y"), None)],
            input: Box::new(scan),
        };

        let proj2 = PlanNode::Projection {
            expressions: vec![(Expr::column("x"), None)],
            input: Box::new(proj1),
        };

        let filter = PlanNode::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::column("x")),
                op: BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
            },
            input: Box::new(proj2),
        };

        let plan = LogicalPlan::new(filter);

        let optimized = rule.optimize(&plan)?;
        assert!(optimized.is_some());

        let optimized_plan = optimized.expect("optimization should produce a plan");
        match optimized_plan.root() {
            PlanNode::Projection { input, .. } => {
                assert!(matches!(input.as_ref(), PlanNode::Filter { .. }));
            }
            _ => panic!("Expected Projection at root"),
        }
        Ok(())
    }
}
