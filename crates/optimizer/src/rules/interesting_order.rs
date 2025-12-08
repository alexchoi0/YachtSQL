use std::collections::HashMap;

use yachtsql_core::error::Result;
use yachtsql_ir::expr::Expr;
use yachtsql_ir::plan::{LogicalPlan, PlanNode};

use crate::ordering::{OrderingProperty, is_equi_join};
use crate::rule::OptimizationRule;

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub ordering: Option<OrderingProperty>,
}

pub struct InterestingOrderRule;

impl InterestingOrderRule {
    pub fn new() -> Self {
        Self
    }

    fn optimize_node(&self, node: &PlanNode) -> Result<Option<PlanNode>> {
        let (optimized, _, changed) =
            self.optimize_with_context(node, &OrderingProperty::empty())?;
        if changed {
            Ok(Some(optimized))
        } else {
            Ok(None)
        }
    }

    fn optimize_with_context(
        &self,
        node: &PlanNode,
        required_order: &OrderingProperty,
    ) -> Result<(PlanNode, OrderingProperty, bool)> {
        match node {
            PlanNode::Sort { order_by, input } => {
                let sort_order = OrderingProperty::from_order_by(order_by);

                let (opt_input, child_order, child_changed) =
                    self.optimize_with_context(input, &sort_order)?;

                if child_order.satisfies(&sort_order) {
                    return Ok((opt_input, child_order, true));
                }

                let new_node = PlanNode::Sort {
                    order_by: order_by.clone(),
                    input: Box::new(opt_input),
                };
                Ok((new_node, sort_order, child_changed))
            }

            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                grouping_metadata,
            } => {
                let group_order = OrderingProperty::from_exprs(group_by);

                let combined_requirement = if !group_order.is_empty() {
                    self.merge_requirements(required_order, &group_order)
                } else {
                    required_order.clone()
                };

                let (opt_input, child_order, child_changed) =
                    self.optimize_with_context(input, &combined_requirement)?;

                let output_order = if child_order.satisfies(&group_order) && !group_order.is_empty()
                {
                    group_order
                } else {
                    OrderingProperty::empty()
                };

                let new_node = PlanNode::Aggregate {
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    input: Box::new(opt_input),
                    grouping_metadata: grouping_metadata.clone(),
                };
                Ok((new_node, output_order, child_changed))
            }

            PlanNode::Join {
                left,
                right,
                on,
                join_type,
            } => {
                let join_order = if is_equi_join(on) {
                    self.extract_join_key_ordering(on)
                } else {
                    OrderingProperty::empty()
                };

                let left_requirement = if !join_order.is_empty() {
                    self.merge_requirements(required_order, &join_order)
                } else {
                    required_order.clone()
                };

                let (opt_left, left_order, left_changed) =
                    self.optimize_with_context(left, &left_requirement)?;

                let right_requirement = join_order.clone();
                let (opt_right, right_order, right_changed) =
                    self.optimize_with_context(right, &right_requirement)?;

                let output_order = if left_order.satisfies(&join_order)
                    && right_order.satisfies(&join_order)
                    && !join_order.is_empty()
                {
                    left_order
                } else {
                    OrderingProperty::empty()
                };

                let new_node = PlanNode::Join {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    on: on.clone(),
                    join_type: *join_type,
                };
                Ok((new_node, output_order, left_changed || right_changed))
            }

            PlanNode::LateralJoin {
                left,
                right,
                on,
                join_type,
            } => {
                let (opt_left, left_order, left_changed) =
                    self.optimize_with_context(left, required_order)?;
                let (opt_right, _right_order, right_changed) =
                    self.optimize_with_context(right, &OrderingProperty::empty())?;

                let new_node = PlanNode::LateralJoin {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    on: on.clone(),
                    join_type: *join_type,
                };
                Ok((new_node, left_order, left_changed || right_changed))
            }

            PlanNode::Filter { predicate, input } => {
                let (opt_input, child_order, changed) =
                    self.optimize_with_context(input, required_order)?;

                let new_node = PlanNode::Filter {
                    predicate: predicate.clone(),
                    input: Box::new(opt_input),
                };
                Ok((new_node, child_order, changed))
            }

            PlanNode::Projection { expressions, input } => {
                let (opt_input, child_order, changed) =
                    self.optimize_with_context(input, required_order)?;

                let output_order = self.project_ordering(&child_order, expressions);

                let new_node = PlanNode::Projection {
                    expressions: expressions.clone(),
                    input: Box::new(opt_input),
                };
                Ok((new_node, output_order, changed))
            }

            PlanNode::Limit {
                limit,
                offset,
                input,
            } => {
                let (opt_input, child_order, changed) =
                    self.optimize_with_context(input, required_order)?;

                let new_node = PlanNode::Limit {
                    limit: *limit,
                    offset: *offset,
                    input: Box::new(opt_input),
                };
                Ok((new_node, child_order, changed))
            }

            PlanNode::Distinct { input } => {
                let (opt_input, _child_order, changed) =
                    self.optimize_with_context(input, required_order)?;

                let new_node = PlanNode::Distinct {
                    input: Box::new(opt_input),
                };
                Ok((new_node, OrderingProperty::empty(), changed))
            }

            PlanNode::DistinctOn { expressions, input } => {
                let distinct_order = OrderingProperty::from_exprs(expressions);
                let combined = self.merge_requirements(required_order, &distinct_order);

                let (opt_input, _child_order, changed) =
                    self.optimize_with_context(input, &combined)?;

                let new_node = PlanNode::DistinctOn {
                    expressions: expressions.clone(),
                    input: Box::new(opt_input),
                };
                Ok((new_node, OrderingProperty::empty(), changed))
            }

            PlanNode::Union { left, right, all } => {
                let (opt_left, _left_order, left_changed) =
                    self.optimize_with_context(left, &OrderingProperty::empty())?;
                let (opt_right, _right_order, right_changed) =
                    self.optimize_with_context(right, &OrderingProperty::empty())?;

                let new_node = PlanNode::Union {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    all: *all,
                };
                Ok((
                    new_node,
                    OrderingProperty::empty(),
                    left_changed || right_changed,
                ))
            }

            PlanNode::Intersect { left, right, all } => {
                let (opt_left, _left_order, left_changed) =
                    self.optimize_with_context(left, &OrderingProperty::empty())?;
                let (opt_right, _right_order, right_changed) =
                    self.optimize_with_context(right, &OrderingProperty::empty())?;

                let new_node = PlanNode::Intersect {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    all: *all,
                };
                Ok((
                    new_node,
                    OrderingProperty::empty(),
                    left_changed || right_changed,
                ))
            }

            PlanNode::Except { left, right, all } => {
                let (opt_left, _left_order, left_changed) =
                    self.optimize_with_context(left, &OrderingProperty::empty())?;
                let (opt_right, _right_order, right_changed) =
                    self.optimize_with_context(right, &OrderingProperty::empty())?;

                let new_node = PlanNode::Except {
                    left: Box::new(opt_left),
                    right: Box::new(opt_right),
                    all: *all,
                };
                Ok((
                    new_node,
                    OrderingProperty::empty(),
                    left_changed || right_changed,
                ))
            }

            PlanNode::SubqueryScan { subquery, alias } => {
                let (opt_subquery, child_order, changed) =
                    self.optimize_with_context(subquery, required_order)?;

                let new_node = PlanNode::SubqueryScan {
                    subquery: Box::new(opt_subquery),
                    alias: alias.clone(),
                };
                Ok((new_node, child_order, changed))
            }

            PlanNode::Cte {
                name,
                cte_plan,
                input,
                recursive,
                use_union_all,
                materialization_hint,
            } => {
                let (opt_cte, _cte_order, cte_changed) =
                    self.optimize_with_context(cte_plan, &OrderingProperty::empty())?;
                let (opt_input, input_order, input_changed) =
                    self.optimize_with_context(input, required_order)?;

                let new_node = PlanNode::Cte {
                    name: name.clone(),
                    cte_plan: Box::new(opt_cte),
                    input: Box::new(opt_input),
                    recursive: *recursive,
                    use_union_all: *use_union_all,
                    materialization_hint: materialization_hint.clone(),
                };
                Ok((new_node, input_order, cte_changed || input_changed))
            }

            PlanNode::Window {
                window_exprs,
                input,
            } => {
                let window_order = self.extract_window_ordering(window_exprs);
                let combined = self.merge_requirements(required_order, &window_order);

                let (opt_input, child_order, changed) =
                    self.optimize_with_context(input, &combined)?;

                let new_node = PlanNode::Window {
                    window_exprs: window_exprs.clone(),
                    input: Box::new(opt_input),
                };
                Ok((new_node, child_order, changed))
            }

            PlanNode::TableSample {
                input,
                method,
                size,
                seed,
            } => {
                let (opt_input, _child_order, changed) =
                    self.optimize_with_context(input, &OrderingProperty::empty())?;

                let new_node = PlanNode::TableSample {
                    input: Box::new(opt_input),
                    method: method.clone(),
                    size: size.clone(),
                    seed: *seed,
                };
                Ok((new_node, OrderingProperty::empty(), changed))
            }

            PlanNode::Pivot {
                input,
                aggregate_expr,
                aggregate_function,
                pivot_column,
                pivot_values,
                group_by_columns,
            } => {
                let (opt_input, _child_order, changed) =
                    self.optimize_with_context(input, &OrderingProperty::empty())?;

                let new_node = PlanNode::Pivot {
                    input: Box::new(opt_input),
                    aggregate_expr: aggregate_expr.clone(),
                    aggregate_function: aggregate_function.clone(),
                    pivot_column: pivot_column.clone(),
                    pivot_values: pivot_values.clone(),
                    group_by_columns: group_by_columns.clone(),
                };
                Ok((new_node, OrderingProperty::empty(), changed))
            }

            PlanNode::Unpivot {
                input,
                value_column,
                name_column,
                unpivot_columns,
            } => {
                let (opt_input, _child_order, changed) =
                    self.optimize_with_context(input, &OrderingProperty::empty())?;

                let new_node = PlanNode::Unpivot {
                    input: Box::new(opt_input),
                    value_column: value_column.clone(),
                    name_column: name_column.clone(),
                    unpivot_columns: unpivot_columns.clone(),
                };
                Ok((new_node, OrderingProperty::empty(), changed))
            }

            PlanNode::ArrayJoin {
                input,
                arrays,
                is_left,
                is_unaligned,
            } => {
                let (opt_input, _child_order, changed) =
                    self.optimize_with_context(input, &OrderingProperty::empty())?;

                let new_node = PlanNode::ArrayJoin {
                    input: Box::new(opt_input),
                    arrays: arrays.clone(),
                    is_left: *is_left,
                    is_unaligned: *is_unaligned,
                };
                Ok((new_node, OrderingProperty::empty(), changed))
            }

            PlanNode::Merge {
                target_table,
                target_alias,
                source,
                source_alias,
                on_condition,
                when_matched,
                when_not_matched,
                when_not_matched_by_source,
                returning,
            } => {
                let (opt_source, _source_order, changed) =
                    self.optimize_with_context(source, &OrderingProperty::empty())?;

                let new_node = PlanNode::Merge {
                    target_table: target_table.clone(),
                    target_alias: target_alias.clone(),
                    source: Box::new(opt_source),
                    source_alias: source_alias.clone(),
                    on_condition: on_condition.clone(),
                    when_matched: when_matched.clone(),
                    when_not_matched: when_not_matched.clone(),
                    when_not_matched_by_source: when_not_matched_by_source.clone(),
                    returning: returning.clone(),
                };
                Ok((new_node, OrderingProperty::empty(), changed))
            }

            PlanNode::Insert {
                table_name,
                columns,
                values,
                source,
            } => {
                let opt_source = if let Some(src) = source {
                    let (opt_src, _, _) =
                        self.optimize_with_context(src, &OrderingProperty::empty())?;
                    Some(Box::new(opt_src))
                } else {
                    None
                };

                let new_node = PlanNode::Insert {
                    table_name: table_name.clone(),
                    columns: columns.clone(),
                    values: values.clone(),
                    source: opt_source,
                };
                Ok((new_node, OrderingProperty::empty(), false))
            }

            PlanNode::Scan { .. }
            | PlanNode::IndexScan { .. }
            | PlanNode::Update { .. }
            | PlanNode::Delete { .. }
            | PlanNode::Truncate { .. }
            | PlanNode::Unnest { .. }
            | PlanNode::TableValuedFunction { .. }
            | PlanNode::AlterTable { .. }
            | PlanNode::InsertOnConflict { .. }
            | PlanNode::EmptyRelation
            | PlanNode::Values { .. } => Ok((node.clone(), OrderingProperty::empty(), false)),
        }
    }

    fn merge_requirements(
        &self,
        parent_requirement: &OrderingProperty,
        child_interest: &OrderingProperty,
    ) -> OrderingProperty {
        if parent_requirement.is_empty() {
            return child_interest.clone();
        }
        if child_interest.is_empty() {
            return parent_requirement.clone();
        }

        let prefix_match = parent_requirement.prefix_match_len(child_interest);
        if prefix_match > 0 {
            if parent_requirement.len() >= child_interest.len() {
                parent_requirement.clone()
            } else {
                child_interest.clone()
            }
        } else {
            parent_requirement.clone()
        }
    }

    fn extract_join_key_ordering(&self, on: &Expr) -> OrderingProperty {
        let keys = Self::extract_equi_join_left_keys(on);
        OrderingProperty::from_column_names(&keys)
    }

    fn extract_equi_join_left_keys(on: &Expr) -> Vec<String> {
        match on {
            Expr::BinaryOp {
                left,
                op: yachtsql_ir::expr::BinaryOp::Equal,
                right: _,
            } => {
                if let Expr::Column { name, .. } = left.as_ref() {
                    vec![name.clone()]
                } else {
                    vec![]
                }
            }
            Expr::BinaryOp {
                left,
                op: yachtsql_ir::expr::BinaryOp::And,
                right,
            } => {
                let mut keys = Self::extract_equi_join_left_keys(left);
                keys.extend(Self::extract_equi_join_left_keys(right));
                keys
            }
            _ => vec![],
        }
    }

    fn project_ordering(
        &self,
        child_order: &OrderingProperty,
        expressions: &[(Expr, Option<String>)],
    ) -> OrderingProperty {
        let projected_columns: HashMap<String, String> = expressions
            .iter()
            .filter_map(|(expr, alias)| match expr {
                Expr::Column { name, .. } => {
                    let output_name = alias.clone().unwrap_or_else(|| name.clone());
                    Some((name.to_lowercase(), output_name))
                }
                _ => None,
            })
            .collect();

        let mut preserved_cols = Vec::new();
        for col in &child_order.columns {
            if let Some(output_name) = projected_columns.get(&col.column_name.to_lowercase()) {
                let mut new_col = col.clone();
                new_col.column_name = output_name.clone();
                preserved_cols.push(new_col);
            } else {
                break;
            }
        }

        OrderingProperty::new(preserved_cols)
    }

    fn extract_window_ordering(&self, window_exprs: &[(Expr, Option<String>)]) -> OrderingProperty {
        for (expr, _) in window_exprs {
            if let Expr::WindowFunction {
                partition_by,
                order_by,
                ..
            } = expr
            {
                let mut cols = Vec::new();

                for pb in partition_by {
                    if let Expr::Column { name, table } = pb {
                        cols.push(crate::ordering::SortColumn {
                            column_name: name.clone(),
                            table: table.clone(),
                            ascending: true,
                            nulls_first: false,
                        });
                    }
                }

                for ob in order_by {
                    if let Expr::Column { name, table } = &ob.expr {
                        let asc = ob.asc.unwrap_or(true);
                        cols.push(crate::ordering::SortColumn {
                            column_name: name.clone(),
                            table: table.clone(),
                            ascending: asc,
                            nulls_first: ob.nulls_first.unwrap_or(!asc),
                        });
                    }
                }

                if !cols.is_empty() {
                    return OrderingProperty::new(cols);
                }
            }
        }
        OrderingProperty::empty()
    }
}

impl Default for InterestingOrderRule {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for InterestingOrderRule {
    fn name(&self) -> &str {
        "interesting_order"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let optimized = self.optimize_node(&plan.root)?;

        if let Some(new_root) = optimized {
            return Ok(Some(LogicalPlan::new(new_root)));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_ir::expr::OrderByExpr;

    use super::*;

    fn col(name: &str) -> Expr {
        Expr::Column {
            name: name.to_string(),
            table: None,
        }
    }

    fn order_by(name: &str) -> OrderByExpr {
        OrderByExpr {
            expr: col(name),
            asc: Some(true),
            nulls_first: Some(false),
            collation: None,
        }
    }

    #[test]
    fn test_sort_over_sort_elimination() {
        let scan = PlanNode::Scan {
            table_name: "t".to_string(),
            alias: None,
            projection: None,
        };

        let inner_sort = PlanNode::Sort {
            order_by: vec![order_by("a"), order_by("b")],
            input: Box::new(scan),
        };

        let outer_sort = PlanNode::Sort {
            order_by: vec![order_by("a")],
            input: Box::new(inner_sort),
        };

        let plan = LogicalPlan::new(outer_sort);
        let rule = InterestingOrderRule::new();
        let result = rule.optimize(&plan).unwrap();

        assert!(result.is_some());
        let optimized = result.unwrap();
        if let PlanNode::Sort { input, .. } = optimized.root() {
            assert!(
                matches!(input.as_ref(), PlanNode::Scan { .. }),
                "Inner sort should be eliminated, leaving just one sort"
            );
        } else {
            panic!("Expected Sort at root");
        }
    }

    #[test]
    fn test_no_elimination_when_order_differs() {
        let scan = PlanNode::Scan {
            table_name: "t".to_string(),
            alias: None,
            projection: None,
        };

        let inner_sort = PlanNode::Sort {
            order_by: vec![order_by("b")],
            input: Box::new(scan),
        };

        let outer_sort = PlanNode::Sort {
            order_by: vec![order_by("a")],
            input: Box::new(inner_sort),
        };

        let plan = LogicalPlan::new(outer_sort);
        let rule = InterestingOrderRule::new();
        let result = rule.optimize(&plan).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_aggregate_preserves_group_by_ordering() {
        let scan = PlanNode::Scan {
            table_name: "t".to_string(),
            alias: None,
            projection: None,
        };

        let sort = PlanNode::Sort {
            order_by: vec![order_by("category")],
            input: Box::new(scan),
        };

        let aggregate = PlanNode::Aggregate {
            group_by: vec![col("category")],
            aggregates: vec![],
            input: Box::new(sort),
            grouping_metadata: None,
        };

        let outer_sort = PlanNode::Sort {
            order_by: vec![order_by("category")],
            input: Box::new(aggregate),
        };

        let plan = LogicalPlan::new(outer_sort);
        let rule = InterestingOrderRule::new();
        let _result = rule.optimize(&plan).unwrap();
    }

    #[test]
    fn test_filter_preserves_ordering() {
        let scan = PlanNode::Scan {
            table_name: "t".to_string(),
            alias: None,
            projection: None,
        };

        let sort = PlanNode::Sort {
            order_by: vec![order_by("a")],
            input: Box::new(scan),
        };

        let filter = PlanNode::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(col("x")),
                op: yachtsql_ir::expr::BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(yachtsql_ir::expr::LiteralValue::Int64(10))),
            },
            input: Box::new(sort),
        };

        let outer_sort = PlanNode::Sort {
            order_by: vec![order_by("a")],
            input: Box::new(filter),
        };

        let plan = LogicalPlan::new(outer_sort);
        let rule = InterestingOrderRule::new();
        let result = rule.optimize(&plan).unwrap();

        assert!(
            result.is_some(),
            "Outer sort should be eliminated since filter preserves sort order"
        );
    }

    #[test]
    fn test_projection_preserves_ordering_for_included_columns() {
        let scan = PlanNode::Scan {
            table_name: "t".to_string(),
            alias: None,
            projection: None,
        };

        let sort = PlanNode::Sort {
            order_by: vec![order_by("a")],
            input: Box::new(scan),
        };

        let projection = PlanNode::Projection {
            expressions: vec![(col("a"), None), (col("b"), None)],
            input: Box::new(sort),
        };

        let outer_sort = PlanNode::Sort {
            order_by: vec![order_by("a")],
            input: Box::new(projection),
        };

        let plan = LogicalPlan::new(outer_sort);
        let rule = InterestingOrderRule::new();
        let result = rule.optimize(&plan).unwrap();

        assert!(
            result.is_some(),
            "Outer sort should be eliminated when projection includes sorted column"
        );
    }
}
