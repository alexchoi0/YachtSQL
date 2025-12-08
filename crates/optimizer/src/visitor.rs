use yachtsql_core::error::Result;
use yachtsql_ir::plan::{LogicalPlan, PlanNode};

pub trait PlanVisitor {
    fn pre_visit(&mut self, node: &PlanNode) -> Result<()> {
        let _ = node;
        Ok(())
    }

    fn post_visit(&mut self, node: &PlanNode) -> Result<()> {
        let _ = node;
        Ok(())
    }

    fn visit(&mut self, plan: &LogicalPlan) -> Result<()> {
        self.visit_node(plan.root())
    }

    fn visit_node(&mut self, node: &PlanNode) -> Result<()> {
        self.pre_visit(node)?;

        for child in node.children() {
            self.visit_node(child)?;
        }

        self.post_visit(node)?;
        Ok(())
    }
}

pub trait PlanRewriter {
    fn rewrite_node(&mut self, node: &PlanNode) -> Result<Option<PlanNode>>;

    fn rewrite(&mut self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        self.rewrite_plan_node(plan.root())
            .map(|opt_node| opt_node.map(LogicalPlan::new))
    }

    fn rewrite_plan_node(&mut self, node: &PlanNode) -> Result<Option<PlanNode>> {
        let node_rewritten = self.rewrite_children(node)?;
        let node_to_check = node_rewritten.as_ref().unwrap_or(node);

        self.rewrite_node(node_to_check)
            .map(|opt| opt.or(node_rewritten))
    }

    fn rewrite_children(&mut self, node: &PlanNode) -> Result<Option<PlanNode>> {
        match node {
            PlanNode::Scan { .. }
            | PlanNode::IndexScan { .. }
            | PlanNode::Update { .. }
            | PlanNode::Delete { .. }
            | PlanNode::Truncate { .. }
            | PlanNode::Unnest { .. }
            | PlanNode::TableValuedFunction { .. }
            | PlanNode::AlterTable { .. }
            | PlanNode::EmptyRelation
            | PlanNode::Values { .. }
            | PlanNode::InsertOnConflict { .. }
            | PlanNode::Insert { .. } => Ok(None),

            PlanNode::Filter { predicate, input } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::Filter {
                    predicate: predicate.clone(),
                    input: Box::new(i),
                }))
            }

            PlanNode::Projection { expressions, input } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::Projection {
                    expressions: expressions.clone(),
                    input: Box::new(i),
                }))
            }

            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                grouping_metadata,
            } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::Aggregate {
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    grouping_metadata: grouping_metadata.clone(),
                    input: Box::new(i),
                }))
            }

            PlanNode::Sort { order_by, input } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::Sort {
                    order_by: order_by.clone(),
                    input: Box::new(i),
                }))
            }

            PlanNode::Limit {
                limit,
                offset,
                input,
            } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::Limit {
                    limit: *limit,
                    offset: *offset,
                    input: Box::new(i),
                }))
            }

            PlanNode::Distinct { input } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::Distinct { input: Box::new(i) }))
            }

            PlanNode::DistinctOn { expressions, input } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::DistinctOn {
                    expressions: expressions.clone(),
                    input: Box::new(i),
                }))
            }

            PlanNode::Window {
                window_exprs,
                input,
            } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::Window {
                    window_exprs: window_exprs.clone(),
                    input: Box::new(i),
                }))
            }

            PlanNode::Join {
                left,
                right,
                on,
                join_type,
            } => {
                let new_left = self.rewrite_plan_node(left)?;
                let new_right = self.rewrite_plan_node(right)?;

                if new_left.is_some() || new_right.is_some() {
                    Ok(Some(PlanNode::Join {
                        left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                        on: on.clone(),
                        join_type: *join_type,
                    }))
                } else {
                    Ok(None)
                }
            }

            PlanNode::LateralJoin {
                left,
                right,
                on,
                join_type,
            } => {
                let new_left = self.rewrite_plan_node(left)?;
                let new_right = self.rewrite_plan_node(right)?;

                if new_left.is_some() || new_right.is_some() {
                    Ok(Some(PlanNode::LateralJoin {
                        left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                        on: on.clone(),
                        join_type: *join_type,
                    }))
                } else {
                    Ok(None)
                }
            }

            PlanNode::Union { left, right, all } => {
                let new_left = self.rewrite_plan_node(left)?;
                let new_right = self.rewrite_plan_node(right)?;

                if new_left.is_some() || new_right.is_some() {
                    Ok(Some(PlanNode::Union {
                        left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                        all: *all,
                    }))
                } else {
                    Ok(None)
                }
            }

            PlanNode::Intersect { left, right, all } => {
                let new_left = self.rewrite_plan_node(left)?;
                let new_right = self.rewrite_plan_node(right)?;

                if new_left.is_some() || new_right.is_some() {
                    Ok(Some(PlanNode::Intersect {
                        left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                        all: *all,
                    }))
                } else {
                    Ok(None)
                }
            }

            PlanNode::Except { left, right, all } => {
                let new_left = self.rewrite_plan_node(left)?;
                let new_right = self.rewrite_plan_node(right)?;

                if new_left.is_some() || new_right.is_some() {
                    Ok(Some(PlanNode::Except {
                        left: Box::new(new_left.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(new_right.unwrap_or_else(|| right.as_ref().clone())),
                        all: *all,
                    }))
                } else {
                    Ok(None)
                }
            }

            PlanNode::SubqueryScan { subquery, alias } => {
                let new_subquery = self.rewrite_plan_node(subquery)?;
                Ok(new_subquery.map(|s| PlanNode::SubqueryScan {
                    subquery: Box::new(s),
                    alias: alias.clone(),
                }))
            }

            PlanNode::Cte {
                name,
                cte_plan,
                input,
                recursive,
                use_union_all,
                materialization_hint,
            } => {
                let new_cte_plan = self.rewrite_plan_node(cte_plan)?;
                let new_input = self.rewrite_plan_node(input)?;

                if new_cte_plan.is_some() || new_input.is_some() {
                    Ok(Some(PlanNode::Cte {
                        name: name.clone(),
                        cte_plan: Box::new(
                            new_cte_plan.unwrap_or_else(|| cte_plan.as_ref().clone()),
                        ),
                        input: Box::new(new_input.unwrap_or_else(|| input.as_ref().clone())),
                        recursive: *recursive,
                        use_union_all: *use_union_all,
                        materialization_hint: materialization_hint.clone(),
                    }))
                } else {
                    Ok(None)
                }
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
                let new_source = self.rewrite_plan_node(source)?;
                Ok(new_source.map(|s| PlanNode::Merge {
                    target_table: target_table.clone(),
                    target_alias: target_alias.clone(),
                    source: Box::new(s),
                    source_alias: source_alias.clone(),
                    on_condition: on_condition.clone(),
                    when_matched: when_matched.clone(),
                    when_not_matched: when_not_matched.clone(),
                    when_not_matched_by_source: when_not_matched_by_source.clone(),
                    returning: returning.clone(),
                }))
            }

            PlanNode::ArrayJoin {
                input,
                arrays,
                is_left,
                is_unaligned,
            } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::ArrayJoin {
                    input: Box::new(i),
                    arrays: arrays.clone(),
                    is_left: *is_left,
                    is_unaligned: *is_unaligned,
                }))
            }

            PlanNode::TableSample {
                input,
                method,
                size,
                seed,
            } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::TableSample {
                    input: Box::new(i),
                    method: method.clone(),
                    size: size.clone(),
                    seed: *seed,
                }))
            }

            PlanNode::Pivot {
                input,
                aggregate_expr,
                aggregate_function,
                pivot_column,
                pivot_values,
                group_by_columns,
            } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::Pivot {
                    input: Box::new(i),
                    aggregate_expr: aggregate_expr.clone(),
                    aggregate_function: aggregate_function.clone(),
                    pivot_column: pivot_column.clone(),
                    pivot_values: pivot_values.clone(),
                    group_by_columns: group_by_columns.clone(),
                }))
            }

            PlanNode::Unpivot {
                input,
                value_column,
                name_column,
                unpivot_columns,
            } => {
                let new_input = self.rewrite_plan_node(input)?;
                Ok(new_input.map(|i| PlanNode::Unpivot {
                    input: Box::new(i),
                    value_column: value_column.clone(),
                    name_column: name_column.clone(),
                    unpivot_columns: unpivot_columns.clone(),
                }))
            }
        }
    }
}
