use std::collections::HashSet;

use yachtsql_core::error::Result;

use crate::optimizer::expr::Expr;
use crate::optimizer::plan::{LogicalPlan, PlanNode};
use crate::optimizer::rule::OptimizationRule;

pub struct ProjectionPushdown;

impl ProjectionPushdown {
    pub fn new() -> Self {
        Self
    }

    fn get_column_references(expr: &Expr) -> HashSet<String> {
        let mut refs = HashSet::new();
        Self::collect_column_references(expr, &mut refs);
        refs
    }

    fn collect_column_references(expr: &Expr, refs: &mut HashSet<String>) {
        match expr {
            Expr::Subquery { .. } => {}
            Expr::Column { name, .. } => {
                refs.insert(name.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_column_references(left, refs);
                Self::collect_column_references(right, refs);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                Self::collect_column_references(inner, refs);
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    Self::collect_column_references(arg, refs);
                }
            }
            Expr::Aggregate { args, .. } => {
                for arg in args {
                    Self::collect_column_references(arg, refs);
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    Self::collect_column_references(op, refs);
                }
                for (when_expr, then_expr) in when_then {
                    Self::collect_column_references(when_expr, refs);
                    Self::collect_column_references(then_expr, refs);
                }
                if let Some(else_e) = else_expr {
                    Self::collect_column_references(else_e, refs);
                }
            }
            Expr::InList { expr, list, .. } => {
                Self::collect_column_references(expr, refs);
                for item in list {
                    Self::collect_column_references(item, refs);
                }
            }
            Expr::TupleInList { tuple, list, .. } => {
                for t_expr in tuple {
                    Self::collect_column_references(t_expr, refs);
                }
                for tuple_list in list {
                    for item in tuple_list {
                        Self::collect_column_references(item, refs);
                    }
                }
            }
            Expr::TupleInSubquery { tuple, .. } => {
                for t_expr in tuple {
                    Self::collect_column_references(t_expr, refs);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::collect_column_references(expr, refs);
                Self::collect_column_references(low, refs);
                Self::collect_column_references(high, refs);
            }
            Expr::Cast { expr, .. } => {
                Self::collect_column_references(expr, refs);
            }
            Expr::TryCast { expr, .. } => {
                Self::collect_column_references(expr, refs);
            }
            Expr::Literal(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::ExpressionWildcard { .. }
            | Expr::Excluded { .. } => {}
            Expr::Tuple(exprs) => {
                for e in exprs {
                    Self::collect_column_references(e, refs);
                }
            }
            Expr::Grouping { column } => {
                refs.insert(column.clone());
            }
            Expr::GroupingId { columns } => {
                for col in columns {
                    refs.insert(col.clone());
                }
            }
            Expr::Exists { .. } => {}
            Expr::InSubquery { expr, .. } => {
                Self::collect_column_references(expr, refs);
            }
            Expr::InTable { expr, .. } => {
                Self::collect_column_references(expr, refs);
            }
            Expr::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    Self::collect_column_references(arg, refs);
                }
                for part in partition_by {
                    Self::collect_column_references(part, refs);
                }
                for order in order_by {
                    Self::collect_column_references(&order.expr, refs);
                }
            }
            Expr::ArrayIndex { array, index, .. } => {
                Self::collect_column_references(array, refs);
                Self::collect_column_references(index, refs);
            }
            Expr::AnyOp { left, .. } | Expr::AllOp { left, .. } => {
                Self::collect_column_references(left, refs);
            }
            Expr::ScalarSubquery { .. } => {}
            Expr::ArraySubquery { .. } => {}
            Expr::ArraySlice { array, start, end } => {
                Self::collect_column_references(array, refs);
                if let Some(s) = start {
                    Self::collect_column_references(s, refs);
                }
                if let Some(e) = end {
                    Self::collect_column_references(e, refs);
                }
            }
            Expr::StructLiteral { fields } => {
                for field in fields {
                    Self::collect_column_references(&field.expr, refs);
                }
            }
            Expr::StructFieldAccess { expr, .. } => {
                Self::collect_column_references(expr, refs);
            }
            Expr::TupleElementAccess { expr, .. } => {
                Self::collect_column_references(expr, refs);
            }
            Expr::IsDistinctFrom { left, right, .. } => {
                Self::collect_column_references(left, refs);
                Self::collect_column_references(right, refs);
            }
            Expr::Lambda { body, .. } => {
                Self::collect_column_references(body, refs);
            }
        }
    }

    fn get_required_columns(node: &PlanNode) -> HashSet<String> {
        match node {
            PlanNode::Projection { expressions, .. } => {
                let mut cols = HashSet::new();
                for (expr, _) in expressions {
                    cols.extend(Self::get_column_references(expr));
                }
                cols
            }
            PlanNode::Filter { predicate, .. } => Self::get_column_references(predicate),
            PlanNode::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                let mut cols = HashSet::new();
                for expr in group_by {
                    cols.extend(Self::get_column_references(expr));
                }
                for expr in aggregates {
                    cols.extend(Self::get_column_references(expr));
                }
                cols
            }
            PlanNode::Sort { order_by, .. } => {
                let mut cols = HashSet::new();
                for order_expr in order_by {
                    cols.extend(Self::get_column_references(&order_expr.expr));
                }
                cols
            }
            PlanNode::Join { on, .. } => Self::get_column_references(on),
            PlanNode::AsOfJoin {
                equality_condition,
                match_condition,
                ..
            } => {
                let mut cols = Self::get_column_references(equality_condition);
                cols.extend(Self::get_column_references(match_condition));
                cols
            }
            _ => HashSet::new(),
        }
    }

    fn add_projection_to_scan(
        scan: &PlanNode,
        required_cols: &HashSet<String>,
    ) -> Option<PlanNode> {
        if let PlanNode::Scan {
            table_name,
            alias,
            projection,
            only,
            final_modifier: false,
        } = scan
            && projection.is_none()
            && !required_cols.is_empty()
        {
            return Some(PlanNode::Scan {
                table_name: table_name.clone(),
                alias: alias.clone(),
                projection: Some(required_cols.iter().cloned().collect()),
                only: *only,
                final_modifier: false,
            });
        }
        None
    }

    #[allow(clippy::only_used_in_recursion)]
    fn optimize_node(&self, node: &PlanNode, required_cols: &HashSet<String>) -> Option<PlanNode> {
        match node {
            PlanNode::Projection { expressions, input } => {
                let mut cols_needed = HashSet::new();
                for (expr, _) in expressions {
                    cols_needed.extend(Self::get_column_references(expr));
                }

                cols_needed.extend(required_cols.iter().cloned());

                self.optimize_node(input, &cols_needed)
                    .map(|optimized_input| PlanNode::Projection {
                        expressions: expressions.clone(),
                        input: Box::new(optimized_input),
                    })
            }
            PlanNode::Filter { predicate, input } => {
                let mut cols_needed = required_cols.clone();
                cols_needed.extend(Self::get_column_references(predicate));

                self.optimize_node(input, &cols_needed)
                    .map(|optimized_input| PlanNode::Filter {
                        predicate: predicate.clone(),
                        input: Box::new(optimized_input),
                    })
            }
            PlanNode::Aggregate {
                group_by,
                aggregates,
                input,
                ..
            } => {
                let mut cols_needed = HashSet::new();
                for expr in group_by {
                    cols_needed.extend(Self::get_column_references(expr));
                }
                for expr in aggregates {
                    cols_needed.extend(Self::get_column_references(expr));
                }

                self.optimize_node(input, &cols_needed)
                    .map(|optimized_input| PlanNode::Aggregate {
                        group_by: group_by.clone(),
                        aggregates: aggregates.clone(),
                        input: Box::new(optimized_input),
                        grouping_metadata: None,
                    })
            }
            PlanNode::Sort { order_by, input } => {
                let mut cols_needed = required_cols.clone();
                for order_expr in order_by {
                    cols_needed.extend(Self::get_column_references(&order_expr.expr));
                }

                self.optimize_node(input, &cols_needed)
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
                .optimize_node(input, required_cols)
                .map(|optimized_input| PlanNode::Limit {
                    limit: *limit,
                    offset: *offset,
                    input: Box::new(optimized_input),
                }),
            PlanNode::LimitPercent {
                percent,
                offset,
                with_ties,
                input,
            } => self
                .optimize_node(input, required_cols)
                .map(|optimized_input| PlanNode::LimitPercent {
                    percent: *percent,
                    offset: *offset,
                    with_ties: *with_ties,
                    input: Box::new(optimized_input),
                }),
            PlanNode::Distinct { input } => {
                self.optimize_node(input, required_cols)
                    .map(|optimized_input| PlanNode::Distinct {
                        input: Box::new(optimized_input),
                    })
            }
            PlanNode::Join {
                left,
                right,
                on,
                join_type,
                using_columns,
            } => {
                let mut cols_needed = required_cols.clone();
                cols_needed.extend(Self::get_column_references(on));

                let left_opt = self.optimize_node(left, &cols_needed);
                let right_opt = self.optimize_node(right, &cols_needed);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::Join {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        on: on.clone(),
                        join_type: *join_type,
                        using_columns: using_columns.clone(),
                    })
                } else {
                    None
                }
            }
            PlanNode::AsOfJoin {
                left,
                right,
                equality_condition,
                match_condition,
                is_left_join,
            } => {
                let mut cols_needed = required_cols.clone();
                cols_needed.extend(Self::get_column_references(equality_condition));
                cols_needed.extend(Self::get_column_references(match_condition));

                let left_opt = self.optimize_node(left, &cols_needed);
                let right_opt = self.optimize_node(right, &cols_needed);

                if left_opt.is_some() || right_opt.is_some() {
                    Some(PlanNode::AsOfJoin {
                        left: Box::new(left_opt.unwrap_or_else(|| left.as_ref().clone())),
                        right: Box::new(right_opt.unwrap_or_else(|| right.as_ref().clone())),
                        equality_condition: equality_condition.clone(),
                        match_condition: match_condition.clone(),
                        is_left_join: *is_left_join,
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
                let mut cols_needed = required_cols.clone();
                cols_needed.extend(Self::get_column_references(on));

                let left_opt = self.optimize_node(left, &cols_needed);
                let right_opt = self.optimize_node(right, &cols_needed);

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
            PlanNode::SubqueryScan { subquery, alias } => self
                .optimize_node(subquery, required_cols)
                .map(|optimized_subquery| PlanNode::SubqueryScan {
                    subquery: Box::new(optimized_subquery),
                    alias: alias.clone(),
                }),
            PlanNode::Union { left, right, all } => {
                let left_opt = self.optimize_node(left, required_cols);
                let right_opt = self.optimize_node(right, required_cols);

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
                let left_opt = self.optimize_node(left, required_cols);
                let right_opt = self.optimize_node(right, required_cols);

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
                let left_opt = self.optimize_node(left, required_cols);
                let right_opt = self.optimize_node(right, required_cols);

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
                let cte_opt = self.optimize_node(cte_plan, required_cols);
                let input_opt = self.optimize_node(input, required_cols);

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
                .optimize_node(input, required_cols)
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
                let mut cols_needed = required_cols.clone();
                cols_needed.extend(Self::get_column_references(aggregate_expr));
                cols_needed.insert(pivot_column.clone());
                for col in group_by_columns {
                    cols_needed.insert(col.clone());
                }

                self.optimize_node(input, &cols_needed)
                    .map(|optimized_input| PlanNode::Pivot {
                        input: Box::new(optimized_input),
                        aggregate_expr: aggregate_expr.clone(),
                        aggregate_function: aggregate_function.clone(),
                        pivot_column: pivot_column.clone(),
                        pivot_values: pivot_values.clone(),
                        group_by_columns: group_by_columns.clone(),
                    })
            }
            PlanNode::Unpivot {
                input,
                value_column,
                name_column,
                unpivot_columns,
            } => {
                let mut cols_needed = required_cols.clone();
                for col in unpivot_columns {
                    cols_needed.insert(col.clone());
                }

                self.optimize_node(input, &cols_needed)
                    .map(|optimized_input| PlanNode::Unpivot {
                        input: Box::new(optimized_input),
                        value_column: value_column.clone(),
                        name_column: name_column.clone(),
                        unpivot_columns: unpivot_columns.clone(),
                    })
            }
            PlanNode::Scan { .. } => Self::add_projection_to_scan(node, required_cols),
            PlanNode::IndexScan { .. } => None,
            PlanNode::Update { .. }
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

impl Default for ProjectionPushdown {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for ProjectionPushdown {
    fn name(&self) -> &str {
        "projection_pushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let required = Self::get_required_columns(plan.root());
        Ok(self
            .optimize_node(plan.root(), &required)
            .map(LogicalPlan::new))
    }
}
