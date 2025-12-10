use yachtsql_core::error::Result;
use yachtsql_ir::expr::{BinaryOp, Expr};
use yachtsql_ir::plan::{JoinType, PlanNode};

use crate::optimizer::plan::LogicalPlan;
use crate::optimizer::rule::OptimizationRule;

pub struct SubqueryFlattening {
    detect_only: bool,
}

#[derive(Debug, Clone)]
struct CorrelatedScalarSubquery {
    aggregate_expr: Expr,
    inner_alias: String,
    inner_column: String,
    outer_alias: String,
    outer_column: String,
    subquery_scan: PlanNode,
    result_alias: String,
}

impl SubqueryFlattening {
    pub fn new() -> Self {
        Self { detect_only: false }
    }

    pub fn detect_only() -> Self {
        Self { detect_only: true }
    }

    fn count_flattenable_subqueries(&self, plan: &LogicalPlan) -> usize {
        let mut visitor = SubqueryCounter::new();
        visitor.visit_plan(plan);
        visitor.count()
    }

    fn transform_node(&self, node: &PlanNode) -> Result<PlanNode> {
        match node {
            PlanNode::Filter { predicate, input } => {
                let transformed_input = self.transform_node(input)?;

                match self.extract_subquery_join(predicate) {
                    Some((join_type, subquery_plan, join_condition)) => {
                        let join_node = PlanNode::Join {
                            left: Box::new(transformed_input),
                            right: Box::new(subquery_plan),
                            on: join_condition,
                            join_type,
                        };

                        match Self::remove_subquery_from_predicate(predicate) {
                            Some(pred) => Ok(PlanNode::Filter {
                                predicate: pred,
                                input: Box::new(join_node),
                            }),
                            None => Ok(join_node),
                        }
                    }
                    None => Ok(PlanNode::Filter {
                        predicate: predicate.clone(),
                        input: Box::new(transformed_input),
                    }),
                }
            }

            PlanNode::Projection { expressions, input } => {
                match self.transform_projection_with_scalar_subquery(expressions, input) {
                    Some(transformed) => self.transform_node(&transformed),
                    None => Ok(PlanNode::Projection {
                        expressions: expressions.clone(),
                        input: Box::new(self.transform_node(input)?),
                    }),
                }
            }
            PlanNode::Join {
                left,
                right,
                on,
                join_type,
            } => Ok(PlanNode::Join {
                left: Box::new(self.transform_node(left)?),
                right: Box::new(self.transform_node(right)?),
                on: on.clone(),
                join_type: *join_type,
            }),
            PlanNode::Aggregate {
                group_by,
                aggregates,
                grouping_metadata,
                input,
            } => Ok(PlanNode::Aggregate {
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                grouping_metadata: grouping_metadata.clone(),
                input: Box::new(self.transform_node(input)?),
            }),
            PlanNode::Sort { order_by, input } => Ok(PlanNode::Sort {
                order_by: order_by.clone(),
                input: Box::new(self.transform_node(input)?),
            }),
            PlanNode::Limit {
                limit,
                offset,
                input,
            } => Ok(PlanNode::Limit {
                limit: *limit,
                offset: *offset,
                input: Box::new(self.transform_node(input)?),
            }),
            PlanNode::Distinct { input } => Ok(PlanNode::Distinct {
                input: Box::new(self.transform_node(input)?),
            }),

            _ => Ok(node.clone()),
        }
    }

    fn extract_subquery_join(&self, predicate: &Expr) -> Option<(JoinType, PlanNode, Expr)> {
        match predicate {
            Expr::Exists { plan, negated } => {
                let join_type = if *negated {
                    JoinType::Anti
                } else {
                    JoinType::Semi
                };

                self.extract_correlated_subquery(plan)
                    .map(|(subquery_plan, join_cond)| (join_type, subquery_plan, join_cond))
            }

            Expr::InSubquery {
                expr,
                plan,
                negated,
            } => {
                if *negated {
                    return None;
                }

                let join_type = JoinType::Semi;

                match self.get_subquery_output_column(plan) {
                    Some(subquery_output) => {
                        let join_cond = Expr::BinaryOp {
                            left: Box::new((**expr).clone()),
                            op: BinaryOp::Equal,
                            right: Box::new(subquery_output),
                        };
                        let subquery_plan = Self::unwrap_subquery_plan(plan);
                        Some((join_type, subquery_plan, join_cond))
                    }
                    None => None,
                }
            }

            _ => None,
        }
    }

    fn extract_correlated_subquery(&self, subquery: &PlanNode) -> Option<(PlanNode, Expr)> {
        match subquery {
            PlanNode::Projection { input, .. } => self.extract_correlated_subquery(input),
            PlanNode::Filter { predicate, input } => {
                let inner_tables = self.collect_tables(input);
                let (correlation_conditions, non_correlation_conditions) =
                    self.split_correlation_conditions(predicate, &inner_tables);

                if correlation_conditions.is_empty() {
                    return None;
                }

                let correlation_expr = self.combine_conditions(&correlation_conditions);

                let subquery_plan = if non_correlation_conditions.is_empty() {
                    (**input).clone()
                } else {
                    let filter_expr = self.combine_conditions(&non_correlation_conditions);
                    PlanNode::Filter {
                        predicate: filter_expr,
                        input: input.clone(),
                    }
                };

                Some((subquery_plan, correlation_expr))
            }
            PlanNode::Scan { .. } => Some((
                subquery.clone(),
                Expr::Literal(yachtsql_ir::expr::LiteralValue::Boolean(true)),
            )),
            PlanNode::Limit { input, .. } => self.extract_correlated_subquery(input),
            PlanNode::Distinct { input, .. } => self.extract_correlated_subquery(input),
            PlanNode::Sort { input, .. } => self.extract_correlated_subquery(input),
            _ => None,
        }
    }

    fn collect_tables(&self, plan: &PlanNode) -> std::collections::HashSet<String> {
        let mut tables = std::collections::HashSet::new();
        Self::collect_tables_recursive(plan, &mut tables);
        tables
    }

    fn collect_tables_recursive(plan: &PlanNode, tables: &mut std::collections::HashSet<String>) {
        match plan {
            PlanNode::Scan {
                table_name, alias, ..
            } => {
                if let Some(alias) = alias {
                    tables.insert(alias.clone());
                }
                tables.insert(table_name.clone());
            }
            _ => {
                for child in plan.children() {
                    Self::collect_tables_recursive(child, tables);
                }
            }
        }
    }

    fn split_correlation_conditions(
        &self,
        predicate: &Expr,
        inner_tables: &std::collections::HashSet<String>,
    ) -> (Vec<Expr>, Vec<Expr>) {
        let mut correlation = Vec::new();
        let mut non_correlation = Vec::new();
        Self::split_conditions_recursive(
            predicate,
            inner_tables,
            &mut correlation,
            &mut non_correlation,
        );
        (correlation, non_correlation)
    }

    fn split_conditions_recursive(
        expr: &Expr,
        inner_tables: &std::collections::HashSet<String>,
        correlation: &mut Vec<Expr>,
        non_correlation: &mut Vec<Expr>,
    ) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                Self::split_conditions_recursive(left, inner_tables, correlation, non_correlation);
                Self::split_conditions_recursive(right, inner_tables, correlation, non_correlation);
            }
            _ => {
                if Self::references_outer_table(expr, inner_tables) {
                    correlation.push(expr.clone());
                } else {
                    non_correlation.push(expr.clone());
                }
            }
        }
    }

    fn references_outer_table(
        expr: &Expr,
        inner_tables: &std::collections::HashSet<String>,
    ) -> bool {
        match expr {
            Expr::Column { table: Some(t), .. } => !inner_tables.contains(t),
            Expr::Column { table: None, .. } => false,
            Expr::BinaryOp { left, right, .. } => {
                Self::references_outer_table(left, inner_tables)
                    || Self::references_outer_table(right, inner_tables)
            }
            Expr::UnaryOp { expr, .. } => Self::references_outer_table(expr, inner_tables),
            Expr::Literal(_) => false,
            _ => false,
        }
    }

    fn combine_conditions(&self, conditions: &[Expr]) -> Expr {
        if conditions.is_empty() {
            return Expr::Literal(yachtsql_ir::expr::LiteralValue::Boolean(true));
        }
        if conditions.len() == 1 {
            return conditions[0].clone();
        }
        conditions
            .iter()
            .skip(1)
            .fold(conditions[0].clone(), |acc, cond| Expr::BinaryOp {
                left: Box::new(acc),
                op: BinaryOp::And,
                right: Box::new(cond.clone()),
            })
    }

    fn get_subquery_output_column(&self, subquery: &PlanNode) -> Option<Expr> {
        match subquery {
            PlanNode::Projection { expressions, .. } if !expressions.is_empty() => {
                Some(expressions[0].0.clone())
            }
            PlanNode::Scan { table_name, .. } => Some(Expr::Column {
                name: "id".to_string(),
                table: Some(table_name.clone()),
            }),
            _ => None,
        }
    }

    fn unwrap_subquery_plan(subquery: &PlanNode) -> PlanNode {
        match subquery {
            PlanNode::Projection { input, .. } => Self::unwrap_subquery_plan(input),
            _ => subquery.clone(),
        }
    }

    fn analyze_correlated_scalar_subquery(
        &self,
        subquery: &PlanNode,
        result_alias: &str,
        outer_tables: &[String],
    ) -> Option<CorrelatedScalarSubquery> {
        match subquery {
            PlanNode::Projection { expressions, input } => {
                let (aggregate_expr, _alias) = expressions.first()?;

                if !matches!(aggregate_expr, Expr::Aggregate { .. }) {
                    return None;
                }

                self.extract_correlation_from_filter(
                    input,
                    aggregate_expr.clone(),
                    result_alias,
                    outer_tables,
                )
            }
            PlanNode::Aggregate {
                aggregates, input, ..
            } => {
                let aggregate_expr = aggregates.first()?.clone();
                self.extract_correlation_from_filter(
                    input,
                    aggregate_expr,
                    result_alias,
                    outer_tables,
                )
            }
            _ => None,
        }
    }

    fn extract_correlation_from_filter(
        &self,
        node: &PlanNode,
        aggregate_expr: Expr,
        result_alias: &str,
        outer_tables: &[String],
    ) -> Option<CorrelatedScalarSubquery> {
        match node {
            PlanNode::Filter { predicate, input } => {
                match self.extract_correlation_columns(predicate, outer_tables) {
                    Some((outer_col, inner_col)) => {
                        let scan = Self::find_scan_node(input)?;
                        Some(CorrelatedScalarSubquery {
                            aggregate_expr,
                            inner_alias: inner_col.0,
                            inner_column: inner_col.1,
                            outer_alias: outer_col.0,
                            outer_column: outer_col.1,
                            subquery_scan: scan,
                            result_alias: result_alias.to_string(),
                        })
                    }
                    None => None,
                }
            }
            _ => None,
        }
    }

    fn extract_correlation_columns(
        &self,
        predicate: &Expr,
        outer_tables: &[String],
    ) -> Option<((String, String), (String, String))> {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Equal,
                right,
            } => {
                let left_col = self.extract_qualified_column(left)?;
                let right_col = self.extract_qualified_column(right)?;

                if outer_tables.contains(&left_col.0) {
                    Some((left_col, right_col))
                } else if outer_tables.contains(&right_col.0) {
                    Some((right_col, left_col))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn extract_qualified_column(&self, expr: &Expr) -> Option<(String, String)> {
        match expr {
            Expr::Column { name, table } => Some((table.clone()?, name.clone())),
            _ => None,
        }
    }

    fn find_scan_node(node: &PlanNode) -> Option<PlanNode> {
        match node {
            PlanNode::Scan { .. } => Some(node.clone()),
            PlanNode::Filter { input, .. }
            | PlanNode::Projection { input, .. }
            | PlanNode::Aggregate { input, .. } => Self::find_scan_node(input),
            _ => None,
        }
    }

    fn get_outer_table_aliases(&self, plan: &PlanNode) -> Vec<String> {
        let mut aliases = Vec::new();
        Self::collect_table_aliases(plan, &mut aliases);
        aliases
    }

    fn collect_table_aliases(plan: &PlanNode, aliases: &mut Vec<String>) {
        match plan {
            PlanNode::Scan {
                table_name, alias, ..
            } => {
                aliases.push(alias.clone().unwrap_or_else(|| table_name.clone()));
            }
            PlanNode::SubqueryScan { alias, .. } => {
                aliases.push(alias.clone());
            }
            PlanNode::Join { left, right, .. } | PlanNode::LateralJoin { left, right, .. } => {
                Self::collect_table_aliases(left, aliases);
                Self::collect_table_aliases(right, aliases);
            }
            PlanNode::Filter { input, .. }
            | PlanNode::Projection { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Sort { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Distinct { input } => {
                Self::collect_table_aliases(input, aliases);
            }
            _ => {}
        }
    }

    fn transform_projection_with_scalar_subquery(
        &self,
        expressions: &[(Expr, Option<String>)],
        input: &PlanNode,
    ) -> Option<PlanNode> {
        let outer_tables = self.get_outer_table_aliases(input);
        let mut decorrelations: Vec<(usize, CorrelatedScalarSubquery)> = Vec::new();

        for (idx, (expr, alias)) in expressions.iter().enumerate() {
            if let Expr::ScalarSubquery { subquery } = expr {
                let result_alias = alias
                    .clone()
                    .unwrap_or_else(|| format!("__scalar_subquery_{}", idx));
                if let Some(info) =
                    self.analyze_correlated_scalar_subquery(subquery, &result_alias, &outer_tables)
                {
                    decorrelations.push((idx, info));
                }
            }
        }

        if decorrelations.is_empty() {
            return None;
        }

        let (subquery_idx, info) = decorrelations.into_iter().next()?;

        let agg_subquery = self.build_aggregate_subquery(&info);
        let agg_alias = format!("__agg_{}", info.result_alias);

        let join_condition = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                name: info.outer_column.clone(),
                table: Some(info.outer_alias.clone()),
            }),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Column {
                name: info.inner_column.clone(),
                table: Some(agg_alias.clone()),
            }),
        };

        let joined = PlanNode::Join {
            left: Box::new(input.clone()),
            right: Box::new(PlanNode::SubqueryScan {
                subquery: Box::new(agg_subquery),
                alias: agg_alias.clone(),
            }),
            on: join_condition,
            join_type: JoinType::Left,
        };

        let new_expressions: Vec<(Expr, Option<String>)> = expressions
            .iter()
            .enumerate()
            .map(|(idx, (expr, alias))| {
                if idx == subquery_idx {
                    (
                        Expr::Column {
                            name: info.result_alias.clone(),
                            table: Some(agg_alias.clone()),
                        },
                        alias.clone(),
                    )
                } else {
                    (expr.clone(), alias.clone())
                }
            })
            .collect();

        Some(PlanNode::Projection {
            expressions: new_expressions,
            input: Box::new(joined),
        })
    }

    fn build_aggregate_subquery(&self, info: &CorrelatedScalarSubquery) -> PlanNode {
        let group_by_col = Expr::Column {
            name: info.inner_column.clone(),
            table: Some(info.inner_alias.clone()),
        };

        let aggregate_node = PlanNode::Aggregate {
            group_by: vec![group_by_col.clone()],
            aggregates: vec![info.aggregate_expr.clone()],
            input: Box::new(info.subquery_scan.clone()),
            grouping_metadata: None,
        };

        let agg_output_name = Self::compute_aggregate_field_name(&info.aggregate_expr);
        let agg_output_ref = Expr::Column {
            name: agg_output_name,
            table: None,
        };

        PlanNode::Projection {
            expressions: vec![
                (group_by_col, Some(info.inner_column.clone())),
                (agg_output_ref, Some(info.result_alias.clone())),
            ],
            input: Box::new(aggregate_node),
        }
    }

    fn compute_aggregate_field_name(expr: &Expr) -> String {
        match expr {
            Expr::Aggregate { name, args, .. } => {
                let arg_str = if args.is_empty()
                    || args.first().is_some_and(|e| matches!(e, Expr::Wildcard))
                {
                    "*".to_string()
                } else {
                    let arg_strs: Vec<String> = args
                        .iter()
                        .map(|arg| match arg {
                            Expr::Column { name: col_name, .. } => col_name.clone(),
                            Expr::Wildcard => "*".to_string(),
                            _ => "...".to_string(),
                        })
                        .collect();
                    arg_strs.join(", ")
                };
                format!("{}({})", name.as_str(), arg_str)
            }
            _ => "agg_0".to_string(),
        }
    }

    fn remove_subquery_from_predicate(predicate: &Expr) -> Option<Expr> {
        match predicate {
            Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::TupleInSubquery { .. } => None,
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let left_cleaned = Self::remove_subquery_from_predicate(left);
                let right_cleaned = Self::remove_subquery_from_predicate(right);

                match (left_cleaned, right_cleaned) {
                    (None, None) => None,
                    (Some(l), None) => Some(l),
                    (None, Some(r)) => Some(r),
                    (Some(l), Some(r)) => Some(Expr::BinaryOp {
                        left: Box::new(l),
                        op: BinaryOp::And,
                        right: Box::new(r),
                    }),
                }
            }
            _ => Some(predicate.clone()),
        }
    }
}

impl Default for SubqueryFlattening {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for SubqueryFlattening {
    fn name(&self) -> &str {
        "subquery_flattening"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        if self.detect_only {
            return Ok(None);
        }

        let subquery_count = self.count_flattenable_subqueries(plan);

        if subquery_count == 0 {
            return Ok(None);
        }

        let transformed = self.transform_node(&plan.root)?;

        if transformed != *plan.root {
            debug_print::debug_eprintln!("[optimizer::subquery_flattening] plan transformed");
            Ok(Some(LogicalPlan::new(transformed)))
        } else {
            debug_print::debug_eprintln!(
                "[optimizer::subquery_flattening] no transformation applied"
            );
            Ok(None)
        }
    }

    fn is_applicable(&self, plan: &LogicalPlan) -> bool {
        self.count_flattenable_subqueries(plan) > 0
    }
}

struct SubqueryCounter {
    exists_count: usize,
    in_subquery_count: usize,
    scalar_subquery_count: usize,
}

impl SubqueryCounter {
    fn new() -> Self {
        Self {
            exists_count: 0,
            in_subquery_count: 0,
            scalar_subquery_count: 0,
        }
    }

    fn count(&self) -> usize {
        self.exists_count + self.in_subquery_count + self.scalar_subquery_count
    }

    fn visit_plan(&mut self, plan: &LogicalPlan) {
        self.visit_node(&plan.root);
    }

    fn visit_node(&mut self, node: &PlanNode) {
        match node {
            PlanNode::Filter { predicate, input } => {
                self.visit_expr(predicate);
                self.visit_node(input);
            }
            PlanNode::Projection { expressions, input } => {
                for (expr, _alias) in expressions {
                    self.visit_expr(expr);
                }
                self.visit_node(input);
            }
            _ => {
                for child in node.children() {
                    self.visit_node(child);
                }
            }
        }
    }

    fn visit_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Exists { .. } => {
                self.exists_count += 1;
            }
            Expr::InSubquery { .. } | Expr::TupleInSubquery { .. } => {
                self.in_subquery_count += 1;
            }
            Expr::ScalarSubquery { .. } => {
                self.scalar_subquery_count += 1;
            }
            Expr::BinaryOp { left, right, .. } => {
                self.visit_expr(left);
                self.visit_expr(right);
            }
            Expr::UnaryOp { expr, .. } => {
                self.visit_expr(expr);
            }
            Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                for (cond, result) in when_then {
                    self.visit_expr(cond);
                    self.visit_expr(result);
                }
                if let Some(else_val) = else_expr {
                    self.visit_expr(else_val);
                }
            }
            _ => {}
        }
    }
}
