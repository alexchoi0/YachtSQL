use std::collections::HashMap;

use yachtsql_core::error::Result;

use crate::optimizer::expr::Expr;
use crate::optimizer::plan::{LogicalPlan, PlanNode};
use crate::optimizer::rule::OptimizationRule;

pub struct CommonSubexpressionElimination;

impl CommonSubexpressionElimination {
    pub fn new() -> Self {
        Self
    }

    fn expr_signature(expr: &Expr) -> String {
        match expr {
            Expr::Column { name, table } => {
                if let Some(t) = table {
                    format!("col:{}.{}", t, name)
                } else {
                    format!("col:{}", name)
                }
            }
            Expr::Literal(lit) => format!("lit:{:?}", lit),
            Expr::BinaryOp { left, op, right } => {
                format!(
                    "binop:{:?}({},{})",
                    op,
                    Self::expr_signature(left),
                    Self::expr_signature(right)
                )
            }
            Expr::UnaryOp { op, expr: inner } => {
                format!("unop:{:?}({})", op, Self::expr_signature(inner))
            }
            Expr::Function { name, args } => {
                let arg_sigs: Vec<_> = args.iter().map(Self::expr_signature).collect();
                format!("fn:{}({})", name, arg_sigs.join(","))
            }
            Expr::Aggregate {
                name,
                args,
                distinct,
                ..
            } => {
                let arg_sigs: Vec<_> = args.iter().map(Self::expr_signature).collect();
                format!("agg:{}({}):{}", name, arg_sigs.join(","), distinct)
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let op_sig = operand
                    .as_ref()
                    .map(|o| Self::expr_signature(o))
                    .unwrap_or_else(|| "none".to_string());
                let when_then_sigs: Vec<_> = when_then
                    .iter()
                    .map(|(w, t)| {
                        format!(
                            "when({})then({})",
                            Self::expr_signature(w),
                            Self::expr_signature(t)
                        )
                    })
                    .collect();
                let else_sig = else_expr
                    .as_ref()
                    .map(|e| Self::expr_signature(e))
                    .unwrap_or_else(|| "none".to_string());
                format!(
                    "case:{}:{}:else:{}",
                    op_sig,
                    when_then_sigs.join(":"),
                    else_sig
                )
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let list_sigs: Vec<_> = list.iter().map(Self::expr_signature).collect();
                format!(
                    "inlist:{}({},{})",
                    if *negated { "not" } else { "" },
                    Self::expr_signature(expr),
                    list_sigs.join(",")
                )
            }
            Expr::TupleInList {
                tuple,
                list,
                negated,
            } => {
                let tuple_sigs: Vec<_> = tuple.iter().map(Self::expr_signature).collect();
                let list_sigs: Vec<_> = list
                    .iter()
                    .map(|t| {
                        let t_sigs: Vec<_> = t.iter().map(Self::expr_signature).collect();
                        format!("({})", t_sigs.join(","))
                    })
                    .collect();
                format!(
                    "tupleinlist:{}(({}),[{}])",
                    if *negated { "not" } else { "" },
                    tuple_sigs.join(","),
                    list_sigs.join(",")
                )
            }
            Expr::TupleInSubquery { .. } => "tuple_in_subquery".to_string(),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                format!(
                    "between:{}({},{},{})",
                    if *negated { "not" } else { "" },
                    Self::expr_signature(expr),
                    Self::expr_signature(low),
                    Self::expr_signature(high)
                )
            }
            Expr::Cast { expr, data_type } => {
                format!("cast:({}):{:?}", Self::expr_signature(expr), data_type)
            }
            Expr::TryCast { expr, data_type } => {
                format!("trycast:({}):{:?}", Self::expr_signature(expr), data_type)
            }
            Expr::Subquery { .. } => "subquery".to_string(),
            Expr::Exists { .. } => "exists".to_string(),
            Expr::InSubquery { .. } => "in_subquery".to_string(),
            Expr::InTable {
                expr,
                table_name,
                negated,
            } => {
                format!(
                    "intable:{}({},{})",
                    if *negated { "not" } else { "" },
                    Self::expr_signature(expr),
                    table_name
                )
            }
            Expr::Wildcard | Expr::QualifiedWildcard { .. } | Expr::ExpressionWildcard { .. } => {
                "wildcard".to_string()
            }
            Expr::Tuple(exprs) => {
                let elem_sigs: Vec<_> = exprs.iter().map(Self::expr_signature).collect();
                format!("tuple:({})", elem_sigs.join(","))
            }
            Expr::Grouping { column } => format!("grouping:{}", column),
            Expr::GroupingId { columns } => format!("grouping_id:{}", columns.join(",")),
            Expr::Excluded { column } => format!("excluded:{}", column),
            Expr::WindowFunction {
                name,
                args,
                partition_by,
                order_by,
                frame_units,
                exclude,
                ..
            } => {
                let arg_sigs: Vec<_> = args.iter().map(Self::expr_signature).collect();
                let part_sigs: Vec<_> = partition_by.iter().map(Self::expr_signature).collect();
                let order_sigs: Vec<_> = order_by
                    .iter()
                    .map(|o| format!("{}:{:?}", Self::expr_signature(&o.expr), o.asc))
                    .collect();
                format!(
                    "window:{}({}):part({}):order({}):frame({:?}):exclude({:?})",
                    name,
                    arg_sigs.join(","),
                    part_sigs.join(","),
                    order_sigs.join(","),
                    frame_units,
                    exclude
                )
            }
            Expr::ArrayIndex { array, index, safe } => {
                format!(
                    "arrayindex:{}[{}]:safe={}",
                    Self::expr_signature(array),
                    Self::expr_signature(index),
                    safe
                )
            }
            Expr::AnyOp {
                left, compare_op, ..
            } => {
                format!("any:{:?}:{}", compare_op, Self::expr_signature(left))
            }
            Expr::AllOp {
                left, compare_op, ..
            } => {
                format!("all:{:?}:{}", compare_op, Self::expr_signature(left))
            }
            Expr::ScalarSubquery { .. } => "scalarsubquery:unique".to_string(),
            Expr::ArraySubquery { .. } => "arraysubquery:unique".to_string(),
            Expr::ArraySlice { array, start, end } => {
                format!(
                    "arrayslice:{}[{}:{}]",
                    Self::expr_signature(array),
                    start
                        .as_ref()
                        .map(|s| Self::expr_signature(s))
                        .unwrap_or_else(|| "".to_string()),
                    end.as_ref()
                        .map(|e| Self::expr_signature(e))
                        .unwrap_or_else(|| "".to_string())
                )
            }
            Expr::StructLiteral { fields } => {
                let field_sigs: Vec<_> = fields
                    .iter()
                    .map(|field| format!("{}:{}", field.name, Self::expr_signature(&field.expr)))
                    .collect();
                format!("struct({})", field_sigs.join(","))
            }
            Expr::StructFieldAccess { expr, field } => {
                format!("structfield:{}.{}", Self::expr_signature(expr), field)
            }
            Expr::TupleElementAccess { expr, index } => {
                format!("tupleelem:{}.{}", Self::expr_signature(expr), index)
            }
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => {
                format!(
                    "isdistinct:{}({},{})",
                    if *negated { "not" } else { "" },
                    Self::expr_signature(left),
                    Self::expr_signature(right)
                )
            }
            Expr::Lambda { params, body } => {
                format!(
                    "lambda({}):{}",
                    params.join(","),
                    Self::expr_signature(body)
                )
            }
        }
    }

    fn find_common_subexpressions(exprs: &[(Expr, Option<String>)]) -> HashMap<String, Expr> {
        let mut expr_counts: HashMap<String, (usize, Expr)> = HashMap::new();

        for (expr, _) in exprs {
            Self::count_subexpressions(expr, &mut expr_counts);
        }

        expr_counts
            .into_iter()
            .filter_map(|(sig, (count, expr))| {
                if count > 1 && Self::is_non_trivial(&expr) {
                    Some((sig, expr))
                } else {
                    None
                }
            })
            .collect()
    }

    fn count_subexpressions(expr: &Expr, counts: &mut HashMap<String, (usize, Expr)>) {
        let sig = Self::expr_signature(expr);

        counts
            .entry(sig)
            .and_modify(|(count, _)| *count += 1)
            .or_insert((1, expr.clone()));

        match expr {
            Expr::BinaryOp { left, right, .. } => {
                Self::count_subexpressions(left, counts);
                Self::count_subexpressions(right, counts);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                Self::count_subexpressions(inner, counts);
            }
            Expr::Function { args, .. } | Expr::Aggregate { args, .. } => {
                for arg in args {
                    Self::count_subexpressions(arg, counts);
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    Self::count_subexpressions(op, counts);
                }
                for (when, then) in when_then {
                    Self::count_subexpressions(when, counts);
                    Self::count_subexpressions(then, counts);
                }
                if let Some(else_e) = else_expr {
                    Self::count_subexpressions(else_e, counts);
                }
            }
            Expr::InList { expr, list, .. } => {
                Self::count_subexpressions(expr, counts);
                for item in list {
                    Self::count_subexpressions(item, counts);
                }
            }
            Expr::TupleInList { tuple, list, .. } => {
                for t_expr in tuple {
                    Self::count_subexpressions(t_expr, counts);
                }
                for tuple_list in list {
                    for item in tuple_list {
                        Self::count_subexpressions(item, counts);
                    }
                }
            }
            Expr::TupleInSubquery { tuple, .. } => {
                for t_expr in tuple {
                    Self::count_subexpressions(t_expr, counts);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::count_subexpressions(expr, counts);
                Self::count_subexpressions(low, counts);
                Self::count_subexpressions(high, counts);
            }
            Expr::Cast { expr, .. } => {
                Self::count_subexpressions(expr, counts);
            }
            Expr::TryCast { expr, .. } => {
                Self::count_subexpressions(expr, counts);
            }
            Expr::Subquery { .. } => {}
            Expr::Exists { .. } => {}
            Expr::InSubquery { expr, .. } => {
                Self::count_subexpressions(expr, counts);
            }
            Expr::InTable { expr, .. } => {
                Self::count_subexpressions(expr, counts);
            }
            Expr::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    Self::count_subexpressions(arg, counts);
                }
                for part in partition_by {
                    Self::count_subexpressions(part, counts);
                }
                for order in order_by {
                    Self::count_subexpressions(&order.expr, counts);
                }
            }
            Expr::ArrayIndex { array, index, .. } => {
                Self::count_subexpressions(array, counts);
                Self::count_subexpressions(index, counts);
            }
            Expr::AnyOp { left, .. } | Expr::AllOp { left, .. } => {
                Self::count_subexpressions(left, counts);
            }
            Expr::ScalarSubquery { .. } => {}
            Expr::ArraySubquery { .. } => {}
            Expr::ArraySlice { array, start, end } => {
                Self::count_subexpressions(array, counts);
                if let Some(s) = start {
                    Self::count_subexpressions(s, counts);
                }
                if let Some(e) = end {
                    Self::count_subexpressions(e, counts);
                }
            }
            Expr::StructLiteral { fields } => {
                for field in fields {
                    Self::count_subexpressions(&field.expr, counts);
                }
            }
            Expr::StructFieldAccess { expr, .. } => {
                Self::count_subexpressions(expr, counts);
            }
            Expr::TupleElementAccess { expr, .. } => {
                Self::count_subexpressions(expr, counts);
            }
            Expr::IsDistinctFrom { left, right, .. } => {
                Self::count_subexpressions(left, counts);
                Self::count_subexpressions(right, counts);
            }
            Expr::Lambda { body, .. } => {
                Self::count_subexpressions(body, counts);
            }
            Expr::ExpressionWildcard { expr } => {
                Self::count_subexpressions(expr, counts);
            }
            Expr::Column { .. }
            | Expr::Literal(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::Grouping { .. }
            | Expr::GroupingId { .. }
            | Expr::Excluded { .. } => {}
            Expr::Tuple(exprs) => {
                for e in exprs {
                    Self::count_subexpressions(e, counts);
                }
            }
        }
    }

    fn is_non_trivial(expr: &Expr) -> bool {
        match expr {
            Expr::Column { .. }
            | Expr::Literal(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::ExpressionWildcard { .. }
            | Expr::Grouping { .. }
            | Expr::GroupingId { .. }
            | Expr::Excluded { .. }
            | Expr::Tuple(_) => false,

            Expr::BinaryOp { .. }
            | Expr::UnaryOp { .. }
            | Expr::Function { .. }
            | Expr::Aggregate { .. }
            | Expr::Case { .. }
            | Expr::InList { .. }
            | Expr::TupleInList { .. }
            | Expr::TupleInSubquery { .. }
            | Expr::InTable { .. }
            | Expr::Between { .. }
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::Subquery { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::WindowFunction { .. }
            | Expr::ArrayIndex { .. }
            | Expr::ArraySlice { .. }
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. }
            | Expr::ScalarSubquery { .. }
            | Expr::ArraySubquery { .. }
            | Expr::StructLiteral { .. }
            | Expr::StructFieldAccess { .. }
            | Expr::TupleElementAccess { .. }
            | Expr::IsDistinctFrom { .. }
            | Expr::Lambda { .. } => true,
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn optimize_node(&self, node: &PlanNode) -> Option<PlanNode> {
        match node {
            PlanNode::Projection { expressions, input } => {
                let _common = Self::find_common_subexpressions(expressions);

                self.optimize_node(input)
                    .map(|optimized_input| PlanNode::Projection {
                        expressions: expressions.clone(),
                        input: Box::new(optimized_input),
                    })
            }
            PlanNode::Filter { predicate, input } => {
                self.optimize_node(input)
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
            } => self
                .optimize_node(input)
                .map(|optimized_input| PlanNode::Aggregate {
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    input: Box::new(optimized_input),
                    grouping_metadata: None,
                }),
            PlanNode::Join {
                left,
                right,
                on,
                join_type,
                using_columns,
            } => {
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

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
                let left_opt = self.optimize_node(left);
                let right_opt = self.optimize_node(right);

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
            PlanNode::LimitPercent {
                percent,
                offset,
                with_ties,
                input,
            } => self
                .optimize_node(input)
                .map(|optimized_input| PlanNode::LimitPercent {
                    percent: *percent,
                    offset: *offset,
                    with_ties: *with_ties,
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

impl Default for CommonSubexpressionElimination {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for CommonSubexpressionElimination {
    fn name(&self) -> &str {
        "common_subexpression_elimination"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(self.optimize_node(plan.root()).map(LogicalPlan::new))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::expr::{BinaryOp, LiteralValue};

    #[test]
    fn test_expr_signature_column() {
        let expr = Expr::column("x");
        let sig = CommonSubexpressionElimination::expr_signature(&expr);
        assert_eq!(sig, "col:x");
    }

    #[test]
    fn test_expr_signature_binary_op() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::column("x")),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };
        let sig = CommonSubexpressionElimination::expr_signature(&expr);
        assert!(sig.contains("binop"));
        assert!(sig.contains("Add"));
    }

    #[test]
    fn test_is_non_trivial() {
        assert!(!CommonSubexpressionElimination::is_non_trivial(
            &Expr::column("x")
        ));
        assert!(!CommonSubexpressionElimination::is_non_trivial(
            &Expr::Literal(LiteralValue::Int64(5))
        ));
        assert!(!CommonSubexpressionElimination::is_non_trivial(
            &Expr::Wildcard
        ));
        assert!(!CommonSubexpressionElimination::is_non_trivial(
            &Expr::QualifiedWildcard {
                qualifier: "t".to_string()
            }
        ));

        let binary_op = Expr::BinaryOp {
            left: Box::new(Expr::column("x")),
            op: BinaryOp::Add,
            right: Box::new(Expr::column("y")),
        };
        assert!(CommonSubexpressionElimination::is_non_trivial(&binary_op));
    }

    #[test]
    fn test_find_common_subexpressions() {
        let common_expr = Expr::BinaryOp {
            left: Box::new(Expr::column("x")),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };

        let exprs = vec![
            (common_expr.clone(), Some("a".to_string())),
            (
                Expr::BinaryOp {
                    left: Box::new(common_expr.clone()),
                    op: BinaryOp::Multiply,
                    right: Box::new(Expr::Literal(LiteralValue::Int64(2))),
                },
                Some("b".to_string()),
            ),
            (common_expr.clone(), Some("c".to_string())),
        ];

        let common = CommonSubexpressionElimination::find_common_subexpressions(&exprs);

        assert!(!common.is_empty());
    }

    #[test]
    fn test_no_common_subexpressions() {
        let exprs = vec![
            (Expr::column("x"), Some("a".to_string())),
            (Expr::column("y"), Some("b".to_string())),
            (Expr::column("z"), Some("c".to_string())),
        ];

        let common = CommonSubexpressionElimination::find_common_subexpressions(&exprs);

        assert!(common.is_empty());
    }

    #[test]
    fn test_rule_application() {
        let rule = CommonSubexpressionElimination::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test".to_string(),
            projection: None,
            only: false,
            final_modifier: false,
        };

        let plan = LogicalPlan::new(scan);
        let optimized = rule.optimize(&plan).expect("optimization should succeed");

        assert!(optimized.is_none());
    }
}
