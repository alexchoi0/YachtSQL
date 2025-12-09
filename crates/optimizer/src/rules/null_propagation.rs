use yachtsql_core::error::Result;

use crate::optimizer::expr::{BinaryOp, Expr, LiteralValue, UnaryOp};
use crate::optimizer::plan::{LogicalPlan, PlanNode};
use crate::optimizer::rule::OptimizationRule;

pub struct NullPropagation;

impl NullPropagation {
    pub fn new() -> Self {
        Self
    }

    fn handles_null_explicitly(func_name: &yachtsql_ir::FunctionName) -> bool {
        use yachtsql_ir::FunctionName;
        matches!(
            func_name,
            FunctionName::Coalesce
                | FunctionName::Ifnull
                | FunctionName::Nvl
                | FunctionName::Isnull
                | FunctionName::ArrayAgg
                | FunctionName::Collect
                | FunctionName::If
                | FunctionName::Iif
                | FunctionName::Custom(_)
        )
    }

    fn all_args_are_null(args: &[Expr]) -> bool {
        !args.is_empty() && args.iter().all(Self::is_always_null)
    }

    fn is_always_null(expr: &Expr) -> bool {
        match expr {
            Expr::Literal(LiteralValue::Null) => true,

            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => Self::is_always_null(left) || Self::is_always_null(right),

            Expr::BinaryOp {
                left,
                op:
                    BinaryOp::Add
                    | BinaryOp::Subtract
                    | BinaryOp::Multiply
                    | BinaryOp::Divide
                    | BinaryOp::Modulo,
                right,
            } => Self::is_always_null(left) || Self::is_always_null(right),

            Expr::BinaryOp {
                left,
                op:
                    BinaryOp::Equal
                    | BinaryOp::NotEqual
                    | BinaryOp::LessThan
                    | BinaryOp::LessThanOrEqual
                    | BinaryOp::GreaterThan
                    | BinaryOp::GreaterThanOrEqual
                    | BinaryOp::Like
                    | BinaryOp::NotLike
                    | BinaryOp::In
                    | BinaryOp::NotIn,
                right,
            } => Self::is_always_null(left) || Self::is_always_null(right),

            Expr::BinaryOp {
                op: BinaryOp::Or, ..
            } => false,

            Expr::UnaryOp { op, expr: inner } => match op {
                UnaryOp::IsNull | UnaryOp::IsNotNull => false,
                _ => Self::is_always_null(inner),
            },

            Expr::Function { name, args } => {
                !Self::handles_null_explicitly(name) && Self::all_args_are_null(args)
            }
            _ => false,
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn simplify_expr(&self, expr: &Expr) -> Expr {
        match expr {
            _ if Self::is_always_null(expr) => Expr::Literal(LiteralValue::Null),

            Expr::BinaryOp { left, op, right } => {
                let simplified_left = self.simplify_expr(left);
                let simplified_right = self.simplify_expr(right);

                if Self::is_always_null(&simplified_left) || Self::is_always_null(&simplified_right)
                {
                    match op {
                        BinaryOp::Add
                        | BinaryOp::Subtract
                        | BinaryOp::Multiply
                        | BinaryOp::Divide
                        | BinaryOp::Modulo
                        | BinaryOp::BitwiseAnd
                        | BinaryOp::BitwiseOr
                        | BinaryOp::BitwiseXor
                        | BinaryOp::ShiftLeft
                        | BinaryOp::ShiftRight
                        | BinaryOp::Concat
                        | BinaryOp::HashMinus => return Expr::Literal(LiteralValue::Null),

                        BinaryOp::Equal
                        | BinaryOp::NotEqual
                        | BinaryOp::LessThan
                        | BinaryOp::LessThanOrEqual
                        | BinaryOp::GreaterThan
                        | BinaryOp::GreaterThanOrEqual
                        | BinaryOp::Like
                        | BinaryOp::NotLike
                        | BinaryOp::ILike
                        | BinaryOp::NotILike
                        | BinaryOp::SimilarTo
                        | BinaryOp::NotSimilarTo
                        | BinaryOp::RegexMatch
                        | BinaryOp::RegexNotMatch
                        | BinaryOp::RegexMatchI
                        | BinaryOp::RegexNotMatchI
                        | BinaryOp::VectorL2Distance
                        | BinaryOp::VectorInnerProduct
                        | BinaryOp::VectorCosineDistance
                        | BinaryOp::ArrayContains
                        | BinaryOp::ArrayContainedBy
                        | BinaryOp::ArrayOverlap
                        | BinaryOp::GeometricDistance
                        | BinaryOp::GeometricContains
                        | BinaryOp::GeometricContainedBy
                        | BinaryOp::GeometricOverlap
                        | BinaryOp::RangeAdjacent
                        | BinaryOp::RangeStrictlyLeft
                        | BinaryOp::RangeStrictlyRight
                        | BinaryOp::InetContains
                        | BinaryOp::InetContainedBy
                        | BinaryOp::InetContainsOrEqual
                        | BinaryOp::InetContainedByOrEqual
                        | BinaryOp::InetOverlap => return Expr::Literal(LiteralValue::Null),

                        BinaryOp::In | BinaryOp::NotIn => return Expr::Literal(LiteralValue::Null),

                        BinaryOp::And => return Expr::Literal(LiteralValue::Null),

                        BinaryOp::Or => {
                            if Self::is_always_null(&simplified_left)
                                && Self::is_always_null(&simplified_right)
                            {
                                return Expr::Literal(LiteralValue::Null);
                            }
                        }
                    }
                }

                Expr::BinaryOp {
                    left: Box::new(simplified_left),
                    op: *op,
                    right: Box::new(simplified_right),
                }
            }

            Expr::UnaryOp { op, expr: inner } => {
                let simplified_inner = self.simplify_expr(inner);

                if Self::is_always_null(&simplified_inner) {
                    match op {
                        UnaryOp::Not => return Expr::Literal(LiteralValue::Null),
                        UnaryOp::Negate => return Expr::Literal(LiteralValue::Null),
                        UnaryOp::Plus => return Expr::Literal(LiteralValue::Null),
                        UnaryOp::BitwiseNot => return Expr::Literal(LiteralValue::Null),
                        UnaryOp::IsNull => {
                            return Expr::Literal(LiteralValue::Boolean(true));
                        }
                        UnaryOp::IsNotNull => {
                            return Expr::Literal(LiteralValue::Boolean(false));
                        }
                    }
                }

                match (op, &simplified_inner) {
                    (UnaryOp::IsNull, Expr::Literal(LiteralValue::Null)) => {
                        return Expr::Literal(LiteralValue::Boolean(true));
                    }
                    (UnaryOp::IsNull, Expr::Literal(_)) => {
                        return Expr::Literal(LiteralValue::Boolean(false));
                    }
                    (UnaryOp::IsNotNull, Expr::Literal(LiteralValue::Null)) => {
                        return Expr::Literal(LiteralValue::Boolean(false));
                    }
                    (UnaryOp::IsNotNull, Expr::Literal(_)) => {
                        return Expr::Literal(LiteralValue::Boolean(true));
                    }
                    _ => {}
                }

                Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(simplified_inner),
                }
            }

            Expr::Function { name, args } => {
                let simplified_args: Vec<_> =
                    args.iter().map(|arg| self.simplify_expr(arg)).collect();

                if matches!(name, yachtsql_ir::FunctionName::Coalesce) {
                    let mut first_non_literal = None;
                    for (idx, arg) in simplified_args.iter().enumerate() {
                        if matches!(arg, Expr::Literal(LiteralValue::Null)) {
                            continue;
                        } else if matches!(arg, Expr::Literal(_)) {
                            return arg.clone();
                        } else if first_non_literal.is_none() {
                            first_non_literal = Some(idx);
                        }
                    }

                    if let Some(first_idx) = first_non_literal {
                        let remaining_args: Vec<_> = simplified_args[first_idx..].to_vec();
                        return Expr::Function {
                            name: name.clone(),
                            args: remaining_args,
                        };
                    }

                    return Expr::Literal(LiteralValue::Null);
                }

                if matches!(
                    name,
                    yachtsql_ir::FunctionName::Ifnull
                        | yachtsql_ir::FunctionName::Nvl
                        | yachtsql_ir::FunctionName::Isnull
                ) && simplified_args.len() == 2
                    && Self::is_always_null(&simplified_args[0])
                {
                    return simplified_args[1].clone();
                }

                if !Self::handles_null_explicitly(name) && Self::all_args_are_null(&simplified_args)
                {
                    return Expr::Literal(LiteralValue::Null);
                }

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

            Expr::StructFieldAccess { expr, field } => Expr::StructFieldAccess {
                expr: Box::new(self.simplify_expr(expr)),
                field: field.clone(),
            },

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
                grouping_metadata,
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
                        grouping_metadata: grouping_metadata.clone(),
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
            } => {
                let simplified_expr = self.simplify_expr(aggregate_expr);
                let changed = simplified_expr != *aggregate_expr;
                let optimized_input = self.optimize_node(input);

                if changed || optimized_input.is_some() {
                    Some(PlanNode::Pivot {
                        input: Box::new(optimized_input.unwrap_or_else(|| input.as_ref().clone())),
                        aggregate_expr: simplified_expr,
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

impl Default for NullPropagation {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for NullPropagation {
    fn name(&self) -> &str {
        "null_propagation"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(self.optimize_node(plan.root()).map(LogicalPlan::new))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_in_arithmetic() {
        let rule = NullPropagation::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Null)),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Null));
    }

    #[test]
    fn test_null_in_comparison() {
        let rule = NullPropagation::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Null)),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Null));
    }

    #[test]
    fn test_is_null_on_null() {
        let rule = NullPropagation::new();

        let expr = Expr::UnaryOp {
            op: UnaryOp::IsNull,
            expr: Box::new(Expr::Literal(LiteralValue::Null)),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Boolean(true)));
    }

    #[test]
    fn test_is_null_on_literal() {
        let rule = NullPropagation::new();

        let expr = Expr::UnaryOp {
            op: UnaryOp::IsNull,
            expr: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Boolean(false)));
    }

    #[test]
    fn test_is_not_null_on_null() {
        let rule = NullPropagation::new();

        let expr = Expr::UnaryOp {
            op: UnaryOp::IsNotNull,
            expr: Box::new(Expr::Literal(LiteralValue::Null)),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Boolean(false)));
    }

    #[test]
    fn test_is_not_null_on_literal() {
        let rule = NullPropagation::new();

        let expr = Expr::UnaryOp {
            op: UnaryOp::IsNotNull,
            expr: Box::new(Expr::Literal(LiteralValue::Int64(5))),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Boolean(true)));
    }

    #[test]
    fn test_coalesce_first_non_null() {
        let rule = NullPropagation::new();

        let expr = Expr::Function {
            name: yachtsql_ir::FunctionName::Coalesce,
            args: vec![
                Expr::Literal(LiteralValue::Null),
                Expr::Literal(LiteralValue::Null),
                Expr::Literal(LiteralValue::Int64(5)),
                Expr::Literal(LiteralValue::Int64(10)),
            ],
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Int64(5)));
    }

    #[test]
    fn test_coalesce_all_null() {
        let rule = NullPropagation::new();

        let expr = Expr::Function {
            name: yachtsql_ir::FunctionName::Coalesce,
            args: vec![
                Expr::Literal(LiteralValue::Null),
                Expr::Literal(LiteralValue::Null),
            ],
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Null));
    }

    #[test]
    fn test_ifnull_with_null() {
        let rule = NullPropagation::new();

        let expr = Expr::Function {
            name: yachtsql_ir::FunctionName::Ifnull,
            args: vec![
                Expr::Literal(LiteralValue::Null),
                Expr::Literal(LiteralValue::Int64(10)),
            ],
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Int64(10)));
    }

    #[test]
    fn test_null_and_x() {
        let rule = NullPropagation::new();

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(LiteralValue::Null)),
            op: BinaryOp::And,
            right: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
        };

        let simplified = rule.simplify_expr(&expr);
        assert_eq!(simplified, Expr::Literal(LiteralValue::Null));
    }

    #[test]
    fn test_null_propagation_in_filter() {
        let rule = NullPropagation::new();

        let scan = PlanNode::Scan {
            alias: None,
            table_name: "test_table".to_string(),
            projection: None,
        };

        let filter = PlanNode::Filter {
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Literal(LiteralValue::Null)),
                    op: BinaryOp::Add,
                    right: Box::new(Expr::column("x")),
                }),
                op: BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Int64(5))),
            },
            input: Box::new(scan),
        };

        let plan = LogicalPlan::new(filter);
        let optimized = rule.optimize(&plan).unwrap();

        assert!(optimized.is_some());
        let optimized_plan = optimized.unwrap();

        match optimized_plan.root() {
            PlanNode::Filter { predicate, .. } => {
                assert_eq!(*predicate, Expr::Literal(LiteralValue::Null));
            }
            _ => panic!("Expected Filter node"),
        }
    }
}
