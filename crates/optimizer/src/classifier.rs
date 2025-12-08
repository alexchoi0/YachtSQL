use yachtsql_ir::plan::{LogicalPlan, PlanNode};

use crate::phase::Phase;

#[derive(Debug, Clone)]
pub struct QueryComplexity {
    pub num_tables: usize,
    pub num_joins: usize,
    pub num_subqueries: usize,
    pub num_aggregates: usize,
    pub num_windows: usize,
    pub has_complex_predicates: bool,
    pub has_filters: bool,
    pub has_unnecessary_projections: bool,
    pub plan_depth: usize,
    pub estimated_cost_micros: u64,
}

impl QueryComplexity {
    pub fn analyze(plan: &LogicalPlan) -> Self {
        let mut analyzer = ComplexityAnalyzer::new();
        analyzer.visit_plan(plan);
        analyzer.into_complexity()
    }

    pub fn needs_optimization(&self) -> bool {
        if self.num_tables == 1
            && self.num_joins == 0
            && self.num_subqueries == 0
            && self.num_aggregates <= 1
            && self.num_windows == 0
            && self.plan_depth <= 3
            && self.estimated_cost_micros <= 500_000
        {
            return false;
        }

        true
    }

    pub fn recommended_phases(&self) -> Vec<Phase> {
        let mut phases = Vec::new();

        if self.has_complex_predicates || self.num_aggregates > 0 || self.num_windows > 0 {
            phases.push(Phase::Simplification);
        }

        if self.has_filters || self.has_unnecessary_projections || self.num_tables > 1 {
            phases.push(Phase::Pushdown);
        }

        if self.num_joins > 1 || self.num_subqueries > 0 {
            phases.push(Phase::Reordering);
        }

        if phases.len() > 1 {
            phases.push(Phase::Cleanup);
        }

        if phases.is_empty() {
            phases.push(Phase::Pushdown);
        }

        phases
    }

    pub fn complexity_level(&self) -> ComplexityLevel {
        if self.num_joins > 4 || self.num_subqueries > 3 || self.num_windows > 2 {
            ComplexityLevel::VeryHigh
        } else if self.num_joins > 2 || self.num_subqueries > 1 || self.num_windows > 0 {
            ComplexityLevel::High
        } else if self.num_joins > 0 || self.num_aggregates > 1 {
            ComplexityLevel::Medium
        } else if self.num_tables > 1 || self.has_filters {
            ComplexityLevel::Low
        } else {
            ComplexityLevel::Trivial
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComplexityLevel {
    Trivial,
    Low,
    Medium,
    High,
    VeryHigh,
}

struct ComplexityAnalyzer {
    num_tables: usize,
    num_joins: usize,
    num_subqueries: usize,
    num_aggregates: usize,
    num_windows: usize,
    has_complex_predicates: bool,
    has_filters: bool,
    has_projections: bool,
    max_depth: usize,
    current_depth: usize,
}

impl ComplexityAnalyzer {
    fn new() -> Self {
        Self {
            num_tables: 0,
            num_joins: 0,
            num_subqueries: 0,
            num_aggregates: 0,
            num_windows: 0,
            has_complex_predicates: false,
            has_filters: false,
            has_projections: false,
            max_depth: 0,
            current_depth: 0,
        }
    }

    fn visit_plan(&mut self, plan: &LogicalPlan) {
        self.visit_node(&plan.root);
    }

    fn visit_node(&mut self, node: &PlanNode) {
        self.current_depth += 1;
        self.max_depth = self.max_depth.max(self.current_depth);

        match node {
            PlanNode::Scan { .. } | PlanNode::IndexScan { .. } => {
                self.num_tables += 1;
            }

            PlanNode::Filter { input, predicate } => {
                self.has_filters = true;

                if self.is_complex_predicate(predicate) {
                    self.has_complex_predicates = true;
                }

                self.visit_node(input);
            }

            PlanNode::Projection { expressions, input } => {
                self.has_projections = true;
                for (expr, _) in expressions {
                    if Self::contains_scalar_subquery(expr) {
                        self.num_subqueries += 1;
                    }
                }
                self.visit_node(input);
            }

            PlanNode::Join { left, right, .. } => {
                self.num_joins += 1;
                self.visit_node(left);
                self.visit_node(right);
            }

            PlanNode::LateralJoin { left, right, .. } => {
                self.num_joins += 1;
                self.visit_node(left);
                self.visit_node(right);
            }

            PlanNode::Aggregate { input, .. } => {
                self.num_aggregates += 1;
                self.visit_node(input);
            }

            PlanNode::Window { input, .. } => {
                self.num_windows += 1;
                self.visit_node(input);
            }

            PlanNode::Sort { input, .. } => {
                self.visit_node(input);
            }

            PlanNode::Limit { input, .. } => {
                self.visit_node(input);
            }

            PlanNode::Union { left, right, .. } => {
                self.visit_node(left);
                self.visit_node(right);
            }

            PlanNode::Intersect { left, right, .. } => {
                self.visit_node(left);
                self.visit_node(right);
            }

            PlanNode::Except { left, right, .. } => {
                self.visit_node(left);
                self.visit_node(right);
            }

            PlanNode::Distinct { input } => {
                self.visit_node(input);
            }

            PlanNode::DistinctOn { input, .. } => {
                self.visit_node(input);
            }

            PlanNode::SubqueryScan { subquery, .. } => {
                self.num_subqueries += 1;
                self.visit_node(subquery);
            }

            PlanNode::Cte { input, .. } => {
                self.visit_node(input);
            }

            PlanNode::Unnest { .. } | PlanNode::TableValuedFunction { .. } => {}

            PlanNode::ArrayJoin { input, .. } => {
                self.visit_node(input);
            }

            PlanNode::TableSample { input, .. } => {
                self.visit_node(input);
            }

            PlanNode::Pivot { input, .. } => {
                self.visit_node(input);
            }

            PlanNode::Unpivot { input, .. } => {
                self.visit_node(input);
            }

            PlanNode::Update { .. } | PlanNode::Delete { .. } | PlanNode::Truncate { .. } => {}

            PlanNode::InsertOnConflict { .. } | PlanNode::Insert { .. } => {}

            PlanNode::Merge { source, .. } => {
                self.visit_node(source);
            }

            PlanNode::AlterTable { .. } => {}

            PlanNode::EmptyRelation => {}

            PlanNode::Values { .. } => {}
        }

        self.current_depth -= 1;
    }

    fn contains_scalar_subquery(expr: &yachtsql_ir::expr::Expr) -> bool {
        use yachtsql_ir::expr::Expr;
        match expr {
            Expr::ScalarSubquery { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_scalar_subquery(left) || Self::contains_scalar_subquery(right)
            }
            Expr::UnaryOp { expr, .. } => Self::contains_scalar_subquery(expr),
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|e| Self::contains_scalar_subquery(e))
                    || when_then.iter().any(|(cond, result)| {
                        Self::contains_scalar_subquery(cond)
                            || Self::contains_scalar_subquery(result)
                    })
                    || else_expr
                        .as_ref()
                        .is_some_and(|e| Self::contains_scalar_subquery(e))
            }
            Expr::Function { args, .. } => args.iter().any(Self::contains_scalar_subquery),
            _ => false,
        }
    }

    fn is_complex_predicate(&mut self, expr: &yachtsql_ir::expr::Expr) -> bool {
        use yachtsql_ir::expr::{BinaryOp, Expr, UnaryOp};

        match expr {
            Expr::BinaryOp { left, op, right } => {
                matches!(op, BinaryOp::Or)
                    || self.is_complex_predicate(left)
                    || self.is_complex_predicate(right)
            }
            Expr::UnaryOp { op, expr } => {
                matches!(op, UnaryOp::Not) || self.is_complex_predicate(expr)
            }
            Expr::Case { .. } => true,
            Expr::ScalarSubquery { .. } => {
                self.num_subqueries += 1;
                true
            }
            Expr::Exists { .. } => {
                self.num_subqueries += 1;
                true
            }
            Expr::InSubquery { .. } | Expr::TupleInSubquery { .. } => {
                self.num_subqueries += 1;
                true
            }
            Expr::InList { .. } => false,
            Expr::Between { .. } => false,
            _ => false,
        }
    }

    fn into_complexity(self) -> QueryComplexity {
        let base_cost = 1000;
        let table_cost = self.num_tables * 5000;
        let join_cost = self.num_joins * 20000;
        let subquery_cost = self.num_subqueries * 50000;
        let aggregate_cost = self.num_aggregates * 10000;
        let window_cost = self.num_windows * 30000;

        let estimated_cost_micros =
            (base_cost + table_cost + join_cost + subquery_cost + aggregate_cost + window_cost)
                as u64;

        QueryComplexity {
            num_tables: self.num_tables,
            num_joins: self.num_joins,
            num_subqueries: self.num_subqueries,
            num_aggregates: self.num_aggregates,
            num_windows: self.num_windows,
            has_complex_predicates: self.has_complex_predicates,
            has_filters: self.has_filters,
            has_unnecessary_projections: self.has_projections,
            plan_depth: self.max_depth,
            estimated_cost_micros,
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_ir::expr::{BinaryOp, Expr};
    use yachtsql_ir::plan::{JoinType, PlanNode};

    use super::*;

    #[test]
    fn test_trivial_query_no_optimization() {
        let plan = LogicalPlan {
            root: Box::new(PlanNode::Scan {
                table_name: "users".to_string(),
                alias: None,
                projection: Some(vec!["id".to_string(), "name".to_string()]),
            }),
        };

        let complexity = QueryComplexity::analyze(&plan);
        assert_eq!(complexity.num_tables, 1);
        assert_eq!(complexity.num_joins, 0);
        assert_eq!(complexity.num_subqueries, 0);
        assert_eq!(complexity.num_aggregates, 0);
        assert!(!complexity.has_filters);
        assert!(!complexity.needs_optimization());
        assert_eq!(complexity.complexity_level(), ComplexityLevel::Trivial);
    }

    #[test]
    fn test_simple_filter_needs_optimization() {
        let plan = LogicalPlan {
            root: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: Some(vec!["id".to_string()]),
                }),
                predicate: Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        name: "id".to_string(),
                        table: None,
                    }),
                    op: BinaryOp::GreaterThan,
                    right: Box::new(Expr::Literal(yachtsql_ir::expr::LiteralValue::Int64(10))),
                },
            }),
        };

        let complexity = QueryComplexity::analyze(&plan);
        assert!(complexity.has_filters);

        let phases = complexity.recommended_phases();
        assert!(phases.contains(&Phase::Pushdown));
    }

    #[test]
    fn test_single_table_with_filter_low_cost_skips_optimization() {
        let plan = LogicalPlan {
            root: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: Some(vec!["id".to_string()]),
                }),
                predicate: Expr::Literal(yachtsql_ir::expr::LiteralValue::Boolean(true)),
            }),
        };

        let complexity = QueryComplexity::analyze(&plan);

        assert!(!complexity.needs_optimization());
    }

    #[test]
    fn test_multi_join_query_needs_reordering() {
        let plan = LogicalPlan {
            root: Box::new(PlanNode::Join {
                left: Box::new(PlanNode::Join {
                    left: Box::new(PlanNode::Scan {
                        table_name: "a".to_string(),
                        alias: None,
                        projection: None,
                    }),
                    right: Box::new(PlanNode::Scan {
                        table_name: "b".to_string(),
                        alias: None,
                        projection: None,
                    }),
                    join_type: JoinType::Inner,
                    on: Expr::Literal(yachtsql_ir::expr::LiteralValue::Boolean(true)),
                }),
                right: Box::new(PlanNode::Scan {
                    table_name: "c".to_string(),
                    alias: None,
                    projection: None,
                }),
                join_type: JoinType::Inner,
                on: Expr::Literal(yachtsql_ir::expr::LiteralValue::Boolean(true)),
            }),
        };

        let complexity = QueryComplexity::analyze(&plan);
        assert_eq!(complexity.num_tables, 3);
        assert_eq!(complexity.num_joins, 2);
        assert!(complexity.needs_optimization());

        let phases = complexity.recommended_phases();
        assert!(phases.contains(&Phase::Reordering));
        assert!(phases.contains(&Phase::Pushdown));
        assert!(phases.contains(&Phase::Cleanup));
    }

    #[test]
    fn test_aggregate_query_needs_simplification() {
        let plan = LogicalPlan {
            root: Box::new(PlanNode::Aggregate {
                group_by: vec![Expr::Column {
                    name: "category".to_string(),
                    table: None,
                }],
                aggregates: vec![Expr::Aggregate {
                    name: yachtsql_ir::FunctionName::Count,
                    args: vec![],
                    distinct: false,
                    filter: None,
                    order_by: None,
                }],
                grouping_metadata: None,
                input: Box::new(PlanNode::Scan {
                    table_name: "products".to_string(),
                    alias: None,
                    projection: None,
                }),
            }),
        };

        let complexity = QueryComplexity::analyze(&plan);
        assert_eq!(complexity.num_aggregates, 1);

        let phases = complexity.recommended_phases();
        assert!(phases.contains(&Phase::Simplification));
    }

    #[test]
    fn test_window_function_query_high_complexity() {
        let plan = LogicalPlan {
            root: Box::new(PlanNode::Window {
                window_exprs: vec![],
                input: Box::new(PlanNode::Scan {
                    table_name: "sales".to_string(),
                    alias: None,
                    projection: None,
                }),
            }),
        };

        let complexity = QueryComplexity::analyze(&plan);
        assert_eq!(complexity.num_windows, 1);

        let phases = complexity.recommended_phases();
        assert!(phases.contains(&Phase::Simplification));
    }

    #[test]
    fn test_subquery_increases_complexity() {
        let plan = LogicalPlan {
            root: Box::new(PlanNode::SubqueryScan {
                alias: "sub".to_string(),
                subquery: Box::new(PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: None,
                }),
            }),
        };

        let complexity = QueryComplexity::analyze(&plan);
        assert_eq!(complexity.num_subqueries, 1);
    }

    #[test]
    fn test_complex_predicate_detection() {
        let plan = LogicalPlan {
            root: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    projection: None,
                }),
                predicate: Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        name: "active".to_string(),
                        table: None,
                    }),
                    op: BinaryOp::Or,
                    right: Box::new(Expr::Column {
                        name: "premium".to_string(),
                        table: None,
                    }),
                },
            }),
        };

        let complexity = QueryComplexity::analyze(&plan);
        assert!(complexity.has_complex_predicates);

        let phases = complexity.recommended_phases();
        assert!(phases.contains(&Phase::Simplification));
    }

    #[test]
    fn test_plan_depth_calculation() {
        let plan = LogicalPlan {
            root: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Projection {
                    expressions: vec![],
                    input: Box::new(PlanNode::Scan {
                        table_name: "users".to_string(),
                        alias: None,
                        projection: None,
                    }),
                }),
                predicate: Expr::Literal(yachtsql_ir::expr::LiteralValue::Boolean(true)),
            }),
        };

        let complexity = QueryComplexity::analyze(&plan);
        assert_eq!(complexity.plan_depth, 3);
    }

    #[test]
    fn test_complexity_level_classification() {
        let trivial = QueryComplexity {
            num_tables: 1,
            num_joins: 0,
            num_subqueries: 0,
            num_aggregates: 0,
            num_windows: 0,
            has_complex_predicates: false,
            has_filters: false,
            has_unnecessary_projections: false,
            plan_depth: 2,
            estimated_cost_micros: 1000,
        };
        assert_eq!(trivial.complexity_level(), ComplexityLevel::Trivial);

        let low = QueryComplexity {
            num_tables: 2,
            num_joins: 0,
            num_subqueries: 0,
            num_aggregates: 0,
            num_windows: 0,
            has_complex_predicates: false,
            has_filters: true,
            has_unnecessary_projections: false,
            plan_depth: 3,
            estimated_cost_micros: 10_000,
        };
        assert_eq!(low.complexity_level(), ComplexityLevel::Low);

        let medium = QueryComplexity {
            num_tables: 2,
            num_joins: 1,
            num_subqueries: 0,
            num_aggregates: 1,
            num_windows: 0,
            has_complex_predicates: false,
            has_filters: true,
            has_unnecessary_projections: false,
            plan_depth: 4,
            estimated_cost_micros: 50_000,
        };
        assert_eq!(medium.complexity_level(), ComplexityLevel::Medium);

        let high = QueryComplexity {
            num_tables: 5,
            num_joins: 3,
            num_subqueries: 2,
            num_aggregates: 1,
            num_windows: 0,
            has_complex_predicates: true,
            has_filters: true,
            has_unnecessary_projections: true,
            plan_depth: 8,
            estimated_cost_micros: 500_000,
        };
        assert_eq!(high.complexity_level(), ComplexityLevel::High);

        let very_high = QueryComplexity {
            num_tables: 8,
            num_joins: 5,
            num_subqueries: 4,
            num_aggregates: 2,
            num_windows: 3,
            has_complex_predicates: true,
            has_filters: true,
            has_unnecessary_projections: true,
            plan_depth: 12,
            estimated_cost_micros: 1_000_000,
        };
        assert_eq!(very_high.complexity_level(), ComplexityLevel::VeryHigh);
    }

    #[test]
    fn test_estimated_cost_calculation() {
        let single_table = QueryComplexity {
            num_tables: 1,
            num_joins: 0,
            num_subqueries: 0,
            num_aggregates: 0,
            num_windows: 0,
            has_complex_predicates: false,
            has_filters: false,
            has_unnecessary_projections: false,
            plan_depth: 1,
            estimated_cost_micros: 6000,
        };
        assert_eq!(single_table.estimated_cost_micros, 6000);

        let three_way_join = QueryComplexity {
            num_tables: 3,
            num_joins: 2,
            num_subqueries: 0,
            num_aggregates: 0,
            num_windows: 0,
            has_complex_predicates: false,
            has_filters: false,
            has_unnecessary_projections: false,
            plan_depth: 3,
            estimated_cost_micros: 56_000,
        };
        assert_eq!(three_way_join.estimated_cost_micros, 56_000);
    }

    #[test]
    fn test_recommended_phases_minimal() {
        let simple = QueryComplexity {
            num_tables: 1,
            num_joins: 0,
            num_subqueries: 0,
            num_aggregates: 0,
            num_windows: 0,
            has_complex_predicates: false,
            has_filters: true,
            has_unnecessary_projections: false,
            plan_depth: 2,
            estimated_cost_micros: 10_000,
        };

        let phases = simple.recommended_phases();
        assert_eq!(phases.len(), 1);
        assert_eq!(phases[0], Phase::Pushdown);
    }

    #[test]
    fn test_recommended_phases_comprehensive() {
        let complex = QueryComplexity {
            num_tables: 4,
            num_joins: 3,
            num_subqueries: 1,
            num_aggregates: 2,
            num_windows: 0,
            has_complex_predicates: true,
            has_filters: true,
            has_unnecessary_projections: true,
            plan_depth: 8,
            estimated_cost_micros: 500_000,
        };

        let phases = complex.recommended_phases();
        assert!(phases.contains(&Phase::Simplification));
        assert!(phases.contains(&Phase::Pushdown));
        assert!(phases.contains(&Phase::Reordering));
        assert!(phases.contains(&Phase::Cleanup));
    }

    #[test]
    fn test_needs_optimization_cost_threshold() {
        let at_threshold = QueryComplexity {
            num_tables: 1,
            num_joins: 0,
            num_subqueries: 0,
            num_aggregates: 0,
            num_windows: 0,
            has_complex_predicates: false,
            has_filters: false,
            has_unnecessary_projections: false,
            plan_depth: 1,
            estimated_cost_micros: 500_000,
        };
        assert!(!at_threshold.needs_optimization());

        let above_threshold = QueryComplexity {
            num_tables: 1,
            num_joins: 0,
            num_subqueries: 0,
            num_aggregates: 0,
            num_windows: 0,
            has_complex_predicates: false,
            has_filters: false,
            has_unnecessary_projections: false,
            plan_depth: 1,
            estimated_cost_micros: 500_001,
        };
        assert!(above_threshold.needs_optimization());
    }
}
