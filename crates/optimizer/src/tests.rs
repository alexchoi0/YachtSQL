#[cfg(test)]
mod optimizer_tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{BinaryOp, Expr, LogicalPlan, PlanField, PlanSchema, SortExpr};

    use crate::{OptimizedLogicalPlan, PhysicalPlanner};

    fn test_schema() -> PlanSchema {
        PlanSchema::from_fields(vec![
            PlanField::new("id", DataType::Int64),
            PlanField::new("name", DataType::String),
            PlanField::new("value", DataType::Float64),
        ])
    }

    fn scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: name.to_string(),
            schema: test_schema(),
            projection: None,
        }
    }

    fn col(name: &str) -> Expr {
        Expr::column(name)
    }

    fn lit_i64(v: i64) -> Expr {
        Expr::literal_i64(v)
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
        }
    }

    fn optimize(plan: &LogicalPlan) -> OptimizedLogicalPlan {
        PhysicalPlanner::new().plan(plan).unwrap()
    }

    mod topn_optimization {
        use super::*;

        #[test]
        fn sort_with_limit_becomes_topn() {
            let plan = LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Sort {
                    input: Box::new(scan("users")),
                    sort_exprs: vec![SortExpr {
                        expr: col("id"),
                        asc: true,
                        nulls_first: false,
                    }],
                }),
                limit: Some(10),
                offset: None,
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::TopN {
                    sort_exprs, limit, ..
                } => {
                    assert_eq!(limit, 10);
                    assert_eq!(sort_exprs.len(), 1);
                    assert!(sort_exprs[0].asc);
                }
                other => panic!("Expected TopN, got {:?}", other),
            }
        }

        #[test]
        fn sort_with_limit_and_offset_stays_separate() {
            let plan = LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Sort {
                    input: Box::new(scan("users")),
                    sort_exprs: vec![SortExpr {
                        expr: col("id"),
                        asc: true,
                        nulls_first: false,
                    }],
                }),
                limit: Some(10),
                offset: Some(5),
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::Limit { offset, .. } => {
                    assert_eq!(offset, Some(5));
                }
                other => panic!("Expected Limit (not TopN due to offset), got {:?}", other),
            }
        }

        #[test]
        fn sort_without_limit_stays_sort() {
            let plan = LogicalPlan::Sort {
                input: Box::new(scan("users")),
                sort_exprs: vec![SortExpr {
                    expr: col("id"),
                    asc: false,
                    nulls_first: true,
                }],
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::Sort { sort_exprs, .. } => {
                    assert_eq!(sort_exprs.len(), 1);
                    assert!(!sort_exprs[0].asc);
                }
                other => panic!("Expected Sort, got {:?}", other),
            }
        }

        #[test]
        fn limit_without_sort_stays_limit() {
            let plan = LogicalPlan::Limit {
                input: Box::new(scan("users")),
                limit: Some(10),
                offset: None,
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::Limit { limit, offset, .. } => {
                    assert_eq!(limit, Some(10));
                    assert_eq!(offset, None);
                }
                other => panic!("Expected Limit, got {:?}", other),
            }
        }

        #[test]
        fn limit_none_with_sort_stays_separate() {
            let plan = LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Sort {
                    input: Box::new(scan("users")),
                    sort_exprs: vec![SortExpr {
                        expr: col("id"),
                        asc: true,
                        nulls_first: false,
                    }],
                }),
                limit: None,
                offset: Some(5),
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::Limit { limit, offset, .. } => {
                    assert_eq!(limit, None);
                    assert_eq!(offset, Some(5));
                }
                other => panic!("Expected Limit, got {:?}", other),
            }
        }

        #[test]
        fn topn_with_filter() {
            let plan = LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Sort {
                    input: Box::new(LogicalPlan::Filter {
                        input: Box::new(scan("users")),
                        predicate: eq(col("id"), lit_i64(1)),
                    }),
                    sort_exprs: vec![SortExpr {
                        expr: col("value"),
                        asc: false,
                        nulls_first: false,
                    }],
                }),
                limit: Some(5),
                offset: None,
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::TopN { input, limit, .. } => {
                    assert_eq!(limit, 5);
                    match input.as_ref() {
                        OptimizedLogicalPlan::Filter { .. } => {}
                        _ => panic!("Expected Filter under TopN"),
                    }
                }
                other => panic!("Expected TopN at top, got {:?}", other),
            }
        }
    }
}
