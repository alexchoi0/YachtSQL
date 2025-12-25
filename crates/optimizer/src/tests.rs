#[cfg(test)]
mod optimizer_tests {
    use yachtsql_common::types::DataType;
    use yachtsql_ir::{BinaryOp, Expr, JoinType, LogicalPlan, PlanField, PlanSchema, SortExpr};

    use crate::{OptimizedLogicalPlan, PhysicalPlanner};

    fn test_schema() -> PlanSchema {
        PlanSchema::from_fields(vec![
            PlanField::new("id", DataType::Int64),
            PlanField::new("name", DataType::String),
            PlanField::new("value", DataType::Float64),
        ])
    }

    fn users_schema() -> PlanSchema {
        PlanSchema::from_fields(vec![
            PlanField::new("id", DataType::Int64),
            PlanField::new("name", DataType::String),
        ])
    }

    fn orders_schema() -> PlanSchema {
        PlanSchema::from_fields(vec![
            PlanField::new("order_id", DataType::Int64),
            PlanField::new("user_id", DataType::Int64),
            PlanField::new("amount", DataType::Float64),
        ])
    }

    fn scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: name.to_string(),
            schema: test_schema(),
            projection: None,
        }
    }

    fn scan_users() -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: "users".to_string(),
            schema: users_schema(),
            projection: None,
        }
    }

    fn scan_orders() -> LogicalPlan {
        LogicalPlan::Scan {
            table_name: "orders".to_string(),
            schema: orders_schema(),
            projection: None,
        }
    }

    fn col(name: &str) -> Expr {
        Expr::column(name)
    }

    fn col_idx(name: &str, index: usize) -> Expr {
        Expr::Column {
            table: None,
            name: name.to_string(),
            index: Some(index),
        }
    }

    fn col_table_idx(table: &str, name: &str, index: usize) -> Expr {
        Expr::Column {
            table: Some(table.to_string()),
            name: name.to_string(),
            index: Some(index),
        }
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

    fn and(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }
    }

    fn gt(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Gt,
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

    mod hash_join_optimization {
        use super::*;

        fn joined_schema() -> PlanSchema {
            PlanSchema::from_fields(vec![
                PlanField::new("id", DataType::Int64),
                PlanField::new("name", DataType::String),
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("user_id", DataType::Int64),
                PlanField::new("amount", DataType::Float64),
            ])
        }

        #[test]
        fn inner_join_with_equi_condition_becomes_hash_join() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: Some(eq(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::HashJoin {
                    join_type,
                    left_keys,
                    right_keys,
                    ..
                } => {
                    assert_eq!(join_type, JoinType::Inner);
                    assert_eq!(left_keys.len(), 1);
                    assert_eq!(right_keys.len(), 1);
                    match &left_keys[0] {
                        Expr::Column { name, index, .. } => {
                            assert_eq!(name, "id");
                            assert_eq!(*index, Some(0));
                        }
                        _ => panic!("Expected column expression in left_keys"),
                    }
                    match &right_keys[0] {
                        Expr::Column { name, index, .. } => {
                            assert_eq!(name, "user_id");
                            assert_eq!(*index, Some(1));
                        }
                        _ => panic!("Expected column expression in right_keys"),
                    }
                }
                other => panic!("Expected HashJoin, got {:?}", other),
            }
        }

        #[test]
        fn inner_join_with_multiple_equi_keys_becomes_hash_join() {
            let multi_key_schema = PlanSchema::from_fields(vec![
                PlanField::new("a", DataType::Int64),
                PlanField::new("b", DataType::Int64),
                PlanField::new("c", DataType::Int64),
                PlanField::new("d", DataType::Int64),
            ]);

            let left_scan = LogicalPlan::Scan {
                table_name: "t1".to_string(),
                schema: PlanSchema::from_fields(vec![
                    PlanField::new("a", DataType::Int64),
                    PlanField::new("b", DataType::Int64),
                ]),
                projection: None,
            };

            let right_scan = LogicalPlan::Scan {
                table_name: "t2".to_string(),
                schema: PlanSchema::from_fields(vec![
                    PlanField::new("c", DataType::Int64),
                    PlanField::new("d", DataType::Int64),
                ]),
                projection: None,
            };

            let plan = LogicalPlan::Join {
                left: Box::new(left_scan),
                right: Box::new(right_scan),
                join_type: JoinType::Inner,
                condition: Some(and(
                    eq(col_idx("a", 0), col_idx("c", 2)),
                    eq(col_idx("b", 1), col_idx("d", 3)),
                )),
                schema: multi_key_schema,
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::HashJoin {
                    left_keys,
                    right_keys,
                    ..
                } => {
                    assert_eq!(left_keys.len(), 2);
                    assert_eq!(right_keys.len(), 2);
                }
                other => panic!("Expected HashJoin with 2 keys, got {:?}", other),
            }
        }

        #[test]
        fn inner_join_without_condition_uses_nested_loop() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: None,
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::NestedLoopJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                other => panic!("Expected NestedLoopJoin (no condition), got {:?}", other),
            }
        }

        #[test]
        fn inner_join_with_non_equi_condition_uses_nested_loop() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: Some(gt(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::NestedLoopJoin { .. } => {}
                other => panic!("Expected NestedLoopJoin (non-equi), got {:?}", other),
            }
        }

        #[test]
        fn left_join_uses_nested_loop() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Left,
                condition: Some(eq(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::NestedLoopJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Left);
                }
                other => panic!("Expected NestedLoopJoin (left join), got {:?}", other),
            }
        }

        #[test]
        fn right_join_uses_nested_loop() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Right,
                condition: Some(eq(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::NestedLoopJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Right);
                }
                other => panic!("Expected NestedLoopJoin (right join), got {:?}", other),
            }
        }

        #[test]
        fn full_join_uses_nested_loop() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Full,
                condition: Some(eq(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::NestedLoopJoin { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Full);
                }
                other => panic!("Expected NestedLoopJoin (full join), got {:?}", other),
            }
        }

        #[test]
        fn cross_join_uses_cross_join() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Cross,
                condition: None,
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::CrossJoin { .. } => {}
                other => panic!("Expected CrossJoin, got {:?}", other),
            }
        }

        #[test]
        fn hash_join_into_logical_restores_right_indices() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: Some(eq(col_idx("id", 0), col_idx("user_id", 3))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match &optimized {
                OptimizedLogicalPlan::HashJoin { .. } => {}
                other => panic!("Expected HashJoin, got {:?}", other),
            }

            let back_to_logical = optimized.into_logical();

            match back_to_logical {
                LogicalPlan::Join { condition, .. } => {
                    let cond = condition.expect("should have condition");
                    match cond {
                        Expr::BinaryOp { left, right, .. } => {
                            match left.as_ref() {
                                Expr::Column { index, .. } => {
                                    assert_eq!(*index, Some(0), "left key should be index 0");
                                }
                                _ => panic!("Expected column"),
                            }
                            match right.as_ref() {
                                Expr::Column { index, .. } => {
                                    assert_eq!(
                                        *index,
                                        Some(3),
                                        "right key should be restored to index 3"
                                    );
                                }
                                _ => panic!("Expected column"),
                            }
                        }
                        _ => panic!("Expected BinaryOp"),
                    }
                }
                _ => panic!("Expected Join"),
            }
        }

        #[test]
        fn hash_join_with_reversed_condition_order() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: Some(eq(col_idx("user_id", 3), col_idx("id", 0))),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::HashJoin {
                    left_keys,
                    right_keys,
                    ..
                } => {
                    assert_eq!(left_keys.len(), 1);
                    assert_eq!(right_keys.len(), 1);
                    match &left_keys[0] {
                        Expr::Column { name, index, .. } => {
                            assert_eq!(name, "id");
                            assert_eq!(*index, Some(0));
                        }
                        _ => panic!("Expected left key to be 'id'"),
                    }
                    match &right_keys[0] {
                        Expr::Column { name, index, .. } => {
                            assert_eq!(name, "user_id");
                            assert_eq!(*index, Some(1));
                        }
                        _ => panic!("Expected right key to be 'user_id'"),
                    }
                }
                other => panic!(
                    "Expected HashJoin even with reversed condition, got {:?}",
                    other
                ),
            }
        }

        #[test]
        fn hash_join_with_table_qualified_columns() {
            let plan = LogicalPlan::Join {
                left: Box::new(scan_users()),
                right: Box::new(scan_orders()),
                join_type: JoinType::Inner,
                condition: Some(eq(
                    col_table_idx("users", "id", 0),
                    col_table_idx("orders", "user_id", 3),
                )),
                schema: joined_schema(),
            };

            let optimized = optimize(&plan);

            match optimized {
                OptimizedLogicalPlan::HashJoin { .. } => {}
                other => panic!("Expected HashJoin with qualified columns, got {:?}", other),
            }
        }

        #[test]
        fn nested_hash_joins() {
            let products_schema = PlanSchema::from_fields(vec![
                PlanField::new("product_id", DataType::Int64),
                PlanField::new("product_name", DataType::String),
            ]);

            let products_scan = LogicalPlan::Scan {
                table_name: "products".to_string(),
                schema: products_schema,
                projection: None,
            };

            let order_items_schema = PlanSchema::from_fields(vec![
                PlanField::new("item_id", DataType::Int64),
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("product_id", DataType::Int64),
            ]);

            let order_items_scan = LogicalPlan::Scan {
                table_name: "order_items".to_string(),
                schema: order_items_schema,
                projection: None,
            };

            let first_join_schema = PlanSchema::from_fields(vec![
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("user_id", DataType::Int64),
                PlanField::new("amount", DataType::Float64),
                PlanField::new("item_id", DataType::Int64),
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("product_id", DataType::Int64),
            ]);

            let first_join = LogicalPlan::Join {
                left: Box::new(scan_orders()),
                right: Box::new(order_items_scan),
                join_type: JoinType::Inner,
                condition: Some(eq(col_idx("order_id", 0), col_idx("order_id", 4))),
                schema: first_join_schema.clone(),
            };

            let final_schema = PlanSchema::from_fields(vec![
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("user_id", DataType::Int64),
                PlanField::new("amount", DataType::Float64),
                PlanField::new("item_id", DataType::Int64),
                PlanField::new("order_id", DataType::Int64),
                PlanField::new("product_id", DataType::Int64),
                PlanField::new("product_id", DataType::Int64),
                PlanField::new("product_name", DataType::String),
            ]);

            let second_join = LogicalPlan::Join {
                left: Box::new(first_join),
                right: Box::new(products_scan),
                join_type: JoinType::Inner,
                condition: Some(eq(col_idx("product_id", 5), col_idx("product_id", 6))),
                schema: final_schema,
            };

            let optimized = optimize(&second_join);

            match &optimized {
                OptimizedLogicalPlan::HashJoin { left, .. } => match left.as_ref() {
                    OptimizedLogicalPlan::HashJoin { .. } => {}
                    other => panic!("Expected nested HashJoin, got {:?}", other),
                },
                other => panic!("Expected outer HashJoin, got {:?}", other),
            }
        }
    }
}
