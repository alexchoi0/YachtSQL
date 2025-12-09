use std::rc::Rc;

use yachtsql_core::types::{DataType, Value};
use yachtsql_executor::Table;
use yachtsql_executor::query_executor::evaluator::physical_plan::{
    AggregateStrategy, ExecutionPlan, ExecutionStatistics, JoinStrategy, MergeJoinExec,
    SortAggregateExec, SortExec,
};
use yachtsql_ir::expr::{Expr, OrderByExpr};
use yachtsql_ir::function::FunctionName;
use yachtsql_ir::plan::JoinType;
use yachtsql_storage::{Column, Field, Schema};

#[derive(Debug)]
struct MockDataExec {
    schema: Schema,
    data: Vec<Table>,
    stats: ExecutionStatistics,
}

impl MockDataExec {
    fn new(schema: Schema, data: Vec<Table>) -> Self {
        Self {
            schema,
            data,
            stats: ExecutionStatistics::default(),
        }
    }

    fn with_sorted(mut self, is_sorted: bool, sort_columns: Vec<String>) -> Self {
        self.stats = ExecutionStatistics {
            is_sorted,
            sort_columns: if sort_columns.is_empty() {
                None
            } else {
                Some(sort_columns)
            },
            num_rows: None,
            memory_usage: None,
        };
        self
    }
}

impl ExecutionPlan for MockDataExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> yachtsql_core::error::Result<Vec<Table>> {
        Ok(self.data.clone())
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![]
    }

    fn describe(&self) -> String {
        "MockData".to_string()
    }

    fn statistics(&self) -> ExecutionStatistics {
        self.stats.clone()
    }
}

fn create_test_batch(schema: Schema, values: Vec<Vec<Value>>) -> Table {
    let num_rows = values.len();
    let num_cols = schema.fields().len();

    let mut columns = Vec::new();
    for col_idx in 0..num_cols {
        let field = &schema.fields()[col_idx];
        let mut column = Column::new(&field.data_type, num_rows);

        for row in &values {
            column.push(row[col_idx].clone()).unwrap();
        }

        columns.push(column);
    }

    Table::new(schema, columns).unwrap()
}

#[test]
fn test_join_strategy_selects_merge_for_sorted_equi_join() {
    let strategy = JoinStrategy::select(true, true, true, None, None);
    assert!(
        matches!(strategy, JoinStrategy::Merge),
        "Expected Merge join for sorted inputs with equi-join"
    );
}

#[test]
fn test_join_strategy_selects_hash_for_unsorted_equi_join() {
    let strategy = JoinStrategy::select(false, false, true, None, None);
    assert!(
        matches!(strategy, JoinStrategy::Hash),
        "Expected Hash join for unsorted equi-join"
    );
}

#[test]
fn test_join_strategy_selects_nested_loop_for_non_equi_join() {
    let strategy = JoinStrategy::select(false, false, false, None, None);
    assert!(
        matches!(strategy, JoinStrategy::NestedLoop),
        "Expected NestedLoop for non-equi join"
    );
}

#[test]
fn test_join_strategy_selects_index_when_available() {
    let strategy = JoinStrategy::select(false, false, true, Some("idx_user_id"), Some(100));
    assert!(
        matches!(strategy, JoinStrategy::IndexNestedLoop { .. }),
        "Expected IndexNestedLoop when index is available and outer is small"
    );
}

#[test]
fn test_aggregate_strategy_selects_sort_for_sorted_input() {
    let strategy = AggregateStrategy::select_with_columns(
        true,
        &["category".to_string()],
        &["category".to_string()],
    );
    assert!(
        matches!(strategy, AggregateStrategy::Sort),
        "Expected Sort aggregate for sorted input on group-by columns"
    );
}

#[test]
fn test_aggregate_strategy_selects_hash_for_unsorted_input() {
    let strategy = AggregateStrategy::select_with_columns(false, &[], &["category".to_string()]);
    assert!(
        matches!(strategy, AggregateStrategy::Hash),
        "Expected Hash aggregate for unsorted input"
    );
}

#[test]
fn test_aggregate_strategy_selects_hash_when_sort_columns_dont_match() {
    let strategy = AggregateStrategy::select_with_columns(
        true,
        &["name".to_string()],
        &["category".to_string()],
    );
    assert!(
        matches!(strategy, AggregateStrategy::Hash),
        "Expected Hash aggregate when sort columns don't match group-by columns"
    );
}

#[test]
fn test_merge_join_inner_join() {
    let left_schema = Schema::from_fields(vec![
        Field::required("id".to_string(), DataType::Int64),
        Field::required("name".to_string(), DataType::String),
    ]);

    let right_schema = Schema::from_fields(vec![
        Field::required("user_id".to_string(), DataType::Int64),
        Field::required("amount".to_string(), DataType::Int64),
    ]);

    let left_data = create_test_batch(
        left_schema.clone(),
        vec![
            vec![Value::int64(1), Value::string("Alice".to_string())],
            vec![Value::int64(2), Value::string("Bob".to_string())],
            vec![Value::int64(3), Value::string("Charlie".to_string())],
        ],
    );

    let right_data = create_test_batch(
        right_schema.clone(),
        vec![
            vec![Value::int64(1), Value::int64(100)],
            vec![Value::int64(1), Value::int64(150)],
            vec![Value::int64(3), Value::int64(200)],
        ],
    );

    let left_key = Expr::Column {
        name: "id".to_string(),
        table: None,
    };
    let right_key = Expr::Column {
        name: "user_id".to_string(),
        table: None,
    };

    let left = Rc::new(
        MockDataExec::new(left_schema, vec![left_data]).with_sorted(true, vec!["id".to_string()]),
    );
    let right = Rc::new(
        MockDataExec::new(right_schema, vec![right_data])
            .with_sorted(true, vec!["user_id".to_string()]),
    );

    let on_conditions = vec![(left_key, right_key)];
    let merge_join = MergeJoinExec::new(left, right, JoinType::Inner, on_conditions).unwrap();

    let result = merge_join.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

    assert_eq!(
        total_rows, 3,
        "Inner join should produce 3 rows (2 for id=1, 1 for id=3)"
    );
}

#[test]
fn test_merge_join_left_join() {
    let left_schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

    let right_schema = Schema::from_fields(vec![
        Field::required("user_id".to_string(), DataType::Int64),
        Field::required("value".to_string(), DataType::Int64),
    ]);

    let left_data = create_test_batch(
        left_schema.clone(),
        vec![
            vec![Value::int64(1)],
            vec![Value::int64(2)],
            vec![Value::int64(3)],
        ],
    );

    let right_data = create_test_batch(
        right_schema.clone(),
        vec![
            vec![Value::int64(1), Value::int64(10)],
            vec![Value::int64(3), Value::int64(30)],
        ],
    );

    let left_key = Expr::Column {
        name: "id".to_string(),
        table: None,
    };
    let right_key = Expr::Column {
        name: "user_id".to_string(),
        table: None,
    };

    let left = Rc::new(
        MockDataExec::new(left_schema, vec![left_data]).with_sorted(true, vec!["id".to_string()]),
    );
    let right = Rc::new(
        MockDataExec::new(right_schema, vec![right_data])
            .with_sorted(true, vec!["user_id".to_string()]),
    );

    let on_conditions = vec![(left_key, right_key)];
    let merge_join = MergeJoinExec::new(left, right, JoinType::Left, on_conditions).unwrap();

    let result = merge_join.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

    assert_eq!(
        total_rows, 3,
        "Left join should produce 3 rows (all left rows)"
    );
}

#[test]
fn test_sort_aggregate_sum() {
    let schema = Schema::from_fields(vec![
        Field::required("category".to_string(), DataType::String),
        Field::required("amount".to_string(), DataType::Int64),
    ]);

    let data = create_test_batch(
        schema.clone(),
        vec![
            vec![Value::string("A".to_string()), Value::int64(10)],
            vec![Value::string("A".to_string()), Value::int64(20)],
            vec![Value::string("B".to_string()), Value::int64(30)],
            vec![Value::string("B".to_string()), Value::int64(40)],
            vec![Value::string("C".to_string()), Value::int64(50)],
        ],
    );

    let group_by_col = Expr::Column {
        name: "category".to_string(),
        table: None,
    };

    let input = Rc::new(
        MockDataExec::new(schema, vec![data]).with_sorted(true, vec!["category".to_string()]),
    );

    let sum_expr = Expr::Aggregate {
        name: FunctionName::Sum,
        args: vec![Expr::Column {
            name: "amount".to_string(),
            table: None,
        }],
        distinct: false,
        filter: None,
        order_by: None,
    };

    let agg =
        SortAggregateExec::new(input, vec![group_by_col], vec![(sum_expr, None)], None).unwrap();

    let result = agg.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 3, "Should have 3 groups: A, B, C");
}

#[test]
fn test_sort_aggregate_count() {
    let schema = Schema::from_fields(vec![
        Field::required("group_id".to_string(), DataType::Int64),
        Field::required("value".to_string(), DataType::Int64),
    ]);

    let data = create_test_batch(
        schema.clone(),
        vec![
            vec![Value::int64(1), Value::int64(100)],
            vec![Value::int64(1), Value::int64(200)],
            vec![Value::int64(1), Value::int64(300)],
            vec![Value::int64(2), Value::int64(400)],
        ],
    );

    let group_by_col = Expr::Column {
        name: "group_id".to_string(),
        table: None,
    };

    let input = Rc::new(
        MockDataExec::new(schema, vec![data]).with_sorted(true, vec!["group_id".to_string()]),
    );

    let count_expr = Expr::Aggregate {
        name: FunctionName::Count,
        args: vec![Expr::Column {
            name: "value".to_string(),
            table: None,
        }],
        distinct: false,
        filter: None,
        order_by: None,
    };

    let agg =
        SortAggregateExec::new(input, vec![group_by_col], vec![(count_expr, None)], None).unwrap();

    let result = agg.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 2, "Should have 2 groups: 1 and 2");
}

#[test]
fn test_sort_exec_sets_is_sorted_flag() {
    let schema = Schema::from_fields(vec![Field::required("id".to_string(), DataType::Int64)]);

    let data = create_test_batch(
        schema.clone(),
        vec![
            vec![Value::int64(3)],
            vec![Value::int64(1)],
            vec![Value::int64(2)],
        ],
    );

    let input = Rc::new(MockDataExec::new(schema, vec![data]));

    let order_by = vec![OrderByExpr {
        expr: Expr::Column {
            name: "id".to_string(),
            table: None,
        },
        asc: Some(true),
        nulls_first: Some(false),
        collation: None,
        with_fill: None,
    }];

    let sort_exec = SortExec::new(input, order_by).unwrap();
    let stats = sort_exec.statistics();

    assert!(stats.is_sorted, "SortExec should report is_sorted=true");
    assert!(
        stats.sort_columns.is_some(),
        "SortExec should report sort columns"
    );
    assert_eq!(
        stats.sort_columns.as_ref().unwrap().len(),
        1,
        "SortExec should have 1 sort column"
    );
}

#[test]
fn test_execution_statistics_default() {
    let stats = ExecutionStatistics::default();
    assert!(!stats.is_sorted);
    assert!(stats.sort_columns.is_none());
    assert!(stats.num_rows.is_none());
    assert!(stats.memory_usage.is_none());
}
