use std::rc::Rc;

use yachtsql_executor::Table;
use yachtsql_executor::query_executor::evaluator::physical_plan::{
    CteExec, ExceptExec, ExecutionPlan, IntersectExec, SubqueryScanExec, UnionExec,
};
use yachtsql_storage::{Column, Schema};

#[derive(Debug)]
struct MockDataExec {
    schema: Schema,
    data: Vec<Table>,
}

impl MockDataExec {
    fn new(schema: Schema, data: Vec<Table>) -> Self {
        Self { schema, data }
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
}

fn create_test_batch(schema: Schema, values: Vec<Vec<yachtsql_core::types::Value>>) -> Table {
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
fn test_union_all_with_data() {
    let schema = yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
        "id".to_string(),
        yachtsql_core::types::DataType::Int64,
    )]);

    let left_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::int64(1)],
            vec![yachtsql_core::types::Value::int64(2)],
            vec![yachtsql_core::types::Value::int64(3)],
        ],
    );

    let right_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::int64(3)],
            vec![yachtsql_core::types::Value::int64(4)],
            vec![yachtsql_core::types::Value::int64(5)],
        ],
    );

    let left = Rc::new(MockDataExec::new(schema.clone(), vec![left_data]));
    let right = Rc::new(MockDataExec::new(schema.clone(), vec![right_data]));

    let union = UnionExec::new(left, right, true).unwrap();
    let result = union.execute().unwrap();

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 6);
}

#[test]
fn test_union_distinct_removes_duplicates() {
    let schema = yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
        "value".to_string(),
        yachtsql_core::types::DataType::String,
    )]);

    let left_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::string("a".to_string())],
            vec![yachtsql_core::types::Value::string("b".to_string())],
            vec![yachtsql_core::types::Value::string("c".to_string())],
        ],
    );

    let right_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::string("b".to_string())],
            vec![yachtsql_core::types::Value::string("c".to_string())],
            vec![yachtsql_core::types::Value::string("d".to_string())],
        ],
    );

    let left = Rc::new(MockDataExec::new(schema.clone(), vec![left_data]));
    let right = Rc::new(MockDataExec::new(schema.clone(), vec![right_data]));

    let union = UnionExec::new(left, right, false).unwrap();
    let result = union.execute().unwrap();

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

#[test]
fn test_intersect_all_with_data() {
    let schema = yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
        "id".to_string(),
        yachtsql_core::types::DataType::Int64,
    )]);

    let left_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::int64(1)],
            vec![yachtsql_core::types::Value::int64(2)],
            vec![yachtsql_core::types::Value::int64(3)],
            vec![yachtsql_core::types::Value::int64(3)],
        ],
    );

    let right_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::int64(2)],
            vec![yachtsql_core::types::Value::int64(3)],
            vec![yachtsql_core::types::Value::int64(4)],
        ],
    );

    let left = Rc::new(MockDataExec::new(schema.clone(), vec![left_data]));
    let right = Rc::new(MockDataExec::new(schema.clone(), vec![right_data]));

    let intersect = IntersectExec::new(left, right, true).unwrap();
    let result = intersect.execute().unwrap();

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[test]
fn test_intersect_distinct() {
    let schema = yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
        "id".to_string(),
        yachtsql_core::types::DataType::Int64,
    )]);

    let left_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::int64(1)],
            vec![yachtsql_core::types::Value::int64(2)],
            vec![yachtsql_core::types::Value::int64(2)],
            vec![yachtsql_core::types::Value::int64(3)],
        ],
    );

    let right_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::int64(2)],
            vec![yachtsql_core::types::Value::int64(3)],
            vec![yachtsql_core::types::Value::int64(4)],
        ],
    );

    let left = Rc::new(MockDataExec::new(schema.clone(), vec![left_data]));
    let right = Rc::new(MockDataExec::new(schema.clone(), vec![right_data]));

    let intersect = IntersectExec::new(left, right, false).unwrap();
    let result = intersect.execute().unwrap();

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[test]
fn test_except_all_with_data() {
    let schema = yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
        "id".to_string(),
        yachtsql_core::types::DataType::Int64,
    )]);

    let left_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::int64(1)],
            vec![yachtsql_core::types::Value::int64(2)],
            vec![yachtsql_core::types::Value::int64(3)],
            vec![yachtsql_core::types::Value::int64(4)],
        ],
    );

    let right_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::int64(3)],
            vec![yachtsql_core::types::Value::int64(4)],
            vec![yachtsql_core::types::Value::int64(5)],
        ],
    );

    let left = Rc::new(MockDataExec::new(schema.clone(), vec![left_data]));
    let right = Rc::new(MockDataExec::new(schema.clone(), vec![right_data]));

    let except = ExceptExec::new(left, right, true).unwrap();
    let result = except.execute().unwrap();

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    if let Some(batch) = result.first() {
        let col = &batch.columns().unwrap()[0];
        assert_eq!(col.get(0).unwrap(), yachtsql_core::types::Value::int64(1));
        assert_eq!(col.get(1).unwrap(), yachtsql_core::types::Value::int64(2));
    }
}

#[test]
fn test_except_distinct() {
    let schema = yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
        "id".to_string(),
        yachtsql_core::types::DataType::Int64,
    )]);

    let left_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::int64(1)],
            vec![yachtsql_core::types::Value::int64(1)],
            vec![yachtsql_core::types::Value::int64(2)],
            vec![yachtsql_core::types::Value::int64(3)],
        ],
    );

    let right_data = create_test_batch(
        schema.clone(),
        vec![vec![yachtsql_core::types::Value::int64(2)]],
    );

    let left = Rc::new(MockDataExec::new(schema.clone(), vec![left_data]));
    let right = Rc::new(MockDataExec::new(schema.clone(), vec![right_data]));

    let except = ExceptExec::new(left, right, false).unwrap();
    let result = except.execute().unwrap();

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[test]
fn test_empty_left_side() {
    let schema = yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
        "id".to_string(),
        yachtsql_core::types::DataType::Int64,
    )]);

    let empty_data = Table::empty(schema.clone());
    let right_data = create_test_batch(
        schema.clone(),
        vec![vec![yachtsql_core::types::Value::int64(1)]],
    );

    let left = Rc::new(MockDataExec::new(schema.clone(), vec![empty_data]));
    let right = Rc::new(MockDataExec::new(schema.clone(), vec![right_data]));

    let union = UnionExec::new(left.clone(), right.clone(), true).unwrap();
    let result = union.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    let intersect = IntersectExec::new(left.clone(), right.clone(), true).unwrap();
    let result = intersect.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    let except = ExceptExec::new(left, right, true).unwrap();
    let result = except.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[test]
fn test_cte_execution() {
    let schema = yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
        "id".to_string(),
        yachtsql_core::types::DataType::Int64,
    )]);

    let cte_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::int64(1)],
            vec![yachtsql_core::types::Value::int64(2)],
        ],
    );

    let main_data = create_test_batch(
        schema.clone(),
        vec![vec![yachtsql_core::types::Value::int64(99)]],
    );

    let cte_plan = Rc::new(MockDataExec::new(schema.clone(), vec![cte_data]));
    let input = Rc::new(MockDataExec::new(schema.clone(), vec![main_data]));

    let cte_exec = CteExec::new(cte_plan, input, false);

    let result = cte_exec.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    if let Some(batch) = result.first() {
        let col = &batch.columns().unwrap()[0];
        assert_eq!(col.get(0).unwrap(), yachtsql_core::types::Value::int64(99));
    }
}

#[test]
fn test_cte_materialization() {
    let schema = yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
        "value".to_string(),
        yachtsql_core::types::DataType::String,
    )]);

    let cte_data = create_test_batch(
        schema.clone(),
        vec![vec![yachtsql_core::types::Value::string(
            "cached".to_string(),
        )]],
    );

    let main_data = create_test_batch(
        schema.clone(),
        vec![vec![yachtsql_core::types::Value::string(
            "result".to_string(),
        )]],
    );

    let cte_plan = Rc::new(MockDataExec::new(schema.clone(), vec![cte_data]));
    let input = Rc::new(MockDataExec::new(schema.clone(), vec![main_data]));

    let cte_exec = CteExec::new(cte_plan, input, true);

    let result1 = cte_exec.execute_cte().unwrap();
    assert_eq!(result1.len(), 1);

    let result2 = cte_exec.execute_cte().unwrap();
    assert_eq!(result2.len(), 1);

    if let (Some(batch1), Some(batch2)) = (result1.first(), result2.first()) {
        let val1 = batch1.columns().unwrap()[0].get(0).unwrap();
        let val2 = batch2.columns().unwrap()[0].get(0).unwrap();
        assert_eq!(val1, val2);
        assert_eq!(
            val1,
            yachtsql_core::types::Value::string("cached".to_string())
        );
    }
}

#[test]
fn test_subquery_scan() {
    let schema = yachtsql_storage::Schema::from_fields(vec![
        yachtsql_storage::Field::required("id".to_string(), yachtsql_core::types::DataType::Int64),
        yachtsql_storage::Field::required(
            "name".to_string(),
            yachtsql_core::types::DataType::String,
        ),
    ]);

    let subquery_data = create_test_batch(
        schema.clone(),
        vec![
            vec![
                yachtsql_core::types::Value::int64(1),
                yachtsql_core::types::Value::string("Alice".to_string()),
            ],
            vec![
                yachtsql_core::types::Value::int64(2),
                yachtsql_core::types::Value::string("Bob".to_string()),
            ],
        ],
    );

    let subquery = Rc::new(MockDataExec::new(schema.clone(), vec![subquery_data]));
    let subquery_scan = SubqueryScanExec::new(subquery);

    let result = subquery_scan.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    assert_eq!(subquery_scan.schema().fields().len(), 2);
    assert_eq!(subquery_scan.schema().fields()[0].name, "id");
    assert_eq!(subquery_scan.schema().fields()[1].name, "name");
}

#[test]
fn test_multi_column_set_operations() {
    let schema = yachtsql_storage::Schema::from_fields(vec![
        yachtsql_storage::Field::required("id".to_string(), yachtsql_core::types::DataType::Int64),
        yachtsql_storage::Field::required(
            "value".to_string(),
            yachtsql_core::types::DataType::String,
        ),
    ]);

    let left_data = create_test_batch(
        schema.clone(),
        vec![
            vec![
                yachtsql_core::types::Value::int64(1),
                yachtsql_core::types::Value::string("a".to_string()),
            ],
            vec![
                yachtsql_core::types::Value::int64(2),
                yachtsql_core::types::Value::string("b".to_string()),
            ],
        ],
    );

    let right_data = create_test_batch(
        schema.clone(),
        vec![
            vec![
                yachtsql_core::types::Value::int64(2),
                yachtsql_core::types::Value::string("b".to_string()),
            ],
            vec![
                yachtsql_core::types::Value::int64(3),
                yachtsql_core::types::Value::string("c".to_string()),
            ],
        ],
    );

    let left = Rc::new(MockDataExec::new(schema.clone(), vec![left_data]));
    let right = Rc::new(MockDataExec::new(schema.clone(), vec![right_data]));

    let union = UnionExec::new(left.clone(), right.clone(), false).unwrap();
    let result = union.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    let intersect = IntersectExec::new(left.clone(), right.clone(), false).unwrap();
    let result = intersect.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    let except = ExceptExec::new(left, right, false).unwrap();
    let result = except.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[test]
fn test_null_handling_in_set_operations() {
    let schema = yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::nullable(
        "value".to_string(),
        yachtsql_core::types::DataType::Int64,
    )]);

    let left_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::int64(1)],
            vec![yachtsql_core::types::Value::null()],
            vec![yachtsql_core::types::Value::int64(3)],
        ],
    );

    let right_data = create_test_batch(
        schema.clone(),
        vec![
            vec![yachtsql_core::types::Value::null()],
            vec![yachtsql_core::types::Value::int64(3)],
            vec![yachtsql_core::types::Value::int64(4)],
        ],
    );

    let left = Rc::new(MockDataExec::new(schema.clone(), vec![left_data]));
    let right = Rc::new(MockDataExec::new(schema.clone(), vec![right_data]));

    let union = UnionExec::new(left.clone(), right.clone(), true).unwrap();
    let result = union.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 6);

    let intersect = IntersectExec::new(left.clone(), right.clone(), true).unwrap();
    let result = intersect.execute().unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}
