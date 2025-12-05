use std::rc::Rc;

use yachtsql_executor::QueryExecutor;
use yachtsql_executor::query_executor::evaluator::physical_plan::ExecutionPlan;
use yachtsql_storage::table::indexes::TableIndexOps;

fn create_test_executor_with_table() -> QueryExecutor {
    let mut executor = QueryExecutor::new();

    {
        let mut storage = executor.storage.borrow_mut();
        let _ = storage.create_dataset("default".to_string());
    }

    executor
        .execute_sql(
            "CREATE TABLE default.users (
            id INT64 NOT NULL,
            name STRING,
            age INT64,
            email STRING
        )",
        )
        .expect("Failed to create table");

    executor
        .execute_sql("INSERT INTO default.users VALUES (1, 'Alice', 30, 'alice@example.com')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO default.users VALUES (2, 'Bob', 25, 'bob@example.com')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO default.users VALUES (3, 'Charlie', 35, 'charlie@example.com')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO default.users VALUES (4, 'Diana', 28, 'diana@example.com')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO default.users VALUES (5, 'Eve', 32, 'eve@example.com')")
        .unwrap();

    executor
}

#[test]
fn test_create_index_basic() {
    let mut executor = create_test_executor_with_table();

    let result = executor.execute_sql("CREATE INDEX idx_users_id ON default.users(id)");
    assert!(result.is_ok(), "Failed to create index: {:?}", result.err());

    let storage = executor.storage.borrow_mut();
    let dataset = storage.get_dataset("default").expect("Dataset not found");
    let table = dataset.get_table("users").expect("Table not found");

    let has_index = table
        .index_metadata()
        .iter()
        .any(|idx| idx.index_name == "idx_users_id");
    assert!(has_index, "Index metadata not found");
}

#[test]
fn test_create_unique_index() {
    let mut executor = create_test_executor_with_table();

    let result =
        executor.execute_sql("CREATE UNIQUE INDEX idx_users_email ON default.users(email)");
    assert!(
        result.is_ok(),
        "Failed to create unique index: {:?}",
        result.err()
    );

    let storage = executor.storage.borrow_mut();
    let dataset = storage.get_dataset("default").expect("Dataset not found");
    let table = dataset.get_table("users").expect("Table not found");

    let index = table
        .index_metadata()
        .iter()
        .find(|idx| idx.index_name == "idx_users_email")
        .expect("Index not found");

    assert!(index.is_unique, "Index should be marked as unique");
}

#[test]
fn test_create_composite_index() {
    let mut executor = create_test_executor_with_table();

    let result =
        executor.execute_sql("CREATE INDEX idx_users_name_age ON default.users(name, age)");
    assert!(
        result.is_ok(),
        "Failed to create composite index: {:?}",
        result.err()
    );

    let storage = executor.storage.borrow_mut();
    let dataset = storage.get_dataset("default").expect("Dataset not found");
    let table = dataset.get_table("users").expect("Table not found");

    let index = table
        .index_metadata()
        .iter()
        .find(|idx| idx.index_name == "idx_users_name_age")
        .expect("Index not found");

    assert_eq!(
        index.columns.len(),
        2,
        "Composite index should have 2 columns"
    );
}

#[test]
fn test_drop_index() {
    let mut executor = create_test_executor_with_table();

    executor
        .execute_sql("CREATE INDEX idx_users_id ON default.users(id)")
        .unwrap();

    {
        let storage = executor.storage.borrow_mut();
        let dataset = storage.get_dataset("default").unwrap();
        let table = dataset.get_table("users").unwrap();
        assert!(
            table
                .index_metadata()
                .iter()
                .any(|idx| idx.index_name == "idx_users_id")
        );
    }

    let result = executor.execute_sql("DROP INDEX idx_users_id");
    assert!(result.is_ok(), "Failed to drop index: {:?}", result.err());

    let storage = executor.storage.borrow_mut();
    let dataset = storage.get_dataset("default").unwrap();
    let table = dataset.get_table("users").unwrap();
    assert!(
        !table
            .index_metadata()
            .iter()
            .any(|idx| idx.index_name == "idx_users_id"),
        "Index should be removed"
    );
}

#[test]
fn test_index_integrity_after_delete() {
    let mut executor = create_test_executor_with_table();

    executor
        .execute_sql("CREATE INDEX idx_users_id ON default.users(id)")
        .unwrap();

    let initial_row_count = {
        let storage = executor.storage.borrow_mut();
        let dataset = storage.get_dataset("default").unwrap();
        let table = dataset.get_table("users").unwrap();
        table.row_count()
    };
    assert_eq!(initial_row_count, 5, "Should have 5 rows initially");

    let result = executor.execute_sql("DELETE FROM default.users WHERE id = 3");
    assert!(result.is_ok(), "Failed to delete row: {:?}", result.err());

    let final_row_count = {
        let storage = executor.storage.borrow_mut();
        let dataset = storage.get_dataset("default").unwrap();
        let table = dataset.get_table("users").unwrap();
        table.row_count()
    };
    assert_eq!(final_row_count, 4, "Should have 4 rows after deletion");
}

#[test]
fn test_index_integrity_after_multiple_deletes() {
    let mut executor = create_test_executor_with_table();

    executor
        .execute_sql("CREATE INDEX idx_users_id ON default.users(id)")
        .unwrap();

    executor
        .execute_sql("DELETE FROM default.users WHERE age > 30")
        .unwrap();

    let storage = executor.storage.borrow_mut();
    let dataset = storage.get_dataset("default").unwrap();
    let table = dataset.get_table("users").unwrap();

    assert_eq!(
        table.row_count(),
        3,
        "Should have 3 rows remaining after delete"
    );
}

#[test]
fn test_index_with_insert_operations() {
    let mut executor = create_test_executor_with_table();

    executor
        .execute_sql("CREATE INDEX idx_users_id ON default.users(id)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO default.users VALUES (6, 'Frank', 40, 'frank@example.com')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO default.users VALUES (7, 'Grace', 22, 'grace@example.com')")
        .unwrap();

    let storage = executor.storage.borrow_mut();
    let dataset = storage.get_dataset("default").unwrap();
    let table = dataset.get_table("users").unwrap();

    assert_eq!(table.row_count(), 7, "Should have 7 rows after insertions");
}

#[test]
fn test_index_with_update_operations() {
    let mut executor = create_test_executor_with_table();

    executor
        .execute_sql("CREATE INDEX idx_users_age ON default.users(age)")
        .unwrap();

    let result = executor.execute_sql("UPDATE default.users SET age = 26 WHERE name = 'Bob'");
    assert!(result.is_ok(), "Failed to update row: {:?}", result.err());

    let storage = executor.storage.borrow_mut();
    let dataset = storage.get_dataset("default").unwrap();
    let table = dataset.get_table("users").unwrap();

    assert_eq!(
        table.row_count(),
        5,
        "Row count should be unchanged after update"
    );
}

#[test]
fn test_physical_planner_creates_index_scan_exec() {
    use yachtsql_executor::query_executor::LogicalToPhysicalPlanner;

    let mut executor = create_test_executor_with_table();
    executor
        .execute_sql("CREATE INDEX idx_users_id ON default.users (id)")
        .expect("Failed to create index");

    let predicate = yachtsql_optimizer::expr::Expr::BinaryOp {
        left: Box::new(yachtsql_optimizer::expr::Expr::Column {
            name: "id".to_string(),
            table: None,
        }),
        op: yachtsql_optimizer::BinaryOp::Equal,
        right: Box::new(yachtsql_optimizer::expr::Expr::Literal(
            yachtsql_optimizer::expr::LiteralValue::Int64(42),
        )),
    };

    let index_scan_node = yachtsql_optimizer::plan::PlanNode::IndexScan {
        table_name: "default.users".to_string(),
        alias: None,
        index_name: "idx_users_id".to_string(),
        predicate,
        projection: None,
    };

    let logical_plan = yachtsql_optimizer::LogicalPlan::new(index_scan_node);

    let planner = LogicalToPhysicalPlanner::new(Rc::clone(&executor.storage));

    let physical_plan = planner
        .create_physical_plan(&logical_plan)
        .expect("Failed to create physical plan");

    let plan_description = physical_plan.root().describe();

    assert!(
        plan_description.contains("IndexScan"),
        "Physical plan should use IndexScan operator, got: {}",
        plan_description
    );
}

#[test]
fn test_index_scan_exec_statistics() {
    use std::cell::RefCell;
    use std::rc::Rc;

    use yachtsql_executor::query_executor::evaluator::physical_plan::IndexScanExec;
    use yachtsql_storage::{Schema, Storage};

    let schema = Schema::from_fields(vec![]);
    let predicate = yachtsql_optimizer::expr::Expr::BinaryOp {
        left: Box::new(yachtsql_optimizer::expr::Expr::Column {
            name: "id".to_string(),
            table: None,
        }),
        op: yachtsql_optimizer::BinaryOp::Equal,
        right: Box::new(yachtsql_optimizer::expr::Expr::Literal(
            yachtsql_optimizer::expr::LiteralValue::Int64(42),
        )),
    };

    let storage = Rc::new(RefCell::new(Storage::new()));

    let index_scan = IndexScanExec::new(
        schema,
        "users".to_string(),
        "idx_users_id".to_string(),
        predicate,
        storage,
    );

    let stats = index_scan.statistics();
    assert_eq!(
        stats.num_rows,
        Some(100),
        "IndexScan should estimate 100 rows (selective)"
    );
}

#[test]
fn test_create_index_on_nonexistent_column_fails() {
    let mut executor = create_test_executor_with_table();

    let result = executor.execute_sql("CREATE INDEX idx_bad ON default.users(nonexistent_column)");

    assert!(
        result.is_err(),
        "Creating index on non-existent column should fail"
    );
}

#[test]
fn test_create_duplicate_index_name_fails() {
    let mut executor = create_test_executor_with_table();

    executor
        .execute_sql("CREATE INDEX idx_users_id ON default.users(id)")
        .unwrap();

    let result = executor.execute_sql("CREATE INDEX idx_users_id ON default.users(name)");

    assert!(result.is_err(), "Creating duplicate index name should fail");
}

#[test]
fn test_drop_nonexistent_index_fails() {
    let mut executor = create_test_executor_with_table();

    let result = executor.execute_sql("DROP INDEX test_db.idx_nonexistent");

    assert!(result.is_err(), "Dropping non-existent index should fail");
}

#[test]
fn test_index_hash_type_creation() {
    let mut executor = create_test_executor_with_table();

    let result = executor.execute_sql("CREATE INDEX idx_users_id ON default.users USING HASH (id)");
    assert!(
        result.is_ok(),
        "Failed to create Hash index: {:?}",
        result.err()
    );

    let storage = executor.storage.borrow_mut();
    let dataset = storage.get_dataset("default").unwrap();
    let table = dataset.get_table("users").unwrap();

    let index = table
        .index_metadata()
        .iter()
        .find(|idx| idx.index_name == "idx_users_id")
        .expect("Index not found");

    assert_eq!(
        index.index_type,
        yachtsql_storage::index::IndexType::Hash,
        "Index should be Hash type"
    );
}

#[test]
fn test_index_btree_type_creation() {
    let mut executor = create_test_executor_with_table();

    let result =
        executor.execute_sql("CREATE INDEX idx_users_age ON default.users USING BTREE (age)");
    assert!(
        result.is_ok(),
        "Failed to create B-tree index: {:?}",
        result.err()
    );

    let storage = executor.storage.borrow_mut();
    let dataset = storage.get_dataset("default").unwrap();
    let table = dataset.get_table("users").unwrap();

    let index = table
        .index_metadata()
        .iter()
        .find(|idx| idx.index_name == "idx_users_age")
        .expect("Index not found");

    assert_eq!(
        index.index_type,
        yachtsql_storage::index::IndexType::BTree,
        "Index should be BTree type"
    );
}
