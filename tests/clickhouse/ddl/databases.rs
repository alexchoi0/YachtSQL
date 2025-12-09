use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_database() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE test_db").unwrap();
}

#[test]
fn test_create_database_if_not_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DATABASE IF NOT EXISTS test_db")
        .unwrap();
    executor
        .execute_sql("CREATE DATABASE IF NOT EXISTS test_db")
        .unwrap();
}

#[test]
fn test_drop_database() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE drop_db").unwrap();
    executor.execute_sql("DROP DATABASE drop_db").unwrap();
}

#[test]
fn test_drop_database_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP DATABASE IF EXISTS nonexistent_db")
        .unwrap();
}

#[test]
fn test_create_database_with_engine() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DATABASE atomic_db ENGINE = Atomic")
        .unwrap();
}

#[test]
fn test_create_database_lazy() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DATABASE lazy_db ENGINE = Lazy(3600)")
        .unwrap();
}

#[test]
fn test_show_databases() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE show_db1").unwrap();
    executor.execute_sql("CREATE DATABASE show_db2").unwrap();
    let result = executor.execute_sql("SHOW DATABASES").unwrap();
    assert!(result.num_rows() >= 2);
}

#[test]
fn test_use_database() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE use_db").unwrap();
    executor.execute_sql("USE use_db").unwrap();
}

#[test]
fn test_create_table_in_database() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE table_db").unwrap();
    executor
        .execute_sql("CREATE TABLE table_db.test_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO table_db.test_table VALUES (1)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT id FROM table_db.test_table")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_database_qualified_name() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE qual_db").unwrap();
    executor
        .execute_sql("CREATE TABLE qual_db.t1 (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE qual_db.t2 (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO qual_db.t1 VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO qual_db.t2 VALUES (2)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT qual_db.t1.id, qual_db.t2.id FROM qual_db.t1, qual_db.t2")
        .unwrap();
    assert_table_eq!(result, [[1, 2]]);
}

#[test]
fn test_rename_database() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE old_name").unwrap();
    executor
        .execute_sql("RENAME DATABASE old_name TO new_name")
        .unwrap();
}

#[test]
fn test_database_comment() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DATABASE comment_db COMMENT 'Test database'")
        .unwrap();
}
