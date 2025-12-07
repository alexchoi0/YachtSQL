use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();

    executor
        .execute_sql("CREATE TABLE my_schema.users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO my_schema.users VALUES (1, 'Alice')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM my_schema.users")
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_create_schema_if_not_exists() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();

    let result = executor.execute_sql("CREATE SCHEMA IF NOT EXISTS my_schema");
    assert!(result.is_ok());
}

#[test]
#[ignore = "Implement me!"]
fn test_drop_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA temp_schema").unwrap();
    executor.execute_sql("DROP SCHEMA temp_schema").unwrap();

    let result = executor.execute_sql("CREATE TABLE temp_schema.test (id INT64)");
    assert!(result.is_err());
}

#[test]
fn test_drop_schema_if_exists() {
    let mut executor = create_executor();

    let result = executor.execute_sql("DROP SCHEMA IF EXISTS nonexistent_schema");
    assert!(result.is_ok());
}

#[test]
fn test_drop_schema_cascade() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();
    executor
        .execute_sql("CREATE TABLE my_schema.table1 (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE my_schema.table2 (id INT64)")
        .unwrap();

    executor
        .execute_sql("DROP SCHEMA my_schema CASCADE")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM my_schema.table1");
    assert!(result.is_err());
}

#[test]
fn test_schema_qualified_table_names() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA schema1").unwrap();
    executor.execute_sql("CREATE SCHEMA schema2").unwrap();

    executor
        .execute_sql("CREATE TABLE schema1.data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE schema2.data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO schema1.data VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO schema2.data VALUES (1, 200)")
        .unwrap();

    let result1 = executor
        .execute_sql("SELECT value FROM schema1.data")
        .unwrap();
    let result2 = executor
        .execute_sql("SELECT value FROM schema2.data")
        .unwrap();

    assert_table_eq!(result1, [[100]]);
    assert_table_eq!(result2, [[200]]);
}

#[test]
fn test_schema_with_options() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SCHEMA my_schema OPTIONS(description='My test schema')")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE my_schema.test (id INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT 1 FROM my_schema.test");
    assert!(result.is_ok());
}

#[test]
#[ignore = "Implement me!"]
fn test_alter_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();

    executor
        .execute_sql("ALTER SCHEMA my_schema SET OPTIONS(description='Updated description')")
        .unwrap();
}

#[test]
fn test_cross_schema_join() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA schema_a").unwrap();
    executor.execute_sql("CREATE SCHEMA schema_b").unwrap();

    executor
        .execute_sql("CREATE TABLE schema_a.users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE schema_b.orders (id INT64, user_id INT64, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO schema_a.users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO schema_b.orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT u.name, SUM(o.amount) AS total FROM schema_a.users u JOIN schema_b.orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name")
        .unwrap();
    assert_table_eq!(result, [["Alice", 300], ["Bob", 150]]);
}

#[test]
fn test_create_view_in_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();
    executor
        .execute_sql("CREATE TABLE my_schema.data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO my_schema.data VALUES (1, 10), (2, 20)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE VIEW my_schema.data_view AS SELECT * FROM my_schema.data WHERE value > 15",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM my_schema.data_view")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_default_schema() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE public_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO public_table VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM public_table").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_schema_search_path() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();
    executor
        .execute_sql("CREATE TABLE my_schema.test (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO my_schema.test VALUES (1)")
        .unwrap();

    executor
        .execute_sql("SET search_path TO my_schema")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM test").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_truncate_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    executor.execute_sql("TRUNCATE TABLE data").unwrap();

    let result = executor.execute_sql("SELECT COUNT(*) FROM data").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_truncate_table_in_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();
    executor
        .execute_sql("CREATE TABLE my_schema.data (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO my_schema.data VALUES (1), (2), (3)")
        .unwrap();

    executor
        .execute_sql("TRUNCATE TABLE my_schema.data")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM my_schema.data")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}
