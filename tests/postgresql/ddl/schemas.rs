use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_schema_basic() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA myschema").unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_schema_if_not_exists() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA test_schema").unwrap();
    executor
        .execute_sql("CREATE SCHEMA IF NOT EXISTS test_schema")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_drop_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA drop_schema").unwrap();
    executor.execute_sql("DROP SCHEMA drop_schema").unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_drop_schema_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP SCHEMA IF EXISTS nonexistent_schema")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_drop_schema_cascade() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SCHEMA cascade_schema")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE cascade_schema.test_table (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("DROP SCHEMA cascade_schema CASCADE")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_drop_schema_restrict() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SCHEMA restrict_schema")
        .unwrap();
    executor
        .execute_sql("DROP SCHEMA restrict_schema RESTRICT")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_table_in_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA data").unwrap();
    executor
        .execute_sql("CREATE TABLE data.users (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data.users VALUES (1, 'Alice')")
        .unwrap();

    let result = executor.execute_sql("SELECT name FROM data.users").unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_schema_qualified_name() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA app").unwrap();
    executor
        .execute_sql("CREATE TABLE app.items (id INTEGER, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO app.items VALUES (1, 100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT app.items.value FROM app.items")
        .unwrap();
    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_set_search_path() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA myapp").unwrap();
    executor.execute_sql("SET search_path TO myapp").unwrap();
    executor
        .execute_sql("CREATE TABLE items (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM items").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_schema_authorization() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SCHEMA auth_schema AUTHORIZATION CURRENT_USER")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_current_schema() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CURRENT_SCHEMA()").unwrap();
    assert_table_eq!(result, [["public"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_information_schema_tables() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE info_test (id INTEGER)")
        .unwrap();
    let result = executor
        .execute_sql(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'info_test'",
        )
        .unwrap();
    assert_table_eq!(result, [["info_test"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_information_schema_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE col_test (id INTEGER, name TEXT)")
        .unwrap();
    let result = executor
        .execute_sql(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'col_test' ORDER BY ordinal_position",
        )
        .unwrap();
    assert_table_eq!(result, [["id"], ["name"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_schema() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 1 FROM pg_catalog.pg_tables LIMIT 1")
        .unwrap();
    assert!(result.num_rows() <= 1);
}

#[test]
fn test_public_schema() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE public.pub_test (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO public.pub_test VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM public.pub_test")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_alter_schema_rename() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA old_name").unwrap();
    executor
        .execute_sql("ALTER SCHEMA old_name RENAME TO new_name")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_alter_schema_owner() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA owner_schema").unwrap();
    executor
        .execute_sql("ALTER SCHEMA owner_schema OWNER TO CURRENT_USER")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_schema_search_path_multiple() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA schema1").unwrap();
    executor.execute_sql("CREATE SCHEMA schema2").unwrap();
    executor
        .execute_sql("SET search_path TO schema1, schema2, public")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_show_search_path() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SHOW search_path").unwrap();
    assert_table_eq!(result, [["\"$user\", public"]]);
}

#[test]
fn test_schema_with_tables_and_indexes() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA indexed").unwrap();
    executor
        .execute_sql("CREATE TABLE indexed.data (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_data_name ON indexed.data (name)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO indexed.data VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM indexed.data")
        .unwrap();
    assert_table_eq!(result, [["test"]]);
}
