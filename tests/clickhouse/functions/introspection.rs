#![allow(unused_variables)]

use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_current_database() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT currentDatabase()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_current_user() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT currentUser()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_version() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT version()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_uptime() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT uptime()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_timezone() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT timezone()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_server_timezone() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT serverTimezone()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_block_number() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE block_test (id Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO block_test VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, blockNumber() FROM block_test")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_row_number_in_block() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE row_test (name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO row_test VALUES ('a'), ('b'), ('c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, rowNumberInBlock() FROM row_test")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_row_number_in_all_blocks() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE row_all_test (val Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO row_all_test VALUES (10), (20), (30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val, rowNumberInAllBlocks() FROM row_all_test")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_host_name() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT hostName()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_fqdn() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT FQDN()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_is_finite() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT isFinite(1.0), isFinite(inf), isFinite(nan)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_is_infinite() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT isInfinite(1.0), isInfinite(inf), isInfinite(-inf)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_is_nan() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT isNaN(1.0), isNaN(nan)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_type_name() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toTypeName(1), toTypeName('hello'), toTypeName(3.14)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_dump_column_structure() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT dumpColumnStructure(1)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_default_value_of_argument_type() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT defaultValueOfArgumentType(toInt32(0))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_default_value_of_type_name() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT defaultValueOfTypeName('Int32')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_block_size() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE block_size_test (x Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO block_size_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT blockSize() FROM block_size_test LIMIT 1")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_materialized_view_refresh() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT currentSchemas()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_query_id() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT queryID()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_initial_query_id() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT initialQueryID()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_server_uuid() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT serverUUID()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_get_setting() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT getSetting('max_threads')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_is_decimal_overflow() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT isDecimalOverflow(toDecimal32(1, 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_count_digits() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT countDigits(12345)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_file_name() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE file_test (id Int64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    executor
        .execute_sql("INSERT INTO file_test VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT _file FROM file_test LIMIT 1")
        .unwrap();
    // TODO: Replace with proper table! assertion
}

#[ignore = "Implement me!"]
#[test]
fn test_introspection_in_select() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE introspect_test (val Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO introspect_test VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT val,
                rowNumberInBlock() AS row_num,
                currentDatabase() AS db
            FROM introspect_test",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}
