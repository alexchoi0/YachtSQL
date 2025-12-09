use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_host_name() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT hostName()").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_fqdn() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT FQDN()").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_version() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT version()").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_uptime() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT uptime()").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_timezone() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT timezone()").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_current_database() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT currentDatabase()").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_current_user() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT currentUser()").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_is_constant() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT isConstant(123)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_finite() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT isFinite(1.0)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_infinite() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT isInfinite(1e309)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_nan() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT isNaN(0.0 / 0.0)").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_has_column_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test_table (id INT64, name STRING)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT hasColumnInTable(currentDatabase(), 'test_table', 'id')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_bar() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT bar(50, 0, 100, 10)").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_format_readable_size() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT formatReadableSize(1073741824)")
        .unwrap();
    assert_table_eq!(result, [["1.00 GiB"]]);
}

#[test]
fn test_format_readable_quantity() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT formatReadableQuantity(1000000)")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_format_readable_time_delta() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT formatReadableTimeDelta(3661)")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_sleep() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT sleep(0.001)").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_throw_if() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT throwIf(0, 'error message')")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_materialize() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT materialize(123)").unwrap();
    assert_table_eq!(result, [[123]]);
}

#[test]
fn test_ignore() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ignore(1, 2, 3)").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_identity() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT identity(123)").unwrap();
    assert_table_eq!(result, [[123]]);
}

#[test]
fn test_get_setting() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT getSetting('max_threads')")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[ignore = "Requires array literal parsing"]
#[test]
fn test_transform() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT transform(1, [1, 2, 3], ['one', 'two', 'three'], 'unknown')")
        .unwrap();
    assert_table_eq!(result, [["one"]]);
}

#[ignore = "Requires array literal parsing"]
#[test]
fn test_transform_not_found() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT transform(5, [1, 2, 3], ['one', 'two', 'three'], 'unknown')")
        .unwrap();
    assert_table_eq!(result, [["unknown"]]);
}

#[ignore = "Requires arrayJoin table function"]
#[test]
fn test_array_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE array_data (id INT64, arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO array_data VALUES (1, [1, 2, 3])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, arrayJoin(arr) AS val FROM array_data ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[1, 1], [1, 2], [1, 3]]);
}

#[test]
fn test_model_evaluate() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT modelEvaluate('model', 1, 2, 3)")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[ignore = "Requires aggregate state functions"]
#[test]
fn test_run_callback_in_final() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT runningAccumulate(sum_state) FROM (SELECT sumState(1) AS sum_state) AS t",
        )
        .unwrap();
    assert!(result.num_rows() == 1);
}
