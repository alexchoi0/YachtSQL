use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_datetime64_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_test (id INT64, ts DateTime64(3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_test VALUES (1, '2023-01-15 10:30:45.123')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, ts FROM dt64_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_datetime64_precision_3() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_ms (ts DateTime64(3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_ms VALUES ('2023-01-15 10:30:45.123')")
        .unwrap();

    let result = executor.execute_sql("SELECT ts FROM dt64_ms").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_datetime64_precision_6() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_us (ts DateTime64(6))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_us VALUES ('2023-01-15 10:30:45.123456')")
        .unwrap();

    let result = executor.execute_sql("SELECT ts FROM dt64_us").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_datetime64_precision_9() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_ns (ts DateTime64(9))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_ns VALUES ('2023-01-15 10:30:45.123456789')")
        .unwrap();

    let result = executor.execute_sql("SELECT ts FROM dt64_ns").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_datetime64_with_timezone() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_tz (ts DateTime64(3, 'UTC'))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_tz VALUES ('2023-01-15 10:30:45.123')")
        .unwrap();

    let result = executor.execute_sql("SELECT ts FROM dt64_tz").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_datetime64_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_cmp (id INT64, ts DateTime64(3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_cmp VALUES (1, '2023-01-01 00:00:00.000'), (2, '2023-06-15 12:00:00.000'), (3, '2023-12-31 23:59:59.999')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM dt64_cmp WHERE ts > '2023-03-01 00:00:00.000' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_datetime64_ordering() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_ord (id INT64, ts DateTime64(3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_ord VALUES (1, '2023-12-31 23:59:59.999'), (2, '2023-01-01 00:00:00.000'), (3, '2023-06-15 12:00:00.000')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM dt64_ord ORDER BY ts")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[test]
#[ignore = "toHour/toMinute/toSecond not yet implemented for DateTime64"]
fn test_datetime64_functions() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_func (ts DateTime64(3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_func VALUES ('2023-06-15 14:30:45.123')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT toYear(ts), toMonth(ts), toDayOfMonth(ts), toHour(ts), toMinute(ts), toSecond(ts) FROM dt64_func")
        .unwrap();
    assert_table_eq!(result, [[2023, 6, 15, 14, 30, 45]]);
}

#[test]
fn test_datetime64_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_null (id INT64, ts Nullable(DateTime64(3)))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_null VALUES (1, '2023-01-01 00:00:00.000'), (2, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM dt64_null WHERE ts IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore = "addSeconds/subtractSeconds not yet implemented"]
fn test_datetime64_arithmetic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_arith (ts DateTime64(3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_arith VALUES ('2023-01-15 10:30:45.000')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT addSeconds(ts, 10), subtractSeconds(ts, 5) FROM dt64_arith")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
#[ignore = "GROUP BY on function expressions not yet supported"]
fn test_datetime64_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_group (ts DateTime64(3), value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_group VALUES ('2023-01-01 00:00:00.000', 10), ('2023-01-01 00:00:00.000', 20), ('2023-01-02 00:00:00.000', 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT toDate(ts), SUM(value) FROM dt64_group GROUP BY toDate(ts) ORDER BY toDate(ts)",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
#[ignore = "now64 function not yet implemented"]
fn test_datetime64_now() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT now64(3)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
#[ignore = "DateTime64 millisecond-precision DISTINCT not yet supported"]
fn test_datetime64_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dt64_distinct (ts DateTime64(3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dt64_distinct VALUES ('2023-01-01 00:00:00.000'), ('2023-01-01 00:00:00.000'), ('2023-01-01 00:00:00.001')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT ts) FROM dt64_distinct")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}
