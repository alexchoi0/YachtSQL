use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_date32_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE date32_test (id INT64, d Date32)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO date32_test VALUES (1, '2023-01-15'), (2, '1900-01-01')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, d FROM date32_test ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_date32_extended_range() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE date32_range (d Date32)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO date32_range VALUES ('1900-01-01'), ('2299-12-31')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT d FROM date32_range ORDER BY d")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_date32_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE date32_cmp (id INT64, d Date32)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO date32_cmp VALUES (1, '2023-01-01'), (2, '2023-06-15'), (3, '2023-12-31')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM date32_cmp WHERE d > '2023-03-01' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_date32_ordering() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE date32_ord (id INT64, d Date32)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO date32_ord VALUES (1, '2023-12-31'), (2, '2023-01-01'), (3, '2023-06-15')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM date32_ord ORDER BY d")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[test]
fn test_date32_arithmetic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE date32_arith (d Date32)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO date32_arith VALUES ('2023-01-15')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT d + 10, d - 5 FROM date32_arith")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_date32_functions() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE date32_func (d Date32)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO date32_func VALUES ('2023-06-15')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT toYear(d), toMonth(d), toDayOfMonth(d) FROM date32_func")
        .unwrap();
    assert_table_eq!(result, [[2023, 6, 15]]);
}

#[test]
fn test_date32_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE date32_null (id INT64, d Nullable(Date32))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO date32_null VALUES (1, '2023-01-01'), (2, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM date32_null WHERE d IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_date32_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE date32_group (d Date32, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO date32_group VALUES ('2023-01-01', 10), ('2023-01-01', 20), ('2023-01-02', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT d, SUM(value) FROM date32_group GROUP BY d ORDER BY d")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_date32_between() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE date32_between (id INT64, d Date32)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO date32_between VALUES (1, '2023-01-01'), (2, '2023-06-15'), (3, '2023-12-31')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM date32_between WHERE d BETWEEN '2023-03-01' AND '2023-09-30' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_date32_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE date32_distinct (d Date32)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO date32_distinct VALUES ('2023-01-01'), ('2023-01-01'), ('2023-01-02')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT d) FROM date32_distinct")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}
