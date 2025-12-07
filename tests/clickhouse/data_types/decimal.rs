use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_decimal32_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE decimal32_test (id INT64, price Decimal32(2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO decimal32_test VALUES (1, 123.45), (2, 678.90)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, price FROM decimal32_test ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_decimal64_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE decimal64_test (id INT64, amount Decimal64(4))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO decimal64_test VALUES (1, 12345.6789)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, amount FROM decimal64_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_decimal128_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE decimal128_test (id INT64, big_val Decimal128(10))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO decimal128_test VALUES (1, 12345678901234567890.1234567890)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM decimal128_test")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_decimal_arithmetic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE decimal_arith (a Decimal32(2), b Decimal32(2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO decimal_arith VALUES (10.50, 3.25)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b, a - b, a * b, a / b FROM decimal_arith")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_decimal_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE decimal_cmp (id INT64, val Decimal32(2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO decimal_cmp VALUES (1, 10.00), (2, 20.00), (3, 15.00)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM decimal_cmp WHERE val > 12.00 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_decimal_ordering() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE decimal_ord (id INT64, price Decimal32(2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO decimal_ord VALUES (1, 99.99), (2, 10.00), (3, 50.50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM decimal_ord ORDER BY price")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_decimal_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE decimal_agg (price Decimal32(2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO decimal_agg VALUES (10.00), (20.00), (30.00)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(price), AVG(price), MIN(price), MAX(price) FROM decimal_agg")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_decimal_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE decimal_null (id INT64, val Nullable(Decimal32(2)))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO decimal_null VALUES (1, 10.00), (2, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM decimal_null WHERE val IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_decimal_cast() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE decimal_cast (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO decimal_cast VALUES (100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT toDecimal32(val, 2) FROM decimal_cast")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_decimal_round() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE decimal_round (val Decimal32(4))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO decimal_round VALUES (123.4567)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT round(val, 2) FROM decimal_round")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_decimal_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE decimal_group (category Decimal32(0), value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO decimal_group VALUES (1, 10), (2, 20), (1, 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, SUM(value) FROM decimal_group GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}
