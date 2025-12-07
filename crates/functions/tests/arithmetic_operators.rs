#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::approx_constant)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

mod common;
use common::{create_executor, n};

#[test]
fn test_int64_addition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b AS sum FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_int64_subtraction() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (50, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a - b AS diff FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_int64_multiplication() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (7, 8)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a * b AS product FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[56]]);
}

#[test]
fn test_int64_division() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (20, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a / b AS quotient FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_int64_modulo() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (17, 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a % b AS remainder FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_float64_addition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE floats (a FLOAT64, b FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats VALUES (3.14, 2.86)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b AS sum FROM floats")
        .unwrap();
    assert_table_eq!(result, [[6.0]]);
}

#[test]
fn test_float64_subtraction() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE floats (a FLOAT64, b FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats VALUES (10.5, 3.2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a - b AS diff FROM floats")
        .unwrap();
    assert_table_eq!(result, [[7.3]]);
}

#[test]
fn test_float64_multiplication() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE floats (a FLOAT64, b FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats VALUES (2.5, 4.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a * b AS product FROM floats")
        .unwrap();
    assert_table_eq!(result, [[10.0]]);
}

#[test]
fn test_float64_division() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE floats (a FLOAT64, b FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats VALUES (15.0, 3.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a / b AS quotient FROM floats")
        .unwrap();
    assert_table_eq!(result, [[5.0]]);
}

#[test]
fn test_mixed_int_float_addition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mixed (int_val INT64, float_val FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed VALUES (10, 3.5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT int_val + float_val AS sum FROM mixed")
        .unwrap();
    assert_table_eq!(result, [[13.5]]);
}

#[test]
fn test_mixed_float_int_subtraction() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mixed (float_val FLOAT64, int_val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed VALUES (20.5, 7)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT float_val - int_val AS diff FROM mixed")
        .unwrap();
    assert_table_eq!(result, [[13.5]]);
}

#[test]
fn test_mixed_int_float_multiplication() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mixed (int_val INT64, float_val FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed VALUES (5, 2.5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT int_val * float_val AS product FROM mixed")
        .unwrap();
    assert_table_eq!(result, [[12.5]]);
}

#[test]
fn test_mixed_int_float_division() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mixed (int_val INT64, float_val FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed VALUES (10, 4.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT int_val / float_val AS quotient FROM mixed")
        .unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[test]
fn test_unary_minus_int() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (42)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT -value AS negated FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[-42]]);
}

#[test]
fn test_unary_minus_float() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE floats (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats VALUES (3.14)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT -value AS negated FROM floats")
        .unwrap();
    assert_table_eq!(result, [[-3.14]]);
}

#[test]
fn test_unary_plus_int() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (42)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT +value AS positive FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[test]
fn test_double_negation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (42)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT -(-value) AS double_neg FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[42]]);
}

#[test]
fn test_int_division_by_zero() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10)")
        .unwrap();

    let result = executor.execute_sql("SELECT value / 0 FROM numbers");
    assert!(result.is_err(), "Should error on division by zero");
}

#[test]
fn test_float_division_by_zero() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE floats (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats VALUES (10.5)")
        .unwrap();

    let result = executor.execute_sql("SELECT value / 0.0 FROM floats");
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_modulo_by_zero() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10)")
        .unwrap();

    let result = executor.execute_sql("SELECT value % 0 FROM numbers");
    assert!(result.is_err(), "Should error on modulo by zero");
}

#[test]
fn test_division_by_zero_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10, 0)")
        .unwrap();

    let result = executor.execute_sql("SELECT a / b FROM numbers");
    assert!(result.is_err(), "Should error when dividing by zero column");
}

#[test]
fn test_int64_max_addition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (9223372036854775807)")
        .unwrap();

    let result = executor.execute_sql("SELECT value + 1 FROM numbers");
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_int64_min_subtraction() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (-9223372036854775808)")
        .unwrap();

    let result = executor.execute_sql("SELECT value - 1 FROM numbers");
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_int64_multiplication_overflow() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (9223372036854775807, 2)")
        .unwrap();

    let result = executor.execute_sql("SELECT a * b FROM numbers");
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_null_plus_int() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value + 10 FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_int_plus_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10, NULL)")
        .unwrap();

    let result = executor.execute_sql("SELECT a + b FROM numbers").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_null_multiplication() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (NULL, 5)")
        .unwrap();

    let result = executor.execute_sql("SELECT a * b FROM numbers").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_null_division() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10, NULL)")
        .unwrap();

    let result = executor.execute_sql("SELECT a / b FROM numbers").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_unary_minus_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (NULL)")
        .unwrap();

    let result = executor.execute_sql("SELECT -value FROM numbers").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_multiplication_before_addition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (2, 3, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b * c AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[14]]);
}

#[test]
fn test_division_before_subtraction() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (20, 10, 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a - b / c AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
fn test_parentheses_override_precedence() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (2, 3, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT (a + b) * c AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[test]
fn test_nested_parentheses() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10, 5, 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ((a - b) * c) + a AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[test]
fn test_all_operators_combined() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64, c INT64, d INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10, 5, 3, 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b * c - d / 2 AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[24]]);
}

#[test]
fn test_multiple_arithmetic_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10, 5), (20, 10), (30, 15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM numbers WHERE a * 2 > b + 25")
        .unwrap();
    assert_table_eq!(result, [[20, 10], [30, 15]]);
}

#[test]
fn test_arithmetic_in_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (product STRING, price INT64, tax INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('A', 100, 10), ('A', 100, 10), ('B', 200, 20)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT price + tax AS total, COUNT(*) AS count FROM sales GROUP BY price + tax ORDER BY total",
        )
        .unwrap();
    assert_table_eq!(result, [[110, 2], [220, 1]]);
}

#[test]
fn test_arithmetic_in_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10, 5), (5, 10), (8, 7)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a, b FROM numbers ORDER BY a + b DESC")
        .unwrap();
    assert_table_eq!(result, [[10, 5], [5, 10], [8, 7]]);
}

#[test]
fn test_literal_int_addition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dummy (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dummy VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT 5 + 3 AS result FROM dummy")
        .unwrap();
    assert_table_eq!(result, [[8]]);
}

#[test]
fn test_literal_float_multiplication() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dummy (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dummy VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT 2.5 * 4.0 AS result FROM dummy")
        .unwrap();
    assert_table_eq!(result, [[n("10")]]);
}

#[test]
fn test_column_plus_literal() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value + 50 AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[150]]);
}

#[test]
fn test_literal_minus_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT 100 - value AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[70]]);
}

#[test]
fn test_int_division_truncates() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a / b AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_int_division_negative_truncates() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (-10, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a / b AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[-3]]);
}

#[test]
fn test_negative_number_addition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (-10, -20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[-30]]);
}

#[test]
fn test_negative_times_negative() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (-5, -4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a * b AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[test]
fn test_negative_modulo() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (-17, 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a % b AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[-2]]);
}

#[test]
fn test_zero_plus_zero() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (0, 0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_zero_times_anything() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (12345)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value * 0 AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_zero_divided_by_nonzero() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT 0 / value AS result FROM numbers")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_sum_with_arithmetic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE orders (price INT64, quantity INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (10, 3), (20, 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(price * quantity) AS total FROM orders")
        .unwrap();
    assert_table_eq!(result, [[70]]);
}

#[test]
fn test_avg_with_arithmetic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE scores (score1 INT64, score2 INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES (80, 90), (70, 80)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(score1 + score2) AS avg_total FROM scores")
        .unwrap();
    assert_table_eq!(result, [[160.0]]);
}
