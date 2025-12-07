use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_and_true_true() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TRUE AND TRUE").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_and_true_false() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TRUE AND FALSE").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_and_false_false() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT FALSE AND FALSE").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_or_true_true() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TRUE OR TRUE").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_or_true_false() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TRUE OR FALSE").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_or_false_false() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT FALSE OR FALSE").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_not_true() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NOT TRUE").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_not_false() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NOT FALSE").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_and_with_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TRUE AND NULL").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_and_false_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT FALSE AND NULL").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_or_with_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT FALSE OR NULL").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_or_true_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TRUE OR NULL").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_not_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NOT NULL").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_complex_logical_expression() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT (TRUE AND FALSE) OR TRUE")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_logical_precedence() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUE OR FALSE AND FALSE")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_logical_with_comparison() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 5 > 3 AND 2 < 4").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_logical_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE flags (a BOOL, b BOOL)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO flags VALUES (TRUE, TRUE), (TRUE, FALSE), (FALSE, TRUE), (FALSE, FALSE)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT a, b FROM flags WHERE a AND b")
        .unwrap();
    assert_table_eq!(result, [[true, true]]);
}

#[test]
fn test_logical_or_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE flags (a BOOL, b BOOL)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO flags VALUES (TRUE, TRUE), (TRUE, FALSE), (FALSE, TRUE), (FALSE, FALSE)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT a, b FROM flags WHERE a OR b ORDER BY a DESC, b DESC")
        .unwrap();
    assert_table_eq!(result, [[true, true], [true, false], [false, true],]);
}

#[test]
fn test_not_in_where() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE flags (a BOOL)").unwrap();
    executor
        .execute_sql("INSERT INTO flags VALUES (TRUE), (FALSE)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a FROM flags WHERE NOT a")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_is_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nullable (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM nullable WHERE val IS NULL")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_is_not_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nullable (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM nullable WHERE val IS NOT NULL ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}
