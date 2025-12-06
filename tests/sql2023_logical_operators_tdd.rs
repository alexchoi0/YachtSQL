#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::{DialectType, QueryExecutor};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

#[test]
fn test_and_true_true() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUE AND TRUE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_and_true_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUE AND FALSE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(!col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_and_true_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUE AND NULL as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_and_false_true() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FALSE AND TRUE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(!col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_and_false_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FALSE AND FALSE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(!col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_and_false_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FALSE AND NULL as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(!col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_and_null_true() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NULL AND TRUE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_and_null_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NULL AND FALSE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(!col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_and_null_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NULL AND NULL as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_or_true_true() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUE OR TRUE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_or_true_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUE OR FALSE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_or_true_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUE OR NULL as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_or_false_true() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FALSE OR TRUE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_or_false_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FALSE OR FALSE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(!col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_or_false_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FALSE OR NULL as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_or_null_true() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NULL OR TRUE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_or_null_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NULL OR FALSE as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_or_null_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NULL OR NULL as result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_not_true() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NOT TRUE as result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(!col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_not_false() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NOT FALSE as result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().as_bool().unwrap());
}

#[test]
fn test_not_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT NOT NULL as result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

fn setup_bool_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE bool_test (id INT64, active BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bool_test VALUES (1, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bool_test VALUES (2, FALSE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bool_test VALUES (3, TRUE)")
        .unwrap();
}

#[test]
fn test_not_column_where() {
    let mut executor = create_executor();
    setup_bool_test(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM bool_test WHERE NOT active ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 2);
}

fn setup_not_cmp(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE not_cmp (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_cmp VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_cmp VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_cmp VALUES (3, 30)")
        .unwrap();
}

#[test]
fn test_not_comparison_where() {
    let mut executor = create_executor();
    setup_not_cmp(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM not_cmp WHERE NOT (value > 20) ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
    assert_eq!(col.get(1).unwrap().as_i64().unwrap(), 2);
}

fn setup_double_not(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE double_not (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO double_not VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO double_not VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO double_not VALUES (3, 30)")
        .unwrap();
}

#[test]
fn test_double_not() {
    let mut executor = create_executor();
    setup_double_not(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM double_not WHERE NOT NOT (value > 15) ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 2);
    assert_eq!(col.get(1).unwrap().as_i64().unwrap(), 3);
}

fn setup_not_null_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE not_null (id INT64, value BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_null VALUES (1, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_null VALUES (2, FALSE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_null VALUES (3, NULL)")
        .unwrap();
}

#[test]
fn test_not_with_null_values() {
    let mut executor = create_executor();
    setup_not_null_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM not_null WHERE NOT value ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 2);
}

fn setup_not_is_null(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE not_is_null (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_is_null VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_is_null VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_is_null VALUES (3, 30)")
        .unwrap();
}

#[test]
fn test_not_is_null() {
    let mut executor = create_executor();
    setup_not_is_null(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM not_is_null WHERE NOT (value IS NULL) ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
    assert_eq!(col.get(1).unwrap().as_i64().unwrap(), 3);
}

fn setup_and_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE and_test (id INT64, a BOOL, b BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO and_test VALUES (1, TRUE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO and_test VALUES (2, TRUE, FALSE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO and_test VALUES (3, FALSE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO and_test VALUES (4, FALSE, FALSE)")
        .unwrap();
}

#[test]
fn test_and_columns_where() {
    let mut executor = create_executor();
    setup_and_test(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM and_test WHERE a AND b ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
}

fn setup_or_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE or_test (id INT64, a BOOL, b BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO or_test VALUES (1, TRUE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO or_test VALUES (2, TRUE, FALSE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO or_test VALUES (3, FALSE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO or_test VALUES (4, FALSE, FALSE)")
        .unwrap();
}

#[test]
fn test_or_columns_where() {
    let mut executor = create_executor();
    setup_or_test(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM or_test WHERE a OR b ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
    assert_eq!(col.get(1).unwrap().as_i64().unwrap(), 2);
    assert_eq!(col.get(2).unwrap().as_i64().unwrap(), 3);
}

fn setup_and_null(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE and_null (id INT64, a BOOL, b BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO and_null VALUES (1, TRUE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO and_null VALUES (2, TRUE, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO and_null VALUES (3, FALSE, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO and_null VALUES (4, NULL, NULL)")
        .unwrap();
}

#[test]
fn test_and_with_null_columns() {
    let mut executor = create_executor();
    setup_and_null(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM and_null WHERE a AND b ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
}

fn setup_or_null(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE or_null (id INT64, a BOOL, b BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO or_null VALUES (1, TRUE, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO or_null VALUES (2, FALSE, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO or_null VALUES (3, NULL, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO or_null VALUES (4, FALSE, FALSE)")
        .unwrap();
}

#[test]
fn test_or_with_null_columns() {
    let mut executor = create_executor();
    setup_or_null(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM or_null WHERE a OR b ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
}

fn setup_demorgan_and(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE demorgan_and (id INT64, a BOOL, b BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO demorgan_and VALUES (1, TRUE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO demorgan_and VALUES (2, TRUE, FALSE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO demorgan_and VALUES (3, FALSE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO demorgan_and VALUES (4, FALSE, FALSE)")
        .unwrap();
}

#[test]
fn test_demorgan_not_and() {
    let mut executor = create_executor();
    setup_demorgan_and(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM demorgan_and WHERE NOT (a AND b) ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 2);
    assert_eq!(col.get(1).unwrap().as_i64().unwrap(), 3);
    assert_eq!(col.get(2).unwrap().as_i64().unwrap(), 4);
}

fn setup_demorgan_or(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE demorgan_or (id INT64, a BOOL, b BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO demorgan_or VALUES (1, TRUE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO demorgan_or VALUES (2, TRUE, FALSE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO demorgan_or VALUES (3, FALSE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO demorgan_or VALUES (4, FALSE, FALSE)")
        .unwrap();
}

#[test]
fn test_demorgan_not_or() {
    let mut executor = create_executor();
    setup_demorgan_or(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM demorgan_or WHERE NOT (a OR b) ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 4);
}

fn setup_complex_bool(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE complex_bool (id INT64, a BOOL, b BOOL, c BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO complex_bool VALUES (1, TRUE, FALSE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO complex_bool VALUES (2, FALSE, TRUE, FALSE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO complex_bool VALUES (3, TRUE, TRUE, TRUE)")
        .unwrap();
}

#[test]
fn test_complex_and_or() {
    let mut executor = create_executor();
    setup_complex_bool(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM complex_bool WHERE (a OR b) AND c ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
    assert_eq!(col.get(1).unwrap().as_i64().unwrap(), 3);
}

fn setup_short_circuit(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE short_circuit (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO short_circuit VALUES (1, 0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO short_circuit VALUES (2, 10)")
        .unwrap();
}

#[test]
fn test_short_circuit_and() {
    let mut executor = create_executor();
    setup_short_circuit(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM short_circuit WHERE value <> 0 AND (100 / value) > 0")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(
        result.column(0).unwrap().get(0).unwrap().as_i64().unwrap(),
        2
    );
}

fn setup_multi_and(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE multi_and (id INT64, age INT64, salary INT64, active BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_and VALUES (1, 30, 60000, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_and VALUES (2, 25, 50000, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_and VALUES (3, 30, 55000, FALSE)")
        .unwrap();
}

#[test]
fn test_multi_and() {
    let mut executor = create_executor();
    setup_multi_and(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT id FROM multi_and WHERE age = 30 AND salary > 55000 AND active ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
}

fn setup_multi_or(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS multi_or")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE multi_or (id INT64, status STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_or VALUES (1, 'active')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_or VALUES (2, 'pending')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_or VALUES (3, 'inactive')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_or VALUES (4, 'archived')")
        .unwrap();
}

#[test]
fn test_multi_or() {
    let mut executor = create_executor();
    setup_multi_or(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM multi_or WHERE status = 'active' OR status = 'pending' OR status = 'archived' ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
    assert_eq!(col.get(1).unwrap().as_i64().unwrap(), 2);
    assert_eq!(col.get(2).unwrap().as_i64().unwrap(), 4);
}

fn setup_precedence(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS precedence")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE precedence (id INT64, a BOOL, b BOOL, c BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO precedence VALUES (1, TRUE, FALSE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO precedence VALUES (2, FALSE, TRUE, FALSE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO precedence VALUES (3, TRUE, TRUE, FALSE)")
        .unwrap();
}

#[test]
fn test_precedence_and_over_or() {
    let mut executor = create_executor();
    setup_precedence(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM precedence WHERE a OR b AND c ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
    assert_eq!(col.get(1).unwrap().as_i64().unwrap(), 3);
}

fn setup_paren_prec(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS paren_prec")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE paren_prec (id INT64, a BOOL, b BOOL, c BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO paren_prec VALUES (1, TRUE, FALSE, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO paren_prec VALUES (2, FALSE, TRUE, FALSE)")
        .unwrap();
}

#[test]
fn test_precedence_with_parens() {
    let mut executor = create_executor();
    setup_paren_prec(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM paren_prec WHERE (a OR b) AND c ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
}

#[test]
fn test_logical_in_select() {
    let mut executor = create_executor();

    let and_result = executor
        .execute_sql("SELECT (TRUE AND FALSE) AS res")
        .unwrap();
    assert_eq!(and_result.num_rows(), 1);
    let and_col = and_result.column(0).unwrap();
    assert!(!and_col.get(0).unwrap().as_bool().unwrap());

    let or_result = executor
        .execute_sql("SELECT (TRUE OR FALSE) AS res")
        .unwrap();
    assert_eq!(or_result.num_rows(), 1);
    let or_col = or_result.column(0).unwrap();
    assert!(or_col.get(0).unwrap().as_bool().unwrap());

    let not_result = executor.execute_sql("SELECT (NOT TRUE) AS res").unwrap();
    assert_eq!(not_result.num_rows(), 1);
    let not_col = not_result.column(0).unwrap();
    assert!(!not_col.get(0).unwrap().as_bool().unwrap());
}

fn setup_null_prop(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS null_prop")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_prop (id INT64, a BOOL, b BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_prop VALUES (1, TRUE, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_prop VALUES (2, FALSE, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_prop VALUES (3, NULL, TRUE)")
        .unwrap();
}

#[test]
fn test_xor_expression_with_null() {
    let mut executor = create_executor();
    setup_null_prop(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM null_prop WHERE (a OR b) AND NOT (a AND b) ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}
