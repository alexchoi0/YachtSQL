#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::{DialectType, QueryExecutor};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

fn setup_eq_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS eq_test")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE eq_test (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO eq_test VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO eq_test VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO eq_test VALUES (3, 10)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_001() {
    let mut executor = create_executor();
    setup_eq_test(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM eq_test WHERE value = 10")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

fn setup_neq_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS neq_test")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE neq_test (id INT64, status STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO neq_test VALUES (1, 'active')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO neq_test VALUES (2, 'inactive')")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_002() {
    let mut executor = create_executor();
    setup_neq_test(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM neq_test WHERE status <> 'active'")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(2));
}

fn setup_neq_test2(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS neq_test2")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE neq_test2 (id INT64, status STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO neq_test2 VALUES (1, 'active')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO neq_test2 VALUES (2, 'inactive')")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_003() {
    let mut executor = create_executor();
    setup_neq_test2(&mut executor);

    let result = executor
        .execute_sql("SELECT id FROM neq_test2 WHERE status != 'active'")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(2));
}

fn setup_lt_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS lt_test")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE lt_test (id INT64, age INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lt_test VALUES (1, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lt_test VALUES (2, 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lt_test VALUES (3, 25)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_004() {
    let mut executor = create_executor();
    setup_lt_test(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM lt_test WHERE age < 25")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let id_col = result.column(0).unwrap();
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(1));
}

fn setup_gt_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS gt_test")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE gt_test (id INT64, price FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO gt_test VALUES (1, 9.99)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO gt_test VALUES (2, 19.99)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO gt_test VALUES (3, 14.99)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_005() {
    let mut executor = create_executor();
    setup_gt_test(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM gt_test WHERE price > 15.0")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let id_col = result.column(0).unwrap();
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(2));
}

fn setup_lte_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS lte_test")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE lte_test (id INT64, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lte_test VALUES (1, 85)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lte_test VALUES (2, 90)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lte_test VALUES (3, 80)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_006() {
    let mut executor = create_executor();
    setup_lte_test(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM lte_test WHERE score <= 85")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

fn setup_gte_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS gte_test")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE gte_test (id INT64, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO gte_test VALUES (1, 85)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO gte_test VALUES (2, 90)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO gte_test VALUES (3, 80)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_007() {
    let mut executor = create_executor();
    setup_gte_test(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM gte_test WHERE score >= 85")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

fn setup_str_cmp(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS str_cmp")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE str_cmp (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO str_cmp VALUES (1, 'Alice')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO str_cmp VALUES (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO str_cmp VALUES (3, 'Charlie')")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_008() {
    let mut executor = create_executor();
    setup_str_cmp(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM str_cmp WHERE name > 'Bob'")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let name_col = result.column(1).unwrap();
    assert_eq!(name_col.get(0).unwrap().as_str(), Some("Charlie"));
}

fn setup_float_cmp(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS float_cmp")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE float_cmp (id INT64, value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO float_cmp VALUES (1, 3.14)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO float_cmp VALUES (2, 2.71)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO float_cmp VALUES (3, 1.41)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_009() {
    let mut executor = create_executor();
    setup_float_cmp(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM float_cmp WHERE value < 3.0")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

fn setup_null_cmp(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS null_cmp")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_cmp (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_cmp VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_cmp VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_cmp VALUES (3, 10)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_010() {
    let mut executor = create_executor();
    setup_null_cmp(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM null_cmp WHERE value = 10")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

fn setup_null_cmp2(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS null_cmp2")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_cmp2 (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_cmp2 VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_cmp2 VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_cmp2 VALUES (3, 20)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_011() {
    let mut executor = create_executor();
    setup_null_cmp2(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM null_cmp2 WHERE value <> 10")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

fn setup_null_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS null_test")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_test (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_test VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_test VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_test VALUES (3, 20)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_012() {
    let mut executor = create_executor();
    setup_null_test(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM null_test WHERE value IS NULL")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let id_col = result.column(0).unwrap();
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(2));
}

fn setup_null_test2(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS null_test2")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_test2 (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_test2 VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_test2 VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_test2 VALUES (3, 20)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_013() {
    let mut executor = create_executor();
    setup_null_test2(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM null_test2 WHERE value IS NOT NULL")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

fn setup_bool_cmp(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS bool_cmp")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE bool_cmp (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bool_cmp VALUES (10, 20)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_014() {
    let mut executor = create_executor();
    setup_bool_cmp(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT a < b AS less_than, a = b AS equal, a > b AS greater_than FROM bool_cmp",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let less_than = result.column(0).unwrap();
    let equal = result.column(1).unwrap();
    let greater_than = result.column(2).unwrap();

    assert_eq!(less_than.get(0).unwrap().as_bool(), Some(true));
    assert_eq!(equal.get(0).unwrap().as_bool(), Some(false));
    assert_eq!(greater_than.get(0).unwrap().as_bool(), Some(false));
}

fn setup_range_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS range_test")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE range_test (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO range_test VALUES (1, 5)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO range_test VALUES (2, 15)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO range_test VALUES (3, 25)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_015() {
    let mut executor = create_executor();
    setup_range_test(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM range_test WHERE value >= 10 AND value <= 20")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let id_col = result.column(0).unwrap();
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(2));
}

fn setup_multi_cmp(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS multi_cmp")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE multi_cmp (id INT64, status STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_cmp VALUES (1, 'active')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_cmp VALUES (2, 'pending')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_cmp VALUES (3, 'inactive')")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_016() {
    let mut executor = create_executor();
    setup_multi_cmp(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM multi_cmp WHERE status = 'active' OR status = 'pending'")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

fn setup_expr_cmp(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS expr_cmp")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE expr_cmp (id INT64, price INT64, quantity INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO expr_cmp VALUES (1, 10, 5)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO expr_cmp VALUES (2, 20, 3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO expr_cmp VALUES (3, 15, 4)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_017() {
    let mut executor = create_executor();
    setup_expr_cmp(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM expr_cmp WHERE price * quantity > 55")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

fn setup_case_test(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS case_test")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE case_test (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO case_test VALUES (1, 'Alice')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO case_test VALUES (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO case_test VALUES (3, 'Charlie')")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_018() {
    let mut executor = create_executor();
    setup_case_test(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM case_test WHERE name = 'Alice'")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let id_col = result.column(0).unwrap();
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(1));
}

fn setup_zero_cmp(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS zero_cmp")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE zero_cmp (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO zero_cmp VALUES (1, -5)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO zero_cmp VALUES (2, 0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO zero_cmp VALUES (3, 5)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_019() {
    let mut executor = create_executor();
    setup_zero_cmp(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM zero_cmp WHERE value > 0")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let id_col = result.column(0).unwrap();
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(3));
}

#[test]
fn test_operators_comparison_test_select_020() {
    let mut executor = create_executor();
    setup_zero_cmp(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM zero_cmp WHERE value = 0")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let id_col = result.column(0).unwrap();
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(2));
}

#[test]
fn test_operators_comparison_test_select_021() {
    let mut executor = create_executor();
    setup_zero_cmp(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM zero_cmp WHERE value < 0")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let id_col = result.column(0).unwrap();
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(1));
}

fn setup_neg_cmp(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS neg_cmp")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE neg_cmp (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO neg_cmp VALUES (1, -10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO neg_cmp VALUES (2, -5)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO neg_cmp VALUES (3, -15)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_022() {
    let mut executor = create_executor();
    setup_neg_cmp(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM neg_cmp WHERE value > -10")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let id_col = result.column(0).unwrap();
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(2));
}

fn setup_float_eq(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS float_eq")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE float_eq (id INT64, value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO float_eq VALUES (1, 0.1 + 0.2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO float_eq VALUES (2, 0.3)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_023() {
    let mut executor = create_executor();
    setup_float_eq(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM float_eq WHERE value = 0.3")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let id_col = result.column(0).unwrap();
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(1));
    assert_eq!(id_col.get(1).unwrap().as_i64(), Some(2));
}

fn setup_empty_str(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS empty_str")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE empty_str (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO empty_str VALUES (1, '')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO empty_str VALUES (2, 'text')")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_024() {
    let mut executor = create_executor();
    setup_empty_str(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM empty_str WHERE value = ''")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let id_col = result.column(0).unwrap();
    assert_eq!(id_col.get(0).unwrap().as_i64(), Some(1));
}

fn setup_order_cmp(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS order_cmp")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE order_cmp (id INT64, priority INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO order_cmp VALUES (1, 3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO order_cmp VALUES (2, 1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO order_cmp VALUES (3, 2)")
        .unwrap();
}

#[test]
fn test_operators_comparison_test_select_025() {
    let mut executor = create_executor();
    setup_order_cmp(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM order_cmp ORDER BY priority ASC")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    let id_col = result.column(0).unwrap();
    let priority_col = result.column(1).unwrap();

    assert_eq!(priority_col.get(0).unwrap().as_i64(), Some(1));
    assert_eq!(priority_col.get(1).unwrap().as_i64(), Some(2));
    assert_eq!(priority_col.get(2).unwrap().as_i64(), Some(3));

    let result2 = executor
        .execute_sql("SELECT * FROM order_cmp WHERE priority < 3 ORDER BY priority DESC")
        .unwrap();

    assert_eq!(result2.num_rows(), 2);
    let priority_col2 = result2.column(1).unwrap();
    assert_eq!(priority_col2.get(0).unwrap().as_i64(), Some(2));
    assert_eq!(priority_col2.get(1).unwrap().as_i64(), Some(1));
}
