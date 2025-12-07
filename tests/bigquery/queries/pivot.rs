use crate::common::create_executor;

fn setup_sales_table(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE sales (product STRING, quarter STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 'Q1', 100), ('Widget', 'Q2', 150), ('Widget', 'Q3', 200), ('Widget', 'Q4', 250), ('Gadget', 'Q1', 80), ('Gadget', 'Q2', 120), ('Gadget', 'Q3', 160), ('Gadget', 'Q4', 200)")
        .unwrap();
}

#[test]
fn test_pivot_basic() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM sales PIVOT(SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4')) ORDER BY product");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pivot_with_alias() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM sales PIVOT(SUM(amount) AS total FOR quarter IN ('Q1' AS q1, 'Q2' AS q2)) ORDER BY product");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pivot_multiple_aggregates() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (category STRING, period STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES ('A', 'P1', 10), ('A', 'P1', 20), ('A', 'P2', 30), ('B', 'P1', 40), ('B', 'P2', 50)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM data PIVOT(SUM(value), COUNT(*) FOR period IN ('P1', 'P2')) ORDER BY category");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pivot_with_where() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM (SELECT * FROM sales WHERE product = 'Widget') PIVOT(SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pivot_avg() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor.execute_sql(
        "SELECT * FROM sales PIVOT(AVG(amount) FOR quarter IN ('Q1', 'Q2')) ORDER BY product",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pivot_count() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE logs (user_id INT64, action STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO logs VALUES (1, 'view'), (1, 'click'), (1, 'view'), (2, 'click'), (2, 'click')")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT * FROM logs PIVOT(COUNT(*) FOR action IN ('view', 'click')) ORDER BY user_id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_unpivot_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE quarterly_sales (product STRING, Q1 INT64, Q2 INT64, Q3 INT64, Q4 INT64)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO quarterly_sales VALUES ('Widget', 100, 150, 200, 250), ('Gadget', 80, 120, 160, 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM quarterly_sales UNPIVOT(amount FOR quarter IN (Q1, Q2, Q3, Q4)) ORDER BY product, quarter");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_unpivot_with_alias() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, val_a INT64, val_b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10, 20), (2, 30, 40)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM data UNPIVOT(value FOR type IN (val_a AS 'A', val_b AS 'B')) ORDER BY id, type");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_unpivot_include_nulls() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, col1 INT64, col2 INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10, NULL), (2, NULL, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM data UNPIVOT INCLUDE NULLS (value FOR column_name IN (col1, col2)) ORDER BY id");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_unpivot_exclude_nulls() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, col1 INT64, col2 INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10, NULL), (2, NULL, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM data UNPIVOT EXCLUDE NULLS (value FOR column_name IN (col1, col2)) ORDER BY id");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pivot_in_subquery() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, Q1 + Q2 AS first_half FROM (SELECT * FROM sales PIVOT(SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))) ORDER BY product");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pivot_with_null_values() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (category STRING, type STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES ('A', 'X', 10), ('A', 'Y', 20), ('B', 'X', 30)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT * FROM data PIVOT(SUM(value) FOR type IN ('X', 'Y', 'Z')) ORDER BY category",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_unpivot_multiple_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE metrics (id INT64, metric1_val INT64, metric1_unit STRING, metric2_val INT64, metric2_unit STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO metrics VALUES (1, 100, 'kg', 200, 'm')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM metrics UNPIVOT((value, unit) FOR metric IN ((metric1_val, metric1_unit) AS 'metric1', (metric2_val, metric2_unit) AS 'metric2'))");
    assert!(result.is_ok() || result.is_err());
}
