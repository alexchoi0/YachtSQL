use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_sales_table(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE sales (id INT64, product STRING, region STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (1, 'Widget', 'East', 100), (2, 'Widget', 'East', 150), (3, 'Widget', 'West', 200), (4, 'Gadget', 'East', 80), (5, 'Gadget', 'West', 120), (6, 'Gadget', 'West', 90)")
        .unwrap();
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_row_number() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, product, region, amount FROM sales QUALIFY ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) = 1 ORDER BY product")
        .unwrap();
    assert_table_eq!(
        result,
        [[5, "Gadget", "West", 120], [3, "Widget", "West", 200],]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_rank() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY RANK() OVER (ORDER BY amount DESC) <= 3 ORDER BY amount DESC")
        .unwrap();
    assert_table_eq!(
        result,
        [[3, "Widget", 200], [2, "Widget", 150], [5, "Gadget", 120],]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_dense_rank() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE scores (id INT64, name STRING, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES (1, 'Alice', 100), (2, 'Bob', 100), (3, 'Charlie', 90), (4, 'Diana', 90), (5, 'Eve', 80)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, score FROM scores QUALIFY DENSE_RANK() OVER (ORDER BY score DESC) <= 2 ORDER BY name")
        .unwrap();
    assert_table_eq!(
        result,
        [["Alice", 100], ["Bob", 100], ["Charlie", 90], ["Diana", 90],]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_with_where() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, product, amount FROM sales WHERE region = 'East' QUALIFY ROW_NUMBER() OVER (ORDER BY amount DESC) = 1")
        .unwrap();
    assert_table_eq!(result, [[2, "Widget", 150]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_with_group_by() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT region, SUM(amount) AS total FROM sales GROUP BY region QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) = 1")
        .unwrap();
    assert_table_eq!(result, [["West", 410]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_ntile() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT id, amount FROM sales QUALIFY NTILE(3) OVER (ORDER BY amount) = 1 ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[4, 80], [6, 90]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_percent_rank() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, amount FROM sales QUALIFY PERCENT_RANK() OVER (ORDER BY amount) >= 0.5 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2, 150], [3, 200], [5, 120]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_cume_dist() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, amount FROM sales QUALIFY CUME_DIST() OVER (ORDER BY amount) <= 0.5 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [4, 80], [6, 90]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_lag() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 15), (4, 25)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value FROM data QUALIFY value > LAG(value) OVER (ORDER BY id) ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[2, 20], [4, 25]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_lead() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 15), (4, 25)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value FROM data QUALIFY value < LEAD(value) OVER (ORDER BY id) ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[1, 10], [3, 15]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_first_value() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount = FIRST_VALUE(amount) OVER (PARTITION BY product ORDER BY amount DESC) ORDER BY product")
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_last_value() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, product, amount FROM sales QUALIFY amount = LAST_VALUE(amount) OVER (PARTITION BY product ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY product")
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_multiple_conditions() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT id, product, region, amount FROM sales QUALIFY ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) <= 2 AND ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) = 1 ORDER BY id")
        .unwrap();
    assert_table_eq!(
        result,
        [[2, "Widget", "East", 150], [3, "Widget", "West", 200],]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_with_cte() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) AS rn FROM sales) SELECT id, product, amount FROM ranked QUALIFY rn = 1 ORDER BY product")
        .unwrap();
    assert_table_eq!(result, [[5, "Gadget", 120], [3, "Widget", 200],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_qualify_with_aggregation() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, SUM(amount) AS total, AVG(amount) AS avg_amount FROM sales GROUP BY product QUALIFY SUM(amount) = MAX(SUM(amount)) OVER ()")
        .unwrap();
    assert_table_eq!(result, [["Widget", 450, 150.0]]);
}
