use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_sales_table(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE sales (product STRING, region STRING, year INT64, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('Widget', 'East', 2023, 100), ('Widget', 'East', 2024, 150), ('Widget', 'West', 2023, 120), ('Widget', 'West', 2024, 180), ('Gadget', 'East', 2023, 80), ('Gadget', 'East', 2024, 90), ('Gadget', 'West', 2023, 70), ('Gadget', 'West', 2024, 110)")
        .unwrap();
}

#[test]
fn test_rollup_single_column() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, SUM(amount) AS total FROM sales GROUP BY ROLLUP(product) ORDER BY product NULLS LAST")
        .unwrap();
    assert_table_eq!(result, [["Gadget", 350], ["Widget", 550], [null, 900],]);
}

#[test]
fn test_rollup_multiple_columns() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) AS total FROM sales GROUP BY ROLLUP(product, region) ORDER BY product NULLS LAST, region NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "East", 170],
            ["Gadget", "West", 180],
            ["Gadget", null, 350],
            ["Widget", "East", 250],
            ["Widget", "West", 300],
            ["Widget", null, 550],
            [null, null, 900],
        ]
    );
}

#[test]
fn test_rollup_three_columns() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, year, SUM(amount) AS total FROM sales GROUP BY ROLLUP(product, region, year) ORDER BY product NULLS LAST, region NULLS LAST, year NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "East", 2023, 80],
            ["Gadget", "East", 2024, 90],
            ["Gadget", "East", null, 170],
            ["Gadget", "West", 2023, 70],
            ["Gadget", "West", 2024, 110],
            ["Gadget", "West", null, 180],
            ["Gadget", null, null, 350],
            ["Widget", "East", 2023, 100],
            ["Widget", "East", 2024, 150],
            ["Widget", "East", null, 250],
            ["Widget", "West", 2023, 120],
            ["Widget", "West", 2024, 180],
            ["Widget", "West", null, 300],
            ["Widget", null, null, 550],
            [null, null, null, 900],
        ]
    );
}

#[test]
fn test_cube_single_column() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, SUM(amount) AS total FROM sales GROUP BY CUBE(product) ORDER BY product NULLS LAST")
        .unwrap();
    assert_table_eq!(result, [["Gadget", 350], ["Widget", 550], [null, 900],]);
}

#[test]
fn test_cube_two_columns() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) AS total FROM sales GROUP BY CUBE(product, region) ORDER BY product NULLS LAST, region NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "East", 170],
            ["Gadget", "West", 180],
            ["Gadget", null, 350],
            ["Widget", "East", 250],
            ["Widget", "West", 300],
            ["Widget", null, 550],
            [null, "East", 420],
            [null, "West", 480],
            [null, null, 900],
        ]
    );
}

#[test]
fn test_cube_three_columns() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM (SELECT product, region, year, SUM(amount) AS total FROM sales GROUP BY CUBE(product, region, year)) AS sub")
        .unwrap();
    assert_table_eq!(result, [[27]]);
}

#[test]
fn test_grouping_sets_basic() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((product), (region)) ORDER BY product NULLS LAST, region NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", null, 350],
            ["Widget", null, 550],
            [null, "East", 420],
            [null, "West", 480],
        ]
    );
}

#[test]
fn test_grouping_sets_with_empty() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((product), (region), ()) ORDER BY product NULLS LAST, region NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", null, 350],
            ["Widget", null, 550],
            [null, "East", 420],
            [null, "West", 480],
            [null, null, 900],
        ]
    );
}

#[test]
#[ignore = "BUG: Column 'product' not found in GROUPING SETS with multiple columns per set"]
fn test_grouping_sets_multiple_columns() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((product, region), (product)) ORDER BY product NULLS LAST, region NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "East", 170],
            ["Gadget", "West", 180],
            ["Gadget", null, 350],
            ["Widget", "East", 250],
            ["Widget", "West", 300],
            ["Widget", null, 550],
        ]
    );
}

#[test]
fn test_grouping_function() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) AS total, GROUPING(product) AS gp, GROUPING(region) AS gr FROM sales GROUP BY ROLLUP(product, region) ORDER BY product NULLS LAST, region NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "East", 170, 0, 0],
            ["Gadget", "West", 180, 0, 0],
            ["Gadget", null, 350, 0, 1],
            ["Widget", "East", 250, 0, 0],
            ["Widget", "West", 300, 0, 0],
            ["Widget", null, 550, 0, 1],
            [null, null, 900, 1, 1],
        ]
    );
}

#[test]
fn test_grouping_id() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) AS total, GROUPING_ID(product, region) AS gid FROM sales GROUP BY CUBE(product, region) ORDER BY gid, product, region")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "East", 170, 0],
            ["Gadget", "West", 180, 0],
            ["Widget", "East", 250, 0],
            ["Widget", "West", 300, 0],
            ["Gadget", null, 350, 1],
            ["Widget", null, 550, 1],
            [null, "East", 420, 2],
            [null, "West", 480, 2],
            [null, null, 900, 3],
        ]
    );
}

#[test]
fn test_rollup_with_having() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, SUM(amount) AS total FROM sales GROUP BY ROLLUP(product) HAVING SUM(amount) > 500 ORDER BY product NULLS LAST")
        .unwrap();
    assert_table_eq!(result, [["Widget", 550], [null, 900],]);
}

#[test]
fn test_cube_with_where() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) AS total FROM sales WHERE year = 2024 GROUP BY CUBE(product, region) ORDER BY product NULLS LAST, region NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "East", 90],
            ["Gadget", "West", 110],
            ["Gadget", null, 200],
            ["Widget", "East", 150],
            ["Widget", "West", 180],
            ["Widget", null, 330],
            [null, "East", 240],
            [null, "West", 290],
            [null, null, 530],
        ]
    );
}

#[test]
fn test_rollup_multiple_aggregates() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) AS total, COUNT(*) AS cnt, AVG(amount) AS avg_amt FROM sales GROUP BY ROLLUP(product, region) ORDER BY product NULLS LAST, region NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "East", 170, 2, 85.0],
            ["Gadget", "West", 180, 2, 90.0],
            ["Gadget", null, 350, 4, 87.5],
            ["Widget", "East", 250, 2, 125.0],
            ["Widget", "West", 300, 2, 150.0],
            ["Widget", null, 550, 4, 137.5],
            [null, null, 900, 8, 112.5],
        ]
    );
}

#[test]
fn test_partial_rollup() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, year, SUM(amount) AS total FROM sales GROUP BY product, ROLLUP(region, year) ORDER BY product, region NULLS LAST, year NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "East", 2023, 80],
            ["Gadget", "East", 2024, 90],
            ["Gadget", "East", null, 170],
            ["Gadget", "West", 2023, 70],
            ["Gadget", "West", 2024, 110],
            ["Gadget", "West", null, 180],
            ["Gadget", null, null, 350],
            ["Widget", "East", 2023, 100],
            ["Widget", "East", 2024, 150],
            ["Widget", "East", null, 250],
            ["Widget", "West", 2023, 120],
            ["Widget", "West", 2024, 180],
            ["Widget", "West", null, 300],
            ["Widget", null, null, 550],
        ]
    );
}

#[test]
fn test_partial_cube() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, year, SUM(amount) AS total FROM sales GROUP BY product, CUBE(region, year) ORDER BY product, region NULLS LAST, year NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "East", 2023, 80],
            ["Gadget", "East", 2024, 90],
            ["Gadget", "East", null, 170],
            ["Gadget", "West", 2023, 70],
            ["Gadget", "West", 2024, 110],
            ["Gadget", "West", null, 180],
            ["Gadget", null, 2023, 150],
            ["Gadget", null, 2024, 200],
            ["Gadget", null, null, 350],
            ["Widget", "East", 2023, 100],
            ["Widget", "East", 2024, 150],
            ["Widget", "East", null, 250],
            ["Widget", "West", 2023, 120],
            ["Widget", "West", 2024, 180],
            ["Widget", "West", null, 300],
            ["Widget", null, 2023, 220],
            ["Widget", null, 2024, 330],
            ["Widget", null, null, 550],
        ]
    );
}

#[test]
#[ignore = "BUG: Parser does not support ROLLUP inside GROUPING SETS"]
fn test_mixed_grouping() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, year, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS (ROLLUP(product, region), (year)) ORDER BY product NULLS LAST, region NULLS LAST, year NULLS LAST")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget", "East", null, 170],
            ["Gadget", "West", null, 180],
            ["Gadget", null, null, 350],
            ["Widget", "East", null, 250],
            ["Widget", "West", null, 300],
            ["Widget", null, null, 550],
            [null, null, 2023, 370],
            [null, null, 2024, 530],
            [null, null, null, 900],
        ]
    );
}

#[test]
fn test_grouping_with_order_by_aggregate() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT product, region, SUM(amount) AS total FROM sales GROUP BY CUBE(product, region) ORDER BY total DESC")
        .unwrap();
    assert_table_eq!(
        result,
        [
            [null, null, 900],
            ["Widget", null, 550],
            [null, "West", 480],
            [null, "East", 420],
            ["Gadget", null, 350],
            ["Widget", "West", 300],
            ["Widget", "East", 250],
            ["Gadget", "West", 180],
            ["Gadget", "East", 170],
        ]
    );
}

#[test]
fn test_grouping_case_expression() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql("SELECT CASE WHEN GROUPING(product) = 1 THEN 'All Products' ELSE product END AS product_label, SUM(amount) AS total FROM sales GROUP BY ROLLUP(product) ORDER BY product_label")
        .unwrap();
    assert_table_eq!(
        result,
        [["All Products", 900], ["Gadget", 350], ["Widget", 550],]
    );
}
