use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_sales_table(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE sales (region TEXT, product TEXT, year INTEGER, amount INTEGER)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO sales VALUES
             ('East', 'Widget', 2023, 1000),
             ('East', 'Widget', 2024, 1200),
             ('East', 'Gadget', 2023, 800),
             ('East', 'Gadget', 2024, 900),
             ('West', 'Widget', 2023, 1100),
             ('West', 'Widget', 2024, 1300),
             ('West', 'Gadget', 2023, 700),
             ('West', 'Gadget', 2024, 850)",
        )
        .unwrap();
}

#[test]
#[ignore = "Implement me!"]
fn test_grouping_sets_basic() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, SUM(amount) AS total
             FROM sales
             GROUP BY GROUPING SETS ((region), (product), ())
             ORDER BY region NULLS LAST, product NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", null, 3900],
            ["West", null, 3950],
            [null, "Gadget", 3250],
            [null, "Widget", 4600],
            [null, null, 7850]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_grouping_sets_multiple_columns() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, year, SUM(amount) AS total
             FROM sales
             GROUP BY GROUPING SETS ((region, product), (year))
             ORDER BY region NULLS LAST, product NULLS LAST, year NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", null, 1700],
            ["East", "Widget", null, 2200],
            ["West", "Gadget", null, 1550],
            ["West", "Widget", null, 2400],
            [null, null, 2023, 3600],
            [null, null, 2024, 4250]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_rollup_basic() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, SUM(amount) AS total
             FROM sales
             GROUP BY ROLLUP (region, product)
             ORDER BY region NULLS LAST, product NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 1700],
            ["East", "Widget", 2200],
            ["East", null, 3900],
            ["West", "Gadget", 1550],
            ["West", "Widget", 2400],
            ["West", null, 3950],
            [null, null, 7850]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_rollup_three_columns() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, year, SUM(amount) AS total
             FROM sales
             GROUP BY ROLLUP (region, product, year)
             ORDER BY region NULLS LAST, product NULLS LAST, year NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 2023, 800],
            ["East", "Gadget", 2024, 900],
            ["East", "Gadget", null, 1700],
            ["East", "Widget", 2023, 1000],
            ["East", "Widget", 2024, 1200],
            ["East", "Widget", null, 2200],
            ["East", null, null, 3900],
            ["West", "Gadget", 2023, 700],
            ["West", "Gadget", 2024, 850],
            ["West", "Gadget", null, 1550],
            ["West", "Widget", 2023, 1100],
            ["West", "Widget", 2024, 1300],
            ["West", "Widget", null, 2400],
            ["West", null, null, 3950],
            [null, null, null, 7850]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_cube_basic() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, SUM(amount) AS total
             FROM sales
             GROUP BY CUBE (region, product)
             ORDER BY region NULLS LAST, product NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 1700],
            ["East", "Widget", 2200],
            ["East", null, 3900],
            ["West", "Gadget", 1550],
            ["West", "Widget", 2400],
            ["West", null, 3950],
            [null, "Gadget", 3250],
            [null, "Widget", 4600],
            [null, null, 7850]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_cube_three_columns() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, year, SUM(amount) AS total
             FROM sales
             GROUP BY CUBE (region, product, year)
             ORDER BY region NULLS LAST, product NULLS LAST, year NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 2023, 800],
            ["East", "Gadget", 2024, 900],
            ["East", "Gadget", null, 1700],
            ["East", "Widget", 2023, 1000],
            ["East", "Widget", 2024, 1200],
            ["East", "Widget", null, 2200],
            ["East", null, 2023, 1800],
            ["East", null, 2024, 2100],
            ["East", null, null, 3900],
            ["West", "Gadget", 2023, 700],
            ["West", "Gadget", 2024, 850],
            ["West", "Gadget", null, 1550],
            ["West", "Widget", 2023, 1100],
            ["West", "Widget", 2024, 1300],
            ["West", "Widget", null, 2400],
            ["West", null, 2023, 1800],
            ["West", null, 2024, 2150],
            ["West", null, null, 3950],
            [null, "Gadget", 2023, 1500],
            [null, "Gadget", 2024, 1750],
            [null, "Gadget", null, 3250],
            [null, "Widget", 2023, 2100],
            [null, "Widget", 2024, 2500],
            [null, "Widget", null, 4600],
            [null, null, 2023, 3600],
            [null, null, 2024, 4250],
            [null, null, null, 7850]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_grouping_function() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, SUM(amount) AS total,
                    GROUPING(region) AS grp_region,
                    GROUPING(product) AS grp_product
             FROM sales
             GROUP BY ROLLUP (region, product)
             ORDER BY region NULLS LAST, product NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 1700, 0, 0],
            ["East", "Widget", 2200, 0, 0],
            ["East", null, 3900, 0, 1],
            ["West", "Gadget", 1550, 0, 0],
            ["West", "Widget", 2400, 0, 0],
            ["West", null, 3950, 0, 1],
            [null, null, 7850, 1, 1]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_grouping_id() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, SUM(amount) AS total,
                    GROUPING_ID(region, product) AS grp_id
             FROM sales
             GROUP BY CUBE (region, product)
             ORDER BY grp_id, region NULLS LAST, product NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 1700, 0],
            ["East", "Widget", 2200, 0],
            ["West", "Gadget", 1550, 0],
            ["West", "Widget", 2400, 0],
            ["East", null, 3900, 1],
            ["West", null, 3950, 1],
            [null, "Gadget", 3250, 2],
            [null, "Widget", 4600, 2],
            [null, null, 7850, 3]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_grouping_sets_with_having() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, SUM(amount) AS total
             FROM sales
             GROUP BY GROUPING SETS ((region), (product))
             HAVING SUM(amount) > 2000
             ORDER BY region NULLS LAST, product NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", null, 3900],
            ["West", null, 3950],
            [null, "Gadget", 3250],
            [null, "Widget", 4600]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_grouping_sets_with_where() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, SUM(amount) AS total
             FROM sales
             WHERE year = 2024
             GROUP BY GROUPING SETS ((region), (product))
             ORDER BY region NULLS LAST, product NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", null, 2100],
            ["West", null, 2150],
            [null, "Gadget", 1750],
            [null, "Widget", 2500]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_rollup_partial() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, year, SUM(amount) AS total
             FROM sales
             GROUP BY region, ROLLUP (product, year)
             ORDER BY region, product NULLS LAST, year NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 2023, 800],
            ["East", "Gadget", 2024, 900],
            ["East", "Gadget", null, 1700],
            ["East", "Widget", 2023, 1000],
            ["East", "Widget", 2024, 1200],
            ["East", "Widget", null, 2200],
            ["East", null, null, 3900],
            ["West", "Gadget", 2023, 700],
            ["West", "Gadget", 2024, 850],
            ["West", "Gadget", null, 1550],
            ["West", "Widget", 2023, 1100],
            ["West", "Widget", 2024, 1300],
            ["West", "Widget", null, 2400],
            ["West", null, null, 3950]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_cube_partial() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, year, SUM(amount) AS total
             FROM sales
             GROUP BY region, CUBE (product, year)
             ORDER BY region, product NULLS LAST, year NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 2023, 800],
            ["East", "Gadget", 2024, 900],
            ["East", "Gadget", null, 1700],
            ["East", "Widget", 2023, 1000],
            ["East", "Widget", 2024, 1200],
            ["East", "Widget", null, 2200],
            ["East", null, 2023, 1800],
            ["East", null, 2024, 2100],
            ["East", null, null, 3900],
            ["West", "Gadget", 2023, 700],
            ["West", "Gadget", 2024, 850],
            ["West", "Gadget", null, 1550],
            ["West", "Widget", 2023, 1100],
            ["West", "Widget", 2024, 1300],
            ["West", "Widget", null, 2400],
            ["West", null, 2023, 1800],
            ["West", null, 2024, 2150],
            ["West", null, null, 3950]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_mixed_grouping() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, year, SUM(amount) AS total
             FROM sales
             GROUP BY GROUPING SETS (
                 (region, product),
                 ROLLUP (year)
             )
             ORDER BY region NULLS LAST, product NULLS LAST, year NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", null, 1700],
            ["East", "Widget", null, 2200],
            ["West", "Gadget", null, 1550],
            ["West", "Widget", null, 2400],
            [null, null, 2023, 3600],
            [null, null, 2024, 4250],
            [null, null, null, 7850]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_grouping_sets_empty_set() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, SUM(amount) AS total
             FROM sales
             GROUP BY GROUPING SETS ((region), ())
             ORDER BY region NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(result, [["East", 3900], ["West", 3950], [null, 7850]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_rollup_with_count() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, COUNT(*) AS cnt, SUM(amount) AS total
             FROM sales
             GROUP BY ROLLUP (region, product)
             ORDER BY region NULLS LAST, product NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 2, 1700],
            ["East", "Widget", 2, 2200],
            ["East", null, 4, 3900],
            ["West", "Gadget", 2, 1550],
            ["West", "Widget", 2, 2400],
            ["West", null, 4, 3950],
            [null, null, 8, 7850]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_cube_with_avg() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, AVG(amount) AS avg_amount
             FROM sales
             GROUP BY CUBE (region, product)
             ORDER BY region NULLS LAST, product NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 850.0],
            ["East", "Widget", 1100.0],
            ["East", null, 975.0],
            ["West", "Gadget", 775.0],
            ["West", "Widget", 1200.0],
            ["West", null, 987.5],
            [null, "Gadget", 812.5],
            [null, "Widget", 1150.0],
            [null, null, 981.25]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_grouping_sets_distinct() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT DISTINCT region, SUM(amount) AS total
             FROM sales
             GROUP BY GROUPING SETS ((region), (region, product))
             ORDER BY region",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", 1700],
            ["East", 2200],
            ["East", 3900],
            ["West", 1550],
            ["West", 2400],
            ["West", 3950]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_filter_by_grouping() {
    let mut executor = create_executor();
    setup_sales_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, product, SUM(amount) AS total
             FROM sales
             GROUP BY CUBE (region, product)
             HAVING GROUPING(region) = 0
             ORDER BY region, product NULLS LAST",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["East", "Gadget", 1700],
            ["East", "Widget", 2200],
            ["East", null, 3900],
            ["West", "Gadget", 1550],
            ["West", "Widget", 2400],
            ["West", null, 3950]
        ]
    );
}
