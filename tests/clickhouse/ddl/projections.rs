use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_table_with_projection() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE proj_test (
                id UInt64,
                date Date,
                category String,
                value Int64,
                PROJECTION proj_by_category (
                    SELECT category, sum(value)
                    GROUP BY category
                )
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_projection_with_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE proj_order (
                id UInt64,
                timestamp DateTime,
                user_id UInt32,
                event String,
                PROJECTION proj_by_time (
                    SELECT *
                    ORDER BY timestamp
                )
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO proj_order VALUES
            (1, '2023-01-01 10:00:00', 100, 'click'),
            (2, '2023-01-01 09:00:00', 101, 'view'),
            (3, '2023-01-01 11:00:00', 100, 'purchase')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM proj_order ORDER BY timestamp")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[test]
fn test_projection_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE proj_agg (
                id UInt64,
                product String,
                region String,
                sales Int64,
                PROJECTION proj_sales_by_region (
                    SELECT region, sum(sales) AS total_sales
                    GROUP BY region
                )
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO proj_agg VALUES
            (1, 'A', 'North', 100),
            (2, 'B', 'South', 200),
            (3, 'A', 'North', 150),
            (4, 'C', 'South', 300)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT region, sum(sales) FROM proj_agg GROUP BY region ORDER BY region")
        .unwrap();
    assert_table_eq!(result, [["North", 250], ["South", 500]]);
}

#[test]
fn test_multiple_projections() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE multi_proj (
                id UInt64,
                date Date,
                category String,
                subcategory String,
                amount Float64,
                PROJECTION proj_by_date (
                    SELECT date, sum(amount)
                    GROUP BY date
                ),
                PROJECTION proj_by_category (
                    SELECT category, sum(amount)
                    GROUP BY category
                ),
                PROJECTION proj_by_both (
                    SELECT date, category, sum(amount)
                    GROUP BY date, category
                )
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO multi_proj VALUES
            (1, '2023-01-01', 'Electronics', 'Phones', 500.0),
            (2, '2023-01-01', 'Electronics', 'Laptops', 1000.0),
            (3, '2023-01-02', 'Clothing', 'Shirts', 50.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, sum(amount) FROM multi_proj GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_add_projection() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE add_proj_test (
                id UInt64,
                name String,
                value Int64
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "ALTER TABLE add_proj_test ADD PROJECTION proj_by_name (
                SELECT name, sum(value)
                GROUP BY name
            )",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_drop_projection() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE drop_proj_test (
                id UInt64,
                value Int64,
                PROJECTION proj_sum (
                    SELECT sum(value)
                )
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    let result = executor
        .execute_sql("ALTER TABLE drop_proj_test DROP PROJECTION proj_sum")
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_materialize_projection() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mat_proj_test (
                id UInt64,
                date Date,
                value Int64
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO mat_proj_test VALUES (1, '2023-01-01', 100), (2, '2023-01-02', 200)",
        )
        .unwrap();

    executor
        .execute_sql(
            "ALTER TABLE mat_proj_test ADD PROJECTION proj_by_date (
                SELECT date, sum(value) GROUP BY date
            )",
        )
        .unwrap();

    let result = executor
        .execute_sql("ALTER TABLE mat_proj_test MATERIALIZE PROJECTION proj_by_date")
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_projection_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE proj_where (
                id UInt64,
                status String,
                amount Int64,
                PROJECTION proj_active (
                    SELECT status, sum(amount)
                    GROUP BY status
                )
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO proj_where VALUES
            (1, 'active', 100),
            (2, 'inactive', 50),
            (3, 'active', 200)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT sum(amount) FROM proj_where WHERE status = 'active'")
        .unwrap();
    assert_table_eq!(result, [[300]]);
}

#[test]
fn test_projection_count() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE proj_count (
                id UInt64,
                category String,
                PROJECTION proj_cat_count (
                    SELECT category, count()
                    GROUP BY category
                )
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO proj_count VALUES
            (1, 'A'), (2, 'B'), (3, 'A'), (4, 'A'), (5, 'B')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, count() FROM proj_count GROUP BY category ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", 3], ["B", 2]]);
}

#[test]
fn test_projection_min_max() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE proj_minmax (
                id UInt64,
                group_id UInt32,
                value Float64,
                PROJECTION proj_stats (
                    SELECT group_id, min(value), max(value), avg(value)
                    GROUP BY group_id
                )
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO proj_minmax VALUES
            (1, 1, 10.5), (2, 1, 20.5), (3, 2, 15.0), (4, 2, 25.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT group_id, min(value), max(value) FROM proj_minmax GROUP BY group_id ORDER BY group_id")
        .unwrap();
    assert_table_eq!(result, [[1, 10.5, 20.5], [2, 15.0, 25.0]]);
}

#[test]
fn test_clear_projection() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE clear_proj (
                id UInt64,
                value Int64,
                PROJECTION proj_sum (SELECT sum(value))
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO clear_proj VALUES (1, 100), (2, 200)")
        .unwrap();

    let result = executor
        .execute_sql("ALTER TABLE clear_proj CLEAR PROJECTION proj_sum")
        .unwrap();
    assert_table_eq!(result, []);
}
