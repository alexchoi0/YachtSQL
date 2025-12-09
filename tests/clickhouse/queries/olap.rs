use crate::common::create_executor;

#[test]
fn test_olap_fact_dimension_join() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE dim_product (
                product_id INT64,
                product_name String,
                category String
            ) ENGINE = MergeTree ORDER BY product_id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE dim_time (
                date_id INT64,
                date Date,
                year INT64,
                month INT64,
                quarter INT64
            ) ENGINE = MergeTree ORDER BY date_id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE fact_sales (
                sale_id INT64,
                product_id INT64,
                date_id INT64,
                quantity INT64,
                revenue INT64
            ) ENGINE = MergeTree ORDER BY sale_id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO dim_product VALUES (1, 'Widget A', 'Electronics'), (2, 'Widget B', 'Electronics'), (3, 'Gadget C', 'Home')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dim_time VALUES (1, '2023-01-15', 2023, 1, 1), (2, '2023-04-20', 2023, 4, 2), (3, '2023-07-10', 2023, 7, 3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fact_sales VALUES (1, 1, 1, 10, 1000), (2, 2, 1, 5, 500), (3, 1, 2, 15, 1500), (4, 3, 3, 8, 800)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT p.category, t.quarter, SUM(f.revenue) AS total_revenue
            FROM fact_sales f
            JOIN dim_product p ON f.product_id = p.product_id
            JOIN dim_time t ON f.date_id = t.date_id
            GROUP BY p.category, t.quarter
            ORDER BY p.category, t.quarter",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_rollup_hierarchy() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE sales_rollup (region String, country String, city String, sales INT64)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO sales_rollup VALUES
            ('Americas', 'USA', 'New York', 1000),
            ('Americas', 'USA', 'Los Angeles', 800),
            ('Americas', 'Canada', 'Toronto', 600),
            ('Europe', 'UK', 'London', 900),
            ('Europe', 'Germany', 'Berlin', 700)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, country, city, SUM(sales) AS total
            FROM sales_rollup
            GROUP BY ROLLUP(region, country, city)
            ORDER BY region, country, city",
        )
        .unwrap();
    assert!(result.num_rows() >= 12);
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_cube_analysis() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cube_analysis (year INT64, product String, channel String, revenue INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO cube_analysis VALUES
            (2022, 'A', 'Online', 1000),
            (2022, 'A', 'Store', 800),
            (2022, 'B', 'Online', 600),
            (2023, 'A', 'Online', 1200),
            (2023, 'B', 'Store', 900)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT year, product, channel, SUM(revenue) AS total
            FROM cube_analysis
            GROUP BY CUBE(year, product, channel)
            ORDER BY year, product, channel",
        )
        .unwrap();
    assert!(result.num_rows() >= 20);
}

#[test]
fn test_olap_year_over_year() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE yoy_sales (year INT64, month INT64, revenue INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO yoy_sales VALUES
            (2022, 1, 1000), (2022, 2, 1100), (2022, 3, 1200),
            (2023, 1, 1100), (2023, 2, 1250), (2023, 3, 1350)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                curr.month,
                curr.revenue AS current_year,
                prev.revenue AS previous_year,
                curr.revenue - prev.revenue AS yoy_change,
                round((curr.revenue - prev.revenue) * 100.0 / prev.revenue, 2) AS yoy_pct
            FROM yoy_sales curr
            JOIN yoy_sales prev ON curr.month = prev.month AND curr.year = prev.year + 1
            ORDER BY curr.month",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[test]
fn test_olap_running_total() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE running_total (date Date, revenue INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO running_total VALUES
            ('2023-01-01', 100), ('2023-01-02', 150), ('2023-01-03', 200),
            ('2023-01-04', 120), ('2023-01-05', 180)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                date,
                revenue,
                SUM(revenue) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
            FROM running_total
            ORDER BY date"
        )
        .unwrap();
    assert!(result.num_rows() == 5); // TODO: use table![[expected_values]]
}

#[test]
fn test_olap_moving_average() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE moving_avg (date Date, value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO moving_avg VALUES
            ('2023-01-01', 100), ('2023-01-02', 120), ('2023-01-03', 110),
            ('2023-01-04', 130), ('2023-01-05', 140), ('2023-01-06', 125),
            ('2023-01-07', 135)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                date,
                value,
                AVG(value) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3
            FROM moving_avg
            ORDER BY date"
        )
        .unwrap();
    assert!(result.num_rows() == 7); // TODO: use table![[expected_values]]
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_percentile_analysis() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE percentile_data (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO percentile_data VALUES
            ('A', 10), ('A', 20), ('A', 30), ('A', 40), ('A', 50),
            ('B', 15), ('B', 25), ('B', 35), ('B', 45), ('B', 55)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                category,
                quantile(0.5)(value) AS median,
                quantile(0.25)(value) AS q1,
                quantile(0.75)(value) AS q3
            FROM percentile_data
            GROUP BY category
            ORDER BY category",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_top_n_per_group() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE top_n_group (category String, product String, sales INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO top_n_group VALUES
            ('Electronics', 'Phone', 1000), ('Electronics', 'Laptop', 1500), ('Electronics', 'Tablet', 800),
            ('Clothing', 'Shirt', 500), ('Clothing', 'Pants', 600), ('Clothing', 'Jacket', 400)"
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, product, sales FROM (
                SELECT category, product, sales,
                    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rn
                FROM top_n_group
            ) WHERE rn <= 2
            ORDER BY category, sales DESC",
        )
        .unwrap();
    assert!(result.num_rows() == 4); // TODO: use table![[expected_values]]
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_market_basket() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE transactions (transaction_id INT64, product String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO transactions VALUES
            (1, 'Bread'), (1, 'Milk'), (1, 'Eggs'),
            (2, 'Bread'), (2, 'Milk'),
            (3, 'Bread'), (3, 'Eggs'),
            (4, 'Milk'), (4, 'Eggs'),
            (5, 'Bread'), (5, 'Milk'), (5, 'Eggs')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                a.product AS product_a,
                b.product AS product_b,
                COUNT(DISTINCT a.transaction_id) AS co_occurrence
            FROM transactions a
            JOIN transactions b ON a.transaction_id = b.transaction_id AND a.product < b.product
            GROUP BY a.product, b.product
            ORDER BY co_occurrence DESC, product_a",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_cohort_analysis() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE user_activity (
                user_id INT64,
                signup_month String,
                activity_month String
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO user_activity VALUES
            (1, '2023-01', '2023-01'), (1, '2023-01', '2023-02'), (1, '2023-01', '2023-03'),
            (2, '2023-01', '2023-01'), (2, '2023-01', '2023-02'),
            (3, '2023-02', '2023-02'), (3, '2023-02', '2023-03'),
            (4, '2023-02', '2023-02')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                signup_month AS cohort,
                activity_month,
                COUNT(DISTINCT user_id) AS active_users
            FROM user_activity
            GROUP BY signup_month, activity_month
            ORDER BY signup_month, activity_month",
        )
        .unwrap();
    assert!(result.num_rows() == 5); // TODO: use table![[expected_values]]
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_funnel_analysis() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE funnel_events (
                user_id INT64,
                event_type String,
                event_time DateTime
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO funnel_events VALUES
            (1, 'view', '2023-01-01 10:00:00'), (1, 'click', '2023-01-01 10:05:00'), (1, 'purchase', '2023-01-01 10:10:00'),
            (2, 'view', '2023-01-01 11:00:00'), (2, 'click', '2023-01-01 11:05:00'),
            (3, 'view', '2023-01-01 12:00:00'),
            (4, 'view', '2023-01-01 13:00:00'), (4, 'click', '2023-01-01 13:05:00'), (4, 'purchase', '2023-01-01 13:10:00')"
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                event_type,
                COUNT(DISTINCT user_id) AS users,
                COUNT(DISTINCT user_id) * 100.0 / (SELECT COUNT(DISTINCT user_id) FROM funnel_events WHERE event_type = 'view') AS conversion_rate
            FROM funnel_events
            GROUP BY event_type
            ORDER BY CASE event_type WHEN 'view' THEN 1 WHEN 'click' THEN 2 WHEN 'purchase' THEN 3 END"
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_sessionization() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE page_views (
                user_id INT64,
                page String,
                view_time DateTime
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO page_views VALUES
            (1, 'home', '2023-01-01 10:00:00'),
            (1, 'product', '2023-01-01 10:05:00'),
            (1, 'cart', '2023-01-01 10:08:00'),
            (1, 'home', '2023-01-01 15:00:00'),
            (1, 'product', '2023-01-01 15:03:00')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                user_id,
                page,
                view_time,
                SUM(new_session) OVER (PARTITION BY user_id ORDER BY view_time) AS session_id
            FROM (
                SELECT
                    user_id,
                    page,
                    view_time,
                    CASE WHEN dateDiff('minute', lagInFrame(view_time) OVER (PARTITION BY user_id ORDER BY view_time), view_time) > 30
                         OR lagInFrame(view_time) OVER (PARTITION BY user_id ORDER BY view_time) IS NULL
                         THEN 1 ELSE 0 END AS new_session
                FROM page_views
            )
            ORDER BY user_id, view_time"
        )
        .unwrap();
    assert!(result.num_rows() == 5); // TODO: use table![[expected_values]]
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_time_series_fill() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sparse_data (date Date, value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO sparse_data VALUES
            ('2023-01-01', 100),
            ('2023-01-03', 150),
            ('2023-01-05', 200)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH dates AS (
                SELECT toDate('2023-01-01') + number AS date
                FROM numbers(5)
            )
            SELECT
                d.date,
                COALESCE(s.value, 0) AS value
            FROM dates d
            LEFT JOIN sparse_data s ON d.date = s.date
            ORDER BY d.date",
        )
        .unwrap();
    assert!(result.num_rows() == 5); // TODO: use table![[expected_values]]
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_pareto_analysis() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pareto_data (product String, revenue INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO pareto_data VALUES
            ('A', 5000), ('B', 3000), ('C', 1500), ('D', 800), ('E', 500), ('F', 200)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                product,
                revenue,
                SUM(revenue) OVER (ORDER BY revenue DESC) AS cumulative_revenue,
                SUM(revenue) OVER (ORDER BY revenue DESC) * 100.0 / SUM(revenue) OVER () AS cumulative_pct
            FROM pareto_data
            ORDER BY revenue DESC"
        )
        .unwrap();
    assert!(result.num_rows() == 6); // TODO: use table![[expected_values]]
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_abc_classification() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE abc_items (item String, value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO abc_items VALUES
            ('I1', 10000), ('I2', 8000), ('I3', 5000), ('I4', 3000),
            ('I5', 2000), ('I6', 1000), ('I7', 500), ('I8', 300), ('I9', 150), ('I10', 50)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                item,
                value,
                CASE
                    WHEN cumulative_pct <= 70 THEN 'A'
                    WHEN cumulative_pct <= 90 THEN 'B'
                    ELSE 'C'
                END AS class
            FROM (
                SELECT
                    item,
                    value,
                    SUM(value) OVER (ORDER BY value DESC) * 100.0 / SUM(value) OVER () AS cumulative_pct
                FROM abc_items
            )
            ORDER BY value DESC"
        )
        .unwrap();
    assert!(result.num_rows() == 10); // TODO: use table![[expected_values]]
}

#[ignore = "Requires complex subquery support"]
#[test]
fn test_olap_growth_rate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE growth_data (period INT64, revenue INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO growth_data VALUES
            (1, 1000), (2, 1200), (3, 1100), (4, 1500), (5, 1800)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                period,
                revenue,
                lagInFrame(revenue) OVER (ORDER BY period) AS prev_revenue,
                (revenue - lagInFrame(revenue) OVER (ORDER BY period)) * 100.0 /
                    lagInFrame(revenue) OVER (ORDER BY period) AS growth_rate
            FROM growth_data
            ORDER BY period",
        )
        .unwrap();
    assert!(result.num_rows() == 5); // TODO: use table![[expected_values]]
}
