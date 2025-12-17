use crate::assert_table_eq;
use crate::common::{create_executor, d, ts};

fn setup_sales_data(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql(
            "CREATE TABLE sales (
                sale_id INT64,
                product_id INT64,
                product_name STRING,
                category STRING,
                region STRING,
                sale_date DATE,
                quantity INT64,
                unit_price INT64,
                total_amount INT64,
                customer_id INT64
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO sales VALUES
            (1, 101, 'Widget A', 'Electronics', 'North', DATE '2024-01-15', 5, 100, 500, 1001),
            (2, 102, 'Widget B', 'Electronics', 'South', DATE '2024-01-16', 3, 150, 450, 1002),
            (3, 103, 'Gadget X', 'Electronics', 'North', DATE '2024-01-17', 2, 200, 400, 1001),
            (4, 201, 'Chair', 'Furniture', 'East', DATE '2024-01-18', 4, 75, 300, 1003),
            (5, 202, 'Desk', 'Furniture', 'West', DATE '2024-01-19', 1, 500, 500, 1004),
            (6, 101, 'Widget A', 'Electronics', 'South', DATE '2024-02-01', 10, 100, 1000, 1002),
            (7, 103, 'Gadget X', 'Electronics', 'East', DATE '2024-02-02', 5, 200, 1000, 1005),
            (8, 201, 'Chair', 'Furniture', 'North', DATE '2024-02-03', 8, 75, 600, 1001),
            (9, 301, 'Book', 'Media', 'West', DATE '2024-02-04', 20, 25, 500, 1006),
            (10, 302, 'Magazine', 'Media', 'South', DATE '2024-02-05', 50, 10, 500, 1007)",
        )
        .unwrap();
}

#[test]
fn test_sales_by_category() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT category, SUM(total_amount) AS revenue
            FROM sales
            GROUP BY category
            ORDER BY revenue DESC",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["Electronics", 3350], ["Furniture", 1400], ["Media", 1000]]
    );
}

#[test]
fn test_sales_by_region_and_category() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT region, category, SUM(total_amount) AS revenue
            FROM sales
            GROUP BY region, category
            ORDER BY region, category",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["East", "Electronics", 1000],
            ["East", "Furniture", 300],
            ["North", "Electronics", 900],
            ["North", "Furniture", 600],
            ["South", "Electronics", 1450],
            ["South", "Media", 500],
            ["West", "Furniture", 500],
            ["West", "Media", 500],
        ]
    );
}

#[test]
fn test_top_products_by_revenue() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT product_name, SUM(total_amount) AS revenue
            FROM sales
            GROUP BY product_name
            ORDER BY revenue DESC, product_name
            LIMIT 5",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Widget A", 1500],
            ["Gadget X", 1400],
            ["Chair", 900],
            ["Book", 500],
            ["Desk", 500],
        ]
    );
}

#[test]
fn test_monthly_revenue_trend() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                DATE_TRUNC(sale_date, MONTH) AS month,
                SUM(total_amount) AS revenue,
                COUNT(*) AS num_sales
            FROM sales
            GROUP BY month
            ORDER BY month",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [[d(2024, 1, 1), 2150, 5], [d(2024, 2, 1), 3600, 5],]
    );
}

#[test]
fn test_running_total() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                sale_date,
                total_amount,
                SUM(total_amount) OVER (ORDER BY sale_date) AS running_total
            FROM sales
            ORDER BY sale_date",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(2024, 1, 15), 500, 500],
            [d(2024, 1, 16), 450, 950],
            [d(2024, 1, 17), 400, 1350],
            [d(2024, 1, 18), 300, 1650],
            [d(2024, 1, 19), 500, 2150],
            [d(2024, 2, 1), 1000, 3150],
            [d(2024, 2, 2), 1000, 4150],
            [d(2024, 2, 3), 600, 4750],
            [d(2024, 2, 4), 500, 5250],
            [d(2024, 2, 5), 500, 5750],
        ]
    );
}

#[test]
fn test_moving_average() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                sale_date,
                total_amount,
                AVG(total_amount) OVER (
                    ORDER BY sale_date
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) AS moving_avg_3
            FROM sales
            ORDER BY sale_date",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(2024, 1, 15), 500, 500.0],
            [d(2024, 1, 16), 450, 475.0],
            [d(2024, 1, 17), 400, 450.0],
            [d(2024, 1, 18), 300, 383.3333333333333],
            [d(2024, 1, 19), 500, 400.0],
            [d(2024, 2, 1), 1000, 600.0],
            [d(2024, 2, 2), 1000, 833.3333333333334],
            [d(2024, 2, 3), 600, 866.6666666666666],
            [d(2024, 2, 4), 500, 700.0],
            [d(2024, 2, 5), 500, 533.3333333333334],
        ]
    );
}

#[test]
fn test_rank_products_by_sales() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                product_name,
                SUM(total_amount) AS revenue,
                RANK() OVER (ORDER BY SUM(total_amount) DESC) AS revenue_rank
            FROM sales
            GROUP BY product_name
            ORDER BY revenue_rank, product_name",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Widget A", 1500, 1],
            ["Gadget X", 1400, 2],
            ["Chair", 900, 3],
            ["Book", 500, 4],
            ["Desk", 500, 4],
            ["Magazine", 500, 4],
            ["Widget B", 450, 7],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_dense_rank_by_category() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                category,
                product_name,
                SUM(total_amount) AS revenue,
                DENSE_RANK() OVER (
                    PARTITION BY category
                    ORDER BY SUM(total_amount) DESC
                ) AS category_rank
            FROM sales
            GROUP BY category, product_name
            ORDER BY category, category_rank, product_name",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Electronics", "Widget A", 1500, 1],
            ["Electronics", "Gadget X", 1400, 2],
            ["Electronics", "Widget B", 450, 3],
            ["Furniture", "Chair", 900, 1],
            ["Furniture", "Desk", 500, 2],
            ["Media", "Book", 500, 1],
            ["Media", "Magazine", 500, 1],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_percentile_analysis() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                product_name,
                total_amount,
                ROUND(PERCENT_RANK() OVER (ORDER BY total_amount), 2) AS percentile
            FROM sales
            ORDER BY percentile DESC, product_name",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gadget X", 1000, 0.89],
            ["Widget A", 1000, 0.89],
            ["Chair", 600, 0.78],
            ["Book", 500, 0.33],
            ["Desk", 500, 0.33],
            ["Magazine", 500, 0.33],
            ["Widget A", 500, 0.33],
            ["Widget B", 450, 0.22],
            ["Gadget X", 400, 0.11],
            ["Chair", 300, 0.0],
        ]
    );
}

#[test]
fn test_ntile_quartiles() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                product_name,
                total_amount,
                NTILE(4) OVER (ORDER BY total_amount) AS quartile
            FROM sales
            ORDER BY quartile, total_amount",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Chair", 300, 1],
            ["Gadget X", 400, 1],
            ["Widget B", 450, 1],
            ["Widget A", 500, 2],
            ["Desk", 500, 2],
            ["Book", 500, 2],
            ["Magazine", 500, 3],
            ["Chair", 600, 3],
            ["Widget A", 1000, 4],
            ["Gadget X", 1000, 4],
        ]
    );
}

#[test]
fn test_lead_lag_analysis() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                sale_date,
                total_amount,
                LAG(total_amount, 1) OVER (ORDER BY sale_date) AS prev_amount,
                LEAD(total_amount, 1) OVER (ORDER BY sale_date) AS next_amount
            FROM sales
            ORDER BY sale_date",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(2024, 1, 15), 500, null, 450],
            [d(2024, 1, 16), 450, 500, 400],
            [d(2024, 1, 17), 400, 450, 300],
            [d(2024, 1, 18), 300, 400, 500],
            [d(2024, 1, 19), 500, 300, 1000],
            [d(2024, 2, 1), 1000, 500, 1000],
            [d(2024, 2, 2), 1000, 1000, 600],
            [d(2024, 2, 3), 600, 1000, 500],
            [d(2024, 2, 4), 500, 600, 500],
            [d(2024, 2, 5), 500, 500, null],
        ]
    );
}

#[test]
fn test_first_last_value() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                category,
                product_name,
                total_amount,
                FIRST_VALUE(product_name) OVER (
                    PARTITION BY category
                    ORDER BY total_amount DESC
                ) AS top_product,
                LAST_VALUE(product_name) OVER (
                    PARTITION BY category
                    ORDER BY total_amount DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS bottom_product
            FROM sales
            ORDER BY category, total_amount DESC",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Electronics", "Widget A", 1000, "Widget A", "Gadget X"],
            ["Electronics", "Gadget X", 1000, "Widget A", "Gadget X"],
            ["Electronics", "Widget A", 500, "Widget A", "Gadget X"],
            ["Electronics", "Widget B", 450, "Widget A", "Gadget X"],
            ["Electronics", "Gadget X", 400, "Widget A", "Gadget X"],
            ["Furniture", "Chair", 600, "Chair", "Chair"],
            ["Furniture", "Desk", 500, "Chair", "Chair"],
            ["Furniture", "Chair", 300, "Chair", "Chair"],
            ["Media", "Book", 500, "Book", "Magazine"],
            ["Media", "Magazine", 500, "Book", "Magazine"],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_customer_purchase_frequency() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                customer_id,
                COUNT(*) AS num_purchases,
                SUM(total_amount) AS total_spent,
                AVG(total_amount) AS avg_purchase
            FROM sales
            GROUP BY customer_id
            HAVING COUNT(*) > 1
            ORDER BY total_spent DESC",
        )
        .unwrap();
    assert_table_eq!(result, [[1001, 3, 1500, 500.0], [1002, 2, 1450, 725.0],]);
}

#[test]
fn test_year_over_year_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE yearly_sales (
                year INT64,
                quarter INT64,
                revenue INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO yearly_sales VALUES
            (2023, 1, 1000), (2023, 2, 1200), (2023, 3, 1100), (2023, 4, 1500),
            (2024, 1, 1100), (2024, 2, 1400), (2024, 3, 1300), (2024, 4, 1700)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                a.quarter,
                a.rev_a AS revenue_2023,
                b.rev_b AS revenue_2024,
                b.rev_b - a.rev_a AS yoy_change,
                ROUND((b.rev_b - a.rev_a) * 100.0 / a.rev_a, 2) AS yoy_pct
            FROM (SELECT quarter, revenue AS rev_a FROM yearly_sales WHERE year = 2023) a
            JOIN (SELECT quarter, revenue AS rev_b FROM yearly_sales WHERE year = 2024) b ON a.quarter = b.quarter
            ORDER BY a.quarter",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, 1000, 1100, 100, 10.0],
            [2, 1200, 1400, 200, 16.67],
            [3, 1100, 1300, 200, 18.18],
            [4, 1500, 1700, 200, 13.33],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_cohort_analysis() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE user_activity (
                user_id INT64,
                signup_date DATE,
                activity_date DATE,
                activity_type STRING
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO user_activity VALUES
            (1, DATE '2024-01-01', DATE '2024-01-01', 'signup'),
            (1, DATE '2024-01-01', DATE '2024-01-15', 'purchase'),
            (1, DATE '2024-01-01', DATE '2024-02-01', 'purchase'),
            (2, DATE '2024-01-15', DATE '2024-01-15', 'signup'),
            (2, DATE '2024-01-15', DATE '2024-01-20', 'purchase'),
            (3, DATE '2024-02-01', DATE '2024-02-01', 'signup'),
            (3, DATE '2024-02-01', DATE '2024-02-15', 'purchase')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                DATE_TRUNC(signup_date, MONTH) AS cohort_month,
                DATE_DIFF(activity_date, signup_date, DAY) AS days_since_signup,
                COUNT(DISTINCT user_id) AS active_users
            FROM user_activity
            WHERE activity_type = 'purchase'
            GROUP BY cohort_month, days_since_signup
            ORDER BY cohort_month, days_since_signup",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(2024, 1, 1), 5, 1],
            [d(2024, 1, 1), 14, 1],
            [d(2024, 1, 1), 31, 1],
            [d(2024, 2, 1), 14, 1],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_funnel_analysis() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE events (
                user_id INT64,
                event_name STRING,
                event_time TIMESTAMP
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO events VALUES
            (1, 'page_view', TIMESTAMP '2024-01-01 10:00:00'),
            (1, 'add_to_cart', TIMESTAMP '2024-01-01 10:05:00'),
            (1, 'checkout', TIMESTAMP '2024-01-01 10:10:00'),
            (1, 'purchase', TIMESTAMP '2024-01-01 10:15:00'),
            (2, 'page_view', TIMESTAMP '2024-01-01 11:00:00'),
            (2, 'add_to_cart', TIMESTAMP '2024-01-01 11:05:00'),
            (3, 'page_view', TIMESTAMP '2024-01-01 12:00:00'),
            (4, 'page_view', TIMESTAMP '2024-01-01 13:00:00'),
            (4, 'add_to_cart', TIMESTAMP '2024-01-01 13:05:00'),
            (4, 'checkout', TIMESTAMP '2024-01-01 13:10:00')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH total_views AS (
                SELECT COUNT(DISTINCT user_id) AS total FROM events WHERE event_name = 'page_view'
            )
            SELECT
                event_name,
                COUNT(DISTINCT user_id) AS users,
                COUNT(DISTINCT user_id) * 100.0 / (SELECT total FROM total_views) AS conversion_rate
            FROM events
            GROUP BY event_name
            ORDER BY
                CASE event_name
                    WHEN 'page_view' THEN 1
                    WHEN 'add_to_cart' THEN 2
                    WHEN 'checkout' THEN 3
                    WHEN 'purchase' THEN 4
                END",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["page_view", 4, 100.0],
            ["add_to_cart", 3, 75.0],
            ["checkout", 2, 50.0],
            ["purchase", 1, 25.0],
        ]
    );
}

#[test]
fn test_retention_analysis() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE logins (
                user_id INT64,
                login_date DATE
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO logins VALUES
            (1, DATE '2024-01-01'), (1, DATE '2024-01-08'), (1, DATE '2024-01-15'),
            (2, DATE '2024-01-01'), (2, DATE '2024-01-08'),
            (3, DATE '2024-01-01'),
            (4, DATE '2024-01-08'), (4, DATE '2024-01-15'),
            (5, DATE '2024-01-01'), (5, DATE '2024-01-08'), (5, DATE '2024-01-15'), (5, DATE '2024-01-22')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH first_login AS (
                SELECT user_id, MIN(login_date) AS first_date
                FROM logins
                GROUP BY user_id
            ),
            weekly_activity AS (
                SELECT
                    l.user_id,
                    f.first_date,
                    DATE_DIFF(l.login_date, f.first_date, WEEK) AS week_number
                FROM logins l
                JOIN first_login f ON l.user_id = f.user_id
            )
            SELECT
                week_number,
                COUNT(DISTINCT user_id) AS retained_users
            FROM weekly_activity
            GROUP BY week_number
            ORDER BY week_number",
        )
        .unwrap();
    assert_table_eq!(result, [[0, 5], [1, 4], [2, 2], [3, 1],]);
}

#[test]
fn test_abc_analysis() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "WITH product_revenue AS (
                SELECT
                    product_name,
                    SUM(total_amount) AS revenue
                FROM sales
                GROUP BY product_name
            ),
            cumulative AS (
                SELECT
                    product_name,
                    revenue,
                    SUM(revenue) OVER (ORDER BY revenue DESC) AS cumulative_revenue,
                    SUM(revenue) OVER () AS total_revenue
                FROM product_revenue
            )
            SELECT
                product_name,
                revenue,
                ROUND(cumulative_revenue * 100.0 / total_revenue, 2) AS cumulative_pct,
                CASE
                    WHEN cumulative_revenue * 100.0 / total_revenue <= 80 THEN 'A'
                    WHEN cumulative_revenue * 100.0 / total_revenue <= 95 THEN 'B'
                    ELSE 'C'
                END AS abc_class
            FROM cumulative
            ORDER BY revenue DESC, product_name",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Widget A", 1500, 26.09, "A"],
            ["Gadget X", 1400, 50.43, "A"],
            ["Chair", 900, 66.09, "A"],
            ["Book", 500, 92.17, "B"],
            ["Desk", 500, 92.17, "B"],
            ["Magazine", 500, 92.17, "B"],
            ["Widget B", 450, 100.0, "C"],
        ]
    );
}

#[test]
fn test_rfm_analysis() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE transactions (
                customer_id INT64,
                transaction_date DATE,
                amount INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO transactions VALUES
            (1, DATE '2024-01-01', 100), (1, DATE '2024-01-15', 200), (1, DATE '2024-02-01', 150),
            (2, DATE '2024-01-05', 500),
            (3, DATE '2024-01-10', 50), (3, DATE '2024-01-20', 75), (3, DATE '2024-01-30', 60), (3, DATE '2024-02-10', 80),
            (4, DATE '2024-02-01', 1000)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH rfm AS (
                SELECT
                    customer_id,
                    DATE_DIFF(DATE '2024-02-15', MAX(transaction_date), DAY) AS recency,
                    COUNT(*) AS frequency,
                    SUM(amount) AS monetary
                FROM transactions
                GROUP BY customer_id
            )
            SELECT
                customer_id,
                recency,
                frequency,
                monetary,
                NTILE(4) OVER (ORDER BY recency DESC, customer_id) AS r_score,
                NTILE(4) OVER (ORDER BY frequency, customer_id) AS f_score,
                NTILE(4) OVER (ORDER BY monetary) AS m_score
            FROM rfm
            ORDER BY monetary DESC",
        )
        .unwrap();
    let r_scores: Vec<i64> = (0..result.num_rows())
        .map(|i| result.column(4).unwrap().get(i).unwrap().as_i64().unwrap())
        .collect();
    let f_scores: Vec<i64> = (0..result.num_rows())
        .map(|i| result.column(5).unwrap().get(i).unwrap().as_i64().unwrap())
        .collect();
    let m_scores: Vec<i64> = (0..result.num_rows())
        .map(|i| result.column(6).unwrap().get(i).unwrap().as_i64().unwrap())
        .collect();
    let recencies: Vec<i64> = (0..result.num_rows())
        .map(|i| result.column(1).unwrap().get(i).unwrap().as_i64().unwrap())
        .collect();
    assert_eq!(recencies, vec![14, 41, 14, 5]);
    assert!(r_scores.iter().all(|&x| (1..=4).contains(&x)));
    assert!(f_scores.iter().all(|&x| (1..=4).contains(&x)));
    assert_eq!(m_scores, vec![4, 3, 2, 1]);
}

#[test]
fn test_time_series_decomposition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE daily_metrics (
                metric_date DATE,
                value INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO daily_metrics VALUES
            (DATE '2024-01-01', 100), (DATE '2024-01-02', 110), (DATE '2024-01-03', 105),
            (DATE '2024-01-04', 115), (DATE '2024-01-05', 120), (DATE '2024-01-06', 125),
            (DATE '2024-01-07', 130), (DATE '2024-01-08', 135), (DATE '2024-01-09', 140),
            (DATE '2024-01-10', 138)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                metric_date,
                value,
                AVG(value) OVER (ORDER BY metric_date ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS trend,
                value - AVG(value) OVER (ORDER BY metric_date ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS residual
            FROM daily_metrics
            ORDER BY metric_date",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(2024, 1, 1), 100, 105.0, -5.0],
            [d(2024, 1, 2), 110, 107.5, 2.5],
            [d(2024, 1, 3), 105, 110.0, -5.0],
            [d(2024, 1, 4), 115, 115.0, 0.0],
            [d(2024, 1, 5), 120, 119.0, 1.0],
            [d(2024, 1, 6), 125, 125.0, 0.0],
            [d(2024, 1, 7), 130, 130.0, 0.0],
            [d(2024, 1, 8), 135, 133.6, 1.4000000000000057],
            [d(2024, 1, 9), 140, 135.75, 4.25],
            [d(2024, 1, 10), 138, 137.66666666666666, 0.3333333333333428],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_sessionization() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE clickstream (
                user_id INT64,
                event_time TIMESTAMP,
                page STRING
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO clickstream VALUES
            (1, TIMESTAMP '2024-01-01 10:00:00', 'home'),
            (1, TIMESTAMP '2024-01-01 10:02:00', 'products'),
            (1, TIMESTAMP '2024-01-01 10:05:00', 'cart'),
            (1, TIMESTAMP '2024-01-01 14:00:00', 'home'),
            (1, TIMESTAMP '2024-01-01 14:03:00', 'checkout'),
            (2, TIMESTAMP '2024-01-01 11:00:00', 'home'),
            (2, TIMESTAMP '2024-01-01 11:01:00', 'products')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH session_breaks AS (
                SELECT
                    user_id,
                    event_time,
                    page,
                    CASE
                        WHEN TIMESTAMP_DIFF(event_time,
                            LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time),
                            MINUTE) > 30
                        THEN 1
                        ELSE 0
                    END AS is_new_session
                FROM clickstream
            )
            SELECT
                user_id,
                event_time,
                page,
                SUM(is_new_session) OVER (
                    PARTITION BY user_id
                    ORDER BY event_time
                ) + 1 AS session_id
            FROM session_breaks
            ORDER BY user_id, event_time",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, ts(2024, 1, 1, 10, 0, 0), "home", 1],
            [1, ts(2024, 1, 1, 10, 2, 0), "products", 1],
            [1, ts(2024, 1, 1, 10, 5, 0), "cart", 1],
            [1, ts(2024, 1, 1, 14, 0, 0), "home", 2],
            [1, ts(2024, 1, 1, 14, 3, 0), "checkout", 2],
            [2, ts(2024, 1, 1, 11, 0, 0), "home", 1],
            [2, ts(2024, 1, 1, 11, 1, 0), "products", 1],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_market_basket_analysis() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE order_items (
                order_id INT64,
                product_id INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO order_items VALUES
            (1, 101), (1, 102), (1, 103),
            (2, 101), (2, 102),
            (3, 102), (3, 103),
            (4, 101), (4, 103),
            (5, 101), (5, 102), (5, 103), (5, 104)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH total_orders AS (
                SELECT COUNT(DISTINCT order_id) AS cnt FROM order_items
            ),
            items_a AS (
                SELECT order_id AS order_a, product_id AS prod_a FROM order_items
            ),
            items_b AS (
                SELECT order_id AS order_b, product_id AS prod_b FROM order_items
            ),
            product_pairs AS (
                SELECT
                    prod_a AS product_a,
                    prod_b AS product_b,
                    COUNT(DISTINCT order_a) AS co_occurrence
                FROM items_a
                JOIN items_b ON order_a = order_b
                WHERE prod_a < prod_b
                GROUP BY prod_a, prod_b
            )
            SELECT
                product_a,
                product_b,
                co_occurrence,
                co_occurrence * 100.0 / (SELECT cnt FROM total_orders) AS support_pct
            FROM product_pairs
            ORDER BY co_occurrence DESC, product_a, product_b",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [101, 102, 3, 60.0],
            [101, 103, 3, 60.0],
            [102, 103, 3, 60.0],
            [101, 104, 1, 20.0],
            [102, 104, 1, 20.0],
            [103, 104, 1, 20.0],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_pareto_analysis() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "WITH customer_value AS (
                SELECT
                    customer_id,
                    SUM(total_amount) AS total_value
                FROM sales
                GROUP BY customer_id
            ),
            ranked AS (
                SELECT
                    customer_id,
                    total_value,
                    ROW_NUMBER() OVER (ORDER BY total_value DESC, customer_id) AS customer_rank,
                    COUNT(*) OVER () AS total_customers,
                    SUM(total_value) OVER () AS grand_total
                FROM customer_value
            )
            SELECT
                customer_id,
                total_value,
                customer_rank * 100.0 / total_customers AS customer_percentile,
                SUM(total_value) OVER (ORDER BY total_value DESC) * 100.0 / grand_total AS cumulative_value_pct
            FROM ranked
            ORDER BY customer_rank, customer_id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1001, 1500, 14.285714285714286, 26.086956521739133],
            [1002, 1450, 28.571428571428573, 51.30434782608696],
            [1005, 1000, 42.857142857142854, 68.6956521739131],
            [1004, 500, 57.142857142857146, 94.78260869565217],
            [1006, 500, 71.42857142857143, 94.78260869565217],
            [1007, 500, 85.71428571428571, 94.78260869565217],
            [1003, 300, 100.0, 100.0],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_growth_rates() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE monthly_revenue (
                month DATE,
                revenue INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO monthly_revenue VALUES
            (DATE '2024-01-01', 10000),
            (DATE '2024-02-01', 12000),
            (DATE '2024-03-01', 11500),
            (DATE '2024-04-01', 14000),
            (DATE '2024-05-01', 15500),
            (DATE '2024-06-01', 16000)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                month,
                revenue,
                LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
                revenue - LAG(revenue) OVER (ORDER BY month) AS absolute_growth,
                ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 /
                    LAG(revenue) OVER (ORDER BY month), 2) AS growth_rate_pct
            FROM monthly_revenue
            ORDER BY month",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(2024, 1, 1), 10000, null, null, null],
            [d(2024, 2, 1), 12000, 10000, 2000, 20.0],
            [d(2024, 3, 1), 11500, 12000, -500, -4.17],
            [d(2024, 4, 1), 14000, 11500, 2500, 21.74],
            [d(2024, 5, 1), 15500, 14000, 1500, 10.71],
            [d(2024, 6, 1), 16000, 15500, 500, 3.23],
        ]
    );
}

#[test]
fn test_variance_analysis() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE budget_vs_actual (
                category STRING,
                budget INT64,
                actual INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO budget_vs_actual VALUES
            ('Marketing', 50000, 55000),
            ('Sales', 100000, 95000),
            ('Engineering', 200000, 210000),
            ('Operations', 75000, 70000)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                category,
                budget,
                actual,
                actual - budget AS variance,
                ROUND((actual - budget) * 100.0 / budget, 2) AS variance_pct,
                CASE
                    WHEN actual > budget THEN 'Over Budget'
                    WHEN actual < budget THEN 'Under Budget'
                    ELSE 'On Budget'
                END AS status
            FROM budget_vs_actual
            ORDER BY ABS(actual - budget) DESC",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Engineering", 200000, 210000, 10000, 5.0, "Over Budget"],
            ["Marketing", 50000, 55000, 5000, 10.0, "Over Budget"],
            ["Sales", 100000, 95000, -5000, -5.0, "Under Budget"],
            ["Operations", 75000, 70000, -5000, -6.67, "Under Budget"],
        ]
    );
}

#[test]
fn test_weighted_average() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE portfolio (
                asset STRING,
                value INT64,
                return_pct FLOAT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO portfolio VALUES
            ('Stocks', 50000, 8.5),
            ('Bonds', 30000, 4.2),
            ('Real Estate', 15000, 6.0),
            ('Cash', 5000, 1.5)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                SUM(value) AS total_portfolio,
                ROUND(SUM(value * return_pct) / SUM(value), 2) AS weighted_avg_return
            FROM portfolio",
        )
        .unwrap();
    assert_table_eq!(result, [[100000, 6.49]]);
}

#[test]
fn test_anomaly_detection_zscore() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE metrics (
                metric_date DATE,
                value INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO metrics VALUES
            (DATE '2024-01-01', 100), (DATE '2024-01-02', 102), (DATE '2024-01-03', 98),
            (DATE '2024-01-04', 105), (DATE '2024-01-05', 500),
            (DATE '2024-01-06', 101), (DATE '2024-01-07', 99), (DATE '2024-01-08', 103)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH stats AS (
                SELECT
                    AVG(value) AS mean_val,
                    STDDEV(value) AS stddev_val
                FROM metrics
            )
            SELECT
                m.metric_date,
                m.value,
                ROUND((m.value - s.mean_val) / s.stddev_val, 2) AS z_score,
                CASE
                    WHEN ABS((m.value - s.mean_val) / s.stddev_val) > 2 THEN 'Anomaly'
                    ELSE 'Normal'
                END AS status
            FROM metrics m
            CROSS JOIN stats s
            ORDER BY m.metric_date",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(2024, 1, 1), 100, -0.36, "Normal"],
            [d(2024, 1, 2), 102, -0.35, "Normal"],
            [d(2024, 1, 3), 98, -0.38, "Normal"],
            [d(2024, 1, 4), 105, -0.33, "Normal"],
            [d(2024, 1, 5), 500, 2.47, "Anomaly"],
            [d(2024, 1, 6), 101, -0.35, "Normal"],
            [d(2024, 1, 7), 99, -0.37, "Normal"],
            [d(2024, 1, 8), 103, -0.34, "Normal"],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_data_quality_checks() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                'Total Records' AS metric, CAST(COUNT(*) AS STRING) AS value
            FROM sales
            UNION ALL
            SELECT
                'Null Product Names', CAST(SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) AS STRING)
            FROM sales
            UNION ALL
            SELECT
                'Negative Amounts', CAST(SUM(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END) AS STRING)
            FROM sales
            UNION ALL
            SELECT
                'Duplicate Sale IDs', CAST(COUNT(*) - COUNT(DISTINCT sale_id) AS STRING)
            FROM sales",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Total Records", "10"],
            ["Null Product Names", "0"],
            ["Negative Amounts", "0"],
            ["Duplicate Sale IDs", "0"],
        ]
    );
}

#[test]
#[ignore = "TODO: CTE scoping for subqueries"]
fn test_complex_nested_ctes_with_array_agg_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE orders (
                order_id INT64,
                customer_id INT64,
                order_date DATE,
                status STRING,
                total_amount FLOAT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE order_items (
                item_id INT64,
                order_id INT64,
                product_id INT64,
                product_name STRING,
                quantity INT64,
                unit_price FLOAT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE customers (
                customer_id INT64,
                customer_name STRING,
                segment STRING,
                region STRING,
                signup_date DATE
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO customers VALUES
            (1, 'Acme Corp', 'Enterprise', 'North', DATE '2022-01-15'),
            (2, 'Beta Inc', 'SMB', 'South', DATE '2022-06-01'),
            (3, 'Gamma LLC', 'Enterprise', 'East', DATE '2023-01-10'),
            (4, 'Delta Co', 'Startup', 'West', DATE '2023-06-15'),
            (5, 'Epsilon Ltd', 'SMB', 'North', DATE '2023-09-01')",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO orders VALUES
            (101, 1, DATE '2024-01-05', 'completed', 5000.0),
            (102, 1, DATE '2024-01-20', 'completed', 3500.0),
            (103, 2, DATE '2024-01-10', 'completed', 1200.0),
            (104, 2, DATE '2024-02-01', 'pending', 800.0),
            (105, 3, DATE '2024-01-15', 'completed', 7500.0),
            (106, 3, DATE '2024-02-10', 'completed', 6200.0),
            (107, 4, DATE '2024-01-25', 'cancelled', 450.0),
            (108, 4, DATE '2024-02-05', 'completed', 320.0),
            (109, 5, DATE '2024-02-01', 'completed', 2100.0),
            (110, 1, DATE '2024-02-15', 'completed', 4200.0)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO order_items VALUES
            (1, 101, 1001, 'Widget Pro', 10, 300.0),
            (2, 101, 1002, 'Gadget Plus', 5, 400.0),
            (3, 102, 1001, 'Widget Pro', 7, 300.0),
            (4, 102, 1003, 'Tool Max', 2, 350.0),
            (5, 103, 1002, 'Gadget Plus', 3, 400.0),
            (6, 105, 1001, 'Widget Pro', 15, 300.0),
            (7, 105, 1002, 'Gadget Plus', 5, 400.0),
            (8, 105, 1004, 'Super Device', 2, 750.0),
            (9, 106, 1003, 'Tool Max', 10, 350.0),
            (10, 106, 1004, 'Super Device', 4, 700.0),
            (11, 109, 1001, 'Widget Pro', 5, 300.0),
            (12, 109, 1002, 'Gadget Plus', 2, 300.0),
            (13, 110, 1001, 'Widget Pro', 8, 300.0),
            (14, 110, 1003, 'Tool Max', 4, 350.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH customer_orders AS (
                SELECT
                    c.customer_id,
                    c.customer_name,
                    c.segment,
                    c.region,
                    o.order_id,
                    o.order_date,
                    o.total_amount,
                    o.status
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
            ),
            customer_metrics AS (
                SELECT
                    customer_id,
                    customer_name,
                    segment,
                    region,
                    COUNT(DISTINCT order_id) AS order_count,
                    SUM(CASE WHEN status = 'completed' THEN total_amount ELSE 0 END) AS completed_revenue,
                    AVG(total_amount) AS avg_order_value,
                    MAX(order_date) AS last_order_date,
                    MIN(order_date) AS first_order_date
                FROM customer_orders
                WHERE order_id IS NOT NULL
                GROUP BY customer_id, customer_name, segment, region
            ),
            customer_rankings AS (
                SELECT
                    *,
                    RANK() OVER (ORDER BY completed_revenue DESC) AS revenue_rank,
                    RANK() OVER (PARTITION BY segment ORDER BY completed_revenue DESC) AS segment_rank,
                    PERCENT_RANK() OVER (ORDER BY completed_revenue) AS revenue_percentile,
                    SUM(completed_revenue) OVER () AS total_revenue,
                    SUM(completed_revenue) OVER (ORDER BY completed_revenue DESC) AS cumulative_revenue
                FROM customer_metrics
            ),
            top_products_per_customer AS (
                SELECT
                    o.customer_id,
                    (SELECT product_name
                     FROM order_items oi2
                     JOIN orders o2 ON oi2.order_id = o2.order_id
                     WHERE o2.customer_id = o.customer_id
                     GROUP BY product_name
                     ORDER BY SUM(oi2.quantity * oi2.unit_price) DESC
                     LIMIT 1) AS top_product
                FROM orders o
                GROUP BY o.customer_id
            )
            SELECT
                cr.customer_id,
                cr.customer_name,
                cr.segment,
                cr.revenue_rank,
                cr.segment_rank,
                cr.completed_revenue,
                ROUND(cr.completed_revenue * 100.0 / cr.total_revenue, 2) AS revenue_share_pct,
                ROUND(cr.cumulative_revenue * 100.0 / cr.total_revenue, 2) AS cumulative_pct,
                tp.top_product
            FROM customer_rankings cr
            LEFT JOIN top_products_per_customer tp ON cr.customer_id = tp.customer_id
            ORDER BY cr.revenue_rank",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [
                3,
                "Gamma LLC",
                "Enterprise",
                1,
                1,
                13700.0,
                45.64,
                45.64,
                "Widget Pro"
            ],
            [
                1,
                "Acme Corp",
                "Enterprise",
                2,
                2,
                12700.0,
                42.31,
                87.94,
                "Widget Pro"
            ],
            [
                5,
                "Epsilon Ltd",
                "SMB",
                3,
                1,
                2100.0,
                6.99,
                94.94,
                "Widget Pro"
            ],
            [
                2,
                "Beta Inc",
                "SMB",
                4,
                2,
                1200.0,
                4.0,
                98.93,
                "Gadget Plus"
            ],
            [
                4,
                "Delta Co",
                "Startup",
                5,
                1,
                320.0,
                1.07,
                100.0,
                "Widget Pro"
            ],
        ]
    );
}

#[test]
#[ignore = "TODO: CTE scoping for subqueries"]
fn test_multi_level_cte_with_correlated_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE products (
                product_id INT64,
                product_name STRING,
                category STRING,
                base_price FLOAT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE sales_history (
                sale_id INT64,
                product_id INT64,
                sale_date DATE,
                quantity INT64,
                sale_price FLOAT64,
                region STRING
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO products VALUES
            (1, 'Laptop Pro', 'Electronics', 1200.0),
            (2, 'Tablet Air', 'Electronics', 600.0),
            (3, 'Office Chair', 'Furniture', 350.0),
            (4, 'Standing Desk', 'Furniture', 800.0),
            (5, 'Wireless Mouse', 'Accessories', 50.0),
            (6, 'Keyboard Pro', 'Accessories', 120.0)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO sales_history VALUES
            (1, 1, DATE '2024-01-05', 5, 1150.0, 'North'),
            (2, 1, DATE '2024-01-12', 3, 1180.0, 'South'),
            (3, 2, DATE '2024-01-08', 10, 580.0, 'East'),
            (4, 2, DATE '2024-01-20', 8, 590.0, 'West'),
            (5, 3, DATE '2024-01-15', 15, 340.0, 'North'),
            (6, 3, DATE '2024-02-01', 12, 345.0, 'South'),
            (7, 4, DATE '2024-01-25', 6, 780.0, 'East'),
            (8, 5, DATE '2024-01-10', 50, 48.0, 'North'),
            (9, 5, DATE '2024-02-05', 45, 49.0, 'West'),
            (10, 6, DATE '2024-01-18', 25, 115.0, 'South'),
            (11, 1, DATE '2024-02-10', 4, 1200.0, 'East'),
            (12, 4, DATE '2024-02-15', 8, 790.0, 'North')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH product_sales AS (
                SELECT
                    p.product_id,
                    p.product_name,
                    p.category,
                    p.base_price,
                    sh.sale_date,
                    sh.quantity,
                    sh.sale_price,
                    sh.region,
                    sh.quantity * sh.sale_price AS line_total
                FROM products p
                JOIN sales_history sh ON p.product_id = sh.product_id
            ),
            category_stats AS (
                SELECT
                    category,
                    SUM(line_total) AS category_revenue,
                    SUM(quantity) AS category_units,
                    COUNT(DISTINCT product_id) AS products_sold,
                    AVG(sale_price) AS avg_sale_price
                FROM product_sales
                GROUP BY category
            ),
            product_with_category_context AS (
                SELECT
                    ps.product_id,
                    ps.product_name,
                    ps.category,
                    SUM(ps.line_total) AS product_revenue,
                    SUM(ps.quantity) AS units_sold,
                    cs.category_revenue,
                    cs.category_units,
                    (SELECT MAX(line_total)
                     FROM product_sales ps2
                     WHERE ps2.product_id = ps.product_id) AS max_single_sale,
                    (SELECT COUNT(DISTINCT region)
                     FROM product_sales ps3
                     WHERE ps3.product_id = ps.product_id) AS regions_sold
                FROM product_sales ps
                JOIN category_stats cs ON ps.category = cs.category
                GROUP BY ps.product_id, ps.product_name, ps.category,
                         cs.category_revenue, cs.category_units
            ),
            final_ranking AS (
                SELECT
                    *,
                    ROUND(product_revenue * 100.0 / category_revenue, 2) AS category_share_pct,
                    RANK() OVER (PARTITION BY category ORDER BY product_revenue DESC) AS category_rank,
                    DENSE_RANK() OVER (ORDER BY product_revenue DESC) AS overall_rank
                FROM product_with_category_context
            )
            SELECT
                product_name,
                category,
                product_revenue,
                units_sold,
                category_share_pct,
                category_rank,
                overall_rank,
                max_single_sale,
                regions_sold
            FROM final_ranking
            WHERE category_rank <= 2
            ORDER BY category, category_rank",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [
                "Wireless Mouse",
                "Accessories",
                4605.0,
                95,
                61.56,
                1,
                5,
                2400.0,
                2
            ],
            [
                "Keyboard Pro",
                "Accessories",
                2875.0,
                25,
                38.44,
                2,
                6,
                2875.0,
                1
            ],
            [
                "Laptop Pro",
                "Electronics",
                14090.0,
                12,
                57.25,
                1,
                1,
                4800.0,
                3
            ],
            [
                "Tablet Air",
                "Electronics",
                10520.0,
                18,
                42.75,
                2,
                3,
                5800.0,
                2
            ],
            [
                "Standing Desk",
                "Furniture",
                11000.0,
                14,
                54.35,
                1,
                2,
                6320.0,
                2
            ],
            [
                "Office Chair",
                "Furniture",
                9240.0,
                27,
                45.65,
                2,
                4,
                5100.0,
                2
            ],
        ]
    );
}

#[test]
#[ignore = "TODO: CTE scoping for subqueries"]
fn test_array_agg_with_order_and_limit_in_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE user_events (
                user_id INT64,
                event_type STRING,
                event_timestamp TIMESTAMP,
                event_value FLOAT64,
                page_url STRING
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO user_events VALUES
            (1, 'page_view', TIMESTAMP '2024-01-01 10:00:00', 0, '/home'),
            (1, 'page_view', TIMESTAMP '2024-01-01 10:05:00', 0, '/products'),
            (1, 'add_to_cart', TIMESTAMP '2024-01-01 10:10:00', 99.99, '/products/widget'),
            (1, 'purchase', TIMESTAMP '2024-01-01 10:15:00', 99.99, '/checkout'),
            (1, 'page_view', TIMESTAMP '2024-01-02 14:00:00', 0, '/home'),
            (2, 'page_view', TIMESTAMP '2024-01-01 11:00:00', 0, '/home'),
            (2, 'page_view', TIMESTAMP '2024-01-01 11:02:00', 0, '/about'),
            (2, 'page_view', TIMESTAMP '2024-01-01 11:05:00', 0, '/pricing'),
            (3, 'page_view', TIMESTAMP '2024-01-02 09:00:00', 0, '/home'),
            (3, 'add_to_cart', TIMESTAMP '2024-01-02 09:10:00', 49.99, '/products/gadget'),
            (3, 'add_to_cart', TIMESTAMP '2024-01-02 09:12:00', 29.99, '/products/tool'),
            (3, 'purchase', TIMESTAMP '2024-01-02 09:20:00', 79.98, '/checkout'),
            (4, 'page_view', TIMESTAMP '2024-01-03 16:00:00', 0, '/home'),
            (4, 'page_view', TIMESTAMP '2024-01-03 16:01:00', 0, '/products')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH user_sessions AS (
                SELECT
                    user_id,
                    DATE(event_timestamp) AS session_date,
                    ARRAY_AGG(event_type ORDER BY event_timestamp) AS event_sequence,
                    ARRAY_AGG(page_url ORDER BY event_timestamp) AS page_sequence,
                    COUNT(*) AS event_count,
                    SUM(event_value) AS session_value
                FROM user_events
                GROUP BY user_id, DATE(event_timestamp)
            ),
            user_journey AS (
                SELECT
                    user_id,
                    COUNT(DISTINCT session_date) AS session_count,
                    SUM(session_value) AS total_value,
                    MAX(session_value) AS max_session_value,
                    (SELECT page_url
                     FROM user_events ue
                     WHERE ue.user_id = us.user_id
                     ORDER BY event_timestamp
                     LIMIT 1) AS first_page,
                    (SELECT page_url
                     FROM user_events ue
                     WHERE ue.user_id = us.user_id
                     ORDER BY event_timestamp DESC
                     LIMIT 1) AS last_page
                FROM user_sessions us
                GROUP BY user_id
            ),
            conversion_analysis AS (
                SELECT
                    uj.user_id,
                    uj.session_count,
                    uj.total_value,
                    uj.max_session_value,
                    uj.first_page,
                    uj.last_page,
                    CASE WHEN total_value > 0 THEN 'Converted' ELSE 'Not Converted' END AS conversion_status,
                    (SELECT COUNT(*)
                     FROM user_events ue
                     WHERE ue.user_id = uj.user_id
                       AND ue.event_type = 'page_view') AS total_page_views,
                    (SELECT COUNT(*)
                     FROM user_events ue
                     WHERE ue.user_id = uj.user_id
                       AND ue.event_type = 'add_to_cart') AS cart_additions
                FROM user_journey uj
            )
            SELECT
                user_id,
                session_count,
                total_value,
                first_page,
                last_page,
                conversion_status,
                total_page_views,
                cart_additions
            FROM conversion_analysis
            ORDER BY total_value DESC, user_id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, 2, 199.98, "/home", "/home", "Converted", 3, 1],
            [3, 1, 159.96, "/home", "/checkout", "Converted", 1, 2],
            [2, 1, 0.0, "/home", "/pricing", "Not Converted", 3, 0],
            [4, 1, 0.0, "/home", "/products", "Not Converted", 2, 0],
        ]
    );
}

#[test]
#[ignore = "TODO: CTE scoping for subqueries"]
fn test_complex_window_with_array_agg_ordered() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE stock_trades (
                trade_id INT64,
                symbol STRING,
                trade_date DATE,
                trade_time TIMESTAMP,
                price FLOAT64,
                volume INT64,
                trade_type STRING
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO stock_trades VALUES
            (1, 'AAPL', DATE '2024-01-02', TIMESTAMP '2024-01-02 09:30:00', 185.50, 1000, 'buy'),
            (2, 'AAPL', DATE '2024-01-02', TIMESTAMP '2024-01-02 10:15:00', 186.20, 500, 'buy'),
            (3, 'AAPL', DATE '2024-01-02', TIMESTAMP '2024-01-02 14:30:00', 185.80, 750, 'sell'),
            (4, 'GOOGL', DATE '2024-01-02', TIMESTAMP '2024-01-02 09:35:00', 140.25, 200, 'buy'),
            (5, 'GOOGL', DATE '2024-01-02', TIMESTAMP '2024-01-02 11:00:00', 141.50, 300, 'buy'),
            (6, 'GOOGL', DATE '2024-01-02', TIMESTAMP '2024-01-02 15:00:00', 142.00, 250, 'sell'),
            (7, 'AAPL', DATE '2024-01-03', TIMESTAMP '2024-01-03 09:30:00', 186.00, 800, 'buy'),
            (8, 'AAPL', DATE '2024-01-03', TIMESTAMP '2024-01-03 13:00:00', 187.50, 600, 'sell'),
            (9, 'MSFT', DATE '2024-01-02', TIMESTAMP '2024-01-02 10:00:00', 375.00, 400, 'buy'),
            (10, 'MSFT', DATE '2024-01-02', TIMESTAMP '2024-01-02 14:00:00', 376.50, 350, 'sell'),
            (11, 'MSFT', DATE '2024-01-03', TIMESTAMP '2024-01-03 09:45:00', 377.00, 500, 'buy')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH trade_stats AS (
                SELECT
                    symbol,
                    trade_date,
                    COUNT(*) AS daily_trades,
                    SUM(volume) AS daily_volume,
                    SUM(price * volume) AS daily_notional,
                    MIN(price) AS daily_low,
                    MAX(price) AS daily_high,
                    ARRAY_AGG(price ORDER BY trade_time) AS price_sequence,
                    ARRAY_AGG(trade_type ORDER BY trade_time) AS trade_type_sequence
                FROM stock_trades
                GROUP BY symbol, trade_date
            ),
            symbol_metrics AS (
                SELECT
                    symbol,
                    trade_date,
                    daily_trades,
                    daily_volume,
                    daily_notional,
                    daily_high - daily_low AS daily_range,
                    price_sequence[OFFSET(0)] AS open_price,
                    price_sequence[OFFSET(ARRAY_LENGTH(price_sequence) - 1)] AS close_price,
                    SUM(daily_volume) OVER (
                        PARTITION BY symbol
                        ORDER BY trade_date
                    ) AS cumulative_volume,
                    AVG(daily_notional) OVER (
                        PARTITION BY symbol
                        ORDER BY trade_date
                        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                    ) AS avg_notional_2day
                FROM trade_stats
            ),
            ranked_symbols AS (
                SELECT
                    *,
                    close_price - open_price AS daily_change,
                    ROUND((close_price - open_price) * 100.0 / open_price, 2) AS daily_return_pct,
                    RANK() OVER (
                        PARTITION BY trade_date
                        ORDER BY daily_volume DESC
                    ) AS volume_rank
                FROM symbol_metrics
            )
            SELECT
                symbol,
                trade_date,
                open_price,
                close_price,
                daily_change,
                daily_return_pct,
                daily_volume,
                volume_rank,
                cumulative_volume
            FROM ranked_symbols
            ORDER BY trade_date, volume_rank",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [
                "AAPL",
                d(2024, 1, 2),
                185.5,
                185.8,
                0.30,
                0.16,
                2250,
                1,
                2250
            ],
            [
                "GOOGL",
                d(2024, 1, 2),
                140.25,
                142.0,
                1.75,
                1.25,
                750,
                2,
                750
            ],
            ["MSFT", d(2024, 1, 2), 375.0, 376.5, 1.5, 0.4, 750, 2, 750],
            [
                "AAPL",
                d(2024, 1, 3),
                186.0,
                187.5,
                1.5,
                0.81,
                1400,
                1,
                3650
            ],
            ["MSFT", d(2024, 1, 3), 377.0, 377.0, 0.0, 0.0, 500, 2, 1250],
        ]
    );
}

#[test]
#[ignore = "TODO: CTE scoping for subqueries"]
fn test_nested_aggregates_with_scalar_subqueries() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE departments (
                dept_id INT64,
                dept_name STRING,
                budget FLOAT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE employees (
                emp_id INT64,
                emp_name STRING,
                dept_id INT64,
                salary FLOAT64,
                hire_date DATE,
                manager_id INT64
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE projects (
                project_id INT64,
                project_name STRING,
                dept_id INT64,
                budget FLOAT64,
                start_date DATE,
                status STRING
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE project_assignments (
                assignment_id INT64,
                project_id INT64,
                emp_id INT64,
                role STRING,
                hours_allocated INT64
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO departments VALUES
            (1, 'Engineering', 500000.0),
            (2, 'Marketing', 300000.0),
            (3, 'Sales', 400000.0),
            (4, 'HR', 150000.0)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO employees VALUES
            (101, 'Alice', 1, 120000.0, DATE '2020-01-15', NULL),
            (102, 'Bob', 1, 95000.0, DATE '2021-03-01', 101),
            (103, 'Charlie', 1, 85000.0, DATE '2022-06-15', 101),
            (104, 'Diana', 2, 110000.0, DATE '2019-09-01', NULL),
            (105, 'Eve', 2, 75000.0, DATE '2023-01-10', 104),
            (106, 'Frank', 3, 130000.0, DATE '2018-05-20', NULL),
            (107, 'Grace', 3, 90000.0, DATE '2021-11-01', 106),
            (108, 'Henry', 4, 80000.0, DATE '2022-02-15', NULL)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO projects VALUES
            (1001, 'Platform Rebuild', 1, 200000.0, DATE '2024-01-01', 'active'),
            (1002, 'Mobile App', 1, 150000.0, DATE '2024-02-01', 'active'),
            (1003, 'Brand Campaign', 2, 100000.0, DATE '2024-01-15', 'active'),
            (1004, 'Sales Automation', 3, 80000.0, DATE '2023-11-01', 'completed'),
            (1005, 'HR Portal', 4, 50000.0, DATE '2024-03-01', 'planning')",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO project_assignments VALUES
            (1, 1001, 101, 'Lead', 160),
            (2, 1001, 102, 'Developer', 200),
            (3, 1001, 103, 'Developer', 180),
            (4, 1002, 102, 'Developer', 100),
            (5, 1002, 103, 'Lead', 150),
            (6, 1003, 104, 'Lead', 120),
            (7, 1003, 105, 'Coordinator', 160),
            (8, 1004, 106, 'Lead', 80),
            (9, 1004, 107, 'Sales Rep', 120),
            (10, 1005, 108, 'Lead', 40)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH dept_employee_stats AS (
                SELECT
                    d.dept_id,
                    d.dept_name,
                    d.budget AS dept_budget,
                    COUNT(e.emp_id) AS emp_count,
                    SUM(e.salary) AS total_salary,
                    AVG(e.salary) AS avg_salary,
                    MAX(e.salary) AS max_salary,
                    MIN(e.hire_date) AS earliest_hire
                FROM departments d
                LEFT JOIN employees e ON d.dept_id = e.dept_id
                GROUP BY d.dept_id, d.dept_name, d.budget
            ),
            dept_project_stats AS (
                SELECT
                    d.dept_id,
                    COUNT(p.project_id) AS project_count,
                    SUM(p.budget) AS project_budget_total,
                    SUM(CASE WHEN p.status = 'active' THEN 1 ELSE 0 END) AS active_projects
                FROM departments d
                LEFT JOIN projects p ON d.dept_id = p.dept_id
                GROUP BY d.dept_id
            ),
            employee_project_hours AS (
                SELECT
                    e.dept_id,
                    e.emp_id,
                    e.emp_name,
                    SUM(pa.hours_allocated) AS total_hours,
                    COUNT(DISTINCT pa.project_id) AS project_count
                FROM employees e
                LEFT JOIN project_assignments pa ON e.emp_id = pa.emp_id
                GROUP BY e.dept_id, e.emp_id, e.emp_name
            ),
            dept_workload AS (
                SELECT
                    dept_id,
                    SUM(total_hours) AS dept_total_hours,
                    AVG(total_hours) AS avg_hours_per_emp,
                    (SELECT emp_name
                     FROM employee_project_hours eph2
                     WHERE eph2.dept_id = eph.dept_id
                     ORDER BY total_hours DESC
                     LIMIT 1) AS busiest_employee,
                    (SELECT MAX(total_hours)
                     FROM employee_project_hours eph3
                     WHERE eph3.dept_id = eph.dept_id) AS max_employee_hours
                FROM employee_project_hours eph
                GROUP BY dept_id
            ),
            final_dept_analysis AS (
                SELECT
                    des.dept_id,
                    des.dept_name,
                    des.dept_budget,
                    des.emp_count,
                    des.total_salary,
                    des.avg_salary,
                    dps.project_count,
                    dps.project_budget_total,
                    dps.active_projects,
                    dw.dept_total_hours,
                    dw.busiest_employee,
                    ROUND(des.total_salary * 100.0 / des.dept_budget, 2) AS salary_to_budget_pct,
                    ROUND(dps.project_budget_total * 100.0 / des.dept_budget, 2) AS project_to_budget_pct,
                    RANK() OVER (ORDER BY des.total_salary DESC) AS salary_rank,
                    RANK() OVER (ORDER BY dps.project_budget_total DESC) AS project_budget_rank
                FROM dept_employee_stats des
                JOIN dept_project_stats dps ON des.dept_id = dps.dept_id
                LEFT JOIN dept_workload dw ON des.dept_id = dw.dept_id
            )
            SELECT
                dept_name,
                emp_count,
                total_salary,
                project_count,
                active_projects,
                project_budget_total,
                salary_to_budget_pct,
                busiest_employee,
                salary_rank
            FROM final_dept_analysis
            ORDER BY salary_rank",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [
                "Engineering",
                3,
                300000.0,
                2,
                2,
                350000.0,
                60.0,
                "Charlie",
                1
            ],
            ["Sales", 2, 220000.0, 1, 0, 80000.0, 55.0, "Grace", 2],
            ["Marketing", 2, 185000.0, 1, 1, 100000.0, 61.67, "Eve", 3],
            ["HR", 1, 80000.0, 1, 0, 50000.0, 53.33, "Henry", 4],
        ]
    );
}

#[test]
#[ignore = "TODO: CTE scoping for subqueries"]
fn test_recursive_like_hierarchy_with_arrays() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE org_hierarchy (
                emp_id INT64,
                emp_name STRING,
                manager_id INT64,
                title STRING,
                level INT64
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO org_hierarchy VALUES
            (1, 'CEO', NULL, 'Chief Executive Officer', 1),
            (2, 'CTO', 1, 'Chief Technology Officer', 2),
            (3, 'CFO', 1, 'Chief Financial Officer', 2),
            (4, 'VP Eng', 2, 'VP of Engineering', 3),
            (5, 'VP Product', 2, 'VP of Product', 3),
            (6, 'Dir Backend', 4, 'Director Backend', 4),
            (7, 'Dir Frontend', 4, 'Director Frontend', 4),
            (8, 'Sr Eng 1', 6, 'Senior Engineer', 5),
            (9, 'Sr Eng 2', 6, 'Senior Engineer', 5),
            (10, 'Jr Eng 1', 8, 'Junior Engineer', 6),
            (11, 'Controller', 3, 'Controller', 3),
            (12, 'Sr Accountant', 11, 'Senior Accountant', 4)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH level_1 AS (
                SELECT emp_id, emp_name, manager_id, title, level,
                       [emp_name] AS path
                FROM org_hierarchy WHERE manager_id IS NULL
            ),
            level_2 AS (
                SELECT o.emp_id, o.emp_name, o.manager_id, o.title, o.level,
                       ARRAY_CONCAT(l1.path, [o.emp_name]) AS path
                FROM org_hierarchy o
                JOIN level_1 l1 ON o.manager_id = l1.emp_id
            ),
            level_3 AS (
                SELECT o.emp_id, o.emp_name, o.manager_id, o.title, o.level,
                       ARRAY_CONCAT(l2.path, [o.emp_name]) AS path
                FROM org_hierarchy o
                JOIN level_2 l2 ON o.manager_id = l2.emp_id
            ),
            level_4 AS (
                SELECT o.emp_id, o.emp_name, o.manager_id, o.title, o.level,
                       ARRAY_CONCAT(l3.path, [o.emp_name]) AS path
                FROM org_hierarchy o
                JOIN level_3 l3 ON o.manager_id = l3.emp_id
            ),
            level_5 AS (
                SELECT o.emp_id, o.emp_name, o.manager_id, o.title, o.level,
                       ARRAY_CONCAT(l4.path, [o.emp_name]) AS path
                FROM org_hierarchy o
                JOIN level_4 l4 ON o.manager_id = l4.emp_id
            ),
            level_6 AS (
                SELECT o.emp_id, o.emp_name, o.manager_id, o.title, o.level,
                       ARRAY_CONCAT(l5.path, [o.emp_name]) AS path
                FROM org_hierarchy o
                JOIN level_5 l5 ON o.manager_id = l5.emp_id
            ),
            all_levels AS (
                SELECT * FROM level_1
                UNION ALL SELECT * FROM level_2
                UNION ALL SELECT * FROM level_3
                UNION ALL SELECT * FROM level_4
                UNION ALL SELECT * FROM level_5
                UNION ALL SELECT * FROM level_6
            ),
            with_subordinates AS (
                SELECT
                    al.emp_id,
                    al.emp_name,
                    al.title,
                    al.level,
                    al.path,
                    (SELECT COUNT(*)
                     FROM org_hierarchy o
                     WHERE o.manager_id = al.emp_id) AS direct_reports,
                    (SELECT m.emp_name
                     FROM org_hierarchy m
                     WHERE m.emp_id = al.manager_id) AS manager_name
                FROM all_levels al
            )
            SELECT
                emp_name,
                title,
                level,
                manager_name,
                direct_reports,
                ARRAY_LENGTH(path) AS path_length,
                path
            FROM with_subordinates
            ORDER BY level, emp_name",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["CEO", "Chief Executive Officer", 1, null, 2, 1, ["CEO"]],
            [
                "CFO",
                "Chief Financial Officer",
                2,
                "CEO",
                1,
                2,
                ["CEO", "CFO"]
            ],
            [
                "CTO",
                "Chief Technology Officer",
                2,
                "CEO",
                2,
                2,
                ["CEO", "CTO"]
            ],
            [
                "Controller",
                "Controller",
                3,
                "CFO",
                1,
                3,
                ["CEO", "CFO", "Controller"]
            ],
            [
                "VP Eng",
                "VP of Engineering",
                3,
                "CTO",
                2,
                3,
                ["CEO", "CTO", "VP Eng"]
            ],
            [
                "VP Product",
                "VP of Product",
                3,
                "CTO",
                0,
                3,
                ["CEO", "CTO", "VP Product"]
            ],
            [
                "Dir Backend",
                "Director Backend",
                4,
                "VP Eng",
                2,
                4,
                ["CEO", "CTO", "VP Eng", "Dir Backend"]
            ],
            [
                "Dir Frontend",
                "Director Frontend",
                4,
                "VP Eng",
                0,
                4,
                ["CEO", "CTO", "VP Eng", "Dir Frontend"]
            ],
            [
                "Sr Accountant",
                "Senior Accountant",
                4,
                "Controller",
                0,
                4,
                ["CEO", "CFO", "Controller", "Sr Accountant"]
            ],
            [
                "Sr Eng 1",
                "Senior Engineer",
                5,
                "Dir Backend",
                1,
                5,
                ["CEO", "CTO", "VP Eng", "Dir Backend", "Sr Eng 1"]
            ],
            [
                "Sr Eng 2",
                "Senior Engineer",
                5,
                "Dir Backend",
                0,
                5,
                ["CEO", "CTO", "VP Eng", "Dir Backend", "Sr Eng 2"]
            ],
            [
                "Jr Eng 1",
                "Junior Engineer",
                6,
                "Sr Eng 1",
                0,
                6,
                [
                    "CEO",
                    "CTO",
                    "VP Eng",
                    "Dir Backend",
                    "Sr Eng 1",
                    "Jr Eng 1"
                ]
            ],
        ]
    );
}

#[test]
#[ignore = "TODO: CTE scoping for subqueries"]
fn test_time_series_with_gaps_and_array_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE sensor_readings (
                sensor_id INT64,
                reading_time TIMESTAMP,
                temperature FLOAT64,
                humidity FLOAT64,
                status STRING
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO sensor_readings VALUES
            (1, TIMESTAMP '2024-01-01 00:00:00', 22.5, 45.0, 'normal'),
            (1, TIMESTAMP '2024-01-01 01:00:00', 22.8, 44.5, 'normal'),
            (1, TIMESTAMP '2024-01-01 02:00:00', 23.1, 44.0, 'normal'),
            (1, TIMESTAMP '2024-01-01 04:00:00', 24.5, 42.0, 'warning'),
            (1, TIMESTAMP '2024-01-01 05:00:00', 25.0, 41.0, 'warning'),
            (1, TIMESTAMP '2024-01-01 06:00:00', 23.5, 43.0, 'normal'),
            (2, TIMESTAMP '2024-01-01 00:00:00', 20.0, 50.0, 'normal'),
            (2, TIMESTAMP '2024-01-01 01:00:00', 19.5, 51.0, 'normal'),
            (2, TIMESTAMP '2024-01-01 03:00:00', 18.0, 55.0, 'cold'),
            (2, TIMESTAMP '2024-01-01 04:00:00', 17.5, 56.0, 'cold'),
            (2, TIMESTAMP '2024-01-01 05:00:00', 19.0, 52.0, 'normal')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH hourly_stats AS (
                SELECT
                    sensor_id,
                    reading_time,
                    temperature,
                    humidity,
                    status,
                    LAG(reading_time) OVER (
                        PARTITION BY sensor_id ORDER BY reading_time
                    ) AS prev_reading_time,
                    LAG(temperature) OVER (
                        PARTITION BY sensor_id ORDER BY reading_time
                    ) AS prev_temperature,
                    LEAD(temperature) OVER (
                        PARTITION BY sensor_id ORDER BY reading_time
                    ) AS next_temperature
                FROM sensor_readings
            ),
            gap_analysis AS (
                SELECT
                    sensor_id,
                    reading_time,
                    temperature,
                    humidity,
                    status,
                    TIMESTAMP_DIFF(reading_time, prev_reading_time, HOUR) AS hours_since_last,
                    temperature - prev_temperature AS temp_change,
                    CASE
                        WHEN TIMESTAMP_DIFF(reading_time, prev_reading_time, HOUR) > 1
                        THEN 'gap_detected'
                        ELSE 'continuous'
                    END AS continuity_status
                FROM hourly_stats
            ),
            sensor_summary AS (
                SELECT
                    sensor_id,
                    COUNT(*) AS reading_count,
                    MIN(temperature) AS min_temp,
                    MAX(temperature) AS max_temp,
                    AVG(temperature) AS avg_temp,
                    ARRAY_AGG(status ORDER BY reading_time) AS status_sequence,
                    ARRAY_AGG(temperature ORDER BY reading_time) AS temp_sequence,
                    SUM(CASE WHEN continuity_status = 'gap_detected' THEN 1 ELSE 0 END) AS gap_count,
                    MAX(hours_since_last) AS max_gap_hours
                FROM gap_analysis
                GROUP BY sensor_id
            ),
            status_transitions AS (
                SELECT
                    sensor_id,
                    COUNT(DISTINCT status) AS unique_statuses,
                    (SELECT status
                     FROM sensor_readings sr
                     WHERE sr.sensor_id = ss.sensor_id
                     ORDER BY reading_time DESC
                     LIMIT 1) AS latest_status,
                    (SELECT temperature
                     FROM sensor_readings sr
                     WHERE sr.sensor_id = ss.sensor_id
                     ORDER BY reading_time DESC
                     LIMIT 1) AS latest_temp
                FROM sensor_summary ss
            )
            SELECT
                ss.sensor_id,
                ss.reading_count,
                ROUND(ss.avg_temp, 2) AS avg_temp,
                ss.min_temp,
                ss.max_temp,
                ss.gap_count,
                st.unique_statuses,
                st.latest_status,
                st.latest_temp,
                ss.status_sequence,
                ss.temp_sequence
            FROM sensor_summary ss
            JOIN status_transitions st ON ss.sensor_id = st.sensor_id
            ORDER BY ss.sensor_id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [
                1,
                6,
                23.57,
                22.5,
                25.0,
                1,
                2,
                "normal",
                23.5,
                ["normal", "normal", "normal", "warning", "warning", "normal"],
                [22.5, 22.8, 23.1, 24.5, 25.0, 23.5]
            ],
            [
                2,
                5,
                18.8,
                17.5,
                20.0,
                1,
                2,
                "normal",
                19.0,
                ["normal", "normal", "cold", "cold", "normal"],
                [20.0, 19.5, 18.0, 17.5, 19.0]
            ],
        ]
    );
}

#[test]
#[ignore = "TODO: CTE scoping for subqueries"]
fn test_complex_pivot_simulation_with_ctes() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE quarterly_sales (
                year INT64,
                quarter INT64,
                region STRING,
                product_line STRING,
                revenue FLOAT64,
                units_sold INT64
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO quarterly_sales VALUES
            (2023, 1, 'North', 'Electronics', 150000.0, 500),
            (2023, 2, 'North', 'Electronics', 175000.0, 600),
            (2023, 3, 'North', 'Electronics', 200000.0, 700),
            (2023, 4, 'North', 'Electronics', 250000.0, 850),
            (2023, 1, 'South', 'Electronics', 120000.0, 400),
            (2023, 2, 'South', 'Electronics', 130000.0, 450),
            (2023, 3, 'South', 'Electronics', 140000.0, 480),
            (2023, 4, 'South', 'Electronics', 180000.0, 600),
            (2023, 1, 'North', 'Furniture', 80000.0, 200),
            (2023, 2, 'North', 'Furniture', 85000.0, 220),
            (2023, 3, 'North', 'Furniture', 90000.0, 230),
            (2023, 4, 'North', 'Furniture', 110000.0, 280),
            (2024, 1, 'North', 'Electronics', 180000.0, 620),
            (2024, 1, 'South', 'Electronics', 145000.0, 490),
            (2024, 1, 'North', 'Furniture', 95000.0, 245)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH quarterly_pivot AS (
                SELECT
                    year,
                    region,
                    product_line,
                    SUM(CASE WHEN quarter = 1 THEN revenue ELSE 0 END) AS q1_revenue,
                    SUM(CASE WHEN quarter = 2 THEN revenue ELSE 0 END) AS q2_revenue,
                    SUM(CASE WHEN quarter = 3 THEN revenue ELSE 0 END) AS q3_revenue,
                    SUM(CASE WHEN quarter = 4 THEN revenue ELSE 0 END) AS q4_revenue,
                    SUM(CASE WHEN quarter = 1 THEN units_sold ELSE 0 END) AS q1_units,
                    SUM(CASE WHEN quarter = 2 THEN units_sold ELSE 0 END) AS q2_units,
                    SUM(CASE WHEN quarter = 3 THEN units_sold ELSE 0 END) AS q3_units,
                    SUM(CASE WHEN quarter = 4 THEN units_sold ELSE 0 END) AS q4_units
                FROM quarterly_sales
                GROUP BY year, region, product_line
            ),
            with_totals AS (
                SELECT
                    *,
                    q1_revenue + q2_revenue + q3_revenue + q4_revenue AS annual_revenue,
                    q1_units + q2_units + q3_units + q4_units AS annual_units
                FROM quarterly_pivot
            ),
            with_growth AS (
                SELECT
                    wt.*,
                    ROUND((q2_revenue - q1_revenue) * 100.0 / NULLIF(q1_revenue, 0), 2) AS q1_to_q2_growth,
                    ROUND((q3_revenue - q2_revenue) * 100.0 / NULLIF(q2_revenue, 0), 2) AS q2_to_q3_growth,
                    ROUND((q4_revenue - q3_revenue) * 100.0 / NULLIF(q3_revenue, 0), 2) AS q3_to_q4_growth,
                    (SELECT SUM(annual_revenue)
                     FROM with_totals wt2
                     WHERE wt2.year = wt.year) AS year_total
                FROM with_totals wt
            ),
            ranked AS (
                SELECT
                    *,
                    ROUND(annual_revenue * 100.0 / year_total, 2) AS share_of_year,
                    RANK() OVER (PARTITION BY year ORDER BY annual_revenue DESC) AS revenue_rank,
                    ARRAY_AGG(STRUCT(region, product_line, annual_revenue)) OVER (
                        PARTITION BY year
                    ) AS year_breakdown
                FROM with_growth
            )
            SELECT
                year,
                region,
                product_line,
                q1_revenue,
                q2_revenue,
                q3_revenue,
                q4_revenue,
                annual_revenue,
                share_of_year,
                q1_to_q2_growth,
                q2_to_q3_growth,
                revenue_rank
            FROM ranked
            ORDER BY year, revenue_rank",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [
                2023,
                "North",
                "Electronics",
                150000.0,
                175000.0,
                200000.0,
                250000.0,
                775000.0,
                45.32,
                16.67,
                14.29,
                1
            ],
            [
                2023,
                "South",
                "Electronics",
                120000.0,
                130000.0,
                140000.0,
                180000.0,
                570000.0,
                33.33,
                8.33,
                7.69,
                2
            ],
            [
                2023,
                "North",
                "Furniture",
                80000.0,
                85000.0,
                90000.0,
                110000.0,
                365000.0,
                21.35,
                6.25,
                5.88,
                3
            ],
            [
                2024,
                "North",
                "Electronics",
                180000.0,
                0.0,
                0.0,
                0.0,
                180000.0,
                42.86,
                null,
                null,
                1
            ],
            [
                2024,
                "South",
                "Electronics",
                145000.0,
                0.0,
                0.0,
                0.0,
                145000.0,
                34.52,
                null,
                null,
                2
            ],
            [
                2024,
                "North",
                "Furniture",
                95000.0,
                0.0,
                0.0,
                0.0,
                95000.0,
                22.62,
                null,
                null,
                3
            ],
        ]
    );
}

#[test]
#[ignore = "TODO: UNNEST column resolution"]
fn test_unnest_with_complex_cte_and_array_operations() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE user_tags (
                user_id INT64,
                username STRING,
                tags ARRAY<STRING>,
                scores ARRAY<INT64>,
                preferences ARRAY<STRUCT<key STRING, value STRING>>
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO user_tags VALUES
            (1, 'alice', ['tech', 'music', 'sports'], [85, 90, 75], [STRUCT('theme', 'dark'), STRUCT('lang', 'en')]),
            (2, 'bob', ['tech', 'gaming'], [95, 88], [STRUCT('theme', 'light'), STRUCT('lang', 'es')]),
            (3, 'charlie', ['music', 'art', 'travel'], [70, 85, 92], [STRUCT('theme', 'dark'), STRUCT('lang', 'fr')]),
            (4, 'diana', ['tech', 'sports', 'fitness'], [80, 78, 95], [STRUCT('theme', 'auto'), STRUCT('lang', 'en')])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH unnested_tags AS (
                SELECT
                    user_id,
                    username,
                    tag,
                    tag_offset
                FROM user_tags,
                UNNEST(tags) AS tag WITH OFFSET AS tag_offset
            ),
            unnested_scores AS (
                SELECT
                    user_id,
                    score,
                    score_offset
                FROM user_tags,
                UNNEST(scores) AS score WITH OFFSET AS score_offset
            ),
            tag_score_combined AS (
                SELECT
                    ut.user_id,
                    ut.username,
                    ut.tag,
                    us.score
                FROM unnested_tags ut
                JOIN unnested_scores us
                    ON ut.user_id = us.user_id
                    AND ut.tag_offset = us.score_offset
            ),
            user_tag_stats AS (
                SELECT
                    user_id,
                    username,
                    COUNT(*) AS tag_count,
                    AVG(score) AS avg_score,
                    MAX(score) AS max_score,
                    ARRAY_AGG(tag ORDER BY score DESC LIMIT 1)[OFFSET(0)] AS top_tag,
                    ARRAY_AGG(STRUCT(tag, score) ORDER BY score DESC) AS tag_scores
                FROM tag_score_combined
                GROUP BY user_id, username
            ),
            tag_popularity AS (
                SELECT
                    tag,
                    COUNT(DISTINCT user_id) AS user_count,
                    AVG(score) AS avg_tag_score
                FROM tag_score_combined
                GROUP BY tag
            ),
            users_with_popular_tags AS (
                SELECT
                    uts.*,
                    (SELECT COUNT(*)
                     FROM tag_score_combined tsc
                     JOIN tag_popularity tp ON tsc.tag = tp.tag
                     WHERE tsc.user_id = uts.user_id
                       AND tp.user_count >= 2) AS popular_tag_count
                FROM user_tag_stats uts
            )
            SELECT
                username,
                tag_count,
                ROUND(avg_score, 2) AS avg_score,
                top_tag,
                popular_tag_count
            FROM users_with_popular_tags
            ORDER BY avg_score DESC",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["bob", 2, 91.5, "tech", 1],
            ["diana", 3, 84.33, "fitness", 2],
            ["alice", 3, 83.33, "music", 3],
            ["charlie", 3, 82.33, "travel", 1],
        ]
    );
}

#[test]
#[ignore = "TODO: UNNEST column resolution"]
fn test_unnest_cross_join_with_nested_structs() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE orders_nested (
                order_id INT64,
                customer_name STRING,
                order_date DATE,
                items ARRAY<STRUCT<
                    product_name STRING,
                    quantity INT64,
                    unit_price FLOAT64,
                    attributes ARRAY<STRUCT<name STRING, value STRING>>
                >>
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO orders_nested VALUES
            (1, 'Alice', DATE '2024-01-15', [
                STRUCT('Laptop', 1, 1200.0, [STRUCT('color', 'silver'), STRUCT('size', '15inch')]),
                STRUCT('Mouse', 2, 25.0, [STRUCT('color', 'black'), STRUCT('wireless', 'yes')])
            ]),
            (2, 'Bob', DATE '2024-01-16', [
                STRUCT('Keyboard', 1, 150.0, [STRUCT('layout', 'US'), STRUCT('backlit', 'yes')]),
                STRUCT('Monitor', 2, 400.0, [STRUCT('size', '27inch'), STRUCT('resolution', '4K')])
            ]),
            (3, 'Charlie', DATE '2024-01-17', [
                STRUCT('Tablet', 1, 600.0, [STRUCT('storage', '256GB'), STRUCT('color', 'gray')])
            ])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH flattened_items AS (
                SELECT
                    o.order_id,
                    o.customer_name,
                    o.order_date,
                    item.product_name,
                    item.quantity,
                    item.unit_price,
                    item.quantity * item.unit_price AS line_total,
                    item.attributes
                FROM orders_nested o,
                UNNEST(o.items) AS item
            ),
            flattened_attributes AS (
                SELECT
                    fi.order_id,
                    fi.customer_name,
                    fi.product_name,
                    fi.line_total,
                    attr.name AS attr_name,
                    attr.value AS attr_value
                FROM flattened_items fi,
                UNNEST(fi.attributes) AS attr
            ),
            product_attribute_summary AS (
                SELECT
                    product_name,
                    ARRAY_AGG(DISTINCT attr_name) AS attribute_names,
                    COUNT(DISTINCT attr_name) AS attribute_count
                FROM flattened_attributes
                GROUP BY product_name
            ),
            order_summary AS (
                SELECT
                    order_id,
                    customer_name,
                    order_date,
                    COUNT(*) AS item_count,
                    SUM(line_total) AS order_total,
                    ARRAY_AGG(product_name ORDER BY line_total DESC) AS products_by_value,
                    ARRAY_AGG(STRUCT(product_name, line_total)) AS item_details
                FROM flattened_items
                GROUP BY order_id, customer_name, order_date
            ),
            color_analysis AS (
                SELECT
                    product_name,
                    attr_value AS color
                FROM flattened_attributes
                WHERE attr_name = 'color'
            )
            SELECT
                os.order_id,
                os.customer_name,
                os.item_count,
                os.order_total,
                (SELECT STRING_AGG(DISTINCT ca.color, ', ')
                 FROM color_analysis ca
                 JOIN flattened_items fi ON ca.product_name = fi.product_name
                 WHERE fi.order_id = os.order_id) AS colors_in_order
            FROM order_summary os
            ORDER BY os.order_total DESC",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Alice", 2, 1250.0, "black, silver"],
            [2, "Bob", 2, 950.0, null],
            [3, "Charlie", 1, 600.0, "gray"],
        ]
    );
}

#[test]
#[ignore = "TODO: UNNEST column resolution"]
fn test_unnest_with_window_functions_and_array_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE product_reviews (
                product_id INT64,
                product_name STRING,
                reviews ARRAY<STRUCT<
                    reviewer STRING,
                    rating INT64,
                    review_date DATE,
                    helpful_votes INT64
                >>
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO product_reviews VALUES
            (101, 'Wireless Headphones', [
                STRUCT('user1', 5, DATE '2024-01-10', 12),
                STRUCT('user2', 4, DATE '2024-01-12', 8),
                STRUCT('user3', 5, DATE '2024-01-15', 15),
                STRUCT('user4', 3, DATE '2024-01-20', 5)
            ]),
            (102, 'Bluetooth Speaker', [
                STRUCT('user5', 4, DATE '2024-01-11', 10),
                STRUCT('user6', 5, DATE '2024-01-14', 20),
                STRUCT('user1', 4, DATE '2024-01-18', 7)
            ]),
            (103, 'Smart Watch', [
                STRUCT('user2', 5, DATE '2024-01-13', 18),
                STRUCT('user7', 4, DATE '2024-01-16', 9),
                STRUCT('user8', 5, DATE '2024-01-19', 14),
                STRUCT('user9', 5, DATE '2024-01-22', 11),
                STRUCT('user10', 4, DATE '2024-01-25', 6)
            ])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH unnested_reviews AS (
                SELECT
                    pr.product_id,
                    pr.product_name,
                    review.reviewer,
                    review.rating,
                    review.review_date,
                    review.helpful_votes
                FROM product_reviews pr,
                UNNEST(pr.reviews) AS review
            ),
            review_with_rankings AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY product_id
                        ORDER BY helpful_votes DESC
                    ) AS helpfulness_rank,
                    RANK() OVER (
                        PARTITION BY product_id
                        ORDER BY rating DESC, helpful_votes DESC
                    ) AS quality_rank,
                    AVG(rating) OVER (PARTITION BY product_id) AS product_avg_rating,
                    SUM(helpful_votes) OVER (PARTITION BY product_id) AS product_total_votes,
                    COUNT(*) OVER (PARTITION BY product_id) AS product_review_count
                FROM unnested_reviews
            ),
            top_reviews_per_product AS (
                SELECT
                    product_id,
                    product_name,
                    ARRAY_AGG(
                        STRUCT(reviewer, rating, helpful_votes)
                        ORDER BY helpful_votes DESC
                        LIMIT 2
                    ) AS top_helpful_reviews,
                    (SELECT reviewer
                     FROM review_with_rankings rwr2
                     WHERE rwr2.product_id = rwr.product_id
                     ORDER BY helpful_votes DESC
                     LIMIT 1) AS most_helpful_reviewer
                FROM review_with_rankings rwr
                GROUP BY product_id, product_name
            ),
            product_summary AS (
                SELECT
                    rwr.product_id,
                    rwr.product_name,
                    MAX(rwr.product_avg_rating) AS avg_rating,
                    MAX(rwr.product_review_count) AS review_count,
                    MAX(rwr.product_total_votes) AS total_votes,
                    SUM(CASE WHEN rwr.rating >= 4 THEN 1 ELSE 0 END) AS positive_reviews,
                    trp.top_helpful_reviews,
                    trp.most_helpful_reviewer
                FROM review_with_rankings rwr
                JOIN top_reviews_per_product trp ON rwr.product_id = trp.product_id
                GROUP BY rwr.product_id, rwr.product_name, trp.top_helpful_reviews, trp.most_helpful_reviewer
            )
            SELECT
                product_name,
                ROUND(avg_rating, 2) AS avg_rating,
                review_count,
                positive_reviews,
                total_votes,
                most_helpful_reviewer
            FROM product_summary
            ORDER BY avg_rating DESC, total_votes DESC",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Smart Watch", 4.6, 5, 5, 58, "user2"],
            ["Bluetooth Speaker", 4.33, 3, 3, 37, "user6"],
            ["Wireless Headphones", 4.25, 4, 3, 40, "user3"],
        ]
    );
}

#[test]
#[ignore = "TODO: UNNEST column resolution"]
fn test_unnest_multi_array_correlation() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE student_grades (
                student_id INT64,
                student_name STRING,
                subjects ARRAY<STRING>,
                grades ARRAY<FLOAT64>,
                semesters ARRAY<STRING>
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO student_grades VALUES
            (1, 'Alice', ['Math', 'Physics', 'Chemistry', 'Biology'], [92.5, 88.0, 85.5, 90.0], ['Fall', 'Fall', 'Spring', 'Spring']),
            (2, 'Bob', ['Math', 'Physics', 'English'], [78.0, 82.5, 88.0], ['Fall', 'Spring', 'Spring']),
            (3, 'Charlie', ['Chemistry', 'Biology', 'English', 'History'], [91.0, 89.5, 84.0, 87.5], ['Fall', 'Fall', 'Fall', 'Spring']),
            (4, 'Diana', ['Math', 'English', 'History'], [95.0, 92.0, 88.5], ['Fall', 'Spring', 'Spring'])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH grade_details AS (
                SELECT
                    sg.student_id,
                    sg.student_name,
                    subject,
                    grade,
                    semester,
                    subject_offset
                FROM student_grades sg,
                UNNEST(sg.subjects) AS subject WITH OFFSET AS subject_offset,
                UNNEST(sg.grades) AS grade WITH OFFSET AS grade_offset,
                UNNEST(sg.semesters) AS semester WITH OFFSET AS semester_offset
                WHERE subject_offset = grade_offset AND grade_offset = semester_offset
            ),
            student_stats AS (
                SELECT
                    student_id,
                    student_name,
                    COUNT(*) AS course_count,
                    AVG(grade) AS overall_gpa,
                    MAX(grade) AS highest_grade,
                    MIN(grade) AS lowest_grade,
                    ARRAY_AGG(subject ORDER BY grade DESC LIMIT 1)[OFFSET(0)] AS best_subject,
                    ARRAY_AGG(STRUCT(subject, grade, semester) ORDER BY grade DESC) AS all_grades
                FROM grade_details
                GROUP BY student_id, student_name
            ),
            semester_breakdown AS (
                SELECT
                    student_id,
                    semester,
                    AVG(grade) AS semester_gpa,
                    COUNT(*) AS courses_taken
                FROM grade_details
                GROUP BY student_id, semester
            ),
            fall_vs_spring AS (
                SELECT
                    student_id,
                    MAX(CASE WHEN semester = 'Fall' THEN semester_gpa END) AS fall_gpa,
                    MAX(CASE WHEN semester = 'Spring' THEN semester_gpa END) AS spring_gpa
                FROM semester_breakdown
                GROUP BY student_id
            ),
            subject_rankings AS (
                SELECT
                    subject,
                    AVG(grade) AS subject_avg,
                    COUNT(DISTINCT student_id) AS student_count
                FROM grade_details
                GROUP BY subject
            ),
            final_analysis AS (
                SELECT
                    ss.student_id,
                    ss.student_name,
                    ss.course_count,
                    ROUND(ss.overall_gpa, 2) AS overall_gpa,
                    ss.best_subject,
                    ROUND(fvs.fall_gpa, 2) AS fall_gpa,
                    ROUND(fvs.spring_gpa, 2) AS spring_gpa,
                    ROUND(COALESCE(fvs.spring_gpa, 0) - COALESCE(fvs.fall_gpa, 0), 2) AS semester_improvement,
                    ss.all_grades,
                    RANK() OVER (ORDER BY ss.overall_gpa DESC) AS class_rank
                FROM student_stats ss
                LEFT JOIN fall_vs_spring fvs ON ss.student_id = fvs.student_id
            )
            SELECT
                student_name,
                course_count,
                overall_gpa,
                best_subject,
                fall_gpa,
                spring_gpa,
                semester_improvement,
                class_rank
            FROM final_analysis
            ORDER BY class_rank",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Diana", 3, 91.83, "Math", 95.0, 90.25, -4.75, 1],
            ["Alice", 4, 89.0, "Math", 90.25, 87.75, -2.5, 2],
            ["Charlie", 4, 88.0, "Chemistry", 88.17, 87.5, -0.67, 3],
            ["Bob", 3, 82.83, "English", 78.0, 85.25, 7.25, 4],
        ]
    );
}

#[test]
#[ignore = "TODO: UNNEST column resolution"]
fn test_unnest_with_lateral_join_simulation() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE sales_teams (
                team_id INT64,
                team_name STRING,
                region STRING,
                members ARRAY<STRUCT<name STRING, role STRING, quota INT64>>,
                monthly_targets ARRAY<INT64>
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO sales_teams VALUES
            (1, 'Alpha Team', 'North',
             [STRUCT('John', 'Lead', 500000), STRUCT('Sarah', 'Rep', 300000), STRUCT('Mike', 'Rep', 250000)],
             [150000, 175000, 200000, 225000]),
            (2, 'Beta Team', 'South',
             [STRUCT('Lisa', 'Lead', 450000), STRUCT('Tom', 'Rep', 280000)],
             [120000, 140000, 160000, 180000]),
            (3, 'Gamma Team', 'East',
             [STRUCT('Anna', 'Lead', 550000), STRUCT('Peter', 'Rep', 320000), STRUCT('Emma', 'Rep', 290000), STRUCT('David', 'Rep', 270000)],
             [180000, 210000, 240000, 270000])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH team_members_flat AS (
                SELECT
                    st.team_id,
                    st.team_name,
                    st.region,
                    member.name AS member_name,
                    member.role AS member_role,
                    member.quota AS member_quota,
                    member_offset
                FROM sales_teams st,
                UNNEST(st.members) AS member WITH OFFSET AS member_offset
            ),
            monthly_targets_flat AS (
                SELECT
                    team_id,
                    target AS monthly_target,
                    target_offset + 1 AS month_number
                FROM sales_teams,
                UNNEST(monthly_targets) AS target WITH OFFSET AS target_offset
            ),
            team_quota_totals AS (
                SELECT
                    team_id,
                    team_name,
                    region,
                    COUNT(*) AS member_count,
                    SUM(member_quota) AS total_quota,
                    SUM(CASE WHEN member_role = 'Lead' THEN member_quota ELSE 0 END) AS lead_quota,
                    SUM(CASE WHEN member_role = 'Rep' THEN member_quota ELSE 0 END) AS rep_quota,
                    ARRAY_AGG(member_name ORDER BY member_quota DESC) AS members_by_quota,
                    (SELECT member_name
                     FROM team_members_flat tmf2
                     WHERE tmf2.team_id = tmf.team_id
                     ORDER BY member_quota DESC
                     LIMIT 1) AS top_performer
                FROM team_members_flat tmf
                GROUP BY team_id, team_name, region
            ),
            team_target_analysis AS (
                SELECT
                    team_id,
                    SUM(monthly_target) AS annual_target,
                    AVG(monthly_target) AS avg_monthly_target,
                    MAX(monthly_target) AS peak_month_target,
                    ARRAY_AGG(monthly_target ORDER BY month_number) AS target_progression
                FROM monthly_targets_flat
                GROUP BY team_id
            ),
            combined_analysis AS (
                SELECT
                    tqt.team_id,
                    tqt.team_name,
                    tqt.region,
                    tqt.member_count,
                    tqt.total_quota,
                    tta.annual_target,
                    ROUND(tqt.total_quota * 100.0 / tta.annual_target, 2) AS quota_to_target_ratio,
                    tqt.top_performer,
                    tqt.members_by_quota,
                    tta.target_progression,
                    RANK() OVER (ORDER BY tqt.total_quota DESC) AS quota_rank,
                    RANK() OVER (ORDER BY tta.annual_target DESC) AS target_rank
                FROM team_quota_totals tqt
                JOIN team_target_analysis tta ON tqt.team_id = tta.team_id
            )
            SELECT
                team_name,
                region,
                member_count,
                total_quota,
                annual_target,
                quota_to_target_ratio,
                top_performer,
                quota_rank
            FROM combined_analysis
            ORDER BY quota_rank",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Gamma Team", "East", 4, 1430000, 900000, 158.89, "Anna", 1],
            ["Alpha Team", "North", 3, 1050000, 750000, 140.0, "John", 2],
            ["Beta Team", "South", 2, 730000, 600000, 121.67, "Lisa", 3],
        ]
    );
}

#[test]
#[ignore = "TODO: UNNEST column resolution"]
fn test_deep_nested_unnest_with_aggregations() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE ecommerce_orders (
                order_id INT64,
                customer_id INT64,
                order_date DATE,
                shipping_address STRUCT<city STRING, country STRING, zip STRING>,
                line_items ARRAY<STRUCT<
                    sku STRING,
                    name STRING,
                    quantity INT64,
                    price FLOAT64,
                    discounts ARRAY<STRUCT<code STRING, amount FLOAT64>>
                >>
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO ecommerce_orders VALUES
            (1001, 1, DATE '2024-01-15',
             STRUCT('New York', 'USA', '10001'),
             [
                 STRUCT('SKU001', 'Laptop', 1, 1200.0, [STRUCT('SAVE10', 120.0), STRUCT('MEMBER5', 60.0)]),
                 STRUCT('SKU002', 'Mouse', 2, 50.0, [STRUCT('BUNDLE', 10.0)])
             ]),
            (1002, 2, DATE '2024-01-16',
             STRUCT('Los Angeles', 'USA', '90001'),
             [
                 STRUCT('SKU003', 'Keyboard', 1, 150.0, [STRUCT('FLASH20', 30.0)]),
                 STRUCT('SKU004', 'Monitor', 1, 400.0, [STRUCT('SAVE10', 40.0), STRUCT('VIP15', 60.0)])
             ]),
            (1003, 1, DATE '2024-01-18',
             STRUCT('New York', 'USA', '10002'),
             [
                 STRUCT('SKU001', 'Laptop', 1, 1200.0, [STRUCT('REPEAT', 100.0)])
             ])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH order_items AS (
                SELECT
                    o.order_id,
                    o.customer_id,
                    o.order_date,
                    o.shipping_address.city AS city,
                    o.shipping_address.country AS country,
                    item.sku,
                    item.name AS product_name,
                    item.quantity,
                    item.price,
                    item.quantity * item.price AS line_subtotal,
                    item.discounts
                FROM ecommerce_orders o,
                UNNEST(o.line_items) AS item
            ),
            item_discounts AS (
                SELECT
                    oi.order_id,
                    oi.sku,
                    oi.product_name,
                    oi.line_subtotal,
                    discount.code AS discount_code,
                    discount.amount AS discount_amount
                FROM order_items oi,
                UNNEST(oi.discounts) AS discount
            ),
            discount_summary AS (
                SELECT
                    order_id,
                    sku,
                    product_name,
                    line_subtotal,
                    SUM(discount_amount) AS total_discounts,
                    COUNT(*) AS num_discounts,
                    ARRAY_AGG(discount_code ORDER BY discount_amount DESC) AS discount_codes
                FROM item_discounts
                GROUP BY order_id, sku, product_name, line_subtotal
            ),
            order_level_stats AS (
                SELECT
                    oi.order_id,
                    oi.customer_id,
                    oi.order_date,
                    oi.city,
                    COUNT(DISTINCT oi.sku) AS unique_items,
                    SUM(oi.line_subtotal) AS order_subtotal,
                    SUM(ds.total_discounts) AS order_discounts,
                    SUM(oi.line_subtotal) - SUM(ds.total_discounts) AS order_total,
                    ARRAY_AGG(oi.product_name ORDER BY oi.line_subtotal DESC) AS products_ordered,
                    (SELECT discount_code
                     FROM item_discounts id
                     WHERE id.order_id = oi.order_id
                     GROUP BY discount_code
                     ORDER BY SUM(discount_amount) DESC
                     LIMIT 1) AS top_discount_code
                FROM order_items oi
                LEFT JOIN discount_summary ds
                    ON oi.order_id = ds.order_id AND oi.sku = ds.sku
                GROUP BY oi.order_id, oi.customer_id, oi.order_date, oi.city
            ),
            customer_summary AS (
                SELECT
                    customer_id,
                    COUNT(DISTINCT order_id) AS order_count,
                    SUM(order_total) AS lifetime_value,
                    AVG(order_total) AS avg_order_value,
                    SUM(order_discounts) AS total_savings,
                    ROUND(SUM(order_discounts) * 100.0 / SUM(order_subtotal), 2) AS discount_rate_pct
                FROM order_level_stats
                GROUP BY customer_id
            )
            SELECT
                ols.order_id,
                ols.customer_id,
                ols.city,
                ols.unique_items,
                ols.order_subtotal,
                ols.order_discounts,
                ols.order_total,
                ols.top_discount_code,
                cs.order_count AS customer_order_count,
                cs.lifetime_value AS customer_ltv,
                ROUND(cs.discount_rate_pct, 2) AS customer_discount_rate,
                (SELECT ARRAY_AGG(STRUCT(ds.product_name, ds.total_discounts, ds.num_discounts))
                 FROM discount_summary ds
                 WHERE ds.order_id = ols.order_id) AS item_discount_details
            FROM order_level_stats ols
            JOIN customer_summary cs ON ols.customer_id = cs.customer_id
            ORDER BY ols.order_id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1001, 1, "New York", 2, 1300.0, 190.0, 1110.0, "SAVE10", 2, 2210.0, 11.6, [{"Laptop", 180.0, 2}, {"Mouse", 10.0, 1}]],
            [1002, 2, "Los Angeles", 2, 550.0, 130.0, 420.0, "VIP15", 1, 420.0, 23.64, [{"Keyboard", 30.0, 1}, {"Monitor", 100.0, 2}]],
            [1003, 1, "New York", 1, 1200.0, 100.0, 1100.0, "REPEAT", 2, 2210.0, 11.6, [{"Laptop", 100.0, 1}]],
        ]
    );
}

#[test]
fn test_union_debug_case_in_agg() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) AS null_count FROM sales",
        )
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_union_debug_with_case() {
    let mut executor = create_executor();
    setup_sales_data(&mut executor);

    let r1 = executor.execute_sql(
        "SELECT 'b', CAST(SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) AS STRING) FROM sales",
    );
    eprintln!("Standalone: {:?}", r1.is_ok());

    let r2 = executor.execute_sql(
        "SELECT 'a' AS m, CAST(COUNT(*) AS STRING) AS v FROM sales
         UNION ALL
         SELECT 'b', CAST(SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) AS STRING) FROM sales",
    );
    eprintln!("UNION: {:?}", r2.is_ok());
    if r2.is_err() {
        eprintln!("UNION Error: {:?}", r2.err());
    }
}
