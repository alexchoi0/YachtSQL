use crate::assert_table_eq;
use crate::common::{create_session, d};

fn setup_ecommerce_schema(session: &mut yachtsql::YachtSQLSession) {
    session
        .execute_sql(
            "CREATE TABLE customers (
                customer_id INT64,
                name STRING,
                email STRING,
                signup_date DATE,
                country STRING,
                segment STRING
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE products (
                product_id INT64,
                name STRING,
                category STRING,
                subcategory STRING,
                price INT64,
                cost INT64
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE orders (
                order_id INT64,
                customer_id INT64,
                order_date DATE,
                status STRING,
                shipping_country STRING
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE order_items (
                order_item_id INT64,
                order_id INT64,
                product_id INT64,
                quantity INT64,
                unit_price INT64,
                discount_pct INT64
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO customers VALUES
            (1, 'Alice Johnson', 'alice@email.com', DATE '2023-01-15', 'USA', 'Premium'),
            (2, 'Bob Smith', 'bob@email.com', DATE '2023-03-20', 'Canada', 'Standard'),
            (3, 'Carol White', 'carol@email.com', DATE '2023-06-10', 'UK', 'Premium'),
            (4, 'David Brown', 'david@email.com', DATE '2023-09-05', 'USA', 'Standard'),
            (5, 'Eve Davis', 'eve@email.com', DATE '2024-01-01', 'Germany', 'New')",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO products VALUES
            (101, 'Laptop Pro', 'Electronics', 'Computers', 1200, 800),
            (102, 'Wireless Mouse', 'Electronics', 'Accessories', 50, 25),
            (103, 'USB-C Hub', 'Electronics', 'Accessories', 80, 40),
            (201, 'Office Chair', 'Furniture', 'Seating', 350, 200),
            (202, 'Standing Desk', 'Furniture', 'Desks', 600, 350),
            (301, 'Notebook Set', 'Office', 'Supplies', 25, 10),
            (302, 'Pen Pack', 'Office', 'Supplies', 15, 5)",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO orders VALUES
            (1001, 1, DATE '2024-01-10', 'Completed', 'USA'),
            (1002, 2, DATE '2024-01-12', 'Completed', 'Canada'),
            (1003, 1, DATE '2024-01-15', 'Completed', 'USA'),
            (1004, 3, DATE '2024-01-18', 'Completed', 'UK'),
            (1005, 4, DATE '2024-01-20', 'Pending', 'USA'),
            (1006, 5, DATE '2024-01-22', 'Completed', 'Germany'),
            (1007, 1, DATE '2024-02-01', 'Completed', 'USA'),
            (1008, 2, DATE '2024-02-05', 'Cancelled', 'Canada')",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO order_items VALUES
            (1, 1001, 101, 1, 1200, 0),
            (2, 1001, 102, 2, 50, 10),
            (3, 1002, 201, 1, 350, 0),
            (4, 1003, 103, 3, 80, 5),
            (5, 1003, 301, 5, 25, 0),
            (6, 1004, 101, 1, 1200, 15),
            (7, 1004, 202, 1, 600, 10),
            (8, 1005, 102, 1, 50, 0),
            (9, 1006, 301, 10, 25, 20),
            (10, 1006, 302, 5, 15, 0),
            (11, 1007, 101, 1, 1200, 5),
            (12, 1007, 103, 2, 80, 0)",
        )
        .unwrap();
}

#[test]
fn test_executive_dashboard_kpis() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "SELECT
                COUNT(DISTINCT o.order_id) AS total_orders,
                COUNT(DISTINCT o.customer_id) AS unique_customers,
                SUM(oi.quantity * oi.unit_price * (100 - oi.discount_pct) / 100) AS gross_revenue,
                SUM(oi.quantity * (oi.unit_price - p.cost)) AS gross_profit,
                AVG(oi.quantity * oi.unit_price) AS avg_order_value
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN products p ON oi.product_id = p.product_id
            WHERE o.status = 'Completed'",
        )
        .unwrap();
    assert_table_eq!(result, [[6, 4, 5128, 2125, 500.0]]);
}

#[test]
fn test_sales_by_geography() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "SELECT
                o.shipping_country,
                COUNT(DISTINCT o.order_id) AS orders,
                COUNT(DISTINCT o.customer_id) AS customers,
                SUM(oi.quantity * oi.unit_price) AS revenue
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.status = 'Completed'
            GROUP BY o.shipping_country
            ORDER BY revenue DESC",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["USA", 3, 1, 3025],
            ["UK", 1, 1, 1800],
            ["Canada", 1, 1, 350],
            ["Germany", 1, 1, 325],
        ]
    );
}

#[test]
fn test_category_performance() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "WITH category_aggregates AS (
                SELECT
                    p.category,
                    p.subcategory,
                    SUM(oi.quantity) AS units_sold,
                    SUM(oi.quantity * oi.unit_price) AS revenue,
                    SUM(oi.quantity * (oi.unit_price - p.cost)) AS profit
                FROM order_items oi
                JOIN products p ON oi.product_id = p.product_id
                JOIN orders o ON oi.order_id = o.order_id
                WHERE o.status = 'Completed'
                GROUP BY p.category, p.subcategory
            )
            SELECT
                category,
                subcategory,
                units_sold,
                revenue,
                profit,
                ROUND(profit * 100.0 / revenue, 2) AS margin_pct
            FROM category_aggregates
            ORDER BY revenue DESC",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Electronics", "Computers", 3, 3600, 1200, 33.33],
            ["Furniture", "Desks", 1, 600, 250, 41.67],
            ["Electronics", "Accessories", 7, 500, 250, 50.0],
            ["Office", "Supplies", 20, 450, 275, 61.11],
            ["Furniture", "Seating", 1, 350, 150, 42.86],
        ]
    );
}

#[test]
fn test_customer_lifetime_value() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "WITH customer_orders AS (
                SELECT
                    c.customer_id,
                    c.name,
                    c.segment,
                    c.signup_date,
                    COUNT(DISTINCT o.order_id) AS order_count,
                    SUM(oi.quantity * oi.unit_price) AS total_revenue,
                    MIN(o.order_date) AS first_order,
                    MAX(o.order_date) AS last_order
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'Completed'
                LEFT JOIN order_items oi ON o.order_id = oi.order_id
                GROUP BY c.customer_id, c.name, c.segment, c.signup_date
            )
            SELECT
                customer_id,
                name,
                segment,
                order_count,
                total_revenue,
                CASE WHEN order_count > 0 THEN total_revenue / order_count ELSE 0 END AS avg_order_value,
                DATE_DIFF(last_order, first_order, DAY) AS customer_tenure_days
            FROM customer_orders
            ORDER BY total_revenue DESC NULLS LAST, customer_id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [
                1,
                "Alice Johnson",
                "Premium",
                3,
                3025,
                1008.3333333333334,
                22
            ],
            [3, "Carol White", "Premium", 1, 1800, 1800.0, 0],
            [2, "Bob Smith", "Standard", 1, 350, 350.0, 0],
            [5, "Eve Davis", "New", 1, 325, 325.0, 0],
            [4, "David Brown", "Standard", 0, null, 0.0, null],
        ]
    );
}

#[test]
fn test_product_affinity_analysis() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "WITH order_products AS (
                SELECT DISTINCT order_id, product_id
                FROM order_items
            )
            SELECT
                p1.name AS product_1,
                p2.name AS product_2,
                COUNT(*) AS times_bought_together
            FROM order_products op1
            JOIN order_products op2 ON op1.order_id = op2.order_id AND op1.product_id < op2.product_id
            JOIN products p1 ON op1.product_id = p1.product_id
            JOIN products p2 ON op2.product_id = p2.product_id
            GROUP BY p1.name, p2.name
            HAVING COUNT(*) >= 1
            ORDER BY times_bought_together DESC, product_1, product_2",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Laptop Pro", "Standing Desk", 1],
            ["Laptop Pro", "USB-C Hub", 1],
            ["Laptop Pro", "Wireless Mouse", 1],
            ["Notebook Set", "Pen Pack", 1],
            ["USB-C Hub", "Notebook Set", 1],
        ]
    );
}

#[test]
fn test_inventory_velocity() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "SELECT
                p.product_id,
                p.name,
                p.category,
                SUM(oi.quantity) AS total_units_sold,
                COUNT(DISTINCT o.order_id) AS order_count,
                AVG(oi.quantity) AS avg_units_per_order,
                DENSE_RANK() OVER (PARTITION BY p.category ORDER BY SUM(oi.quantity) DESC) AS category_rank
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.status = 'Completed'
            GROUP BY p.product_id, p.name, p.category
            ORDER BY p.category, category_rank, p.product_id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [103, "USB-C Hub", "Electronics", 5, 2, 2.5, 1],
            [101, "Laptop Pro", "Electronics", 3, 3, 1.0, 2],
            [102, "Wireless Mouse", "Electronics", 2, 1, 2.0, 3],
            [201, "Office Chair", "Furniture", 1, 1, 1.0, 1],
            [202, "Standing Desk", "Furniture", 1, 1, 1.0, 1],
            [301, "Notebook Set", "Office", 15, 2, 7.5, 1],
            [302, "Pen Pack", "Office", 5, 1, 5.0, 2],
        ]
    );
}

#[test]
fn test_customer_segmentation() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "WITH customer_metrics AS (
                SELECT
                    c.customer_id,
                    c.segment,
                    COUNT(DISTINCT o.order_id) AS orders,
                    COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS revenue
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.status = 'Completed'
                LEFT JOIN order_items oi ON o.order_id = oi.order_id
                GROUP BY c.customer_id, c.segment
            )
            SELECT
                segment,
                COUNT(*) AS customer_count,
                SUM(orders) AS total_orders,
                SUM(revenue) AS total_revenue,
                AVG(orders) AS avg_orders_per_customer,
                AVG(revenue) AS avg_revenue_per_customer
            FROM customer_metrics
            GROUP BY segment
            ORDER BY total_revenue DESC",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Premium", 2, 4, 4825, 2.0, 2412.5],
            ["Standard", 2, 1, 350, 0.5, 175.0],
            ["New", 1, 1, 325, 1.0, 325.0],
        ]
    );
}

#[test]
fn test_conversion_funnel() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "SELECT
                'Registered Customers' AS stage, COUNT(*) AS count
            FROM customers
            UNION ALL
            SELECT
                'Made First Order' AS stage, COUNT(DISTINCT customer_id) AS count
            FROM orders
            UNION ALL
            SELECT
                'Completed Order' AS stage, COUNT(DISTINCT customer_id) AS count
            FROM orders WHERE status = 'Completed'",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Registered Customers", 5],
            ["Made First Order", 5],
            ["Completed Order", 4],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_order_status_breakdown() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "SELECT
                status,
                COUNT(*) AS order_count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
            FROM orders
            GROUP BY status
            ORDER BY order_count DESC, status",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Completed", 6, 75.0],
            ["Cancelled", 1, 12.5],
            ["Pending", 1, 12.5],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_daily_sales_trend() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "SELECT
                o.order_date,
                COUNT(DISTINCT o.order_id) AS orders,
                SUM(oi.quantity * oi.unit_price) AS revenue,
                SUM(SUM(oi.quantity * oi.unit_price)) OVER (ORDER BY o.order_date) AS cumulative_revenue
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.status = 'Completed'
            GROUP BY o.order_date
            ORDER BY o.order_date",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(2024, 1, 10), 1, 1300, 1300],
            [d(2024, 1, 12), 1, 350, 1650],
            [d(2024, 1, 15), 1, 365, 2015],
            [d(2024, 1, 18), 1, 1800, 3815],
            [d(2024, 1, 22), 1, 325, 4140],
            [d(2024, 2, 1), 1, 1360, 5500],
        ]
    );
}

#[test]
fn test_discount_impact_analysis() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "SELECT
                CASE
                    WHEN discount_pct = 0 THEN 'No Discount'
                    WHEN discount_pct <= 10 THEN '1-10%'
                    WHEN discount_pct <= 20 THEN '11-20%'
                    ELSE '21%+'
                END AS discount_tier,
                COUNT(*) AS line_items,
                SUM(quantity) AS units_sold,
                SUM(quantity * unit_price) AS gross_revenue,
                SUM(quantity * unit_price * discount_pct / 100) AS discount_given,
                SUM(quantity * unit_price * (100 - discount_pct) / 100) AS net_revenue
            FROM order_items
            GROUP BY discount_tier
            ORDER BY
                CASE discount_tier
                    WHEN 'No Discount' THEN 1
                    WHEN '1-10%' THEN 2
                    WHEN '11-20%' THEN 3
                    ELSE 4
                END",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["No Discount", 6, 15, 1960, 0, 1960],
            ["1-10%", 4, 7, 2140, 142, 1998],
            ["11-20%", 2, 11, 1450, 230, 1220],
        ]
    );
}

#[test]
fn test_new_vs_returning_customers() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "WITH first_orders AS (
                SELECT customer_id, MIN(order_date) AS first_order_date
                FROM orders
                WHERE status = 'Completed'
                GROUP BY customer_id
            )
            SELECT
                DATE_TRUNC(o.order_date, MONTH) AS month,
                SUM(CASE WHEN o.order_date = f.first_order_date THEN 1 ELSE 0 END) AS new_customer_orders,
                SUM(CASE WHEN o.order_date > f.first_order_date THEN 1 ELSE 0 END) AS returning_customer_orders
            FROM orders o
            JOIN first_orders f ON o.customer_id = f.customer_id
            WHERE o.status = 'Completed'
            GROUP BY month
            ORDER BY month",
        )
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1), 4, 1], [d(2024, 2, 1), 0, 1],]);
}

#[test]
fn test_average_basket_composition() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "WITH order_summary AS (
                SELECT
                    oi.order_id,
                    COUNT(DISTINCT oi.product_id) AS unique_products,
                    SUM(oi.quantity) AS total_items,
                    COUNT(DISTINCT p.category) AS unique_categories
                FROM order_items oi
                JOIN products p ON oi.product_id = p.product_id
                GROUP BY oi.order_id
            )
            SELECT
                AVG(unique_products) AS avg_unique_products,
                AVG(total_items) AS avg_total_items,
                AVG(unique_categories) AS avg_categories,
                MIN(unique_products) AS min_products,
                MAX(unique_products) AS max_products
            FROM order_summary",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [[
            1.7142857142857142,
            4.714285714285714,
            1.2857142857142858,
            1,
            2
        ]]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_customer_acquisition_by_month() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "SELECT
                DATE_TRUNC(signup_date, MONTH) AS signup_month,
                COUNT(*) AS new_customers,
                SUM(COUNT(*)) OVER (ORDER BY DATE_TRUNC(signup_date, MONTH)) AS cumulative_customers
            FROM customers
            GROUP BY signup_month
            ORDER BY signup_month",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [d(2023, 1, 1), 1, 1],
            [d(2023, 3, 1), 1, 2],
            [d(2023, 6, 1), 1, 3],
            [d(2023, 9, 1), 1, 4],
            [d(2024, 1, 1), 1, 5],
        ]
    );
}

#[test]
fn test_product_margin_analysis() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "SELECT
                p.product_id,
                p.name,
                p.category,
                p.price,
                p.cost,
                p.price - p.cost AS unit_margin,
                ROUND((p.price - p.cost) * 100.0 / p.price, 2) AS margin_pct,
                COALESCE(SUM(oi.quantity), 0) AS units_sold,
                COALESCE(SUM(oi.quantity * (oi.unit_price - p.cost)), 0) AS total_profit
            FROM products p
            LEFT JOIN order_items oi ON p.product_id = oi.product_id
            LEFT JOIN orders o ON oi.order_id = o.order_id AND o.status = 'Completed'
            GROUP BY p.product_id, p.name, p.category, p.price, p.cost
            ORDER BY total_profit DESC, p.product_id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [
                101,
                "Laptop Pro",
                "Electronics",
                1200,
                800,
                400,
                33.33,
                3,
                1200
            ],
            [
                202,
                "Standing Desk",
                "Furniture",
                600,
                350,
                250,
                41.67,
                1,
                250
            ],
            [301, "Notebook Set", "Office", 25, 10, 15, 60.0, 15, 225],
            [103, "USB-C Hub", "Electronics", 80, 40, 40, 50.0, 5, 200],
            [
                201,
                "Office Chair",
                "Furniture",
                350,
                200,
                150,
                42.86,
                1,
                150
            ],
            [
                102,
                "Wireless Mouse",
                "Electronics",
                50,
                25,
                25,
                50.0,
                3,
                75
            ],
            [302, "Pen Pack", "Office", 15, 5, 10, 66.67, 5, 50],
        ]
    );
}

#[test]
fn test_order_size_distribution() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "WITH order_totals AS (
                SELECT
                    order_id,
                    SUM(quantity * unit_price) AS order_total
                FROM order_items
                GROUP BY order_id
            )
            SELECT
                CASE
                    WHEN order_total < 100 THEN 'Small (<$100)'
                    WHEN order_total < 500 THEN 'Medium ($100-$500)'
                    WHEN order_total < 1000 THEN 'Large ($500-$1000)'
                    ELSE 'Enterprise ($1000+)'
                END AS order_size,
                COUNT(*) AS order_count,
                SUM(order_total) AS total_revenue,
                AVG(order_total) AS avg_order_value
            FROM order_totals
            GROUP BY order_size
            ORDER BY avg_order_value",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Small (<$100)", 1, 50, 50.0],
            ["Medium ($100-$500)", 3, 1040, 346.6666666666667],
            ["Enterprise ($1000+)", 3, 4460, 1486.6666666666667],
        ]
    );
}

#[test]
fn test_cross_sell_opportunities() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "WITH customer_categories AS (
                SELECT DISTINCT
                    o.customer_id,
                    p.category
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                JOIN products p ON oi.product_id = p.product_id
                WHERE o.status = 'Completed'
            ),
            all_categories AS (
                SELECT DISTINCT category FROM products
            )
            SELECT
                c.customer_id,
                cust.name,
                STRING_AGG(cc.category, ', ') AS purchased_categories,
                STRING_AGG(ac.category, ', ') AS missing_categories
            FROM customers c
            LEFT JOIN customer_categories cc ON c.customer_id = cc.customer_id
            CROSS JOIN all_categories ac
            LEFT JOIN customer_categories cc2 ON c.customer_id = cc2.customer_id AND ac.category = cc2.category
            JOIN customers cust ON c.customer_id = cust.customer_id
            WHERE cc2.customer_id IS NULL
            GROUP BY c.customer_id, cust.name
            HAVING COUNT(DISTINCT cc.category) > 0
            ORDER BY c.customer_id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [
                1,
                "Alice Johnson",
                "Electronics, Office",
                "Furniture, Furniture"
            ],
            [
                2,
                "Bob Smith",
                "Furniture, Furniture",
                "Electronics, Office"
            ],
            [3, "Carol White", "Electronics, Furniture", "Office, Office"],
            [5, "Eve Davis", "Office, Office", "Electronics, Furniture"],
        ]
    );
}

#[test]
fn test_revenue_concentration() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "WITH product_revenue AS (
                SELECT
                    p.product_id,
                    p.name,
                    SUM(oi.quantity * oi.unit_price) AS revenue
                FROM order_items oi
                JOIN products p ON oi.product_id = p.product_id
                JOIN orders o ON oi.order_id = o.order_id
                WHERE o.status = 'Completed'
                GROUP BY p.product_id, p.name
            ),
            total AS (
                SELECT SUM(revenue) AS total_revenue FROM product_revenue
            )
            SELECT
                pr.name,
                pr.revenue,
                ROUND(pr.revenue * 100.0 / t.total_revenue, 2) AS pct_of_total,
                SUM(pr.revenue) OVER (ORDER BY pr.revenue DESC) * 100.0 / t.total_revenue AS cumulative_pct
            FROM product_revenue pr
            CROSS JOIN total t
            ORDER BY pr.revenue DESC",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Laptop Pro", 3600, 65.45, 65.45454545454545],
            ["Standing Desk", 600, 10.91, 76.36363636363636],
            ["USB-C Hub", 400, 7.27, 83.63636363636364],
            ["Notebook Set", 375, 6.82, 90.45454545454545],
            ["Office Chair", 350, 6.36, 96.81818181818181],
            ["Wireless Mouse", 100, 1.82, 98.63636363636364],
            ["Pen Pack", 75, 1.36, 100.0],
        ]
    );
}

#[test]
fn test_days_between_orders() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "WITH ordered AS (
                SELECT
                    customer_id,
                    order_date,
                    LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date
                FROM orders
                WHERE status = 'Completed'
            )
            SELECT
                customer_id,
                AVG(DATE_DIFF(order_date, prev_order_date, DAY)) AS avg_days_between_orders,
                MIN(DATE_DIFF(order_date, prev_order_date, DAY)) AS min_days,
                MAX(DATE_DIFF(order_date, prev_order_date, DAY)) AS max_days
            FROM ordered
            WHERE prev_order_date IS NOT NULL
            GROUP BY customer_id",
        )
        .unwrap();
    assert_table_eq!(result, [[1, 11.0, 5, 17],]);
}

#[test]
fn test_category_share_of_wallet() {
    let mut session = create_session();
    setup_ecommerce_schema(&mut session);

    let result = session
        .execute_sql(
            "WITH customer_category_spend AS (
                SELECT
                    o.customer_id,
                    p.category,
                    SUM(oi.quantity * oi.unit_price) AS category_spend
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                JOIN products p ON oi.product_id = p.product_id
                WHERE o.status = 'Completed'
                GROUP BY o.customer_id, p.category
            ),
            customer_total AS (
                SELECT
                    customer_id,
                    SUM(category_spend) AS total_spend
                FROM customer_category_spend
                GROUP BY customer_id
            )
            SELECT
                ccs.customer_id,
                ccs.category,
                ccs.category_spend,
                ct.total_spend,
                ROUND(ccs.category_spend * 100.0 / ct.total_spend, 2) AS share_of_wallet
            FROM customer_category_spend ccs
            JOIN customer_total ct ON ccs.customer_id = ct.customer_id
            ORDER BY ccs.customer_id, share_of_wallet DESC",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Electronics", 2900, 3025, 95.87],
            [1, "Office", 125, 3025, 4.13],
            [2, "Furniture", 350, 350, 100.0],
            [3, "Electronics", 1200, 1800, 66.67],
            [3, "Furniture", 600, 1800, 33.33],
            [5, "Office", 325, 325, 100.0],
        ]
    );
}
