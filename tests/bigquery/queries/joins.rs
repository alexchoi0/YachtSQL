use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

fn setup_tables(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, dept_id INT64)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1), (4, 'Diana', NULL)")
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')",
        )
        .unwrap();
}

#[test]
fn test_inner_join() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql("SELECT u.name, d.name FROM users u INNER JOIN departments d ON u.dept_id = d.id ORDER BY u.name")
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Bob", "Sales"],
            ["Charlie", "Engineering"],
        ]
    );
}

#[test]
fn test_left_join() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql("SELECT u.name, d.name FROM users u LEFT JOIN departments d ON u.dept_id = d.id ORDER BY u.name")
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Bob", "Sales"],
            ["Charlie", "Engineering"],
            ["Diana", null],
        ]
    );
}

#[test]
fn test_full_outer_join() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql("SELECT u.name, d.name FROM users u FULL OUTER JOIN departments d ON u.dept_id = d.id ORDER BY u.name NULLS LAST, d.name NULLS LAST")
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", "Engineering"],
            ["Bob", "Sales"],
            ["Charlie", "Engineering"],
            ["Diana", null],
            [null, "Marketing"],
        ]
    );
}

#[test]
fn test_cross_join() {
    let mut session = create_session();

    session.execute_sql("CREATE TABLE a (x INT64)").unwrap();
    session.execute_sql("CREATE TABLE b (y INT64)").unwrap();
    session
        .execute_sql("INSERT INTO a VALUES (1), (2)")
        .unwrap();
    session
        .execute_sql("INSERT INTO b VALUES (10), (20)")
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y FROM a CROSS JOIN b ORDER BY x, y")
        .unwrap();

    assert_table_eq!(result, [[1, 10], [1, 20], [2, 10], [2, 20],]);
}

#[test]
fn test_self_join() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, manager_id INT64)")
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO employees VALUES (1, 'Alice', NULL), (2, 'Bob', 1), (3, 'Charlie', 1)",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT e.name, m.name FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.name")
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice", null], ["Bob", "Alice"], ["Charlie", "Alice"],]
    );
}

#[test]
fn test_self_join_with_inequality() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE nums (id INT64, val INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let result = session
        .execute_sql("SELECT a.id AS a_id, b.id AS b_id FROM nums a JOIN nums b ON a.id < b.id ORDER BY a_id, b_id")
        .unwrap();

    assert_table_eq!(result, [[1, 2], [1, 3], [2, 3],]);
}

#[test]
fn test_cte_self_join_with_inequality() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE nums (id INT64, val INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let result = session
        .execute_sql("WITH nums_cte AS (SELECT id, val FROM nums) SELECT a.id AS a_id, b.id AS b_id FROM nums_cte a JOIN nums_cte b ON a.id < b.id ORDER BY a_id, b_id")
        .unwrap();

    assert_table_eq!(result, [[1, 2], [1, 3], [2, 3],]);
}

#[test]
fn test_cte_self_join_with_compound_condition() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE order_items (order_id INT64, product_id INT64)")
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO order_items VALUES (1, 100), (1, 200), (1, 300), (2, 100), (2, 400)",
        )
        .unwrap();

    let result = session
        .execute_sql("WITH order_products AS (SELECT DISTINCT order_id, product_id FROM order_items) SELECT op1.order_id, op1.product_id AS p1, op2.product_id AS p2 FROM order_products op1 JOIN order_products op2 ON op1.order_id = op2.order_id AND op1.product_id < op2.product_id ORDER BY op1.order_id, p1, p2")
        .unwrap();

    assert_table_eq!(
        result,
        [[1, 100, 200], [1, 100, 300], [1, 200, 300], [2, 100, 400],]
    );
}

#[test]
fn test_cte_self_join_with_additional_joins() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE oi (order_id INT64, product_id INT64)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE prods (product_id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO oi VALUES (1, 100), (1, 200), (1, 300), (2, 100), (2, 400)")
        .unwrap();
    session
        .execute_sql("INSERT INTO prods VALUES (100, 'A'), (200, 'B'), (300, 'C'), (400, 'D')")
        .unwrap();

    let result = session
        .execute_sql("WITH order_products AS (SELECT DISTINCT order_id, product_id FROM oi) SELECT p1.name AS product_1, p2.name AS product_2, COUNT(*) AS times_bought_together FROM order_products op1 JOIN order_products op2 ON op1.order_id = op2.order_id AND op1.product_id < op2.product_id JOIN prods p1 ON op1.product_id = p1.product_id JOIN prods p2 ON op2.product_id = p2.product_id GROUP BY p1.name, p2.name ORDER BY product_1, product_2")
        .unwrap();

    assert_table_eq!(
        result,
        [["A", "B", 1], ["A", "C", 1], ["A", "D", 1], ["B", "C", 1],]
    );
}

#[test]
fn test_cte_self_join_with_having() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE oi2 (order_id INT64, product_id INT64)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE prods2 (product_id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO oi2 VALUES (1, 100), (1, 200), (1, 300), (2, 100), (2, 400)")
        .unwrap();
    session
        .execute_sql("INSERT INTO prods2 VALUES (100, 'A'), (200, 'B'), (300, 'C'), (400, 'D')")
        .unwrap();

    let result = session
        .execute_sql("WITH order_products AS (SELECT DISTINCT order_id, product_id FROM oi2) SELECT p1.name AS product_1, p2.name AS product_2, COUNT(*) AS times_bought_together FROM order_products op1 JOIN order_products op2 ON op1.order_id = op2.order_id AND op1.product_id < op2.product_id JOIN prods2 p1 ON op1.product_id = p1.product_id JOIN prods2 p2 ON op2.product_id = p2.product_id GROUP BY p1.name, p2.name HAVING COUNT(*) >= 1 ORDER BY times_bought_together DESC, product_1, product_2")
        .unwrap();

    assert_table_eq!(
        result,
        [["A", "B", 1], ["A", "C", 1], ["A", "D", 1], ["B", "C", 1],]
    );
}

#[test]
fn test_anti_join_pattern() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE users3 (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE orders3 (id INT64, user_id INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO users3 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();
    session
        .execute_sql("INSERT INTO orders3 VALUES (100, 1), (101, 1)")
        .unwrap();

    let result = session
        .execute_sql("SELECT u.id, u.name FROM users3 u LEFT JOIN orders3 o ON u.id = o.user_id WHERE o.id IS NULL ORDER BY u.id")
        .unwrap();

    assert_table_eq!(result, [[2, "Bob"], [3, "Charlie"],]);
}

#[test]
fn test_cross_join_anti_pattern() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE customers4 (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE customer_prefs (customer_id INT64, category STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats (category STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO customers4 VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    session
        .execute_sql("INSERT INTO customer_prefs VALUES (1, 'Electronics'), (1, 'Books')")
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats VALUES ('Electronics'), ('Books'), ('Clothing')")
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT c.id, c.name, ac.category AS missing
             FROM customers4 c
             CROSS JOIN all_cats ac
             LEFT JOIN customer_prefs cp ON c.id = cp.customer_id AND ac.category = cp.category
             WHERE cp.customer_id IS NULL
             ORDER BY c.id, missing",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "Alice", "Clothing"],
            [2, "Bob", "Books"],
            [2, "Bob", "Clothing"],
            [2, "Bob", "Electronics"],
        ]
    );
}

#[test]
fn test_cross_join_anti_pattern_with_ctes() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE customers5 (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE customer_prefs5 (customer_id INT64, category STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats5 (category STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO customers5 VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    session
        .execute_sql("INSERT INTO customer_prefs5 VALUES (1, 'Electronics'), (1, 'Books')")
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats5 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .unwrap();

    let result = session
        .execute_sql(
            "WITH prefs AS (SELECT DISTINCT customer_id, category FROM customer_prefs5),
                  cats AS (SELECT DISTINCT category FROM all_cats5)
             SELECT c.id, c.name, ac.category AS missing
             FROM customers5 c
             CROSS JOIN cats ac
             LEFT JOIN prefs cp ON c.id = cp.customer_id AND ac.category = cp.category
             WHERE cp.customer_id IS NULL
             ORDER BY c.id, missing",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            [1, "Alice", "Clothing"],
            [2, "Bob", "Books"],
            [2, "Bob", "Clothing"],
            [2, "Bob", "Electronics"],
        ]
    );
}

#[test]
fn test_cross_sell_pattern() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE cust6 (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE cust_cats6 (customer_id INT64, category STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats6 (category STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO cust6 VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    session
        .execute_sql("INSERT INTO cust_cats6 VALUES (1, 'Electronics'), (1, 'Books')")
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats6 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .unwrap();

    let result = session
        .execute_sql(
            "WITH customer_categories AS (SELECT DISTINCT customer_id, category FROM cust_cats6),
                  all_categories AS (SELECT DISTINCT category FROM all_cats6)
             SELECT
                c.id,
                c.name,
                STRING_AGG(cc.category, ', ') AS purchased,
                STRING_AGG(ac.category, ', ') AS missing
             FROM cust6 c
             LEFT JOIN customer_categories cc ON c.id = cc.customer_id
             CROSS JOIN all_categories ac
             LEFT JOIN customer_categories cc2 ON c.id = cc2.customer_id AND ac.category = cc2.category
             WHERE cc2.customer_id IS NULL
             GROUP BY c.id, c.name
             HAVING COUNT(DISTINCT cc.category) > 0
             ORDER BY c.id"
        )
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Alice", "Electronics, Books", "Clothing, Clothing"],]
    );
}

#[test]
fn test_cte_used_twice() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE data7 (id INT64, val STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data7 VALUES (1, 'A'), (2, 'B'), (3, 'C')")
        .unwrap();

    let result = session
        .execute_sql(
            "WITH cte AS (SELECT id, val FROM data7)
             SELECT a.id AS a_id, a.val AS a_val, b.id AS b_id, b.val AS b_val
             FROM cte a
             JOIN cte b ON a.id = b.id
             ORDER BY a.id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "A", 1, "A"], [2, "B", 2, "B"], [3, "C", 3, "C"],]
    );
}

#[test]
fn test_multiple_left_joins_to_same_cte() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE base8 (id INT64)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE lookup8 (id INT64, val STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO base8 VALUES (1), (2)")
        .unwrap();
    session
        .execute_sql("INSERT INTO lookup8 VALUES (1, 'A'), (2, 'B')")
        .unwrap();

    let result = session
        .execute_sql(
            "WITH lkp AS (SELECT id, val FROM lookup8)
             SELECT b.id, a.val AS a_val, c.val AS c_val
             FROM base8 b
             LEFT JOIN lkp a ON b.id = a.id
             LEFT JOIN lkp c ON b.id = c.id
             ORDER BY b.id",
        )
        .unwrap();

    assert_table_eq!(result, [[1, "A", "A"], [2, "B", "B"],]);
}

#[test]
fn test_cross_sell_pattern_no_group() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE cust9 (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE cust_cats9 (customer_id INT64, category STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats9 (category STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO cust9 VALUES (1, 'Alice')")
        .unwrap();
    session
        .execute_sql("INSERT INTO cust_cats9 VALUES (1, 'Electronics'), (1, 'Books')")
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats9 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .unwrap();

    let result = session
        .execute_sql(
            "WITH customer_categories AS (SELECT DISTINCT customer_id, category FROM cust_cats9),
                  all_categories AS (SELECT DISTINCT category FROM all_cats9)
             SELECT c.id, cc.category AS purchased, ac.category AS all_cat, cc2.category AS check_cat
             FROM cust9 c
             LEFT JOIN customer_categories cc ON c.id = cc.customer_id
             CROSS JOIN all_categories ac
             LEFT JOIN customer_categories cc2 ON c.id = cc2.customer_id AND ac.category = cc2.category
             ORDER BY c.id, purchased, all_cat"
        )
        .unwrap();

    println!("Result row count: {}", result.row_count());
    for r in result.to_records().unwrap() {
        println!("  {:?}", r.values());
    }
}

#[test]
fn test_cross_sell_pattern_with_where() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE cust10 (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE cust_cats10 (customer_id INT64, category STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats10 (category STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO cust10 VALUES (1, 'Alice')")
        .unwrap();
    session
        .execute_sql("INSERT INTO cust_cats10 VALUES (1, 'Electronics'), (1, 'Books')")
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats10 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .unwrap();

    let result = session
        .execute_sql(
            "WITH customer_categories AS (SELECT DISTINCT customer_id, category FROM cust_cats10),
                  all_categories AS (SELECT DISTINCT category FROM all_cats10)
             SELECT c.id, cc.category AS purchased, ac.category AS missing_cat
             FROM cust10 c
             LEFT JOIN customer_categories cc ON c.id = cc.customer_id
             CROSS JOIN all_categories ac
             LEFT JOIN customer_categories cc2 ON c.id = cc2.customer_id AND ac.category = cc2.category
             WHERE cc2.customer_id IS NULL
             ORDER BY c.id, purchased, missing_cat"
        )
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Books", "Clothing"], [1, "Electronics", "Clothing"],]
    );
}

#[test]
fn test_cross_sell_pattern_with_group() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE cust11 (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE cust_cats11 (customer_id INT64, category STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats11 (category STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO cust11 VALUES (1, 'Alice')")
        .unwrap();
    session
        .execute_sql("INSERT INTO cust_cats11 VALUES (1, 'Electronics'), (1, 'Books')")
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats11 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .unwrap();

    let result = session
        .execute_sql(
            "WITH customer_categories AS (SELECT DISTINCT customer_id, category FROM cust_cats11),
                  all_categories AS (SELECT DISTINCT category FROM all_cats11)
             SELECT
                c.id,
                c.name,
                STRING_AGG(cc.category, ', ') AS purchased,
                STRING_AGG(ac.category, ', ') AS missing
             FROM cust11 c
             LEFT JOIN customer_categories cc ON c.id = cc.customer_id
             CROSS JOIN all_categories ac
             LEFT JOIN customer_categories cc2 ON c.id = cc2.customer_id AND ac.category = cc2.category
             WHERE cc2.customer_id IS NULL
             GROUP BY c.id, c.name
             ORDER BY c.id"
        )
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Alice", "Electronics, Books", "Clothing, Clothing"],]
    );
}

#[test]
fn test_cross_sell_pattern_with_having() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE cust12 (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE cust_cats12 (customer_id INT64, category STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE all_cats12 (category STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO cust12 VALUES (1, 'Alice')")
        .unwrap();
    session
        .execute_sql("INSERT INTO cust_cats12 VALUES (1, 'Electronics'), (1, 'Books')")
        .unwrap();
    session
        .execute_sql("INSERT INTO all_cats12 VALUES ('Electronics'), ('Books'), ('Clothing')")
        .unwrap();

    let result = session
        .execute_sql(
            "WITH customer_categories AS (SELECT DISTINCT customer_id, category FROM cust_cats12),
                  all_categories AS (SELECT DISTINCT category FROM all_cats12)
             SELECT
                c.id,
                c.name,
                STRING_AGG(cc.category, ', ') AS purchased,
                STRING_AGG(ac.category, ', ') AS missing
             FROM cust12 c
             LEFT JOIN customer_categories cc ON c.id = cc.customer_id
             CROSS JOIN all_categories ac
             LEFT JOIN customer_categories cc2 ON c.id = cc2.customer_id AND ac.category = cc2.category
             WHERE cc2.customer_id IS NULL
             GROUP BY c.id, c.name
             HAVING COUNT(DISTINCT cc.category) > 0
             ORDER BY c.id"
        )
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "Alice", "Electronics, Books", "Clothing, Clothing"],]
    );
}

#[test]
fn test_join_with_where_clause() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql("SELECT u.name, d.name FROM users u INNER JOIN departments d ON u.dept_id = d.id WHERE d.name = 'Engineering' ORDER BY u.name")
        .unwrap();

    assert_table_eq!(
        result,
        [["Alice", "Engineering"], ["Charlie", "Engineering"],]
    );
}

#[test]
fn test_join_multiple_conditions() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64, product_id INT64)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 101), (3, 2, 100)")
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')")
        .unwrap();

    let result = session
        .execute_sql("SELECT o.id, p.name FROM orders o INNER JOIN products p ON o.product_id = p.id WHERE o.user_id = 1 ORDER BY o.id")
        .unwrap();

    assert_table_eq!(result, [[1, "Widget"], [2, "Gadget"],]);
}

#[test]
fn test_join_three_tables() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE customers (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64, product_id INT64)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    session
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 2, 101)")
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')")
        .unwrap();

    let result = session
        .execute_sql("SELECT c.name, p.name FROM customers c INNER JOIN orders o ON c.id = o.customer_id INNER JOIN products p ON o.product_id = p.id ORDER BY c.name")
        .unwrap();

    assert_table_eq!(result, [["Alice", "Widget"], ["Bob", "Gadget"],]);
}
