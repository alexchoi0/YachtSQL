#![allow(dead_code)]
#![allow(unused_variables)]

mod common;

use common::{assert_batch_eq, build_batch};
use yachtsql::{DialectType, QueryExecutor, Value};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

fn setup_yacht_owners_orders(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS yacht_owners")
        .unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();

    executor
        .execute_sql("CREATE TABLE yacht_owners (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (order_id INT64, owner_id INT64, amount FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO yacht_owners VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();

    executor
        .execute_sql("INSERT INTO orders VALUES (101, 1, 100.0), (102, 1, 200.0), (103, 2, 150.0)")
        .unwrap();
}

#[test]
fn test_inner_join_basic() {
    let mut executor = create_executor();
    setup_yacht_owners_orders(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT y.name AS owner_name, o.amount
             FROM yacht_owners y
             INNER JOIN orders o ON y.id = o.owner_id
             ORDER BY o.amount",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    let names = result.column(0).unwrap();
    let amounts = result.column(1).unwrap();

    assert_eq!(names.get(0).unwrap().as_str().unwrap(), "Alice");
    assert_eq!(amounts.get(0).unwrap().as_f64().unwrap(), 100.0);
    assert_eq!(names.get(1).unwrap().as_str().unwrap(), "Bob");
    assert_eq!(amounts.get(1).unwrap().as_f64().unwrap(), 150.0);
    assert_eq!(names.get(2).unwrap().as_str().unwrap(), "Alice");
    assert_eq!(amounts.get(2).unwrap().as_f64().unwrap(), 200.0);
}

#[test]
fn test_inner_join_with_where_clause() {
    let mut executor = create_executor();
    setup_yacht_owners_orders(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT y.name AS owner_name, o.amount
             FROM yacht_owners y
             INNER JOIN orders o ON y.id = o.owner_id
             WHERE o.amount > 150.0",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let names = result.column(0).unwrap();
    let amounts = result.column(1).unwrap();
    assert_eq!(names.get(0).unwrap().as_str().unwrap(), "Alice");
    assert_eq!(amounts.get(0).unwrap().as_f64().unwrap(), 200.0);
}

fn setup_equipment_inventory(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS equipment")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS inventory")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE equipment (equip_id INT64, equip_name STRING, category STRING)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE inventory (inv_id INT64, equipment_id INT64, product_category STRING, quantity INT64)",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO equipment VALUES (1, 'Anchor', 'Hardware'), (2, 'Rope', 'Hardware'), (3, 'GPS', 'Electronics')",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO inventory VALUES (1, 1, 'Hardware', 10), (2, 2, 'Hardware', 25), (3, 3, 'Electronics', 5)",
        )
        .unwrap();
}

#[test]
fn test_inner_join_multiple_conditions() {
    let mut executor = create_executor();
    setup_equipment_inventory(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT e.equip_name, i.quantity
             FROM equipment e
             INNER JOIN inventory i
               ON e.equip_id = i.equipment_id
               AND e.category = i.product_category
             ORDER BY e.equip_name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    let names = result.column(0).unwrap();
    let quantities = result.column(1).unwrap();

    assert_eq!(names.get(0).unwrap().as_str().unwrap(), "Anchor");
    assert_eq!(quantities.get(0).unwrap().as_i64().unwrap(), 10);
    assert_eq!(names.get(1).unwrap().as_str().unwrap(), "GPS");
    assert_eq!(quantities.get(1).unwrap().as_i64().unwrap(), 5);
    assert_eq!(names.get(2).unwrap().as_str().unwrap(), "Rope");
    assert_eq!(quantities.get(2).unwrap().as_i64().unwrap(), 25);
}

#[test]
fn test_left_join_basic() {
    let mut executor = create_executor();
    setup_yacht_owners_orders(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT y.name AS owner_name, o.amount
             FROM yacht_owners y
             LEFT JOIN orders o ON y.id = o.owner_id
             ORDER BY o.amount",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);

    let names = result.column(0).unwrap();
    let amounts = result.column(1).unwrap();

    let mut alice_count = 0;
    let mut bob_count = 0;
    let mut charlie_count = 0;
    let mut null_amount_found = false;

    for i in 0..4 {
        let name_val = names.get(i).unwrap();
        let name = name_val.as_str().unwrap();
        match name {
            "Alice" => alice_count += 1,
            "Bob" => bob_count += 1,
            "Charlie" => {
                charlie_count += 1;
                assert!(
                    amounts.get(i).unwrap().is_null(),
                    "Charlie should have NULL amount"
                );
                null_amount_found = true;
            }
            _ => panic!("Unexpected name: {}", name),
        }
    }

    assert_eq!(alice_count, 2, "Alice should have 2 orders");
    assert_eq!(bob_count, 1, "Bob should have 1 order");
    assert_eq!(
        charlie_count, 1,
        "Charlie should have 1 row (with no orders)"
    );
    assert!(
        null_amount_found,
        "Should have found Charlie with NULL amount"
    );
}

#[test]
fn test_left_join_null_check() {
    let mut executor = create_executor();
    setup_yacht_owners_orders(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT y.name AS owner_name
             FROM yacht_owners y
             LEFT JOIN orders o ON y.id = o.owner_id
             WHERE o.order_id IS NULL",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let names = result.column(0).unwrap();
    assert_eq!(names.get(0).unwrap().as_str().unwrap(), "Charlie");
}

fn setup_crew_members_fleets(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS crew_members")
        .unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS fleets").unwrap();

    executor
        .execute_sql(
            "CREATE TABLE crew_members (crew_id INT64, crew_name STRING, manager_id INT64)",
        )
        .unwrap();
    executor
        .execute_sql("CREATE TABLE fleets (dept_id INT64, crew_member_id INT64, dept_name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO crew_members VALUES (1, 'Alice', NULL), (2, 'Bob', 1)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO fleets VALUES (1, 1, 'Engineering'), (2, 3, 'Sales')")
        .unwrap();
}

#[test]
fn test_right_join_basic() {
    let mut executor = create_executor();
    setup_crew_members_fleets(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT c.crew_name, f.dept_name
             FROM crew_members c
             RIGHT JOIN fleets f ON c.crew_id = f.crew_member_id
             ORDER BY f.dept_name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);

    let names = result.column(0).unwrap();
    let depts = result.column(1).unwrap();

    assert_eq!(depts.get(0).unwrap().as_str().unwrap(), "Engineering");
    assert_eq!(names.get(0).unwrap().as_str().unwrap(), "Alice");
    assert_eq!(depts.get(1).unwrap().as_str().unwrap(), "Sales");
    assert!(names.get(1).unwrap().is_null());
}

fn setup_partial_overlap_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS table_a")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS table_b")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE table_a (a_id INT64, a_value STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE table_b (b_id INT64, b_value STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO table_a VALUES (1, 'A1'), (2, 'A2')")
        .unwrap();

    executor
        .execute_sql("INSERT INTO table_b VALUES (2, 'B2'), (3, 'B3')")
        .unwrap();
}

#[test]
fn test_full_outer_join_basic() {
    let mut executor = create_executor();
    setup_partial_overlap_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT ta.a_value, tb.b_value
             FROM table_a ta
             FULL OUTER JOIN table_b tb ON ta.a_id = tb.b_id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    let a_values = result.column(0).unwrap();
    let b_values = result.column(1).unwrap();

    let mut a1_found = false;
    let mut a2_b2_found = false;
    let mut b3_found = false;

    for i in 0..3 {
        let a_val = a_values.get(i).unwrap();
        let b_val = b_values.get(i).unwrap();

        if a_val.as_str() == Some("A1") && b_val.is_null() {
            a1_found = true;
        } else if a_val.as_str() == Some("A2") && b_val.as_str() == Some("B2") {
            a2_b2_found = true;
        } else if a_val.is_null() && b_val.as_str() == Some("B3") {
            b3_found = true;
        }
    }

    assert!(a1_found, "Expected A1 | NULL row");
    assert!(a2_b2_found, "Expected A2 | B2 row");
    assert!(b3_found, "Expected NULL | B3 row");
}

fn setup_colors_sizes(executor: &mut QueryExecutor) {
    executor.execute_sql("DROP TABLE IF EXISTS colors").unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS sizes").unwrap();

    executor
        .execute_sql("CREATE TABLE colors (color STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE sizes (size STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO colors VALUES ('Red'), ('Blue')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sizes VALUES ('S'), ('M'), ('L')")
        .unwrap();
}

#[test]
fn test_cross_join_explicit() {
    let mut executor = create_executor();
    setup_colors_sizes(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT c.color, s.size
             FROM colors c
             CROSS JOIN sizes s
             ORDER BY c.color, s.size",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 6);

    let colors = result.column(0).unwrap();
    let sizes = result.column(1).unwrap();

    assert_eq!(colors.get(0).unwrap().as_str().unwrap(), "Blue");
    assert_eq!(sizes.get(0).unwrap().as_str().unwrap(), "L");
    assert_eq!(colors.get(1).unwrap().as_str().unwrap(), "Blue");
    assert_eq!(sizes.get(1).unwrap().as_str().unwrap(), "M");
    assert_eq!(colors.get(2).unwrap().as_str().unwrap(), "Blue");
    assert_eq!(sizes.get(2).unwrap().as_str().unwrap(), "S");
    assert_eq!(colors.get(3).unwrap().as_str().unwrap(), "Red");
    assert_eq!(sizes.get(3).unwrap().as_str().unwrap(), "L");
    assert_eq!(colors.get(4).unwrap().as_str().unwrap(), "Red");
    assert_eq!(sizes.get(4).unwrap().as_str().unwrap(), "M");
    assert_eq!(colors.get(5).unwrap().as_str().unwrap(), "Red");
    assert_eq!(sizes.get(5).unwrap().as_str().unwrap(), "S");
}

#[test]
fn test_cross_join_implicit() {
    let mut executor = create_executor();
    setup_colors_sizes(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT c.color, s.size
             FROM colors c, sizes s
             ORDER BY c.color, s.size",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 6);
}

#[test]
fn test_self_join_manager_hierarchy() {
    let mut executor = create_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS employees")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE employees (id INT64, name STRING, manager_id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO employees VALUES (1, 'Alice', NULL), (2, 'Bob', 1), (3, 'Charlie', 1)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT e.name AS employee, m.name AS manager
             FROM employees e
             LEFT JOIN employees m ON e.manager_id = m.id
             ORDER BY employee",
        )
        .expect("Self-join should succeed");

    assert_eq!(result.num_rows(), 3, "Should have 3 rows");

    let employees = result.column(0).unwrap();
    let managers = result.column(1).unwrap();

    assert_eq!(employees.get(0).unwrap().as_str().unwrap(), "Alice");
    assert!(
        managers.get(0).unwrap().is_null(),
        "Alice should have no manager"
    );

    assert_eq!(employees.get(1).unwrap().as_str().unwrap(), "Bob");
    assert_eq!(managers.get(1).unwrap().as_str().unwrap(), "Alice");

    assert_eq!(employees.get(2).unwrap().as_str().unwrap(), "Charlie");
    assert_eq!(managers.get(2).unwrap().as_str().unwrap(), "Alice");
}

#[test]
fn test_self_join_matching_pairs() {
    let mut executor = create_executor();

    executor
        .execute_sql("DROP TABLE IF EXISTS city_residents_a")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS city_residents_b")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE city_residents_a (aid INT64, city STRING, age INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE city_residents_b (bid INT64, city STRING, age INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO city_residents_a VALUES (1, 'NYC', 25), (3, 'LA', 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO city_residents_b VALUES (2, 'NYC', 25), (4, 'NYC', 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT a.aid AS id1, b.bid AS id2
             FROM city_residents_a a
             JOIN city_residents_b b
               ON a.city = b.city
               AND a.age = b.age
               AND a.aid < b.bid
             ORDER BY id1, id2",
        )
        .expect("JOIN to find pairs should succeed");

    assert_eq!(result.num_rows(), 1, "Should find exactly one pair");

    let id1 = result.column(0).unwrap();
    let id2 = result.column(1).unwrap();

    assert_eq!(id1.get(0).unwrap().as_i64().unwrap(), 1);
    assert_eq!(id2.get(0).unwrap().as_i64().unwrap(), 2);
}

#[test]
fn test_self_join_consecutive_rows() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE metrics_today (today_day INT64, metric_value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE metrics_tomorrow (tomorrow_day INT64, tomorrow_value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO metrics_today VALUES (1, 100), (2, 120), (3, 115)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO metrics_tomorrow VALUES (2, 120), (3, 115), (4, 130)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                    m1.today_day AS day_num,
                    m1.metric_value AS today_val,
                    m2.tomorrow_value AS tomorrow_val
             FROM metrics_today m1
             JOIN metrics_tomorrow m2 ON m2.tomorrow_day = m1.today_day + 1
             ORDER BY day_num",
        )
        .expect("JOIN for consecutive rows should succeed");

    assert_eq!(result.num_rows(), 3, "Should have 3 consecutive day pairs");

    let days = result.column(0).unwrap();
    let today_vals = result.column(1).unwrap();
    let tomorrow_vals = result.column(2).unwrap();

    assert_eq!(days.get(0).unwrap().as_i64().unwrap(), 1);
    assert_eq!(today_vals.get(0).unwrap().as_i64().unwrap(), 100);
    assert_eq!(tomorrow_vals.get(0).unwrap().as_i64().unwrap(), 120);

    assert_eq!(days.get(1).unwrap().as_i64().unwrap(), 2);
    assert_eq!(today_vals.get(1).unwrap().as_i64().unwrap(), 120);
    assert_eq!(tomorrow_vals.get(1).unwrap().as_i64().unwrap(), 115);

    assert_eq!(days.get(2).unwrap().as_i64().unwrap(), 3);
    assert_eq!(today_vals.get(2).unwrap().as_i64().unwrap(), 115);
    assert_eq!(tomorrow_vals.get(2).unwrap().as_i64().unwrap(), 130);
}

fn setup_multi_join_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS customers")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS customer_orders")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS products")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE customers (cust_id INT64, cust_name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE customer_orders (order_id INT64, cust_id INT64, prod_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE products (prod_id INT64, prod_name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO customer_orders VALUES (101, 1, 201), (102, 2, 202)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (201, 'Anchor'), (202, 'Compass')")
        .unwrap();
}

#[test]
fn test_multiple_inner_joins() {
    let mut executor = create_executor();
    setup_multi_join_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT c.cust_name, p.prod_name
             FROM customer_orders o
             INNER JOIN customers c ON o.cust_id = c.cust_id
             INNER JOIN products p ON o.prod_id = p.prod_id
             ORDER BY c.cust_name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);

    let cust_names = result.column(0).unwrap();
    let prod_names = result.column(1).unwrap();

    assert_eq!(cust_names.get(0).unwrap().as_str().unwrap(), "Alice");
    assert_eq!(prod_names.get(0).unwrap().as_str().unwrap(), "Anchor");
    assert_eq!(cust_names.get(1).unwrap().as_str().unwrap(), "Bob");
    assert_eq!(prod_names.get(1).unwrap().as_str().unwrap(), "Compass");
}

fn setup_mixed_join_tables(executor: &mut QueryExecutor) {
    executor.execute_sql("DROP TABLE IF EXISTS owners").unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS owner_orders")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS payments")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE owners (owner_id INT64, owner_name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE owner_orders (ord_id INT64, owner_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE payments (pay_id INT64, ord_id INT64, pay_amount FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO owners VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO owner_orders VALUES (101, 1), (102, 1), (103, 2)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO payments VALUES (1001, 101, 100.0), (1002, 103, 200.0)")
        .unwrap();
}

fn setup_join_using_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS customers_u")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS orders_u")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE customers_u (id INT64, cust_name STRING)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE orders_u (id INT64, order_amount FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO customers_u VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders_u VALUES (1, 100.0), (2, 200.0)")
        .unwrap();
}

#[test]
fn test_join_using_single_column() {
    let mut executor = create_executor();
    setup_join_using_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT c.cust_name, o.order_amount
             FROM customers_u c
             JOIN orders_u o USING (id)
             ORDER BY o.order_amount",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);

    let names = result.column(0).unwrap();
    let amounts = result.column(1).unwrap();

    assert_eq!(names.get(0).unwrap().as_str().unwrap(), "Alice");
    assert_eq!(amounts.get(0).unwrap().as_f64().unwrap(), 100.0);
    assert_eq!(names.get(1).unwrap().as_str().unwrap(), "Bob");
    assert_eq!(amounts.get(1).unwrap().as_f64().unwrap(), 200.0);
}

fn setup_join_using_multi_column(executor: &mut QueryExecutor) {
    executor.execute_sql("DROP TABLE IF EXISTS t1").unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS t2").unwrap();

    executor
        .execute_sql("CREATE TABLE t1 (a INT64, b INT64, val1 INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE t2 (a INT64, b INT64, val2 INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO t1 VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t2 VALUES (1, 10, 1000), (2, 20, 2000), (4, 40, 4000)")
        .unwrap();
}

#[test]
fn test_join_with_aggregation() {
    let mut executor = create_executor();
    setup_yacht_owners_orders(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT DISTINCT y.name AS owner_name
             FROM yacht_owners y
             INNER JOIN orders o ON y.id = o.owner_id
             ORDER BY owner_name",
        )
        .expect("JOIN with DISTINCT should succeed");

    let expected = build_batch(
        result.schema().clone(),
        vec![
            vec![Value::string("Alice".into())],
            vec![Value::string("Bob".into())],
        ],
    );

    assert_batch_eq(&result, &expected);
}

#[test]
fn test_join_with_order_by() {
    let mut executor = create_executor();
    setup_yacht_owners_orders(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT y.name AS owner_name, o.amount
             FROM yacht_owners y
             INNER JOIN orders o ON y.id = o.owner_id
             ORDER BY o.amount DESC",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    let names = result.column(0).unwrap();
    let amounts = result.column(1).unwrap();

    assert_eq!(amounts.get(0).unwrap().as_f64().unwrap(), 200.0);
    assert_eq!(names.get(0).unwrap().as_str().unwrap(), "Alice");
    assert_eq!(amounts.get(1).unwrap().as_f64().unwrap(), 150.0);
    assert_eq!(names.get(1).unwrap().as_str().unwrap(), "Bob");
    assert_eq!(amounts.get(2).unwrap().as_f64().unwrap(), 100.0);
    assert_eq!(names.get(2).unwrap().as_str().unwrap(), "Alice");
}

#[test]
fn test_join_with_limit() {
    let mut executor = create_executor();
    setup_yacht_owners_orders(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT y.name AS owner_name, o.amount
             FROM yacht_owners y
             INNER JOIN orders o ON y.id = o.owner_id
             ORDER BY o.amount
             LIMIT 2",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);

    let amounts = result.column(1).unwrap();

    assert_eq!(amounts.get(0).unwrap().as_f64().unwrap(), 100.0);
    assert_eq!(amounts.get(1).unwrap().as_f64().unwrap(), 150.0);
}

fn setup_prices_discounts(executor: &mut QueryExecutor) {
    executor.execute_sql("DROP TABLE IF EXISTS prices").unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS discounts")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE prices (price_id INT64, item STRING, price FLOAT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE discounts (disc_id INT64, min_price FLOAT64, max_price FLOAT64, discount FLOAT64)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO prices VALUES (1, 'Small Item', 10.0), (2, 'Medium Item', 50.0), (3, 'Large Item', 150.0)",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO discounts VALUES (1, 0.0, 25.0, 5.0), (2, 25.0, 100.0, 10.0), (3, 100.0, 200.0, 15.0)",
        )
        .unwrap();
}

#[test]
fn test_join_on_range_condition() {
    let mut executor = create_executor();
    setup_prices_discounts(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT p.item, p.price, d.discount
             FROM prices p
             JOIN discounts d
               ON p.price >= d.min_price
               AND p.price < d.max_price
             ORDER BY p.price",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    let items = result.column(0).unwrap();
    let prices = result.column(1).unwrap();
    let discounts = result.column(2).unwrap();

    assert_eq!(items.get(0).unwrap().as_str().unwrap(), "Small Item");
    assert_eq!(prices.get(0).unwrap().as_f64().unwrap(), 10.0);
    assert_eq!(discounts.get(0).unwrap().as_f64().unwrap(), 5.0);

    assert_eq!(items.get(1).unwrap().as_str().unwrap(), "Medium Item");
    assert_eq!(prices.get(1).unwrap().as_f64().unwrap(), 50.0);
    assert_eq!(discounts.get(1).unwrap().as_f64().unwrap(), 10.0);

    assert_eq!(items.get(2).unwrap().as_str().unwrap(), "Large Item");
    assert_eq!(prices.get(2).unwrap().as_f64().unwrap(), 150.0);
    assert_eq!(discounts.get(2).unwrap().as_f64().unwrap(), 15.0);
}

fn setup_computed_join_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS computed_t1")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS computed_t2")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE computed_t1 (t1_id INT64, t1_value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE computed_t2 (t2_id INT64, double_value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO computed_t1 VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO computed_t2 VALUES (1, 20), (2, 40), (3, 100)")
        .unwrap();
}

#[test]
fn test_join_on_computed_column() {
    let mut executor = create_executor();
    setup_computed_join_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT t1.t1_id, t1.t1_value, t2.double_value
             FROM computed_t1 t1
             JOIN computed_t2 t2 ON t2.double_value = t1.t1_value * 2
             ORDER BY t1.t1_id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);

    let ids = result.column(0).unwrap();
    let values = result.column(1).unwrap();
    let doubles = result.column(2).unwrap();

    assert_eq!(ids.get(0).unwrap().as_i64().unwrap(), 1);
    assert_eq!(values.get(0).unwrap().as_i64().unwrap(), 10);
    assert_eq!(doubles.get(0).unwrap().as_i64().unwrap(), 20);

    assert_eq!(ids.get(1).unwrap().as_i64().unwrap(), 2);
    assert_eq!(values.get(1).unwrap().as_i64().unwrap(), 20);
    assert_eq!(doubles.get(1).unwrap().as_i64().unwrap(), 40);
}

fn setup_email_domain_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS email_users")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS domains")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE email_users (user_id INT64, email STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE domains (domain_id INT64, domain STRING)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO email_users VALUES (1, 'alice@example.com'), (2, 'bob@test.org'), (3, 'charlie@example.com')",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO domains VALUES (1, 'example.com'), (2, 'other.net')")
        .unwrap();
}

#[test]
fn test_join_with_functions() {
    let mut executor = create_executor();
    setup_email_domain_tables(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT u.email, d.domain
             FROM email_users u
             JOIN domains d
               ON SUBSTRING(u.email, POSITION('@' IN u.email) + 1) = d.domain",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);

    let emails = result.column(0).unwrap();
    let domain_col = result.column(1).unwrap();

    let mut found_alice = false;
    let mut found_charlie = false;

    for i in 0..2 {
        let email_val = emails.get(i).unwrap();
        let email = email_val.as_str().unwrap();
        let domain_val = domain_col.get(i).unwrap();
        let domain = domain_val.as_str().unwrap();
        assert_eq!(domain, "example.com");

        if email == "alice@example.com" {
            found_alice = true;
        } else if email == "charlie@example.com" {
            found_charlie = true;
        }
    }

    assert!(found_alice, "alice@example.com should be in result");
    assert!(found_charlie, "charlie@example.com should be in result");
}

fn setup_empty_table_join(executor: &mut QueryExecutor) {
    executor
        .execute_sql("DROP TABLE IF EXISTS nonempty_table")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS empty_table")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE nonempty_table (ne_id INT64, ne_name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE empty_table (e_id INT64, e_value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO nonempty_table VALUES (1, 'One'), (2, 'Two')")
        .unwrap();
}

#[test]
fn test_inner_join_with_empty_table() {
    let mut executor = create_executor();
    setup_empty_table_join(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT ne.ne_id, ne.ne_name, e.e_id, e.e_value
             FROM nonempty_table ne
             INNER JOIN empty_table e ON ne.ne_id = e.e_id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_left_join_with_empty_table() {
    let mut executor = create_executor();
    setup_empty_table_join(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT ne.ne_id, ne.ne_name, e.e_id, e.e_value
             FROM nonempty_table ne
             LEFT JOIN empty_table e ON ne.ne_id = e.e_id
             ORDER BY ne.ne_id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);

    let ids = result.column(0).unwrap();
    let names = result.column(1).unwrap();
    let e_ids = result.column(2).unwrap();
    let values = result.column(3).unwrap();

    assert_eq!(ids.get(0).unwrap().as_i64().unwrap(), 1);
    assert_eq!(names.get(0).unwrap().as_str().unwrap(), "One");
    assert!(e_ids.get(0).unwrap().is_null());
    assert!(values.get(0).unwrap().is_null());

    assert_eq!(ids.get(1).unwrap().as_i64().unwrap(), 2);
    assert_eq!(names.get(1).unwrap().as_str().unwrap(), "Two");
    assert!(e_ids.get(1).unwrap().is_null());
    assert!(values.get(1).unwrap().is_null());
}
