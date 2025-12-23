use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

fn setup_tables(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE sales (id INT64, employee STRING, department STRING, amount INT64, sale_date DATE)")
        .unwrap();
    session
        .execute_sql("INSERT INTO sales VALUES (1, 'Alice', 'Electronics', 1000, '2024-01-01'), (2, 'Bob', 'Electronics', 1500, '2024-01-02'), (3, 'Alice', 'Electronics', 2000, '2024-01-03'), (4, 'Charlie', 'Clothing', 800, '2024-01-01'), (5, 'Diana', 'Clothing', 1200, '2024-01-02')")
        .unwrap();
}

#[test]
fn test_row_number() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn FROM sales ORDER BY rn",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, 1],
            ["Bob", 1500, 2],
            ["Diana", 1200, 3],
            ["Alice", 1000, 4],
            ["Charlie", 800, 5],
        ]
    );
}

#[test]
fn test_row_number_with_partition() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, ROW_NUMBER() OVER (PARTITION BY department ORDER BY amount DESC) AS rn FROM sales ORDER BY department, rn",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Diana", "Clothing", 1200, 1],
            ["Charlie", "Clothing", 800, 2],
            ["Alice", "Electronics", 2000, 1],
            ["Bob", "Electronics", 1500, 2],
            ["Alice", "Electronics", 1000, 3],
        ]
    );
}

#[test]
fn test_rank() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES ('A', 100), ('B', 100), ('C', 90), ('D', 80)")
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT name, score, RANK() OVER (ORDER BY score DESC) AS rank FROM scores ORDER BY rank, name",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [["A", 100, 1], ["B", 100, 1], ["C", 90, 3], ["D", 80, 4],]
    );
}

#[test]
fn test_dense_rank() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE scores (name STRING, score INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO scores VALUES ('A', 100), ('B', 100), ('C', 90), ('D', 80)")
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drank FROM scores ORDER BY drank, name",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [["A", 100, 1], ["B", 100, 1], ["C", 90, 2], ["D", 80, 3],]
    );
}

#[test]
fn test_ntile() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, NTILE(2) OVER (ORDER BY amount DESC) AS bucket FROM sales ORDER BY bucket, amount DESC",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, 1],
            ["Bob", 1500, 1],
            ["Diana", 1200, 1],
            ["Alice", 1000, 2],
            ["Charlie", 800, 2],
        ]
    );
}

#[test]
fn test_lag() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, LAG(amount) OVER (ORDER BY id) AS prev_amount FROM sales ORDER BY id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, null],
            ["Bob", 1500, 1000],
            ["Alice", 2000, 1500],
            ["Charlie", 800, 2000],
            ["Diana", 1200, 800],
        ]
    );
}

#[test]
fn test_lag_with_offset() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, LAG(amount, 2) OVER (ORDER BY id) AS prev2_amount FROM sales ORDER BY id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, null],
            ["Bob", 1500, null],
            ["Alice", 2000, 1000],
            ["Charlie", 800, 1500],
            ["Diana", 1200, 2000],
        ]
    );
}

#[test]
fn test_lag_with_default() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, LAG(amount, 1, 0) OVER (ORDER BY id) AS prev_amount FROM sales ORDER BY id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 0],
            ["Bob", 1500, 1000],
            ["Alice", 2000, 1500],
            ["Charlie", 800, 2000],
            ["Diana", 1200, 800],
        ]
    );
}

#[test]
fn test_lead() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, LEAD(amount) OVER (ORDER BY id) AS next_amount FROM sales ORDER BY id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1500],
            ["Bob", 1500, 2000],
            ["Alice", 2000, 800],
            ["Charlie", 800, 1200],
            ["Diana", 1200, null],
        ]
    );
}

#[test]
fn test_first_value() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, FIRST_VALUE(employee) OVER (PARTITION BY department ORDER BY amount DESC) AS top_seller FROM sales ORDER BY department, id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, "Diana"],
            ["Diana", "Clothing", 1200, "Diana"],
            ["Alice", "Electronics", 1000, "Alice"],
            ["Bob", "Electronics", 1500, "Alice"],
            ["Alice", "Electronics", 2000, "Alice"],
        ]
    );
}

#[test]
fn test_last_value() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, LAST_VALUE(employee) OVER (PARTITION BY department ORDER BY amount DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lowest_seller FROM sales ORDER BY department, id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, "Charlie"],
            ["Diana", "Clothing", 1200, "Charlie"],
            ["Alice", "Electronics", 1000, "Alice"],
            ["Bob", "Electronics", 1500, "Alice"],
            ["Alice", "Electronics", 2000, "Alice"],
        ]
    );
}

#[test]
fn test_sum_over() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER (ORDER BY id) AS running_total FROM sales ORDER BY id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 1000],
            ["Bob", 1500, 2500],
            ["Alice", 2000, 4500],
            ["Charlie", 800, 5300],
            ["Diana", 1200, 6500],
        ]
    );
}

#[test]
fn test_avg_over_partition() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, AVG(amount) OVER (PARTITION BY department) AS dept_avg FROM sales ORDER BY department, id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", "Clothing", 800, 1000.0],
            ["Diana", "Clothing", 1200, 1000.0],
            ["Alice", "Electronics", 1000, 1500.0],
            ["Bob", "Electronics", 1500, 1500.0],
            ["Alice", "Electronics", 2000, 1500.0],
        ]
    );
}

#[test]
fn test_count_over() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, COUNT(*) OVER (PARTITION BY employee) AS sale_count FROM sales ORDER BY employee, id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2],
            ["Alice", 2],
            ["Bob", 1],
            ["Charlie", 1],
            ["Diana", 1],
        ]
    );
}

#[test]
fn test_min_max_over() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, MIN(amount) OVER () AS min_sale, MAX(amount) OVER () AS max_sale FROM sales ORDER BY id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 800, 2000],
            ["Bob", 1500, 800, 2000],
            ["Alice", 2000, 800, 2000],
            ["Charlie", 800, 800, 2000],
            ["Diana", 1200, 800, 2000],
        ]
    );
}

#[test]
fn test_window_frame_rows() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS window_sum FROM sales ORDER BY id",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 1000, 2500],
            ["Bob", 1500, 4500],
            ["Alice", 2000, 4300],
            ["Charlie", 800, 4000],
            ["Diana", 1200, 2000],
        ]
    );
}

#[test]
fn test_window_frame_range() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER (ORDER BY amount RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumsum FROM sales ORDER BY amount",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", 800, 800],
            ["Alice", 1000, 1800],
            ["Diana", 1200, 3000],
            ["Bob", 1500, 4500],
            ["Alice", 2000, 6500],
        ]
    );
}

#[test]
fn test_multiple_window_functions() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn, RANK() OVER (ORDER BY amount DESC) AS rnk, SUM(amount) OVER () AS total FROM sales ORDER BY rn",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Alice", 2000, 1, 1, 6500],
            ["Bob", 1500, 2, 2, 6500],
            ["Diana", 1200, 3, 3, 6500],
            ["Alice", 1000, 4, 4, 6500],
            ["Charlie", 800, 5, 5, 6500],
        ]
    );
}

#[test]
fn test_percent_rank() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, PERCENT_RANK() OVER (ORDER BY amount) AS prank FROM sales ORDER BY amount",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", 800, 0.0],
            ["Alice", 1000, 0.25],
            ["Diana", 1200, 0.5],
            ["Bob", 1500, 0.75],
            ["Alice", 2000, 1.0],
        ]
    );
}

#[test]
fn test_cume_dist() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, CUME_DIST() OVER (ORDER BY amount) AS cdist FROM sales ORDER BY amount",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", 800, 0.2],
            ["Alice", 1000, 0.4],
            ["Diana", 1200, 0.6],
            ["Bob", 1500, 0.8],
            ["Alice", 2000, 1.0],
        ]
    );
}

#[test]
fn test_named_window() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER w AS running_sum
             FROM sales
             WINDOW w AS (ORDER BY amount)
             ORDER BY amount",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", 800, 800],
            ["Alice", 1000, 1800],
            ["Diana", 1200, 3000],
            ["Bob", 1500, 4500],
            ["Alice", 2000, 6500],
        ]
    );
}

#[test]
fn test_named_window_with_partition() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, department, amount, ROW_NUMBER() OVER w AS rn
             FROM sales
             WINDOW w AS (PARTITION BY department ORDER BY amount DESC)
             ORDER BY department, rn",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Diana", "Clothing", 1200, 1],
            ["Charlie", "Clothing", 800, 2],
            ["Alice", "Electronics", 2000, 1],
            ["Bob", "Electronics", 1500, 2],
            ["Alice", "Electronics", 1000, 3],
        ]
    );
}

#[test]
fn test_named_window_with_rows_between() {
    let mut session = create_session();
    setup_tables(&mut session);

    let result = session
        .execute_sql(
            "SELECT employee, amount, SUM(amount) OVER w AS rolling_sum
             FROM sales
             WINDOW w AS (ORDER BY amount ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
             ORDER BY amount",
        )
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["Charlie", 800, 800],
            ["Alice", 1000, 1800],
            ["Diana", 1200, 2200],
            ["Bob", 1500, 2700],
            ["Alice", 2000, 3500],
        ]
    );
}
