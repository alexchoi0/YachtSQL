use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_lag_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sales (month INT64, revenue INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (2, 150)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (3, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (4, 175)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT month, revenue, LAG(revenue, 1) OVER (ORDER BY month) as prev_revenue
             FROM sales ORDER BY month",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);

    let month1_prev = result.column(2).unwrap().get(0).unwrap();
    assert!(
        month1_prev.is_null(),
        "Month 1 should have NULL prev_revenue"
    );

    let month2_prev = result.column(2).unwrap().get(1).unwrap();
    assert_eq!(month2_prev.as_i64().unwrap(), 100);

    let month3_prev = result.column(2).unwrap().get(2).unwrap();
    assert_eq!(month3_prev.as_i64().unwrap(), 150);

    let month4_prev = result.column(2).unwrap().get(3).unwrap();
    assert_eq!(month4_prev.as_i64().unwrap(), 200);
}

#[test]
fn test_lag_offset_2() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (3, 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (4, 40)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (5, 50)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value, LAG(value, 2) OVER (ORDER BY id) as lag2
             FROM data ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 5);

    assert!(result.column(2).unwrap().get(0).unwrap().is_null());
    assert!(result.column(2).unwrap().get(1).unwrap().is_null());

    assert_eq!(
        result.column(2).unwrap().get(2).unwrap().as_i64().unwrap(),
        10
    );

    assert_eq!(
        result.column(2).unwrap().get(3).unwrap().as_i64().unwrap(),
        20
    );

    assert_eq!(
        result.column(2).unwrap().get(4).unwrap().as_i64().unwrap(),
        30
    );
}

#[test]
fn test_lag_with_default() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE temps (day INT64, temp INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO temps VALUES (1, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO temps VALUES (2, 22)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO temps VALUES (3, 25)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT day, temp, LAG(temp, 1, 0) OVER (ORDER BY day) as prev_temp
             FROM temps ORDER BY day",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    let day1_prev = result.column(2).unwrap().get(0).unwrap();
    assert_eq!(day1_prev.as_i64().unwrap(), 0);

    let day2_prev = result.column(2).unwrap().get(1).unwrap();
    assert_eq!(day2_prev.as_i64().unwrap(), 20);

    let day3_prev = result.column(2).unwrap().get(2).unwrap();
    assert_eq!(day3_prev.as_i64().unwrap(), 22);
}

#[test]
fn test_lag_with_partition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE metrics (region STRING, month INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO metrics VALUES ('North', 1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO metrics VALUES ('North', 2, 150)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO metrics VALUES ('South', 1, 80)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO metrics VALUES ('South', 2, 90)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, month, value,
                    LAG(value, 1) OVER (PARTITION BY region ORDER BY month) as prev_value
             FROM metrics ORDER BY region, month",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);

    assert!(result.column(3).unwrap().get(0).unwrap().is_null());

    assert_eq!(
        result.column(3).unwrap().get(1).unwrap().as_i64().unwrap(),
        100
    );

    assert!(result.column(3).unwrap().get(2).unwrap().is_null());

    assert_eq!(
        result.column(3).unwrap().get(3).unwrap().as_i64().unwrap(),
        80
    );
}

#[test]
fn test_lag_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value, LAG(value, 1) OVER (ORDER BY id) as prev_value
             FROM data ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    assert!(result.column(2).unwrap().get(0).unwrap().is_null());

    assert_eq!(
        result.column(2).unwrap().get(1).unwrap().as_i64().unwrap(),
        10
    );

    assert!(result.column(2).unwrap().get(2).unwrap().is_null());
}

#[test]
fn test_lead_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sales (month INT64, revenue INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (2, 150)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (3, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (4, 175)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT month, revenue, LEAD(revenue, 1) OVER (ORDER BY month) as next_revenue
             FROM sales ORDER BY month",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);

    let month1_next = result.column(2).unwrap().get(0).unwrap();
    assert_eq!(month1_next.as_i64().unwrap(), 150);

    let month2_next = result.column(2).unwrap().get(1).unwrap();
    assert_eq!(month2_next.as_i64().unwrap(), 200);

    let month3_next = result.column(2).unwrap().get(2).unwrap();
    assert_eq!(month3_next.as_i64().unwrap(), 175);

    let month4_next = result.column(2).unwrap().get(3).unwrap();
    assert!(
        month4_next.is_null(),
        "Last month should have NULL next_revenue"
    );
}

#[test]
fn test_lead_offset_2() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (3, 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (4, 40)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (5, 50)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value, LEAD(value, 2) OVER (ORDER BY id) as lead2
             FROM data ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 5);

    assert_eq!(
        result.column(2).unwrap().get(0).unwrap().as_i64().unwrap(),
        30
    );

    assert_eq!(
        result.column(2).unwrap().get(1).unwrap().as_i64().unwrap(),
        40
    );

    assert_eq!(
        result.column(2).unwrap().get(2).unwrap().as_i64().unwrap(),
        50
    );

    assert!(result.column(2).unwrap().get(3).unwrap().is_null());
    assert!(result.column(2).unwrap().get(4).unwrap().is_null());
}

#[test]
fn test_lead_with_default() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sequence (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sequence VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sequence VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sequence VALUES (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value, LEAD(value, 1, -1) OVER (ORDER BY id) as next_value
             FROM sequence ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    assert_eq!(
        result.column(2).unwrap().get(0).unwrap().as_i64().unwrap(),
        20
    );

    assert_eq!(
        result.column(2).unwrap().get(1).unwrap().as_i64().unwrap(),
        30
    );

    assert_eq!(
        result.column(2).unwrap().get(2).unwrap().as_i64().unwrap(),
        -1
    );
}

#[test]
fn test_lead_with_partition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE metrics (region STRING, month INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO metrics VALUES ('North', 1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO metrics VALUES ('North', 2, 150)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO metrics VALUES ('South', 1, 80)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO metrics VALUES ('South', 2, 90)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, month, value,
                    LEAD(value, 1) OVER (PARTITION BY region ORDER BY month) as next_value
             FROM metrics ORDER BY region, month",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);

    assert_eq!(
        result.column(3).unwrap().get(0).unwrap().as_i64().unwrap(),
        150
    );

    assert!(result.column(3).unwrap().get(1).unwrap().is_null());

    assert_eq!(
        result.column(3).unwrap().get(2).unwrap().as_i64().unwrap(),
        90
    );

    assert!(result.column(3).unwrap().get(3).unwrap().is_null());
}

#[test]
fn test_first_value_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (3, 150)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (4, 300)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value, FIRST_VALUE(value) OVER (ORDER BY id) as first_val
             FROM data ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);

    for i in 0..4 {
        let first_val = result.column(2).unwrap().get(i).unwrap();
        assert_eq!(first_val.as_i64().unwrap(), 100);
    }
}

#[test]
fn test_first_value_with_partition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sales (region STRING, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES ('North', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('North', 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('South', 150)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('South', 250)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, amount,
                    FIRST_VALUE(amount) OVER (PARTITION BY region ORDER BY amount) as first_amount
             FROM sales ORDER BY region, amount",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);

    assert_eq!(
        result.column(2).unwrap().get(0).unwrap().as_i64().unwrap(),
        100
    );
    assert_eq!(
        result.column(2).unwrap().get(1).unwrap().as_i64().unwrap(),
        100
    );

    assert_eq!(
        result.column(2).unwrap().get(2).unwrap().as_i64().unwrap(),
        150
    );
    assert_eq!(
        result.column(2).unwrap().get(3).unwrap().as_i64().unwrap(),
        150
    );
}

#[test]
fn test_first_value_with_frame() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (3, 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (4, 40)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (5, 50)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value,
                    FIRST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as first_in_frame
             FROM data ORDER BY id"
        )
        .unwrap();

    assert_eq!(result.num_rows(), 5);

    assert_eq!(
        result.column(2).unwrap().get(0).unwrap().as_i64().unwrap(),
        10
    );

    assert_eq!(
        result.column(2).unwrap().get(1).unwrap().as_i64().unwrap(),
        10
    );

    assert_eq!(
        result.column(2).unwrap().get(2).unwrap().as_i64().unwrap(),
        20
    );

    assert_eq!(
        result.column(2).unwrap().get(3).unwrap().as_i64().unwrap(),
        30
    );

    assert_eq!(
        result.column(2).unwrap().get(4).unwrap().as_i64().unwrap(),
        40
    );
}

#[test]
fn test_last_value_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (3, 150)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (4, 300)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value,
                    LAST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_val
             FROM data ORDER BY id"
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);

    for i in 0..4 {
        let last_val = result.column(2).unwrap().get(i).unwrap();
        assert_eq!(last_val.as_i64().unwrap(), 300);
    }
}

#[test]
fn test_last_value_current_row_frame() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value, LAST_VALUE(value) OVER (ORDER BY id) as last_val
             FROM data ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    assert_eq!(
        result.column(2).unwrap().get(0).unwrap().as_i64().unwrap(),
        10
    );
    assert_eq!(
        result.column(2).unwrap().get(1).unwrap().as_i64().unwrap(),
        20
    );
    assert_eq!(
        result.column(2).unwrap().get(2).unwrap().as_i64().unwrap(),
        30
    );
}

#[test]
fn test_last_value_with_frame() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (3, 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (4, 40)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (5, 50)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value,
                    LAST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as last_in_frame
             FROM data ORDER BY id"
        )
        .unwrap();

    assert_eq!(result.num_rows(), 5);

    assert_eq!(
        result.column(2).unwrap().get(0).unwrap().as_i64().unwrap(),
        20
    );

    assert_eq!(
        result.column(2).unwrap().get(1).unwrap().as_i64().unwrap(),
        30
    );

    assert_eq!(
        result.column(2).unwrap().get(2).unwrap().as_i64().unwrap(),
        40
    );

    assert_eq!(
        result.column(2).unwrap().get(3).unwrap().as_i64().unwrap(),
        50
    );

    assert_eq!(
        result.column(2).unwrap().get(4).unwrap().as_i64().unwrap(),
        50
    );
}

#[test]
fn test_last_value_with_partition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sales (region STRING, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sales VALUES ('North', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('North', 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('South', 150)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('South', 250)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, amount,
                    LAST_VALUE(amount) OVER (
                        PARTITION BY region
                        ORDER BY amount
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as last_amount
             FROM sales ORDER BY region, amount",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4);

    assert_eq!(
        result.column(2).unwrap().get(0).unwrap().as_i64().unwrap(),
        200
    );
    assert_eq!(
        result.column(2).unwrap().get(1).unwrap().as_i64().unwrap(),
        200
    );

    assert_eq!(
        result.column(2).unwrap().get(2).unwrap().as_i64().unwrap(),
        250
    );
    assert_eq!(
        result.column(2).unwrap().get(3).unwrap().as_i64().unwrap(),
        250
    );
}

#[test]
fn test_nth_value_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (3, 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (4, 40)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (5, 50)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value, NTH_VALUE(value, 3) OVER (ORDER BY id) as third_val
             FROM data ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 5);

    assert!(result.column(2).unwrap().get(0).unwrap().is_null());
    assert!(result.column(2).unwrap().get(1).unwrap().is_null());

    assert_eq!(
        result.column(2).unwrap().get(2).unwrap().as_i64().unwrap(),
        30
    );
    assert_eq!(
        result.column(2).unwrap().get(3).unwrap().as_i64().unwrap(),
        30
    );
    assert_eq!(
        result.column(2).unwrap().get(4).unwrap().as_i64().unwrap(),
        30
    );
}

#[test]
fn test_nth_value_with_frame() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    for i in 1..=6 {
        executor
            .execute_sql(&format!("INSERT INTO data VALUES ({}, {})", i, i * 10))
            .unwrap();
    }

    let result = executor
        .execute_sql(
            "SELECT id, value,
                    NTH_VALUE(value, 2) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) as second_in_frame
             FROM data ORDER BY id"
        )
        .unwrap();

    assert_eq!(result.num_rows(), 6);

    assert_eq!(
        result.column(2).unwrap().get(0).unwrap().as_i64().unwrap(),
        20
    );

    assert_eq!(
        result.column(2).unwrap().get(1).unwrap().as_i64().unwrap(),
        20
    );

    assert_eq!(
        result.column(2).unwrap().get(2).unwrap().as_i64().unwrap(),
        30
    );

    assert_eq!(
        result.column(2).unwrap().get(3).unwrap().as_i64().unwrap(),
        40
    );
}

#[test]
fn test_lag_lead_combined() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sequence (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO sequence VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sequence VALUES (2, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sequence VALUES (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value,
                    LAG(value, 1) OVER (ORDER BY id) as prev,
                    LEAD(value, 1) OVER (ORDER BY id) as next
             FROM sequence ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    assert!(result.column(2).unwrap().get(0).unwrap().is_null());
    assert_eq!(
        result.column(3).unwrap().get(0).unwrap().as_i64().unwrap(),
        200
    );

    assert_eq!(
        result.column(2).unwrap().get(1).unwrap().as_i64().unwrap(),
        100
    );
    assert_eq!(
        result.column(3).unwrap().get(1).unwrap().as_i64().unwrap(),
        300
    );

    assert_eq!(
        result.column(2).unwrap().get(2).unwrap().as_i64().unwrap(),
        200
    );
    assert!(result.column(3).unwrap().get(2).unwrap().is_null());
}

#[test]
fn test_first_last_combined() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO data VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, value,
                    FIRST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first,
                    LAST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last
             FROM data ORDER BY id"
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    for i in 0..3 {
        assert_eq!(
            result.column(2).unwrap().get(i).unwrap().as_i64().unwrap(),
            10
        );
        assert_eq!(
            result.column(3).unwrap().get(i).unwrap().as_i64().unwrap(),
            30
        );
    }
}
