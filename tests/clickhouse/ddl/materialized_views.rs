use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_materialized_view_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mv_source (id Int64, value Int64)")
        .unwrap();

    let result = executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_basic
            ENGINE = MergeTree ORDER BY id
            AS SELECT id, value * 2 AS doubled FROM mv_source",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_create_materialized_view_to_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mv_src (event_time DateTime, user_id Int64, action String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE mv_dest (
                event_date Date,
                user_count Int64
            ) ENGINE = SummingMergeTree ORDER BY event_date",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_to_table TO mv_dest
            AS SELECT
                toDate(event_time) AS event_date,
                count() AS user_count
            FROM mv_src
            GROUP BY event_date",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_materialized_view_aggregation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (sale_date Date, product_id Int64, amount Float64)")
        .unwrap();

    let result = executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW daily_sales
            ENGINE = SummingMergeTree ORDER BY (sale_date, product_id)
            AS SELECT
                sale_date,
                product_id,
                sum(amount) AS total_amount,
                count() AS sale_count
            FROM sales
            GROUP BY sale_date, product_id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_materialized_view_insert_propagation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE events (ts DateTime, user_id Int64, event String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW event_counts
            ENGINE = SummingMergeTree ORDER BY event
            AS SELECT event, count() AS cnt FROM events GROUP BY event",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO events VALUES
            ('2023-01-01 10:00:00', 1, 'click'),
            ('2023-01-01 10:01:00', 2, 'view'),
            ('2023-01-01 10:02:00', 1, 'click')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM event_counts ORDER BY event")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[test]
fn test_materialized_view_populate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mv_pop_src (id Int64, val Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mv_pop_src VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_populated
            ENGINE = MergeTree ORDER BY id
            POPULATE
            AS SELECT id, val FROM mv_pop_src",
        )
        .unwrap();
    assert_table_eq!(result, []);

    let view_data = executor
        .execute_sql("SELECT * FROM mv_populated ORDER BY id")
        .unwrap();
    assert_eq!(view_data.num_rows(), 3);
}

#[test]
fn test_drop_materialized_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mv_drop_src (x Int64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_to_drop
            ENGINE = MergeTree ORDER BY x
            AS SELECT x FROM mv_drop_src",
        )
        .unwrap();

    let result = executor.execute_sql("DROP VIEW mv_to_drop").unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_alter_materialized_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mv_alter_src (id Int64, name String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_to_alter
            ENGINE = MergeTree ORDER BY id
            AS SELECT id, name FROM mv_alter_src",
        )
        .unwrap();

    let result = executor
        .execute_sql("ALTER TABLE mv_to_alter MODIFY SETTING index_granularity = 1024")
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_materialized_view_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE logs (ts DateTime, level String, message String)")
        .unwrap();

    let result = executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW error_logs
            ENGINE = MergeTree ORDER BY ts
            AS SELECT ts, message FROM logs WHERE level = 'ERROR'",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_materialized_view_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE orders (order_id Int64, customer_id Int64, amount Float64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE customers (id Int64, name String)")
        .unwrap();

    let result = executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW order_summary
            ENGINE = MergeTree ORDER BY customer_id
            AS SELECT
                o.customer_id,
                c.name,
                sum(o.amount) AS total
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            GROUP BY o.customer_id, c.name",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_materialized_view_chain() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE raw_events (ts DateTime, data String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW processed_events
            ENGINE = MergeTree ORDER BY ts
            AS SELECT ts, lower(data) AS normalized FROM raw_events",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW event_stats
            ENGINE = SummingMergeTree ORDER BY hour
            AS SELECT
                toStartOfHour(ts) AS hour,
                count() AS cnt
            FROM processed_events
            GROUP BY hour",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_show_create_materialized_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE show_mv_src (x Int64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW show_mv
            ENGINE = MergeTree ORDER BY x
            AS SELECT x FROM show_mv_src",
        )
        .unwrap();

    let result = executor.execute_sql("SHOW CREATE TABLE show_mv").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_materialized_view_definer() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE definer_src (id Int64)")
        .unwrap();

    let result = executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW definer_mv
            DEFINER = CURRENT_USER
            ENGINE = MergeTree ORDER BY id
            AS SELECT id FROM definer_src",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_refreshable_materialized_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE refresh_src (id Int64, val Int64)")
        .unwrap();

    let result = executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW refresh_mv
            REFRESH EVERY 1 HOUR
            ENGINE = MergeTree ORDER BY id
            AS SELECT id, sum(val) AS total FROM refresh_src GROUP BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}
