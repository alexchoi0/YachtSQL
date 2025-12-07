use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_asof_join_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE quotes (
                symbol String,
                ts DateTime,
                price Float64
            ) ENGINE = MergeTree ORDER BY (symbol, ts)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE trades (
                symbol String,
                ts DateTime,
                quantity Int64
            ) ENGINE = MergeTree ORDER BY (symbol, ts)",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO quotes VALUES
            ('AAPL', '2023-01-01 10:00:00', 150.00),
            ('AAPL', '2023-01-01 10:01:00', 150.50),
            ('AAPL', '2023-01-01 10:02:00', 151.00)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO trades VALUES
            ('AAPL', '2023-01-01 10:00:30', 100),
            ('AAPL', '2023-01-01 10:01:15', 200)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT t.symbol, t.ts, t.quantity, q.price
            FROM trades t
            ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts
            ORDER BY t.ts",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_asof_left_join() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE events (
                id Int64,
                event_time DateTime
            ) ENGINE = MergeTree ORDER BY event_time",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE states (
                state_time DateTime,
                state String
            ) ENGINE = MergeTree ORDER BY state_time",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO events VALUES
            (1, '2023-01-01 10:00:00'),
            (2, '2023-01-01 11:00:00'),
            (3, '2023-01-01 12:00:00')",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO states VALUES
            ('2023-01-01 09:00:00', 'initial'),
            ('2023-01-01 10:30:00', 'active'),
            ('2023-01-01 14:00:00', 'completed')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT e.id, e.event_time, s.state
            FROM events e
            ASOF LEFT JOIN states s ON e.event_time >= s.state_time
            ORDER BY e.id",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_asof_join_with_using() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE sensor_readings (
                sensor_id String,
                ts DateTime,
                value Float64
            ) ENGINE = MergeTree ORDER BY (sensor_id, ts)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE calibrations (
                sensor_id String,
                ts DateTime,
                offset Float64
            ) ENGINE = MergeTree ORDER BY (sensor_id, ts)",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO sensor_readings VALUES
            ('S1', '2023-01-01 10:00:00', 25.5),
            ('S1', '2023-01-01 11:00:00', 26.0),
            ('S2', '2023-01-01 10:00:00', 30.0)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO calibrations VALUES
            ('S1', '2023-01-01 09:00:00', 0.5),
            ('S2', '2023-01-01 08:00:00', 0.3)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT r.sensor_id, r.ts, r.value, c.offset, r.value + c.offset AS calibrated
            FROM sensor_readings r
            ASOF JOIN calibrations c USING (sensor_id, ts)
            ORDER BY r.sensor_id, r.ts",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_asof_join_strict() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE prices (
                product_id Int64,
                valid_from DateTime,
                price Float64
            ) ENGINE = MergeTree ORDER BY (product_id, valid_from)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE orders (
                order_id Int64,
                product_id Int64,
                order_time DateTime
            ) ENGINE = MergeTree ORDER BY (product_id, order_time)",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO prices VALUES
            (1, '2023-01-01 00:00:00', 10.00),
            (1, '2023-01-15 00:00:00', 12.00),
            (2, '2023-01-01 00:00:00', 20.00)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO orders VALUES
            (100, 1, '2023-01-10 12:00:00'),
            (101, 1, '2023-01-20 12:00:00'),
            (102, 2, '2023-01-05 12:00:00')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT o.order_id, o.product_id, o.order_time, p.price
            FROM orders o
            ASOF JOIN prices p
            ON o.product_id = p.product_id AND o.order_time >= p.valid_from
            ORDER BY o.order_id",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_asof_join_less_than() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE forward_events (
                id Int64,
                ts DateTime
            ) ENGINE = MergeTree ORDER BY ts",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE future_states (
                ts DateTime,
                state String
            ) ENGINE = MergeTree ORDER BY ts",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO forward_events VALUES
            (1, '2023-01-01 10:00:00'),
            (2, '2023-01-01 11:00:00')",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO future_states VALUES
            ('2023-01-01 10:30:00', 'state_a'),
            ('2023-01-01 11:30:00', 'state_b')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT e.id, e.ts, s.state
            FROM forward_events e
            ASOF JOIN future_states s ON e.ts <= s.ts
            ORDER BY e.id",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_asof_join_with_multiple_conditions() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE market_data (
                exchange String,
                symbol String,
                ts DateTime,
                bid Float64,
                ask Float64
            ) ENGINE = MergeTree ORDER BY (exchange, symbol, ts)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE trade_executions (
                exchange String,
                symbol String,
                ts DateTime,
                side String,
                qty Int64
            ) ENGINE = MergeTree ORDER BY (exchange, symbol, ts)",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO market_data VALUES
            ('NYSE', 'AAPL', '2023-01-01 10:00:00', 149.90, 150.10),
            ('NYSE', 'AAPL', '2023-01-01 10:01:00', 150.00, 150.20),
            ('NASDAQ', 'AAPL', '2023-01-01 10:00:00', 149.85, 150.05)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO trade_executions VALUES
            ('NYSE', 'AAPL', '2023-01-01 10:00:30', 'BUY', 100),
            ('NASDAQ', 'AAPL', '2023-01-01 10:00:15', 'SELL', 50)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT t.exchange, t.symbol, t.ts, t.side, t.qty, m.bid, m.ask
            FROM trade_executions t
            ASOF JOIN market_data m
            ON t.exchange = m.exchange AND t.symbol = m.symbol AND t.ts >= m.ts
            ORDER BY t.ts",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_asof_join_time_series() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE measurements (
                device_id String,
                measure_time DateTime,
                value Float64
            ) ENGINE = MergeTree ORDER BY (device_id, measure_time)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE thresholds (
                device_id String,
                effective_time DateTime,
                min_val Float64,
                max_val Float64
            ) ENGINE = MergeTree ORDER BY (device_id, effective_time)",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO measurements VALUES
            ('D1', '2023-01-01 10:00:00', 55.0),
            ('D1', '2023-01-01 11:00:00', 75.0),
            ('D1', '2023-01-01 12:00:00', 65.0)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO thresholds VALUES
            ('D1', '2023-01-01 09:00:00', 50.0, 70.0),
            ('D1', '2023-01-01 10:30:00', 60.0, 80.0)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                m.device_id,
                m.measure_time,
                m.value,
                t.min_val,
                t.max_val,
                m.value BETWEEN t.min_val AND t.max_val AS in_range
            FROM measurements m
            ASOF JOIN thresholds t
            ON m.device_id = t.device_id AND m.measure_time >= t.effective_time
            ORDER BY m.measure_time",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_asof_join_currency_rates() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE currency_rates (
                currency String,
                rate_date DateTime,
                rate Float64
            ) ENGINE = MergeTree ORDER BY (currency, rate_date)",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE transactions (
                tx_id Int64,
                currency String,
                tx_time DateTime,
                amount Float64
            ) ENGINE = MergeTree ORDER BY (currency, tx_time)",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO currency_rates VALUES
            ('EUR', '2023-01-01 00:00:00', 1.10),
            ('EUR', '2023-01-02 00:00:00', 1.12),
            ('GBP', '2023-01-01 00:00:00', 1.25)",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO transactions VALUES
            (1, 'EUR', '2023-01-01 14:00:00', 100.00),
            (2, 'EUR', '2023-01-02 10:00:00', 200.00),
            (3, 'GBP', '2023-01-01 16:00:00', 150.00)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                t.tx_id,
                t.currency,
                t.amount,
                r.rate,
                t.amount * r.rate AS usd_amount
            FROM transactions t
            ASOF JOIN currency_rates r
            ON t.currency = r.currency AND t.tx_time >= r.rate_date
            ORDER BY t.tx_id",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}
