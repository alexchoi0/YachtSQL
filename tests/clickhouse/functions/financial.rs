use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_to_decimal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toDecimal64(123.456, 2)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_round_currency() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT round(toDecimal64(123.456, 4), 2)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_simple_interest() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                toDecimal64(1000, 2) AS principal,
                toDecimal64(0.05, 4) AS rate,
                3 AS years,
                toDecimal64(1000 * 0.05 * 3, 2) AS simple_interest",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_compound_interest() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                toDecimal64(1000, 2) AS principal,
                toDecimal64(0.05, 4) AS rate,
                3 AS years,
                toDecimal64(1000 * pow(1 + 0.05, 3), 2) AS compound_amount",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_present_value() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                toDecimal64(1000, 2) AS future_value,
                toDecimal64(0.05, 4) AS rate,
                5 AS periods,
                toDecimal64(1000 / pow(1 + 0.05, 5), 2) AS present_value",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_net_present_value() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "WITH
                cash_flows AS (SELECT [-1000, 300, 400, 500, 600] AS flows),
                rate AS (SELECT 0.1 AS r)
            SELECT
                arraySum(arrayMap((cf, i) -> cf / pow(1 + r, i),
                    flows,
                    arrayEnumerate(flows))) AS npv
            FROM cash_flows, rate",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_annuity_payment() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                toDecimal64(100000, 2) AS principal,
                toDecimal64(0.05/12, 8) AS monthly_rate,
                360 AS months,
                toDecimal64(100000 * (0.05/12 * pow(1 + 0.05/12, 360)) / (pow(1 + 0.05/12, 360) - 1), 2) AS monthly_payment"
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_percentage_change() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                100 AS old_value,
                120 AS new_value,
                (120 - 100) / 100 * 100 AS percent_change",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_cagr() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                1000 AS beginning_value,
                2000 AS ending_value,
                5 AS years,
                (pow(2000 / 1000, 1.0 / 5) - 1) * 100 AS cagr_percent",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_roi() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                10000 AS investment,
                15000 AS final_value,
                ((15000 - 10000) / 10000) * 100 AS roi_percent",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_currency_conversion() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE exchange_rates (
                from_currency String,
                to_currency String,
                rate Decimal64(6)
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO exchange_rates VALUES
            ('USD', 'EUR', 0.85),
            ('USD', 'GBP', 0.73),
            ('EUR', 'USD', 1.18)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                100 AS amount_usd,
                r.rate,
                100 * r.rate AS amount_eur
            FROM exchange_rates r
            WHERE from_currency = 'USD' AND to_currency = 'EUR'",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_amortization_schedule() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "WITH
                loan AS (SELECT 10000 AS principal, 0.05/12 AS r, 12 AS n),
                payment AS (SELECT principal * (r * pow(1 + r, n)) / (pow(1 + r, n) - 1) AS pmt FROM loan)
            SELECT
                number + 1 AS month,
                round(pmt, 2) AS payment,
                round(principal * pow(1 + r, number) - pmt * (pow(1 + r, number) - 1) / r, 2) AS remaining
            FROM numbers(12), loan, payment"
        )
        .unwrap();
    assert!(result.num_rows() == 12); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_weighted_average_cost() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE inventory (
                item String,
                quantity Int64,
                unit_cost Decimal64(2)
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('Widget', 100, 10.00),
            ('Widget', 50, 12.00),
            ('Widget', 75, 11.50)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                item,
                sum(quantity) AS total_units,
                sum(quantity * unit_cost) AS total_cost,
                sum(quantity * unit_cost) / sum(quantity) AS weighted_avg_cost
            FROM inventory
            GROUP BY item",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_profit_margin() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                1000 AS revenue,
                600 AS cost,
                (1000 - 600) AS gross_profit,
                ((1000 - 600) / 1000) * 100 AS gross_margin_percent",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_break_even() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                10000 AS fixed_costs,
                25 AS price_per_unit,
                15 AS variable_cost_per_unit,
                10000 / (25 - 15) AS break_even_units",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_depreciation_straight_line() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                10000 AS asset_cost,
                1000 AS salvage_value,
                5 AS useful_life_years,
                (10000 - 1000) / 5 AS annual_depreciation",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_moving_average_price() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE stock_prices (
                date Date,
                close_price Decimal64(2)
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO stock_prices VALUES
            ('2023-01-01', 100.00),
            ('2023-01-02', 102.50),
            ('2023-01-03', 101.00),
            ('2023-01-04', 103.00),
            ('2023-01-05', 105.00)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
                date,
                close_price,
                avg(close_price) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma_3
            FROM stock_prices
            ORDER BY date"
        )
        .unwrap();
    assert!(result.num_rows() == 5); // TODO: use table![[expected_values]]
}
