use yachtsql_parser::{DialectType, Parser};

#[test]
fn test_interval_basic_day() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT INTERVAL 7 DAY";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL 7 DAY: {:?}",
        result
    );
}

#[test]
fn test_interval_basic_hour() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT INTERVAL 3 HOUR";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL 3 HOUR: {:?}",
        result
    );
}

#[test]
fn test_interval_basic_minute() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT INTERVAL 15 MINUTE";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL 15 MINUTE: {:?}",
        result
    );
}

#[test]
fn test_interval_basic_second() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT INTERVAL 30 SECOND";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL 30 SECOND: {:?}",
        result
    );
}

#[test]
fn test_interval_basic_month() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT INTERVAL 2 MONTH";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL 2 MONTH: {:?}",
        result
    );
}

#[test]
fn test_interval_basic_year() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT INTERVAL 5 YEAR";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL 5 YEAR: {:?}",
        result
    );
}

#[test]
fn test_interval_with_date_add() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT DATE_ADD('2024-01-01', INTERVAL 7 DAY)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse DATE_ADD with INTERVAL: {:?}",
        result
    );
}

#[test]
fn test_interval_with_date_sub() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT DATE_SUB('2024-12-31', INTERVAL 3 MONTH)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse DATE_SUB with INTERVAL: {:?}",
        result
    );
}

#[test]
fn test_interval_with_timestamp_add() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT TIMESTAMP_ADD('2024-01-01 12:00:00', INTERVAL 5 HOUR)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse TIMESTAMP_ADD with INTERVAL: {:?}",
        result
    );
}

#[test]
fn test_interval_with_timestamp_sub() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT TIMESTAMP_SUB('2024-01-01 12:00:00', INTERVAL 30 MINUTE)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse TIMESTAMP_SUB with INTERVAL: {:?}",
        result
    );
}

#[test]
fn test_interval_arithmetic_date_plus_interval() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT DATE '2024-01-01' + INTERVAL '1 day'";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse DATE + INTERVAL: {:?}",
        result
    );
}

#[test]
fn test_interval_arithmetic_date_minus_interval() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT DATE '2024-01-01' - INTERVAL '7 days'";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse DATE - INTERVAL: {:?}",
        result
    );
}

#[test]
fn test_interval_arithmetic_timestamp_plus_interval() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT TIMESTAMP '2024-01-01 12:00:00' + INTERVAL '3 hours'";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse TIMESTAMP + INTERVAL: {:?}",
        result
    );
}

#[test]
fn test_interval_zero_value() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT INTERVAL 0 DAY";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL 0 DAY: {:?}",
        result
    );
}

#[test]
fn test_interval_negative_value() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT INTERVAL -7 DAY";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL -7 DAY: {:?}",
        result
    );
}

#[test]
fn test_interval_string_format() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT INTERVAL '7 days'";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL '7 days': {:?}",
        result
    );
}

#[test]
fn test_interval_multiple_units() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT INTERVAL '1 year 2 months 3 days'";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse multi-unit INTERVAL: {:?}",
        result
    );
}

#[test]
fn test_interval_time_format() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT INTERVAL '1:30:00'";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse time format INTERVAL: {:?}",
        result
    );
}

#[test]
fn test_interval_in_where_clause() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT * FROM events WHERE created_at > NOW() - INTERVAL '7 days'";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL in WHERE: {:?}",
        result
    );
}

#[test]
fn test_interval_in_case_expression() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT CASE WHEN active THEN INTERVAL 1 DAY ELSE INTERVAL 7 DAY END";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL in CASE: {:?}",
        result
    );
}

#[test]
fn test_interval_in_subquery() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT * FROM (SELECT DATE_ADD('2024-01-01', INTERVAL 1 MONTH) as future_date)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL in subquery: {:?}",
        result
    );
}

#[test]
fn test_interval_multiple_in_select() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = r#"
        SELECT
            INTERVAL 1 DAY,
            INTERVAL 2 HOUR,
            INTERVAL 3 MINUTE
    "#;
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse multiple INTERVALs: {:?}",
        result
    );
}

#[test]
fn test_interval_in_cte() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = r#"
        WITH future_dates AS (
            SELECT
                id,
                created_at,
                DATE_ADD(created_at, INTERVAL 30 DAY) as expires_at
            FROM users
        )
        SELECT * FROM future_dates WHERE expires_at > CURRENT_DATE()
    "#;
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL in CTE: {:?}",
        result
    );
}

#[test]
fn test_interval_complex_date_range_query() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = r#"
        SELECT *
        FROM sales
        WHERE sale_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY)
          AND sale_date < CURRENT_DATE()
    "#;
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Failed to parse complex INTERVAL query: {:?}",
        result
    );
}
