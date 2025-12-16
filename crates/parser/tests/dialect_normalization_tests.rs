use yachtsql_parser::{DialectType, Parser};

fn parse_sql(sql: &str, dialect: DialectType) -> Result<(), String> {
    let parser = Parser::with_dialect(dialect);
    parser
        .parse_sql(sql)
        .map(|_| ())
        .map_err(|e| format!("Parse error: {:?}", e))
}

#[test]
fn test_bigquery_safe_divide_parses() {
    let sql = "SELECT SAFE_DIVIDE(10, 2)";
    let result = parse_sql(sql, DialectType::BigQuery);
    assert!(
        result.is_ok(),
        "Failed to parse SAFE_DIVIDE with BigQuery dialect: {:?}",
        result
    );
}

#[test]
fn test_bigquery_safe_divide_with_columns() {
    let sql = "SELECT SAFE_DIVIDE(revenue, quantity) FROM sales";
    let result = parse_sql(sql, DialectType::BigQuery);
    assert!(
        result.is_ok(),
        "Failed to parse SAFE_DIVIDE with columns: {:?}",
        result
    );
}

#[test]
fn test_bigquery_safe_divide_with_expressions() {
    let sql = "SELECT SAFE_DIVIDE(a + b, c * 2) FROM data";
    let result = parse_sql(sql, DialectType::BigQuery);
    assert!(
        result.is_ok(),
        "Failed to parse SAFE_DIVIDE with expressions: {:?}",
        result
    );
}

#[test]
fn test_bigquery_parses_own_functions() {
    let sql = "SELECT SAFE_DIVIDE(10, 2) FROM data";
    let result = parse_sql(sql, DialectType::BigQuery);
    assert!(
        result.is_ok(),
        "BigQuery should parse SAFE_DIVIDE: {:?}",
        result
    );
}

#[test]
fn test_bigquery_standard_functions() {
    let sql =
        "SELECT CAST(x AS INT64), CAST(y AS STRING), CASE WHEN a > 0 THEN 1 ELSE 0 END FROM data";
    let result = parse_sql(sql, DialectType::BigQuery);
    assert!(
        result.is_ok(),
        "BigQuery should parse standard SQL: {:?}",
        result
    );
}

#[test]
fn test_mixed_standard_and_dialect_functions() {
    let sql = "SELECT SAFE_DIVIDE(a, b), CAST(c AS INT64), COALESCE(d, 0) FROM data";
    let result = parse_sql(sql, DialectType::BigQuery);
    assert!(
        result.is_ok(),
        "Should parse mix of dialect and standard functions: {:?}",
        result
    );
}

#[test]
fn test_bigquery_if_function() {
    let sql = "SELECT IF(x > 0, 'positive', 'negative')";
    let result = parse_sql(sql, DialectType::BigQuery);
    assert!(
        result.is_ok(),
        "Failed to parse IF with BigQuery dialect: {:?}",
        result
    );
}

#[test]
fn test_bigquery_if_with_complex_condition() {
    let sql = "SELECT IF(status = 'active' AND balance > 100, 1, 0) FROM accounts";
    let result = parse_sql(sql, DialectType::BigQuery);
    assert!(
        result.is_ok(),
        "Failed to parse IF with complex condition: {:?}",
        result
    );
}

#[test]
fn test_bigquery_if_nested() {
    let sql = "SELECT IF(x > 0, IF(x > 10, 'large', 'small'), 'negative') FROM data";
    let result = parse_sql(sql, DialectType::BigQuery);
    assert!(result.is_ok(), "Failed to parse nested IF: {:?}", result);
}
