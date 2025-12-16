use yachtsql_parser::{DialectType, Parser};

#[test]
fn test_json_value_no_options() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.name')";
    let result = parser.parse_sql(sql);
    assert!(result.is_ok(), "Should parse JSON_VALUE without options");
}

#[test]
fn test_json_value_with_returning() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.age' RETURNING INT64)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with RETURNING clause: {:?}",
        result
    );
}

#[test]
fn test_json_value_null_on_empty() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.name' NULL ON EMPTY)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with NULL ON EMPTY: {:?}",
        result
    );
}

#[test]
fn test_json_value_error_on_empty() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.name' ERROR ON EMPTY)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with ERROR ON EMPTY: {:?}",
        result
    );
}

#[test]
fn test_json_value_default_on_empty() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.name' DEFAULT 'unknown' ON EMPTY)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with DEFAULT ON EMPTY: {:?}",
        result
    );
}

#[test]
fn test_json_value_null_on_error() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.name' NULL ON ERROR)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with NULL ON ERROR: {:?}",
        result
    );
}

#[test]
fn test_json_value_error_on_error() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.name' ERROR ON ERROR)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with ERROR ON ERROR: {:?}",
        result
    );
}

#[test]
fn test_json_value_default_on_error() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.name' DEFAULT 'N/A' ON ERROR)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with DEFAULT ON ERROR: {:?}",
        result
    );
}

#[test]
fn test_json_value_returning_with_on_empty() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.age' RETURNING INT64 NULL ON EMPTY)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with RETURNING and ON EMPTY: {:?}",
        result
    );
}

#[test]
fn test_json_value_returning_with_on_error() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.age' RETURNING INT64 NULL ON ERROR)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with RETURNING and ON ERROR: {:?}",
        result
    );
}

#[test]
fn test_json_value_all_options() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.price' RETURNING NUMERIC NULL ON EMPTY ERROR ON ERROR)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with all options: {:?}",
        result
    );
}

#[test]
fn test_json_value_with_default_on_empty_and_null_on_error() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.status' DEFAULT 'pending' ON EMPTY NULL ON ERROR)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with DEFAULT ON EMPTY and NULL ON ERROR: {:?}",
        result
    );
}

#[test]
fn test_json_value_nested_path() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.user.address.city' RETURNING STRING)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with nested path: {:?}",
        result
    );
}

#[test]
fn test_json_value_array_index() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.items[0].name' NULL ON EMPTY)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with array index: {:?}",
        result
    );
}

#[test]
fn test_json_value_multiple_in_query() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = r#"
        SELECT
            JSON_VALUE(data, '$.name' DEFAULT 'unknown' ON EMPTY) as name,
            JSON_VALUE(data, '$.age' RETURNING INT64 NULL ON ERROR) as age,
            JSON_VALUE(data, '$.email' ERROR ON EMPTY) as email
        FROM users
    "#;
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse multiple JSON_VALUE calls: {:?}",
        result
    );
}

#[test]
fn test_json_value_in_where_clause() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT * FROM users WHERE JSON_VALUE(data, '$.active' RETURNING BOOL) = TRUE";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE in WHERE clause: {:?}",
        result
    );
}

#[test]
fn test_json_value_complex_default() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.count' DEFAULT 0 ON EMPTY) + 10";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with numeric default: {:?}",
        result
    );
}

#[test]
fn test_json_value_with_cast() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT CAST(JSON_VALUE(data, '$.price' NULL ON ERROR) AS FLOAT64)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE wrapped in CAST: {:?}",
        result
    );
}

#[test]
fn test_json_value_in_join() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = r#"
        SELECT u.*, p.product_name
        FROM users u
        JOIN products p ON p.id = JSON_VALUE(u.data, '$.favorite_product_id' RETURNING INT64)
    "#;
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE in JOIN condition: {:?}",
        result
    );
}

#[test]
fn test_json_value_in_subquery() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = r#"
        SELECT * FROM (
            SELECT JSON_VALUE(data, '$.score' RETURNING FLOAT64 DEFAULT 0.0 ON EMPTY) as score
            FROM results
        ) WHERE score > 0.5
    "#;
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE in subquery: {:?}",
        result
    );
}

#[test]
fn test_json_value_with_case() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = r#"
        SELECT CASE
            WHEN JSON_VALUE(data, '$.status' DEFAULT 'pending' ON EMPTY) = 'active' THEN 1
            ELSE 0
        END as is_active
    "#;
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE in CASE expression: {:?}",
        result
    );
}

#[test]
fn test_json_value_empty_path() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$' NULL ON ERROR)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with root path: {:?}",
        result
    );
}

#[test]
fn test_json_value_with_wildcard() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.items[*].name' NULL ON EMPTY)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with wildcard: {:?}",
        result
    );
}

#[test]
fn test_json_value_returning_timestamp() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.created_at' RETURNING TIMESTAMP NULL ON ERROR)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE RETURNING TIMESTAMP: {:?}",
        result
    );
}

#[test]
fn test_json_value_returning_date() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.birth_date' RETURNING DATE NULL ON ERROR)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE RETURNING DATE: {:?}",
        result
    );
}

#[test]
fn test_json_value_with_aggregation() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = r#"
        SELECT
            COUNT(*),
            AVG(JSON_VALUE(data, '$.rating' RETURNING FLOAT64 DEFAULT 0.0 ON EMPTY))
        FROM reviews
        GROUP BY product_id
    "#;
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with aggregation: {:?}",
        result
    );
}

#[test]
fn test_json_value_order_by() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT * FROM users ORDER BY JSON_VALUE(data, '$.last_login' RETURNING TIMESTAMP)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE in ORDER BY: {:?}",
        result
    );
}

#[test]
fn test_json_value_with_returning_clause() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = "SELECT JSON_VALUE(data, '$.name' RETURNING TEXT NULL ON EMPTY)";
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with RETURNING clause: {:?}",
        result
    );
}

#[test]
fn test_json_value_quoted_path() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = r#"SELECT JSON_VALUE(data, "$.user.name" NULL ON ERROR)"#;
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with double-quoted path: {:?}",
        result
    );
}

#[test]
fn test_json_value_with_comments() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = r#"
        SELECT
            -- Extract user name
            JSON_VALUE(data, '$.name' /* with options */ DEFAULT 'anonymous' ON EMPTY)
        FROM users
    "#;
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE with comments: {:?}",
        result
    );
}

#[test]
fn test_json_value_comparison() {
    let parser = Parser::with_dialect(DialectType::BigQuery);
    let sql = r#"
        SELECT * FROM products
        WHERE JSON_VALUE(data, '$.price' RETURNING NUMERIC) > 100
    "#;
    let result = parser.parse_sql(sql);
    assert!(
        result.is_ok(),
        "Should parse JSON_VALUE in comparison: {:?}",
        result
    );
}
