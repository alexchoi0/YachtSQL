use std::hash::{Hash, Hasher};

use yachtsql_parser::DialectType;

pub fn normalize_sql(sql: &str) -> String {
    let normalized_whitespace: String = sql.split_whitespace().collect::<Vec<&str>>().join(" ");

    let mut result = String::with_capacity(normalized_whitespace.len());
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut prev_char = ' ';

    for ch in normalized_whitespace.chars() {
        if ch == '\'' && prev_char != '\\' && !in_double_quote {
            in_single_quote = !in_single_quote;
            result.push(ch);
        } else if ch == '"' && prev_char != '\\' && !in_single_quote {
            in_double_quote = !in_double_quote;
            result.push(ch);
        } else if in_single_quote || in_double_quote {
            result.push(ch);
        } else {
            result.push(ch.to_ascii_lowercase());
        }
        prev_char = ch;
    }

    result
}

pub fn hash_sql(sql: &str, dialect: DialectType) -> u64 {
    let normalized = normalize_sql(sql);
    let mut hasher = ahash::AHasher::default();
    normalized.hash(&mut hasher);

    std::mem::discriminant(&dialect).hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_sql_whitespace() {
        let sql = "SELECT  *  FROM   users   WHERE  id = 1";
        let normalized = normalize_sql(sql);
        assert_eq!(normalized, "select * from users where id = 1");
    }

    #[test]
    fn test_normalize_sql_case() {
        let sql = "SeLeCt * FrOm UsErS";
        let normalized = normalize_sql(sql);
        assert_eq!(normalized, "select * from users");
    }

    #[test]
    fn test_normalize_preserves_string_literal_case() {
        let sql = "SELECT 'ALICE' SIMILAR TO 'Alice'";
        let normalized = normalize_sql(sql);
        assert_eq!(normalized, "select 'ALICE' similar to 'Alice'");
    }

    #[test]
    fn test_normalize_preserves_double_quoted_case() {
        let sql = "SELECT \"ColumnName\" FROM users";
        let normalized = normalize_sql(sql);
        assert_eq!(normalized, "select \"ColumnName\" from users");
    }

    #[test]
    fn test_normalize_sql_tabs_newlines() {
        let sql = "SELECT\t*\nFROM\r\nusers";
        let normalized = normalize_sql(sql);
        assert_eq!(normalized, "select * from users");
    }

    #[test]
    fn test_normalize_sql_trim() {
        let sql = "  \n  SELECT * FROM users  \n  ";
        let normalized = normalize_sql(sql);
        assert_eq!(normalized, "select * from users");
    }

    #[test]
    fn test_hash_sql_consistency() {
        let sql1 = "SELECT * FROM users";
        let sql2 = "select   *   from   users";
        let sql3 = "  SELECT\n  *\n  FROM\n  users  ";
        let dialect = DialectType::PostgreSQL;

        assert_eq!(hash_sql(sql1, dialect), hash_sql(sql2, dialect));
        assert_eq!(hash_sql(sql1, dialect), hash_sql(sql3, dialect));
    }

    #[test]
    fn test_hash_sql_different() {
        let sql1 = "SELECT * FROM users";
        let sql2 = "SELECT * FROM products";
        let dialect = DialectType::PostgreSQL;

        assert_ne!(hash_sql(sql1, dialect), hash_sql(sql2, dialect));
    }

    #[test]
    fn test_hash_sql_different_dialects() {
        let sql = "SELECT * FROM users";

        let pg_hash = hash_sql(sql, DialectType::PostgreSQL);
        let bq_hash = hash_sql(sql, DialectType::BigQuery);
        let ch_hash = hash_sql(sql, DialectType::ClickHouse);

        assert_ne!(pg_hash, bq_hash);
        assert_ne!(pg_hash, ch_hash);
        assert_ne!(bq_hash, ch_hash);
    }

    #[test]
    fn test_hash_sql_same_dialect_consistency() {
        let sql = "SELECT * FROM users";
        let dialect = DialectType::BigQuery;

        assert_eq!(hash_sql(sql, dialect), hash_sql(sql, dialect));
    }

    #[test]
    fn test_normalize_preserves_literals_issue() {
        let sql = "SELECT 'SELECT' FROM users";
        let normalized = normalize_sql(sql);

        assert_eq!(normalized, "select 'SELECT' from users");
    }
}
