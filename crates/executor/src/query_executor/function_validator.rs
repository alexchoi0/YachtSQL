use std::collections::HashSet;

use yachtsql_core::error::Result;
use yachtsql_parser::DialectType;

pub fn validate_function(function_name: &str, dialect: DialectType) -> Result<()> {
    validate_function_with_udfs(function_name, dialect, None)
}

pub fn validate_function_with_udfs(
    function_name: &str,
    _dialect: DialectType,
    udf_names: Option<&HashSet<String>>,
) -> Result<()> {
    let function_upper = function_name.to_uppercase();

    if function_upper.starts_with("YACHTSQL.") {
        return Ok(());
    }

    if is_sql_keyword_pseudo_function(&function_upper) {
        return Ok(());
    }

    if is_core_function(&function_upper) {
        return Ok(());
    }

    if let Some(udfs) = udf_names {
        if udfs.contains(&function_upper) {
            return Ok(());
        }
    }

    if is_bigquery_function(&function_upper) {
        return Ok(());
    }

    Ok(())
}

fn is_sql_keyword_pseudo_function(function_name: &str) -> bool {
    matches!(function_name, "ALL" | "ANY" | "SOME" | "__NAMED_TUPLE__")
}

fn is_core_function(function_name: &str) -> bool {
    use yachtsql_functions::dialects::*;

    let scalars = core_scalar_functions();
    let aggregates = core_aggregate_functions();

    scalars
        .iter()
        .any(|f| f.eq_ignore_ascii_case(function_name))
        || aggregates
            .iter()
            .any(|f| f.eq_ignore_ascii_case(function_name))
}

fn is_bigquery_function(function_name: &str) -> bool {
    use yachtsql_functions::dialects::*;

    let scalars = bigquery_scalar_functions();
    let aggregates = bigquery_aggregate_functions();

    scalars
        .iter()
        .any(|f| f.eq_ignore_ascii_case(function_name))
        || aggregates
            .iter()
            .any(|f| f.eq_ignore_ascii_case(function_name))
}

#[cfg(test)]
mod tests {
    use yachtsql_parser::DialectType;

    use super::*;

    #[test]
    fn core_functions_available() {
        assert!(validate_function("UPPER", DialectType::BigQuery).is_ok());
        assert!(validate_function("COUNT", DialectType::BigQuery).is_ok());
    }

    #[test]
    fn bigquery_specific_functions() {
        assert!(validate_function("SAFE_CAST", DialectType::BigQuery).is_ok());
    }

    #[test]
    fn case_insensitive_validation() {
        assert!(validate_function("upper", DialectType::BigQuery).is_ok());
        assert!(validate_function("UPPER", DialectType::BigQuery).is_ok());
        assert!(validate_function("Upper", DialectType::BigQuery).is_ok());
    }

    #[test]
    fn yachtsql_system_functions_available() {
        assert!(validate_function("yachtsql.is_feature_enabled", DialectType::BigQuery).is_ok());
        assert!(validate_function("YACHTSQL.IS_FEATURE_ENABLED", DialectType::BigQuery).is_ok());
        assert!(validate_function("YachtSQL.is_feature_enabled", DialectType::BigQuery).is_ok());
    }
}
