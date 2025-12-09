use std::collections::HashSet;

use yachtsql_capability::FeatureRegistry;
use yachtsql_core::error::{Error, Result};

pub fn validate_function(function_name: &str, registry: &FeatureRegistry) -> Result<()> {
    validate_function_with_udfs(function_name, registry, None)
}

pub fn validate_function_with_udfs(
    function_name: &str,
    registry: &FeatureRegistry,
    udf_names: Option<&HashSet<String>>,
) -> Result<()> {
    let dialect = registry.dialect();
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

    let is_available = match dialect {
        yachtsql_parser::DialectType::PostgreSQL => is_postgres_function(&function_upper),
        yachtsql_parser::DialectType::BigQuery => is_bigquery_function(&function_upper),
        yachtsql_parser::DialectType::ClickHouse => is_clickhouse_function(&function_upper),
    };

    if is_available {
        Ok(())
    } else {
        Err(Error::invalid_query(format!(
            "Function {} is not available in {:?} dialect",
            function_name, dialect
        )))
    }
}

fn is_sql_keyword_pseudo_function(function_name: &str) -> bool {
    matches!(function_name, "ALL" | "ANY" | "SOME")
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

fn is_postgres_function(function_name: &str) -> bool {
    use yachtsql_functions::dialects::*;

    let scalars = postgres_scalar_functions();
    let aggregates = postgres_aggregate_functions();

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

fn is_clickhouse_function(function_name: &str) -> bool {
    use yachtsql_functions::dialects::*;

    let scalars = clickhouse_scalar_functions();
    let aggregates = clickhouse_aggregate_functions();

    scalars
        .iter()
        .any(|f| f.eq_ignore_ascii_case(function_name))
        || aggregates
            .iter()
            .any(|f| f.eq_ignore_ascii_case(function_name))
}

#[cfg(test)]
mod tests {
    use yachtsql_capability::FeatureRegistry;
    use yachtsql_parser::DialectType;

    use super::*;

    #[test]
    fn core_functions_available_in_all_dialects() {
        let pg_registry = FeatureRegistry::new(DialectType::PostgreSQL);
        let bq_registry = FeatureRegistry::new(DialectType::BigQuery);
        let ch_registry = FeatureRegistry::new(DialectType::ClickHouse);

        assert!(validate_function("UPPER", &pg_registry).is_ok());
        assert!(validate_function("UPPER", &bq_registry).is_ok());
        assert!(validate_function("UPPER", &ch_registry).is_ok());

        assert!(validate_function("COUNT", &pg_registry).is_ok());
        assert!(validate_function("COUNT", &bq_registry).is_ok());
        assert!(validate_function("COUNT", &ch_registry).is_ok());
    }

    #[test]
    fn postgres_specific_functions() {
        let pg_registry = FeatureRegistry::new(DialectType::PostgreSQL);
        let bq_registry = FeatureRegistry::new(DialectType::BigQuery);

        assert!(validate_function("JSONB", &pg_registry).is_ok());
        assert!(validate_function("JSONB", &bq_registry).is_err());
    }

    #[test]
    fn bigquery_specific_functions() {
        let bq_registry = FeatureRegistry::new(DialectType::BigQuery);
        let pg_registry = FeatureRegistry::new(DialectType::PostgreSQL);

        assert!(validate_function("SAFE_CAST", &bq_registry).is_ok());
        assert!(validate_function("SAFE_CAST", &pg_registry).is_err());
    }

    #[test]
    fn clickhouse_specific_functions() {
        let ch_registry = FeatureRegistry::new(DialectType::ClickHouse);
        let pg_registry = FeatureRegistry::new(DialectType::PostgreSQL);

        assert!(validate_function("UNIQ_EXACT", &ch_registry).is_ok());
        assert!(validate_function("UNIQ_EXACT", &pg_registry).is_err());
    }

    #[test]
    fn case_insensitive_validation() {
        let pg_registry = FeatureRegistry::new(DialectType::PostgreSQL);

        assert!(validate_function("upper", &pg_registry).is_ok());
        assert!(validate_function("UPPER", &pg_registry).is_ok());
        assert!(validate_function("Upper", &pg_registry).is_ok());
    }

    #[test]
    fn yachtsql_system_functions_available_in_all_dialects() {
        let pg_registry = FeatureRegistry::new(DialectType::PostgreSQL);
        let bq_registry = FeatureRegistry::new(DialectType::BigQuery);
        let ch_registry = FeatureRegistry::new(DialectType::ClickHouse);

        assert!(validate_function("yachtsql.is_feature_enabled", &pg_registry).is_ok());
        assert!(validate_function("YACHTSQL.IS_FEATURE_ENABLED", &bq_registry).is_ok());
        assert!(validate_function("YachtSQL.is_feature_enabled", &ch_registry).is_ok());
    }
}
