use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::config::RegistryConfig;
use crate::error::{CapabilityError, Result};
use crate::feature_ids::lookup_feature_id;
use crate::manifests::{bigquery_capabilities, postgres_capabilities};
use crate::{FeatureId, profiles};

#[derive(Debug, Deserialize)]
struct Manifest {
    metadata: ManifestMetadata,
    #[serde(default)]
    core_features: HashMap<String, bool>,
    #[serde(default)]
    optional_features: HashMap<String, bool>,
    #[serde(default)]
    dialect_extensions: HashMap<String, bool>,
}

#[derive(Debug, Deserialize)]
struct ManifestMetadata {
    dialect: String,
    version: String,
}

pub fn load_manifest(source: &str) -> Result<HashSet<FeatureId>> {
    match source {
        "postgres" => return Ok(postgres_capabilities()),
        "bigquery" => return Ok(bigquery_capabilities()),
        _ => {}
    }

    let path = Path::new(source);
    if path.exists() || source.ends_with(".toml") {
        return load_manifest_from_file(source);
    }

    Err(CapabilityError::UnsupportedDialect {
        dialect: source.to_string(),
    })
}

fn load_manifest_from_file(path: &str) -> Result<HashSet<FeatureId>> {
    let content = fs::read_to_string(path).map_err(|e| CapabilityError::ManifestLoadError {
        path: path.to_string(),
        reason: e.to_string(),
    })?;

    let manifest: Manifest =
        toml::from_str(&content).map_err(|e| CapabilityError::InvalidManifest {
            path: path.to_string(),
            reason: e.to_string(),
        })?;

    if manifest.metadata.version.is_empty() {
        return Err(CapabilityError::InvalidManifest {
            path: path.to_string(),
            reason: "Missing version".to_string(),
        });
    }

    let dialect = manifest.metadata.dialect.as_str();
    if dialect != "postgres" && dialect != "bigquery" {
        return Err(CapabilityError::UnsupportedDialect {
            dialect: dialect.to_string(),
        });
    }

    let mut features = HashSet::new();

    let mut process_features = |section_name: &str, map: HashMap<String, bool>| -> Result<()> {
        for (name, enabled) in map {
            if enabled {
                if let Some(id) = lookup_feature_id(&name) {
                    features.insert(id);
                } else {
                    return Err(CapabilityError::UnknownFeatureInManifest {
                        feature_id: name,
                        section: section_name.to_string(),
                        path: path.to_string(),
                    });
                }
            }
        }
        Ok(())
    };

    process_features("core_features", manifest.core_features)?;
    process_features("optional_features", manifest.optional_features)?;
    process_features("dialect_extensions", manifest.dialect_extensions)?;

    Ok(features)
}

pub fn load_dialect_manifest(dialect_name: &str) -> Result<HashSet<FeatureId>> {
    load_manifest(dialect_name)
}

pub fn load_dialect_manifest_with_config(
    dialect_name: &str,
    config: &RegistryConfig,
) -> Result<HashSet<FeatureId>> {
    match load_manifest(dialect_name) {
        Ok(features) => Ok(features),
        Err(_) if config.use_fallback() => Ok(default_capabilities()),
        Err(e) => Err(e),
    }
}

pub fn default_capabilities() -> HashSet<FeatureId> {
    use crate::{
        F001_BASIC_SQL, F051_BASIC_DATE_TIME, F052_INTERVALS_DATETIME_ARITHMETIC,
        F311_SCHEMA_DEFINITION, F312_SCALAR_SUBQUERY_AS_EXPRESSION, F402_TRANSACTION_COMMIT,
        F403_TRANSACTION_ROLLBACK, F471_TUPLE_IN_CLAUSE, F781_SELF_REFERENCING_OPERATIONS,
        F782_COMMIT_STATEMENT, F783_ROLLBACK_STATEMENT, F784_SAVEPOINT_STATEMENT,
        F785_ROLLBACK_TO_SAVEPOINT_STATEMENT, F786_SAVEPOINTS, T301_SAVEPOINTS,
        T321_TRANSACTION_ISOLATION, T611_WINDOW_FUNCTIONS, T619_WINDOW_FRAME_GROUPS,
    };

    let mut features = HashSet::new();
    features.insert(F001_BASIC_SQL);
    features.insert(F051_BASIC_DATE_TIME);
    features.insert(F052_INTERVALS_DATETIME_ARITHMETIC);
    features.insert(F311_SCHEMA_DEFINITION);
    features.insert(F312_SCALAR_SUBQUERY_AS_EXPRESSION);
    features.insert(F471_TUPLE_IN_CLAUSE);
    features.insert(F402_TRANSACTION_COMMIT);
    features.insert(F403_TRANSACTION_ROLLBACK);
    features.insert(F781_SELF_REFERENCING_OPERATIONS);
    features.insert(F782_COMMIT_STATEMENT);
    features.insert(F783_ROLLBACK_STATEMENT);
    features.insert(F784_SAVEPOINT_STATEMENT);
    features.insert(F785_ROLLBACK_TO_SAVEPOINT_STATEMENT);
    features.insert(F786_SAVEPOINTS);
    features.insert(T301_SAVEPOINTS);
    features.insert(T321_TRANSACTION_ISOLATION);
    features.insert(T611_WINDOW_FUNCTIONS);
    features.insert(T619_WINDOW_FRAME_GROUPS);

    features
}

pub fn load_layered_manifest_with_config(
    dialect_name: &str,
    config: &RegistryConfig,
) -> Result<HashSet<FeatureId>> {
    let mut feature_map: HashMap<String, bool> = HashMap::new();

    let base_features = load_dialect_manifest_with_config(dialect_name, config)?;
    for feature_id in base_features {
        feature_map.insert(feature_id.as_str().to_string(), true);
    }

    if let Some(profile_name) = config.profile()
        && let Ok(profile_overrides) = load_profile_config(profile_name)
    {
        for (feature_id, enabled) in profile_overrides {
            feature_map.insert(feature_id, enabled);
        }
    }

    for (feature_id, enabled) in config.feature_overrides() {
        feature_map.insert(feature_id.clone(), *enabled);
    }

    let mut enabled_features = HashSet::new();
    for (feature_name, enabled) in feature_map {
        if enabled && let Some(feature_id) = lookup_feature_id(&feature_name) {
            enabled_features.insert(feature_id);
        }
    }

    Ok(enabled_features)
}

fn load_profile_config(profile_name: &str) -> Result<HashMap<String, bool>> {
    profiles::get_profile(profile_name).ok_or_else(|| CapabilityError::Internal {
        message: format!("Unknown profile: {}", profile_name),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::F001_BASIC_SQL;

    #[test]
    fn test_load_postgres_manifest() {
        let result = load_dialect_manifest("postgres");
        assert!(result.is_ok(), "Should load postgres manifest");

        let features = result.unwrap();
        assert!(!features.is_empty(), "Should have enabled features");

        assert!(
            features.contains(&FeatureId("PG_SERIAL")),
            "PostgreSQL should support PG_SERIAL"
        );
        assert!(
            features.contains(&FeatureId("PG_JSONB_OPERATORS")),
            "PostgreSQL should support PG_JSONB_OPERATORS"
        );
    }

    #[test]
    fn test_load_bigquery_manifest() {
        let result = load_dialect_manifest("bigquery");
        assert!(result.is_ok(), "Should load bigquery manifest");

        let features = result.unwrap();
        assert!(!features.is_empty(), "Should have enabled features");

        assert!(
            features.contains(&FeatureId("BQ_SAFE_FUNCTIONS")),
            "BigQuery should support BQ_SAFE_FUNCTIONS"
        );
        assert!(
            features.contains(&FeatureId("BQ_QUALIFY_CLAUSE")),
            "BigQuery should support BQ_QUALIFY_CLAUSE"
        );
    }

    #[test]
    fn test_postgres_excludes_bigquery_features() {
        let result = load_dialect_manifest("postgres");
        assert!(result.is_ok());

        let features = result.unwrap();

        assert!(
            !features.contains(&FeatureId("BQ_QUALIFY_CLAUSE")),
            "PostgreSQL should not support BQ_QUALIFY_CLAUSE"
        );
    }

    #[test]
    fn test_bigquery_excludes_postgres_features() {
        let result = load_dialect_manifest("bigquery");
        assert!(result.is_ok());

        let features = result.unwrap();

        assert!(
            !features.contains(&FeatureId("PG_JSONB_OPERATORS")),
            "BigQuery should not support PG_JSONB_OPERATORS"
        );
    }

    #[test]
    fn test_default_capabilities_not_empty() {
        let features = default_capabilities();
        assert!(
            !features.is_empty(),
            "Default capabilities should not be empty"
        );
        assert!(
            features.contains(&F001_BASIC_SQL),
            "Default should include F001_BASIC_SQL"
        );
    }

    #[test]
    fn test_unsupported_dialect() {
        let result = load_dialect_manifest("unknown_dialect");
        assert!(result.is_err(), "Unknown dialect should produce error");
    }
}
