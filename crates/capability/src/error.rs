use std::fmt;

use yachtsql_common::diagnostics::{FEATURE_NOT_SUPPORTED, INTERNAL_ERROR, SqlState};

pub type Result<T> = std::result::Result<T, CapabilityError>;

#[derive(Debug, Clone)]
pub enum CapabilityError {
    FeatureNotFound {
        feature_id: String,
    },

    UnsatisfiedDependency {
        feature_id: String,
        missing_dependencies: Vec<String>,
    },

    CircularDependency {
        feature_id: String,
        cycle_path: Vec<String>,
    },

    DependentFeaturesEnabled {
        feature_id: String,
        dependents: Vec<String>,
    },

    ManifestLoadError {
        path: String,
        reason: String,
    },

    InvalidManifest {
        path: String,
        reason: String,
    },

    UnknownFeatureInManifest {
        feature_id: String,
        section: String,
        path: String,
    },

    DialectMismatch {
        feature_id: String,
        dialect: String,
        supported_dialects: Vec<String>,
    },

    UnsupportedDialect {
        dialect: String,
    },

    InvalidFeatureId {
        feature_id: String,
        reason: String,
    },

    Internal {
        message: String,
    },
}

impl CapabilityError {
    pub fn sqlstate(&self) -> SqlState {
        match self {
            CapabilityError::FeatureNotFound { .. } => FEATURE_NOT_SUPPORTED,
            CapabilityError::UnsatisfiedDependency { .. } => FEATURE_NOT_SUPPORTED,
            CapabilityError::CircularDependency { .. } => INTERNAL_ERROR,
            CapabilityError::DependentFeaturesEnabled { .. } => FEATURE_NOT_SUPPORTED,
            CapabilityError::ManifestLoadError { .. } => INTERNAL_ERROR,
            CapabilityError::InvalidManifest { .. } => INTERNAL_ERROR,
            CapabilityError::UnknownFeatureInManifest { .. } => FEATURE_NOT_SUPPORTED,
            CapabilityError::DialectMismatch { .. } => FEATURE_NOT_SUPPORTED,
            CapabilityError::UnsupportedDialect { .. } => FEATURE_NOT_SUPPORTED,
            CapabilityError::InvalidFeatureId { .. } => INTERNAL_ERROR,
            CapabilityError::Internal { .. } => INTERNAL_ERROR,
        }
    }

    pub fn message(&self) -> String {
        match self {
            CapabilityError::FeatureNotFound { feature_id } => {
                format!("Feature '{}' not found in registry", feature_id)
            }
            CapabilityError::UnsatisfiedDependency {
                feature_id,
                missing_dependencies,
            } => {
                format!(
                    "Cannot enable '{}' - missing dependencies: {}. Enable these features first.",
                    feature_id,
                    missing_dependencies.join(", ")
                )
            }
            CapabilityError::CircularDependency {
                feature_id,
                cycle_path,
            } => {
                format!(
                    "Circular dependency detected for '{}': {}. Check feature dependencies.",
                    feature_id,
                    cycle_path.join(" -> ")
                )
            }
            CapabilityError::DependentFeaturesEnabled {
                feature_id,
                dependents,
            } => {
                format!(
                    "Cannot disable '{}' - required by: {}. Disable dependent features first.",
                    feature_id,
                    dependents.join(", ")
                )
            }
            CapabilityError::ManifestLoadError { path, reason } => {
                format!("Failed to load manifest file '{}': {}", path, reason)
            }
            CapabilityError::InvalidManifest { path, reason } => {
                format!("Invalid manifest file '{}': {}", path, reason)
            }
            CapabilityError::UnknownFeatureInManifest {
                feature_id,
                section,
                path,
            } => {
                format!(
                    "Unknown feature '{}' in [{}] section of manifest '{}'. Check feature ID spelling.",
                    feature_id, section, path
                )
            }
            CapabilityError::DialectMismatch {
                feature_id,
                dialect,
                supported_dialects,
            } => {
                format!(
                    "Feature '{}' not supported by dialect '{}'. Supported dialects: {}",
                    feature_id,
                    dialect,
                    supported_dialects.join(", ")
                )
            }
            CapabilityError::UnsupportedDialect { dialect } => {
                format!(
                    "Unsupported SQL dialect: '{}'. Supported dialects: PostgreSQL, BigQuery",
                    dialect
                )
            }
            CapabilityError::InvalidFeatureId { feature_id, reason } => {
                format!("Invalid feature ID '{}': {}", feature_id, reason)
            }
            CapabilityError::Internal { message } => {
                format!("Internal error: {}", message)
            }
        }
    }
}

impl fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (SQLSTATE {})", self.message(), self.sqlstate())
    }
}

impl std::error::Error for CapabilityError {}

impl From<CapabilityError> for yachtsql_common::error::Error {
    fn from(err: CapabilityError) -> Self {
        match err {
            CapabilityError::FeatureNotFound { feature_id } => {
                yachtsql_common::error::Error::unsupported_feature(format!(
                    "Unknown feature: {}",
                    feature_id
                ))
            }
            CapabilityError::UnsatisfiedDependency {
                feature_id,
                missing_dependencies,
            } => yachtsql_common::error::Error::unsupported_feature(format!(
                "Cannot enable {} - missing dependencies: {}",
                feature_id,
                missing_dependencies.join(", ")
            )),
            CapabilityError::CircularDependency {
                feature_id,
                cycle_path,
            } => yachtsql_common::error::Error::InternalError(format!(
                "Circular dependency for {}: {}",
                feature_id,
                cycle_path.join(" -> ")
            )),
            CapabilityError::DependentFeaturesEnabled {
                feature_id,
                dependents,
            } => yachtsql_common::error::Error::unsupported_feature(format!(
                "Cannot disable {} - required by: {}",
                feature_id,
                dependents.join(", ")
            )),
            CapabilityError::ManifestLoadError { path, reason } => {
                yachtsql_common::error::Error::InternalError(format!(
                    "Failed to read manifest file {}: {}",
                    path, reason
                ))
            }
            CapabilityError::InvalidManifest { path, reason } => {
                yachtsql_common::error::Error::InternalError(format!(
                    "Failed to parse manifest file {}: {}",
                    path, reason
                ))
            }
            CapabilityError::UnknownFeatureInManifest {
                feature_id,
                section,
                path,
            } => yachtsql_common::error::Error::unsupported_feature(format!(
                "Unknown feature `{}` in [{}] of {}",
                feature_id, section, path
            )),
            CapabilityError::DialectMismatch {
                feature_id,
                dialect,
                ..
            } => yachtsql_common::error::Error::unsupported_feature(format!(
                "Feature {} not supported by dialect {}",
                feature_id, dialect
            )),
            CapabilityError::UnsupportedDialect { dialect } => {
                yachtsql_common::error::Error::unsupported_feature(format!(
                    "Unsupported dialect: {}",
                    dialect
                ))
            }
            CapabilityError::InvalidFeatureId { feature_id, reason } => {
                yachtsql_common::error::Error::InternalError(format!(
                    "Invalid feature ID {}: {}",
                    feature_id, reason
                ))
            }
            CapabilityError::Internal { message } => {
                yachtsql_common::error::Error::InternalError(message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_not_found_error() {
        let err = CapabilityError::FeatureNotFound {
            feature_id: "F999".to_string(),
        };
        assert_eq!(err.sqlstate(), FEATURE_NOT_SUPPORTED);
        assert!(err.message().contains("F999"));
        assert!(err.message().contains("not found"));
    }

    #[test]
    fn test_unsatisfied_dependency_error() {
        let err = CapabilityError::UnsatisfiedDependency {
            feature_id: "T801".to_string(),
            missing_dependencies: vec!["F001".to_string(), "F312".to_string()],
        };
        assert_eq!(err.sqlstate(), FEATURE_NOT_SUPPORTED);
        let msg = err.message();
        assert!(msg.contains("T801"));
        assert!(msg.contains("F001"));
        assert!(msg.contains("F312"));
        assert!(msg.contains("Enable these features first"));
    }

    #[test]
    fn test_circular_dependency_error() {
        let err = CapabilityError::CircularDependency {
            feature_id: "F001".to_string(),
            cycle_path: vec!["F001".to_string(), "F002".to_string(), "F001".to_string()],
        };
        assert_eq!(err.sqlstate(), INTERNAL_ERROR);
        let msg = err.message();
        assert!(msg.contains("Circular dependency"));
        assert!(msg.contains("F001 -> F002 -> F001"));
    }

    #[test]
    fn test_manifest_load_error() {
        let err = CapabilityError::ManifestLoadError {
            path: "/path/to/manifest.toml".to_string(),
            reason: "File not found".to_string(),
        };
        assert_eq!(err.sqlstate(), INTERNAL_ERROR);
        assert!(err.message().contains("/path/to/manifest.toml"));
        assert!(err.message().contains("File not found"));
    }

    #[test]
    fn test_unknown_feature_in_manifest() {
        let err = CapabilityError::UnknownFeatureInManifest {
            feature_id: "INVALID_FEATURE".to_string(),
            section: "core_features".to_string(),
            path: "test.toml".to_string(),
        };
        assert_eq!(err.sqlstate(), FEATURE_NOT_SUPPORTED);
        let msg = err.message();
        assert!(msg.contains("INVALID_FEATURE"));
        assert!(msg.contains("core_features"));
        assert!(msg.contains("Check feature ID spelling"));
    }

    #[test]
    fn test_error_display_includes_sqlstate() {
        let err = CapabilityError::FeatureNotFound {
            feature_id: "F999".to_string(),
        };
        let display = format!("{}", err);
        assert!(display.contains("SQLSTATE"));
        assert!(display.contains("0A000"));
    }

    #[test]
    fn test_conversion_to_core_error() {
        let cap_err = CapabilityError::FeatureNotFound {
            feature_id: "F999".to_string(),
        };
        let core_err: yachtsql_common::error::Error = cap_err.into();
        assert!(matches!(
            core_err,
            yachtsql_common::error::Error::UnsupportedFeature(_)
        ));
    }
}
