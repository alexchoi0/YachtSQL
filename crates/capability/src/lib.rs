//! Feature capability detection and dialect feature registry.

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

pub mod config;
pub mod error;
pub mod feature;
pub mod feature_defs;
pub mod feature_ids;
pub mod feature_registry_builder;
pub mod manifest_loader;
pub mod manifests;
pub mod profiles;
pub mod registry;
pub mod resolver;
pub mod sql_features;

pub use config::RegistryConfig;
pub use error::{CapabilityError, Result};
pub use feature::{Feature, FeatureCategory};
pub use feature_ids::{FeatureId, *};
pub use manifest_loader::{
    default_capabilities, load_dialect_manifest, load_layered_manifest_with_config, load_manifest,
};
pub use manifests::{bigquery_capabilities, clickhouse_capabilities, postgres_capabilities};
pub use profiles::{development_profile, get_profile, production_profile, testing_profile};
pub use registry::{CapabilitySnapshot, FeatureRegistry};
pub use sql_features::{
    FeatureCategory as SqlFeatureCategory, FeatureMetadata, feature_catalog, find_feature,
};
pub use yachtsql_parser::DialectType;
