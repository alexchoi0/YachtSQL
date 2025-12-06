//! Core dialect infrastructure for SQL dialect support.

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

use yachtsql_capability::{
    CapabilitySnapshot, FeatureId, FeatureRegistry, Result as CapabilityResult,
};
use yachtsql_parser::DialectType;

pub type FeatureToggle = (FeatureId, bool);

pub struct DialectManifest<'a> {
    pub dialect: DialectType,
    pub version: &'static str,
    pub core_features: &'a [FeatureToggle],
    pub optional_features: &'a [FeatureToggle],
    pub extensions: &'a [FeatureToggle],
}

impl<'a> DialectManifest<'a> {
    pub fn snapshot(&self) -> CapabilityResult<CapabilitySnapshot> {
        let mut registry = FeatureRegistry::new(self.dialect);
        register_core_features(&mut registry, self.core_features.iter().copied())?;
        register_optional_features(&mut registry, self.optional_features.iter().copied())?;
        register_extension_features(&mut registry, self.extensions.iter().copied())?;
        Ok(registry.snapshot_view())
    }
}

pub fn register_core_features<I>(registry: &mut FeatureRegistry, entries: I) -> CapabilityResult<()>
where
    I: IntoIterator<Item = FeatureToggle>,
{
    register_features(registry, entries)
}

pub fn register_optional_features<I>(
    registry: &mut FeatureRegistry,
    entries: I,
) -> CapabilityResult<()>
where
    I: IntoIterator<Item = FeatureToggle>,
{
    register_features(registry, entries)
}

pub fn register_extension_features<I>(
    registry: &mut FeatureRegistry,
    entries: I,
) -> CapabilityResult<()>
where
    I: IntoIterator<Item = FeatureToggle>,
{
    register_features(registry, entries)
}

fn register_features<I>(registry: &mut FeatureRegistry, entries: I) -> CapabilityResult<()>
where
    I: IntoIterator<Item = FeatureToggle>,
{
    let enabled = entries
        .into_iter()
        .filter_map(|(feature, enabled)| enabled.then_some(feature));
    registry.enable_features(enabled)
}

pub fn snapshot_from_sections(
    dialect: DialectType,
    core: &[FeatureToggle],
    optional: &[FeatureToggle],
    extensions: &[FeatureToggle],
) -> CapabilityResult<CapabilitySnapshot> {
    DialectManifest {
        dialect,
        version: "embedded",
        core_features: core,
        optional_features: optional,
        extensions,
    }
    .snapshot()
}
