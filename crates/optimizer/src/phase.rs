use std::time::Duration;

use crate::rule::OptimizationRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageType {
    Row,
    Columnar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Phase {
    Simplification,
    Pushdown,
    Reordering,
    Cleanup,
}

impl Phase {
    pub fn name(&self) -> &'static str {
        match self {
            Phase::Simplification => "Simplification",
            Phase::Pushdown => "Pushdown",
            Phase::Reordering => "Reordering",
            Phase::Cleanup => "Cleanup",
        }
    }

    pub fn all() -> Vec<Phase> {
        vec![
            Phase::Simplification,
            Phase::Pushdown,
            Phase::Reordering,
            Phase::Cleanup,
        ]
    }
}

pub struct PhasedRule {
    pub rule: Box<dyn OptimizationRule>,
    pub phase: Phase,
}

impl PhasedRule {
    pub fn new(rule: Box<dyn OptimizationRule>, phase: Phase) -> Self {
        Self { rule, phase }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhaseConfig {
    pub max_iterations: usize,
    pub enable_telemetry: bool,
    pub time_budget_micros: Option<u64>,
    pub storage_type: StorageType,
}

impl PhaseConfig {
    pub fn new() -> Self {
        Self {
            max_iterations: 10,
            enable_telemetry: true,
            time_budget_micros: None,
            storage_type: StorageType::Row,
        }
    }

    pub fn with_max_iterations(mut self, max_iterations: usize) -> Self {
        self.max_iterations = max_iterations;
        self
    }

    pub fn with_telemetry(mut self, enable: bool) -> Self {
        self.enable_telemetry = enable;
        self
    }

    pub fn with_time_budget_micros(mut self, micros: u64) -> Self {
        self.time_budget_micros = Some(micros);
        self
    }

    pub fn with_time_budget(mut self, duration: Duration) -> Self {
        self.time_budget_micros = Some(duration.as_micros() as u64);
        self
    }

    pub fn with_storage_type(mut self, storage_type: StorageType) -> Self {
        self.storage_type = storage_type;
        self
    }

    pub fn time_budget(&self) -> Option<Duration> {
        self.time_budget_micros.map(Duration::from_micros)
    }
}

impl Default for PhaseConfig {
    fn default() -> Self {
        Self::new()
    }
}

pub struct PhaseBuilder {
    rules: Vec<PhasedRule>,
    configs: std::collections::HashMap<Phase, PhaseConfig>,
    global_telemetry: bool,
}

impl PhaseBuilder {
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            configs: std::collections::HashMap::new(),
            global_telemetry: false,
        }
    }

    pub fn add_rule(mut self, rule: Box<dyn OptimizationRule>, phase: Phase) -> Self {
        self.rules.push(PhasedRule::new(rule, phase));
        self
    }

    pub fn configure_phase(mut self, phase: Phase, config: PhaseConfig) -> Self {
        self.configs.insert(phase, config);
        self
    }

    pub fn with_telemetry(mut self, enabled: bool) -> Self {
        self.global_telemetry = enabled;
        self
    }

    pub fn get_config(&self, phase: &Phase) -> PhaseConfig {
        self.configs.get(phase).copied().unwrap_or_default()
    }

    pub fn rules_for_phase(&self, phase: Phase) -> Vec<&PhasedRule> {
        self.rules.iter().filter(|r| r.phase == phase).collect()
    }

    pub fn telemetry_enabled(&self) -> bool {
        self.global_telemetry
    }

    pub fn rules(&self) -> &[PhasedRule] {
        &self.rules
    }
}

impl Default for PhaseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_phase_ordering() {
        let phases = Phase::all();
        assert_eq!(phases.len(), 4);
        assert_eq!(phases[0], Phase::Simplification);
        assert_eq!(phases[1], Phase::Pushdown);
        assert_eq!(phases[2], Phase::Reordering);
        assert_eq!(phases[3], Phase::Cleanup);
    }

    #[test]
    fn test_phase_names() {
        assert_eq!(Phase::Simplification.name(), "Simplification");
        assert_eq!(Phase::Pushdown.name(), "Pushdown");
        assert_eq!(Phase::Reordering.name(), "Reordering");
        assert_eq!(Phase::Cleanup.name(), "Cleanup");
    }

    #[test]
    fn test_phase_config_builder() {
        let config = PhaseConfig::new()
            .with_max_iterations(5)
            .with_telemetry(false)
            .with_time_budget_micros(1000)
            .with_storage_type(StorageType::Columnar);

        assert_eq!(config.max_iterations, 5);
        assert!(!config.enable_telemetry);
        assert_eq!(config.time_budget_micros, Some(1000));
        assert_eq!(config.storage_type, StorageType::Columnar);
    }

    #[test]
    fn test_storage_type_variants() {
        assert_ne!(StorageType::Row, StorageType::Columnar);
    }

    #[test]
    fn test_phase_builder() {
        let builder = PhaseBuilder::new().with_telemetry(true).configure_phase(
            Phase::Simplification,
            PhaseConfig::new().with_max_iterations(5),
        );

        assert!(builder.telemetry_enabled());

        let config = builder.get_config(&Phase::Simplification);
        assert_eq!(config.max_iterations, 5);
    }
}
