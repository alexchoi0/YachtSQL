//! Query optimizer and plan transformation rules.

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

pub mod catalog;
pub mod classifier;
pub mod cost_model;
pub mod grouping_sets;
pub mod ordering;
pub mod phase;
pub mod priority;
pub mod properties;
pub mod rule;
pub mod rules;
pub mod statistics;
pub mod telemetry;
pub mod visitor;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub use catalog::{CatalogRef, EmptyCatalog, IndexCatalog, IndexInfo, IndexType};
pub use classifier::{ComplexityLevel, QueryComplexity};
pub use cost_model::{AggregateStrategy, Cost, CostModel, JoinStrategy, TableStats};
pub use ordering::{OrderingProperty, OrderingRequirement, SortColumn};
pub use phase::{Phase, PhaseBuilder, PhaseConfig, PhasedRule, StorageType};
pub use priority::{RuleTier, rule_priority, rule_tier};
pub use rule::{OptimizationRule, RuleApplication};
pub use statistics::{ColumnStatistics, Histogram, StatisticsRegistry, TableStatistics};
pub use telemetry::{OptimizerTelemetry, RuleStats};
pub use visitor::{PlanRewriter, PlanVisitor};
use yachtsql_common::error::Result;
pub use yachtsql_ir::expr::{BinaryOp, Expr, OrderByExpr, UnaryOp};
pub use yachtsql_ir::plan::{LogicalPlan, PlanNode};
pub use yachtsql_ir::{expr, plan};

fn telemetry_default_enabled() -> bool {
    match std::env::var("YACHTSQL_OPTIMIZER_TELEMETRY") {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => cfg!(debug_assertions),
        },
        Err(_) => cfg!(debug_assertions),
    }
}

fn default_phase_config(phase: Phase) -> PhaseConfig {
    let budget_micros = priority::default_phase_budget_micros(phase);

    match phase {
        Phase::Simplification => PhaseConfig::new().with_max_iterations(5),
        Phase::Pushdown => PhaseConfig::new().with_max_iterations(6),
        Phase::Reordering => PhaseConfig::new().with_max_iterations(4),
        Phase::Cleanup => PhaseConfig::new().with_max_iterations(2),
    }
    .with_telemetry(true)
    .with_time_budget_micros(budget_micros)
}

fn phase_for_name(name: &str) -> Phase {
    match name {
        "constant_folding"
        | "boolean_simplification"
        | "expression_normalization"
        | "null_propagation"
        | "in_list_conversion"
        | "RemoveTrueFilters"
        | "common_subexpression_elimination" => Phase::Simplification,

        "predicate_pushdown"
        | "projection_pushdown"
        | "limit_pushdown"
        | "partition_pruning"
        | "aggregate_pushdown" => Phase::Pushdown,

        "join_reorder" | "subquery_flattening" | "materialization_points" => Phase::Reordering,

        "dead_code_elimination"
        | "distinct_elimination"
        | "eliminate_redundant"
        | "filter_merge"
        | "FilterMergeV2"
        | "union_optimization"
        | "window_optimization"
        | "interesting_order"
        | "sort_elimination" => Phase::Cleanup,

        _ => Phase::Simplification,
    }
}

fn infer_phase(rule: &dyn OptimizationRule) -> Phase {
    phase_for_name(rule.name())
}

pub mod optimizer {
    pub use yachtsql_ir::plan::{LogicalPlan, PlanNode};

    pub mod expr {
        pub use yachtsql_ir::expr::*;
    }

    pub mod plan {
        pub use yachtsql_ir::plan::*;
    }

    pub mod rule {
        pub use crate::rule::*;
    }
}

pub struct Optimizer {
    inner: MultiPhaseOptimizer,
    phase_configs: BTreeMap<Phase, PhaseConfig>,
}

impl Optimizer {
    pub fn new() -> Self {
        let telemetry_enabled = telemetry_default_enabled();
        let mut optimizer = Self {
            inner: MultiPhaseOptimizer::new(),
            phase_configs: BTreeMap::new(),
        };

        for phase in Phase::all() {
            let mut config = default_phase_config(phase);
            if !telemetry_enabled {
                config = config.with_telemetry(false);
            }
            optimizer.phase_configs.insert(phase, config);
            optimizer.inner = optimizer.inner.configure_phase(phase, config);
        }

        optimizer
    }

    pub fn disabled() -> Self {
        let mut optimizer = Self {
            inner: MultiPhaseOptimizer::new(),
            phase_configs: BTreeMap::new(),
        };
        for phase in Phase::all() {
            let config = PhaseConfig::default().with_max_iterations(0);
            optimizer.phase_configs.insert(phase, config);
            optimizer.inner = optimizer.inner.configure_phase(phase, config);
        }
        optimizer
    }

    pub fn with_rule(mut self, rule: Box<dyn OptimizationRule>) -> Self {
        let inferred = infer_phase(rule.as_ref());
        self.inner = self.inner.add_rule(rule, inferred);
        self
    }

    pub fn with_rule_in_phase(mut self, rule: Box<dyn OptimizationRule>, phase: Phase) -> Self {
        self.inner = self.inner.add_rule(rule, phase);
        self
    }

    pub fn with_max_iterations(mut self, max_iterations: usize) -> Self {
        self = self.configure_all_phases(|_, config| config.with_max_iterations(max_iterations));
        self
    }

    pub fn configure_phase(mut self, phase: Phase, config: PhaseConfig) -> Self {
        self.phase_configs.insert(phase, config);
        self.inner = self.inner.configure_phase(phase, config);
        self
    }

    pub fn with_telemetry(mut self, enabled: bool) -> Self {
        self.inner = self.inner.with_telemetry(enabled);
        self = self.configure_all_phases(|_, mut config| {
            config.enable_telemetry = enabled;
            config
        });
        self
    }

    pub fn telemetry(&self) -> &OptimizerTelemetry {
        self.inner.telemetry()
    }

    pub fn clear_telemetry(&mut self) {
        self.inner.clear_telemetry();
    }

    pub fn optimize(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.inner.optimize(plan)
    }

    fn configure_all_phases<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(Phase, PhaseConfig) -> PhaseConfig,
    {
        for phase in Phase::all() {
            let current = self
                .phase_configs
                .get(&phase)
                .copied()
                .unwrap_or_else(|| default_phase_config(phase));
            let updated = f(phase, current);
            self.phase_configs.insert(phase, updated);
            self.inner = self.inner.configure_phase(phase, updated);
        }
        self
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MultiPhaseOptimizer {
    builder: PhaseBuilder,
    telemetry: OptimizerTelemetry,
    global_time_budget: Duration,
    enable_fast_path: bool,
    storage_type: StorageType,
}

impl MultiPhaseOptimizer {
    pub fn new() -> Self {
        let telemetry_enabled = telemetry_default_enabled();
        Self {
            builder: PhaseBuilder::new().with_telemetry(telemetry_enabled),
            telemetry: OptimizerTelemetry::new(telemetry_enabled),
            global_time_budget: Duration::from_millis(1),
            enable_fast_path: true,
            storage_type: StorageType::Row,
        }
    }

    pub fn with_time_budget(mut self, budget: Duration) -> Self {
        self.global_time_budget = budget;
        self
    }

    pub fn with_fast_path(mut self, enabled: bool) -> Self {
        self.enable_fast_path = enabled;
        self
    }

    pub fn with_storage_type(mut self, storage_type: StorageType) -> Self {
        self.storage_type = storage_type;
        self
    }

    pub fn add_rule(mut self, rule: Box<dyn OptimizationRule>, phase: Phase) -> Self {
        self.builder = self.builder.add_rule(rule, phase);
        self
    }

    pub fn configure_phase(mut self, phase: Phase, config: PhaseConfig) -> Self {
        self.builder = self.builder.configure_phase(phase, config);
        self
    }

    pub fn with_telemetry(mut self, enabled: bool) -> Self {
        self.builder = self.builder.with_telemetry(enabled);
        self.telemetry = OptimizerTelemetry::new(enabled);
        self
    }

    pub fn telemetry(&self) -> &OptimizerTelemetry {
        &self.telemetry
    }

    pub fn clear_telemetry(&mut self) {
        self.telemetry.clear();
    }

    pub fn optimize(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let optimization_start = Instant::now();

        if self.enable_fast_path {
            let complexity = QueryComplexity::analyze(&plan);

            if !complexity.needs_optimization() {
                self.telemetry
                    .record_total_time(optimization_start.elapsed());
                return Ok(plan);
            }

            let recommended_phases = complexity.recommended_phases();
            let mut current_plan = plan;

            for phase in recommended_phases {
                if optimization_start.elapsed() >= self.global_time_budget {
                    break;
                }

                let config = self
                    .builder
                    .get_config(&phase)
                    .with_storage_type(self.storage_type);

                current_plan =
                    self.optimize_phase(current_plan, phase, &config, &optimization_start)?;
            }

            self.telemetry
                .record_total_time(optimization_start.elapsed());
            return Ok(current_plan);
        }

        let mut current_plan = plan;
        for phase in Phase::all() {
            if optimization_start.elapsed() >= self.global_time_budget {
                break;
            }

            let config = self
                .builder
                .get_config(&phase)
                .with_storage_type(self.storage_type);

            current_plan =
                self.optimize_phase(current_plan, phase, &config, &optimization_start)?;
        }

        self.telemetry
            .record_total_time(optimization_start.elapsed());
        Ok(current_plan)
    }

    fn optimize_phase(
        &mut self,
        mut plan: LogicalPlan,
        phase: Phase,
        config: &PhaseConfig,
        optimization_start: &Instant,
    ) -> Result<LogicalPlan> {
        let mut iteration = 0;
        let phase_start = Instant::now();

        let phase_budget = config
            .time_budget()
            .unwrap_or_else(|| Duration::from_micros(priority::default_phase_budget_micros(phase)));

        while iteration < config.max_iterations {
            if optimization_start.elapsed() >= self.global_time_budget {
                break;
            }

            if phase_start.elapsed() >= phase_budget {
                break;
            }

            self.telemetry.start_iteration();
            self.telemetry.record_phase_iteration(phase);
            let mut changed = false;

            let mut phase_rules: Vec<_> = self.builder.rules_for_phase(phase).into_iter().collect();

            if phase_rules.is_empty() {
                break;
            }

            phase_rules.sort_by_cached_key(|r| {
                std::cmp::Reverse(priority::rule_priority(r.rule.name(), config.storage_type))
            });

            for phased_rule in phase_rules {
                let global_remaining = self
                    .global_time_budget
                    .saturating_sub(optimization_start.elapsed());
                let phase_remaining = phase_budget.saturating_sub(phase_start.elapsed());

                let remaining = global_remaining.min(phase_remaining);
                let remaining_micros = remaining.as_micros() as u64;

                let fast_mode = remaining_micros < 200;
                if priority::should_skip_rule(phased_rule.rule.name(), remaining_micros, fast_mode)
                {
                    continue;
                }

                if remaining_micros < 50 {
                    break;
                }

                let rule_start = Instant::now();
                let result = phased_rule.rule.optimize(&plan)?;
                let rule_duration = rule_start.elapsed();

                let applied = result.is_some();
                if let Some(new_plan) = result {
                    plan = new_plan;
                    changed = true;
                }

                if config.enable_telemetry {
                    self.telemetry.record_rule(
                        phase,
                        phased_rule.rule.name(),
                        rule_duration,
                        applied,
                    );
                }
            }

            if !changed {
                break;
            }

            iteration += 1;
        }

        self.telemetry
            .record_phase_time(phase, phase_start.elapsed());
        Ok(plan)
    }
}

impl Default for MultiPhaseOptimizer {
    fn default() -> Self {
        Self::new()
    }
}
