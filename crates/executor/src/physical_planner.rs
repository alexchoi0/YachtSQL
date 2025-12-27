use yachtsql_optimizer::OptimizedLogicalPlan;

use crate::concurrent_catalog::ConcurrentCatalog;
use crate::concurrent_session::ConcurrentSession;
use crate::plan::{BoundType, ExecutionHints, PARALLEL_ROW_THRESHOLD, PhysicalPlan};

pub struct PhysicalPlanner<'a> {
    catalog: &'a ConcurrentCatalog,
    session: &'a ConcurrentSession,
}

impl<'a> PhysicalPlanner<'a> {
    pub fn new(catalog: &'a ConcurrentCatalog, session: &'a ConcurrentSession) -> Self {
        Self { catalog, session }
    }

    pub fn plan(&self, logical: &OptimizedLogicalPlan) -> PhysicalPlan {
        let mut plan = PhysicalPlan::from_physical(logical);
        plan.populate_row_counts(self.catalog);
        self.compute_hints(&mut plan);
        plan
    }

    fn is_parallel_enabled(&self) -> bool {
        if let Some(val) = self.session.get_variable("PARALLEL_EXECUTION") {
            return val.as_bool().unwrap_or(true);
        }
        if let Some(val) = self.session.get_system_variable("PARALLEL_EXECUTION") {
            return val.as_bool().unwrap_or(true);
        }
        match std::env::var("YACHTSQL_PARALLEL_EXECUTION") {
            Ok(val) => !val.eq_ignore_ascii_case("false") && val != "0",
            Err(_) => true,
        }
    }

    fn compute_hints(&self, plan: &mut PhysicalPlan) {
        let parallel_enabled = self.is_parallel_enabled();
        self.compute_hints_recursive(plan, parallel_enabled);
    }

    fn compute_hints_recursive(&self, plan: &mut PhysicalPlan, parallel_enabled: bool) {
        match plan {
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                parallel,
                hints,
                ..
            }
            | PhysicalPlan::HashJoin {
                left,
                right,
                parallel,
                hints,
                ..
            } => {
                self.compute_hints_recursive(left, parallel_enabled);
                self.compute_hints_recursive(right, parallel_enabled);
                *hints = self.binary_join_hints(left, right, *parallel, parallel_enabled);
            }

            PhysicalPlan::CrossJoin {
                left,
                right,
                parallel,
                hints,
                ..
            } => {
                self.compute_hints_recursive(left, parallel_enabled);
                self.compute_hints_recursive(right, parallel_enabled);
                let bound = Self::binary_bound_type(left, right);
                *hints = ExecutionHints {
                    parallel: parallel_enabled && *parallel && bound == BoundType::Compute,
                    bound_type: bound,
                    estimated_rows: left.estimate_rows().saturating_mul(right.estimate_rows()),
                };
            }

            PhysicalPlan::Intersect {
                left,
                right,
                parallel,
                hints,
                ..
            } => {
                self.compute_hints_recursive(left, parallel_enabled);
                self.compute_hints_recursive(right, parallel_enabled);
                let bound = Self::binary_bound_type(left, right);
                *hints = ExecutionHints {
                    parallel: parallel_enabled && *parallel && bound == BoundType::Compute,
                    bound_type: bound,
                    estimated_rows: left.estimate_rows().min(right.estimate_rows()),
                };
            }

            PhysicalPlan::Except {
                left,
                right,
                parallel,
                hints,
                ..
            } => {
                self.compute_hints_recursive(left, parallel_enabled);
                self.compute_hints_recursive(right, parallel_enabled);
                let bound = Self::binary_bound_type(left, right);
                *hints = ExecutionHints {
                    parallel: parallel_enabled && *parallel && bound == BoundType::Compute,
                    bound_type: bound,
                    estimated_rows: left.estimate_rows(),
                };
            }

            PhysicalPlan::Union {
                inputs,
                parallel,
                hints,
                ..
            } => {
                for input in inputs.iter_mut() {
                    self.compute_hints_recursive(input, parallel_enabled);
                }
                let bound = Self::union_bound_type(inputs);
                *hints = ExecutionHints {
                    parallel: parallel_enabled && *parallel && bound == BoundType::Compute,
                    bound_type: bound,
                    estimated_rows: inputs.iter().map(|p| p.estimate_rows()).sum(),
                };
            }

            PhysicalPlan::HashAggregate { input, hints, .. }
            | PhysicalPlan::Window { input, hints, .. }
            | PhysicalPlan::Sort { input, hints, .. } => {
                self.compute_hints_recursive(input, parallel_enabled);
                *hints = ExecutionHints {
                    parallel: false,
                    bound_type: BoundType::Compute,
                    estimated_rows: input.estimate_rows(),
                };
            }

            PhysicalPlan::WithCte {
                ctes,
                body,
                parallel_ctes,
                hints,
            } => {
                self.compute_hints_recursive(body, parallel_enabled);
                *parallel_ctes = self.compute_cte_parallelism(ctes, parallel_enabled);
                *hints = ExecutionHints {
                    parallel: !parallel_ctes.is_empty(),
                    bound_type: body.bound_type(),
                    estimated_rows: body.estimate_rows(),
                };
            }

            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::TopN { input, .. }
            | PhysicalPlan::Sample { input, .. }
            | PhysicalPlan::Distinct { input, .. }
            | PhysicalPlan::Qualify { input, .. }
            | PhysicalPlan::Unnest { input, .. } => {
                self.compute_hints_recursive(input, parallel_enabled);
            }

            PhysicalPlan::Insert { source, .. } => {
                self.compute_hints_recursive(source, parallel_enabled);
            }

            PhysicalPlan::Update {
                from: Some(from), ..
            } => {
                self.compute_hints_recursive(from, parallel_enabled);
            }

            PhysicalPlan::Merge { source, .. } => {
                self.compute_hints_recursive(source, parallel_enabled);
            }

            _ => {}
        }
    }

    fn binary_join_hints(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        parallel: bool,
        parallel_enabled: bool,
    ) -> ExecutionHints {
        let bound = Self::binary_bound_type(left, right);
        ExecutionHints {
            parallel: parallel_enabled && parallel && bound == BoundType::Compute,
            bound_type: bound,
            estimated_rows: left.estimate_rows().saturating_add(right.estimate_rows()),
        }
    }

    fn compute_cte_parallelism(
        &self,
        ctes: &[yachtsql_ir::CteDefinition],
        parallel_enabled: bool,
    ) -> Vec<usize> {
        if !parallel_enabled {
            return vec![];
        }
        ctes.iter()
            .enumerate()
            .filter(|(_, cte)| !cte.recursive)
            .filter(|(_, cte)| {
                if let Ok(optimized) = yachtsql_optimizer::optimize(&cte.query) {
                    let mut plan = PhysicalPlan::from_physical(&optimized);
                    plan.populate_row_counts(self.catalog);
                    plan.bound_type() == BoundType::Compute
                        && plan.estimate_rows() >= PARALLEL_ROW_THRESHOLD
                } else {
                    false
                }
            })
            .map(|(i, _)| i)
            .collect()
    }

    fn binary_bound_type(left: &PhysicalPlan, right: &PhysicalPlan) -> BoundType {
        if left.bound_type() == BoundType::Memory && right.bound_type() == BoundType::Memory {
            BoundType::Memory
        } else {
            BoundType::Compute
        }
    }

    fn union_bound_type(inputs: &[PhysicalPlan]) -> BoundType {
        let compute_count = inputs
            .iter()
            .filter(|p| p.bound_type() == BoundType::Compute)
            .count();
        if compute_count >= 2 {
            BoundType::Compute
        } else {
            BoundType::Memory
        }
    }
}
