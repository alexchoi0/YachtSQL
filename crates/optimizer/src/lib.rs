mod optimized_logical_plan;
mod planner;
#[cfg(test)]
mod tests;

pub use optimized_logical_plan::{OptimizedLogicalPlan, SampleType};
pub use planner::PhysicalPlanner;
use yachtsql_common::error::Result;
use yachtsql_ir::LogicalPlan;

pub fn optimize(logical: &LogicalPlan) -> Result<OptimizedLogicalPlan> {
    PhysicalPlanner::new().plan(logical)
}
