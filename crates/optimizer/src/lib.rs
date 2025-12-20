mod physical_plan;
mod planner;

pub use physical_plan::PhysicalPlan;
pub use planner::PhysicalPlanner;
use yachtsql_common::error::Result;
use yachtsql_ir::LogicalPlan;

pub fn optimize(logical: &LogicalPlan) -> Result<PhysicalPlan> {
    PhysicalPlanner::new().plan(logical)
}
