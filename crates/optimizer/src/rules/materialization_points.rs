use yachtsql_common::error::Result;

use crate::optimizer::plan::LogicalPlan;
use crate::optimizer::rule::OptimizationRule;

pub struct MaterializationPoints;

impl MaterializationPoints {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MaterializationPoints {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for MaterializationPoints {
    fn name(&self) -> &str {
        "materialization_points"
    }

    fn optimize(&self, _plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(None)
    }
}
