use yachtsql_common::error::Result;

use crate::optimizer::plan::LogicalPlan;
use crate::optimizer::rule::OptimizationRule;

pub struct RangeMerge;

impl RangeMerge {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RangeMerge {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizationRule for RangeMerge {
    fn name(&self) -> &str {
        "range_merge"
    }

    fn optimize(&self, _plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        Ok(None)
    }
}
