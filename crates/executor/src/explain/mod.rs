mod plan_formatter;
mod profiler;
mod timing;

pub use plan_formatter::{ExplainOptions, RuntimeStats, format_plan};
pub use profiler::{
    Disabled, Enabled, MetricValue, OperatorId, OperatorMetrics, ProfilerDisabled, ProfilerEnabled,
    ProfilingMode, QueryProfiler,
};
pub use timing::FastTimestamp;
use yachtsql_core::error::Result;

use crate::Table;

pub fn explain_query(
    plan: &yachtsql_optimizer::plan::PlanNode,
    options: ExplainOptions,
) -> Result<Table> {
    plan_formatter::format_plan(plan, options)
}
