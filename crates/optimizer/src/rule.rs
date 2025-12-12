use yachtsql_common::error::Result;

use crate::LogicalPlan;

pub trait OptimizationRule: Send + Sync {
    fn name(&self) -> &str;

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>>;

    fn is_applicable(&self, plan: &LogicalPlan) -> bool {
        let _ = plan;
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleApplication {
    Applied,
    NotApplied,
    Error,
}

impl RuleApplication {
    pub fn was_applied(&self) -> bool {
        matches!(self, RuleApplication::Applied)
    }

    pub fn was_not_applied(&self) -> bool {
        matches!(self, RuleApplication::NotApplied)
    }

    pub fn is_error(&self) -> bool {
        matches!(self, RuleApplication::Error)
    }
}

impl From<Option<LogicalPlan>> for RuleApplication {
    fn from(result: Option<LogicalPlan>) -> Self {
        match result {
            Some(_) => RuleApplication::Applied,
            None => RuleApplication::NotApplied,
        }
    }
}

impl From<Result<Option<LogicalPlan>>> for RuleApplication {
    fn from(result: Result<Option<LogicalPlan>>) -> Self {
        match result {
            Ok(Some(_)) => RuleApplication::Applied,
            Ok(None) => RuleApplication::NotApplied,
            Err(_) => RuleApplication::Error,
        }
    }
}
