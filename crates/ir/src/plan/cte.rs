use serde::{Deserialize, Serialize};

use super::LogicalPlan;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CteDefinition {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: Box<LogicalPlan>,
    pub recursive: bool,
    pub materialized: Option<bool>,
}
