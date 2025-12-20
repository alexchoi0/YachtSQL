use serde::{Deserialize, Serialize};

use super::Expr;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortExpr {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: bool,
}
