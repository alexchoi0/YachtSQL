use serde::{Deserialize, Serialize};

use crate::expr::Expr;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedWindowDefinition {
    pub name: String,
    pub window_spec: WindowSpec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<crate::expr::SortExpr>,
    pub frame: Option<crate::expr::WindowFrame>,
}
