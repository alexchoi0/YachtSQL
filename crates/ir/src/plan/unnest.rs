use serde::{Deserialize, Serialize};

use crate::expr::Expr;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnnestColumn {
    pub expr: Expr,
    pub alias: Option<String>,
    pub with_offset: bool,
    pub offset_alias: Option<String>,
}
