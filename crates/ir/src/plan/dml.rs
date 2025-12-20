use serde::{Deserialize, Serialize};

use crate::expr::Expr;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MergeClause {
    MatchedUpdate {
        condition: Option<Expr>,
        assignments: Vec<crate::schema::Assignment>,
    },
    MatchedDelete {
        condition: Option<Expr>,
    },
    NotMatched {
        condition: Option<Expr>,
        columns: Vec<String>,
        values: Vec<Expr>,
    },
    NotMatchedBySource {
        condition: Option<Expr>,
        assignments: Vec<crate::schema::Assignment>,
    },
    NotMatchedBySourceDelete {
        condition: Option<Expr>,
    },
}
