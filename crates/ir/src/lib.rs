//! Intermediate representation for SQL queries and expressions.

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

pub mod expr;
pub mod function;
pub mod plan;

pub use expr::{BinaryOp, Expr, OrderByExpr, UnaryOp};
pub use function::FunctionName;
pub use plan::{LogicalPlan, PlanNode};
