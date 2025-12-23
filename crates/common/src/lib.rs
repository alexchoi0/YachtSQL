//! Common types and error handling for YachtSQL (BigQuery dialect).

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

pub mod error;
pub mod float_utils;
pub mod result;
pub mod static_cell;
pub mod types;

pub use error::{Error, Result};
pub use result::{ColumnInfo, QueryResult, Row};
pub use static_cell::{LazyStaticRefCell, StaticCell, StaticRefCell};
