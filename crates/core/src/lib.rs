//! Core types and error handling for YachtSQL.

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

pub mod diagnostics;
pub mod error;
pub mod float_utils;
pub mod static_cell;
pub mod types;

#[cfg(test)]
mod value_size_test;

pub use error::{Error, Result};
pub use static_cell::{LazyStaticRefCell, StaticCell, StaticRefCell};
