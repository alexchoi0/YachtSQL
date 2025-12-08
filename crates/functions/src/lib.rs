//! SQL function implementations for YachtSQL.

#![allow(clippy::collapsible_if)]
#![allow(clippy::needless_return)]
#![allow(clippy::single_match)]
#![allow(clippy::collapsible_match)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::manual_range_contains)]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

pub mod aggregate;
#[cfg(feature = "approximate")]
pub mod approximate;
pub mod array;
pub mod datetime;
pub mod dialects;
pub mod encoding;
#[cfg(feature = "encryption")]
pub mod encryption;
pub mod fulltext;
pub mod generator;
pub mod geography;
pub mod geometric;
pub mod hstore;
pub mod interval;
#[cfg(feature = "json")]
pub mod json;
pub mod misc;
#[cfg(feature = "network")]
pub mod network;
pub mod polymorphic_table_functions;
pub mod range;
pub mod scalar;
pub mod statistical;
pub mod vector;

mod registry;

pub use aggregate::{Accumulator, AggregateFunction};
pub use registry::FunctionRegistry;
pub use scalar::{ScalarFunction, ScalarFunctionImpl};
