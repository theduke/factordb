pub type AnyError = anyhow::Error;

pub mod data;
pub mod error;
pub mod query;
pub mod schema;

pub mod prelude;

pub mod db;

pub use factor_macros::{Attribute, Class};
