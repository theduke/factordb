pub type AnyError = anyhow::Error;

mod util;

pub mod data;
pub mod error;
pub mod query;
pub mod schema;

pub mod backend;
pub mod registry;

pub mod prelude;

mod db;
pub use self::db::Db;

pub use factor_macros::{Attribute, Entity};

pub mod tests;
