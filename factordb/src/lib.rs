pub type AnyError = anyhow::Error;

pub mod data;
pub mod error;
pub mod query;
pub mod schema;

pub mod backend;
pub mod registry;

pub mod prelude;

mod db;

pub use self::{
    data::{value::ValueMap, Id, Ident, Value},
    db::Db,
};

pub use factor_macros::{Attribute, Entity};

pub mod tests;
