pub type AnyError = anyhow::Error;

pub mod data;
pub mod error;
pub mod query;
pub mod schema;

pub mod backend;
pub mod registry;

mod db;

pub use self::{
    data::{Id, Ident},
    db::Db,
};

pub use factor_macros::{Attribute, Entity};

pub mod tests;
