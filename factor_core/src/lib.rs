#![cfg_attr(
    feature = "unstable",
    feature(provide_any, error_generic_member_access)
)]

#[macro_use]
pub mod data;
pub mod db;
pub mod error;
pub mod query;
pub mod schema;

pub mod simple_db;
