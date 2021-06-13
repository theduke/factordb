use std::collections::HashMap;

use crate::data::{Ident, Value};

use super::expr::Expr;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Sort {
    value: Expr,
    order: Order,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Join {
    pub name: String,
    pub attr: Ident,
    pub flatten_relation: bool,
}

pub type Cursor = String;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Select {
    pub joins: Vec<Join>,
    pub filter: Option<Expr>,
    pub sort: Vec<Sort>,
    pub variables: HashMap<String, Value>,
    pub limit: u64,
    pub cursor: Option<Cursor>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<Cursor>,
}
