use std::collections::HashMap;

use crate::data::{DataMap, Id, Ident, Value};

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
    pub cursor: Option<Id>,
}

impl Select {
    pub fn new() -> Self {
        Self {
            joins: Default::default(),
            filter: None,
            sort: Vec::new(),
            variables: Default::default(),
            limit: 100,
            cursor: None,
        }
    }

    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_cursor(mut self, cursor: Id) -> Self {
        self.cursor = Some(cursor);
        self
    }

    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct JoinItem<T> {
    pub name: String,
    pub items: Vec<Item<T>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Item<T = DataMap> {
    pub data: T,
    pub joins: Vec<JoinItem<T>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<Cursor>,
}

impl<T> Page<Item<T>> {
    /// Extract each the item.data, dropping joins.
    pub fn take_data(self) -> Vec<T> {
        self.items.into_iter().map(|item| item.data).collect()
    }
}
