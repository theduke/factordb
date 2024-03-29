use std::collections::HashMap;

use crate::data::{DataMap, Id, IdOrIdent, Value};

use super::expr::Expr;

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub enum Order {
    Asc,
    Desc,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Sort {
    pub on: Expr,
    pub order: Order,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Join {
    pub name: String,
    pub attr: IdOrIdent,
    pub limit: u64,
    pub flatten_relation: bool,
}

pub type Cursor = Id;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Select {
    pub filter: Option<Expr>,
    #[serde(default = "Vec::<Join>::new")]
    pub joins: Vec<Join>,
    #[serde(default = "Vec::<Sort>::new")]
    pub sort: Vec<Sort>,

    #[serde(default)]
    pub aggregate: Vec<Aggregation>,

    #[serde(default = "HashMap::<String, Value>::new")]
    pub variables: HashMap<String, Value>,
    pub limit: u64,
    #[serde(default)]
    pub offset: u64,
    pub cursor: Option<Id>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
pub struct Aggregation {
    pub name: String,
    pub op: AggregationOp,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
pub enum AggregationOp {
    Count,
}

impl Select {
    pub fn new() -> Self {
        Self {
            joins: Default::default(),
            filter: None,
            sort: Vec::new(),
            variables: Default::default(),
            aggregate: Vec::new(),
            limit: 0,
            offset: 0,
            cursor: None,
        }
    }

    #[cfg(feature = "sql")]
    pub fn parse_sql(sql: &str) -> Result<Self, super::sql::SqlParseError> {
        super::sql::parse_select(sql)
    }

    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
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

    pub fn with_sort(mut self, on: impl Into<Expr>, order: Order) -> Self {
        self.sort.push(Sort {
            on: on.into(),
            order,
        });
        self
    }

    pub fn with_aggregate(mut self, op: AggregationOp, name: String) -> Self {
        self.aggregate.push(Aggregation { name, op });
        self
    }
}

impl Default for Select {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct JoinItem<T> {
    pub name: String,
    pub items: Vec<Item<T>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Item<T = DataMap> {
    pub data: T,
    #[serde(default = "Vec::<JoinItem<T>>::new")]
    pub joins: Vec<JoinItem<T>>,
}

impl<T: Default> Default for Item<T> {
    fn default() -> Self {
        Self {
            data: T::default(),
            joins: Vec::new(),
        }
    }
}

impl<T> Item<T> {
    pub fn new(data: T) -> Self {
        Self {
            data,
            joins: Vec::new(),
        }
    }

    pub fn with_join(
        mut self,
        name: impl Into<String>,
        items: impl IntoIterator<Item = Item<T>>,
    ) -> Self {
        self.joins.push(JoinItem {
            name: name.into(),
            items: items.into_iter().collect(),
        });
        self
    }

    pub fn flatten_into(self, list: &mut Vec<T>) {
        list.push(self.data);
        for join in self.joins {
            for item in join.items {
                item.flatten_into(list);
            }
        }
    }

    pub fn flatten_list(items: Vec<Self>) -> Vec<T> {
        let mut list = Vec::new();
        for item in items {
            item.flatten_into(&mut list);
        }
        list
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<Cursor>,
}

impl<T> Page<T> {
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            next_cursor: None,
        }
    }
}

impl<T> Default for Page<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Page<Item<T>> {
    /// Extract each the item.data, dropping joins.
    pub fn take_data(self) -> Vec<T> {
        self.items.into_iter().map(|item| item.data).collect()
    }
}

impl Page<Item<DataMap>> {
    pub fn convert_data<T: serde::de::DeserializeOwned>(
        self,
    ) -> Result<Page<T>, crate::data::value::ValueDeserializeError> {
        let items = self
            .items
            .into_iter()
            .map(|item| -> Result<T, _> { crate::data::value::from_value_map(item.data) })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Page {
            items,
            next_cursor: self.next_cursor,
        })
    }
}

pub type ItemPage<T = DataMap> = Page<Item<T>>;
