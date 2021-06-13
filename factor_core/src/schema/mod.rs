pub mod builtin;

pub mod logic;

use crate::data::{Id, Ident, ValueType};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttributeSchema {
    #[serde(rename = "factor/id")]
    pub id: Id,
    #[serde(rename = "factor/ident")]
    pub name: String,
    #[serde(rename = "factor/description")]
    pub description: Option<String>,
    #[serde(rename = "factor/valueType")]
    pub value_type: ValueType,
    #[serde(rename = "factor/unique")]
    pub unique: bool,
    #[serde(rename = "factor/index")]
    pub index: bool,
    /// If an attribute is set to strict, this attribute can only be used
    /// in entities with a schema that specifies the attribute.
    #[serde(rename = "factor/isStrict")]
    pub strict: bool,
}

pub trait AttributeDescriptor {
    const ID: Id;
    const NAME: &'static str;
    const IDENT: Ident = Ident::new_static(Self::NAME);
    fn schema() -> AttributeSchema;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EntitySchema {
    #[serde(rename = "factor/id")]
    pub id: Id,
    #[serde(rename = "factor/ident")]
    pub name: String,
    #[serde(rename = "factor/description")]
    pub description: Option<String>,
    #[serde(rename = "factor/attributes")]
    pub attributes: Vec<Ident>,
    #[serde(rename = "factor/extend")]
    pub extend: Option<Ident>,
    /// If a schema is set to strict, additional attributes not specified
    /// by the schema will be rejected.
    #[serde(rename = "factor/isStrict")]
    pub strict: bool,

    // TODO: refactor to embedded/compound entity
    #[serde(rename = "factor/isRelation")]
    pub is_relation: bool,
    #[serde(rename = "factor/realtionFrom")]
    pub from: Option<Ident>,
    #[serde(rename = "factor/realtionTo")]
    pub to: Option<Ident>,
}

pub trait EntityDescriptor {
    const ID: Id;
    const NAME: &'static str;
    const IDENT: Ident = Ident::new_static(Self::NAME);
    fn schema() -> EntitySchema;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DbSchema {
    pub attributes: Vec<AttributeSchema>,
    pub entities: Vec<EntitySchema>,
}
