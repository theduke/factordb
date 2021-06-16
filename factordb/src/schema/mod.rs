pub mod builtin;

pub mod logic;

use std::convert::TryFrom;

use crate::data::{value::ValueMap, Id, Ident, Value, ValueType};

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

impl AttributeSchema {
    pub fn new(name: impl Into<String>, value_type: ValueType) -> Self {
        Self {
            id: Id::nil(),
            name: name.into(),
            description: None,
            value_type,
            unique: false,
            index: false,
            strict: false,
        }
    }
}

pub trait AttributeDescriptor {
    const NAME: &'static str;
    const IDENT: Ident = Ident::new_static(Self::NAME);
    type Type;
    fn schema() -> AttributeSchema;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    Optional,
    Required,
    Many,
}

impl Cardinality {
    #[inline]
    pub fn is_optional(&self) -> bool {
        matches!(self, Self::Optional)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EntityAttribute {
    pub attribute: Ident,
    pub cardinality: Cardinality,
}

impl EntityAttribute {
    pub fn into_optional(self) -> Self {
        Self {
            attribute: self.attribute,
            cardinality: Cardinality::Optional,
        }
    }

    pub fn into_many(self) -> Self {
        Self {
            attribute: self.attribute,
            cardinality: Cardinality::Many,
        }
    }
}

impl From<Id> for EntityAttribute {
    fn from(id: Id) -> Self {
        Self {
            attribute: id.into(),
            cardinality: Cardinality::Required,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EntitySchema {
    #[serde(rename = "factor/id")]
    pub id: Id,
    #[serde(rename = "factor/ident")]
    pub name: String,
    #[serde(rename = "factor/description")]
    pub description: Option<String>,
    #[serde(rename = "factor/entityAttributes")]
    pub attributes: Vec<EntityAttribute>,
    #[serde(rename = "factor/extend")]
    pub extends: Vec<Ident>,
    /// If a schema is set to strict, additional attributes not specified
    /// by the schema will be rejected.
    #[serde(rename = "factor/isStrict")]
    pub strict: bool,
    // TODO: refactor to embedded/compound entity
    // #[serde(rename = "factor/isRelation")]
    // pub is_relation: bool,
    // #[serde(rename = "factor/relationFrom")]
    // pub from: Option<Ident>,
    // #[serde(rename = "factor/relationTo")]
    // pub to: Option<Ident>,
}

/// Trait that provides a static metadata for an entity.
pub trait EntityDescriptor {
    const NAME: &'static str;
    const IDENT: Ident = Ident::new_static(Self::NAME);
    fn schema() -> EntitySchema;
}

pub trait EntityContainer {
    fn id(&self) -> Id;
    fn entity_type(&self) -> Ident;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SchemaItem {
    Attribute(AttributeSchema),
    Entity(EntitySchema),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DbSchema {
    pub attributes: Vec<AttributeSchema>,
    pub entities: Vec<EntitySchema>,
}

pub trait AttrMapExt {
    fn get_id(&self) -> Option<Id>;
    fn get_type(&self) -> Option<Ident>;
    fn get_attr<A: AttributeDescriptor>(&self) -> Option<A::Type>
    where
        A::Type: TryFrom<Value>;
    fn insert_attr<A: AttributeDescriptor>(&mut self, value: A::Type)
    where
        A::Type: Into<Value>;
}

impl AttrMapExt for ValueMap<String> {
    fn get_id(&self) -> Option<Id> {
        self.get(self::builtin::AttrId::NAME)
            .and_then(|v| v.as_id())
    }

    fn get_type(&self) -> Option<Ident> {
        self.get(self::builtin::AttrType::NAME)
            .and_then(|v| match v {
                Value::String(name) => Some(Ident::Name(name.to_string().into())),
                Value::Id(id) => Some(Ident::Id(*id)),
                _ => None,
            })
    }

    fn get_attr<A: AttributeDescriptor>(&self) -> Option<A::Type>
    where
        A::Type: TryFrom<Value>,
    {
        let value = self.get(A::NAME)?.clone();
        TryFrom::try_from(value).ok()
    }

    fn insert_attr<A: AttributeDescriptor>(&mut self, value: A::Type)
    where
        A::Type: Into<Value>,
    {
        self.insert(A::NAME.to_string(), value.into());
    }
}
