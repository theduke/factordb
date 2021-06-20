pub mod builtin;

pub mod logic;

use std::convert::TryFrom;

use crate::data::{value::ValueMap, Id, Ident, Value, ValueType};

pub fn validate_namespace_name(value: &str) -> Result<(), crate::AnyError> {
    if value.is_empty() {
        return Err(anyhow::anyhow!("invalid namespace: name is empty"));
    }
    if !value
        .chars()
        .all(|c| c.is_alphanumeric() || c == '.' || c == '_')
    {
        return Err(anyhow::anyhow!(
            "invalid namespace: must only contain alphanumeric chars, '.' or '_'"
        ));
    }
    Ok(())
}

pub fn validate_name(value: &str) -> Result<(), crate::AnyError> {
    if value.is_empty() {
        return Err(anyhow::anyhow!("invalid name: name is empty"));
    }
    if !value.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(anyhow::anyhow!(
            "invalid name: must only contain alphanumeric chars  or '_'"
        ));
    }
    Ok(())
}

pub fn validate_namespaced_ident(value: &str) -> Result<(&str, &str), crate::AnyError> {
    let (ns, name) = value.split_once('/').ok_or_else(|| {
        anyhow::anyhow!("Invalid namespaced name: must be of format 'namespace/name'")
    })?;

    validate_namespace_name(ns)?;
    validate_name(name)?;

    Ok((ns, name))
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AttributeSchema {
    #[serde(rename = "factor/id")]
    pub id: Id,
    #[serde(rename = "factor/ident")]
    pub ident: String,
    #[serde(rename = "factor/title")]
    pub title: Option<String>,
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
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        value_type: ValueType,
    ) -> Self {
        Self {
            id: Id::nil(),
            ident: format!("{}/{}", namespace.into(), name.into()),
            title: None,
            description: None,
            value_type,
            unique: false,
            index: false,
            strict: false,
        }
    }

    /// Split the ident into (namespace, name)
    pub fn parse_split_ident(&self) -> Result<(&str, &str), crate::AnyError> {
        validate_namespaced_ident(&self.ident)
    }

    pub fn parse_namespace(&self) -> Result<&str, crate::AnyError> {
        self.parse_split_ident().map(|x| x.0)
    }
}

/// A marker trait for attributes.
///
/// Makes working with statically typed attributes in Rust code easier.
///
/// Useful for defining attributes in migrations, or getting attribute values
/// from a value map with [AttrMapExt].
///
/// NOTE: Types implementing this trait won't usually be used to represent
/// attributes, but act merely as a descriptor.
///
/// This trait should generally not be implemented manually.
/// A custom derive proc macro is available.
/// See [`crate::Attribute`] for how to use the derive.
pub trait AttributeDescriptor {
    /// The namespace fo the attribute.
    const NAMESPACE: &'static str;
    /// The name of the attribute.
    const PLAIN_NAME: &'static str;
    /// The qualified name of the attribute.
    /// This MUST be equal to `format!("{}/{}", Self::NAMESPACE, Self::NAME)`.
    /// Only exists to not require string allocation and concatenation at
    /// runtime.
    const QUALIFIED_NAME: &'static str;
    const IDENT: Ident = Ident::new_static(Self::QUALIFIED_NAME);

    /// The Rust type used to represent this attribute.
    type Type;

    /// Build the schema for this attribute.
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
    pub ident: String,
    #[serde(rename = "factor/title")]
    pub title: Option<String>,
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

impl EntitySchema {
    /// Split the ident into (namespace, name)
    pub fn parse_split_ident(&self) -> Result<(&str, &str), crate::AnyError> {
        validate_namespaced_ident(&self.ident)
    }

    pub fn parse_namespace(&self) -> Result<&str, crate::AnyError> {
        self.parse_split_ident().map(|x| x.0)
    }
}

/// Trait that provides a static metadata for an entity.
pub trait EntityDescriptor {
    /// The namespace.
    const NAMESPACE: &'static str;
    /// The plain attribute name without the namespace.
    const PLAIN_NAME: &'static str;
    /// The qualified name of the entity.
    /// This MUST be equal to `format!("{}/{}", Self::NAMESPACE, Self::NAME)`.
    /// Only exists to not require string allocation and concatenation at
    /// runtime.
    const QUALIFIED_NAME: &'static str;
    const IDENT: Ident = Ident::new_static(Self::QUALIFIED_NAME);
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

impl DbSchema {
    pub fn resolve_attr(&self, ident: &Ident) -> Option<&AttributeSchema> {
        self.attributes.iter().find(|attr| match &ident {
            Ident::Id(id) => attr.id == *id,
            Ident::Name(name) => attr.ident.as_str() == name,
        })
    }

    pub fn resolve_entity(&self, ident: &Ident) -> Option<&EntitySchema> {
        self.entities.iter().find(|entity| match &ident {
            Ident::Id(id) => entity.id == *id,
            Ident::Name(name) => entity.ident.as_str() == name,
        })
    }
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
        self.get(self::builtin::AttrId::QUALIFIED_NAME)
            .and_then(|v| v.as_id())
    }

    fn get_type(&self) -> Option<Ident> {
        self.get(self::builtin::AttrType::QUALIFIED_NAME)
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
        let value = self.get(A::QUALIFIED_NAME)?.clone();
        TryFrom::try_from(value).ok()
    }

    fn insert_attr<A: AttributeDescriptor>(&mut self, value: A::Type)
    where
        A::Type: Into<Value>,
    {
        self.insert(A::QUALIFIED_NAME.to_string(), value.into());
    }
}
