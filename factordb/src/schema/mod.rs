pub mod builtin;

mod attribute;
pub use self::attribute::{AttrMapExt, AttributeDescriptor, AttributeSchema};

mod entity;
pub use self::entity::{
    Cardinality, EntityAttribute, EntityContainer, EntityDescriptor, EntitySchema,
};

mod index;
pub use self::index::IndexSchema;

pub mod logic;

use crate::data::Ident;

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
pub enum SchemaItem {
    Attribute(AttributeSchema),
    Entity(EntitySchema),
}

#[derive(serde::Serialize, serde::Deserialize, Default, Clone, Debug, PartialEq, Eq)]
pub struct DbSchema {
    pub attributes: Vec<AttributeSchema>,
    pub entities: Vec<EntitySchema>,
    pub indexes: Vec<IndexSchema>,
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

    pub fn merge(mut self, other: Self) -> Self {
        self.attributes.extend(other.attributes);
        self.entities.extend(other.entities);
        self.indexes.extend(other.indexes);

        self
    }
}
