pub mod builtin;

mod attribute;
pub use self::attribute::{AttrMapExt, Attribute, AttributeMeta};

mod entity;
pub use self::entity::{Cardinality, Class, ClassAttribute, ClassContainer, ClassMeta};

mod index;
pub use self::index::IndexSchema;

use crate::data::IdOrIdent;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SchemaItem {
    Attribute(Attribute),
    Entity(Class),
}

#[derive(serde::Serialize, serde::Deserialize, Default, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct DbSchema {
    // FIXME: make these private and provide accessors.
    // They should no tbe pub because of the sentinel 0 id.
    pub attributes: Vec<Attribute>,
    pub entities: Vec<Class>,
    pub indexes: Vec<IndexSchema>,
}

impl DbSchema {
    pub fn resolve_attr(&self, ident: &IdOrIdent) -> Option<&Attribute> {
        self.attributes.iter().find(|attr| match &ident {
            IdOrIdent::Id(id) => attr.id == *id,
            IdOrIdent::Name(name) => attr.ident.as_str() == name,
        })
    }

    pub fn resolve_entity(&self, ident: &IdOrIdent) -> Option<&Class> {
        self.entities.iter().find(|entity| match &ident {
            IdOrIdent::Id(id) => entity.id == *id,
            IdOrIdent::Name(name) => entity.ident.as_str() == name,
        })
    }

    /// Find the attribute definition for a given attribute by searching the parents of an entity.
    pub fn parent_entity_attr(
        &self,
        entity: &IdOrIdent,
        attr: &IdOrIdent,
    ) -> Option<&ClassAttribute> {
        let entity = self.resolve_entity(entity)?;

        for parent_ident in &entity.extends {
            let parent_entity = self.resolve_entity(parent_ident)?;
            if let Some(attr) = parent_entity
                .attributes
                .iter()
                .find(|a| &a.attribute == attr)
            {
                return Some(attr);
            } else if let Some(attr) = self.parent_entity_attr(parent_ident, attr) {
                return Some(attr);
            }
        }

        None
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.attributes.extend(other.attributes);
        self.entities.extend(other.entities);
        self.indexes.extend(other.indexes);

        self
    }
}
