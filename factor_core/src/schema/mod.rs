pub mod builtin;
pub mod dsl;

mod attribute;
pub use self::attribute::{AttrMapExt, Attribute, AttributeMeta};

mod class;
pub use self::class::{Cardinality, Class, ClassAttribute, ClassContainer, ClassMeta};

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
    pub classes: Vec<Class>,
    pub indexes: Vec<IndexSchema>,
}

impl DbSchema {
    pub fn resolve_attr(&self, ident: &IdOrIdent) -> Option<&Attribute> {
        self.attributes.iter().find(|attr| match &ident {
            IdOrIdent::Id(id) => attr.id == *id,
            IdOrIdent::Name(name) => attr.ident.as_str() == name,
        })
    }

    pub fn attr_by_ident(&self, ident: &str) -> Option<&Attribute> {
        self.attributes.iter().find(|attr| attr.ident == ident)
    }

    pub fn resolve_class(&self, ident: &IdOrIdent) -> Option<&Class> {
        self.classes.iter().find(|entity| match &ident {
            IdOrIdent::Id(id) => entity.id == *id,
            IdOrIdent::Name(name) => entity.ident.as_str() == name,
        })
    }

    pub fn class_by_ident(&self, ident: &str) -> Option<&Class> {
        self.classes.iter().find(|entity| entity.ident == ident)
    }

    /// Find the attribute definition for a given attribute by searching the parents of an entity.
    pub fn parent_class_attr(&self, entity: &str, attr: &IdOrIdent) -> Option<&ClassAttribute> {
        let entity = self.class_by_ident(entity)?;

        for parent_ident in &entity.extends {
            let parent_entity = self.class_by_ident(parent_ident)?;
            if let Some(attr) = parent_entity
                .attributes
                .iter()
                .find(|a| Some(a.attribute.as_str()) == attr.as_name())
            {
                return Some(attr);
            } else if let Some(attr) = self.parent_class_attr(parent_ident, attr) {
                return Some(attr);
            }
        }

        None
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.attributes.extend(other.attributes);
        self.classes.extend(other.classes);
        self.indexes.extend(other.indexes);

        self
    }
}
