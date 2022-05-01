use serde::de::DeserializeOwned;

use crate::data::{value::ValueDeserializeError, DataMap, Id, IdOrIdent};

use super::AttrMapExt;

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
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
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct EntityAttribute {
    pub attribute: IdOrIdent,
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
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
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
    pub extends: Vec<IdOrIdent>,
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
    pub fn attribute(&self, name: &str) -> Option<&EntityAttribute> {
        self.attributes
            .iter()
            .find(|a| a.attribute.as_name() == Some(name))
    }

    /// Split the ident into (namespace, name)
    pub fn parse_split_ident(&self) -> Result<(&str, &str), crate::AnyError> {
        super::validate_namespaced_ident(&self.ident)
    }

    pub fn parse_namespace(&self) -> Result<&str, crate::AnyError> {
        self.parse_split_ident().map(|x| x.0)
    }

    /// The title, if present, otherwise the unique name.
    pub fn pretty_name(&self) -> &str {
        self.title
            .as_ref()
            .map(|t| t.as_str())
            .unwrap_or(self.ident.as_str())
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
    const IDENT: IdOrIdent = IdOrIdent::new_static(Self::QUALIFIED_NAME);
    fn schema() -> EntitySchema;
}

pub trait EntityContainer {
    fn id(&self) -> Id;
    fn entity_type(&self) -> IdOrIdent;

    // TODO: remove this once we have a proper custom derive for De/Serialize
    // in the #[derive(Entity)]
    fn into_map(self) -> Result<DataMap, crate::data::value::ValueSerializeError>
    where
        Self: serde::Serialize + Sized,
    {
        let ty = self.entity_type();
        let mut map = crate::data::value::to_value_map(self)?;
        map.insert_attr::<super::builtin::AttrType>(ty);
        Ok(map)
    }

    fn try_from_map(map: DataMap) -> Result<Self, ValueDeserializeError>
    where
        Self: Sized + DeserializeOwned,
    {
        crate::data::value::from_value_map(map)
    }
}
