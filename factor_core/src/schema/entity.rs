use serde::de::DeserializeOwned;

use crate::data::{value::ValueDeserializeError, DataMap, Id, IdOrIdent, Ident, InvalidIdentError};

use super::AttrMapExt;

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub enum Cardinality {
    Optional,
    Required,
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
pub struct ClassAttribute {
    pub attribute: IdOrIdent,
    pub cardinality: Cardinality,
}

impl ClassAttribute {
    pub fn new_optional(attribute: impl Into<IdOrIdent>) -> Self {
        Self {
            attribute: attribute.into(),
            cardinality: Cardinality::Optional,
        }
    }

    pub fn new_required(attribute: impl Into<IdOrIdent>) -> Self {
        Self {
            attribute: attribute.into(),
            cardinality: Cardinality::Required,
        }
    }

    pub fn into_optional(self) -> Self {
        Self {
            attribute: self.attribute,
            cardinality: Cardinality::Optional,
        }
    }

    pub fn into_required(self) -> Self {
        Self {
            attribute: self.attribute,
            cardinality: Cardinality::Required,
        }
    }
}

impl From<Id> for ClassAttribute {
    fn from(id: Id) -> Self {
        Self {
            attribute: id.into(),
            cardinality: Cardinality::Required,
        }
    }
}

impl From<Ident> for ClassAttribute {
    fn from(ident: Ident) -> Self {
        Self {
            attribute: IdOrIdent::new_str(ident.as_ref()),
            cardinality: Cardinality::Required,
        }
    }
}

impl ClassMeta for ClassAttribute {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "ClassAttribute";
    const QUALIFIED_NAME: &'static str = "factor/ClassAttribute";
    const IDENT: IdOrIdent = IdOrIdent::new_static(Self::QUALIFIED_NAME);

    fn schema() -> Class {
        Class {
            id: Id::nil(),
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("ClassAttribute".to_string()),
            description: Some("A single attribute on a class.".to_string()),
            attributes: vec![],
            extends: vec![],
            strict: false,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Class {
    #[serde(rename = "factor/id")]
    pub id: Id,
    #[serde(rename = "factor/ident")]
    pub ident: String,
    #[serde(rename = "factor/title")]
    pub title: Option<String>,
    #[serde(rename = "factor/description")]
    pub description: Option<String>,
    #[serde(rename = "factor/entityAttributes")]
    pub attributes: Vec<ClassAttribute>,
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

impl Class {
    pub fn new(ident: impl Into<String>) -> Self {
        Self {
            id: Id::nil(),
            ident: ident.into(),
            title: None,
            description: None,
            attributes: vec![],
            extends: vec![],
            strict: false,
        }
    }

    pub fn ident(&self) -> IdOrIdent {
        IdOrIdent::Name(self.ident.clone().into())
    }

    pub fn attribute(&self, name: &str) -> Option<&ClassAttribute> {
        self.attributes
            .iter()
            .find(|a| a.attribute.as_name() == Some(name))
    }

    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_attribute(mut self, attr: impl Into<IdOrIdent>, cardinality: Cardinality) -> Self {
        self.attributes.push(ClassAttribute {
            attribute: attr.into(),
            cardinality,
        });
        self
    }

    pub fn with_attributes(mut self, attributes: Vec<ClassAttribute>) -> Self {
        self.attributes.extend(attributes);
        self
    }

    pub fn with_extend(mut self, extend: impl Into<IdOrIdent>) -> Self {
        self.extends.push(extend.into());
        self
    }

    /// Split the ident into (namespace, name)
    pub fn parse_split_ident(&self) -> Result<(&str, &str), InvalidIdentError> {
        crate::data::Ident::parse_parts(&self.ident)
    }

    pub fn parse_namespace(&self) -> Result<&str, InvalidIdentError> {
        self.parse_split_ident().map(|x| x.0)
    }

    /// The title, if present, otherwise the unique name.
    pub fn pretty_name(&self) -> &str {
        self.title.as_deref().unwrap_or(self.ident.as_str())
    }
}

/// Trait that provides a static metadata for an entity.
pub trait ClassMeta {
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
    fn schema() -> Class;
}

pub trait ClassContainer {
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
