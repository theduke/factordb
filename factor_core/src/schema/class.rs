use serde::de::DeserializeOwned;

use crate::data::{value::ValueDeserializeError, DataMap, Id, IdOrIdent, InvalidIdentError};

use super::{AttrMapExt, AttributeMeta};

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

    /// Returns `true` if the cardinality is [`Required`].
    ///
    /// [`Required`]: Cardinality::Required
    #[must_use]
    pub fn is_required(&self) -> bool {
        matches!(self, Self::Required)
    }
}

#[derive(serde::Serialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct ClassAttribute {
    #[serde(rename = "factor/attribute", default)]
    pub attribute: String,

    #[serde(rename = "factor/required", default)]
    pub required: bool,
}

#[allow(deprecated)]
impl ClassAttribute {
    pub fn cardinality(&self) -> Cardinality {
        if self.required {
            Cardinality::Required
        } else {
            Cardinality::Optional
        }
    }

    pub fn from_schema_required<A: AttributeMeta>() -> Self {
        Self {
            attribute: A::QUALIFIED_NAME.to_string(),
            required: true,
        }
    }

    pub fn from_schema_optional<A: AttributeMeta>() -> Self {
        Self {
            attribute: A::QUALIFIED_NAME.to_string(),
            required: false,
        }
    }

    pub fn new_optional(attribute: impl Into<String>) -> Self {
        Self {
            attribute: attribute.into(),
            required: false,
        }
    }

    pub fn new_required(attribute: impl Into<String>) -> Self {
        Self {
            attribute: attribute.into(),
            required: true,
        }
    }

    pub fn into_optional(self) -> Self {
        Self {
            attribute: self.attribute,
            required: false,
        }
    }

    pub fn into_required(self) -> Self {
        Self {
            attribute: self.attribute,
            required: true,
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

// Custom deserialize  impl that supports the legacy format with un-prefixed attribute
// and cardinality.
// ( attribute vs factor/attribute, cardinality (Required, Optional) vs factor/required )
// TODO: Remove custom deserialize impl once old data is migrated
impl<'de> serde::Deserialize<'de> for ClassAttribute {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = ClassAttribute;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "ClassAttribute")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut attr: Option<String> = None;
                let mut required: Option<bool> = None;

                loop {
                    match map.next_key::<String>()?.as_deref() {
                        Some("attribute" | "factor/attribute") => {
                            let s = map.next_value::<String>()?;
                            attr = Some(s);
                        }
                        Some("cardinality") => {
                            let c = map.next_value::<String>()?;
                            match c.as_str() {
                                "Required" => {
                                    required = Some(true);
                                }
                                "Optional" => {
                                    required = Some(false);
                                }
                                other => {
                                    return Err(<A::Error as serde::de::Error>::invalid_value(
                                        serde::de::Unexpected::Str(other),
                                        &"Required or Optional",
                                    ));
                                }
                            }
                        }
                        Some("factor/required") => {
                            let c = map.next_value::<bool>()?;
                            required = Some(c);
                        }
                        Some(_) => {
                            continue;
                        }
                        None => {
                            break;
                        }
                    }
                }

                let attribute = attr.ok_or_else(|| {
                    <A::Error as serde::de::Error>::missing_field("factor/attribute")
                })?;
                let required = required.unwrap_or(false);

                Ok(ClassAttribute {
                    attribute,
                    required,
                })
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

#[test]
fn test_class_attribute_deser() {
    // legacy format
    let attr = serde_json::from_str::<ClassAttribute>(
        r#"{
        "attribute": "test",
        "cardinality": "Required"
    }"#,
    )
    .unwrap();
    assert_eq!(
        attr,
        ClassAttribute {
            attribute: "test".to_string(),
            required: true,
        }
    );

    // new format
    let attr = serde_json::from_str::<ClassAttribute>(
        r#"{
        "factor/attribute": "test",
        "factor/required": true
    }"#,
    )
    .unwrap();
    assert_eq!(
        attr,
        ClassAttribute {
            attribute: "test".to_string(),
            required: true,
        }
    );
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Class {
    #[serde(rename = "factor/id", default)]
    pub id: Id,
    #[serde(rename = "factor/ident")]
    pub ident: String,
    #[serde(rename = "factor/title")]
    pub title: Option<String>,
    #[serde(rename = "factor/description")]
    pub description: Option<String>,
    #[serde(rename = "factor/classAttributes", default)]
    pub attributes: Vec<ClassAttribute>,
    #[serde(rename = "factor/extend", default)]
    pub extends: Vec<IdOrIdent>,
    /// If a schema is set to strict, additional attributes not specified
    /// by the schema will be rejected.
    #[serde(rename = "factor/isStrict", default)]
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
            .find(|a| a.attribute.as_str() == name)
    }

    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_attribute(mut self, attr: impl Into<String>, required: bool) -> Self {
        self.attributes.push(ClassAttribute {
            attribute: attr.into(),
            required,
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
