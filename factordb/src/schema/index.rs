use crate::data::Id;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct IndexSchema {
    #[serde(rename = "factor/id")]
    pub id: Id,
    #[serde(rename = "factor/ident")]
    pub ident: String,
    #[serde(rename = "factor/title")]
    pub title: Option<String>,
    #[serde(rename = "factor/index_attributes")]
    pub attributes: Vec<Id>,
    #[serde(rename = "factor/description")]
    pub description: Option<String>,
    #[serde(rename = "factor/unique")]
    pub unique: bool,
}

impl IndexSchema {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>, attributes: Vec<Id>) -> Self {
        Self {
            id: Id::nil(),
            ident: format!("{}/{}", namespace.into(), name.into()),
            title: None,
            description: None,
            unique: false,
            attributes,
        }
    }
}
