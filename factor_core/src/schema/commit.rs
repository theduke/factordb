use fnv::FnvHashMap;

use crate::data::{DataMap, Timestamp, Value};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PreCommit {
    #[serde(rename = "factor/subject")]
    pub subject: String,
    #[serde(rename = "factor/set")]
    pub set: Option<DataMap>,
    #[serde(rename = "factor/replace", default)]
    pub replace: bool,
    #[serde(rename = "factor/remove")]
    pub remove: Option<FnvHashMap<String, Option<Value>>>,
    #[serde(rename = "factor/destroy", default)]
    pub destroy: bool,
    pub created_at: Option<Timestamp>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PreBatchCommit {
    #[serde(rename = "factor/commits")]
    pub commits: Vec<PreCommit>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PreMigration {
    #[serde(rename = "factor/ident")]
    pub ident: Option<String>,
    #[serde(rename = "factor/commits")]
    pub commits: Vec<PreCommit>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StaticSchema {
    #[serde(rename = "factor/ident")]
    pub ident: String,

    #[serde(rename = "factor/imports", default)]
    pub imports: Vec<String>,

    #[serde(rename = "factor.schema/rust-import-path")]
    pub rust_import_path: Option<String>,

    #[serde(rename = "factor/migrations")]
    pub migrations: Vec<PreMigration>,
}
