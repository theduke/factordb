use crate::data::{DataMap, Id};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Create {
    pub id: Id,
    pub data: DataMap,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Replace {
    pub id: Id,
    pub data: DataMap,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Patch {
    pub id: Id,
    pub data: DataMap,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Delete {
    pub id: Id,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Update {
    Create(Create),
    Replace(Replace),
    Patch(Patch),
    Delete(Delete),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BatchUpdate {
    pub actions: Vec<Update>,
}
