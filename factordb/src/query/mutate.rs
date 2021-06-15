use crate::{
    data::{DataMap, Id},
    schema::AttrMapExt,
};

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
pub struct Merge {
    pub id: Id,
    pub data: DataMap,
}

impl Merge {
    pub fn try_from_map(map: DataMap) -> Result<Self, crate::AnyError> {
        let id = map
            .get_id()
            .and_then(Id::as_non_nil)
            .ok_or_else(|| anyhow::anyhow!("Merge data must have a non-nil id"))?;
        Ok(Self { id, data: map })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Delete {
    pub id: Id,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Mutate {
    Create(Create),
    Replace(Replace),
    Merge(Merge),
    Delete(Delete),
}

impl From<Create> for Mutate {
    fn from(v: Create) -> Self {
        Self::Create(v)
    }
}

impl From<Replace> for Mutate {
    fn from(v: Replace) -> Self {
        Self::Replace(v)
    }
}

impl From<Merge> for Mutate {
    fn from(v: Merge) -> Self {
        Self::Merge(v)
    }
}

impl From<Delete> for Mutate {
    fn from(v: Delete) -> Self {
        Self::Delete(v)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct BatchUpdate {
    pub actions: Vec<Mutate>,
}

impl From<Vec<Mutate>> for BatchUpdate {
    fn from(v: Vec<Mutate>) -> Self {
        BatchUpdate { actions: v }
    }
}
