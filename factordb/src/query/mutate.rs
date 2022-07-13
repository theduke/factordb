use std::collections::HashMap;

use crate::{
    data::{patch::Patch, DataMap, Id},
    prelude::{Expr, Value},
    schema::AttrMapExt,
};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Create {
    pub id: Id,
    pub data: DataMap,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Replace {
    pub id: Id,
    pub data: DataMap,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
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
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct EntityPatch {
    pub id: Id,
    pub patch: Patch,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Remove {
    pub id: Id,
    pub data: DataMap,
}

impl Remove {
    pub fn try_from_map(map: DataMap) -> Result<Self, crate::AnyError> {
        let id = map
            .get_id()
            .and_then(Id::as_non_nil)
            .ok_or_else(|| anyhow::anyhow!("Remove data must have a non-nil id"))?;
        Ok(Self { id, data: map })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Delete {
    pub id: Id,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub enum MutateSelectAction {
    Delete,
    Patch(Patch),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct MutateSelect {
    pub filter: Expr,
    pub variables: HashMap<String, Value>,
    pub action: MutateSelectAction,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub enum Mutate {
    Create(Create),
    Replace(Replace),
    Merge(Merge),
    Patch(EntityPatch),
    Delete(Delete),
    Select(MutateSelect),
}

impl Mutate {
    pub fn create(id: Id, data: DataMap) -> Self {
        Self::Create(Create { id, data })
    }

    pub fn create_from_map(data: DataMap) -> Self {
        let id = data.get_id().unwrap_or_else(Id::random);
        Self::Create(Create { id, data })
    }

    pub fn replace(id: Id, data: DataMap) -> Self {
        Self::Replace(Replace { id, data })
    }

    pub fn merge(id: Id, data: DataMap) -> Self {
        Self::Merge(Merge { id, data })
    }

    pub fn merge_from_map(data: DataMap) -> Result<Self, crate::AnyError> {
        let id = data
            .get_id()
            .ok_or_else(|| anyhow::anyhow!("Update requires an id"))?;
        Ok(Self::Merge(Merge { id, data }))
    }

    pub fn patch(id: Id, patch: Patch) -> Self {
        Self::Patch(EntityPatch { id, patch })
    }

    pub fn delete(id: Id) -> Self {
        Self::Delete(Delete { id })
    }
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

impl From<MutateSelect> for Mutate {
    fn from(v: MutateSelect) -> Self {
        Self::Select(v)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Batch {
    pub actions: Vec<Mutate>,
}

impl Batch {
    pub fn new() -> Self {
        Self {
            actions: Vec::new(),
        }
    }

    pub fn with_action(action: impl Into<Mutate>) -> Self {
        Self {
            actions: vec![action.into()],
        }
    }

    pub fn and_create(mut self, create: Create) -> Self {
        self.actions.push(Mutate::Create(create));
        self
    }

    pub fn and_replace(mut self, replace: Replace) -> Self {
        self.actions.push(Mutate::Replace(replace));
        self
    }

    pub fn and_merge(mut self, merge: Merge) -> Self {
        self.actions.push(Mutate::Merge(merge));
        self
    }

    pub fn and_patch(mut self, patch: EntityPatch) -> Self {
        self.actions.push(Mutate::Patch(patch));
        self
    }

    pub fn and_delete(mut self, delete: Delete) -> Self {
        self.actions.push(Mutate::Delete(delete));
        self
    }

    pub fn and_select(mut self, sel: MutateSelect) -> Self {
        self.actions.push(Mutate::Select(sel));
        self
    }
}

impl From<Mutate> for Batch {
    fn from(v: Mutate) -> Self {
        Self { actions: vec![v] }
    }
}

impl From<Vec<Mutate>> for Batch {
    fn from(v: Vec<Mutate>) -> Self {
        Batch { actions: v }
    }
}
