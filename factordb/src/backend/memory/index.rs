use std::collections::{BTreeMap, HashSet};

use anyhow::Result;

use crate::{data::Id, registry::LocalIndexId, util::stable_map::DerivedStableMap};

use super::memory_data::MemoryValue;

/// A unique index.
///
/// Can only map values to a single id.
#[derive(Debug)]
pub struct UniqueIndex {
    data: BTreeMap<MemoryValue, Id>,
}

pub struct InsertUniqueError;

impl UniqueIndex {
    pub(super) fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    pub(super) fn get(&self, value: &MemoryValue) -> Option<Id> {
        self.data.get(value).cloned()
    }

    pub(super) fn insert_unchecked(&mut self, value: MemoryValue, id: Id) {
        self.data.insert(value, id);
    }

    pub(super) fn insert_unique(
        &mut self,
        value: MemoryValue,
        id: Id,
    ) -> Result<(), InsertUniqueError> {
        match self.data.entry(value) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(id);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(_) => Err(InsertUniqueError),
        }
    }

    // pub(super) fn replace(
    //     &mut self,
    //     old_value: MemoryValue,
    //     new_value: MemoryValue,
    //     id: Id,
    // ) -> Result<(), InsertUniqueError> {
    //     self.data.remove(&old_value);
    //     self.insert_unique(new_value, id)
    // }

    pub(super) fn remove(&mut self, value: &MemoryValue) -> Option<Id> {
        self.data.remove(&value)
    }

    pub(super) fn clear(&mut self) {
        self.data.clear();
    }
}

impl Default for UniqueIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct MultiIndex {
    data: BTreeMap<MemoryValue, HashSet<Id>>,
}

impl MultiIndex {
    pub(super) fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    // pub(super) fn get(&self, value: &MemoryValue) -> Option<&HashSet<Id>> {
    //     self.data.get(value)
    // }

    pub(super) fn add(&mut self, value: MemoryValue, id: Id) {
        self.data.entry(value).or_default().insert(id);
    }

    // pub(super) fn replace(&mut self, old_value: MemoryValue, new_value: MemoryValue, id: Id) {
    //     self.remove(&old_value, id);
    //     self.add(new_value, id);
    // }

    pub(super) fn remove(&mut self, value: &MemoryValue, id: Id) -> Option<Id> {
        let (removed, purge) = if let Some(set) = self.data.get_mut(&value) {
            set.remove(&id);
            (Some(id), set.is_empty())
        } else {
            (None, false)
        };
        if purge {
            self.data.remove(&value);
        }
        removed
    }

    pub(super) fn clear(&mut self) {
        self.data.clear();
    }
}

impl Default for MultiIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(super) enum Index {
    Unique(UniqueIndex),
    Multi(MultiIndex),
}

impl Index {
    pub fn clear(&mut self) {
        match self {
            Index::Unique(idx) => idx.clear(),
            Index::Multi(idx) => idx.clear(),
        }
    }

    pub fn get_unique(&self, value: &MemoryValue) -> Option<Id> {
        match self {
            Index::Unique(idx) => idx.get(value),
            Index::Multi(_) => None,
        }
    }
}

pub(super) type MemoryIndexMap = DerivedStableMap<LocalIndexId, Index>;

pub(super) fn new_memory_index_map() -> MemoryIndexMap {
    MemoryIndexMap::new()
}
