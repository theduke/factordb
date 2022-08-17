use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use factor_core::{data::Id, query::select::Order};

use crate::{registry::LocalIndexId, util::stable_map::DerivedStableMap};

use super::memory_data::MemoryValue;

/// A unique index.
///
/// Can only map values to a single id.
#[derive(Debug)]
pub(super) struct UniqueIndex {
    data: BTreeMap<MemoryValue, Id>,
}

pub struct InsertUniqueError;

impl UniqueIndex {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    pub fn get(&self, value: &MemoryValue) -> Option<Id> {
        self.data.get(value).cloned()
    }

    pub fn range<'a>(
        &'a self,
        from: Option<MemoryValue>,
        until: Option<MemoryValue>,
        direction: Order,
    ) -> Box<dyn Iterator<Item = Id> + 'a> {
        match (from, until, direction) {
            (None, None, Order::Asc) => {
                let out = self.data.values().cloned();
                Box::new(out)
            }
            (None, None, Order::Desc) => {
                let out = self.data.values().rev().cloned();
                Box::new(out)
            }
            (None, Some(end), Order::Asc) => {
                let out = self.data.range(..=end).map(|(_v, id)| *id);
                Box::new(out)
            }
            (None, Some(end), Order::Desc) => {
                let out = self.data.range(..=end).rev().map(|(_v, id)| *id);
                Box::new(out)
            }
            (Some(start), None, Order::Asc) => {
                let out = self.data.range(start..).map(|(_v, id)| *id);
                Box::new(out)
            }
            (Some(start), None, Order::Desc) => {
                let out = self.data.range(start..).rev().map(|(_v, id)| *id);
                Box::new(out)
            }
            (Some(start), Some(end), Order::Asc) => {
                let out = self.data.range(start..=end).map(|(_v, id)| *id);
                Box::new(out)
            }
            (Some(start), Some(end), Order::Desc) => {
                let out = self.data.range(start..=end).rev().map(|(_v, id)| *id);
                Box::new(out)
            }
        }
    }

    pub fn range_prefix<'a>(
        &'a self,
        prefix: MemoryValue,
        direction: Order,
    ) -> Box<dyn Iterator<Item = Id> + 'a> {
        // TODO: earlier error if values not string?
        match (&prefix, direction) {
            (v @ MemoryValue::String(s), Order::Asc) => {
                let prefix: String = s.as_ref().to_string();
                let out = self
                    .data
                    .range(v.clone()..)
                    .take_while(move |(key, _value)| match key {
                        MemoryValue::String(value) => value.as_ref().starts_with(&prefix),
                        // Should never happen!
                        _ => true,
                    })
                    .map(|(_key, id)| *id);
                Box::new(out)
            }
            (v @ MemoryValue::String(s), Order::Desc) => {
                let prefix: String = s.as_ref().to_string();
                let out = self
                    .data
                    .range(v..)
                    .rev()
                    // TODO: can this be faster? (start iterating from reverse direction?)
                    .skip_while(move |(key, _value)| match key {
                        MemoryValue::String(value) => !value.as_ref().starts_with(&prefix),
                        // Should never happen!
                        _ => true,
                    })
                    .map(|(_key, id)| *id);
                Box::new(out)
            }
            (_, Order::Asc) => {
                let out = self.data.values().cloned();
                Box::new(out)
            }
            (_, Order::Desc) => {
                let out = self.data.values().rev().cloned();
                Box::new(out)
            }
        }
    }

    pub fn insert_unchecked(&mut self, value: MemoryValue, id: Id) {
        self.data.insert(value, id);
    }

    pub fn insert_unique(&mut self, value: MemoryValue, id: Id) -> Result<(), InsertUniqueError> {
        match self.data.entry(value) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(id);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(_) => Err(InsertUniqueError),
        }
    }

    // pub fn replace(
    //     &mut self,
    //     old_value: MemoryValue,
    //     new_value: MemoryValue,
    //     id: Id,
    // ) -> Result<(), InsertUniqueError> {
    //     self.data.remove(&old_value);
    //     self.insert_unique(new_value, id)
    // }

    pub fn remove(&mut self, value: &MemoryValue) -> Option<Id> {
        self.data.remove(value)
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }
}

impl Default for UniqueIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(super) struct MultiIndex {
    data: BTreeMap<MemoryValue, HashSet<Id>>,
}

impl MultiIndex {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    pub fn get(&self, value: &MemoryValue) -> Option<&HashSet<Id>> {
        self.data.get(value)
    }

    pub fn add(&mut self, value: MemoryValue, id: Id) {
        self.data.entry(value).or_default().insert(id);
    }

    // pub fn replace(&mut self, old_value: MemoryValue, new_value: MemoryValue, id: Id) {
    //     self.remove(&old_value, id);
    //     self.add(new_value, id);
    // }

    pub fn remove(&mut self, value: &MemoryValue, id: Id) -> Option<Id> {
        let (removed, purge) = if let Some(set) = self.data.get_mut(value) {
            set.remove(&id);
            (Some(id), set.is_empty())
        } else {
            (None, false)
        };
        if purge {
            self.data.remove(value);
        }
        removed
    }

    pub fn range<'a>(
        &'a self,
        from: Option<MemoryValue>,
        until: Option<MemoryValue>,
        direction: Order,
    ) -> Box<dyn Iterator<Item = Id> + 'a> {
        match (from, until, direction) {
            (None, None, Order::Asc) => {
                let out = self.data.values().flatten().cloned();
                Box::new(out)
            }
            (None, None, Order::Desc) => {
                let out = self.data.values().rev().flatten().cloned();
                Box::new(out)
            }
            (None, Some(end), Order::Asc) => {
                let out = self.data.range(..=end).flat_map(|(_v, id)| id.clone());
                Box::new(out)
            }
            (None, Some(end), Order::Desc) => {
                let out = self
                    .data
                    .range(..=end)
                    .rev()
                    .flat_map(|(_v, id)| id.clone());
                Box::new(out)
            }
            (Some(start), None, Order::Asc) => {
                let out = self.data.range(start..).flat_map(|(_v, id)| id.clone());
                Box::new(out)
            }
            (Some(start), None, Order::Desc) => {
                let out = self
                    .data
                    .range(start..)
                    .rev()
                    .flat_map(|(_v, id)| id.clone());
                Box::new(out)
            }
            (Some(start), Some(end), Order::Asc) => {
                let out = self.data.range(start..=end).flat_map(|(_v, id)| id.clone());
                Box::new(out)
            }
            (Some(start), Some(end), Order::Desc) => {
                let out = self
                    .data
                    .range(start..=end)
                    .rev()
                    .flat_map(|(_v, id)| id.clone());
                Box::new(out)
            }
        }
    }

    pub fn range_prefix<'a>(
        &'a self,
        prefix: MemoryValue,
        direction: Order,
    ) -> Box<dyn Iterator<Item = Id> + 'a> {
        // TODO: earlier error if values not string?
        match (&prefix, direction) {
            (v @ MemoryValue::String(s), Order::Asc) => {
                let prefix: String = s.as_ref().to_string();
                let out = self
                    .data
                    .range(v.clone()..)
                    .take_while(move |(key, _value)| match key {
                        MemoryValue::String(value) => value.as_ref().starts_with(&prefix),
                        // Should never happen!
                        _ => true,
                    })
                    .flat_map(|(_key, id)| id.clone());
                Box::new(out)
            }
            (v @ MemoryValue::String(s), Order::Desc) => {
                let prefix: String = s.as_ref().to_string();
                let out = self
                    .data
                    .range(v..)
                    .rev()
                    // TODO: can this be faster? (start iterating from reverse direction?)
                    .skip_while(move |(key, _value)| match key {
                        MemoryValue::String(value) => !value.as_ref().starts_with(&prefix),
                        // Should never happen!
                        _ => true,
                    })
                    .flat_map(|(_key, id)| id.clone());
                Box::new(out)
            }
            (_, Order::Asc) => {
                let out = self.data.values().flatten().cloned();
                Box::new(out)
            }
            (_, Order::Desc) => {
                let out = self.data.values().rev().flatten().cloned();
                Box::new(out)
            }
        }
    }

    pub fn clear(&mut self) {
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
