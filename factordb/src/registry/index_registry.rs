use anyhow::{anyhow, Context};
use fnv::FnvHashMap;

use crate::{
    data::Id,
    error, schema,
    util::stable_map::{StableMap, StableMapKey},
    AnyError,
};

use super::{attribute_registry::AttributeRegistry, LocalAttributeId};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LocalIndexId(u32);

impl LocalIndexId {
    pub(super) const fn from_u32(val: u32) -> Self {
        Self(val)
    }
}

const MAX_INDEX_NAME_LEN: usize = 300;

#[derive(Clone, Debug)]
pub struct RegisteredIndex {
    pub local_id: LocalIndexId,
    pub schema: schema::IndexSchema,
    pub is_deleted: bool,
    pub namespace: String,
    pub plain_name: String,
}

#[derive(Clone, Debug)]
pub struct IndexRegistry {
    items: StableMap<LocalIndexId, RegisteredIndex>,
    uids: FnvHashMap<Id, LocalIndexId>,
    names: FnvHashMap<String, LocalIndexId>,

    /// A mapping from local attribute id to all indexes the attribute appears
    /// in.
    attribute_id_map: FnvHashMap<LocalAttributeId, Vec<LocalIndexId>>,
}

impl StableMapKey for LocalIndexId {
    #[inline]
    fn from_index(index: usize) -> Self {
        Self(index as u32)
    }

    #[inline]
    fn as_index(self) -> usize {
        self.0 as usize
    }
}

impl IndexRegistry {
    pub fn new() -> Self {
        Self {
            items: StableMap::new(),
            uids: Default::default(),
            names: Default::default(),
            attribute_id_map: FnvHashMap::default(),
        }
    }

    pub fn reset(&mut self) {
        self.items = StableMap::new();
        self.uids.clear();
        self.names.clear();
        self.attribute_id_map.clear();
    }

    fn add(
        &mut self,
        schema: schema::IndexSchema,
        local_attribute_ids: Vec<LocalAttributeId>,
    ) -> Result<LocalIndexId, AnyError> {
        assert!(self.items.len() < u32::MAX as usize - 1);

        let (namespace, plain_name) = crate::schema::validate_namespaced_ident(&schema.ident)
            .map(|(a, b)| (a.to_string(), b.to_string()))?;

        let uid = schema.id;
        let ident = schema.ident.clone();

        let local_id = self.items.insert_with(|local_id| RegisteredIndex {
            local_id,
            namespace: namespace.to_string(),
            plain_name: plain_name.to_string(),
            schema,
            is_deleted: false,
        });
        self.uids.insert(uid, local_id);
        self.names.insert(ident, local_id);

        for id in local_attribute_ids {
            self.attribute_id_map.entry(id).or_default().push(local_id);
        }

        Ok(local_id)
    }

    // #[inline]
    // pub fn get_maybe_deleted(&self, id: LocalIndexId) -> &RegisteredIndex {
    //     // NOTE: this panics, but this is acceptable because a LocalIndexId
    //     // is always valid.
    //     &self.items[id.0 as usize]
    // }

    #[inline]
    pub fn get(&self, id: LocalIndexId) -> Option<&RegisteredIndex> {
        // NOTE: this panics, but this is acceptable because a LocalIndexId
        // is always valid.
        let item = self.items.get(id);
        if item.is_deleted {
            None
        } else {
            Some(item)
        }
    }

    // pub fn must_get(
    //     &self,
    //     id: LocalIndexId,
    // ) -> Result<&RegisteredIndex, error::IndexNotFound> {
    //     let item = self.get_maybe_deleted(id);
    //     if item.is_deleted {
    //         Err(error::IndexNotFound::new(
    //             item.schema.ident.clone().into(),
    //         ))
    //     } else {
    //         Ok(item)
    //     }
    // }

    pub fn get_by_uid(&self, uid: Id) -> Option<&RegisteredIndex> {
        self.uids.get(&uid).and_then(|id| self.get(*id))
    }

    pub fn must_get_by_uid(&self, uid: Id) -> Result<&RegisteredIndex, error::IndexNotFound> {
        self.get_by_uid(uid)
            .ok_or_else(|| error::IndexNotFound::new(uid.into()))
    }

    pub fn get_by_name(&self, name: &str) -> Option<&RegisteredIndex> {
        self.names.get(name).and_then(|id| self.get(*id))
    }

    pub fn must_get_by_name(&self, name: &str) -> Result<&RegisteredIndex, error::IndexNotFound> {
        self.get_by_name(name)
            .ok_or_else(|| error::IndexNotFound::new(name.into()))
    }

    // pub fn get_by_ident(&self, ident: &Ident) -> Option<&RegisteredIndex> {
    //     match ident {
    //         Ident::Id(id) => self.get_by_uid(*id),
    //         Ident::Name(name) => self.get_by_name(name),
    //     }
    // }

    // pub fn must_get_by_ident(
    //     &self,
    //     ident: &Ident,
    // ) -> Result<&RegisteredIndex, error::IndexNotFound> {
    //     match ident {
    //         Ident::Id(id) => self.must_get_by_uid(*id),
    //         Ident::Name(name) => self.must_get_by_name(name),
    //     }
    // }

    pub fn iter(&self) -> impl Iterator<Item = &RegisteredIndex> {
        self.items.iter().filter(|x| !x.is_deleted)
    }

    // /// Get all indexes an attribute appears in.
    // pub fn attribute_indexes_ids(&self, attr_id: LocalAttributeId) -> &[LocalIndexId] {
    //     self.attribute_id_map
    //         .get(&attr_id)
    //         .map(|x| x.as_slice())
    //         .unwrap_or(&[])
    // }

    /// Get all indexes an attribute appears in.
    // TODO: this does a lot of work for a common operation...  Need to find a way to make this quicker.
    // Probably best to keep a copy of the RegisteredIndex in the mapping!
    pub fn attribute_indexes(&self, attr_id: LocalAttributeId) -> Vec<&RegisteredIndex> {
        self.attribute_id_map
            .get(&attr_id)
            .map(|ids| ids.into_iter().map(|id| self.items.get(*id)).collect())
            .unwrap_or_default()
    }

    // NOTE: Only pub(super) because [Registry] might do additional validation.
    pub(super) fn register(
        &mut self,
        index: schema::IndexSchema,
        attrs: &AttributeRegistry,
    ) -> Result<LocalIndexId, AnyError> {
        let local_attribute_ids = self.validate_schema(&index, attrs)?;
        self.add(index, local_attribute_ids)
    }

    pub(super) fn remove(&mut self, id: Id) -> Result<(), AnyError> {
        let local_id = self.must_get_by_uid(id)?.local_id;
        self.items.get_mut(local_id).is_deleted = true;
        Ok(())
    }

    /// Validates an [`IndexSchema`].
    ///
    /// Returnes a list of [`LocalAttributeId`]s that the index covers.
    fn validate_schema(
        &self,
        index: &schema::IndexSchema,
        attrs: &AttributeRegistry,
    ) -> Result<Vec<LocalAttributeId>, AnyError> {
        index
            .id
            .verify_non_nil()
            .context("Index can not have a nil Id")?;

        if let Some(_old) = self.get_by_uid(index.id) {
            return Err(anyhow!("Index id already exists: '{}'", index.id));
        }

        crate::schema::validate_namespaced_ident(&index.ident)?;
        if let Some(_old) = self.get_by_name(&index.ident) {
            return Err(anyhow!("Index ident already exists: '{}'", index.ident));
        }

        if index.ident.len() > MAX_INDEX_NAME_LEN {
            return Err(anyhow!(
                "Index name '{}' exceeds maximum name length {}",
                index.ident,
                super::MAX_NAME_LEN
            ));
        }

        // Set used for uniqueness checking.
        let mut local_attribute_ids = Vec::new();

        for attr_id in &index.attributes {
            let attr_schema = attrs.must_get_by_uid(*attr_id)?;
            if local_attribute_ids.contains(&attr_schema.local_id) {
                return Err(anyhow!(
                    "Duplicate attribute in index: {}",
                    attr_schema.schema.ident
                ));
            }
            local_attribute_ids.push(attr_schema.local_id);
        }

        Ok(local_attribute_ids)
    }
}
