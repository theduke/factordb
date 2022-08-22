use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, bail, Context};
use fnv::{FnvHashMap, FnvHashSet};

use crate::util::stable_map::{StableMap, StableMapKey};

use factor_core::{
    data::{Id, IdOrIdent, Ident},
    error::EntityNotFound,
    schema,
};

use super::attribute_registry::AttributeRegistry;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LocalEntityId(u32);

#[derive(Clone, Debug)]
pub struct RegisteredEntity {
    pub local_id: LocalEntityId,
    pub schema: schema::Class,
    pub is_deleted: bool,
    pub namespace: String,
    pub plain_name: String,
    pub extends: FnvHashSet<LocalEntityId>,
    /// Stores all child ids, including nested children.
    pub nested_children: FnvHashSet<Id>,
    pub nested_attribute_names: FnvHashSet<String>,
}

#[derive(Clone, Debug)]
pub struct EntityRegistry {
    items: StableMap<LocalEntityId, RegisteredEntity>,
    /// Lookup table that maps an [`Id`]` to it's [`LocalEntityId`].
    /// Useful for fast lookups.
    uids: FnvHashMap<Id, LocalEntityId>,
    /// Lookup table that maps the entity name to it's [`LocalEntityId`].
    /// Useful for fast lookups.
    names: FnvHashMap<String, LocalEntityId>,
}

impl StableMapKey for LocalEntityId {
    #[inline]
    fn from_index(index: usize) -> Self {
        Self(index as u32)
    }

    #[inline]
    fn as_index(&self) -> usize {
        self.0 as usize
    }
}

impl EntityRegistry {
    pub fn new() -> Self {
        Self {
            items: StableMap::new(),
            uids: Default::default(),
            names: Default::default(),
        }
    }

    fn add_entity_hierarchy_item(&mut self, parent: LocalEntityId, child_id: Id) {
        if let Some(parent) = self.get_mut(parent) {
            parent.nested_children.insert(child_id);

            let nested = parent.extends.clone();
            for nested_parent in nested {
                self.add_entity_hierarchy_item(nested_parent, child_id);
            }
        }
    }

    fn add_entity_to_hierarchy(&mut self, entity: &RegisteredEntity) {
        for parent_id in &entity.extends {
            self.add_entity_hierarchy_item(*parent_id, entity.schema.id);
        }
    }

    fn build_registered_entity(
        &self,
        schema: schema::Class,
        attrs: &AttributeRegistry,
    ) -> Result<RegisteredEntity, anyhow::Error> {
        let (namespace, plain_name) =
            Ident::parse_parts(&schema.ident).map(|(a, b)| (a.to_string(), b.to_string()))?;

        let mut parent_ids = FnvHashSet::<LocalEntityId>::default();
        let mut nested_attribute_names = FnvHashSet::<String>::default();

        for parent_name in &schema.extends {
            let parent = self.must_get_by_name(parent_name)?;
            parent_ids.insert(parent.local_id);
            nested_attribute_names.extend(parent.nested_attribute_names.clone());
        }

        for field in &schema.attributes {
            let attr = attrs.must_get_by_name(&field.attribute)?;
            nested_attribute_names.insert(attr.schema.ident.clone());
        }

        Ok(RegisteredEntity {
            local_id: LocalEntityId(0),
            schema,
            is_deleted: false,
            namespace,
            plain_name,
            extends: parent_ids,
            nested_children: FnvHashSet::default(),
            nested_attribute_names,
        })
    }

    fn add(
        &mut self,
        schema: schema::Class,
        attrs: &AttributeRegistry,
    ) -> Result<LocalEntityId, anyhow::Error> {
        assert!(self.items.len() < u32::MAX as usize - 1);
        assert!(!schema.id.is_nil());

        let registered = self.build_registered_entity(schema.clone(), attrs)?;

        let local_id = self.items.insert_with(move |local_id| RegisteredEntity {
            local_id,
            ..registered
        });

        self.add_entity_to_hierarchy(&self.get(local_id).unwrap().clone());
        self.uids.insert(schema.id, local_id);
        self.names.insert(schema.ident, local_id);
        Ok(local_id)
    }

    // #[inline]
    // pub fn get_maybe_deleted(&self, id: LocalEntityId) -> &RegisteredEntity {
    //     // NOTE: this panics, but this is acceptable because a LocalEntityId
    //     // is always valid.
    //     &self.items[id.0 as usize]
    // }

    pub fn get(&self, id: LocalEntityId) -> Option<&RegisteredEntity> {
        let item = self.items.get(id);
        if item.is_deleted {
            None
        } else {
            Some(item)
        }
    }

    pub fn get_mut(&mut self, id: LocalEntityId) -> Option<&mut RegisteredEntity> {
        let item = self.items.get_mut(id);
        if item.is_deleted {
            None
        } else {
            Some(item)
        }
    }

    pub fn get_by_uid(&self, uid: Id) -> Option<&RegisteredEntity> {
        self.uids.get(&uid).and_then(|id| self.get(*id))
    }

    pub fn must_get_by_uid(&self, uid: Id) -> Result<&RegisteredEntity, EntityNotFound> {
        self.get_by_uid(uid)
            .ok_or_else(|| EntityNotFound::new(uid.into()))
    }

    pub fn get_by_name(&self, name: &str) -> Option<&RegisteredEntity> {
        self.names.get(name).and_then(|id| self.get(*id))
    }

    pub fn must_get_by_name(&self, name: &str) -> Result<&RegisteredEntity, EntityNotFound> {
        self.get_by_name(name)
            .ok_or_else(|| EntityNotFound::new(name.into()))
    }

    pub fn get_by_ident(&self, ident: &IdOrIdent) -> Option<&RegisteredEntity> {
        match ident {
            IdOrIdent::Id(id) => self.get_by_uid(*id),
            IdOrIdent::Name(name) => self.get_by_name(name),
        }
    }

    pub fn must_get_by_ident(
        &self,
        ident: &IdOrIdent,
    ) -> Result<&RegisteredEntity, EntityNotFound> {
        match ident {
            IdOrIdent::Id(id) => self.must_get_by_uid(*id),
            IdOrIdent::Name(name) => self.must_get_by_name(name),
        }
    }

    pub fn iter(&self) -> std::slice::Iter<RegisteredEntity> {
        self.items.iter()
    }

    /* pub fn iter_mut(&mut self) -> std::slice::IterMut<RegisteredEntity> {
        self.items.iter_mut()
    } */

    /// Register a new entity.
    // NOTE: Only pub(super) because [Registry] might do additional validation.
    pub(super) fn register(
        &mut self,
        entity: schema::Class,
        validate: bool,
        attrs: &AttributeRegistry,
    ) -> Result<LocalEntityId, anyhow::Error> {
        entity
            .id
            .verify_non_nil()
            .context("Entity can not have a nil id")?;

        // FIXME: this shouldn't be commented out!
        // if let Some(_old) = self.get_by_uid(entity.id) {
        //     return Err(anyhow!("Entity id already exists: '{}'", entity.id));
        // }
        // if let Some(_old) = self.get_by_name(&entity.ident) {
        //     return Err(anyhow!("Entity ident already exists: '{}'", entity.id));
        // }

        if validate {
            self.validate_schema(&entity, attrs, true)?;
        }

        self.add(entity, attrs)
    }

    /// Register a new entity.
    // NOTE: Only pub(super) because [Registry] might do additional validation.
    pub(super) fn update(
        &mut self,
        entity: schema::Class,
        validate: bool,
        attrs: &AttributeRegistry,
    ) -> Result<LocalEntityId, anyhow::Error> {
        entity
            .id
            .verify_non_nil()
            .context("Entity can not have a nil id")?;

        if validate {
            self.validate_schema(&entity, attrs, false)?;
        }

        // FIXME: this is incomplete, we need to update the hierarchy etc!

        let local_id = {
            let old = self.must_get_by_uid(entity.id)?;
            if old.schema.ident != entity.ident {
                bail!(
                    "Changing the ident of an entity is not supported (old: {}, new: {})",
                    old.schema.ident,
                    entity.ident
                );
            }
            old.local_id
        };

        let new = self.build_registered_entity(entity, attrs)?;
        *self.items.get_mut(local_id) = new;

        self.add_entity_to_hierarchy(&self.items.get(local_id).clone());

        // FIXME: also need to do additional cleanup of the hierarchy
        // (remove from nested_children if parent removed)!

        Ok(local_id)
    }

    fn validate_schema(
        &self,
        entity: &schema::Class,
        attrs: &AttributeRegistry,
        is_new: bool,
    ) -> Result<(), anyhow::Error> {
        Ident::parse_parts(&entity.ident)?;

        if is_new {
            if self.get_by_name(&entity.ident).is_some() {
                return Err(anyhow!(
                    "Entity with name '{}' already exists",
                    entity.ident
                ));
            }
            if self.get_by_uid(entity.id).is_some() {
                return Err(anyhow!(
                    "Duplicate entity id: '{}' for new entity '{}'",
                    entity.id,
                    entity.ident
                ));
            }
        }

        // Set for ensuring no duplicate extends.
        let mut extended_ids = HashSet::<Id>::new();
        // All extended fields.
        // Used for ensuring that extends do not differ in type.
        let mut extended_fields = HashMap::<Id, schema::ClassAttribute>::new();

        for parent_ident in &entity.extends {
            let parent = self
                .must_get_by_name(parent_ident)
                .context("Invalid parent")?;
            if parent.schema.id == entity.id {
                return Err(anyhow!("Entity can't extend itself"));
            }
            if extended_ids.contains(&parent.schema.id) {
                return Err(anyhow!("Can't specify the same parent type twice"));
            }
            extended_ids.insert(parent.schema.id);

            for field in &parent.schema.attributes {
                let attr = attrs.must_get_by_name(&field.attribute)?;
                if let Some(existing_field) = extended_fields.get(&attr.schema.id) {
                    if field.required != existing_field.required {
                        return Err(anyhow!(
                            "Invalid extend of parent entity '{}': the attribute '{}' already exists with a different cardinality", 
                            parent.schema.ident,
                            attr.schema.ident,
                        ));
                    }
                } else {
                    extended_fields.insert(attr.schema.id, field.clone());
                }
            }
        }

        // Set for ensuring field attribute uniqueness.
        let mut attr_set = HashSet::new();

        for field in &entity.attributes {
            let attr = attrs.must_get_by_name(&field.attribute)?;

            if attr_set.contains(&attr.schema.id) {
                return Err(anyhow!("Duplicate attribute: '{}'", attr.schema.ident,));
            }
            attr_set.insert(attr.schema.id);

            if let Some(extended_field) = extended_fields.get(&attr.schema.id) {
                if field.required != extended_field.required {
                    return Err(anyhow!(
                        "Invalid field '{}': the attribute already exists with a different cardinality on a parent entity",  
                        attr.schema.ident
                    ));
                }
            }
        }

        // FIXME: validate other stuff, like Relation.

        Ok(())
    }

    pub(super) fn remove(&mut self, id: Id) -> Result<(), anyhow::Error> {
        let local_id = self.must_get_by_uid(id)?.local_id;
        self.items.get_mut(local_id).is_deleted = true;
        // FIXME: need to update entity hierarchy (nested_children)
        Ok(())
    }
}
