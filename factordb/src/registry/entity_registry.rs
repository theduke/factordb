use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Context};
use fnv::FnvHashMap;

use crate::{
    data::{Id, Ident},
    error, schema, AnyError,
};

use super::attribute_registry::AttributeRegistry;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LocalEntityId(u32);

#[derive(Clone, Debug)]
pub struct RegisteredEntity {
    pub local_id: LocalEntityId,
    pub schema: crate::schema::EntitySchema,
    pub is_deleted: bool,
    pub namespace: String,
    pub plain_name: String,
}

#[derive(Clone, Debug)]
pub struct EntityRegistry {
    pub items: Vec<RegisteredEntity>,
    uids: FnvHashMap<Id, LocalEntityId>,
    names: FnvHashMap<String, LocalEntityId>,
}

impl EntityRegistry {
    pub fn new() -> Self {
        // NOTE: we start ids at 1, so the vec contains a None sentinel for
        // the 0 id.
        let sentinel = RegisteredEntity {
            local_id: LocalEntityId(0),
            schema: crate::schema::EntitySchema {
                id: Id::nil(),
                ident: "".to_string(),
                title: None,
                description: None,
                attributes: Vec::new(),
                extends: Vec::new(),
                strict: true,
            },
            is_deleted: true,
            namespace: String::new(),
            plain_name: String::new(),
        };
        Self {
            items: vec![sentinel],
            uids: Default::default(),
            names: Default::default(),
        }
    }

    pub fn reset(&mut self) {
        self.items.truncate(1);
        self.uids.clear();
        self.names.clear();
    }

    fn add(&mut self, schema: crate::schema::EntitySchema) -> Result<LocalEntityId, AnyError> {
        assert!(self.items.len() < u32::MAX as usize - 1);

        let (namespace, plain_name) = crate::schema::validate_namespaced_ident(&schema.ident)?;

        let local_id = LocalEntityId(self.items.len() as u32);
        let item = RegisteredEntity {
            local_id,
            namespace: namespace.to_string(),
            plain_name: plain_name.to_string(),
            schema,
            is_deleted: false,
        };
        self.uids.insert(item.schema.id, local_id);
        self.names.insert(item.schema.ident.clone(), local_id);
        if self.names.contains_key(&item.schema.ident) {
            self.names.remove(&item.schema.ident);
        } else {
            self.names.insert(item.schema.ident.clone(), local_id);
        }
        self.items.push(item);
        Ok(local_id)
    }

    #[inline]
    // pub fn get_maybe_deleted(&self, id: LocalEntityId) -> &RegisteredEntity {
    //     // NOTE: this panics, but this is acceptable because a LocalEntityId
    //     // is always valid.
    //     &self.items[id.0 as usize]
    // }
    #[inline]
    pub fn get(&self, id: LocalEntityId) -> Option<&RegisteredEntity> {
        // NOTE: this panics, but this is acceptable because a LocalEntityId
        // is always valid.
        let item = &self.items[id.0 as usize];
        if item.is_deleted {
            None
        } else {
            Some(item)
        }
    }

    // pub fn must_get(&self, id: LocalEntityId) -> Result<&RegisteredEntity, error::EntityNotFound> {
    //     let item = self.get_maybe_deleted(id);
    //     if item.is_deleted {
    //         Err(error::EntityNotFound::new(item.schema.ident.clone().into()))
    //     } else {
    //         Ok(item)
    //     }
    // }

    pub fn get_by_uid(&self, uid: Id) -> Option<&RegisteredEntity> {
        self.uids.get(&uid).and_then(|id| self.get(*id))
    }

    pub fn must_get_by_uid(&self, uid: Id) -> Result<&RegisteredEntity, error::EntityNotFound> {
        self.get_by_uid(uid)
            .ok_or_else(|| error::EntityNotFound::new(uid.into()))
    }

    pub fn get_by_name(&self, name: &str) -> Option<&RegisteredEntity> {
        self.names.get(name).and_then(|id| self.get(*id))
    }

    pub fn must_get_by_name(&self, name: &str) -> Result<&RegisteredEntity, error::EntityNotFound> {
        self.get_by_name(name)
            .ok_or_else(|| error::EntityNotFound::new(name.into()))
    }

    pub fn get_by_ident(&self, ident: &Ident) -> Option<&RegisteredEntity> {
        match ident {
            Ident::Id(id) => self.get_by_uid(*id),
            Ident::Name(name) => self.get_by_name(name),
        }
    }

    pub fn must_get_by_ident(
        &self,
        ident: &Ident,
    ) -> Result<&RegisteredEntity, error::EntityNotFound> {
        match ident {
            Ident::Id(id) => self.must_get_by_uid(*id),
            Ident::Name(name) => self.must_get_by_name(name),
        }
    }

    /// Register a new entity.
    // NOTE: Only pub(super) because [Registry] might do additional validation.
    pub(super) fn register(
        &mut self,
        entity: schema::EntitySchema,
        validate: bool,
        attrs: &AttributeRegistry,
    ) -> Result<LocalEntityId, AnyError> {
        entity
            .id
            .verify_non_nil()
            .context("Entity can not have a nil id")?;

        if let Some(_old) = self.get_by_uid(entity.id) {
            return Err(anyhow!("Entity id already exists: '{}'", entity.id));
        }
        if let Some(_old) = self.get_by_name(&entity.ident) {
            return Err(anyhow!("Entity id already exists: '{}'", entity.id));
        }

        if validate {
            self.validate_schema(&entity, attrs)?;
        }

        self.add(entity)
    }

    fn validate_schema(
        &self,
        entity: &schema::EntitySchema,
        attrs: &AttributeRegistry,
    ) -> Result<(), AnyError> {
        crate::schema::validate_namespaced_ident(&entity.ident)?;

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

        // Set for ensuring no duplicate extends.
        let mut extended_ids = HashSet::<Id>::new();
        // All extended fields.
        // Used for ensuring that extends do not differ in type.
        let mut extended_fields = HashMap::<Id, schema::EntityAttribute>::new();

        for parent_ident in &entity.extends {
            let parent = self
                .must_get_by_ident(parent_ident)
                .context("Invalid parent")?;
            if parent.schema.id == entity.id {
                return Err(anyhow!("Entity can't extend itself"));
            }
            if extended_ids.contains(&parent.schema.id) {
                return Err(anyhow!("Can't specify the same parent type twice"));
            }
            extended_ids.insert(parent.schema.id);

            for field in &parent.schema.attributes {
                let attr = attrs.must_get_by_ident(&field.attribute)?;
                if let Some(existing_field) = extended_fields.get(&attr.schema.id) {
                    if field.cardinality != existing_field.cardinality {
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
            let attr = attrs.must_get_by_ident(&field.attribute)?;

            if attr_set.contains(&attr.schema.id) {
                return Err(anyhow!("Duplicate attribute: '{}'", attr.schema.ident,));
            }
            attr_set.insert(attr.schema.id);

            if let Some(extended_field) = extended_fields.get(&attr.schema.id) {
                if field.cardinality != extended_field.cardinality {
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
}
