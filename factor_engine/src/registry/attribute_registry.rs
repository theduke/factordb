use anyhow::{anyhow, Context};
use fnv::FnvHashMap;

use factor_core::{
    data::{Id, IdOrIdent, Ident, ValueType},
    error::{AttributeNotFound, EntityNotFound},
    schema,
};

use crate::util::{
    stable_map::{StableMap, StableMapKey},
    VecSet,
};

use super::entity_registry::EntityRegistry;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LocalAttributeId(u32);

impl LocalAttributeId {
    pub(super) const fn from_u32(val: u32) -> Self {
        Self(val)
    }
}

#[derive(Clone, Debug)]
pub struct RegisteredAttribute {
    pub local_id: LocalAttributeId,
    pub schema: schema::Attribute,
    pub is_deleted: bool,
    pub namespace: String,
    pub plain_name: String,

    pub ref_allowed_entity_types: Option<VecSet<Id>>,
}

#[derive(Clone, Debug)]
pub struct AttributeRegistry {
    pub items: StableMap<LocalAttributeId, RegisteredAttribute>,
    uids: FnvHashMap<Id, LocalAttributeId>,
    names: FnvHashMap<String, LocalAttributeId>,
}

impl StableMapKey for LocalAttributeId {
    #[inline]
    fn from_index(index: usize) -> Self {
        Self(index as u32)
    }

    #[inline]
    fn as_index(&self) -> usize {
        self.0 as usize
    }
}

impl AttributeRegistry {
    pub fn new() -> Self {
        Self {
            items: StableMap::new(),
            uids: Default::default(),
            names: Default::default(),
        }
    }

    pub fn reset(&mut self) {
        self.items = StableMap::new();
        self.uids.clear();
        self.names.clear();
    }

    fn add(
        &mut self,
        schema: schema::Attribute,
        entities: &EntityRegistry,
    ) -> Result<LocalAttributeId, anyhow::Error> {
        assert!(self.items.len() < u32::MAX as usize - 1);

        let (namespace, plain_name) =
            Ident::parse_parts(&schema.ident).map(|(a, b)| (a.to_string(), b.to_string()))?;
        let uid = schema.id;
        let ident = schema.ident.clone();

        let ref_allowed_entity_types = match &schema.value_type {
            ValueType::List(inner) => match &**inner {
                ValueType::RefConstrained(con) => {
                    let ids = con
                        .allowed_entity_types
                        .iter()
                        .map(|ty| -> Result<_, EntityNotFound> {
                            let entity = entities.must_get_by_ident(ty)?;
                            Ok(entity.schema.id)
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    let ids = VecSet::from_iter(ids);
                    Some(ids)
                }
                _ => None,
            },
            ValueType::Ref => None,
            ValueType::RefConstrained(con) => {
                // TODO: code is same as above, unify with helper funciton!
                let ids = con
                    .allowed_entity_types
                    .iter()
                    .map(|ty| -> Result<_, EntityNotFound> {
                        let entity = entities.must_get_by_ident(ty)?;
                        Ok(entity.schema.id)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let ids = VecSet::from_iter(ids);
                Some(ids)
            }
            _ => None,
        };

        let local_id = self.items.insert_with(move |local_id| RegisteredAttribute {
            local_id,
            namespace: namespace.to_string(),
            plain_name: plain_name.to_string(),
            schema,
            is_deleted: false,
            ref_allowed_entity_types,
        });

        self.uids.insert(uid, local_id);
        self.names.insert(ident, local_id);
        Ok(local_id)
    }

    #[inline]
    pub fn get_maybe_deleted(&self, id: LocalAttributeId) -> &RegisteredAttribute {
        self.items.get(id)
    }

    #[inline]
    pub fn get(&self, id: LocalAttributeId) -> Option<&RegisteredAttribute> {
        let item = self.items.get(id);
        if item.is_deleted {
            None
        } else {
            Some(item)
        }
    }

    // pub fn must_get(
    //     &self,
    //     id: LocalAttributeId,
    // ) -> Result<&RegisteredAttribute, AttributeNotFound> {
    //     let item = self.get_maybe_deleted(id);
    //     if item.is_deleted {
    //         Err(AttributeNotFound::new(
    //             item.schema.ident.clone().into(),
    //         ))
    //     } else {
    //         Ok(item)
    //     }
    // }

    pub fn get_by_uid(&self, uid: Id) -> Option<&RegisteredAttribute> {
        self.uids.get(&uid).and_then(|id| self.get(*id))
    }

    pub fn must_get_by_uid(&self, uid: Id) -> Result<&RegisteredAttribute, AttributeNotFound> {
        self.get_by_uid(uid)
            .ok_or_else(|| AttributeNotFound::new(uid.into()))
    }

    pub fn get_by_name(&self, name: &str) -> Option<&RegisteredAttribute> {
        self.names.get(name).and_then(|id| self.get(*id))
    }

    pub fn must_get_by_name(&self, name: &str) -> Result<&RegisteredAttribute, AttributeNotFound> {
        self.get_by_name(name)
            .ok_or_else(|| AttributeNotFound::new(name.into()))
    }

    pub fn get_by_ident(&self, ident: &IdOrIdent) -> Option<&RegisteredAttribute> {
        match ident {
            IdOrIdent::Id(id) => self.get_by_uid(*id),
            IdOrIdent::Name(name) => self.get_by_name(name),
        }
    }

    pub fn must_get_by_ident(
        &self,
        ident: &IdOrIdent,
    ) -> Result<&RegisteredAttribute, AttributeNotFound> {
        match ident {
            IdOrIdent::Id(id) => self.must_get_by_uid(*id),
            IdOrIdent::Name(name) => self.must_get_by_name(name),
        }
    }

    // NOTE: Only pub(super) because [Registry] might do additional validation.
    pub(super) fn register(
        &mut self,
        attr: schema::Attribute,
        entities: &EntityRegistry,
    ) -> Result<LocalAttributeId, anyhow::Error> {
        self.validate_schema(&attr, false)?;
        self.add(attr, entities)
    }

    /// Update an existing entity.
    pub(super) fn update(
        &mut self,
        schema: schema::Attribute,
        validate: bool,
    ) -> Result<LocalAttributeId, anyhow::Error> {
        schema
            .id
            .verify_non_nil()
            .context("Attribute can not have a nil id")?;
        let old_id = self.must_get_by_uid(schema.id)?.local_id;

        if validate {
            self.validate_schema(&schema, true)?;
        }

        self.items.get_mut(old_id).schema = schema;
        Ok(old_id)
    }

    fn validate_schema(
        &self,
        attr: &schema::Attribute,
        allow_existing: bool,
    ) -> Result<(), anyhow::Error> {
        attr.id
            .verify_non_nil()
            .context("Attribute can not have a nil Id")?;

        if !allow_existing {
            if let Some(_old) = self.get_by_uid(attr.id) {
                return Err(anyhow!("Attribute id already exists: '{}'", attr.id));
            }
            if let Some(_old) = self.get_by_name(&attr.ident) {
                return Err(anyhow!("Attribute ident already exists: '{}'", attr.ident));
            }
        }

        Ident::parse_parts(&attr.ident)?;

        if attr.ident.len() > super::MAX_NAME_LEN {
            return Err(anyhow!(
                "Attribute name '{}' exceeds maximum name length {}",
                attr.ident,
                super::MAX_NAME_LEN
            ));
        }

        match &attr.value_type {
            x if x.is_scalar() => {}
            ValueType::Object(obj) => {
                for field in &obj.fields {
                    if field.name.len() > super::MAX_NAME_LEN {
                        return Err(anyhow!(
                            "attribute field name '{}' exceeds maximum field name length of {}",
                            field.name,
                            super::MAX_NAME_LEN
                        ));
                    }
                }
            }
            ValueType::List(item_type) => {
                if !item_type.is_scalar() {
                    // FIXME: removed after allowing List<_>, validate if any checks need to be added
                    // bail!("List item type '{:?}' is not a scalar", item_type);
                }
            }
            other => {
                return Err(anyhow!(
                    "Invalid attribute type {:?} - attributes must be scalar values",
                    other,
                ));
            }
        }
        Ok(())
    }

    pub(super) fn remove(&mut self, id: Id) -> Result<(), anyhow::Error> {
        let local_id = self.must_get_by_uid(id)?.local_id;
        self.items.get_mut(local_id).is_deleted = true;
        Ok(())
    }
}
