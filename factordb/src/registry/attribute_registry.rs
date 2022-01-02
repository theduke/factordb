use anyhow::{anyhow, Context};
use fnv::FnvHashMap;

use crate::{
    data::{Id, IdOrIdent, ValueType},
    error, schema,
    util::stable_map::{StableMap, StableMapKey},
    AnyError,
};

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
    pub schema: schema::AttributeSchema,
    pub is_deleted: bool,
    pub namespace: String,
    pub plain_name: String,
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
    fn as_index(self) -> usize {
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

    fn add(&mut self, schema: schema::AttributeSchema) -> Result<LocalAttributeId, AnyError> {
        assert!(self.items.len() < u32::MAX as usize - 1);

        let (namespace, plain_name) = crate::schema::validate_namespaced_ident(&schema.ident)
            .map(|(a, b)| (a.to_string(), b.to_string()))?;
        let uid = schema.id;
        let ident = schema.ident.clone();

        let local_id = self.items.insert_with(move |local_id| RegisteredAttribute {
            local_id,
            namespace: namespace.to_string(),
            plain_name: plain_name.to_string(),
            schema,
            is_deleted: false,
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
    // ) -> Result<&RegisteredAttribute, error::AttributeNotFound> {
    //     let item = self.get_maybe_deleted(id);
    //     if item.is_deleted {
    //         Err(error::AttributeNotFound::new(
    //             item.schema.ident.clone().into(),
    //         ))
    //     } else {
    //         Ok(item)
    //     }
    // }

    pub fn get_by_uid(&self, uid: Id) -> Option<&RegisteredAttribute> {
        self.uids.get(&uid).and_then(|id| self.get(*id))
    }

    pub fn must_get_by_uid(
        &self,
        uid: Id,
    ) -> Result<&RegisteredAttribute, error::AttributeNotFound> {
        self.get_by_uid(uid)
            .ok_or_else(|| error::AttributeNotFound::new(uid.into()))
    }

    pub fn get_by_name(&self, name: &str) -> Option<&RegisteredAttribute> {
        self.names.get(name).and_then(|id| self.get(*id))
    }

    pub fn must_get_by_name(
        &self,
        name: &str,
    ) -> Result<&RegisteredAttribute, error::AttributeNotFound> {
        self.get_by_name(name)
            .ok_or_else(|| error::AttributeNotFound::new(name.into()))
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
    ) -> Result<&RegisteredAttribute, error::AttributeNotFound> {
        match ident {
            IdOrIdent::Id(id) => self.must_get_by_uid(*id),
            IdOrIdent::Name(name) => self.must_get_by_name(name),
        }
    }

    // NOTE: Only pub(super) because [Registry] might do additional validation.
    pub(super) fn register(
        &mut self,
        attr: schema::AttributeSchema,
    ) -> Result<LocalAttributeId, AnyError> {
        self.validate_schema(&attr, false)?;
        self.add(attr)
    }

    /// Update an existing entity.
    pub(super) fn update(
        &mut self,
        schema: schema::AttributeSchema,
        validate: bool,
    ) -> Result<LocalAttributeId, AnyError> {
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
        attr: &schema::AttributeSchema,
        allow_existing: bool,
    ) -> Result<(), AnyError> {
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

        crate::schema::validate_namespaced_ident(&attr.ident)?;

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
            other => {
                return Err(anyhow!(
                    "Invalid attribute type {:?} - attributes must be scalar values",
                    other,
                ));
            }
        }
        Ok(())
    }

    pub(super) fn remove(&mut self, id: Id) -> Result<(), AnyError> {
        let local_id = self.must_get_by_uid(id)?.local_id;
        self.items.get_mut(local_id).is_deleted = true;
        Ok(())
    }
}
