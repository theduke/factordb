use anyhow::{anyhow, Context};
use fnv::FnvHashMap;

use crate::{
    data::{Ident, ValueType},
    error, schema, AnyError, Id,
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LocalAttributeId(u32);

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
    pub items: Vec<RegisteredAttribute>,
    uids: FnvHashMap<Id, LocalAttributeId>,
    names: FnvHashMap<String, LocalAttributeId>,
}

impl AttributeRegistry {
    pub fn new() -> Self {
        // NOTE: we start ids at 1, so the vec contains a None sentinel for
        // the 0 id.
        let sentinel = RegisteredAttribute {
            local_id: LocalAttributeId(0),
            schema: schema::AttributeSchema::new("", "", ValueType::Any),
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

    fn add(&mut self, schema: schema::AttributeSchema) -> Result<LocalAttributeId, AnyError> {
        assert!(self.items.len() < u32::MAX as usize - 1);

        let (namespace, plain_name) = crate::schema::validate_namespaced_ident(&schema.ident)?;

        let local_id = LocalAttributeId(self.items.len() as u32);
        let item = RegisteredAttribute {
            local_id,
            namespace: namespace.to_string(),
            plain_name: plain_name.to_string(),
            schema,
            is_deleted: false,
        };
        self.uids.insert(item.schema.id, local_id);
        self.names.insert(item.schema.ident.clone(), local_id);
        self.items.push(item);
        Ok(local_id)
    }

    #[inline]
    pub fn get_maybe_deleted(&self, id: LocalAttributeId) -> &RegisteredAttribute {
        // NOTE: this panics, but this is acceptable because a LocalAttributeId
        // is always valid.
        &self.items[id.0 as usize]
    }

    #[inline]
    pub fn get(&self, id: LocalAttributeId) -> Option<&RegisteredAttribute> {
        // NOTE: this panics, but this is acceptable because a LocalAttributeId
        // is always valid.
        let item = &self.items[id.0 as usize];
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

    pub fn get_by_ident(&self, ident: &Ident) -> Option<&RegisteredAttribute> {
        match ident {
            Ident::Id(id) => self.get_by_uid(*id),
            Ident::Name(name) => self.get_by_name(name),
        }
    }

    pub fn must_get_by_ident(
        &self,
        ident: &Ident,
    ) -> Result<&RegisteredAttribute, error::AttributeNotFound> {
        match ident {
            Ident::Id(id) => self.must_get_by_uid(*id),
            Ident::Name(name) => self.must_get_by_name(name),
        }
    }

    // NOTE: Only pub(super) because [Registry] might do additional validation.
    pub(super) fn register(
        &mut self,
        attr: schema::AttributeSchema,
    ) -> Result<LocalAttributeId, AnyError> {
        self.validate_schema(&attr)?;
        self.add(attr)
    }

    fn validate_schema(&self, attr: &schema::AttributeSchema) -> Result<(), AnyError> {
        attr.id
            .verify_non_nil()
            .context("Attribute can not have a nil Id")?;

        if let Some(_old) = self.get_by_uid(attr.id) {
            return Err(anyhow!("Attribute id already exists: '{}'", attr.id));
        }

        crate::schema::validate_namespaced_ident(&attr.ident)?;
        if let Some(_old) = self.get_by_name(&attr.ident) {
            return Err(anyhow!("Attribute ident already exists: '{}'", attr.ident));
        }

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
        self.items[local_id.0 as usize].is_deleted = true;
        Ok(())
    }
}
