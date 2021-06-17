mod attribute_registry;
mod entity_registry;

use std::{
    str::FromStr,
    sync::{Arc, RwLock},
};

use anyhow::anyhow;

use crate::{
    backend::{DbOp, TupleCreate, TupleDelete, TupleMerge, TupleOp, TupleReplace},
    data::{DataMap, Id, IdMap, Ident, Value, ValueType},
    error, query,
    schema::{self, builtin::AttrId, AttrMapExt, AttributeDescriptor, Cardinality},
    AnyError,
};

pub use self::{
    attribute_registry::{LocalAttributeId, RegisteredAttribute},
    entity_registry::{LocalEntityId, RegisteredEntity},
};

const MAX_NAME_LEN: usize = 50;

#[derive(Clone, Debug)]
pub struct Registry {
    entities: entity_registry::EntityRegistry,
    attrs: attribute_registry::AttributeRegistry,
}

impl Registry {
    pub fn new() -> Self {
        let mut s = Self {
            attrs: attribute_registry::AttributeRegistry::new(),
            entities: entity_registry::EntityRegistry::new(),
        };
        s.add_builtins();
        s
    }

    /// Reset all state.
    /// Removes all registered entities and attributes, but restores the
    /// builtins.
    pub fn reset(&mut self) {
        self.attrs.reset();
        self.entities.reset();
        self.add_builtins();
    }

    pub fn into_shared(self) -> SharedRegistry {
        Arc::new(RwLock::new(self))
    }

    pub fn attr(&self, id: LocalAttributeId) -> &RegisteredAttribute {
        self.attrs.get_maybe_deleted(id)
    }

    #[inline]
    pub fn attr_by_name(&self, name: &str) -> Option<&RegisteredAttribute> {
        self.attrs.get_by_name(name)
    }

    #[inline]
    pub fn attr_by_ident(&self, ident: &Ident) -> Option<&RegisteredAttribute> {
        self.attrs.get_by_ident(ident)
    }

    #[inline]
    pub fn require_attr(&self, id: Id) -> Result<&RegisteredAttribute, error::AttributeNotFound> {
        self.attrs.must_get_by_uid(id)
    }

    #[inline]
    pub fn entity_by_ident(&self, ident: &Ident) -> Option<&RegisteredEntity> {
        self.entities.get_by_ident(ident)
    }

    #[inline]
    pub fn require_attr_by_name(
        &self,
        name: &str,
    ) -> Result<&RegisteredAttribute, error::AttributeNotFound> {
        self.attrs.must_get_by_name(name)
    }

    fn add_builtins(&mut self) {
        let schema = schema::builtin::builtin_db_schema();
        for attr in schema.attributes {
            self.register_attribute(attr).unwrap();
        }
        for entity in schema.entities {
            self.register_entity(entity, true).unwrap();
        }
    }

    pub fn id_to_data_map(&self, map: IdMap) -> Result<DataMap, AnyError> {
        let ident_map = map
            .into_iter()
            .map(|(id, value)| -> Result<_, AnyError> {
                let ident = self.attrs.must_get_by_uid(id)?.schema.ident.to_string();
                Ok((ident, value))
            })
            .collect::<Result<_, _>>()?;
        Ok(ident_map)
    }

    pub fn data_to_id_map(&self, map: DataMap) -> Result<IdMap, AnyError> {
        let data_map = map
            .into_inner()
            .into_iter()
            .map(|(name, value)| -> Result<_, AnyError> {
                let attr = self.attrs.must_get_by_name(&name)?;
                Ok((attr.schema.id, value))
            })
            .collect::<Result<_, _>>()?;
        Ok(data_map)
    }

    // /// Ensure that an attribute can be deleted.
    // pub fn validate_remove_attr(
    //     &mut self,
    //     id: Id,
    // ) -> Result<schema::AttributeSchema, AnyError> {

    //     let attr = self
    //         .attrs
    //         .must_get_by_uid(id)?;

    //     self.entities.items.iter().for_each(|entity| {
    //         entity.schema.attributes.retain(|field| match &field.attribute {
    //             Ident::Id(ent_id) => *ent_id != id,
    //             Ident::Name(name) => name.as_ref() != attr.name,
    //         });
    //     });

    //     Ok(attr)
    // }

    pub fn register_attribute(
        &mut self,
        attr: schema::AttributeSchema,
    ) -> Result<LocalAttributeId, AnyError> {
        self.attrs.register(attr)
    }

    pub fn register_entity(
        &mut self,
        entity: schema::EntitySchema,
        validate: bool,
    ) -> Result<LocalEntityId, AnyError> {
        self.entities.register(entity, validate, &self.attrs)
    }

    /// Valdiate that a value conforms to a value type.
    /// Coerces values into the desired type where appropriate.
    fn validate_coerce_value_named(
        name: &str,
        ty: &ValueType,
        value: &mut Value,
    ) -> Result<(), AnyError> {
        match ty {
            ValueType::Unit
            | ValueType::Bool
            | ValueType::Int
            | ValueType::UInt
            | ValueType::Float
            | ValueType::String
            | ValueType::Map
            | ValueType::Bytes => {
                let actual_ty = value.value_type();
                if &actual_ty != ty {
                    return Err(anyhow!(
                        "Invalid attribute '{}' - expected a {:?} but got '{:?}'",
                        name,
                        ty,
                        actual_ty
                    ));
                }
            }
            ValueType::List(_item_type) => {
                panic!("Internal error: List is not a valid ValueType for attributes");
            }
            ValueType::Any => {
                // Everything is allowed.
            }
            ValueType::Union(variants) => {
                for variant_ty in variants {
                    if Self::validate_coerce_value_named(name, variant_ty, value).is_ok() {
                        return Ok(());
                    }
                }
                return Err(anyhow!(
                    "Invalid attribute '{}' - does not conform to any variant of '{:?}'",
                    name,
                    variants,
                ));
            }
            ValueType::Object(_obj) => {
                // FIXME: validate objects properly.

                let actual_ty = value.value_type();
                if &actual_ty != ty {
                    return Err(anyhow!(
                        "Invalid attribute '{}' - expected a {:?} but got '{:?}'",
                        name,
                        ty,
                        actual_ty
                    ));
                }
            }
            ValueType::DateTime => {
                if !value.is_uint() {
                    // TODO: coerce?
                    return Err(anyhow!("Invalid timestamp - must be an unsigned integer"));
                }
            }
            ValueType::Url => {
                if let Some(v) = value.as_str() {
                    if let Err(_err) = url::Url::parse(v) {
                        return Err(anyhow!(
                            "Invalid attribute '{}' - expected a valid URL",
                            name
                        ));
                    }
                } else {
                    return Err(anyhow!("Invalid url - expected an integer"));
                }
            }
            ValueType::Ref => {
                match value {
                    Value::String(strval) => {
                        // TODO: resolve idents?
                        if let Err(_err) = uuid::Uuid::from_str(strval) {
                            return Err(anyhow!(
                                "Invalid attribute '{}' - expected a valid id (UUID)",
                                name
                            ));
                        }
                    }
                    Value::Id(_) => {
                        // Ok
                    }
                    _other => {
                        return Err(anyhow!(
                            "Invalid attribute '{}' - expected a valid ID (UUID)",
                            name
                        ))
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_attr_value(
        &self,
        attr: &RegisteredAttribute,
        value: &mut Value,
    ) -> Result<(), AnyError> {
        Self::validate_coerce_value_named(&attr.schema.ident, &attr.schema.value_type, value)
    }

    // fn make_id_map(
    //     &self,
    //     map: IdentifiableMap,
    //     validate: bool,
    // ) -> Result<FnvHashMap<Id, Value>, AnyError> {
    //     map.into_iter()
    //         .map(|(key, mut value)| {
    //             let attr = self.must_resolve_attr(&key)?;
    //             if validate {
    //                 value = self.validate_attr_value(attr, value)?;
    //             }
    //             Ok((attr.id, value))
    //         })
    //         .collect()
    // }
    //

    fn validate_entity_data(
        &self,
        data: &mut DataMap,
        entity: &RegisteredEntity,
    ) -> Result<(), AnyError> {
        for field in &entity.schema.attributes {
            // TODO: create a static list of fields for each entity so that
            // we don't have to do this lookup each time.
            let attr = self.attrs.must_get_by_ident(&field.attribute)?;

            match (data.get_mut(&attr.schema.ident), field.cardinality) {
                (None, Cardinality::Optional) => {}
                (None, Cardinality::Required) => {
                    return Err(anyhow!(
                        "Missing required attribute '{}'",
                        attr.schema.ident
                    ));
                }
                (None, Cardinality::Many) => {
                    // We could insert a list here, but that decision is
                    // probably better left to the backend.
                }
                (Some(value), Cardinality::Optional) => {
                    self.validate_attr_value(attr, value)?;
                }
                (Some(value), Cardinality::Required) => {
                    self.validate_attr_value(attr, value)?;
                }
                (Some(value), Cardinality::Many) => match value {
                    Value::List(items) => {
                        for item in items {
                            self.validate_attr_value(attr, item)?;
                        }
                    }
                    single => {
                        self.validate_attr_value(attr, single)?;
                        let value = std::mem::replace(single, Value::Unit);
                        *single = Value::List(vec![value]);
                    }
                },
            }
        }

        // Validate extended parent fields.
        for parent_ident in &entity.schema.extends {
            let parent = self.entities.must_get_by_ident(parent_ident)?;
            self.validate_entity_data(data, parent)?;
        }

        // FIXME: if entity is strict, validate that no extra fields are present

        Ok(())
    }

    fn validate_attributes(&self, mut data: DataMap) -> Result<DataMap, AnyError> {
        if let Some(ty) = data.get_type() {
            let entity = self.entities.must_get_by_ident(&ty)?;
            self.validate_entity_data(&mut data, entity)?;
        } else {
            for (key, value) in &mut data.0 {
                let attr = self.attrs.must_get_by_name(&key)?;
                self.validate_attr_value(attr, value)?;
            }
        }

        Ok(data)
    }

    pub fn validate_create(&self, create: query::mutate::Create) -> Result<Vec<DbOp>, AnyError> {
        let id = create.id.non_nil_or_randomize();

        let mut data = self.validate_attributes(create.data)?;
        data.insert(AttrId::QUALIFIED_NAME.into(), id.into());

        let mut ops = Vec::new();
        ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
            id: create.id,
            data,
        })));

        Ok(ops)
    }

    pub fn validate_replace(
        &self,
        create: query::mutate::Replace,
        _old: Option<DataMap>,
    ) -> Result<Vec<DbOp>, AnyError> {
        let id = create.id.non_nil_or_randomize();

        let mut data = self.validate_attributes(create.data)?;
        data.insert(AttrId::QUALIFIED_NAME.into(), id.into());

        let mut ops = Vec::new();
        ops.push(DbOp::Tuple(TupleOp::Replace(TupleReplace {
            id: create.id,
            data,
        })));

        // FIXME: cleanup for old data (index removal etc)

        Ok(ops)
    }

    pub fn validate_merge(
        &self,
        merge: query::mutate::Merge,
        mut old: DataMap,
    ) -> Result<Vec<DbOp>, AnyError> {
        let id = merge.id.non_nil_or_randomize();

        // FIXME: can't use extend here, have to respect list patching etc.
        old.0.extend(merge.data.0.into_iter());
        let mut data = self.validate_attributes(old)?;
        data.insert(AttrId::QUALIFIED_NAME.into(), id.into());

        let mut ops = Vec::new();
        ops.push(DbOp::Tuple(TupleOp::Merge(TupleMerge { id, data })));

        // FIXME: index updates etc

        Ok(ops)
    }

    pub fn validate_delete(
        &self,
        del: query::mutate::Delete,
        _old: DataMap,
    ) -> Result<Vec<DbOp>, AnyError> {
        let id = del.id;

        let mut ops = Vec::new();
        ops.push(DbOp::Tuple(TupleOp::Delete(TupleDelete { id })));

        // FIXME: index updates etc

        Ok(ops)
    }
}

pub type SharedRegistry = Arc<RwLock<Registry>>;
