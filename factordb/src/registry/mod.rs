mod attribute_registry;
mod entity_registry;
mod index_registry;

use fnv::FnvHashSet;
pub use index_registry::IndexMap;

use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Context};

use crate::{
    backend::{
        DbOp, TupleCreate, TupleDelete, TupleIndexInsert, TupleIndexOp, TupleIndexRemove,
        TupleIndexReplace, TupleMerge, TupleOp, TupleReplace,
    },
    data::{DataMap, Id, IdMap, Ident, Value, ValueType},
    error::{self, EntityNotFound},
    query,
    schema::{self, builtin::AttrId, AttrMapExt, AttributeDescriptor, Cardinality, DbSchema},
    AnyError,
};

pub use self::{
    attribute_registry::{LocalAttributeId, RegisteredAttribute},
    entity_registry::{LocalEntityId, RegisteredEntity},
    index_registry::{LocalIndexId, RegisteredIndex},
};

const MAX_NAME_LEN: usize = 50;

pub const ATTR_ID_LOCAL: LocalAttributeId = LocalAttributeId::from_u32(1);
pub const ATTR_TYPE_LOCAL: LocalAttributeId = LocalAttributeId::from_u32(5);
pub const INDEX_IDENT_LOCAL: LocalIndexId = LocalIndexId::from_u32(2);

#[derive(Clone, Debug)]
pub struct Registry {
    entities: entity_registry::EntityRegistry,
    attrs: attribute_registry::AttributeRegistry,
    indexes: index_registry::IndexRegistry,
}

impl Registry {
    pub fn new() -> Self {
        let mut s = Self {
            attrs: attribute_registry::AttributeRegistry::new(),
            entities: entity_registry::EntityRegistry::new(),
            indexes: index_registry::IndexRegistry::new(),
        };
        s.add_builtins();
        s
    }

    pub fn build_schema(&self) -> DbSchema {
        DbSchema {
            attributes: self
                .attrs
                .items
                .iter()
                // Skip sentinel
                .skip(1)
                .map(|item| item.schema.clone())
                .collect(),
            entities: self
                .entities
                .items
                .iter()
                // Skip sentinel
                .skip(1)
                .map(|item| item.schema.clone())
                .collect(),
            indexes: self
                .indexes
                .items
                .iter()
                // Skip sentinel
                .skip(1)
                .map(|item| item.schema.clone())
                .collect(),
        }
    }

    /// Reset all state.
    /// Removes all registered entities and attributes, but restores the
    /// builtins.
    pub fn reset(&mut self) {
        self.attrs.reset();
        self.entities.reset();
        self.indexes.reset();

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
    pub fn entity_by_id(&self, id: Id) -> Option<&RegisteredEntity> {
        self.entities.get_by_uid(id)
    }

    #[inline]
    pub fn entity_by_ident(&self, ident: &Ident) -> Option<&RegisteredEntity> {
        self.entities.get_by_ident(ident)
    }

    #[inline]
    pub fn entity_by_name(&self, name: &str) -> Option<&RegisteredEntity> {
        self.entities.get_by_name(name)
    }

    #[inline]
    pub fn entity_by_name_mut(&self, name: &str) -> Option<&RegisteredEntity> {
        self.entities.get_by_name(name)
    }

    #[inline]
    pub fn require_entity_by_name(&self, name: &str) -> Result<&RegisteredEntity, EntityNotFound> {
        self.entities.must_get_by_name(name)
    }

    #[inline]
    pub fn require_entity_by_name_mut(
        &mut self,
        name: &str,
    ) -> Result<&mut RegisteredEntity, EntityNotFound> {
        let id = self.require_entity_by_name(name)?.local_id;
        Ok(self.entities.get_mut(id).unwrap())
    }

    pub fn entity_child_ids(&self, id: LocalEntityId) -> &FnvHashSet<Id> {
        &self.entities.get(id).unwrap().nested_children
    }

    pub fn iter_entities(&self) -> impl Iterator<Item = &RegisteredEntity> {
        self.entities.items.iter().skip(1)
    }

    #[inline]
    pub fn require_attr_by_name(
        &self,
        name: &str,
    ) -> Result<&RegisteredAttribute, error::AttributeNotFound> {
        self.attrs.must_get_by_name(name)
    }

    #[inline]
    pub fn require_attr_by_ident(
        &self,
        ident: &Ident,
    ) -> Result<&RegisteredAttribute, error::AttributeNotFound> {
        self.attrs.must_get_by_ident(ident)
    }

    pub fn index_by_local_id(&self, id: LocalIndexId) -> Option<&RegisteredIndex> {
        self.indexes.get(id)
    }

    pub fn index_by_id(&self, id: Id) -> Option<&RegisteredIndex> {
        self.indexes.get_by_uid(id)
    }

    pub fn index_by_name(&self, name: &str) -> Option<&RegisteredIndex> {
        self.indexes.get_by_name(name)
    }

    pub fn require_index_by_id(&self, id: Id) -> Result<&RegisteredIndex, error::IndexNotFound> {
        self.indexes.must_get_by_uid(id)
    }

    pub fn require_index_by_name(
        &self,
        name: &str,
    ) -> Result<&RegisteredIndex, error::IndexNotFound> {
        self.indexes.must_get_by_name(name)
    }

    pub fn iter_indexes(&self) -> impl Iterator<Item = &RegisteredIndex> {
        self.indexes.iter()
    }

    pub fn indexes_for_attribute(
        &self,
        attribute_id: Id,
    ) -> Result<Vec<&RegisteredIndex>, error::AttributeNotFound> {
        let attr = self.attrs.must_get_by_uid(attribute_id)?;
        Ok(self.indexes.attribute_indexes(attr.local_id))
    }

    fn add_builtins(&mut self) {
        let schema = schema::builtin::builtin_db_schema();
        for attr in schema.attributes {
            let local_id = self
                .register_attribute(attr.clone())
                .expect("Internal error: could not register builtin attribute");

            if attr.id == schema::builtin::ATTR_ID {
                assert_eq!(local_id, ATTR_ID_LOCAL);
            }
            if attr.id == schema::builtin::ATTR_TYPE {
                assert_eq!(local_id, ATTR_TYPE_LOCAL);
            }
        }
        for entity in schema.entities {
            self.register_entity(entity.clone(), true).expect(&format!(
                "Internal error: could not register builtin entity {}",
                entity.ident
            ));
        }
        for index in schema.indexes {
            let local_id = self
                .register_index(index.clone())
                .expect("Internal error: could not register builtin index");
            if index.id == schema::builtin::INDEX_IDENT {
                assert_eq!(local_id, INDEX_IDENT_LOCAL);
            }
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

    pub fn remove_attribute(&mut self, id: Id) -> Result<(), AnyError> {
        // FIXME: validate that attribute is not used by any entity or index.
        self.attrs.remove(id)?;
        Ok(())
    }

    pub fn register_entity(
        &mut self,
        entity: schema::EntitySchema,
        validate: bool,
    ) -> Result<LocalEntityId, AnyError> {
        self.entities.register(entity, validate, &self.attrs)
    }

    pub fn entity_update(
        &mut self,
        entity: schema::EntitySchema,
        validate: bool,
    ) -> Result<(), AnyError> {
        self.entities.update(entity, validate, &self.attrs)?;
        Ok(())
    }

    pub fn register_index(&mut self, index: schema::IndexSchema) -> Result<LocalIndexId, AnyError> {
        self.indexes.register(index, &self.attrs)
    }

    pub fn remove_index(&mut self, id: Id) -> Result<(), AnyError> {
        self.indexes.remove(id)?;
        Ok(())
    }

    /// Valdiate that a value conforms to a value type.
    /// Coerces values into the desired type where appropriate.
    ///
    /// The name is provided for better errors.
    fn validate_coerce_value_named(
        name: &str,
        ty: &ValueType,
        value: &mut Value,
    ) -> Result<(), AnyError> {
        value
            .coerce_mut(ty)
            .context(format!("Invalid attribute '{}'", name))
    }

    fn validate_attr_value(
        &self,
        attr: &RegisteredAttribute,
        value: &mut Value,
    ) -> Result<(), AnyError> {
        Self::validate_coerce_value_named(&attr.schema.ident, &attr.schema.value_type, value)
            .context(format!("Invalid value for attribute {}", attr.schema.ident))
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
                // Handle optional fields that have a Unit value.
                (Some(Value::Unit), Cardinality::Optional) => {
                    // Remove the unit value.
                    data.remove(&attr.schema.ident);
                }
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

    /// Build the index operations for a entity persist.
    fn build_index_ops_create(&self, attrs: &DataMap) -> Result<Vec<TupleIndexInsert>, AnyError> {
        let mut ops = Vec::new();

        for (attr_name, value) in attrs.iter() {
            let attr = self.require_attr_by_name(attr_name)?;
            for index in self.indexes.attribute_indexes(attr.local_id) {
                if index.schema.attributes.len() > 1 {
                    return Err(anyhow!("Multi-attribute indexes are not implemented yet!"));
                }

                ops.push(TupleIndexInsert {
                    index: index.local_id,
                    value: value.clone(),
                    unique: index.schema.unique,
                });
            }
        }

        Ok(ops)
    }

    /// Build the index operations for a entity persist.
    fn build_index_ops_update(
        &self,
        attrs: &DataMap,
        old: &DataMap,
    ) -> Result<Vec<TupleIndexOp>, AnyError> {
        let mut ops = Vec::new();

        let mut covered_attrs = fnv::FnvHashSet::<LocalAttributeId>::default();

        for (attr_name, value) in attrs.iter() {
            let attr = self.attr_by_name(attr_name).unwrap();
            covered_attrs.insert(attr.local_id);

            for index in self.indexes.attribute_indexes(attr.local_id) {
                if index.schema.attributes.len() > 1 {
                    // FIXME: implement multi-attribute indexes.
                    return Err(anyhow!("Multi-attribute indexes are not implemented yet!"));
                }

                if let Some(old) = old.get(attr_name) {
                    if old != value {
                        ops.push(TupleIndexOp::Replace(TupleIndexReplace {
                            index: index.local_id,
                            value: value.clone(),
                            old_value: old.clone(),
                            unique: index.schema.unique,
                        }));
                    }
                } else {
                    ops.push(TupleIndexOp::Insert(TupleIndexInsert {
                        index: index.local_id,
                        value: value.clone(),
                        unique: index.schema.unique,
                    }));
                }
            }
        }

        for (attr_name, value) in old.iter() {
            let attr = self.attr_by_name(&attr_name).unwrap();
            if covered_attrs.contains(&attr.local_id) {
                continue;
            }

            for index in self.indexes.attribute_indexes(attr.local_id) {
                if index.schema.attributes.len() > 1 {
                    // FIXME: implement multi-attribute indexes.
                    return Err(anyhow!("Multi-attribute indexes are not implemented yet!"));
                }
                ops.push(TupleIndexOp::Remove(TupleIndexRemove {
                    index: index.local_id,
                    value: value.clone(),
                }));
            }
        }

        Ok(ops)
    }

    /// Build the index operations for an entity deletion.
    fn build_index_ops_delete(&self, attrs: &DataMap) -> Result<Vec<TupleIndexRemove>, AnyError> {
        let mut ops = Vec::new();

        for (attr_name, value) in attrs.iter() {
            let attr = self.attr_by_name(attr_name).unwrap();
            for index in self.indexes.attribute_indexes(attr.local_id) {
                if index.schema.attributes.len() > 1 {
                    return Err(anyhow!("Multi-attribute indexes are not implemented yet!"));
                }
                ops.push(TupleIndexRemove {
                    index: index.local_id,
                    value: value.clone(),
                });
            }
        }

        Ok(ops)
    }

    pub fn validate_create(&self, create: query::mutate::Create) -> Result<Vec<DbOp>, AnyError> {
        let id = create.id.non_nil_or_randomize();

        let mut data = self.validate_attributes(create.data)?;
        data.insert(AttrId::QUALIFIED_NAME.into(), id.into());

        let mut ops = Vec::new();
        let index_ops = self.build_index_ops_create(&data)?;
        ops.push(DbOp::Tuple(TupleOp::Create(TupleCreate {
            id: create.id,
            data,
            index_ops,
        })));

        Ok(ops)
    }

    pub fn validate_replace(
        &self,
        replace: query::mutate::Replace,
        old_opt: Option<DataMap>,
    ) -> Result<Vec<DbOp>, AnyError> {
        let old = if let Some(old) = old_opt {
            old
        } else {
            return self.validate_create(query::mutate::Create {
                id: replace.id,
                data: replace.data,
            });
        };

        let id = replace.id.non_nil_or_randomize();

        let mut data = self.validate_attributes(replace.data)?;
        data.insert(AttrId::QUALIFIED_NAME.into(), id.into());

        let index_ops = self.build_index_ops_update(&data, &old)?;

        let mut ops = Vec::new();
        ops.push(DbOp::Tuple(TupleOp::Replace(TupleReplace {
            id: replace.id,
            data,
            index_ops,
        })));

        // FIXME: cleanup for old data (index removal etc)

        Ok(ops)
    }

    pub fn validate_patch(
        &self,
        epatch: query::mutate::EntityPatch,
        current_entity: DataMap,
    ) -> Result<Vec<DbOp>, AnyError> {
        debug_assert_eq!(Some(epatch.id), current_entity.get_id());

        let new_entity = epatch.patch.apply_map(current_entity.clone())?;
        let data = self.validate_attributes(new_entity)?;

        let index_ops = self.build_index_ops_update(&data, &current_entity)?;

        let mut ops = Vec::new();
        ops.push(DbOp::Tuple(TupleOp::Replace(TupleReplace {
            id: epatch.id,
            data,
            index_ops,
        })));

        // FIXME: cleanup for old data (index removal etc)

        Ok(ops)
    }

    pub fn validate_merge(
        &self,
        merge: query::mutate::Merge,
        old: DataMap,
    ) -> Result<Vec<DbOp>, AnyError> {
        let id = merge.id.non_nil_or_randomize();

        // TODO: Avoid clone
        // The old data is cloned below to allow for build_index_ops below.
        // There is a more performant way to do this...
        let mut values = old.clone();
        // FIXME: can't use extend here, have to respect list patching etc.
        values.0.extend(merge.data.0.into_iter());
        let mut data = self.validate_attributes(values)?;
        data.insert(AttrId::QUALIFIED_NAME.into(), id.into());

        let mut ops = Vec::new();
        let index_ops = self.build_index_ops_update(&data, &old)?;
        ops.push(DbOp::Tuple(TupleOp::Merge(TupleMerge {
            id,
            data,
            index_ops,
        })));

        // FIXME: index updates etc

        Ok(ops)
    }

    pub fn validate_delete(
        &self,
        del: query::mutate::Delete,
        old: DataMap,
    ) -> Result<Vec<DbOp>, AnyError> {
        let id = del.id;

        let mut ops = Vec::new();
        let index_ops = self.build_index_ops_delete(&old)?;
        ops.push(DbOp::Tuple(TupleOp::Delete(TupleDelete { id, index_ops })));

        // FIXME: index updates etc

        Ok(ops)
    }
}

pub type SharedRegistry = Arc<RwLock<Registry>>;
