use anyhow::{anyhow, Context, Result};

use crate::{
    backend::{self, DbOp, TupleIndexInsert, TupleIndexOp, TupleIndexRemove, TupleIndexReplace},
    data::{value::ValueMap, DataMap, Id, Ident, Value},
    error,
    query::{self, expr, migrate::Migration, select::Item},
    registry::{LocalIndexId, RegisteredIndex},
    schema, AnyError,
};

use super::{
    index::{self, MemoryIndexMap},
    memory_data::{MemoryTuple, MemoryValue},
};

/// Memory store for building a backend.
///
/// The [MemoryDb] is a simple memory-only backend, but the store can also
/// be used by other backends as a caching layer or for other purposes.
pub struct MemoryStore {
    interner: super::interner::Interner,
    registry: crate::registry::SharedRegistry,
    entities: fnv::FnvHashMap<Id, MemoryTuple>,
    idents: std::collections::HashMap<String, Id>,
    indexes: MemoryIndexMap,

    ignore_index_constraints: bool,

    revert_epoch: RevertEpoch,
    revert_ops: Option<(RevertEpoch, RevertList)>,
}

impl MemoryStore {
    pub fn new(registry: crate::registry::SharedRegistry) -> Self {
        let mut s = Self {
            interner: super::interner::Interner::new(),
            registry: registry.clone(),
            entities: fnv::FnvHashMap::default(),
            idents: std::collections::HashMap::new(),
            indexes: self::index::new_memory_index_map(),
            revert_epoch: 0,
            revert_ops: None,
            // FIXME: set to false, add setter.
            ignore_index_constraints: false,
        };

        // FIXME: this is a temporary hack to work around the fact that
        // migrations are not yet used for internal schemas.
        // Remove once everything is properly done with migrations.
        let indexes = {
            registry
                .read()
                .unwrap()
                .iter_indexes()
                .cloned()
                .collect::<Vec<_>>()
        };
        for index in indexes {
            s.index_create(&index).unwrap();
        }

        s
    }

    pub fn set_ignore_index_constraints(&mut self, ignore: bool) {
        self.ignore_index_constraints = ignore;
    }

    pub fn registry(&self) -> &crate::registry::SharedRegistry {
        &self.registry
    }

    fn resolve_ident(&self, ident: &Ident) -> Option<Id> {
        match ident {
            Ident::Id(id) => Some(*id),
            Ident::Name(name) => self.idents.get(name.as_ref()).cloned(),
        }
    }

    fn resolve_entity(&self, ident: &Ident) -> Option<&MemoryTuple> {
        let id = self.resolve_ident(ident)?;
        self.entities.get(&id)
    }

    fn must_resolve_entity(&self, ident: &Ident) -> Result<&MemoryTuple, error::EntityNotFound> {
        self.resolve_entity(ident)
            .ok_or_else(|| error::EntityNotFound::new(ident.clone()))
    }

    // fn resolve_entity_mut(&mut self, ident: &Ident) -> Option<&mut MemoryTuple> {
    //     let id = self.resolve_ident(ident)?;
    //     self.entities.get_mut(&id)
    // }

    // fn entity_ident_map(&self, ident: &Ident) -> Result<IdMap, AnyError> {
    //     let tuple = self
    //         .resolve_entity(&ident)
    //         .ok_or_else(|| anyhow!("Not found"))?;
    //     Ok(memory_to_id_map(tuple))
    // }

    fn intern_data_map(&mut self, map: DataMap) -> Result<MemoryTuple, AnyError> {
        // TODO: fix this... pass in the registry
        let reg = self.registry.clone();
        let reg = reg.read().unwrap();

        let map = map
            .0
            .into_iter()
            .map(|(key, value)| -> Result<_, AnyError> {
                let attr = reg.require_attr_by_name(&key)?;
                let value = self.interner.intern_value(value);
                Ok((attr.schema.id, value))
            })
            .collect::<Result<_, _>>()?;
        Ok(MemoryTuple(map))
    }

    fn tuple_to_data_map(&self, tuple: &MemoryTuple) -> Result<DataMap, AnyError> {
        let reg = self.registry.read().unwrap();

        let map: std::collections::BTreeMap<_, _> = tuple
            .0
            .iter()
            .map(|(id, value)| -> Result<_, AnyError> {
                let attr = reg.require_attr(*id)?;
                let value = value.into();
                Ok((attr.schema.ident.clone(), value))
            })
            .collect::<Result<_, _>>()?;

        Ok(ValueMap(map))
    }

    // fn persist_tuple(&mut self, tuple: TuplePersist) -> Result<Id, AnyError> {
    //     let ident_id = tuple
    //         .ident
    //         .as_ref()
    //         .and_then(|ident| self.idents.get(ident.as_str()))
    //         .cloned();

    //     let id = match (tuple.id, ident_id) {
    //         (None, None) => {
    //             if tuple.create == Some(false) {
    //                 bail!("Tuple update does not specify an id or ident");
    //             }
    //             Id::random()
    //         }
    //         (None, Some(id)) => id,
    //         (Some(id), None) => id,
    //         (Some(id), Some(ident_id)) => {
    //             if id == ident_id {
    //                 id
    //             } else if id.is_nil() {
    //                 ident_id
    //             } else {
    //                 bail!("Tuple persist - id mismatch between specified and and ident")
    //             }
    //         }
    //     };

    //     let mut new_tuple: MemoryTuple = tuple
    //         .data
    //         .into_iter()
    //         .map(|(key, value)| (key, self.interner.intern_value(value)))
    //         .collect();

    //     if let Some(old) = self.entities.get_mut(&id) {
    //         if tuple.create == Some(true) {
    //             bail!(
    //                 "Persisted tuple forces creation, but the id '{}' already exists",
    //                 id
    //             );
    //         }

    //         // FIXME: patch instead of plain overwrite.
    //         old.extend(new_tuple);
    //         // FIXME: handle added/changed ident.
    //     } else {
    //         new_tuple.insert(builtin::AttrId::ID, MemoryValue::Id(id));
    //         if let Some(ident) = &tuple.ident {
    //             new_tuple.insert(
    //                 builtin::AttrIdent::ID,
    //                 MemoryValue::String(self.interner.intern_str(ident.to_string())),
    //             );
    //         }
    //         self.entities.insert(id, new_tuple);

    //         if let Some(ident) = tuple.ident {
    //             self.idents.insert(ident.into_string(), id);
    //         }
    //     }

    //     Ok(id)
    // }

    // fn persist_multi(&mut self, tuples: Vec<TuplePersist>) -> Result<Vec<Id>, AnyError> {
    //     // FIXME: rollback if any thing fails!
    //     let mut ids = Vec::new();
    //     for tuple in tuples {
    //         let id = self.persist_tuple(tuple)?;
    //         ids.push(id)
    //     }
    //     Ok(ids)
    // }

    // fn apply_batch(&mut self, batch: Batch) -> Result<Vec<Id>, AnyError> {
    //     // FIXME: rollback if any thing fails!
    //     let mut ids = Vec::new();
    //     for op in batch.ops {
    //         match op {
    //             schema::Op::Assert(assert) => {
    //                 let id = self.apply_assert(assert)?;
    //                 ids.push(id);
    //             }
    //             schema::Op::Retract(_) => todo!(),
    //             schema::Op::Evict(_) => todo!(),
    //         }
    //     }

    //     Ok(ids)
    // }

    // fn apply_assert(&mut self, assert: schema::Assert) -> Result<Id, AnyError> {
    //     let current = self.resolve_entity_mut(&assert.ident);

    //     let current_value = current.map(|x| memory_to_id_map(x));

    //     let persist = self
    //         .registry
    //         .read()
    //         .unwrap()
    //         .validate_assert(assert, current_value.as_ref())?;

    //     self.persist_tuple(persist)
    // }
    //

    pub(super) fn index_create(&mut self, schema: &RegisteredIndex) -> Result<(), AnyError> {
        let index = if schema.schema.unique {
            index::Index::Unique(index::UniqueIndex::new())
        } else {
            index::Index::Multi(index::MultiIndex::new())
        };

        self.indexes.create(schema.local_id, index)?;
        Ok(())
    }

    fn index_delete(&mut self, schema: &crate::registry::RegisteredIndex) -> Result<(), AnyError> {
        // Since the index list is addressed by numeric local index, the index
        // is not actually removed, but just it's data is cleared to free up
        // memory.
        self.indexes.get_mut(schema.local_id).clear();
        Ok(())
    }

    fn tuple_index_insert(
        &mut self,
        id: Id,
        op: TupleIndexInsert,
        reverts: &mut RevertList,
    ) -> Result<(), AnyError> {
        let value = self.interner.intern_value(op.value);

        let index_id = op.index;

        match self.indexes.get_mut(op.index) {
            super::index::Index::Unique(idx) => {
                if self.ignore_index_constraints {
                    idx.insert_unchecked(value.clone(), id);
                } else {
                    idx.insert_unique(value.clone(), id).map_err(|_| {
                        let reg = self.registry.read().unwrap();
                        let index = reg
                            .index_by_local_id(index_id)
                            .expect("Invalid local index id");
                        error::UniqueConstraintViolation {
                            index: index.schema.ident.clone(),
                            entity_id: id,
                            // TODO: add attribute name!
                            attribute: "?".to_string(),
                            value: Some(value.to_value()),
                        }
                    })?;
                }
            }
            super::index::Index::Multi(idx) => {
                idx.add(value.clone(), id);
            }
        }

        reverts.push(RevertOp::IndexValueInserted {
            index: index_id,
            entity_id: id,
            value,
        });

        Ok(())
    }

    fn tuple_index_replace(
        &mut self,
        id: Id,
        op: TupleIndexReplace,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        let reg = self.registry.read().unwrap();
        let value = self.interner.intern_value(op.value);
        let old_value = self.interner.intern_value(op.old_value);

        let index_id = op.index;

        let removed = match self.indexes.get_mut(op.index) {
            super::index::Index::Unique(idx) => {
                let removed = idx.remove(&old_value);

                if self.ignore_index_constraints {
                    idx.insert_unchecked(value.clone(), id);
                } else {
                    idx.insert_unique(value.clone(), id).map_err(|_| {
                        let index = reg
                            .index_by_local_id(index_id)
                            .expect("Invalid local index id");
                        error::UniqueConstraintViolation {
                            index: index.schema.ident.clone(),
                            entity_id: id,
                            // TODO: add attribute name!
                            attribute: "?".to_string(),
                            value: Some(value.to_value()),
                        }
                    })?;
                }

                removed.is_some()
            }
            super::index::Index::Multi(idx) => {
                let removed = idx.remove(&old_value, id);
                idx.add(value.clone(), id);
                removed.is_some()
            }
        };

        revert.push(RevertOp::IndexValueInserted {
            index: index_id,
            entity_id: id,
            value,
        });
        if removed {
            revert.push(RevertOp::IndexValueRemoved {
                index: index_id,
                entity_id: id,
                value: old_value,
            });
        }

        Ok(())
    }

    fn tuple_index_remove(
        &mut self,
        id: Id,
        op: TupleIndexRemove,
        reverts: &mut RevertList,
    ) -> Result<(), AnyError> {
        let value = self.interner.intern_value(op.value);
        let index_id = op.index;

        let removed = match self.indexes.get_mut(op.index) {
            super::index::Index::Unique(idx) => idx.remove(&value).is_some(),
            super::index::Index::Multi(idx) => idx.remove(&value, id).is_some(),
        };

        if removed {
            reverts.push(RevertOp::IndexValueRemoved {
                index: index_id,
                entity_id: id,
                value,
            });
        }

        Ok(())
    }

    fn apply_tuple_index_op(
        &mut self,
        tuple_id: Id,
        op: TupleIndexOp,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        match op {
            TupleIndexOp::Insert(op) => self.tuple_index_insert(tuple_id, op, revert),
            TupleIndexOp::Replace(op) => self.tuple_index_replace(tuple_id, op, revert),
            TupleIndexOp::Remove(op) => self.tuple_index_remove(tuple_id, op, revert),
        }
    }

    fn tuple_create(
        &mut self,
        create: backend::TupleCreate,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        if self.entities.contains_key(&create.id) {
            return Err(anyhow!("Entity id already exists: '{}'", create.id));
        }

        for op in create.index_ops {
            self.tuple_index_insert(create.id, op, revert)?;
        }

        let map = self.intern_data_map(create.data)?;
        self.entities.insert(create.id, map);
        revert.push(RevertOp::TupleCreated { id: create.id });
        Ok(())
    }

    fn tuple_replace(
        &mut self,
        replace: backend::TupleReplace,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        for op in replace.index_ops {
            self.apply_tuple_index_op(replace.id, op, revert)?;
        }

        let old = self.entities.remove(&replace.id);
        let map = self.intern_data_map(replace.data)?;
        self.entities.insert(replace.id, map);
        revert.push(RevertOp::TupleReplaced {
            id: replace.id,
            data: old,
        });
        Ok(())
    }

    fn tuple_merge(
        &mut self,
        mut update: backend::TupleMerge,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        for op in std::mem::take(&mut update.index_ops) {
            self.apply_tuple_index_op(update.id, op, revert)?;
        }

        let old = self
            .entities
            .get_mut(&update.id)
            .ok_or_else(|| error::EntityNotFound::new(update.id.into()))?;

        let reg = self.registry.read().unwrap();

        let mut replaced_values = Vec::new();

        for (key, new_value) in update.data.0 {
            let attr = reg.require_attr_by_name(&key)?;

            // FIXME: this logic should not be here, but be handled by
            // Registry::validate_merge
            if let Some(old_value) = old.remove(&attr.schema.id) {
                // FIXME: this is hacky and only covers lists...
                match (old_value, new_value) {
                    (MemoryValue::List(mut old_items), Value::List(new_items)) => {
                        for item in new_items {
                            old_items.push(self.interner.intern_value(item));
                        }
                        old.0.insert(attr.schema.id, MemoryValue::List(old_items));
                    }
                    (old_value, new_value) => {
                        old.0
                            .insert(attr.schema.id, self.interner.intern_value(new_value));
                        replaced_values.push((attr.schema.id, Some(old_value)));
                    }
                }
            } else {
                old.0
                    .insert(attr.schema.id, self.interner.intern_value(new_value));
            };
        }

        if !replaced_values.is_empty() {
            // FIXME: this doesn't UNDO properly for new values.
            revert.push(RevertOp::TupleMerged {
                id: update.id,
                replaced_data: replaced_values,
            });
        }

        Ok(())
    }

    fn tuple_remove_attrs(
        &mut self,
        mut rem: backend::TupleRemoveAttrs,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        for op in std::mem::take(&mut rem.index_ops) {
            self.tuple_index_remove(rem.id, op, revert)?;
        }

        let old = self
            .entities
            .get_mut(&rem.id)
            .ok_or_else(|| error::EntityNotFound::new(rem.id.into()))?;

        let reg = self.registry.read().unwrap();
        let mut removed = Vec::new();
        for attr_id in rem.attrs {
            let attr = reg.require_attr(attr_id)?;
            if let Some(value) = old.0.remove(&attr.schema.id) {
                removed.push((attr_id, value));
            }
        }

        if !removed.is_empty() {
            revert.push(RevertOp::TupleAttrsRemoved {
                id: rem.id,
                attrs: removed,
            });
        }

        Ok(())
    }

    fn tuple_delete(
        &mut self,
        del: backend::TupleDelete,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        for op in del.index_ops {
            self.tuple_index_remove(del.id, op, revert)?;
        }

        match self.entities.remove(&del.id) {
            Some(data) => {
                revert.push(RevertOp::TupleDeleted { id: del.id, data });
                Ok(())
            }
            None => Err(error::EntityNotFound::new(del.id.into()).into()),
        }
    }

    fn tuple_select_remove(
        &mut self,
        selector: &query::expr::Expr,
        rem: &backend::TupleRemoveAttrs,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        let reg = self.registry.clone();

        // TODO: use query logic instead of full table scan for speedup.
        // TODO: figure out how to do this in one step. Two step process
        // right now due to borrow checker errors.

        let mut to_remove = Vec::new();

        for (entity_id, entity) in self.entities.iter() {
            if self.entity_filter(entity, selector, &*reg.read().unwrap()) {
                let mut removed_attr_ids = Vec::new();
                for attr_id in &rem.attrs {
                    if entity.contains_key(&attr_id) {
                        removed_attr_ids.push(attr_id);
                    }
                }

                if !removed_attr_ids.is_empty() {
                    to_remove.push((*entity_id, removed_attr_ids));
                }
            }
        }

        for (entity_id, removed_attr_ids) in to_remove {
            let entity = self.entities.get_mut(&entity_id).unwrap();

            let attrs = removed_attr_ids
                .into_iter()
                .filter_map(|attr_id| Some((*attr_id, entity.remove(&attr_id)?)))
                .collect();

            revert.push(RevertOp::TupleAttrsRemoved {
                id: entity_id,
                attrs,
            });
        }

        Ok(())
    }

    /// Apply database operations.
    /// [RevertOp]s are collected into the provided revert list, which allows
    /// undoing operations.
    fn apply_db_ops(&mut self, ops: Vec<DbOp>, revert: &mut RevertList) -> Result<(), AnyError> {
        use crate::backend::TupleOp;

        // FIXME: implement validate_ checks via registry for all operations.
        // FIXME: guard against schema changes outside of a migration.
        for op in ops {
            match op {
                DbOp::Tuple(tuple) => match tuple {
                    TupleOp::Create(create) => {
                        self.tuple_create(create, revert)?;
                    }
                    TupleOp::Replace(repl) => {
                        self.tuple_replace(repl, revert)?;
                    }
                    TupleOp::Merge(update) => {
                        self.tuple_merge(update, revert)?;
                    }
                    TupleOp::RemoveAttrs(remove) => {
                        self.tuple_remove_attrs(remove, revert)?;
                    }
                    TupleOp::Delete(del) => {
                        self.tuple_delete(del, revert)?;
                    }
                },
                DbOp::Select(sel) => match sel.op {
                    TupleOp::Create(_) => todo!(),
                    TupleOp::Replace(_) => todo!(),
                    TupleOp::Merge(_) => todo!(),
                    TupleOp::RemoveAttrs(remove) => {
                        self.tuple_select_remove(&sel.selector, &remove, revert)?;
                    }
                    TupleOp::Delete(_) => todo!(),
                },
            }
        }

        Ok(())
    }

    fn apply_create(
        &mut self,
        create: query::mutate::Create,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        let ops = self.registry.read().unwrap().validate_create(create)?;
        self.apply_db_ops(ops, revert)?;
        Ok(())
    }

    fn apply_replace(
        &mut self,
        repl: query::mutate::Replace,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        let old = match self.entities.get(&repl.id) {
            Some(tuple) => Some(self.tuple_to_data_map(&tuple)?),
            None => None,
        };

        let ops = self.registry.read().unwrap().validate_replace(repl, old)?;
        self.apply_db_ops(ops, revert)?;
        Ok(())
    }

    fn apply_merge(
        &mut self,
        merge: query::mutate::Merge,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        if let Some(old_tuple) = self.entities.get(&merge.id) {
            let old = self.tuple_to_data_map(old_tuple)?;
            let ops = self.registry.read().unwrap().validate_merge(merge, old)?;
            self.apply_db_ops(ops, revert)
        } else {
            let create = query::mutate::Create {
                id: merge.id,
                data: merge.data,
            };
            self.apply_create(create, revert)
        }
    }

    fn apply_delete(
        &mut self,
        delete: query::mutate::Delete,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        let old = self
            .entities
            .get(&delete.id)
            .ok_or_else(|| anyhow!("Entity not found: {:?}", delete.id))
            .and_then(|t| self.tuple_to_data_map(t))?;

        let ops = self.registry.read().unwrap().validate_delete(delete, old)?;
        self.apply_db_ops(ops, revert)?;
        Ok(())
    }

    /// Apply a batch of operations.
    fn apply_batch_impl(
        &mut self,
        batch: query::mutate::BatchUpdate,
    ) -> Result<RevertList, AnyError> {
        // FIXME: rollback when errors happen.

        let mut revert = Vec::new();

        for action in batch.actions {
            let res = match action {
                query::mutate::Mutate::Create(create) => self.apply_create(create, &mut revert),
                query::mutate::Mutate::Replace(repl) => self.apply_replace(repl, &mut revert),
                query::mutate::Mutate::Merge(merge) => self.apply_merge(merge, &mut revert),
                query::mutate::Mutate::Delete(del) => self.apply_delete(del, &mut revert),
            };

            if let Err(err) = res {
                // An error happened, so revert changes before returning.
                self.apply_revert(revert);
                return Err(err);
            }
        }

        Ok(revert)
    }

    pub fn apply_batch(
        &mut self,
        batch: crate::query::mutate::BatchUpdate,
    ) -> Result<(), AnyError> {
        self.apply_batch_impl(batch)?;
        Ok(())
    }

    fn persist_revert_epoch(&mut self, revert: RevertList) -> RevertEpoch {
        self.revert_epoch = self.revert_epoch.wrapping_add(1);
        let epoch = self.revert_epoch;
        self.revert_ops = Some((epoch, revert));
        epoch
    }

    /// Apply a batch update and internally retain a revert list that allows
    /// undoing the change.
    /// The returned [RevertEpoch] can be passed to [Self::revert_changes] to
    /// apply the revert.
    pub fn apply_batch_revertable(
        &mut self,
        batch: crate::query::mutate::BatchUpdate,
    ) -> Result<RevertEpoch, AnyError> {
        let ops = self.apply_batch_impl(batch)?;
        let epoch = self.persist_revert_epoch(ops);
        Ok(epoch)
    }

    /// Revert a list of changes.
    fn apply_revert(&mut self, revert: RevertList) {
        // NOTE: MUST revert in reverse order to preserve consistency.
        for op in revert.into_iter().rev() {
            match op {
                RevertOp::TupleCreated { id } => {
                    self.entities.remove(&id);
                }
                RevertOp::TupleReplaced { id, data } => {
                    if let Some(old) = data {
                        self.entities.insert(id, old);
                    } else {
                        self.entities.remove(&id);
                    }
                }
                RevertOp::TupleMerged { id, replaced_data } => {
                    let data = self.entities.get_mut(&id).expect(
                        "Consistency error: can't revert change because tuple was not found",
                    );

                    for (attr_id, value_opt) in replaced_data {
                        if let Some(value) = value_opt {
                            data.insert(attr_id, value);
                        } else {
                            data.remove(&id);
                        }
                    }
                }
                RevertOp::TupleAttrsRemoved { id, attrs } => {
                    let data = self.entities.get_mut(&id).expect(
                        "Consistency error: can't revert change because tuple was not found",
                    );
                    for (attr_id, value) in attrs {
                        data.insert(attr_id, value);
                    }
                }
                RevertOp::TupleDeleted { id, data } => {
                    self.entities.insert(id, data);
                }
                RevertOp::IndexValueInserted {
                    index,
                    entity_id,
                    value,
                } => {
                    match self.indexes.get_mut(index) {
                        super::index::Index::Unique(idx) => idx.remove(&value),
                        super::index::Index::Multi(idx) => idx.remove(&value, entity_id),
                    };
                }
                RevertOp::IndexValueRemoved {
                    index,
                    entity_id,
                    value,
                } => match self.indexes.get_mut(index) {
                    super::index::Index::Unique(idx) => idx
                        .insert_unique(value, entity_id)
                        .map_err(|_| ())
                        .expect("Consistentcy error"),
                    super::index::Index::Multi(idx) => idx.add(value, entity_id),
                },
            }
        }
    }

    /// Revert the last change to the database.
    /// Fails if the given [RevertEpoch] does not match the last change.
    pub fn revert_changes(&mut self, epoch: RevertEpoch) -> Result<(), AnyError> {
        match self.revert_ops.take() {
            None => Err(anyhow!(
                "Invalid revert epoch - epoch does not match last change"
            )),
            Some((current_epoch, ops)) => {
                if current_epoch != epoch {
                    Err(anyhow!(
                        "Invalid revert epoch - epoch does not match last change"
                    ))
                } else {
                    self.apply_revert(ops);
                    Ok(())
                }
            }
        }
    }

    fn migrate_impl(&mut self, mig: Migration, is_internal: bool) -> Result<RevertList, AnyError> {
        let mut reg = self.registry.read().unwrap().clone();
        let (mig, ops) = schema::logic::build_migration(&mut reg, mig, is_internal)?;

        for action in mig.actions {
            match action {
                query::migrate::SchemaAction::IndexCreate(create) => {
                    let index = reg.require_index_by_id(create.schema.id).context(format!(
                        "Registry does not contain index '{}'",
                        create.schema.ident
                    ))?;
                    self.index_create(index)?;
                }
                query::migrate::SchemaAction::IndexDelete(del) => {
                    let index = reg
                        .require_index_by_name(&del.name)
                        .context(format!("Registry does not contain index '{}'", del.name,))?;
                    self.index_delete(index)?;
                }
                query::migrate::SchemaAction::AttributeCreate(_) => {}
                query::migrate::SchemaAction::AttributeUpsert(_) => {}
                query::migrate::SchemaAction::AttributeDelete(_) => {}
                query::migrate::SchemaAction::EntityCreate(_) => {}
                query::migrate::SchemaAction::EntityUpsert(_) => {}
                query::migrate::SchemaAction::EntityDelete(_) => {}
            }
        }

        let mut revert = Vec::new();
        if let Err(err) = self.apply_db_ops(ops.clone(), &mut revert) {
            self.apply_revert(revert);
            Err(err)
        } else {
            *self.registry.write().unwrap() = reg;
            Ok(revert)
        }
    }

    pub fn migrate(&mut self, mig: Migration) -> Result<(), AnyError> {
        self.migrate_impl(mig, false)?;
        Ok(())
    }

    pub fn migrate_revertable(&mut self, mig: Migration) -> Result<RevertEpoch, AnyError> {
        let ops = self.migrate_impl(mig, false)?;
        let epoch = self.persist_revert_epoch(ops);
        Ok(epoch)
    }

    pub fn entity(&self, id: Ident) -> Result<DataMap, AnyError> {
        self.must_resolve_entity(&id)
            .map_err(AnyError::from)
            .and_then(|tuple| self.tuple_to_data_map(tuple))
    }

    pub fn select(
        &self,
        query: query::select::Select,
    ) -> Result<query::select::Page<Item>, AnyError> {
        // TODO: query validation and planning

        let reg = self.registry.read().unwrap();

        let items: Vec<(&Id, &MemoryTuple)> = if let Some(filter) = query.filter {
            // Fast path for single ident select.

            if let Some(Ident::Id(id)) = expr::expr_is_entity_ident(&filter) {
                // Fast path for single ID select.
                // This is messy and should be handled by an external query
                // planner, but it helps for now.

                if let Some(tuple) = self.entities.get(&id) {
                    let id = tuple
                        .0
                        .get(&schema::builtin::ATTR_ID)
                        .unwrap()
                        .as_id_ref()
                        .unwrap();
                    vec![(id, tuple)]
                } else {
                    vec![]
                }
            } else {
                self.entities
                    .iter()
                    .filter(|(_id, item)| {
                        // Skip builtin types.

                        if let Some(id) = item
                            .get(&crate::schema::builtin::ATTR_TYPE)
                            .and_then(|x| x.as_id())
                        {
                            if crate::schema::builtin::id_is_builtin_entity_type(id) {
                                return false;
                            }
                        }

                        let flag = self.entity_filter(item, &filter, &*reg);
                        flag
                    })
                    .collect()
            }
        } else {
            self.entities
                .iter()
                .filter(|(_id, item)| {
                    if let Some(id) = item
                        .get(&crate::schema::builtin::ATTR_TYPE)
                        .and_then(|x| x.as_id())
                    {
                        if crate::schema::builtin::id_is_builtin_entity_type(id) {
                            return false;
                        }
                    }
                    true
                })
                .collect()
        };

        if !query.sort.is_empty() {
            todo!()
        }

        let items = if let Some(cursor) = query.cursor {
            items
                .into_iter()
                .skip_while(|(id, _)| **id != cursor)
                .take(query.limit as usize)
                .map(|(_id, data)| {
                    Ok(Item {
                        data: self.tuple_to_data_map(data)?,
                        joins: Vec::new(),
                    })
                })
                .collect::<Result<_, AnyError>>()?
        } else {
            items
                .into_iter()
                .take(query.limit as usize)
                .map(|(_id, data)| {
                    Ok(Item {
                        data: self.tuple_to_data_map(data)?,
                        joins: Vec::new(),
                    })
                })
                .collect::<Result<_, AnyError>>()?
        };

        Ok(query::select::Page {
            items,
            next_cursor: None,
        })
    }

    fn eval_expr<'a>(
        &self,
        entity: &MemoryTuple,
        expr: &'a query::expr::Expr,
        reg: &crate::registry::Registry,
    ) -> std::borrow::Cow<'a, Value> {
        use std::borrow::Cow;

        let out = match expr {
            query::expr::Expr::Literal(v) => Cow::Borrowed(v),
            query::expr::Expr::Attr(ident) => match ident {
                Ident::Id(id) => entity
                    .get(id)
                    .map(|x| Cow::Owned(x.to_value()))
                    .unwrap_or(cowal_unit()),
                Ident::Name(name) => reg
                    .attr_by_name(name)
                    .and_then(|attr| {
                        entity
                            .get(&attr.schema.id)
                            .map(|x| Cow::Owned(x.to_value()))
                    })
                    .unwrap_or(cowal_unit()),
            },
            query::expr::Expr::Ident(ident) => match ident {
                Ident::Id(id) => Cow::Owned(Value::Id(*id)),
                Ident::Name(name) => {
                    if let Some(id) = self.idents.get(name.as_ref()) {
                        Cow::Owned(id.clone().into())
                    } else {
                        cowal_unit()
                    }
                }
            },
            query::expr::Expr::Variable(_) => todo!(),
            query::expr::Expr::UnaryOp { op, expr } => {
                let value = self.eval_expr(entity, expr, reg);
                match op {
                    query::expr::UnaryOp::Not => {
                        let flag = value.as_bool().unwrap_or(false);
                        Cow::Owned(Value::Bool(!flag))
                    }
                }
            }
            query::expr::Expr::BinaryOp { left, op, right } => match op {
                query::expr::BinaryOp::And => {
                    let left_flag = self.eval_expr(entity, left, reg).as_bool().unwrap_or(false);
                    let flag = if left_flag {
                        self.eval_expr(entity, right, reg)
                            .as_bool()
                            .unwrap_or(false)
                    } else {
                        false
                    };
                    Cow::Owned(Value::Bool(flag))
                }
                query::expr::BinaryOp::Or => {
                    let left_flag = self.eval_expr(entity, left, reg).as_bool().unwrap_or(false);
                    let flag = if left_flag {
                        true
                    } else {
                        self.eval_expr(entity, right, reg)
                            .as_bool()
                            .unwrap_or(false)
                    };
                    Cow::Owned(Value::Bool(flag))
                }
                other => {
                    let left = self.eval_expr(entity, left, reg);
                    let right = self.eval_expr(entity, right, reg);

                    let flag = match other {
                        query::expr::BinaryOp::Eq => {
                            tracing::trace!(?left, ?right, "BinaryOp::Eq");
                            left == right
                        }
                        query::expr::BinaryOp::Neq => left != right,
                        query::expr::BinaryOp::Gt => left > right,
                        query::expr::BinaryOp::Gte => left >= right,
                        query::expr::BinaryOp::Lt => left < right,
                        query::expr::BinaryOp::Lte => left <= right,
                        query::expr::BinaryOp::Contains => match (left.as_ref(), right.as_ref()) {
                            (Value::String(value), Value::String(pattern)) => {
                                value.contains(pattern)
                            }
                            (Value::List(left), Value::List(right)) => {
                                left.iter().any(|item| right.contains(item))
                            }
                            (_left, _right) => {
                                // TODO: this should be rejected by query
                                // validation.
                                false
                            }
                        },
                        query::expr::BinaryOp::In => {
                            tracing::trace!(?left, ?right, "comparing BinaryOp::In");
                            // TODO: probably need to cover more variants here!
                            match (left.as_ref(), right.as_ref()) {
                                (value, Value::List(items)) => items.iter().any(|x| x == value),
                                _other => false,
                            }
                        }

                        // Covered above.
                        query::expr::BinaryOp::And | query::expr::BinaryOp::Or => {
                            unreachable!()
                        }
                    };
                    Cow::Owned(Value::Bool(flag))
                }
            },
            query::expr::Expr::If { value, then, or } => {
                let flag = self
                    .eval_expr(entity, &*value, reg)
                    .as_bool()
                    .unwrap_or(false);
                if flag {
                    self.eval_expr(entity, &*then, reg)
                } else {
                    self.eval_expr(entity, &*or, reg)
                }
            }
        };
        out
    }

    fn entity_filter(
        &self,
        entity: &MemoryTuple,
        expr: &query::expr::Expr,
        reg: &crate::registry::Registry,
    ) -> bool {
        self.eval_expr(entity, expr, reg).as_bool().unwrap_or(false)
    }

    pub fn purge_all_data(&mut self) {
        /*
        self.entities.retain(|id, entity| {
            let flag = entity
                .0
                .get(&builtin::ATTR_TYPE)
                .and_then(|ty| ty.as_id())
                .map(|id| builtin::entity_type_is_builtin(id))
                .unwrap_or(false);
            flag
        });
        */
        self.entities.clear();
        self.idents.clear();
        self.interner.clear();
        self.indexes = index::new_memory_index_map();
        self.registry.write().unwrap().reset();

        let indexes = {
            self.registry
                .clone()
                .read()
                .unwrap()
                .iter_indexes()
                .map(|x| x.clone())
                .collect::<Vec<_>>()
        };
        for index in indexes {
            self.index_create(&index).unwrap();
        }
    }
}

#[inline]
const fn cowal_unit<'a>() -> std::borrow::Cow<'a, Value> {
    std::borrow::Cow::Owned(Value::Unit)
}

/// An identifier for the current version of a database.
/// Some methods return this epoch to provide extern reverts.
/// The epoch can be passed to [MemoryStore::revert_epoch].
pub type RevertEpoch = u64;

/// Record of a reversible operation performed on data.
/// A `RevertOp` can be reverted to restore the old state.
#[derive(Debug)]
enum RevertOp {
    TupleCreated {
        id: Id,
    },
    TupleReplaced {
        id: Id,
        data: Option<MemoryTuple>,
    },
    TupleMerged {
        id: Id,
        replaced_data: Vec<(Id, Option<super::memory_data::MemoryValue>)>,
    },
    TupleAttrsRemoved {
        id: Id,
        attrs: Vec<(Id, super::memory_data::MemoryValue)>,
    },
    TupleDeleted {
        id: Id,
        data: MemoryTuple,
    },
    IndexValueInserted {
        index: LocalIndexId,
        entity_id: Id,
        value: MemoryValue,
    },
    IndexValueRemoved {
        index: LocalIndexId,
        entity_id: Id,
        value: MemoryValue,
    },
}

type RevertList = Vec<RevertOp>;
