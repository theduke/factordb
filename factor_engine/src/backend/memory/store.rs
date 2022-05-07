use std::borrow::Cow;

use anyhow::{anyhow, bail, Context, Result};

use factordb::{
    data::{patch::Patch, DataMap, Id, IdOrIdent, Value, ValueMap},
    error::{self, EntityNotFound},
    prelude::{Batch, Select},
    query::{
        self,
        expr::Expr,
        migrate::Migration,
        mutate::EntityPatch,
        select::{Item, Order, Page},
    },
    AnyError,
};

use crate::{
    backend::{
        self,
        query_planner::{self, QueryPlan, ResolvedExpr, Sort},
        DbOp, TupleIndexInsert, TupleIndexOp, TupleIndexRemove, TupleIndexReplace,
    },
    registry::{self, LocalAttributeId, LocalIndexId, RegisteredIndex, Registry},
};

use super::{
    index::{self, MemoryIndexMap},
    memory_data::{self, MemoryExpr, MemoryTuple, MemoryValue, SharedStr},
};

/// Memory store for building a backend.
///
/// The [MemoryDb] is a simple memory-only backend, but the store can also
/// be used by other backends as a caching layer or for other purposes.
pub struct MemoryStore {
    interner: super::interner::Interner,
    registry: crate::registry::SharedRegistry,
    entities: fnv::FnvHashMap<Id, MemoryTuple>,
    indexes: MemoryIndexMap,

    ignore_index_constraints: bool,

    revert_epoch: RevertEpoch,
    revert_ops: Option<(RevertEpoch, RevertList)>,
}

type TupleIter<'a> = Box<dyn Iterator<Item = Cow<'a, MemoryTuple>> + 'a>;

impl MemoryStore {
    pub fn new(registry: crate::registry::SharedRegistry) -> Self {
        let mut s = Self {
            interner: super::interner::Interner::new(),
            registry: registry.clone(),
            entities: fnv::FnvHashMap::default(),
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

    fn resolve_ident(&self, ident: &IdOrIdent) -> Option<Id> {
        match ident {
            IdOrIdent::Id(id) => Some(*id),
            IdOrIdent::Name(name) => {
                self.indexes
                    .get(registry::INDEX_IDENT_LOCAL)
                    .get_unique(&MemoryValue::String(SharedStr::from_string(
                        name.to_string(),
                    )))
            }
        }
    }

    fn resolve_entity(&self, ident: &IdOrIdent) -> Option<&MemoryTuple> {
        let id = self.resolve_ident(ident)?;
        self.entities.get(&id)
    }

    fn must_resolve_entity(
        &self,
        ident: &IdOrIdent,
    ) -> Result<&MemoryTuple, error::EntityNotFound> {
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
                Ok((attr.local_id, value))
            })
            .collect::<Result<_, _>>()?;
        Ok(MemoryTuple(map))
    }

    fn tuple_to_data_map(&self, tuple: &MemoryTuple) -> DataMap {
        let reg = self.registry.read().unwrap();

        let map: std::collections::BTreeMap<_, _> = tuple
            .0
            .iter()
            .map(|(id, value)| {
                let attr = reg.attr(*id);
                let value = value.into();
                (attr.schema.ident.clone(), value)
            })
            .collect();

        ValueMap(map)
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

        self.indexes.append_checked(schema.local_id, index);
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
        reg: &Registry,
    ) -> Result<(), AnyError> {
        let value = self.interner.intern_value(op.value);

        let index_id = op.index;

        match self.indexes.get_mut(op.index) {
            super::index::Index::Unique(idx) => {
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
        reg: &Registry,
    ) -> Result<(), AnyError> {
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
        reg: &Registry,
    ) -> Result<(), AnyError> {
        match op {
            TupleIndexOp::Insert(op) => self.tuple_index_insert(tuple_id, op, revert, reg),
            TupleIndexOp::Replace(op) => self.tuple_index_replace(tuple_id, op, revert, reg),
            TupleIndexOp::Remove(op) => self.tuple_index_remove(tuple_id, op, revert),
        }
    }

    fn tuple_create(
        &mut self,
        create: backend::TupleCreate,
        revert: &mut RevertList,
        reg: &Registry,
    ) -> Result<(), AnyError> {
        if self.entities.contains_key(&create.id) {
            return Err(anyhow!("Entity id already exists: '{}'", create.id));
        }

        for op in create.index_ops {
            self.tuple_index_insert(create.id, op, revert, reg)?;
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
        reg: &Registry,
    ) -> Result<(), AnyError> {
        for op in replace.index_ops {
            self.apply_tuple_index_op(replace.id, op, revert, reg)?;
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
        reg: &Registry,
    ) -> Result<(), AnyError> {
        for op in std::mem::take(&mut update.index_ops) {
            self.apply_tuple_index_op(update.id, op, revert, reg)?;
        }

        let old = self
            .entities
            .get_mut(&update.id)
            .ok_or_else(|| error::EntityNotFound::new(update.id.into()))?;

        let reg = self.registry.read().unwrap();

        let mut replaced_values = Vec::<(LocalAttributeId, Option<MemoryValue>)>::new();

        for (key, new_value) in update.data.0 {
            let attr = reg.require_attr_by_name(&key)?;

            // FIXME: this logic should not be here, but be handled by
            // Registry::validate_merge
            if let Some(old_value) = old.remove(&attr.local_id) {
                // FIXME: this is hacky and only covers lists...
                match (old_value, new_value) {
                    (MemoryValue::List(mut old_items), Value::List(new_items)) => {
                        for item in new_items {
                            let item = self.interner.intern_value(item);
                            if !old_items.contains(&item) {
                                old_items.push(item);
                            }
                        }
                        old.0.insert(attr.local_id, MemoryValue::List(old_items));
                    }
                    (old_value, new_value) => {
                        old.0
                            .insert(attr.local_id, self.interner.intern_value(new_value));
                        replaced_values.push((attr.local_id, Some(old_value)));
                    }
                }
            } else {
                old.0
                    .insert(attr.local_id, self.interner.intern_value(new_value));
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
            if let Some(value) = old.0.remove(&attr.local_id) {
                removed.push((attr.local_id, value));
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

    fn tuple_select_patch(
        &mut self,
        expr: &Expr,
        patch: &Patch,
        revert: &mut RevertList,
        reg: &Registry,
    ) -> Result<(), AnyError> {
        // TODO: should there be a helper function to plan a scna directly?
        let select = Select::new().with_filter(expr.clone());
        let raw_ops = query_planner::plan_select(select, reg)?;
        let plan = self.build_query_plan(raw_ops, reg)?;
        let ids: Vec<Id> = self
            .run_query(plan)
            .filter_map(|tuple| tuple.get_id())
            .collect();

        for id in ids {
            let mem_entity = self.entities.get(&id).unwrap();
            let entity = self.tuple_to_data_map(mem_entity);

            let ops = reg.validate_patch(
                EntityPatch {
                    id: mem_entity.get_id().unwrap(),
                    patch: patch.clone(),
                },
                entity,
            )?;

            self.apply_db_ops(ops, revert, reg)?;
        }

        Ok(())
    }

    fn tuple_select_remove(
        &mut self,
        selector: &MemoryExpr,
        rem: &backend::TupleRemoveAttrs,
        revert: &mut RevertList,
        reg: &Registry,
    ) -> Result<(), AnyError> {
        // TODO: use query logic instead of full table scan for speedup.
        // TODO: figure out how to do this in one step. Two step process
        // right now due to borrow checker errors.
        //
        let mut to_remove = Vec::new();

        for (entity_id, entity) in self.entities.iter() {
            if Self::entity_filter(entity, selector) {
                let mut removed_attr_ids = Vec::new();
                for attr_id in &rem.attrs {
                    let attr = reg.require_attr(*attr_id)?;
                    if entity.contains_key(&attr.local_id) {
                        removed_attr_ids.push(attr);
                    }
                }

                if !removed_attr_ids.is_empty() {
                    to_remove.push((*entity_id, removed_attr_ids));
                }
            }
        }

        for (entity_id, removed_attr_ids) in to_remove {
            let entity = self.entities.get_mut(&entity_id).unwrap();

            // FIXME: need to respect index changes via registry.build_remove!
            let attrs = removed_attr_ids
                .into_iter()
                .filter_map(|attr| Some((attr.local_id, entity.remove(&attr.local_id)?)))
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
    fn apply_db_ops(
        &mut self,
        ops: Vec<DbOp>,
        revert: &mut RevertList,
        reg: &Registry,
    ) -> Result<(), AnyError> {
        use crate::backend::TupleOp;

        // FIXME: implement validate_ checks via registry for all operations.
        // FIXME: guard against schema changes outside of a migration.
        for op in ops {
            match op {
                DbOp::Tuple(tuple) => match tuple {
                    TupleOp::Create(create) => {
                        self.tuple_create(create, revert, reg)?;
                    }
                    TupleOp::Replace(repl) => {
                        self.tuple_replace(repl, revert, reg)?;
                    }
                    TupleOp::Merge(update) => {
                        self.tuple_merge(update, revert, reg)?;
                    }
                    TupleOp::RemoveAttrs(remove) => {
                        self.tuple_remove_attrs(remove, revert)?;
                    }
                    TupleOp::Delete(del) => {
                        self.tuple_delete(del, revert)?;
                    }
                    TupleOp::Patch(_patch) => {
                        // will never exist as a real op.
                        unreachable!()
                    }
                },
                DbOp::Select(sel) => match sel.op {
                    TupleOp::Create(_) => todo!(),
                    TupleOp::Replace(_) => todo!(),
                    TupleOp::Merge(_) => todo!(),
                    TupleOp::RemoveAttrs(remove) => {
                        let resolved = query_planner::resolve_expr(sel.selector, reg)?;
                        let expr = self.build_memory_expr(resolved, reg)?;
                        self.tuple_select_remove(&expr, &remove, revert, reg)?;
                    }
                    TupleOp::Delete(_) => todo!(),
                    TupleOp::Patch(patch) => {
                        self.tuple_select_patch(&sel.selector, &patch.patch, revert, reg)?;
                    }
                },
                DbOp::IndexPopulate(pop) => {
                    let index = reg.require_index_by_id(pop.index_id)?;
                    self.index_populate(&reg, index, revert)?;
                }
            }
        }

        Ok(())
    }

    fn index_populate(
        &mut self,
        reg: &Registry,
        index: &RegisteredIndex,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        let attrs = index
            .schema
            .attributes
            .iter()
            .map(|id| reg.require_attr_by_id(*id).map(|a| a.local_id))
            .collect::<Result<Vec<_>, _>>()?;
        if attrs.len() != 1 {
            // TODO: Implement multi-attribute indexes
            bail!("Multi-attribute indexes not supported yet");
        }
        let attr_id = attrs[0];

        // FIXME: prevent accumulating all ops in memory.
        // Indexes should be behind a separate lock!
        let mut ops = Vec::new();
        for (entity_id, data) in &self.entities {
            if let Some(value) = data.0.get(&attr_id) {
                let op = TupleIndexOp::Insert(TupleIndexInsert {
                    index: index.local_id,
                    value: value.into(),
                    unique: index.schema.unique,
                });
                ops.push((*entity_id, op));
            }
        }

        for (tuple_id, op) in ops {
            self.apply_tuple_index_op(tuple_id, op, revert, reg)?;
        }

        Ok(())
    }

    fn apply_create(
        &mut self,
        create: query::mutate::Create,
        revert: &mut RevertList,
        reg: &Registry,
    ) -> Result<(), AnyError> {
        let ops = self.registry.read().unwrap().validate_create(create)?;
        self.apply_db_ops(ops, revert, reg)?;
        Ok(())
    }

    fn apply_replace(
        &mut self,
        repl: query::mutate::Replace,
        revert: &mut RevertList,
        registry: &Registry,
    ) -> Result<(), AnyError> {
        let old = match self.entities.get(&repl.id) {
            Some(tuple) => Some(self.tuple_to_data_map(&tuple)),
            None => None,
        };

        let ops = self.registry.read().unwrap().validate_replace(repl, old)?;
        self.apply_db_ops(ops, revert, registry)?;
        Ok(())
    }

    fn apply_merge(
        &mut self,
        merge: query::mutate::Merge,
        revert: &mut RevertList,
        reg: &Registry,
    ) -> Result<(), AnyError> {
        if let Some(old_tuple) = self.entities.get(&merge.id) {
            let old = self.tuple_to_data_map(old_tuple);
            let ops = self.registry.read().unwrap().validate_merge(merge, old)?;
            self.apply_db_ops(ops, revert, reg)
        } else {
            let create = query::mutate::Create {
                id: merge.id,
                data: merge.data,
            };
            self.apply_create(create, revert, reg)
        }
    }

    fn apply_patch(
        &mut self,
        epatch: query::mutate::EntityPatch,
        revert: &mut RevertList,
        registry: &Registry,
    ) -> Result<(), AnyError> {
        let current_entity = self.entity(epatch.id.into())?;

        let ops = self
            .registry
            .read()
            .unwrap()
            .validate_patch(epatch, current_entity)?;
        self.apply_db_ops(ops, revert, registry)?;
        Ok(())
    }

    fn apply_delete(
        &mut self,
        delete: query::mutate::Delete,
        revert: &mut RevertList,
        reg: &Registry,
    ) -> Result<(), AnyError> {
        let old = self
            .entities
            .get(&delete.id)
            .ok_or_else(|| EntityNotFound::new(delete.id.into()))
            .map(|t| self.tuple_to_data_map(t))?;

        let ops = reg.validate_delete(delete, old)?;
        self.apply_db_ops(ops, revert, reg)?;
        Ok(())
    }

    /// Apply a batch of operations.
    fn apply_batch_impl(
        &mut self,
        batch: query::mutate::Batch,
        reg: &Registry,
    ) -> Result<RevertList, AnyError> {
        // FIXME: rollback when errors happen.

        let mut revert = Vec::new();

        for action in batch.actions {
            let res = match action {
                query::mutate::Mutate::Create(create) => {
                    self.apply_create(create, &mut revert, reg)
                }
                query::mutate::Mutate::Replace(repl) => self.apply_replace(repl, &mut revert, reg),
                query::mutate::Mutate::Merge(merge) => self.apply_merge(merge, &mut revert, reg),
                query::mutate::Mutate::Delete(del) => self.apply_delete(del, &mut revert, reg),
                query::mutate::Mutate::Patch(patch) => self.apply_patch(patch, &mut revert, reg),
            };

            if let Err(err) = res {
                // An error happened, so revert changes before returning.
                self.apply_revert(revert);
                return Err(err);
            }
        }

        Ok(revert)
    }

    pub fn apply_batch(&mut self, batch: Batch) -> Result<(), AnyError> {
        let shared_reg = self.registry().clone();
        let reg = shared_reg.read().unwrap();
        self.apply_batch_impl(batch, &reg)?;
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
    pub fn apply_batch_revertable(&mut self, batch: Batch) -> Result<RevertEpoch, AnyError> {
        let shared_reg = self.registry().clone();
        let reg = shared_reg.read().unwrap();
        let ops = self.apply_batch_impl(batch, &reg)?;
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
                            data.remove(&attr_id);
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
        let (mig, ops) = crate::schema_builder::build_migration(&mut reg, mig, is_internal)?;

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
                query::migrate::SchemaAction::EntityAttributeAdd(_) => {}
                query::migrate::SchemaAction::EntityAttributeChangeCardinality(_) => {}
                query::migrate::SchemaAction::AttributeCreateIndex(_) => {}
                query::migrate::SchemaAction::AttributeChangeType(_) => {}
            }
        }

        let mut revert = Vec::new();
        if let Err(err) = self.apply_db_ops(ops.clone(), &mut revert, &reg) {
            self.apply_revert(revert);
            Err(err)
        } else {
            *self.registry.write().unwrap() = reg;
            Ok(revert)
        }
    }

    pub fn migrate(&mut self, mig: Migration) -> Result<(), AnyError> {
        tracing::trace!(migration=?mig, "applying migration to memory store");
        self.migrate_impl(mig, false)?;
        Ok(())
    }

    pub fn migrate_revertable(&mut self, mig: Migration) -> Result<RevertEpoch, AnyError> {
        let ops = self.migrate_impl(mig, false)?;
        let epoch = self.persist_revert_epoch(ops);
        Ok(epoch)
    }

    pub fn entity(&self, id: IdOrIdent) -> Result<DataMap, AnyError> {
        self.must_resolve_entity(&id)
            .map_err(AnyError::from)
            .map(|tuple| self.tuple_to_data_map(tuple))
    }

    pub fn entity_opt(&self, id: IdOrIdent) -> Result<Option<DataMap>, AnyError> {
        let opt = self
            .resolve_entity(&id)
            .map(|tuple| self.tuple_to_data_map(tuple));
        Ok(opt)
    }

    fn apply_sort<'a>(items: &mut Vec<Cow<'a, MemoryTuple>>, sorts: &[Sort<MemoryExpr>]) {
        match sorts.len() {
            0 => {}
            1 => {
                let sort = &sorts[0];

                if sort.order == Order::Asc {
                    items.sort_by(|a, b| {
                        let aval = Self::eval_expr(&a, &sort.on);
                        let bval = Self::eval_expr(&b, &sort.on);
                        aval.cmp(&bval)
                    })
                } else {
                    items.sort_by(|a, b| {
                        let aval = Self::eval_expr(&a, &sort.on);
                        let bval = Self::eval_expr(&b, &sort.on);
                        bval.cmp(&aval)
                    })
                }
            }
            _ => {
                items.sort_by(|a, b| {
                    let mut ord = std::cmp::Ordering::Equal;

                    for sort in sorts {
                        let aval = Self::eval_expr(&a, &sort.on);
                        let bval = Self::eval_expr(&b, &sort.on);

                        ord = if sort.order == Order::Asc {
                            aval.cmp(&bval)
                        } else {
                            bval.cmp(&aval)
                        };
                        if ord != std::cmp::Ordering::Equal {
                            break;
                        }
                    }

                    ord
                });
            }
        }
    }

    fn run_query<'a>(
        &'a self,
        op: query_planner::QueryPlan<MemoryValue, MemoryExpr>,
    ) -> TupleIter<'_> {
        match op {
            QueryPlan::EmptyRelation => Box::new(Vec::new().into_iter()),
            QueryPlan::SelectEntity { id } => {
                if let Some(entity) = self.entities.get(&id) {
                    Box::new(vec![Cow::Borrowed(entity)].into_iter())
                } else {
                    Box::new(Vec::new().into_iter())
                }
            }
            QueryPlan::Scan { filter } => {
                if let Some(filter) = filter {
                    let out = self
                        .entities
                        .values()
                        .map(Cow::Borrowed)
                        .filter(move |tuple| Self::entity_filter(tuple, &filter));
                    Box::new(out)
                } else {
                    Box::new(self.entities.values().map(Cow::Borrowed))
                }
            }
            QueryPlan::Filter { expr, input } => {
                let input = self.run_query(*input);
                let out = input.filter(move |tuple| Self::entity_filter(tuple, &expr));
                Box::new(out)
            }
            QueryPlan::Limit { limit, input } => {
                let input = self.run_query(*input);
                let out = input.take(limit as usize);
                Box::new(out)
            }
            QueryPlan::Merge { left, right } => {
                let left = self.run_query(*left);
                let right = self.run_query(*right);
                let out = left.chain(right);
                Box::new(out)
            }
            QueryPlan::IndexScan {
                index,
                from,
                until,
                direction,
            } => {
                let iter = match self.indexes.get(index) {
                    index::Index::Unique(index) => index.range(from, until, direction),
                    index::Index::Multi(index) => index.range(from, until, direction),
                };

                let out = iter
                    .map(|id| self.entities.get(&id).map(Cow::Borrowed))
                    .flatten();
                Box::new(out)
            }
            QueryPlan::IndexScanPrefix {
                index,
                prefix,
                direction,
            } => {
                let iter = match self.indexes.get(index) {
                    index::Index::Unique(index) => index.range_prefix(prefix, direction),
                    index::Index::Multi(index) => index.range_prefix(prefix, direction),
                };

                let out = iter
                    .map(|id| self.entities.get(&id).map(Cow::Borrowed))
                    .flatten();
                Box::new(out)
            }
            QueryPlan::Sort { sorts, input } => {
                let input = self.run_query(*input);
                let mut items: Vec<_> = input.collect();
                Self::apply_sort(&mut items, &sorts);
                Box::new(items.into_iter())
            }
            QueryPlan::Skip { count, input } => {
                let input = self.run_query(*input);
                let out = input.skip(count as usize);
                Box::new(out)
            }
            QueryPlan::IndexSelect { index, value } => match self.indexes.get(index) {
                index::Index::Unique(index) => {
                    let out = index
                        .get(&value)
                        .and_then(|id| self.entities.get(&id))
                        .map(Cow::Borrowed)
                        .into_iter();
                    Box::new(out)
                }
                index::Index::Multi(index) => {
                    let out = index
                        .get(&value)
                        .into_iter()
                        .flatten()
                        .filter_map(|id| self.entities.get(&id))
                        .map(Cow::Borrowed)
                        .into_iter();
                    Box::new(out)
                }
            },
        }
    }

    fn build_query_plan(
        &self,
        plan: QueryPlan<Value, ResolvedExpr>,
        reg: &Registry,
    ) -> Result<QueryPlan<MemoryValue, MemoryExpr>, AnyError> {
        let plan = match plan {
            QueryPlan::EmptyRelation => QueryPlan::EmptyRelation,
            QueryPlan::SelectEntity { id } => QueryPlan::SelectEntity { id },
            QueryPlan::Scan { filter } => QueryPlan::Scan {
                filter: filter
                    .map(|expr| self.build_memory_expr(expr, reg))
                    .transpose()?,
            },
            QueryPlan::Merge { left, right } => {
                let left = Box::new(self.build_query_plan(*left, reg)?);
                let right = Box::new(self.build_query_plan(*right, reg)?);

                QueryPlan::Merge { left, right }
            }
            QueryPlan::IndexScan {
                index,
                from,
                until,
                direction,
            } => QueryPlan::IndexScan {
                index,
                from: from.map(MemoryValue::from_value_standalone),
                until: until.map(MemoryValue::from_value_standalone),
                direction,
            },
            QueryPlan::IndexScanPrefix {
                index,
                prefix,
                direction,
            } => QueryPlan::IndexScanPrefix {
                index,
                prefix: MemoryValue::from_value_standalone(prefix),
                direction,
            },
            QueryPlan::Sort { sorts, input } => QueryPlan::Sort {
                input: Box::new(self.build_query_plan(*input, reg)?),
                sorts: sorts
                    .into_iter()
                    .map(|s| -> Result<Sort<MemoryExpr>, AnyError> {
                        Ok(Sort {
                            on: self.build_memory_expr(s.on, reg)?,
                            order: s.order,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            },
            QueryPlan::Filter { expr, input } => QueryPlan::Filter {
                expr: self.build_memory_expr(expr, reg)?,
                input: Box::new(self.build_query_plan(*input, reg)?),
            },
            QueryPlan::Limit { limit, input } => QueryPlan::Limit {
                limit,
                input: Box::new(self.build_query_plan(*input, reg)?),
            },
            QueryPlan::Skip { count, input } => QueryPlan::Skip {
                count,
                input: Box::new(self.build_query_plan(*input, reg)?),
            },
            QueryPlan::IndexSelect { index, value } => QueryPlan::IndexSelect {
                index,
                value: MemoryValue::from_value_standalone(value),
            },
        };
        Ok(plan)
    }

    pub fn select(
        &self,
        query: query::select::Select,
    ) -> Result<query::select::Page<Item>, AnyError> {
        // TODO: query validation and planning

        let reg = self.registry().read().unwrap();

        let raw_plan = query_planner::plan_select(query, &reg)?;
        let mem_plan = self.build_query_plan(raw_plan, &reg)?;

        let items = self
            .run_query(mem_plan)
            .map(|tuple| {
                Ok(Item {
                    data: self.tuple_to_data_map(tuple.as_ref()),
                    joins: Vec::new(),
                })
            })
            .collect::<Result<Vec<Item>, anyhow::Error>>()?;

        Ok(Page {
            next_cursor: None,
            items,
        })
    }

    fn build_memory_expr(
        &self,
        expr: ResolvedExpr,
        reg: &Registry,
    ) -> Result<MemoryExpr, AnyError> {
        use ResolvedExpr as E;

        match expr {
            E::Literal(lit) => Ok(MemoryExpr::Literal(MemoryValue::from_value_standalone(lit))),
            E::List(items) => {
                let items = items
                    .into_iter()
                    .map(|e| self.build_memory_expr(e, reg))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(MemoryExpr::List(items))
            }
            E::Attr(attr) => Ok(MemoryExpr::Attr(attr)),
            E::Ident(ident) => {
                let id = self
                    .resolve_ident(&ident)
                    .ok_or_else(|| EntityNotFound::new(ident))?;
                Ok(MemoryExpr::Ident(id))
            }
            E::UnaryOp { op, expr } => Ok(MemoryExpr::UnaryOp {
                op,
                expr: Box::new(self.build_memory_expr(*expr, reg)?),
            }),
            E::BinaryOp(op) => Ok(MemoryExpr::BinaryOp {
                left: Box::new(self.build_memory_expr(op.left, reg)?),
                op: op.op,
                right: Box::new(self.build_memory_expr(op.right, reg)?),
            }),
            E::If { value, then, or } => Ok(MemoryExpr::If {
                value: Box::new(self.build_memory_expr(*value, reg)?),
                then: Box::new(self.build_memory_expr(*then, reg)?),
                or: Box::new(self.build_memory_expr(*or, reg)?),
            }),
            E::Op(_) => {
                #[cfg(debug_assertions)]
                panic!("Invalid ResolvedExpr");

                #[cfg(not(debug_assertions))]
                Err(anyhow!("Internal error: Invalid resolved expr"))
            }
            E::InLiteral { value, items } => {
                let items = items
                    .into_iter()
                    .map(|v| MemoryValue::from_value_standalone(v))
                    .collect();
                Ok(MemoryExpr::InLiteral {
                    value: Box::new(self.build_memory_expr(*value, reg)?),
                    items,
                })
            }
            E::Regex(e) => Ok(MemoryExpr::Regex(e)),
        }
    }

    fn eval_expr<'a>(
        entity: &'a MemoryTuple,
        expr: &'a MemoryExpr,
    ) -> std::borrow::Cow<'a, MemoryValue> {
        use query::expr::BinaryOp;
        use MemoryExpr as E;

        match expr {
            E::Literal(v) => Cow::Borrowed(v),
            E::List(values) => {
                let values = values
                    .into_iter()
                    .map(|v| Self::eval_expr(entity, v).into_owned())
                    .collect();
                Cow::Owned(MemoryValue::List(values))
            }
            E::Attr(local_id) => entity
                .get(local_id)
                .map(|attr_value| Cow::Borrowed(attr_value))
                .unwrap_or(cowal_unit()),
            E::Ident(id) => Cow::Owned(MemoryValue::Id(*id)),
            E::UnaryOp { op, expr } => {
                let value = Self::eval_expr(entity, expr);
                match op {
                    query::expr::UnaryOp::Not => {
                        Cow::Owned(MemoryValue::Bool(!value.as_bool_discard_other()))
                    }
                }
            }
            E::BinaryOp { left, op, right } => match op {
                query::expr::BinaryOp::And => {
                    let left_flag = Self::eval_expr(entity, left);

                    if left_flag.is_true() {
                        let right = Self::eval_expr(entity, right);
                        right
                    } else {
                        Cow::Owned(MemoryValue::Bool(false))
                    }
                }
                query::expr::BinaryOp::Or => {
                    let left_flag = Self::eval_expr(entity, left);
                    if left_flag.is_true() {
                        left_flag
                    } else {
                        Self::eval_expr(entity, right)
                    }
                }
                query::expr::BinaryOp::RegexMatch => {
                    let left = Self::eval_expr(entity, left);

                    let re = if let MemoryExpr::Regex(re) = &**right {
                        re
                    } else {
                        #[cfg(debug_assertions)]
                        panic!("invalid regex match query: right operand must be a regex");

                        #[cfg(not(debug_assertions))]
                        return Cow::Owned(MemoryValue::Unit);
                    };
                    let value = if let MemoryValue::String(s) = &*left {
                        s
                    } else {
                        return Cow::Owned(MemoryValue::Bool(false));
                    };

                    let is_match = re.is_match(value.as_ref());

                    Cow::Owned(MemoryValue::Bool(is_match))
                }
                other => {
                    let left = Self::eval_expr(entity, left);
                    let right = Self::eval_expr(entity, right);

                    let flag = match other {
                        BinaryOp::Eq => left == right,
                        BinaryOp::Neq => left != right,
                        BinaryOp::Gt => left > right,
                        BinaryOp::Gte => left >= right,
                        BinaryOp::Lt => left < right,
                        BinaryOp::Lte => left <= right,
                        BinaryOp::Contains => match (left.as_ref(), right.as_ref()) {
                            (MemoryValue::String(value), MemoryValue::String(pattern)) => {
                                value.as_ref().contains(pattern.as_ref())
                            }
                            (MemoryValue::List(left), MemoryValue::List(right)) => {
                                left.iter().any(|item| right.contains(item))
                            }
                            (_left, _right) => {
                                // TODO: this should be rejected by query
                                // validation.
                                false
                            }
                        },
                        BinaryOp::In => {
                            tracing::trace!(?left, ?right, "comparing BinaryOp::In");
                            // TODO: probably need to cover more variants here!
                            match (left.as_ref(), right.as_ref()) {
                                (value, MemoryValue::List(items)) => {
                                    items.iter().any(|x| x == value)
                                }
                                _other => false,
                            }
                        }
                        BinaryOp::And | BinaryOp::Or | BinaryOp::RegexMatch => {
                            // Covered above in separate matches.
                            unreachable!()
                        }
                    };
                    Cow::Owned(MemoryValue::Bool(flag))
                }
            },
            E::If { value, then, or } => {
                let flag = Self::eval_expr(entity, &*value);
                // TODO: handle non-boolean flag! (report warning/error)
                if flag.is_true() {
                    Self::eval_expr(entity, &*then)
                } else {
                    Self::eval_expr(entity, &*or)
                }
            }
            E::InLiteral { value, items } => {
                let value = Self::eval_expr(entity, value);
                Cow::Owned(MemoryValue::Bool(items.contains(&*value)))
            }
            E::Regex(_) => Cow::Owned(MemoryValue::Unit),
        }
    }

    fn entity_filter(entity: &MemoryTuple, expr: &memory_data::MemoryExpr) -> bool {
        Self::eval_expr(entity, expr).as_bool_discard_other()
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
const fn cowal_unit<'a>() -> std::borrow::Cow<'a, MemoryValue> {
    std::borrow::Cow::Owned(MemoryValue::Unit)
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
        replaced_data: Vec<(LocalAttributeId, Option<super::memory_data::MemoryValue>)>,
    },
    TupleAttrsRemoved {
        id: Id,
        attrs: Vec<(LocalAttributeId, super::memory_data::MemoryValue)>,
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

#[cfg(test)]
mod tests {
    use factordb::query::expr::BinaryOp;

    use super::*;

    #[test]
    fn test_memory_expr_eval() {
        use memory_data::MemoryExpr;

        let reg = Registry::new();
        let title_id = reg.require_attr_by_name("factor/title").unwrap().local_id;

        let mut tuple = MemoryTuple::new();
        let hello = MemoryValue::String(memory_data::SharedStr::from_string("hello".to_string()));
        tuple.0.insert(title_id, hello.clone());

        let expr = MemoryExpr::BinaryOp {
            left: Box::new(MemoryExpr::Attr(title_id)),
            op: BinaryOp::Eq,
            right: Box::new(MemoryExpr::Literal(hello)),
        };

        let flag = MemoryStore::eval_expr(&tuple, &expr);
        assert_eq!(flag.as_bool_discard_other(), true);
    }
}
