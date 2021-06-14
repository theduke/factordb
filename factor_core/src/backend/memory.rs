use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
};

use anyhow::anyhow;
use fnv::FnvHashMap;
use futures::{future::ready, FutureExt};
use ordered_float::OrderedFloat;

use crate::{
    data::{value::ValueMap, DataMap, Id, Ident, Value},
    error,
    query::{self, expr::Expr, migrate::Migration},
    registry::{Registry, SharedRegistry},
    schema::{self, AttributeDescriptor},
    AnyError,
};

use super::{BackendFuture, DbOp, TupleCreate, TupleDelete, TupleMerge, TupleOp, TupleReplace};

#[derive(Clone)]
pub struct MemoryDb {
    registry: SharedRegistry,
    state: Arc<RwLock<MemoryStore>>,
}

impl MemoryDb {
    pub fn new() -> Self {
        let registry = crate::registry::Registry::new().into_shared();
        Self {
            registry: registry.clone(),
            state: Arc::new(RwLock::new(MemoryStore::new(registry))),
        }
    }
}

/// Memory store for building a backend.
///
/// The [MemoryDb] is a simple memory-only backend, but the store can also
/// be used by other backends as a caching layer or for other purposes.
pub struct MemoryStore {
    interner: Interner,
    registry: SharedRegistry,
    entities: FnvHashMap<Id, MemoryTuple>,
    idents: HashMap<String, Id>,

    revert_epoch: RevertEpoch,
    revert_ops: Option<(RevertEpoch, RevertList)>,
}

impl MemoryStore {
    pub fn new(registry: SharedRegistry) -> Self {
        Self {
            interner: Interner::new(),
            registry,
            entities: FnvHashMap::default(),
            idents: HashMap::new(),
            revert_epoch: 0,
            revert_ops: None,
        }
    }

    pub fn registry(&self) -> &SharedRegistry {
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
                Ok((attr.id, value))
            })
            .collect::<Result<_, _>>()?;
        Ok(MemoryTuple(map))
    }

    fn tuple_to_data_map(&self, tuple: &MemoryTuple) -> Result<DataMap, AnyError> {
        let reg = self.registry.read().unwrap();

        let map: BTreeMap<_, _> = tuple
            .0
            .iter()
            .map(|(id, value)| -> Result<_, AnyError> {
                let attr = reg.require_attr(*id)?;
                let value = value.into();
                Ok((attr.name.clone(), value))
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

    fn tuple_create(
        &mut self,
        create: TupleCreate,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        if self.entities.contains_key(&create.id) {
            return Err(anyhow!("Entity id already exists: '{}'", create.id));
        }
        let map = self.intern_data_map(create.data)?;
        self.entities.insert(create.id, map);
        revert.push(RevertOp::TupleCreated { id: create.id });
        Ok(())
    }

    fn tuple_replace(
        &mut self,
        replace: TupleReplace,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        let old = self.entities.remove(&replace.id);
        let map = self.intern_data_map(replace.data)?;
        self.entities.insert(replace.id, map);
        revert.push(RevertOp::TupleReplaced {
            id: replace.id,
            data: old,
        });
        Ok(())
    }

    fn tuple_merge(&mut self, update: TupleMerge, revert: &mut RevertList) -> Result<(), AnyError> {
        let old = self
            .entities
            .get_mut(&update.id)
            .ok_or_else(|| error::EntityNotFound::new(update.id.into()))?;

        let reg = self.registry.read().unwrap();

        let mut replaced_values = Vec::new();

        for (key, value) in update.data.0 {
            // FIXME: properly patch!
            let attr = reg.require_attr_by_name(&key)?;
            let value = self.interner.intern_value(value);
            let old_value = old.0.remove(&attr.id);
            replaced_values.push((attr.id, old_value));
            old.0.insert(attr.id, value);
        }

        if !replaced_values.is_empty() {
            revert.push(RevertOp::TupleMerged {
                id: update.id,
                replaced_data: replaced_values,
            });
        }

        Ok(())
    }

    fn tuple_remove_attrs(
        &mut self,
        rem: super::TupleRemoveAttrs,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        let old = self
            .entities
            .get_mut(&rem.id)
            .ok_or_else(|| error::EntityNotFound::new(rem.id.into()))?;

        let reg = self.registry.read().unwrap();
        let mut removed = Vec::new();
        for attr_id in rem.attrs {
            let attr = reg.require_attr(attr_id)?;
            if let Some(value) = old.0.remove(&attr.id) {
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

    fn tuple_delete(&mut self, del: TupleDelete, revert: &mut RevertList) -> Result<(), AnyError> {
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
        selector: &Expr,
        rem: &super::TupleRemoveAttrs,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        let reg = self.registry.clone();

        // TODO: use query logic instead of full table scan for speedup.
        for (id, entity) in self.entities.iter_mut() {
            if Self::entity_filter(entity, selector, &*reg.read().unwrap()) {
                let mut removed = Vec::new();
                for attr_id in &rem.attrs {
                    if let Some(value) = entity.remove(&attr_id) {
                        removed.push((*attr_id, value));
                    }
                }

                if !removed.is_empty() {
                    revert.push(RevertOp::TupleAttrsRemoved {
                        id: *id,
                        attrs: removed,
                    })
                }
            }
        }

        Ok(())
    }

    /// Apply database operations.
    /// [RevertOp]s are collected into the provided revert list, which allows
    /// undoing operations.
    fn apply_db_ops(&mut self, ops: Vec<DbOp>, revert: &mut RevertList) -> Result<(), AnyError> {
        // FIXME: revert changes if anything fails.
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
        create: query::update::Create,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        let ops = self.registry.read().unwrap().validate_create(create)?;
        self.apply_db_ops(ops, revert)?;
        Ok(())
    }

    fn apply_replace(
        &mut self,
        repl: query::update::Replace,
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

    fn apply_patch(
        &mut self,
        patch: query::update::Patch,
        revert: &mut RevertList,
    ) -> Result<(), AnyError> {
        let old = self
            .entities
            .get(&patch.id)
            .ok_or_else(|| anyhow!("Entity not found: {:?}", patch.id))
            .and_then(|t| self.tuple_to_data_map(t))?;

        let ops = self.registry.read().unwrap().validate_patch(patch, old)?;
        self.apply_db_ops(ops, revert)?;
        Ok(())
    }

    fn apply_delete(
        &mut self,
        delete: query::update::Delete,
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
        batch: query::update::BatchUpdate,
    ) -> Result<RevertList, AnyError> {
        // FIXME: rollback when errors happen.

        let mut revert = Vec::new();

        for action in batch.actions {
            let res = match action {
                query::update::Update::Create(create) => self.apply_create(create, &mut revert),
                query::update::Update::Replace(repl) => self.apply_replace(repl, &mut revert),
                query::update::Update::Patch(patch) => self.apply_patch(patch, &mut revert),
                query::update::Update::Delete(del) => self.apply_delete(del, &mut revert),
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
        batch: crate::query::update::BatchUpdate,
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
        batch: crate::query::update::BatchUpdate,
    ) -> Result<RevertEpoch, AnyError> {
        let ops = self.apply_batch_impl(batch)?;
        let epoch = self.persist_revert_epoch(ops);
        Ok(epoch)
    }

    /// Revert a list of changes.
    fn apply_revert(&mut self, revert: RevertList) {
        dbg!("applying revert", &revert);

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

    fn migrate_impl(&mut self, mig: Migration) -> Result<RevertList, AnyError> {
        let mut reg = self.registry.read().unwrap().duplicate();
        let (_mig, ops) = schema::logic::validate_migration(&mut reg, mig)?;

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
        self.migrate_impl(mig)?;
        Ok(())
    }

    pub fn migrate_revertable(&mut self, mig: Migration) -> Result<RevertEpoch, AnyError> {
        let ops = self.migrate_impl(mig)?;
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
    ) -> Result<query::select::Page<DataMap>, AnyError> {
        // TODO: query validation and planning

        let reg = self.registry.read().unwrap();

        let items: Vec<(&Id, &MemoryTuple)> = if let Some(filter) = query.filter {
            self.entities
                .iter()
                .filter(|(_id, item)| {
                    // Skip builtin types.

                    if let Some(id) = item
                        .get(&crate::schema::builtin::AttrType::ID)
                        .and_then(|x| x.as_id())
                    {
                        if crate::schema::builtin::id_is_builtin_entity_type(id) {
                            return false;
                        }
                    }

                    let flag = Self::entity_filter(item, &filter, &*reg);
                    flag
                })
                .collect()
        } else {
            self.entities
                .iter()
                .filter(|(_id, item)| {
                    if let Some(id) = item
                        .get(&crate::schema::builtin::AttrType::ID)
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
                .map(|(_id, data)| self.tuple_to_data_map(data))
                .collect::<Result<_, _>>()?
        } else {
            items
                .into_iter()
                .take(query.limit as usize)
                .map(|(_id, data)| self.tuple_to_data_map(data))
                .collect::<Result<_, _>>()?
        };

        Ok(query::select::Page {
            items,
            next_cursor: None,
        })
    }

    fn eval_expr<'a>(
        entity: &MemoryTuple,
        expr: &'a query::expr::Expr,
        reg: &Registry,
    ) -> Cow<'a, Value> {
        let out = match expr {
            query::expr::Expr::Literal(v) => Cow::Borrowed(v),
            query::expr::Expr::Ident(ident) => match ident {
                Ident::Id(id) => entity
                    .get(id)
                    .map(|x| Cow::Owned(x.to_value()))
                    .unwrap_or(cowal_unit()),
                Ident::Name(name) => reg
                    .attr_by_name(name)
                    .and_then(|attr| entity.get(&attr.id).map(|x| Cow::Owned(x.to_value())))
                    .unwrap_or(cowal_unit()),
            },
            query::expr::Expr::Variable(_) => todo!(),
            query::expr::Expr::UnaryOp { op, expr } => {
                let value = Self::eval_expr(entity, expr, reg);
                match op {
                    query::expr::UnaryOp::Not => {
                        let flag = value.as_bool().unwrap_or(false);
                        Cow::Owned(Value::Bool(!flag))
                    }
                }
            }
            query::expr::Expr::BinaryOp { left, op, right } => match op {
                query::expr::BinaryOp::And => {
                    let left_flag = Self::eval_expr(entity, left, reg)
                        .as_bool()
                        .unwrap_or(false);
                    let flag = if left_flag {
                        Self::eval_expr(entity, right, reg)
                            .as_bool()
                            .unwrap_or(false)
                    } else {
                        false
                    };
                    Cow::Owned(Value::Bool(flag))
                }
                query::expr::BinaryOp::Or => {
                    let left_flag = Self::eval_expr(entity, left, reg)
                        .as_bool()
                        .unwrap_or(false);
                    let flag = if left_flag {
                        true
                    } else {
                        Self::eval_expr(entity, right, reg)
                            .as_bool()
                            .unwrap_or(false)
                    };
                    Cow::Owned(Value::Bool(flag))
                }
                other => {
                    let left = Self::eval_expr(entity, left, reg);
                    let right = Self::eval_expr(entity, right, reg);

                    let flag = match other {
                        query::expr::BinaryOp::Eq => left == right,
                        query::expr::BinaryOp::Neq => left != right,
                        query::expr::BinaryOp::Gt => left > right,
                        query::expr::BinaryOp::Gte => left >= right,
                        query::expr::BinaryOp::Lt => left < right,
                        query::expr::BinaryOp::Lte => left <= right,
                        query::expr::BinaryOp::In(_) => todo!(),

                        // Covered above.
                        query::expr::BinaryOp::And | query::expr::BinaryOp::Or => {
                            unreachable!()
                        }
                    };
                    Cow::Owned(Value::Bool(flag))
                }
            },
            query::expr::Expr::If { value, then, or } => {
                let flag = Self::eval_expr(entity, &*value, reg)
                    .as_bool()
                    .unwrap_or(false);
                if flag {
                    Self::eval_expr(entity, &*then, reg)
                } else {
                    Self::eval_expr(entity, &*or, reg)
                }
            }
        };
        out
    }

    fn entity_filter(entity: &MemoryTuple, expr: &query::expr::Expr, reg: &Registry) -> bool {
        Self::eval_expr(entity, expr, reg)
            .as_bool()
            .unwrap_or(false)
    }

    pub fn purge_all_data(&mut self) {
        self.entities.clear();
        self.idents.clear();
        self.interner.clear();
    }
}

fn cowal_unit<'a>() -> Cow<'a, Value> {
    Cow::Owned(Value::Unit)
}

#[derive(Clone, Hash, Debug, PartialOrd, Ord)]
struct SharedStr(Arc<str>);

impl SharedStr {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl PartialEq for SharedStr {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ptr() == other.0.as_ptr()
    }
}

impl Eq for SharedStr {}

#[derive(Debug)]
struct MemoryTuple(FnvHashMap<Id, MemoryValue>);

impl std::ops::Deref for MemoryTuple {
    type Target = FnvHashMap<Id, MemoryValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for MemoryTuple {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
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
        replaced_data: Vec<(Id, Option<MemoryValue>)>,
    },
    TupleAttrsRemoved {
        id: Id,
        attrs: Vec<(Id, MemoryValue)>,
    },
    TupleDeleted {
        id: Id,
        data: MemoryTuple,
    },
}

type RevertList = Vec<RevertOp>;

// fn memory_to_id_map(mem: &MemoryTuple) -> IdMap {
//     mem.iter()
//         .map(|(key, value)| (*key, value.into()))
//         .collect()
// }

// Value for in-memory storage.
// Uses shared strings to save memory usage.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum MemoryValue {
    Unit,

    Bool(bool),
    UInt(u64),
    Int(i64),
    Float(OrderedFloat<f64>),
    String(SharedStr),
    Bytes(Vec<u8>),

    List(Vec<Self>),
    Map(BTreeMap<Self, Self>),

    Id(Id),
}

impl MemoryValue {
    fn to_value(&self) -> Value {
        use MemoryValue as V;
        match self {
            V::Unit => Value::Unit,
            V::Bool(v) => Value::Bool(*v),
            V::UInt(v) => Value::UInt(*v),
            V::Int(v) => Value::Int(*v),
            V::Float(v) => Value::Float(*v),
            V::String(v) => Value::String(v.to_string()),
            V::Bytes(v) => Value::Bytes(v.clone()),
            V::List(v) => Value::List(v.into_iter().map(Into::into).collect()),
            V::Map(v) => Value::Map(
                v.into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
            ),
            V::Id(v) => Value::Id(*v),
        }
    }

    fn as_id(&self) -> Option<Id> {
        if let Self::Id(id) = self {
            Some(*id)
        } else {
            None
        }
    }
}

impl<'a> From<&'a MemoryValue> for Value {
    fn from(v: &'a MemoryValue) -> Self {
        v.to_value()
    }
}

struct Interner {
    strings: HashMap<SharedStr, SharedStr>,
}

impl Interner {
    fn new() -> Self {
        Self {
            strings: HashMap::new(),
        }
    }

    fn clear(&mut self) {
        self.strings.clear();
    }

    fn intern_str(&mut self, value: String) -> SharedStr {
        let shared: SharedStr = SharedStr(Arc::from(value));
        match self.strings.get(&shared) {
            Some(v) => v.clone(),
            None => {
                self.strings.insert(shared.clone(), shared.clone());
                shared
            }
        }
    }

    fn intern_value(&mut self, value: Value) -> MemoryValue {
        use MemoryValue as M;
        match value {
            Value::Unit => M::Unit,
            Value::Bool(v) => M::Bool(v),
            Value::UInt(v) => M::UInt(v),
            Value::Int(v) => M::Int(v),
            Value::Float(v) => M::Float(v),
            Value::String(v) => M::String(self.intern_str(v)),
            Value::Bytes(v) => M::Bytes(v),
            Value::List(v) => M::List(v.into_iter().map(|v| self.intern_value(v)).collect()),
            Value::Map(v) => M::Map(
                v.0.into_iter()
                    .map(|(key, value)| (self.intern_value(key), self.intern_value(value)))
                    .collect(),
            ),
            Value::Id(v) => M::Id(v),
        }
    }
}

impl super::Dao for MemoryTuple {
    fn get(&self, attr: &schema::AttributeSchema) -> Result<Option<Value>, AnyError> {
        Ok(self.0.get(&attr.id).map(|v| v.into()))
    }

    fn set(&mut self, _attr: &schema::AttributeSchema, _value: Value) {
        todo!()
    }
}

impl super::Backend for MemoryDb {
    fn registry(&self) -> &SharedRegistry {
        &self.registry
    }

    fn purge_all_data(&self) -> BackendFuture<()> {
        self.state.write().unwrap().purge_all_data();
        ready(Ok(())).boxed()
    }

    fn entity(&self, id: Ident) -> super::BackendFuture<DataMap> {
        let res = self.state.read().unwrap().entity(id);
        ready(res).boxed()
    }

    fn select(&self, query: query::select::Select) -> BackendFuture<query::select::Page<DataMap>> {
        let res = self.state.read().unwrap().select(query);
        ready(res).boxed()
    }

    fn apply_batch(&self, batch: query::update::BatchUpdate) -> BackendFuture<()> {
        let res = self.state.write().unwrap().apply_batch(batch);
        ready(res).boxed()
    }

    fn migrate(&self, migration: query::migrate::Migration) -> super::BackendFuture<()> {
        let res = self.state.write().unwrap().migrate(migration).map(|_| ());
        ready(res).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_backend() {
        let mem = MemoryDb::new();
        crate::tests::test_backend(mem, |f| futures::executor::block_on(f));
    }
}
