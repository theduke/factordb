//! A simple event log based db backend.
//! See [LogDb] for details.

pub mod convert_json;
pub mod store_memory;

#[cfg(feature = "log_fs")]
pub mod store_file;

mod event;
use anyhow::Context;
pub use event::LogEvent;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use futures::{
    future::{ready, BoxFuture},
    stream::BoxStream,
    FutureExt, StreamExt,
};

use factordb::{
    data,
    prelude::{AttrId, AttrType, AttributeMeta, DataMap, Id, Mutate, Value},
    query::{self, migrate::SchemaAction, mutate::Batch, select::Item},
    AnyError,
};

use crate::registry;

use self::event::LogOp;

use super::{
    memory::store::{MemoryStore, RevertEpoch},
    Backend, BackendFuture,
};

pub struct LogConfig {}

pub type EventId = u64;

/// LogDb is a simple database backend that is based on an event log.
/// Mutations are written to the event log.
/// On restart, the log is read and aggregated.
/// All data is kept in memory.
///
/// The underlying storage mechanism is pluggable via the [LogStore] trait.
///
/// Since the log grows large over time, the log can be compacted by rewriting
/// the event stream and only retaining relevant events.
/// TODO: implement compaction.
///
/// TODO: implement mechanism for only keeping some data in memory and loading
/// the rest on demand.
///
#[derive(Clone)]
pub struct LogDb {
    state: Arc<State>,
}

struct State {
    registry: registry::SharedRegistry,
    mutable: futures::lock::Mutex<MutableState>,
    mem: RwLock<MemoryStore>,
}

struct MutableState {
    store: Box<dyn LogStore + Send + Sync + 'static>,
    current_event_id: EventId,
    migrations: Vec<query::migrate::Migration>,
}

impl MutableState {
    fn increment_event_id(&mut self) -> EventId {
        self.current_event_id = self.current_event_id.wrapping_add(1);
        self.current_event_id
    }
}

impl LogDb {
    /// Get access to the store.
    ///
    /// Since the store is behind a Mutex, you must provide a closure.
    pub async fn with_store<F, O>(&self, f: F) -> O
    where
        F: FnOnce(&dyn LogStore) -> O,
    {
        let state = self.state.mutable.lock().await;
        f(&*state.store)
    }

    pub async fn open<S>(store: S) -> Result<Self, AnyError>
    where
        S: LogStore + Send + Sync + 'static,
    {
        let registry = registry::Registry::new().into_shared();

        // Ignore index constraints during the restore phase.
        // Constraints are re-enabled after the restore.
        let mut memory = MemoryStore::new(registry.clone());
        memory.set_ignore_index_constraints(true);

        let state = State {
            mem: RwLock::new(memory),
            registry,
            mutable: futures::lock::Mutex::new(MutableState {
                migrations: Vec::new(),
                store: Box::new(store),
                current_event_id: 0,
            }),
        };
        let s = Self {
            state: Arc::new(state),
        };

        s.restore().await?;

        // Re-enable index constraints.
        s.state
            .mem
            .write()
            .unwrap()
            .set_ignore_index_constraints(false);

        Ok(s)
    }

    /// Build entity data from a possibly corrupted event stream.
    ///
    /// All errors or invalid operations will be ignored.
    pub async fn recover_data<S>(store: S) -> Result<Vec<DataMap>, AnyError>
    where
        S: LogStore + Send + Sync + 'static,
    {
        fn find_ids_in_value(value: &Value) -> Vec<Id> {
            match value {
                Value::Unit
                | Value::Bool(_)
                | Value::UInt(_)
                | Value::Int(_)
                | Value::Float(_)
                | Value::Bytes(_) => Vec::new(),
                Value::String(s) => {
                    if let Ok(id) = s.parse() {
                        vec![id]
                    } else {
                        Vec::new()
                    }
                }
                Value::List(items) => items.iter().flat_map(find_ids_in_value).collect(),
                Value::Map(map) => map.values().flat_map(find_ids_in_value).collect(),
                Value::Id(id) => {
                    vec![*id]
                }
            }
        }

        fn find_ids(data: &DataMap) -> Vec<Id> {
            data.values().flat_map(find_ids_in_value).collect()
        }

        let mut stream = store.iter_events(0, EventId::MAX).await?;

        let mut data = HashMap::<Id, DataMap>::new();

        while let Some(res) = stream.next().await {
            let event = res?;

            match event.op {
                LogOp::Batch(batch) => {
                    for action in batch.actions {
                        match action {
                            Mutate::Create(mut create) => {
                                create
                                    .data
                                    .insert(AttrId::QUALIFIED_NAME.to_string(), create.id.into());
                                data.insert(create.id, create.data);
                            }
                            Mutate::Replace(mut replace) => {
                                replace
                                    .data
                                    .insert(AttrId::QUALIFIED_NAME.to_string(), replace.id.into());
                                data.insert(replace.id, replace.data);
                            }
                            Mutate::Merge(mut merge) => {
                                if let Some(old) = data.get_mut(&merge.id) {
                                    old.0.extend(merge.data.0.into_iter());
                                } else {
                                    merge.data.insert(
                                        AttrId::QUALIFIED_NAME.to_string(),
                                        merge.id.into(),
                                    );
                                    data.insert(merge.id, merge.data);
                                }
                            }
                            Mutate::Patch(patch) => {
                                let values = data.get(&patch.id).cloned().unwrap_or_default();
                                if let Ok(patched) = patch.patch.apply_map(values) {
                                    data.insert(patch.id, patched);
                                }
                            }
                            Mutate::Delete(del) => {
                                data.remove(&del.id);
                            }
                            Mutate::Select(_sel) => {
                                todo!("recover_data does not yet support Mutate::Select");
                            }
                        }
                    }
                }
                LogOp::Migrate(mig) => {
                    for action in mig.actions {
                        match action {
                            SchemaAction::AttributeCreate(_) => {}
                            SchemaAction::AttributeUpsert(_) => {}
                            SchemaAction::AttributeChangeType(_) => {
                                // TODO: should cast the type...
                            }
                            SchemaAction::AttributeCreateIndex(_) => {}
                            SchemaAction::AttributeDelete(spec) => {
                                for values in data.values_mut() {
                                    values.0.remove(&spec.name);
                                }
                            }
                            SchemaAction::EntityCreate(_) => {}
                            SchemaAction::EntityAttributeAdd(spec) => {
                                if let Some(default) = spec.default_value {
                                    for values in data.values_mut() {
                                        if let Some(ty) = values
                                            .get(AttrType::QUALIFIED_NAME)
                                            .and_then(|v| v.as_str())
                                        {
                                            if ty != spec.entity {
                                                continue;
                                            }
                                        }
                                        values.insert(spec.attribute.clone(), default.clone());
                                    }
                                }
                            }
                            SchemaAction::EntityAttributeChangeCardinality(_) => {
                                // TODO: change type?
                                // (not currently done in backend anyway)
                            }
                            SchemaAction::EntityUpsert(_) => {}
                            SchemaAction::EntityDelete(del) => {
                                if del.delete_all {
                                    data.retain(|_id, values| {
                                        if let Some(ty) = values
                                            .get(AttrType::QUALIFIED_NAME)
                                            .and_then(|v| v.as_str())
                                        {
                                            ty == del.name
                                        } else {
                                            false
                                        }
                                    });
                                }
                            }
                            SchemaAction::IndexCreate(_) => {}
                            SchemaAction::IndexDelete(_) => {}
                            SchemaAction::EntityAttributeRemove(rem) => {
                                if rem.delete_values {
                                    for values in data.values_mut() {
                                        if let Some(ty) = values
                                            .get(AttrType::QUALIFIED_NAME)
                                            .and_then(|v| v.as_str())
                                        {
                                            if ty == rem.entity_type {
                                                values.remove(&rem.attribute);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Now try to order data so that entities that reference other entities
        // come before the referenced ones.
        // This is useful to enable re-importing into new stores.

        let mut available = HashSet::<Id>::new();

        let mut items = Vec::new();

        while !data.is_empty() {
            let mut removed = Vec::new();
            for (id, values) in data.iter() {
                let needs_more = find_ids(values).iter().any(|id| !available.contains(id));
                if !needs_more {
                    available.insert(*id);
                    items.push(values.clone());
                    removed.push(*id);
                }
            }

            if removed.is_empty() {
                // Could not make any more progress.
                break;
            }
            for id in &removed {
                data.remove(id);
            }
        }

        // Append the remaining entities that could not be ordered by references.
        for values in data.values() {
            items.push(values.clone());
        }

        Ok(items)
    }

    /// Export all events in the log.
    ///
    /// The provided callback will be invoked for each event.
    ///
    /// WARNING: Locks the database until all events are read!
    pub async fn export_events(
        &self,
        mut writer: impl FnMut(LogEvent) -> Result<(), AnyError>,
    ) -> Result<(), AnyError> {
        let state = self.state.mutable.lock().await;

        for event_id in 0..=state.current_event_id {
            if let Some(event) = state.store.read_event(event_id).await? {
                writer(event)?;
            }
        }

        Ok(())
    }

    async fn restore(&self) -> Result<(), AnyError> {
        tracing::debug!("log restore started");
        let mut mutable = self.state.mutable.lock().await;

        self.state.mem.write().unwrap().purge_all_data();

        let mut migrations = Vec::new();

        let mut event_id = 0;
        {
            let mut stream = mutable.store.iter_events(0, EventId::MAX).await?;

            while let Some(res) = stream.next().await {
                let event = res?;
                event_id = event.id;

                tracing::trace!(?event, "restoring logdb event");

                match event.op {
                    LogOp::Batch(batch) => {
                        self.state
                            .mem
                            .write()
                            .unwrap()
                            .apply_batch(batch)
                            .context(format!(
                                "Could not apply event '{}' to memory state",
                                event_id
                            ))?;
                    }
                    LogOp::Migrate(migration) => {
                        self.state
                            .mem
                            .write()
                            .unwrap()
                            .migrate(migration.clone())
                            .context(format!(
                                "Could not apply event '{}' to memory state",
                                event_id
                            ))?;
                        migrations.push(migration);
                    }
                }
            }
        }

        mutable.migrations = migrations;
        mutable.current_event_id = event_id;

        tracing::debug!("log restore finished");

        Ok(())
    }

    /// Reset the in-memory state and rebuild from the log store.
    ///
    /// Primarily used for testing.
    pub async fn force_rebuild(&self) -> Result<(), AnyError> {
        self.restore().await?;
        Ok(())
    }

    async fn write_event(
        &self,
        mutable: &mut MutableState,
        event: LogEvent,
    ) -> Result<(), AnyError> {
        mutable.store.write_event(event).await?;
        Ok(())
    }

    async fn write_event_revertable(
        &self,
        mutable: &mut MutableState,
        event: LogEvent,
        revert_epoch: RevertEpoch,
    ) -> Result<(), AnyError> {
        match self.write_event(mutable, event).await {
            Ok(_) => Ok(()),
            Err(err) => {
                self.state
                    .mem
                    .write()
                    .unwrap()
                    .revert_changes(revert_epoch)
                    .unwrap_or_else(|_| panic!("Consistency violation - could not revert changes after log write failure: {:?}", err));
                Err(err)
            }
        }
    }

    async fn migrate(
        self,
        migration: query::migrate::Migration,
        is_internal: bool,
    ) -> Result<(), AnyError> {
        if let Some(name) = &migration.name {
            // Ensure name uniqueness.
            let state = self.state.mutable.lock().await;

            let name_exists = state
                .migrations
                .iter()
                .filter_map(|m| m.name.as_ref())
                .any(|n| n == name);
            if name_exists {
                anyhow::bail!("Duplicate migration name: '{}'", name);
            }
        }

        // First, check if the migration would actually change anything.
        // If not, we do not write it.
        // This is important to not spam the log with migrations when UPSERTS
        // happen.
        let mut reg = self.state.registry.read().unwrap().clone();
        let (mig, ops) =
            crate::schema_builder::build_migration(&mut reg, migration.clone(), is_internal)?;

        if ops.is_empty() && mig.actions.is_empty() {
            return Ok(());
        }

        let mut mutable = self.state.mutable.lock().await;
        let revert_epoch = self
            .state
            .mem
            .write()
            .unwrap()
            .migrate_revertable(migration.clone())?;

        let event = LogEvent {
            id: mutable.increment_event_id(),
            op: LogOp::Migrate(migration),
        };
        self.write_event_revertable(&mut mutable, event, revert_epoch)
            .await?;

        *self.state.registry.write().unwrap() = reg;
        Ok(())
    }

    async fn apply_batch(self, batch: Batch) -> Result<(), AnyError> {
        let mut mutable = self.state.mutable.lock().await;
        let revert_epoch = self
            .state
            .mem
            .write()
            .unwrap()
            .apply_batch_revertable(batch.clone())?;

        let event = LogEvent {
            id: mutable.increment_event_id(),
            op: LogOp::Batch(batch),
        };
        self.write_event_revertable(&mut mutable, event, revert_epoch)
            .await?;

        Ok(())
    }
}

impl Backend for LogDb {
    fn registry(&self) -> &registry::SharedRegistry {
        &self.state.registry
    }

    fn entity(&self, id: data::IdOrIdent) -> BackendFuture<Option<data::DataMap>> {
        let res = self.state.mem.read().unwrap().entity_opt(id);
        ready(res).boxed()
    }

    fn select(
        &self,
        query: query::select::Select,
    ) -> super::BackendFuture<query::select::Page<Item>> {
        let res = self.state.mem.read().unwrap().select(query);
        ready(res).boxed()
    }

    fn select_map(&self, query: query::select::Select) -> BackendFuture<Vec<DataMap>> {
        let res = self.state.mem.read().unwrap().select_map(query);
        ready(res).boxed()
    }

    fn apply_batch(&self, batch: Batch) -> super::BackendFuture<()> {
        self.clone().apply_batch(batch).boxed()
    }

    fn migrate(&self, migration: query::migrate::Migration) -> super::BackendFuture<()> {
        self.clone().migrate(migration, false).boxed()
    }

    fn purge_all_data(&self) -> super::BackendFuture<()> {
        let s = self.clone();
        async move {
            let mut mutable = s.state.mutable.lock().await;
            mutable.store.clear().await?;
            mutable.current_event_id = 0;
            // FIXME: handle a failed purge by tainting the state and
            // rejecting all usage.
            s.state.mem.write().unwrap().purge_all_data();
            Ok(())
        }
        .boxed()
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    fn migrations(&self) -> BackendFuture<Vec<query::migrate::Migration>> {
        let s = self.clone();
        async move { Ok(s.state.mutable.lock().await.migrations.clone()) }.boxed()
    }

    fn memory_usage(&self) -> BackendFuture<Option<u64>> {
        ready(Ok(None)).boxed()
    }

    fn storage_usage(&self) -> BackendFuture<Option<u64>> {
        let s = self.clone();
        async move {
            let mut m = s.state.mutable.lock().await;
            let size = m.store.size_log().await?;
            Ok(size)
        }
        .boxed()
    }
}

/// Defines a storage backend used by a [LogStore].
pub trait LogStore {
    fn as_any(&self) -> &dyn std::any::Any;

    /// Iterate over the event log.
    /// use until: EventId::MAX to read until the end.
    fn iter_events(
        &self,
        from: EventId,
        until: EventId,
    ) -> BoxFuture<'_, Result<BoxStream<'_, Result<LogEvent, AnyError>>, AnyError>>;

    /// Read a single event.
    fn read_event(&self, id: EventId) -> BoxFuture<Result<Option<LogEvent>, AnyError>>;

    /// Write an event to the log.
    /// Returns the event id.
    /// Note that this required mutable access
    fn write_event(&mut self, event: LogEvent) -> BoxFuture<'_, Result<(), AnyError>>;

    /// Delete all events.
    fn clear(&mut self) -> BoxFuture<'_, Result<(), AnyError>>;

    /// Get the full size of the log in bytes.
    fn size_log(&mut self) -> BoxFuture<'static, Result<Option<u64>, AnyError>>;

    /// Get the full size of log entries.
    /// This differs from [`Self::size_log`] since it does not include log
    /// overhead or redundant/overwritten data.
    fn size_data(&mut self) -> BoxFuture<'static, Result<Option<u64>, AnyError>>;
}

/// De/serialier for a [LogStore].
pub trait LogConverter: Clone + Send + Sync + 'static {
    fn serialize(&self, event: &LogEvent) -> Result<Vec<u8>, AnyError>;
    fn deserialize(&self, data: &[u8]) -> Result<LogEvent, AnyError>;
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use factordb::{
        map,
        prelude::{AttrMapExt, Id},
        schema,
    };

    use crate::Engine;

    use super::*;

    #[test]
    fn test_log_backend_with_memory_store() {
        let log = futures::executor::block_on(async {
            LogDb::open(store_memory::MemoryLogStore::new())
                .await
                .unwrap()
        });
        crate::tests::test_backend(log, |f| futures::executor::block_on(f));
    }

    #[tokio::test]
    async fn test_log_backend_with_memory_store_restore() {
        // Test that restores work.
        let log = LogDb::open(store_memory::MemoryLogStore::new())
            .await
            .unwrap();
        let db = crate::Engine::new(log.clone()).into_client();

        let mig = query::migrate::Migration {
            name: None,
            actions: vec![query::migrate::SchemaAction::AttributeCreate(
                query::migrate::AttributeCreate {
                    schema: schema::Attribute::new("test/text", data::ValueType::String),
                },
            )],
        };
        db.migrate(mig).await.unwrap();

        let id = Id::random();
        db.create(
            id,
            map! {
                "test/text": "hello",
            },
        )
        .await
        .unwrap();

        let data = db.entity(id).await.unwrap();
        assert_eq!(data::Value::from("hello"), data["test/text"]);

        // Restore.
        log.restore().await.unwrap();

        // Test that data is still there.
        let data = db.entity(id).await.unwrap();
        assert_eq!(data::Value::from("hello"), data["test/text"]);
    }

    #[tokio::test]
    async fn test_log_backend_with_memory_store_export() {
        let log = LogDb::open(store_memory::MemoryLogStore::new())
            .await
            .unwrap();
        let db = Engine::new(log.clone()).into_client();

        let id = Id::random();
        let data = map! {
            "factor/title": "y",
        };
        db.create(id, data.clone()).await.unwrap();

        db.delete(id).await.unwrap();

        let mut events = Vec::new();

        // Restore.
        log.export_events(|event| {
            events.push(event);
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(
            events,
            vec![
                LogEvent {
                    id: 1,
                    op: LogOp::Batch(Batch {
                        actions: vec![query::mutate::Mutate::Create(query::mutate::Create {
                            id,
                            data
                        }),]
                    })
                },
                LogEvent {
                    id: 2,
                    op: LogOp::Batch(Batch {
                        actions: vec![query::mutate::Mutate::Delete(query::mutate::Delete { id }),]
                    })
                }
            ]
        );
    }

    #[tokio::test]
    async fn test_log_backend_recover_data() {
        let id1 = Id::from_str("00000000-0000-0000-1000-000000000000").unwrap();
        let id2 = Id::from_str("00000000-0000-0000-2000-000000000000").unwrap();
        let id3 = Id::from_str("00000000-0000-0000-3000-000000000000").unwrap();

        let mem = store_memory::MemoryLogStore::new();

        {
            let log = LogDb::open(mem.clone()).await.unwrap();
            let db = Engine::new(log.clone()).into_client();

            let data1 = map! {
                "factor/title": "y",
            };
            db.create(id1, data1).await.unwrap();

            let data2 = map! {
                "factor/title": "y",
                "factor/description": id2.to_string(),
            };
            db.create(id2, data2).await.unwrap();

            let data3 = map! {
                "factor/title": "y",
                "factor/description": id3.to_string(),
            };
            db.create(id3, data3).await.unwrap();
        }

        let mut restored = LogDb::recover_data(mem).await.unwrap();
        restored.sort_by_key(|a| a.get_id().unwrap());

        assert_eq!(3, restored.len());
        assert_eq!(id1, restored[0].get_id().unwrap());
        assert_eq!(id2, restored[1].get_id().unwrap());
        assert_eq!(id3, restored[2].get_id().unwrap());
    }
}
