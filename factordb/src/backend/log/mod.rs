//! A simple event log based db backend.
//! See [LogDb] for details.

pub mod convert_json;
pub mod log_memory;

use std::sync::{Arc, RwLock};

use anyhow::Context;
use futures::{
    future::{ready, BoxFuture},
    stream::BoxStream,
    FutureExt, StreamExt,
};
use query::mutate::BatchUpdate;

use crate::{
    data,
    query::{self, migrate::Migration, select::Item},
    registry, AnyError,
};

use super::{
    memory::store::{MemoryStore, RevertEpoch},
    Backend, BackendFuture,
};

pub struct LogConfig {}

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

impl LogDb {
    pub async fn open<S, C>(store: S, converter: C) -> Result<Self, AnyError>
    where
        C: LogConverter + Send + Sync + 'static,
        S: LogStore + Send + Sync + 'static,
    {
        let registry = registry::Registry::new().into_shared();
        let state = State {
            converter: Box::new(converter),
            mem: RwLock::new(MemoryStore::new(registry.clone())),
            registry,
            mutable: futures::lock::Mutex::new(MutableState {
                store: Box::new(store),
                current_event_id: 0,
            }),
        };
        let s = Self {
            state: Arc::new(state),
        };
        s.restore().await?;
        Ok(s)
    }

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

    async fn restore(&self) -> Result<(), AnyError> {
        {
            let mut mutable = self.state.mutable.lock().await;

            self.state.mem.write().unwrap().purge_all_data();

            let mut event_id = 0;
            {
                let mut stream = mutable.store.iter_events(0, EventId::MAX).await?;

                while let Some(res) = stream.next().await {
                    let raw_event = res?;
                    let event = self
                        .state
                        .converter
                        .deserialize(raw_event)
                        .context("Could not deserialize event")?;
                    event_id = event.id;

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
                                .migrate(migration)
                                .context(format!(
                                    "Could not apply event '{}' to memory state",
                                    event_id
                                ))?;
                        }
                    }
                }
            }
            mutable.current_event_id = event_id;
        }
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
        let raw = self.state.converter.serialize(&event)?;
        let written_id = mutable.store.write_event(event.id, raw).await?;
        assert_eq!(written_id, event.id);
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
                    .expect(&format!("Consistency violation - could not revert changes after log write failure: {:?}", err));
                Err(err)
            }
        }
    }

    async fn migrate(self, migration: query::migrate::Migration) -> Result<(), AnyError> {
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
        Ok(())
    }

    async fn apply_batch(self, batch: BatchUpdate) -> Result<(), AnyError> {
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

    fn entity(&self, id: data::Ident) -> BackendFuture<data::DataMap> {
        let res = self.state.mem.read().unwrap().entity(id);
        ready(res).boxed()
    }

    fn select(
        &self,
        query: query::select::Select,
    ) -> super::BackendFuture<query::select::Page<Item>> {
        let res = self.state.mem.read().unwrap().select(query);
        ready(res).boxed()
    }

    fn apply_batch(&self, batch: BatchUpdate) -> super::BackendFuture<()> {
        self.clone().apply_batch(batch).boxed()
    }

    fn migrate(&self, migration: query::migrate::Migration) -> super::BackendFuture<()> {
        self.clone().migrate(migration).boxed()
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
        Some(&*self)
    }
}

struct State {
    registry: registry::SharedRegistry,
    converter: Box<dyn LogConverter + Send + Sync + 'static>,
    mutable: futures::lock::Mutex<MutableState>,
    mem: RwLock<MemoryStore>,
}

struct MutableState {
    store: Box<dyn LogStore + Send + Sync + 'static>,
    current_event_id: EventId,
}

impl MutableState {
    fn increment_event_id(&mut self) -> EventId {
        self.current_event_id = self.current_event_id.wrapping_add(1);
        self.current_event_id
    }
}

pub type EventId = u64;

/// A event persisted in the log.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LogEvent {
    id: EventId,
    op: LogOp,
}

impl LogEvent {
    // /// Get a reference to the log event's id.
    // fn id(&self) -> EventId {
    //     self.id
    // }

    // fn from_op(op: super::DbOp) -> Option<Self> {
    //     use super::{DbOp, TupleOp};
    //     match op {
    //         DbOp::Tuple(t) => match t {
    //             TupleOp::Create(_) => todo!(),
    //             TupleOp::Replace(_) => todo!(),
    //             TupleOp::Merge(_) => todo!(),
    //             TupleOp::Delete(_) => todo!(),
    //             TupleOp::RemoveAttrs(_) => todo!(),
    //         },
    //         DbOp::Select(_) => todo!(),
    //     }
    // }
}

/// A log operation stored in a log event.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
enum LogOp {
    Batch(BatchUpdate),
    Migrate(Migration),
}

/// Defines a storage backend used by a [LogStore].
pub trait LogStore {
    /// Iterate over the event log.
    /// use until: EventId::MAX to read until the end.
    fn iter_events(
        &self,
        from: EventId,
        until: EventId,
    ) -> BoxFuture<Result<BoxStream<Result<Vec<u8>, AnyError>>, AnyError>>;
    fn read_event(&self, id: EventId) -> BoxFuture<Result<Option<Vec<u8>>, AnyError>>;

    /// Write an event to the log.
    /// Returns the event id.
    /// Note that this required mutable access
    fn write_event(&mut self, id: EventId, event: Vec<u8>) -> BoxFuture<Result<EventId, AnyError>>;

    /// Delete all events.
    fn clear(&mut self) -> BoxFuture<'static, Result<(), AnyError>>;
}

/// De/serialier for a [LogStore].
pub trait LogConverter {
    fn serialize(&self, event: &LogEvent) -> Result<Vec<u8>, AnyError>;
    fn deserialize(&self, data: Vec<u8>) -> Result<LogEvent, AnyError>;
}

#[cfg(test)]
mod tests {
    use crate::{data::Id, schema};

    use super::*;

    #[test]
    fn test_log_backend_with_memory_store() {
        let log = futures::executor::block_on(async {
            LogDb::open(
                log_memory::MemoryLogStore::new(),
                convert_json::JsonConverter,
            )
            .await
            .unwrap()
        });
        crate::tests::test_backend(log, |f| futures::executor::block_on(f));
    }

    #[test]
    fn test_log_backend_with_memory_store_restore() {
        // Test that restores work.
        futures::executor::block_on(async {
            let log = LogDb::open(
                log_memory::MemoryLogStore::new(),
                convert_json::JsonConverter,
            )
            .await
            .unwrap();
            let db = crate::Db::new(log.clone());

            let mig = query::migrate::Migration {
                actions: vec![query::migrate::SchemaAction::AttributeCreate(
                    query::migrate::AttributeCreate {
                        schema: schema::AttributeSchema {
                            id: Id::nil(),
                            name: "test/text".into(),
                            description: None,
                            value_type: crate::data::ValueType::String,
                            unique: false,
                            index: false,
                            strict: true,
                        },
                    },
                )],
            };
            db.migrate(mig).await.unwrap();

            let id = Id::random();
            db.create(
                id,
                crate::map! {
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
        });
    }
}