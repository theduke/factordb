//! A simple event log based db backend.
//! See [LogDb] for details.

pub mod convert_json;
pub mod log_memory;

mod event;
pub use event::LogEvent;

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
    query::{self, select::Item},
    registry, schema, AnyError,
};

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
}

impl MutableState {
    fn increment_event_id(&mut self) -> EventId {
        self.current_event_id = self.current_event_id.wrapping_add(1);
        self.current_event_id
    }
}

impl LogDb {
    pub async fn open<S>(store: S) -> Result<Self, AnyError>
    where
        S: LogStore + Send + Sync + 'static,
    {
        let registry = registry::Registry::new().into_shared();
        let state = State {
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
        {
            let mut mutable = self.state.mutable.lock().await;

            self.state.mem.write().unwrap().purge_all_data();

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
                    .expect(&format!("Consistency violation - could not revert changes after log write failure: {:?}", err));
                Err(err)
            }
        }
    }

    async fn migrate(
        self,
        migration: query::migrate::Migration,
        is_internal: bool,
    ) -> Result<(), AnyError> {
        // First, check if the migration would actually change anything.
        // If not, we do not write it.
        // This is important to not spam the log with migrations when UPSERTS
        // happen.
        let mut reg = self.state.registry.read().unwrap().clone();
        let (mig, ops) = schema::logic::build_migration(&mut reg, migration.clone(), is_internal)?;

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
        Some(&*self)
    }
}

/// Defines a storage backend used by a [LogStore].
pub trait LogStore {
    /// Iterate over the event log.
    /// use until: EventId::MAX to read until the end.
    fn iter_events(
        &self,
        from: EventId,
        until: EventId,
    ) -> BoxFuture<Result<BoxStream<Result<LogEvent, AnyError>>, AnyError>>;

    /// Read a single event.
    fn read_event(&self, id: EventId) -> BoxFuture<Result<Option<LogEvent>, AnyError>>;

    /// Write an event to the log.
    /// Returns the event id.
    /// Note that this required mutable access
    fn write_event(&mut self, event: LogEvent) -> BoxFuture<Result<(), AnyError>>;

    /// Delete all events.
    fn clear(&mut self) -> BoxFuture<'static, Result<(), AnyError>>;
}

/// De/serialier for a [LogStore].
pub trait LogConverter: Clone {
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
            LogDb::open(log_memory::MemoryLogStore::new())
                .await
                .unwrap()
        });
        crate::tests::test_backend(log, |f| futures::executor::block_on(f));
    }

    #[test]
    fn test_log_backend_with_memory_store_restore() {
        // Test that restores work.
        futures::executor::block_on(async {
            let log = LogDb::open(log_memory::MemoryLogStore::new())
                .await
                .unwrap();
            let db = crate::Db::new(log.clone());

            let mig = query::migrate::Migration {
                actions: vec![query::migrate::SchemaAction::AttributeCreate(
                    query::migrate::AttributeCreate {
                        schema: schema::AttributeSchema::new(
                            "test",
                            "text",
                            crate::data::ValueType::String,
                        ),
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

    #[test]
    fn test_log_backend_with_memory_store_export() {
        futures::executor::block_on(async {
            let log = LogDb::open(log_memory::MemoryLogStore::new())
                .await
                .unwrap();
            let db = crate::Db::new(log.clone());

            let id = Id::random();
            let data = crate::map! {
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
                        op: LogOp::Batch(BatchUpdate {
                            actions: vec![query::mutate::Mutate::Create(query::mutate::Create {
                                id,
                                data
                            }),]
                        })
                    },
                    LogEvent {
                        id: 2,
                        op: LogOp::Batch(BatchUpdate {
                            actions: vec![query::mutate::Mutate::Delete(query::mutate::Delete {
                                id
                            }),]
                        })
                    }
                ]
            );
        });
    }
}
