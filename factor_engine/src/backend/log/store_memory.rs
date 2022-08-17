use std::sync::{Arc, RwLock};

use futures::{
    future::{ready, BoxFuture},
    stream::BoxStream,
    FutureExt, StreamExt,
};

use super::{EventId, LogEvent};

/// Mock memory log store.
/// Only useful for testing.
#[derive(Clone)]
pub struct MemoryLogStore {
    events: Arc<RwLock<std::collections::BTreeMap<super::EventId, LogEvent>>>,
}

impl MemoryLogStore {
    pub fn new() -> Self {
        Self {
            events: Default::default(),
        }
    }

    pub fn duplicate(&self) -> Self {
        Self {
            events: self.events.clone(),
        }
    }
}

impl Default for MemoryLogStore {
    fn default() -> Self {
        Self::new()
    }
}

type StreamFuture<'a> =
    BoxFuture<'a, Result<BoxStream<'a, Result<LogEvent, anyhow::Error>>, anyhow::Error>>;

impl super::LogStore for MemoryLogStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn iter_events(&self, from: super::EventId, until: super::EventId) -> StreamFuture {
        let stream = self
            .events
            .read()
            .unwrap()
            .range(from..until)
            .map(|(_key, value)| Ok(value.clone()))
            .collect::<Vec<_>>();
        let boxed_stream = futures::stream::iter(stream).boxed();
        let res = Ok(boxed_stream);
        ready(res).boxed()
    }

    fn read_event(&self, id: EventId) -> BoxFuture<Result<Option<LogEvent>, anyhow::Error>> {
        let res = self
            .events
            .read()
            .unwrap()
            .get(&id)
            .cloned()
            .map(Ok)
            .transpose();
        ready(res).boxed()
    }

    fn write_event(&mut self, event: LogEvent) -> BoxFuture<Result<(), anyhow::Error>> {
        let mut events = self.events.write().unwrap();

        let expected_id = events.len() as u64 + 1;

        let res = if event.id != expected_id {
            Err(anyhow::anyhow!(
                "Event id mismatch - expected {}, got {}",
                expected_id,
                event.id,
            ))
        } else {
            events.insert(event.id, event);
            Ok(())
        };

        ready(res).boxed()
    }

    fn clear(&mut self) -> BoxFuture<'static, Result<(), anyhow::Error>> {
        self.events.write().unwrap().clear();
        ready(Ok(())).boxed()
    }

    fn size_log(&mut self) -> BoxFuture<'static, Result<Option<u64>, anyhow::Error>> {
        ready(Ok(None)).boxed()
    }

    fn size_data(&mut self) -> BoxFuture<'static, Result<Option<u64>, anyhow::Error>> {
        ready(Ok(None)).boxed()
    }
}
