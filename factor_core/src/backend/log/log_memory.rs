use futures::{
    future::{ready, BoxFuture},
    stream::BoxStream,
    FutureExt, StreamExt,
};

use crate::AnyError;

use super::EventId;

/// Mock memory log store.
/// Only useful for testing.
pub struct MemoryLogStore {
    events: std::collections::BTreeMap<super::EventId, Vec<u8>>,
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

type StreamFuture<'a> = BoxFuture<'a, Result<BoxStream<'a, Result<Vec<u8>, AnyError>>, AnyError>>;

impl super::LogStore for MemoryLogStore {
    fn iter_events(&self, from: super::EventId, until: super::EventId) -> StreamFuture {
        let stream = self
            .events
            .range(from..until)
            .map(|(_key, value)| Ok(value.clone()));
        let boxed_stream = futures::stream::iter(stream).boxed();
        let res = Ok(boxed_stream);
        ready(res).boxed()
    }

    fn read_event(&self, id: EventId) -> BoxFuture<Result<Option<Vec<u8>>, AnyError>> {
        let res = self.events.get(&id).cloned().map(Ok).transpose();
        ready(res).boxed()
    }

    fn write_event(&mut self, id: EventId, event: Vec<u8>) -> BoxFuture<Result<EventId, AnyError>> {
        let expected_id = self.events.len() as u64 + 1;

        let res = if id != expected_id {
            Err(anyhow::anyhow!(
                "Event id mismatch - expected {}, got {}",
                expected_id,
                id,
            ))
        } else {
            self.events.insert(id, event);
            Ok(id)
        };

        ready(res).boxed()
    }

    fn clear(&mut self) -> BoxFuture<'static, Result<(), AnyError>> {
        self.events.clear();
        ready(Ok(())).boxed()
    }
}
