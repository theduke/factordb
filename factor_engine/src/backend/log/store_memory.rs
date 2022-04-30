use futures::{
    future::{ready, BoxFuture},
    stream::BoxStream,
    FutureExt, StreamExt,
};

use factordb::AnyError;

use super::{EventId, LogEvent};

/// Mock memory log store.
/// Only useful for testing.
pub struct MemoryLogStore {
    events: std::collections::BTreeMap<super::EventId, LogEvent>,
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

type StreamFuture<'a> = BoxFuture<'a, Result<BoxStream<'a, Result<LogEvent, AnyError>>, AnyError>>;

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

    fn read_event(&self, id: EventId) -> BoxFuture<Result<Option<LogEvent>, AnyError>> {
        let res = self.events.get(&id).cloned().map(Ok).transpose();
        ready(res).boxed()
    }

    fn write_event(&mut self, event: LogEvent) -> BoxFuture<Result<(), AnyError>> {
        let expected_id = self.events.len() as u64 + 1;

        let res = if event.id != expected_id {
            Err(anyhow::anyhow!(
                "Event id mismatch - expected {}, got {}",
                expected_id,
                event.id,
            ))
        } else {
            self.events.insert(event.id, event);
            Ok(())
        };

        ready(res).boxed()
    }

    fn clear(&mut self) -> BoxFuture<'static, Result<(), AnyError>> {
        self.events.clear();
        ready(Ok(())).boxed()
    }

    fn size_log(&mut self) -> BoxFuture<'static, Result<Option<u64>, AnyError>> {
        ready(Ok(None)).boxed()
    }

    fn size_data(&mut self) -> BoxFuture<'static, Result<Option<u64>, AnyError>> {
        ready(Ok(None)).boxed()
    }
}
