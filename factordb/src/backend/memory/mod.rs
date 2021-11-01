mod index;
mod interner;
mod memory_data;
pub mod store;

use futures::{future::ready, FutureExt};

use crate::{
    data,
    query::{self, select::Item},
};

use super::BackendFuture;

#[derive(Clone)]
pub struct MemoryDb {
    registry: crate::registry::SharedRegistry,
    state: std::sync::Arc<std::sync::RwLock<store::MemoryStore>>,
}

impl MemoryDb {
    pub fn new() -> Self {
        let registry = crate::registry::Registry::new().into_shared();
        let s = Self {
            registry: registry.clone(),
            state: std::sync::Arc::new(std::sync::RwLock::new(store::MemoryStore::new(registry))),
        };

        s
    }
}

// fn memory_to_id_map(mem: &MemoryTuple) -> IdMap {
//     mem.iter()
//         .map(|(key, value)| (*key, value.into()))
//         .collect()
// }

impl super::Backend for MemoryDb {
    fn registry(&self) -> &crate::registry::SharedRegistry {
        &self.registry
    }

    fn purge_all_data(&self) -> BackendFuture<()> {
        self.state.write().unwrap().purge_all_data();
        ready(Ok(())).boxed()
    }

    fn entity(&self, id: data::Ident) -> super::BackendFuture<data::DataMap> {
        let res = self.state.read().unwrap().entity(id);
        ready(res).boxed()
    }

    fn select(&self, query: query::select::Select) -> BackendFuture<query::select::Page<Item>> {
        let res = self.state.read().unwrap().select(query);
        ready(res).boxed()
    }

    fn apply_batch(&self, batch: query::mutate::BatchUpdate) -> BackendFuture<()> {
        let res = self.state.write().unwrap().apply_batch(batch);
        ready(res).boxed()
    }

    fn migrate(&self, migration: query::migrate::Migration) -> super::BackendFuture<()> {
        let res = self.state.write().unwrap().migrate(migration).map(|_| ());
        ready(res).boxed()
    }

    fn migrations(&self) -> BackendFuture<Vec<query::migrate::Migration>> {
        // TODO: keep track of migrations!?
        ready(Ok(Vec::new())).boxed()
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
