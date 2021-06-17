use std::sync::Arc;

use data::DataMap;

use crate::{
    backend::Backend,
    data::{self, Id, Ident},
    query, AnyError,
};

#[derive(Clone)]
pub struct Db {
    backend: Arc<dyn Backend + Send + Sync + 'static>,
}

impl Db {
    pub fn new(backend: impl Backend + Sync + Send + 'static) -> Self {
        Self {
            backend: Arc::new(backend),
        }
    }

    pub async fn entity<I>(&self, id: I) -> Result<DataMap, AnyError>
    where
        I: Into<Ident>,
    {
        self.backend.entity(id.into()).await
    }

    pub async fn select(
        &self,
        query: query::select::Select,
    ) -> Result<query::select::Page<query::select::Item>, AnyError> {
        self.backend.select(query).await
    }

    pub async fn create(&self, id: Id, data: DataMap) -> Result<(), AnyError> {
        dbg!(self.backend.registry());
        self.batch(query::mutate::BatchUpdate {
            actions: vec![query::mutate::Mutate::Create(query::mutate::Create {
                id,
                data,
            })],
        })
        .await
    }

    pub async fn replace(&self, id: Id, data: DataMap) -> Result<(), AnyError> {
        self.batch(query::mutate::BatchUpdate {
            actions: vec![query::mutate::Mutate::Replace(query::mutate::Replace {
                id,
                data,
            })],
        })
        .await
    }

    pub async fn merge(&self, id: Id, data: DataMap) -> Result<(), AnyError> {
        self.batch(query::mutate::BatchUpdate {
            actions: vec![query::mutate::Mutate::Merge(query::mutate::Merge {
                id,
                data,
            })],
        })
        .await
    }

    pub async fn delete(&self, id: Id) -> Result<(), AnyError> {
        self.batch(query::mutate::BatchUpdate {
            actions: vec![query::mutate::Mutate::Delete(query::mutate::Delete { id })],
        })
        .await
    }

    pub async fn batch(&self, batch: query::mutate::BatchUpdate) -> Result<(), AnyError> {
        self.backend.apply_batch(batch).await
    }

    pub async fn migrate(&self, migration: query::migrate::Migration) -> Result<(), AnyError> {
        self.backend.migrate(migration).await
    }

    pub async fn purge_all_data(&self) -> Result<(), AnyError> {
        self.backend.purge_all_data().await
    }
}
