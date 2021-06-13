use std::sync::Arc;

use data::DataMap;

use crate::{
    backend::Backend,
    data::{self, Id, Ident},
    query, AnyError,
};

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
    ) -> Result<query::select::Page<DataMap>, AnyError> {
        self.backend.select(query).await
    }

    pub async fn create(&self, id: Id, data: DataMap) -> Result<(), AnyError> {
        self.batch(query::update::BatchUpdate {
            actions: vec![query::update::Update::Create(query::update::Create {
                id,
                data,
            })],
        })
        .await
    }

    pub async fn replace(&self, id: Id, data: DataMap) -> Result<(), AnyError> {
        self.batch(query::update::BatchUpdate {
            actions: vec![query::update::Update::Replace(query::update::Replace {
                id,
                data,
            })],
        })
        .await
    }

    pub async fn patch(&self, id: Id, data: DataMap) -> Result<(), AnyError> {
        self.batch(query::update::BatchUpdate {
            actions: vec![query::update::Update::Patch(query::update::Patch {
                id,
                data,
            })],
        })
        .await
    }

    pub async fn delete(&self, id: Id) -> Result<(), AnyError> {
        self.batch(query::update::BatchUpdate {
            actions: vec![query::update::Update::Delete(query::update::Delete { id })],
        })
        .await
    }

    pub async fn batch(&self, batch: query::update::BatchUpdate) -> Result<(), AnyError> {
        self.backend.apply_batch(batch).await
    }

    pub async fn migrate(&self, migration: query::migrate::Migration) -> Result<(), AnyError> {
        self.backend.migrate(migration).await
    }

    pub async fn purge_all_data(&self) -> Result<(), AnyError> {
        self.backend.purge_all_data().await
    }
}
