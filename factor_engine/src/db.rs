use std::sync::Arc;

use data::DataMap;

use crate::backend::Backend;
use factordb::{data, prelude::IdOrIdent, query, schema, AnyError};

#[derive(Clone)]
pub struct Engine {
    backend: Arc<dyn Backend + Send + Sync + 'static>,
}

impl Engine {
    pub fn new(backend: impl Backend + Sync + Send + 'static) -> Self {
        Self {
            backend: Arc::new(backend),
        }
    }

    pub fn into_client(self) -> factordb::db::Db {
        factordb::db::Db::new(self)
    }

    pub fn backend(&self) -> &Arc<dyn Backend + Send + Sync + 'static> {
        &self.backend
    }

    pub fn schema(&self) -> Result<schema::DbSchema, AnyError> {
        let reg = {
            self.backend()
                .registry()
                .read()
                .map_err(|_| AnyError::msg("Could not retrieve registry"))?
                .clone()
        };

        Ok(reg.build_schema())
    }

    pub async fn entity(&self, id: IdOrIdent) -> Result<Option<DataMap>, AnyError> {
        self.backend.entity(id).await
    }

    pub async fn select(
        &self,
        query: query::select::Select,
    ) -> Result<query::select::Page<query::select::Item>, AnyError> {
        self.backend.select(query).await
    }

    pub async fn batch(&self, batch: query::mutate::Batch) -> Result<(), AnyError> {
        self.backend.apply_batch(batch).await
    }

    pub async fn migrate(&self, migration: query::migrate::Migration) -> Result<(), AnyError> {
        self.backend.migrate(migration).await
    }

    pub async fn purge_all_data(&self) -> Result<(), AnyError> {
        self.backend.purge_all_data().await
    }
}

impl factordb::db::DbClient for Engine {
    fn schema(&self) -> factordb::db::DbFuture<'_, factordb::schema::DbSchema> {
        Box::pin(futures::future::ready(self.schema()))
    }

    fn entity(&self, id: IdOrIdent) -> factordb::db::DbFuture<'_, Option<DataMap>> {
        Box::pin(async { self.entity(id).await })
    }

    fn select(
        &self,
        query: query::select::Select,
    ) -> factordb::db::DbFuture<'_, query::select::Page<query::select::Item>> {
        Box::pin(async { self.select(query).await })
    }

    fn batch(&self, batch: factordb::prelude::Batch) -> factordb::db::DbFuture<'_, ()> {
        Box::pin(async { self.batch(batch).await })
    }

    fn migrate(&self, migration: query::migrate::Migration) -> factordb::db::DbFuture<'_, ()> {
        Box::pin(async { self.migrate(migration).await })
    }

    fn purge_all_data(&self) -> factordb::db::DbFuture<'_, ()> {
        Box::pin(async { self.purge_all_data().await })
    }
}
