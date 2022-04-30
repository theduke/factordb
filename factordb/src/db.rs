use std::sync::Arc;

use crate::{
    data::{patch::Patch, DataMap, Id, IdOrIdent},
    query::{
        self,
        mutate::{Batch, Mutate},
    },
    schema::EntityContainer,
    AnyError,
};

#[derive(Clone)]
pub struct Db {
    client: Arc<dyn DbClient + Send + Sync + 'static>,
}

impl Db {
    pub fn new<D>(client: D) -> Self
    where
        D: DbClient + Sync + Send + 'static,
    {
        Self {
            client: Arc::new(client),
        }
    }

    pub fn client(&self) -> &Arc<dyn DbClient + Send + Sync + 'static> {
        &self.client
    }

    /// Retrieve the full database schema.
    pub async fn schema(&self) -> Result<crate::schema::DbSchema, AnyError> {
        self.client.schema().await
    }

    /// Select a single entity by its id or ident.
    pub async fn entity<I>(&self, id: I) -> Result<DataMap, AnyError>
    where
        I: Into<IdOrIdent>,
    {
        use query::expr::Expr;

        // FIXME: remove this once index persistence logic is implemented.
        match id.into() {
            IdOrIdent::Id(id) => {
                self.client.entity(id.into()).await?.ok_or_else(|| {
                    anyhow::Error::from(crate::error::EntityNotFound::new(id.into()))
                })
            }
            IdOrIdent::Name(name) => {
                let sel = query::select::Select::new()
                    .with_limit(1)
                    .with_filter(Expr::eq(
                        Expr::attr::<crate::schema::builtin::AttrIdent>(),
                        Expr::literal(name.as_ref()),
                    ));
                let mut page = self.select(sel).await?;
                page.items
                    .pop()
                    .map(|item| item.data)
                    .ok_or_else(|| crate::error::EntityNotFound::new(name.as_ref().into()).into())
            }
        }
    }

    /// Query entities.
    pub async fn select(
        &self,
        query: query::select::Select,
    ) -> Result<query::select::Page<query::select::Item>, AnyError> {
        self.client.select(query).await
    }

    // Mutate.

    pub async fn batch(&self, batch: Batch) -> Result<(), AnyError> {
        self.client.batch(batch).await
    }

    pub async fn create(&self, id: Id, data: DataMap) -> Result<(), AnyError> {
        self.batch(Mutate::create(id, data).into()).await
    }

    pub async fn create_entity<E: EntityContainer + serde::Serialize>(
        &self,
        entity: E,
    ) -> Result<(), AnyError> {
        let id = entity.id();
        let data = entity.into_map()?;
        self.create(id, data).await
    }

    pub async fn replace(&self, id: Id, data: DataMap) -> Result<(), AnyError> {
        self.batch(Mutate::replace(id, data).into()).await
    }

    pub async fn merge(&self, id: Id, data: DataMap) -> Result<(), AnyError> {
        self.batch(Mutate::merge(id, data).into()).await
    }

    pub async fn patch(&self, id: Id, patch: Patch) -> Result<(), AnyError> {
        self.batch(Mutate::patch(id, patch).into()).await
    }

    pub async fn delete(&self, id: Id) -> Result<(), AnyError> {
        self.batch(Mutate::delete(id).into()).await
    }

    /// Run a migration.
    pub async fn migrate(&self, migration: query::migrate::Migration) -> Result<(), AnyError> {
        self.client.migrate(migration).await
    }

    /// Delete all data.
    pub async fn purge_all_data(&self) -> Result<(), AnyError> {
        self.client.purge_all_data().await
    }
}

pub type DbFuture<'a, T> = futures::future::BoxFuture<'a, Result<T, anyhow::Error>>;

pub trait DbClient {
    fn schema(&self) -> DbFuture<'_, crate::schema::DbSchema>;
    fn entity(&self, id: IdOrIdent) -> DbFuture<'_, Option<DataMap>>;
    fn select(
        &self,
        query: query::select::Select,
    ) -> DbFuture<'_, query::select::Page<query::select::Item>>;
    fn batch(&self, batch: Batch) -> DbFuture<'_, ()>;
    fn migrate(&self, migration: query::migrate::Migration) -> DbFuture<'_, ()>;
    fn purge_all_data(&self) -> DbFuture<'_, ()>;
}
