use std::sync::Arc;

use crate::{
    data::{patch::Patch, DataMap, Id, IdOrIdent},
    error::EntityNotFound,
    query::{
        self,
        migrate::Migration,
        mutate::{Batch, Mutate},
        select::Page,
    },
    schema::{self, ClassContainer},
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
    pub async fn schema(&self) -> Result<schema::DbSchema, anyhow::Error> {
        self.client.schema().await
    }

    /// Select a single entity by its id or ident.
    pub async fn entity<I>(&self, id: I) -> Result<DataMap, anyhow::Error>
    where
        I: Into<IdOrIdent>,
    {
        use query::expr::Expr;

        // FIXME: remove this once index persistence logic is implemented.
        match id.into() {
            IdOrIdent::Id(id) => self
                .client
                .entity(id.into())
                .await?
                .ok_or_else(|| anyhow::Error::from(EntityNotFound::new(id.into()))),
            IdOrIdent::Name(name) => {
                let sel = query::select::Select::new()
                    .with_limit(1)
                    .with_filter(Expr::eq(
                        Expr::attr::<schema::builtin::AttrIdent>(),
                        Expr::literal(name.as_ref()),
                    ));
                let mut page = self.select(sel).await?;
                page.items
                    .pop()
                    .map(|item| item.data)
                    .ok_or_else(|| EntityNotFound::new(name.as_ref().into()).into())
            }
        }
    }

    /// Query entities.
    pub async fn select(
        &self,
        query: query::select::Select,
    ) -> Result<query::select::Page<query::select::Item>, anyhow::Error> {
        self.client.select(query).await
    }

    pub async fn select_map(
        &self,
        query: query::select::Select,
    ) -> Result<Vec<DataMap>, anyhow::Error> {
        self.client.select_map(query).await
    }

    // Mutate.

    pub async fn batch(&self, batch: Batch) -> Result<(), anyhow::Error> {
        self.client.batch(batch).await
    }

    pub async fn create(&self, id: Id, data: DataMap) -> Result<(), anyhow::Error> {
        self.batch(Mutate::create(id, data).into()).await
    }

    pub async fn create_entity<E: ClassContainer + serde::Serialize>(
        &self,
        entity: E,
    ) -> Result<(), anyhow::Error> {
        let id = entity.id();
        let data = entity.into_map()?;
        self.create(id, data).await
    }

    pub async fn mutate(&self, mutate: Mutate) -> Result<(), anyhow::Error> {
        self.batch(mutate.into()).await
    }

    pub async fn replace(&self, id: Id, data: DataMap) -> Result<(), anyhow::Error> {
        self.batch(Mutate::replace(id, data).into()).await
    }

    pub async fn merge(&self, id: Id, data: DataMap) -> Result<(), anyhow::Error> {
        self.batch(Mutate::merge(id, data).into()).await
    }

    pub async fn patch(&self, id: Id, patch: Patch) -> Result<(), anyhow::Error> {
        self.batch(Mutate::patch(id, patch).into()).await
    }

    pub async fn delete(&self, id: Id) -> Result<(), anyhow::Error> {
        self.batch(Mutate::delete(id).into()).await
    }

    /// Execute a SQL statement.
    ///
    /// Supported statements are SELECT, UPDATE and DELETE.
    pub async fn sql(
        &self,
        sql: String,
    ) -> Result<query::select::Page<query::select::Item>, anyhow::Error> {
        match crate::query::sql::parse_sql(&sql)? {
            query::sql::ParsedSqlQuery::Select(sel) => self.select(sel).await,
            query::sql::ParsedSqlQuery::Mutate(m) => {
                self.mutate(Mutate::Select(m)).await?;
                // TODO: support selections/returning?
                Ok(Page::new())
            }
        }
    }

    /// Run a migration.
    pub async fn migrate(&self, migration: query::migrate::Migration) -> Result<(), anyhow::Error> {
        self.client.migrate(migration).await
    }

    pub async fn migrations(&self) -> Result<Vec<Migration>, anyhow::Error> {
        self.client.migrations().await
    }

    pub async fn storage_usage(&self) -> Result<Option<u64>, anyhow::Error> {
        self.client.storage_usage().await
    }

    /// Delete all data.
    pub async fn purge_all_data(&self) -> Result<(), anyhow::Error> {
        self.client.purge_all_data().await
    }
}

pub type DbFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, anyhow::Error>> + Send + 'a>>;

pub trait DbClient {
    fn as_any(&self) -> &dyn std::any::Any;

    fn schema(&self) -> DbFuture<'_, schema::DbSchema>;
    fn entity(&self, id: IdOrIdent) -> DbFuture<'_, Option<DataMap>>;

    fn select(
        &self,
        query: query::select::Select,
    ) -> DbFuture<'_, query::select::Page<query::select::Item>>;

    fn select_map(&self, query: query::select::Select) -> DbFuture<'_, Vec<DataMap>>;

    fn batch(&self, batch: Batch) -> DbFuture<'_, ()>;
    fn migrate(&self, migration: query::migrate::Migration) -> DbFuture<'_, ()>;
    fn migrations(&self) -> DbFuture<'_, Vec<Migration>>;
    fn storage_usage(&self) -> DbFuture<'_, Option<u64>>;
    fn purge_all_data(&self) -> DbFuture<'_, ()>;
}
