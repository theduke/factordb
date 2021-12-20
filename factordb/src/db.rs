use std::sync::Arc;

use data::DataMap;

use crate::{
    backend::Backend,
    data::{self, patch::Patch, Id, IdOrIdent},
    query::{self, mutate::Mutate},
    schema::EntityContainer,
    AnyError,
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

    pub fn backend(&self) -> &Arc<dyn Backend + Send + Sync + 'static> {
        &self.backend
    }

    pub fn schema(&self) -> Result<crate::schema::DbSchema, AnyError> {
        let reg = {
            self.backend()
                .registry()
                .read()
                .map_err(|_| AnyError::msg("Could not retrieve registry"))?
                .clone()
        };

        Ok(reg.build_schema())
    }

    pub async fn entity<I>(&self, id: I) -> Result<DataMap, AnyError>
    where
        I: Into<IdOrIdent>,
    {
        use query::expr::Expr;

        // FIXME: remove this once index persistence logic is implemented.
        match id.into() {
            IdOrIdent::Id(id) => self.backend.entity(id.into()).await,
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

    pub async fn select(
        &self,
        query: query::select::Select,
    ) -> Result<query::select::Page<query::select::Item>, AnyError> {
        self.backend.select(query).await
    }

    pub async fn create(&self, id: Id, data: DataMap) -> Result<(), AnyError> {
        self.batch(query::mutate::Batch {
            actions: vec![query::mutate::Mutate::Create(query::mutate::Create {
                id,
                data,
            })],
        })
        .await
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
