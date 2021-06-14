//! UNFINISHED/INOPERABLE sqlite backend

mod pool;

use anyhow::Context;
use factordb::{
    data::{DataMap, Ident},
    registry::SharedRegistry,
    AnyError,
};
use futures::future::FutureExt;
use pool::Connection;

use self::pool::Pool;

#[derive(Clone)]
pub struct SqliteDb {
    pool: Pool,
    registry: SharedRegistry,
}

impl SqliteDb {
    pub async fn open(path: impl Into<String>) -> Result<Self, AnyError> {
        let pool = pool::build_pool(path);

        let schema_items = pool
            .get()
            .await?
            .interact(|con| Self::migrate(&con))
            .await?;

        // First register all attributes.
        let mut registry = factordb::registry::Registry::new();
        for item in schema_items {
            match item {
                factordb::schema::SchemaItem::Attribute(attr) => {
                    registry.register_attr(attr)?;
                }
                factordb::schema::SchemaItem::Entity(entity) => {
                    registry.register_entity(entity, false)?;
                }
            }
        }

        // TODO: validate entire schema. Needs a Registry::validate() helper.

        let shared_reg = registry.into_shared();

        Ok(Self {
            pool,
            registry: shared_reg,
        })
    }

    fn migrate(con: &Connection) -> Result<Vec<factordb::schema::SchemaItem>, AnyError> {
        let res = con.query_row_and_then("SELECT MAX(version) FROM migrations", [], |row| {
            row.get::<_, u64>(0)
        });

        let version = match res {
            Ok(version) => version,
            Err(rusqlite::Error::SqliteFailure(err, msg)) => {
                if err.code == rusqlite::ErrorCode::Unknown {
                    0
                } else {
                    return Err(rusqlite::Error::SqliteFailure(err, msg).into());
                }
            }
            Err(other) => Err(other)?,
        };

        let migrations = vec![
            "
            CREATE TABLE migrations (version INTEGER NOT NULL PRIMARY KEY);
            ",
            r#"
            CREATE TABLE schema_entities (id BLOB NOT NULL PRIMARY KEY, content BLOB NOT NULL);
            CREATE TABLE entities(id BLOB NOT NULL UNIQUE PRIMARY KEY, ident TEXT UNIQUE, content BLOB NOT NULL);
            "#,
        ];

        for (version, sql) in migrations.iter().enumerate().skip(version as usize) {
            let full_sql = format!(
                r#"
            BEGIN;
            {}
            INSERT INTO migrations (version) VALUES ({});
            COMMIT;
            "#,
                sql,
                version + 1
            );

            con.execute_batch(&full_sql)?;
        }

        Self::load_schema(con)
    }

    fn load_schema(con: &Connection) -> Result<Vec<factordb::schema::SchemaItem>, AnyError> {
        con.prepare("SELECT id, content FROM schema_entities")?
            .query_and_then([], |row| -> Result<_, AnyError> {
                let content: Vec<u8> = row.get("content")?;
                let item: factordb::schema::SchemaItem = serde_json::from_slice(&content)?;
                Ok(item)
            })?
            .collect()
    }

    async fn do_sql<O, F>(&self, f: F) -> Result<O, AnyError>
    where
        F: FnOnce(&Connection) -> Result<O, AnyError> + Send + 'static,
        O: Send + 'static,
    {
        let res = self.pool.get().await?.interact(f).await?;
        Ok(res)
    }

    async fn entity(&self, ident: Ident) -> Result<DataMap, AnyError> {
        self.do_sql(|c| Self::load_entity(c, ident)).await
    }

    fn load_entity(c: &Connection, ident: Ident) -> Result<DataMap, AnyError> {
        let res = match &ident {
            Ident::Id(id) => c
                .prepare_cached("SELECT content FROM entities WHERE id = ?")?
                .query_row([&id.as_uuid()], |row| row.get::<_, Vec<u8>>(0)),
            Ident::Name(name) => c
                .prepare_cached("SELECT content FROM entities WHERE ident = ?")?
                .query_row([name.as_ref()], |row| row.get::<_, Vec<u8>>(0)),
        };

        let data = match res {
            Ok(data) => data,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                return Err(factordb::error::EntityNotFound::new(ident).into());
            }
            Err(other) => {
                return Err(other.into());
            }
        };

        let map = serde_json::from_slice(&data).context("Could not deserialize entity data")?;
        Ok(map)
    }

    async fn purge_all_data(&self) -> Result<(), AnyError> {
        self.do_sql(|c| {
            c.execute_batch("DELETE FROM entities")?;
            Ok(())
        })
        .await
    }
}

impl factordb::backend::Backend for SqliteDb {
    fn registry(&self) -> &SharedRegistry {
        &self.registry
    }

    fn entity(
        &self,
        id: factordb::data::Ident,
    ) -> factordb::backend::BackendFuture<factordb::data::DataMap> {
        let s = self.clone();
        async move { s.entity(id).await }.boxed()
    }

    fn select(
        &self,
        _query: factordb::query::select::Select,
    ) -> factordb::backend::BackendFuture<factordb::query::select::Page<factordb::data::DataMap>>
    {
        todo!()
    }

    fn apply_batch(
        &self,
        _batch: factordb::query::update::BatchUpdate,
    ) -> factordb::backend::BackendFuture<()> {
        todo!()
    }

    fn migrate(
        &self,
        _migration: factordb::query::migrate::Migration,
    ) -> factordb::backend::BackendFuture<()> {
        todo!()
    }

    fn purge_all_data(&self) -> factordb::backend::BackendFuture<()> {
        let s = self.clone();
        async move { s.purge_all_data().await }.boxed()
    }
}

#[tokio::test]
async fn test() {
    let path = "/tmp/db.sqlite3";
    SqliteDb::open(path).await.unwrap();
}
