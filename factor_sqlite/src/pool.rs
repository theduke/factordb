use deadpool_sqlite::Manager as SqliteManager;

pub type Connection = rusqlite::Connection;

// Need a custom manager to initialize db with correct settings.
pub struct Manager {
    encryption_key: Option<String>,
    manager: SqliteManager,
}

#[async_trait::async_trait]
impl deadpool::managed::Manager for Manager {
    type Type = <deadpool_sqlite::Manager as deadpool::managed::Manager>::Type;
    type Error = <deadpool_sqlite::Manager as deadpool::managed::Manager>::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let con = self.manager.create().await?;

        let key = self.encryption_key.clone();
        con.interact(move |con| -> Result<(), rusqlite::Error> {
            // TODO: add config options for the settings below.

            // For some reason PRAGMA journal_mode now returns a row, which
            // means execute(_batch) fails with an error, so we need
            // a seprate query.
            let mode = con.query_row("PRAGMA journal_mode = WAL;", [], |row| {
                row.get::<_, String>("journal_mode")
            })?;

            if mode != "wal" {
                // Abusing the InvalidColumnName error here for convenience...
                return Err(rusqlite::Error::InvalidColumnName(format!(
                    "Expected journal mode to be wal, but got '{}'",
                    mode
                )));
            }

            con.execute_batch(
                r#"
                PRAGMA synchronous = normal;
                PRAGMA temp_store = memory;
                PRAGMA cache_size = -64000;
            "#,
            )?;

            if let Some(key) = key {
                con.execute("PRAMGA_KEY", &[&key])?;
                // Customize sqlcipher settings.
                // TODO: add config options for the settings below.
                con.execute_batch(
                    r#"                    
                    PRAGMA cipher_memory_security = OFF;
                    PRAGMA cipher_page_size = 65536;
                "#,
                )?;
            }

            Ok(())
        })
        .await?;
        Ok(con)
    }

    async fn recycle(&self, obj: &mut Self::Type) -> deadpool::managed::RecycleResult<Self::Error> {
        self.manager.recycle(obj).await
    }

    fn detach(&self, obj: &mut Self::Type) {
        self.manager.detach(obj)
    }
}

pub type Pool = deadpool::managed::Pool<Manager>;

pub fn build_pool(path: impl Into<String>) -> Pool {
    let path = path.into();
    let man = Manager {
        encryption_key: None,
        manager: deadpool_sqlite::Manager::from_config(&deadpool_sqlite::Config {
            path,
            pool: None,
        }),
    };
    Pool::new(man, 10)
}
