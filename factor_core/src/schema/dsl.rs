use anyhow::Context;

use crate::{
    data::DataMap,
    simple_db::{NamespaceImport, SimpleDb},
};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DslSchema {
    pub id: String,

    #[serde(default)]
    pub imports: Vec<NamespaceImport>,
    pub namespace: String,

    pub migrations: Vec<DslMigration>,
}

impl DslSchema {
    pub fn apply_to_simple_db(self, mut db: SimpleDb, skip_resolve_namespaced: bool) -> Result<SimpleDb, anyhow::Error> {
        let config = crate::simple_db::NamespaceConfig {
            namespace: self.namespace.clone(),
            imports: self.imports.clone(),
        };

        for migration in self.migrations {
            let migration_id = migration.id;
            for (index, commit) in migration.commits.into_iter().enumerate() {
                let built = db.resolve_commit(&config, commit, skip_resolve_namespaced).with_context(|| {
                    format!("Could not resolve commit {index} in migration {migration_id}")
                })?;
                db = db.apply_dsl_commit(built).with_context(|| {
                    format!("Could not apply commit {index} in migration {migration_id}")
                })?;
            }
        }

        Ok(db)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DslMigration {
    pub id: String,
    pub commits: Vec<DslCommit>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DslCommit {
    pub subject: String,
    pub set: Option<DataMap>,

    #[serde(default)]
    pub replace: bool,
    #[serde(default)]
    pub destroy: bool,
}
