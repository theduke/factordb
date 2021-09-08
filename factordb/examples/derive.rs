use factordb::{
    query::{expr::Expr, select::Select},
    schema::{AttributeDescriptor, EntityDescriptor},
    AnyError, Attribute, Entity, Id,
};
use serde::{Deserialize, Serialize};

#[derive(Attribute)]
#[factor(namespace = "todo")]
pub struct AttrTitle(String);

#[derive(Attribute)]
#[factor(namespace = "todo")]
pub struct AttrDone(bool);

#[derive(Serialize, Deserialize, Entity, Clone, Debug)]
#[factor(namespace = "semantic")]
struct Todo {
    #[factor(attr = AttrId)]
    #[serde(rename = "factor/id")]
    pub id: Id,

    #[factor(attr = AttrTitle)]
    #[serde(rename = "todo/title")]
    pub title: String,

    #[factor(attr = AttrDone)]
    #[serde(rename = "todo/done")]
    pub done: bool,
}

async fn run() -> Result<(), AnyError> {
    let db = factordb::Db::new(factordb::backend::memory::MemoryDb::new());

    // Run a migration with **upserts**. This can be re-run at will.
    let migration = factordb::query::migrate::Migration::new()
        .attr_upsert(AttrTitle::schema())
        .attr_upsert(AttrDone::schema())
        .entity_upsert(Todo::schema());
    db.migrate(migration).await?;

    let id = Id::random();
    let todo1 = Todo {
        id,
        title: "Get shit done".into(),
        done: false,
    };
    db.create_entity(todo1).await?;

    let _loaded_todo_1 = db.entity(id).await?;

    let query = Select::new()
        .with_filter(Expr::is_entity::<Todo>())
        .with_limit(100);
    let _all_todos = db.select(query).await?;

    Ok(())
}

fn main() {
    futures::executor::block_on(run()).unwrap();
}