use factordb::{
    db::Db,
    prelude::{AttributeDescriptor, DataMap, EntityDescriptor, Expr, Id, Migration, Select},
    schema::builtin::{AttrDescription, AttrTitle},
    AnyError, Attribute, Entity,
};
use serde::{Deserialize, Serialize};

#[derive(Attribute)]
#[factor(namespace = "test")]
pub struct AttrTodoDone(bool);

#[derive(Entity, Serialize, Deserialize)]
#[factor(namespace = "test")]
pub struct Todo {
    #[factor(attr = AttrId)]
    #[serde(rename = "factor/id")]
    pub id: Id,

    #[factor(attr=AttrTitle)]
    #[serde(rename = "factor/title")]
    pub title: String,

    #[factor(attr=AttrDescription)]
    #[serde(rename = "factor/description")]
    pub description: Option<String>,

    #[factor(attr=AttrTodoDone)]
    #[serde(rename = "test/todo_done")]
    pub done: bool,
}

impl Todo {
    pub fn new_from_index(index: usize) -> Self {
        Todo {
            id: Id::from_uuid(uuid::Uuid::from_u128(100_000 + index as u128)),
            title: index.to_string(),
            description: if index % 2 == 0 {
                Some(index.to_string())
            } else {
                None
            },
            done: index % 2 == 0,
        }
    }
}

pub async fn select_single_todo_with_title_eq(db: &Db, title: String) -> Result<DataMap, AnyError> {
    let filter = Expr::eq(AttrTitle::expr(), title);
    let select = Select::new().with_filter(filter).with_limit(1);

    let mut page = db.select(select).await.unwrap();
    Ok(page.items.remove(0).data)
}

pub async fn apply_schema(db: &Db) -> Result<(), AnyError> {
    let mig = Migration::new()
        .attr_upsert(AttrTodoDone::schema())
        .entity_upsert(Todo::schema());
    db.migrate(mig).await?;

    Ok(())
}
