use futures::{future::BoxFuture, FutureExt};
use schema::AttributeSchema;

use crate::{
    backend::Backend,
    data::{Id, Value, ValueType},
    error, map,
    query::{self, expr::Expr, migrate::Migration, select::Select},
    schema::{self, builtin::AttrId, AttributeDescriptor},
    Db,
};

pub fn test_backend<F>(b: impl Backend + Send + Sync + 'static, spawner: F)
where
    F: Fn(BoxFuture<()>),
{
    let db = Db::new(b);
    spawner(test_db(db).boxed());
}

const ATTR_TEXT: &'static str = "t/text";
const ATTR_INT: &'static str = "t/int";

async fn apply_test_schema(db: &Db) {
    let mig = query::migrate::Migration::new()
        .attr_create(AttributeSchema::new(ATTR_TEXT, ValueType::String))
        .attr_create(AttributeSchema::new(ATTR_INT, ValueType::Int));
    db.migrate(mig).await.unwrap();
}

async fn test_db(db: Db) {
    test_assert_simple(&db).await;
    db.purge_all_data().await.unwrap();

    test_create_attribute(&db).await;
    db.purge_all_data().await.unwrap();

    test_assert_fails_with_incorrect_value_type(&db).await;
    db.purge_all_data().await.unwrap();

    test_remove_attr(&db).await;
    db.purge_all_data().await.unwrap();

    test_db_with_test_schema(&db).await;
}

async fn test_db_with_test_schema(db: &Db) {
    apply_test_schema(db).await;

    test_select(db).await;
}

async fn test_assert_fails_with_incorrect_value_type(f: &Db) {
    let res = f
        .create(
            Id::random(),
            map! {
                "factor/description": 22,
            },
        )
        .await;

    assert!(res.is_err());
}

async fn test_create_attribute(f: &Db) {
    let mig = query::migrate::Migration {
        actions: vec![query::migrate::SchemaAction::AttributeCreate(
            query::migrate::AttributeCreate {
                schema: schema::AttributeSchema {
                    id: Id::nil(),
                    name: "test/text".into(),
                    description: None,
                    value_type: ValueType::String,
                    unique: false,
                    index: false,
                    strict: true,
                },
            },
        )],
    };
    f.migrate(mig).await.unwrap();

    let id = Id::random();
    f.create(
        id,
        map! {
            "test/text": "hello",
        },
    )
    .await
    .unwrap();

    let data = f.entity(id).await.unwrap();
    assert_eq!(Value::from("hello"), data["test/text"]);
}

async fn test_assert_simple(f: &Db) {
    let id = Id::random();

    // Check that inexistant id returns EntityNotFound error.
    let err = f.entity(id).await.unwrap_err();
    assert!(err.is::<error::EntityNotFound>());

    // Check that a query returns nothing.
    let page = f.select(Select::new()).await.unwrap();
    dbg!(&page.items);
    assert!(page.items.is_empty());

    // Create entity.
    let data = map! {
        "factor/description": "a",
    };
    f.create(id, data.clone()).await.unwrap();

    // Load and compare.
    let data1 = f.entity(id).await.unwrap();
    let mut expected = map! {
        "factor/description": "a",
        // "factor/ident": ident.clone(),
        "factor/id": id,
    };
    assert_eq!(expected, data1);

    // Load via unfiltered select query.
    let page = f.select(Select::new()).await.unwrap();
    assert_eq!(vec![expected.clone()], page.items);

    // Now change an attribute.
    let data2 = map! {
        "factor/description": "b",
    };
    f.patch(id, data2.clone()).await.unwrap();

    // Load and compare again.
    expected.insert("factor/description".into(), "b".into());
    let data3 = f.entity(id).await.unwrap();
    assert_eq!(expected, data3);

    // Delete
    f.delete(id).await.unwrap();

    // Ensure entity is gone.
    let err = f.entity(id).await.unwrap_err();
    assert!(err.is::<error::EntityNotFound>());
}

async fn test_select(db: &Db) {
    let id = Id::random();
    let mut data = map! {
        "t/text": "hello",
        "t/int": 42,
    };
    db.create(id, data.clone()).await.unwrap();

    data.insert("factor/id".into(), id.into());

    let page_match = vec![data];

    let page = db.select(Select::new()).await.unwrap();
    assert_eq!(page.items, page_match);

    let page = db
        .select(Select::new().with_filter(Expr::eq(Expr::ident("t/text"), Expr::literal("hello"))))
        .await
        .unwrap();
    assert_eq!(page.items, page_match);
}

async fn test_remove_attr(db: &Db) {
    // Create new attribute.
    let mig = Migration::new().attr_create(AttributeSchema::new("t/removeAttr", ValueType::String));
    db.migrate(mig).await.unwrap();

    // Insert an entity.
    let id = Id::random();
    let mut data = map! {
        "factor/description": "lala",
        "t/removeAttr": "toRemove",
    };
    db.create(id, data.clone()).await.unwrap();
    data.insert(AttrId::NAME.into(), id.into());

    // Check data is as expected.
    let data2 = db.entity(id).await.unwrap();
    assert_eq!(data, data2);

    // Delete the attribute.
    let mig2 = Migration::new().attr_delete("t/removeAttr");
    db.migrate(mig2).await.unwrap();

    // Assert that attribute has been removed from entity.
    data.remove("t/removeAttr");
    let data3 = db.entity(id).await.unwrap();
    assert_eq!(data, data3);
}
