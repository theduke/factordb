use futures::{future::BoxFuture, FutureExt};

use crate::{
    backend::Backend,
    data::{Id, Value, ValueType},
    error, map, query, schema, Db,
};

pub fn test_backend<F>(b: impl Backend + Send + Sync + 'static, spawner: F)
where
    F: Fn(BoxFuture<()>),
{
    let db = Db::new(b);
    spawner(test_db(db).boxed());
}

async fn test_db(f: Db) {
    test_assert_simple(&f).await;
    test_create_attribute(&f).await;
    test_assert_fails_with_incorrect_value_type(&f).await;
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

    let data = f.entity(id.into()).await.unwrap();
    assert_eq!(Value::from("hello"), data["test/text"]);
}

async fn test_assert_simple(f: &Db) {
    let id = Id::random();

    let err = f.entity(id.into()).await.unwrap_err();
    assert!(err.is::<error::EntityNotFound>());

    let data = map! {
        "factor/description": "a",
    };
    f.create(id, data.clone()).await.unwrap();
    let data1 = f.entity(id.into()).await.unwrap();

    let mut expected = map! {
        "factor/description": "a",
        // "factor/ident": ident.clone(),
        "factor/id": id,
    };
    assert_eq!(expected, data1);

    // Now change an attribute.
    let data2 = map! {
        "factor/description": "b",
    };
    f.patch(id, data2.clone()).await.unwrap();

    expected.insert("factor/description".into(), "b".into());
    let data3 = f.entity(id.into()).await.unwrap();
    assert_eq!(expected, data3);

    f.delete(id).await.unwrap();

    let err = f.entity(id.into()).await.unwrap_err();
    assert!(err.is::<error::EntityNotFound>());
}
