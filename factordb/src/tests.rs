use futures::{future::BoxFuture, FutureExt};
use schema::AttributeSchema;

use crate::{
    backend::Backend,
    data::{value::patch::Patch, Id, Value, ValueType},
    error::{self, UniqueConstraintViolation},
    map,
    query::{self, expr::Expr, migrate::Migration, select::Select},
    schema::{
        self, builtin::AttrId, AttrMapExt, AttributeDescriptor, EntityAttribute, EntitySchema,
    },
    Db,
};

pub fn test_backend<F>(b: impl Backend + Send + Sync + 'static, spawner: F)
where
    F: Fn(BoxFuture<()>),
{
    let db = Db::new(b);
    spawner(test_db(db).boxed());
}

const NS_TEST: &'static str = "test";

const ATTR_TEXT: &'static str = "text";
const ATTR_INT: &'static str = "int";
const ENTITY_COMMENT: &'static str = "test/comment";

async fn apply_test_schema(db: &Db) {
    let mig = query::migrate::Migration::new()
        .attr_create(AttributeSchema::new(NS_TEST, ATTR_TEXT, ValueType::String))
        .attr_create(AttributeSchema::new(NS_TEST, ATTR_INT, ValueType::Int))
        .entity_create(EntitySchema {
            id: Id::nil(),
            ident: ENTITY_COMMENT.into(),
            title: Some("Comment".into()),
            description: None,
            attributes: vec![EntityAttribute {
                attribute: "test/int".into(),
                cardinality: schema::Cardinality::Many,
            }],
            extends: Vec::new(),
            strict: false,
        });

    db.migrate(mig).await.unwrap();
}

macro_rules! run_tests {
    ( $db:expr, [ $( $name:ident , )* ] ) => {
        {
        let db = $db;
        $(
        eprintln!("Running test '{}' ...", stringify!($name));
        apply_test_schema(db).await;
        $name(db).await;
        db.purge_all_data().await.unwrap();
        )*
        }
    };
}

async fn test_db(db: Db) {
    test_assert_simple(&db).await;
    db.purge_all_data().await.unwrap();

    test_create_attribute(&db).await;
    db.purge_all_data().await.unwrap();

    test_remove_attr(&db).await;
    db.purge_all_data().await.unwrap();

    test_db_with_test_schema(&db).await;
}

async fn test_db_with_test_schema(db: &Db) {
    run_tests!(
        db,
        [
            test_select,
            test_query_in,
            test_merge_list_attr,
            test_patch,
            test_query_contains_with_two_lists,
            test_assert_fails_with_incorrect_value_type,
            test_index_unique,
            test_index_non_unique,
            test_sort_simple,
            test_query_entity_select_ident,
            test_entity_delete_not_found,
        ]
    );
}

async fn test_entity_delete_not_found(db: &Db) {
    let id = Id::random();
    db.create(id, map! {"factor/title": "title"}).await.unwrap();
    db.delete(id).await.unwrap();

    let err = db.delete(id).await.expect_err("Must fail");
    dbg!(&err);
    dbg!(&err.downcast_ref::<error::EntityNotFound>());
    assert!(err.is::<error::EntityNotFound>());
}

async fn test_query_entity_select_ident(db: &Db) {
    let id = Id::random();
    db.create(
        id,
        map! {
            "factor/title": "hello",
            "factor/ident": "hello-ident",
        },
    )
    .await
    .unwrap();

    let page = db
        .select(
            Select::new().with_filter(Expr::eq(AttrId::expr(), Expr::Ident("hello-ident".into()))),
        )
        .await
        .unwrap();

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].data.get_id().unwrap(), id);
}

async fn test_merge_list_attr(db: &Db) {
    let id = Id::random();
    db.create(
        id,
        map! {
            "factor/type": ENTITY_COMMENT,
            "test/int": vec![22],
        },
    )
    .await
    .unwrap();

    db.merge(
        id,
        map! {
            "test/int": vec![23],
        },
    )
    .await
    .unwrap();

    let map = db.entity(id).await.unwrap();
    let values = map.get("test/int").unwrap();
    let v: Value = vec![22, 23].into();
    assert_eq!(values, &v);
}

async fn test_patch(db: &Db) {
    let id = Id::random();
    db.create(
        id,
        map! {
            "factor/type": ENTITY_COMMENT,
            "factor/title": "hello",
            "test/text": "no",
            "test/int": vec![22, 55],
        },
    )
    .await
    .unwrap();

    db.patch(
        id,
        Patch::new()
            .remove("factor/title")
            .replace("test/text", "yes")
            .add("test/int", 33)
            .remove_with_old("test/int", 55),
    )
    .await
    .unwrap();

    let map = db.entity(id).await.unwrap();
    assert_eq!(
        map,
        map! {
            "factor/id": id,
            "factor/type": ENTITY_COMMENT,
            "test/text": "yes",
            "test/int": vec![22, 33]
        }
    );
}

async fn test_create_attribute(f: &Db) {
    let mig = query::migrate::Migration {
        name: None,
        actions: vec![query::migrate::SchemaAction::AttributeCreate(
            query::migrate::AttributeCreate {
                schema: schema::AttributeSchema::new(NS_TEST, "text", ValueType::String),
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
    let items = f.select(Select::new()).await.unwrap().take_data();
    assert_eq!(vec![expected.clone()], items);

    // Now change an attribute.
    let data2 = map! {
        "factor/description": "b",
    };
    f.merge(id, data2.clone()).await.unwrap();

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
        "test/text": "hello",
        "test/int": 42,
    };
    db.create(id, data.clone()).await.unwrap();
    data.insert("factor/id".into(), id.into());

    let page_match = vec![data];

    let items = db.select(Select::new()).await.unwrap().take_data();
    assert_eq!(items, page_match);

    // Select by id.
    let items = db
        .select(Select::new().with_filter(Expr::eq(Expr::literal(id), Expr::ident("factor/id"))))
        .await
        .unwrap()
        .take_data();
    assert_eq!(items, page_match);

    // Simple field comparison select
    let items = db
        .select(
            Select::new().with_filter(Expr::eq(Expr::ident("test/text"), Expr::literal("hello"))),
        )
        .await
        .unwrap()
        .take_data();
    assert_eq!(items, page_match);
}

async fn test_sort_simple(db: &Db) {
    let id1 = Id::random();
    let mut data1 = map! {
        "test/int": 100,
    };
    db.create(id1, data1.clone()).await.unwrap();
    data1.insert("factor/id".into(), id1.into());

    let id2 = Id::random();
    let mut data2 = map! {
        "test/int": 0,
    };
    db.create(id2, data2.clone()).await.unwrap();
    data2.insert("factor/id".into(), id2.into());

    let id3 = Id::random();
    let mut data3 = map! {
        "test/int": 50,
    };
    db.create(id3, data3.clone()).await.unwrap();
    data3.insert("factor/id".into(), id3.into());

    // Ascending.
    let page_match = vec![data2.clone(), data3.clone(), data1.clone()];
    let items = db
        .select(Select::new().with_sort(Expr::Attr("test/int".into()), query::select::Order::Asc))
        .await
        .unwrap()
        .take_data();
    assert_eq!(items, page_match);

    // Descending.
    let page_match = vec![data1, data3, data2];
    let items = db
        .select(Select::new().with_sort(Expr::Attr("test/int".into()), query::select::Order::Desc))
        .await
        .unwrap()
        .take_data();
    assert_eq!(items, page_match);
}

async fn test_query_in(db: &Db) {
    let id = Id::random();
    let mut data = map! {
        "test/int": 42,
    };
    db.create(id, data.clone()).await.unwrap();
    data.insert("factor/id".into(), id.into());

    let page_match = vec![data];

    let filter = Expr::in_(Expr::ident("test/int"), Value::List(vec![42.into()]));
    let items = db
        .select(Select::new().with_filter(filter))
        .await
        .unwrap()
        .take_data();
    assert_eq!(items, page_match);
}

async fn test_query_contains_with_two_lists(db: &Db) {
    let id = Id::random();
    db.create(
        id,
        map! {
            "factor/type": ENTITY_COMMENT,
            "test/int": vec![22, 23],
        },
    )
    .await
    .unwrap();

    db.create(
        Id::random(),
        map! {
            "factor/type": ENTITY_COMMENT,
            "test/int": vec![1],
        },
    )
    .await
    .unwrap();

    let filter = Expr::contains(Expr::Attr("test/int".into()), vec![22]);
    let page = db.select(Select::new().with_filter(filter)).await.unwrap();

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].data.get_id().unwrap(), id);
}

async fn test_remove_attr(db: &Db) {
    // Create new attribute.
    let mig = Migration::new().attr_create(AttributeSchema::new(
        NS_TEST,
        "removeAttr",
        ValueType::String,
    ));
    db.migrate(mig).await.unwrap();

    // Insert an entity.
    let id = Id::random();
    let mut data = map! {
        "factor/description": "lala",
        "test/removeAttr": "toRemove",
    };
    db.create(id, data.clone()).await.unwrap();
    data.insert(AttrId::QUALIFIED_NAME.into(), id.into());

    // Check data is as expected.
    let data2 = db.entity(id).await.unwrap();
    assert_eq!(data, data2);

    // Delete the attribute.
    let mig2 = Migration::new().attr_delete("test/removeAttr");
    db.migrate(mig2).await.unwrap();

    // Assert that attribute has been removed from entity.
    data.remove("test/removeAttr");
    let data3 = db.entity(id).await.unwrap();
    assert_eq!(data, data3);
}

async fn test_assert_fails_with_incorrect_value_type(f: &Db) {
    let res = f
        .create(
            Id::random(),
            map! {
                "test/int": "hello",
            },
        )
        .await;

    assert!(res.is_err());
}

async fn test_index_unique(db: &Db) {
    db.migrate(query::migrate::Migration::new().attr_create(
        AttributeSchema::new(NS_TEST, "indexed_unique", ValueType::String).with_unique(true),
    ))
    .await
    .unwrap();

    let id = Id::random();
    let e1 = map! {
        "factor/id": id,
        "test/indexed_unique": "a",
    };
    db.create(id, e1.clone()).await.unwrap();

    // Insert second entity with same unique value
    let id = Id::random();
    let e1 = map! {
        "factor/id": id,
        "test/indexed_unique": "a",
    };
    let err = db.create(id, e1.clone()).await.expect_err("Must fail");
    assert!(err.is::<UniqueConstraintViolation>());
}

async fn test_index_non_unique(db: &Db) {
    db.migrate(query::migrate::Migration::new().attr_create(
        AttributeSchema::new(NS_TEST, "indexed", ValueType::String).with_indexed(true),
    ))
    .await
    .unwrap();

    let id = Id::random();
    let e1 = map! {
        "factor/id": id,
        "test/indexed": "a",
    };
    db.create(id, e1.clone()).await.unwrap();

    // Insert second entity with same unique value
    let id = Id::random();
    let e1 = map! {
        "factor/id": id,
        "test/indexed": "a",
    };
    db.create(id, e1.clone()).await.unwrap();
}
