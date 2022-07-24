use futures::{future::BoxFuture, FutureExt};
use schema::AttributeSchema;

use crate::{backend::Backend, Engine};
use factordb::{
    data::{
        patch::Patch, value::ValueCoercionError, value_type::ConstrainedRefType, Id, Value,
        ValueType,
    },
    error::{self, EntityNotFound, ReferenceConstraintViolation, UniqueConstraintViolation},
    map,
    prelude::{Batch, Db, IdOrIdent, Order},
    query::{
        self,
        expr::Expr,
        migrate::{
            AttributeCreateIndex, EntityAttributeAdd, EntityAttributeChangeCardinality, Migration,
            SchemaAction,
        },
        select::Select,
    },
    schema::{
        self,
        builtin::{AttrId, AttrTitle},
        AttrMapExt, AttributeDescriptor, EntityAttribute, EntitySchema,
    },
};

pub fn test_backend<F>(b: impl Backend + Send + Sync + 'static, spawner: F)
where
    F: Fn(BoxFuture<()>),
{
    let engine = Engine::new(b);
    let db = Db::new(engine);
    spawner(test_db(db).boxed());
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

    test_attr_type_list(&db).await;
    db.purge_all_data().await.unwrap();

    test_convert_attr_to_list(&db).await;
    db.purge_all_data().await.unwrap();

    test_db_with_test_schema(&db).await;
}

async fn test_db_with_test_schema(db: &Db) {
    run_tests!(
        db,
        [
            test_schema_contains_builtins,
            test_ref_insert_with_id_or_ident,
            test_select_in_with_list,
            test_select,
            test_query_in,
            test_query_regex,
            test_attr_corcions,
            test_merge_list_attr,
            test_patch,
            test_patch_replace_skip_existing,
            test_query_contains_with_two_lists,
            test_assert_fails_with_incorrect_value_type,
            test_index_unique,
            test_index_non_unique,
            test_sort_simple,
            test_query_entity_select_ident,
            test_query_entity_is_type_nested,
            test_entity_delete_not_found,
            test_entity_attr_add_with_default,
            test_entity_attr_change_cardinality_from_required_to_optional,
            test_attribute_create_index,
            test_attribute_create_unique_index_fails_with_duplicate_values,
            test_attr_union_add_variant,
            test_int_sort,
            test_uint_sort,
            test_float_sort,
            test_select_delete,
            test_aggregate_count,
            test_reference_validation,
            test_reference_validation_constrained_type,
            test_attr_disallows_multiple_values,
        ]
    );
}

const NS_TEST: &'static str = "test";

const ATTR_TEXT: &'static str = "text";
const ATTR_INT: &'static str = "int";
const ATTR_INT_LIST: &'static str = "int_list";
const ATTR_UINT: &'static str = "uint";
const ATTR_FLOAT: &'static str = "float";
const ENTITY_COMMENT: &'static str = "test/comment";
const ATTR_REF: &'static str = "test/ref";
const ATTR_REF_IMAGE: &'static str = "test/ref_image";

const ENTITY_FILE: &'static str = "test/File";
const ENTITY_IMAGE: &'static str = "test/Image";
const ENTITY_IMAGE_JPEG: &'static str = "test/ImageJpeg";

async fn apply_test_schema(db: &Db) {
    let mig = query::migrate::Migration::new()
        .attr_create(AttributeSchema::new(
            format!("{}/{}", NS_TEST, ATTR_TEXT),
            ValueType::String,
        ))
        .attr_create(AttributeSchema::new(
            format!("{}/{}", NS_TEST, ATTR_INT),
            ValueType::Int,
        ))
        .attr_create(AttributeSchema::new(
            format!("{}/{}", NS_TEST, ATTR_INT_LIST),
            ValueType::new_list(ValueType::Int),
        ))
        .attr_create(AttributeSchema::new(
            format!("{}/{}", NS_TEST, ATTR_UINT),
            ValueType::UInt,
        ))
        .attr_create(AttributeSchema::new(
            format!("{}/{}", NS_TEST, ATTR_FLOAT),
            ValueType::Float,
        ))
        .attr_create(AttributeSchema::new(
            format!("{}/{}", NS_TEST, "ref"),
            ValueType::Ref,
        ))
        .entity_create(EntitySchema {
            id: Id::nil(),
            ident: ENTITY_COMMENT.into(),
            title: Some("Comment".into()),
            description: None,
            attributes: vec![EntityAttribute {
                attribute: "test/int_list".into(),
                cardinality: schema::Cardinality::Optional,
            }],
            extends: Vec::new(),
            strict: false,
        })
        .entity_create(EntitySchema {
            id: Id::nil(),
            ident: ENTITY_FILE.into(),
            title: Some("File".into()),
            description: None,
            attributes: vec![EntityAttribute {
                attribute: "test/int_list".into(),
                cardinality: schema::Cardinality::Optional,
            }],
            extends: Vec::new(),
            strict: false,
        })
        .entity_create(EntitySchema {
            id: Id::nil(),
            ident: ENTITY_IMAGE.into(),
            title: Some("Image".into()),
            description: None,
            attributes: vec![],
            extends: vec![ENTITY_FILE.into()],
            strict: false,
        })
        .entity_create(EntitySchema {
            id: Id::nil(),
            ident: ENTITY_IMAGE_JPEG.into(),
            title: Some("Jpeg Image".into()),
            description: None,
            attributes: vec![],
            extends: vec![ENTITY_IMAGE.into()],
            strict: false,
        })
        .attr_create(AttributeSchema::new(
            format!("{}/{}", NS_TEST, "ref_image"),
            ValueType::RefConstrained(ConstrainedRefType {
                allowed_entity_types: vec!["test/Image".into()],
            }),
        ));

    db.migrate(mig).await.unwrap();
}

async fn test_attr_disallows_multiple_values(db: &Db) {
    let is_coercion = db
        .create(Id::random(), map! {"test/int": vec![22]})
        .await
        .err()
        .unwrap()
        .is::<ValueCoercionError>();
    assert_eq!(true, is_coercion);
}

async fn test_ref_insert_with_id_or_ident(db: &Db) {
    // let ident = "insert_ident1";
    // let id1 = Id::random();
    // db.create(id1, map! {"factor/ident": ident}).await.unwrap();

    // let id3 = Id::random();
    // db.create(id3, map! {"test/ref": id1}).await.unwrap();

    // let id2 = Id::random();
    // db.create(id2, map! {"test/ref": ident}).await.unwrap();

    // let tuple2 = db.entity(id2).await.unwrap();
    // let tuple3 = db.entity(id3).await.unwrap();

    // tuple3.get("test/ref").unwrap().as_id().unwrap();
    // tuple2.get("test/ref").unwrap().as_id().unwrap();

    // TODO: enable!
}

async fn test_attr_corcions(db: &factordb::prelude::Db) {
    // int coerces to uint
    db.create(
        Id::random(),
        map! {
            "test/uint": 10i64,
        },
    )
    .await
    .unwrap();

    // uint coerces to int
    db.create(
        Id::random(),
        map! {
            "test/int": 10u64,
        },
    )
    .await
    .unwrap();

    // int coerces to float
    let id = Id::random();
    db.create(
        id,
        map! {
            "test/float": i64::MAX,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        db.entity(id)
            .await
            .unwrap()
            .get("test/float")
            .unwrap()
            .as_float()
            .unwrap() as i64,
        i64::MAX,
    );

    // uint coerces to float
    let id = Id::random();
    db.create(
        id,
        map! {
            "test/float": u64::MAX,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        db.entity(id)
            .await
            .unwrap()
            .get("test/float")
            .unwrap()
            .as_float()
            .unwrap() as u64,
        u64::MAX,
    );
}

async fn test_schema_contains_builtins(db: &Db) {
    db.schema()
        .await
        .unwrap()
        .resolve_attr(&IdOrIdent::new_static("factor/id"))
        .unwrap();
}

async fn test_attribute_create_index(db: &Db) {
    let attr_name = "test/add_schema".to_string();
    db.migrate(Migration::new().attr_create(AttributeSchema {
        id: Id::nil(),
        ident: attr_name.clone(),
        title: None,
        description: None,
        value_type: ValueType::Int,
        unique: false,
        index: false,
        strict: true,
    }))
    .await
    .unwrap();

    let id1 = Id::random();
    db.create(
        id1,
        map! {
            "test/add_schema": 22,
        },
    )
    .await
    .unwrap();
    let id2 = Id::random();
    db.create(
        id2,
        map! {
            "test/add_schema": 23,
        },
    )
    .await
    .unwrap();

    db.migrate(
        Migration::new().action(SchemaAction::AttributeCreateIndex(AttributeCreateIndex {
            attribute: attr_name.clone(),
            unique: true,
        })),
    )
    .await
    .unwrap();

    let schema = db.schema().await.unwrap();
    let attr = schema
        .attributes
        .iter()
        .find(|a| a.ident == attr_name)
        .unwrap();
    assert_eq!(attr.unique, true);

    schema
        .indexes
        .iter()
        .find(|idx| idx.attributes == vec![attr.id])
        .unwrap();
}

async fn test_attribute_create_unique_index_fails_with_duplicate_values(db: &Db) {
    let attr_name = "test/add_index_unique_fails".to_string();
    db.migrate(Migration::new().attr_create(AttributeSchema {
        id: Id::nil(),
        ident: attr_name.clone(),
        title: None,
        description: None,
        value_type: ValueType::Int,
        unique: false,
        index: false,
        strict: true,
    }))
    .await
    .unwrap();

    let id1 = Id::random();
    db.create(
        id1,
        map! {
            "test/add_index_unique_fails": 22,
        },
    )
    .await
    .unwrap();
    let id2 = Id::random();
    db.create(
        id2,
        map! {
            "test/add_index_unique_fails": 22,
        },
    )
    .await
    .unwrap();

    let err = db
        .migrate(Migration::new().action(SchemaAction::AttributeCreateIndex(
            AttributeCreateIndex {
                attribute: attr_name.clone(),
                unique: true,
            },
        )))
        .await
        .expect_err("Expected migration to faild due to unique index constraints");

    assert!(err.is::<UniqueConstraintViolation>());

    let schema = db.schema().await.unwrap();
    let attr = schema
        .attributes
        .iter()
        .find(|a| a.ident == attr_name)
        .unwrap();
    assert_eq!(attr.index, false);
    assert_eq!(attr.unique, false);

    let index = schema
        .indexes
        .iter()
        .find(|idx| idx.attributes == vec![attr.id]);
    assert!(index.is_none());
}

async fn test_attr_union_add_variant(db: &Db) {
    let attr_name = "test/union_change_type".to_string();
    // Create union attribute.
    db.migrate(Migration::new().attr_create(AttributeSchema {
        id: Id::nil(),
        ident: attr_name.clone(),
        title: None,
        description: None,
        value_type: ValueType::Union(vec![
            ValueType::Const("a".into()),
            ValueType::Const("b".into()),
        ]),
        unique: false,
        index: false,
        strict: false,
    }))
    .await
    .unwrap();

    // Insert with valid value.
    db.create(
        Id::random(),
        map! {
            "test/union_change_type": "a",
        },
    )
    .await
    .unwrap();

    // Test insert fails with invalid value.
    db.create(
        Id::random(),
        map! {
            "test/union_change_type": "c",
        },
    )
    .await
    .expect_err("Expected insert to fail due to unsupported union value");

    // Add union variant.
    db.migrate(Migration::new().attr_change_type(
        &attr_name,
        ValueType::Union(vec![
            ValueType::Const("a".into()),
            ValueType::Const("b".into()),
            ValueType::Const("c".into()),
        ]),
    ))
    .await
    .unwrap();

    // Test that insert works now.
    db.create(
        Id::random(),
        map! {
            "test/union_change_type": "c",
        },
    )
    .await
    .unwrap();
}

async fn test_entity_delete_not_found(db: &Db) {
    let id = Id::random();
    db.create(id, map! {"factor/title": "title"}).await.unwrap();
    db.delete(id).await.unwrap();

    let err = db.delete(id).await.expect_err("Must fail");
    assert!(err.is::<error::EntityNotFound>());
}

async fn test_entity_attr_add_with_default(db: &Db) {
    let ty = "t/AddTest";
    db.migrate(Migration::new().entity_create(EntitySchema {
        id: Id::nil(),
        ident: ty.to_string(),
        title: None,
        description: None,
        attributes: vec![EntityAttribute {
            attribute: AttrTitle::IDENT.clone(),
            cardinality: schema::Cardinality::Required,
        }],
        extends: vec![],
        strict: false,
    }))
    .await
    .unwrap();

    let id_no_default = Id::random();
    db.create(
        id_no_default,
        map! {
            "factor/type": ty,
            "factor/title": "hello",
        },
    )
    .await
    .unwrap();

    let id_with_default = Id::random();
    db.create(
        id_with_default,
        map! {
            "factor/type": ty,
            "factor/title": "hello",
            "test/int": 100,
        },
    )
    .await
    .unwrap();

    // Migration should fail without a default value.
    let err = db
        .migrate(
            Migration::new().action(SchemaAction::EntityAttributeAdd(EntityAttributeAdd {
                entity: ty.into(),
                attribute: "test/int".into(),
                cardinality: schema::Cardinality::Required,
                default_value: None,
            })),
        )
        .await
        .expect_err("Must fail");
    assert!(err.to_string().contains("requires a default value"));

    // Now supply a default value.
    db.migrate(
        Migration::new().action(SchemaAction::EntityAttributeAdd(EntityAttributeAdd {
            entity: ty.into(),
            attribute: "test/int".into(),
            cardinality: schema::Cardinality::Required,
            default_value: Some(42u64.into()),
        })),
    )
    .await
    .unwrap();

    // Ensure that the attribute was added to the schema.
    db.schema()
        .await
        .unwrap()
        .resolve_entity(&ty.into())
        .unwrap()
        .attributes
        .iter()
        .find(|a| a.attribute.as_name().unwrap() == "test/int")
        .expect("attribute not added to schema!");

    // The previously created entity should now have the new attribute with the default value.
    let entity = db.entity(id_no_default).await.unwrap();
    let val = entity.get("test/int").unwrap().as_int().unwrap();
    assert_eq!(val, 42);

    // The entity with a correct value should still have the old one.
    let entity = db.entity(id_with_default).await.unwrap();
    let val = entity.get("test/int").unwrap().as_int().unwrap();
    assert_eq!(val, 100);
}

async fn test_entity_attr_change_cardinality_from_required_to_optional(f: &Db) {
    f.migrate(
        Migration::new()
            .attr_create(AttributeSchema {
                id: Id::nil(),
                ident: "test/tochange".into(),
                title: None,
                description: None,
                value_type: ValueType::Bool,
                unique: false,
                index: false,
                strict: false,
            })
            .entity_create(EntitySchema {
                id: Id::nil(),
                ident: "test/MutableEntity".into(),
                title: None,
                description: None,
                attributes: vec![EntityAttribute {
                    attribute: "test/tochange".into(),
                    cardinality: schema::Cardinality::Required,
                }],
                extends: vec![],
                strict: false,
            }),
    )
    .await
    .unwrap();

    f.migrate(
        Migration::new().action(SchemaAction::EntityAttributeChangeCardinality(
            EntityAttributeChangeCardinality {
                entity_type: "test/MutableEntity".into(),
                attribute: "test/tochange".into(),
                new_cardinality: schema::Cardinality::Optional,
            },
        )),
    )
    .await
    .unwrap();
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

async fn test_query_entity_is_type_nested(db: &Db) {
    let id1 = Id::random();
    db.create(id1, map! {"factor/type": ENTITY_FILE})
        .await
        .unwrap();

    let id2 = Id::random();
    db.create(id2, map! {"factor/type": ENTITY_IMAGE})
        .await
        .unwrap();

    let id3 = Id::random();
    db.create(id3, map! {"factor/type": ENTITY_IMAGE_JPEG})
        .await
        .unwrap();

    let page = db
        .select(Select::new().with_filter(Expr::InheritsEntityType(ENTITY_FILE.to_string())))
        .await
        .unwrap();

    assert_eq!(page.items.len(), 3);
}

async fn test_merge_list_attr(db: &Db) {
    let id = Id::random();
    db.create(
        id,
        map! {
            "factor/type": ENTITY_COMMENT,
            "test/int_list": vec![22],
        },
    )
    .await
    .unwrap();

    db.merge(
        id,
        map! {
            "test/int_list": vec![22, 23],
        },
    )
    .await
    .unwrap();

    let map = db.entity(id).await.unwrap();
    let values = map.get("test/int_list").unwrap();
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
            "test/int": 42,
            "test/int_list": vec![22, 55],
        },
    )
    .await
    .unwrap();

    db.patch(
        id,
        Patch::new()
            .remove("factor/title")
            .replace("test/text", "yes")
            .replace("test/int", 33)
            .remove_with_old("test/int", 33)
            .replace("test/int", 100)
            .add("test/int_list", 33)
            .remove_with_old("test/int_list", 22),
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
            "test/int": 100,
            "test/int_list": vec![55, 33]
        }
    );
}

/// Test that PatchOp::Replace correctly inserts new entries, but does not
/// replace existing values. (with old = Value::Unit and must_replace = false)
async fn test_patch_replace_skip_existing(f: &Db) {
    let id1 = Id::random();
    f.create(
        id1,
        map! {
            "test/text": "hello",
        },
    )
    .await
    .unwrap();

    let p = Patch::new().replace_with_old("test/text", "new", Value::Unit, false);

    f.patch(id1, p.clone()).await.unwrap();
    let data = f.entity(id1).await.unwrap();
    assert_eq!(data.get("test/text").unwrap(), &Value::from("hello"),);

    let id2 = Id::random();
    f.create(id2, map! {}).await.unwrap();
    f.patch(id2, p).await.unwrap();
    let data = f.entity(id2).await.unwrap();
    assert_eq!(data.get("test/text").unwrap(), &Value::from("new"),);
}

async fn test_create_attribute(f: &Db) {
    let mig = query::migrate::Migration {
        name: None,
        actions: vec![query::migrate::SchemaAction::AttributeCreate(
            query::migrate::AttributeCreate {
                schema: schema::AttributeSchema::new(
                    format!("{}/{}", NS_TEST, "text"),
                    ValueType::String,
                ),
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

    let filter = Expr::in_(Expr::ident("test/int"), vec![42]);
    let items = db
        .select(Select::new().with_filter(filter))
        .await
        .unwrap()
        .take_data();
    assert_eq!(items, page_match);

    let filter = Expr::in_(Expr::ident("test/int"), vec![42, 43, 0]);
    let items = db
        .select(Select::new().with_filter(filter))
        .await
        .unwrap()
        .take_data();
    assert_eq!(items, page_match);

    let filter = Expr::in_(Expr::ident("test/int"), vec![41, 43, 0]);
    let items = db
        .select(Select::new().with_filter(filter))
        .await
        .unwrap()
        .take_data();
    assert!(items.is_empty());
}

async fn test_query_regex(db: &Db) {
    let id1 = Id::random();
    let mut data1 = map! {
        "test/text": "alpha 1",
    };
    db.create(id1, data1.clone()).await.unwrap();
    data1.insert("factor/id".into(), id1.into());

    let id2 = Id::random();
    let mut data2 = map! {
        "test/text": "ALPHA 223",
    };
    db.create(id2, data2.clone()).await.unwrap();
    data2.insert("factor/id".into(), id2.into());

    let id3 = Id::random();
    let mut data3 = map! {
        "test/text": "alpha",
    };
    db.create(id3, data3.clone()).await.unwrap();
    data3.insert("factor/id".into(), id3.into());

    let filter = Expr::regex_match(Expr::ident("test/text"), "alpha \\d+");
    let items = db
        .select(Select::new().with_filter(filter))
        .await
        .unwrap()
        .take_data();
    assert_eq!(items, vec![data1.clone()]);

    let filter = Expr::regex_match(Expr::ident("test/text"), "(?i)alpha \\d+");
    let items = db
        .select(
            Select::new()
                .with_filter(filter)
                .with_sort(Expr::ident("test/text"), Order::Asc),
        )
        .await
        .unwrap()
        .take_data();
    assert_eq!(items, vec![data2, data1]);
}

async fn test_query_contains_with_two_lists(db: &Db) {
    let id = Id::random();
    db.create(
        id,
        map! {
            "factor/type": ENTITY_COMMENT,
            "test/int_list": vec![22, 23],
        },
    )
    .await
    .unwrap();

    db.create(
        Id::random(),
        map! {
            "factor/type": ENTITY_COMMENT,
            "test/int_list": vec![1],
        },
    )
    .await
    .unwrap();

    let filter = Expr::contains(Expr::Attr("test/int_list".into()), vec![22]);
    let page = db.select(Select::new().with_filter(filter)).await.unwrap();

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].data.get_id().unwrap(), id);
}

async fn test_remove_attr(db: &Db) {
    // Create new attribute.
    let mig = Migration::new().attr_create(AttributeSchema::new(
        format!("{}/{}", NS_TEST, "removeAttr"),
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
    db.migrate(
        query::migrate::Migration::new().attr_create(
            AttributeSchema::new(
                format!("{}/{}", NS_TEST, "indexed_unique"),
                ValueType::String,
            )
            .with_unique(true),
        ),
    )
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
    db.migrate(
        query::migrate::Migration::new().attr_create(
            AttributeSchema::new(format!("{}/{}", NS_TEST, "indexed"), ValueType::String)
                .with_indexed(true),
        ),
    )
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

async fn test_int_sort(db: &Db) {
    let mut ids = Vec::new();
    for x in -10..=10 {
        let e1 = map! {
            "test/int": x,
        };
        let id = Id::random();
        db.create(id, e1.clone()).await.unwrap();
        ids.push(id);
    }

    // Test equality.
    let res1 = db
        .select(Select::new().with_filter(Expr::eq(Expr::attr_ident("test/int"), 5)))
        .await
        .unwrap();
    assert_eq!(res1.items.len(), 1);
    assert_eq!(
        res1.items[0]
            .data
            .get("test/int")
            .unwrap()
            .as_int()
            .unwrap(),
        5
    );

    // Test greater than.
    let res1 = db
        .select(
            Select::new()
                .with_filter(Expr::gt(Expr::attr_ident("test/int"), 0))
                .with_sort(Expr::attr_ident("test/int"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res1
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&res_ids, &ids[11..]);

    // Test greatern than or equal.
    let res2 = db
        .select(
            Select::new()
                .with_filter(Expr::gte(Expr::attr_ident("test/int"), 0))
                .with_sort(Expr::attr_ident("test/int"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res2
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&res_ids, &ids[10..]);

    // Test less than.
    let res1 = db
        .select(
            Select::new()
                .with_filter(Expr::lt(Expr::attr_ident("test/int"), 0))
                .with_sort(Expr::attr_ident("test/int"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res1
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&res_ids, &ids[0..10]);

    // Test less than or equal.
    let res2 = db
        .select(
            Select::new()
                .with_filter(Expr::lte(Expr::attr_ident("test/int"), 0))
                .with_sort(Expr::attr_ident("test/int"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res2
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&res_ids, &ids[0..11]);
}

async fn test_uint_sort(db: &Db) {
    let mut ids = Vec::new();
    for x in 0..20 {
        let e1 = map! {
            "test/uint": x,
        };
        let id = Id::random();
        db.create(id, e1.clone()).await.unwrap();
        ids.push(id);
    }

    // Test equality.
    let res1 = db
        .select(Select::new().with_filter(Expr::eq(Expr::attr_ident("test/uint"), 5)))
        .await
        .unwrap();
    assert_eq!(res1.items.len(), 1);
    assert_eq!(res1.items[0].data.get_id().unwrap(), ids[5]);

    // Test greater than.
    let res1 = db
        .select(
            Select::new()
                .with_filter(Expr::gt(Expr::attr_ident("test/uint"), 10))
                .with_sort(Expr::attr_ident("test/uint"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res1
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&res_ids, &ids[11..]);

    // Test greatern than or equal.
    let res2 = db
        .select(
            Select::new()
                .with_filter(Expr::gte(Expr::attr_ident("test/uint"), 10))
                .with_sort(Expr::attr_ident("test/uint"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res2
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&res_ids, &ids[10..]);

    // Test less than.
    let res1 = db
        .select(
            Select::new()
                .with_filter(Expr::lt(Expr::attr_ident("test/uint"), 10))
                .with_sort(Expr::attr_ident("test/uint"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res1
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(&res_ids, &ids[0..10]);

    // Test less than or equal.
    let res2 = db
        .select(
            Select::new()
                .with_filter(Expr::lte(Expr::attr_ident("test/uint"), 10))
                .with_sort(Expr::attr_ident("test/uint"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res2
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&res_ids, &ids[0..11]);
}

async fn test_float_sort(db: &Db) {
    let mut ids = Vec::new();
    for x in -10..=10 {
        let e1 = map! {
            "test/float": x as f64,
        };
        let id = Id::random();
        db.create(id, e1.clone()).await.unwrap();
        ids.push(id);
    }

    // Test equality.
    let res1 = db
        .select(Select::new().with_filter(Expr::eq(Expr::attr_ident("test/float"), 5.0)))
        .await
        .unwrap();
    assert_eq!(res1.items.len(), 1);
    assert_eq!(
        res1.items[0]
            .data
            .get("test/float")
            .unwrap()
            .as_float()
            .unwrap(),
        5.0
    );

    // Test greater than.
    let res1 = db
        .select(
            Select::new()
                .with_filter(Expr::gt(Expr::attr_ident("test/float"), 0))
                .with_sort(Expr::attr_ident("test/float"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res1
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&res_ids, &ids[11..]);

    // Test greatern than or equal.
    let res2 = db
        .select(
            Select::new()
                .with_filter(Expr::gte(Expr::attr_ident("test/float"), 0))
                .with_sort(Expr::attr_ident("test/float"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res2
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&res_ids, &ids[10..]);

    // Test less than.
    let res1 = db
        .select(
            Select::new()
                .with_filter(Expr::lt(Expr::attr_ident("test/float"), 0))
                .with_sort(Expr::attr_ident("test/float"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res1
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&res_ids, &ids[0..10]);

    // Test less than or equal.
    let res2 = db
        .select(
            Select::new()
                .with_filter(Expr::lte(Expr::attr_ident("test/float"), 0))
                .with_sort(Expr::attr_ident("test/float"), Order::Asc),
        )
        .await
        .unwrap();
    let res_ids = res2
        .items
        .iter()
        .map(|x| x.data.get_id().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&res_ids, &ids[0..11]);
}

async fn test_select_delete(db: &Db) {
    for index in 1..=10 {
        db.create(
            Id::random(),
            map! {
                "test/int": index,
            },
        )
        .await
        .unwrap();
    }

    let page = db.select(Select::new()).await.unwrap();
    assert_eq!(page.items.len(), 10);

    let page = db
        .select(
            Select::new()
                .with_filter(Expr::gt(Expr::attr_ident("test/int"), 5))
                .with_sort(Expr::attr_ident("test/int"), Order::Asc),
        )
        .await
        .unwrap();
    let values: Vec<_> = page
        .items
        .iter()
        .map(|item| item.data.get("test/int").unwrap().as_int().unwrap())
        .collect();
    assert_eq!(values, vec![6, 7, 8, 9, 10]);

    db.batch(Batch::new().and_select(query::mutate::MutateSelect {
        filter: Expr::lt(Expr::attr_ident("test/int"), 6),
        variables: Default::default(),
        action: query::mutate::MutateSelectAction::Delete,
    }))
    .await
    .unwrap();

    let page = db
        .select(Select::new().with_sort(Expr::attr_ident("test/int"), Order::Asc))
        .await
        .unwrap();

    let values: Vec<_> = page
        .items
        .iter()
        .map(|item| item.data.get("test/int").unwrap().as_int().unwrap())
        .collect();
    assert_eq!(values, vec![6, 7, 8, 9, 10]);
}

async fn test_aggregate_count(db: &Db) {
    let q = Select::new().with_aggregate(query::select::AggregationOp::Count, "count".to_string());
    let q_filtered = q
        .clone()
        .with_filter(Expr::gte(Expr::attr_ident("test/int"), 5));

    assert_eq!(
        db.select(q.clone())
            .await
            .unwrap()
            .items
            .pop()
            .unwrap()
            .data
            .get("factor/count")
            .unwrap()
            .as_uint()
            .unwrap(),
        0
    );

    for x in 0..10 {
        db.create(
            Id::random(),
            map! {
                "test/int": x,
            },
        )
        .await
        .unwrap();
    }

    assert_eq!(
        db.select(q.clone()).await.unwrap().items[0]
            .data
            .get("factor/count")
            .unwrap()
            .as_uint()
            .unwrap(),
        10,
    );

    assert_eq!(
        db.select(q_filtered.clone()).await.unwrap().items[0]
            .data
            .get("factor/count")
            .unwrap()
            .as_uint()
            .unwrap(),
        5,
    );

    // let sel_sql =
    //     Select::parse_sql("select count() from entities where \"test/int\" >= 5").unwrap();
    // dbg!(&sel_sql);
    // assert_eq!(
    //     db.select(sel_sql.clone()).await.unwrap().items[0]
    //         .data
    //         .get("factor/count")
    //         .unwrap()
    //         .as_uint()
    //         .unwrap(),
    //     5,
    // );
}

async fn test_reference_validation(db: &Db) {
    let id1 = Id::random();
    db.create(id1, map! {}).await.unwrap();

    let id2 = Id::random();
    db.create(
        id2,
        map! {
            ATTR_REF: id1,
        },
    )
    .await
    .unwrap();

    let err = db
        .create(
            id2,
            map! {
                ATTR_REF: Id::nil(),
            },
        )
        .await
        .err()
        .unwrap()
        .downcast::<EntityNotFound>()
        .unwrap();
    assert_eq!(err.ident.as_id().unwrap(), Id::nil());
}

async fn test_reference_validation_constrained_type(db: &Db) {
    let id1 = Id::random();
    db.create(
        id1,
        map! {
            "factor/type": ENTITY_IMAGE,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        db.entity(id1)
            .await
            .unwrap()
            .get_type()
            .unwrap()
            .to_string(),
        ENTITY_IMAGE,
    );

    let id2 = Id::random();
    db.create(
        id2,
        map! {
            ATTR_REF_IMAGE: id1,
        },
    )
    .await
    .unwrap();

    let id3 = Id::random();
    db.create(id3, map! {}).await.unwrap();

    // TODO: test error details
    let _err = db
        .create(
            Id::random(),
            map! {
                ATTR_REF_IMAGE: id3,
            },
        )
        .await
        .err()
        .unwrap()
        .downcast::<ReferenceConstraintViolation>()
        .unwrap();
}

async fn test_attr_type_list(db: &Db) {
    db.migrate(Migration::new().attr_create(AttributeSchema {
        id: Id::nil(),
        ident: "test/list".into(),
        title: None,
        description: None,
        value_type: ValueType::List(Box::new(ValueType::UInt)),
        unique: false,
        index: false,
        strict: false,
    }))
    .await
    .unwrap();

    let id1 = Id::random();
    db.create(
        id1,
        map! {
            "test/list": vec![1, 2, 3],
        },
    )
    .await
    .unwrap();

    let data = db.entity(id1).await.unwrap();
    let values = data
        .get("test/list")
        .unwrap()
        .clone()
        .try_into_list::<u64>()
        .unwrap();
    assert_eq!(values, vec![1, 2, 3]);
}

async fn test_convert_attr_to_list(db: &Db) {
    db.migrate(Migration::new().attr_create(AttributeSchema {
        id: Id::nil(),
        ident: "test/int_to_list".to_string(),
        title: None,
        description: None,
        value_type: ValueType::Int,
        unique: false,
        index: false,
        strict: false,
    }))
    .await
    .unwrap();

    let id1 = Id::random();
    db.create(
        id1,
        map! {
            "test/int_to_list": 1,
        },
    )
    .await
    .unwrap();

    let id2 = Id::random();
    db.create(
        id2,
        map! {
            "test/int_to_list": 2,
        },
    )
    .await
    .unwrap();

    db.migrate(Migration::new().attr_change_type(
        "test/int_to_list",
        ValueType::List(Box::new(ValueType::Int)),
    ))
    .await
    .unwrap();

    let attr = db
        .schema()
        .await
        .unwrap()
        .resolve_attr(&"test/int_to_list".to_string().into())
        .unwrap()
        .clone();
    assert_eq!(attr.value_type, ValueType::List(Box::new(ValueType::Int)));

    let val1 = db
        .entity(id1)
        .await
        .unwrap()
        .get("test/int_to_list")
        .unwrap()
        .clone()
        .try_into_list::<i64>()
        .unwrap();
    assert_eq!(val1, vec![1]);

    let val2 = db
        .entity(id2)
        .await
        .unwrap()
        .get("test/int_to_list")
        .unwrap()
        .clone()
        .try_into_list::<i64>()
        .unwrap();
    assert_eq!(val2, vec![2]);
}

async fn test_select_in_with_list(db: &Db) {
    let id1 = Id::random();
    db.create(
        id1,
        map! {
            "test/int_list": vec![1, 2, 3],
        },
    )
    .await
    .unwrap();

    let sel = Select::new().with_filter(Expr::in_(1i64, Expr::attr_ident("test/int_list")));
    let items = db.select_map(sel).await.unwrap();
    assert!(items.len() == 1 && items[0].get_id().unwrap() == id1);

    let sel = Select::new().with_filter(Expr::in_(4i64, Expr::attr_ident("test/int_list")));
    let items = db.select_map(sel).await.unwrap();
    assert!(items.is_empty());
}
