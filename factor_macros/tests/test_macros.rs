use factor_macros::{Attribute, Entity};
use factordb::{
    data::{Id, ValueType},
    schema::{builtin::AttrDescription, AttributeDescriptor, EntityAttribute, EntityDescriptor},
};

#[derive(Attribute)]
#[factor(namespace = "test")]
struct AttrSomeTitle(String);

#[derive(Attribute)]
#[factor(namespace = "test")]
struct AttrLength(u64);

#[derive(Attribute)]
#[factor(namespace = "test")]
struct AttrFlag(bool);

#[derive(Entity)]
#[factor(namespace = "test")]
struct Entity1 {
    #[factor(attr = AttrId)]
    pub id: Id,
    #[factor(attr = AttrSomeTitle)]
    pub text: String,
    #[factor(attr = AttrDescription)]
    pub text_opt: Option<String>,
    #[factor(attr = AttrLength)]
    pub length: Vec<u64>,
}

#[derive(Entity)]
#[factor(namespace = "test")]
struct Child {
    #[factor(attr = AttrFlag)]
    flag: bool,
    #[factor(extend)]
    parent: Entity1,
}

#[test]
fn test_attr_derive() {
    assert_eq!(
        factordb::schema::AttributeSchema {
            id: Id::nil(),
            description: None,
            index: false,
            strict: false,
            unique: false,
            name: "test/some_title".into(),
            value_type: ValueType::String,
        },
        AttrSomeTitle::schema()
    );
}

#[test]
fn test_entity_derive() {
    assert_eq!(
        factordb::schema::EntitySchema {
            id: Id::nil(),
            name: "test/Entity1".into(),
            description: None,
            attributes: vec![
                EntityAttribute {
                    attribute: AttrSomeTitle::IDENT,
                    cardinality: factordb::schema::Cardinality::Required,
                },
                EntityAttribute {
                    attribute: AttrDescription::IDENT,
                    cardinality: factordb::schema::Cardinality::Optional,
                },
                EntityAttribute {
                    attribute: AttrLength::IDENT,
                    cardinality: factordb::schema::Cardinality::Many,
                },
            ],
            extends: Vec::new(),
            strict: false,
        },
        Entity1::schema(),
    );

    let schema = Child::schema();
    assert_eq!(schema.extends, vec![Entity1::IDENT]);
}

// #[test]
// fn test_derive_entity_serialize() {
//     let e = Child {
//         parent: Entity1 {
//             id: Id::nil(),
//             text: "a".into(),
//             text_opt: Some("b".into()),
//             length: vec![42],
//         },
//         flag: true,
//     };

//     let val = serde_json::to_value(e).unwrap();
//     assert_eq!(
//         serde_json::json!({
//             "factor/id": "0",
//             "test/text": "a",
//             "test/text_opt": "b",
//             "test/flag": true,
//         }),
//         val
//     );
// }
