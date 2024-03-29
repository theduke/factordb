use factdb::{AttributeMeta, ClassAttribute, ClassMeta, Id, ValueType};
use factor_macros::{Attribute, Class};

use factor_core::schema::builtin::AttrDescription;

#[derive(Attribute)]
#[factor(namespace = "test")]
struct AttrSomeTitle(String);

#[derive(Attribute)]
#[factor(namespace = "test")]
struct AttrLength(Vec<u64>);

#[derive(Attribute)]
#[factor(namespace = "test")]
struct AttrFlag(bool);

#[derive(Class, serde::Serialize, serde::Deserialize)]
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

#[derive(Class, serde::Serialize, serde::Deserialize)]
#[factor(namespace = "test")]
struct Child {
    #[factor(attr = AttrFlag)]
    flag: bool,
    #[factor(extend)]
    parent: Entity1,
}

#[test]
fn test_attr_derive() {
    use factdb::Attribute;

    assert_eq!(
        Attribute {
            id: Id::nil(),
            ident: "test/some_title".into(),
            description: None,
            title: None,
            index: false,
            strict: false,
            unique: false,
            value_type: ValueType::String,
        },
        AttrSomeTitle::schema()
    );
}

#[test]
fn test_entity_derive() {
    assert_eq!(
        factdb::Class {
            id: Id::nil(),
            ident: "test/Entity1".into(),
            title: Some("Entity1".to_string()),
            description: None,
            attributes: vec![
                ClassAttribute {
                    attribute: AttrSomeTitle::QUALIFIED_NAME.to_string(),
                    required: true,
                },
                ClassAttribute {
                    attribute: AttrDescription::QUALIFIED_NAME.to_string(),
                    required: false,
                },
                ClassAttribute {
                    attribute: AttrLength::QUALIFIED_NAME.to_string(),
                    required: true,
                },
            ],
            extends: Vec::new(),
            strict: false,
        },
        Entity1::schema(),
    );

    let schema = Child::schema();
    assert_eq!(schema.extends, vec![Entity1::QUALIFIED_NAME.to_string()]);
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
