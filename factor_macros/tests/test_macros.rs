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

#[derive(Entity)]
#[factor(namespace = "test")]
struct Entity1 {
    #[factor(attr = AttrId)]
    pub id: Id,
    #[factor(attr = AttrSomeTitle)]
    pub attr1: String,
    #[factor(attr = AttrDescription)]
    pub attr2: Option<String>,
    #[factor(attr = AttrLength)]
    pub length: Vec<u64>,
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
            name: "test/someTitle".into(),
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
            extend: None,
            strict: false,
        },
        Entity1::schema(),
    );
}
