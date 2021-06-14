use factor_core::{
    data::{Id, ValueType},
    schema::AttributeDescriptor,
};
use factor_macros::{Attribute, Entity};

#[derive(Attribute)]
#[factor(namespace = "test", value = ValueType::String)]
struct Attr1;

#[derive(Entity)]
#[factor(namespace = "test")]
struct Entity1 {
    #[factor(attr = AttrId)]
    pub id: Id,
    #[factor(attr = Attr1)]
    pub attr1: String,
}

#[test]
fn test_attr_derive() {
    assert_eq!(
        factor_core::schema::AttributeSchema {
            id: Id::nil(),
            description: None,
            index: false,
            strict: false,
            unique: false,
            name: "test/attr1".into(),
            value_type: ValueType::String,
        },
        Attr1::schema()
    );
}
