use crate::{
    data::{Id, Ident, ValueType},
    schema::{
        AttributeDescriptor, AttributeSchema, Cardinality, EntityAttribute, EntityDescriptor,
        EntitySchema,
    },
};

// Built-in attributes.
// Constants are kept together to see ids at a glance.
pub const ATTR_ID: Id = Id::from_u128(1);
pub const ATTR_IDENT: Id = Id::from_u128(2);
pub const ATTR_TITLE: Id = Id::from_u128(3);
pub const ATTR_TYPE: Id = Id::from_u128(4);
pub const ATTR_VALUETYPE: Id = Id::from_u128(5);
pub const ATTR_UNIQUE: Id = Id::from_u128(6);
pub const ATTR_INDEX: Id = Id::from_u128(7);
pub const ATTR_DESCRIPTION: Id = Id::from_u128(8);
pub const ATTR_STRICT: Id = Id::from_u128(9);
const ATTR_ATTRIBUTES: Id = Id::from_u128(10);
const ATTR_EXTEND: Id = Id::from_u128(11);
const ATTR_ISRELATION: Id = Id::from_u128(12);

// Built-in entity types.
// Constants are kept together to see ids at a glance.
pub const ATTRIBUTE_ID: Id = Id::from_u128(1000);
pub const ENTITY_ID: Id = Id::from_u128(1001);

pub struct AttrId;

impl AttributeDescriptor for AttrId {
    const NAME: &'static str = "factor/id";
    type Type = Ident;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_ID,
            name: Self::NAME.to_string(),
            title: Some("Id".into()),
            description: None,
            value_type: ValueType::Ref,
            unique: true,
            index: true,
            strict: true,
        }
    }
}

pub struct AttrIdent;

impl AttributeDescriptor for AttrIdent {
    const NAME: &'static str = "factor/ident";
    type Type = String;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_IDENT,
            name: Self::NAME.to_string(),
            title: Some("Ident".into()),
            description: None,
            value_type: ValueType::String,
            unique: true,
            index: true,
            strict: true,
        }
    }
}

pub struct AttrTitle;

impl AttributeDescriptor for AttrTitle {
    const NAME: &'static str = "factor/title";
    type Type = String;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_TITLE,
            name: Self::NAME.to_string(),
            title: Some("Title".into()),
            description: None,
            value_type: ValueType::String,
            unique: true,
            index: true,
            strict: true,
        }
    }
}

pub struct AttrType;

impl AttributeDescriptor for AttrType {
    const NAME: &'static str = "factor/type";
    type Type = Ident;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_TYPE,
            name: Self::NAME.to_string(),
            title: Some("Type".into()),
            description: None,
            value_type: ValueType::Ref,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrValueType;

impl AttributeDescriptor for AttrValueType {
    const NAME: &'static str = "factor/valueType";
    type Type = String;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_VALUETYPE,
            name: Self::NAME.to_string(),
            title: Some("Value Type".into()),
            description: None,
            value_type: ValueType::String,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrUnique;

impl AttributeDescriptor for AttrUnique {
    const NAME: &'static str = "factor/unique";
    type Type = bool;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_UNIQUE,
            name: Self::NAME.to_string(),
            title: Some("Unique".into()),
            description: None,
            value_type: ValueType::Bool,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrIndex;

impl AttributeDescriptor for AttrIndex {
    const NAME: &'static str = "factor/index";
    type Type = bool;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_INDEX,
            name: Self::NAME.to_string(),
            title: Some("Index".into()),
            description: None,
            value_type: ValueType::Bool,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrDescription;

impl AttributeDescriptor for AttrDescription {
    const NAME: &'static str = "factor/description";
    type Type = String;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_DESCRIPTION,
            name: Self::NAME.to_string(),
            title: Some("Description".into()),
            description: None,
            value_type: ValueType::String,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrStrict;

impl AttributeDescriptor for AttrStrict {
    const NAME: &'static str = "factor/isStrict";
    type Type = bool;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_STRICT,
            name: Self::NAME.to_string(),
            title: Some("Strict".into()),
            description: None,
            value_type: ValueType::Bool,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttributeSchemaType;

impl EntityDescriptor for AttributeSchemaType {
    const NAME: &'static str = "factor/Attribute";

    fn schema() -> EntitySchema {
        EntitySchema {
            id: ATTRIBUTE_ID,
            name: Self::NAME.to_string(),
            title: Some("Attribute".into()),
            description: None,
            attributes: vec![
                ATTR_ID.into(),
                ATTR_IDENT.into(),
                EntityAttribute::from(ATTR_TITLE).into_optional(),
                EntityAttribute::from(ATTR_DESCRIPTION).into_optional(),
                ATTR_VALUETYPE.into(),
                ATTR_UNIQUE.into(),
                ATTR_INDEX.into(),
                ATTR_STRICT.into(),
            ],
            extends: Vec::new(),
            strict: true,
        }
    }
}

pub struct AttrAttributes;

impl AttributeDescriptor for AttrAttributes {
    const NAME: &'static str = "factor/entityAttributes";
    type Type = Vec<EntityAttribute>;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_ATTRIBUTES,
            name: Self::NAME.to_string(),
            title: Some("Entity Attributes".into()),
            description: None,
            value_type: ValueType::Object(crate::data::value::ObjectType {
                fields: vec![
                    crate::data::value::ObjectField {
                        name: "attribute".to_string(),
                        value_type: ValueType::Ref,
                    },
                    crate::data::value::ObjectField {
                        name: "value_type".to_string(),
                        value_type: ValueType::Any,
                    },
                ],
            }),
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrExtend;

impl AttributeDescriptor for AttrExtend {
    const NAME: &'static str = "factor/extend";
    type Type = Option<Ident>;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_EXTEND,
            name: Self::NAME.to_string(),
            title: Some("Extends".into()),
            description: None,
            value_type: ValueType::Bool,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrIsRelation;

impl AttributeDescriptor for AttrIsRelation {
    const NAME: &'static str = "factor/isRelation";
    type Type = bool;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_ISRELATION,
            name: Self::NAME.to_string(),
            title: Some("Is Relation".into()),
            description: None,
            value_type: ValueType::Bool,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct EntitySchemaType;

impl EntityDescriptor for EntitySchemaType {
    const NAME: &'static str = "factor/Entity";

    fn schema() -> EntitySchema {
        EntitySchema {
            id: ENTITY_ID,
            name: Self::NAME.to_string(),
            title: Some("Entity".into()),
            description: None,
            attributes: vec![
                ATTR_ID.into(),
                ATTR_IDENT.into(),
                EntityAttribute::from(ATTR_TITLE).into_optional(),
                EntityAttribute::from(ATTR_DESCRIPTION).into_optional(),
                ATTR_STRICT.into(),
                ATTR_ISRELATION.into(),
                EntityAttribute {
                    attribute: ATTR_EXTEND.into(),
                    cardinality: Cardinality::Many,
                },
                EntityAttribute {
                    attribute: ATTR_ATTRIBUTES.into(),
                    cardinality: Cardinality::Many,
                },
            ],
            extends: Vec::new(),
            strict: true,
        }
    }
}

pub fn id_is_builtin_entity_type(id: Id) -> bool {
    match id {
        ATTRIBUTE_ID | ENTITY_ID => true,
        _ => false,
    }
}

pub fn id_is_builtin_entity_filter() -> crate::query::expr::Expr {
    use crate::query::expr::Expr;
    // TODO: use IN query
    let a = Expr::neq(Expr::ident(ATTR_ID), Expr::literal(ATTRIBUTE_ID));
    let b = Expr::neq(Expr::ident(ATTR_ID), Expr::literal(ENTITY_ID));

    Expr::or(a, b)
}
