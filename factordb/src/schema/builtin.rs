//! This module defines attributes and entities that are built in to factor.
//! The builtins describe the database schema (attributes, entities, indexes).
//!
//! The code here is collected in a single file rather than split across  the
//! various sibling modules to prevent mistakes with attribute and entity ids,
//! which are statically defined.

use crate::{
    data::{Id, Ident, ValueType},
    schema::{
        AttributeDescriptor, AttributeSchema, Cardinality, EntityAttribute, EntityDescriptor,
        EntitySchema,
    },
};

use super::IndexSchema;

pub const NS_FACTOR: &'static str = "factor";

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
const ATTR_INDEX_ATTRIBUTES: Id = Id::from_u128(13);

// Built-in entity types.
// Constants are kept together to see ids at a glance.
pub const ATTRIBUTE_ID: Id = Id::from_u128(1000);
pub const ENTITY_ID: Id = Id::from_u128(1001);
pub const INDEX_ID: Id = Id::from_u128(1002);

// Built-in indexes.
// Constants are kept together to see ids at a glance.
pub const INDEX_ENTITY_TYPE: Id = Id::from_u128(2001);
pub const INDEX_IDENT: Id = Id::from_u128(2002);

pub struct AttrId;

impl AttributeDescriptor for AttrId {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "id";
    const QUALIFIED_NAME: &'static str = "factor/id";
    type Type = Ident;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_ID,
            ident: Self::QUALIFIED_NAME.to_string(),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "ident";
    const QUALIFIED_NAME: &'static str = "factor/ident";
    type Type = String;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_IDENT,
            ident: Self::QUALIFIED_NAME.to_string(),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "title";
    const QUALIFIED_NAME: &'static str = "factor/title";
    type Type = String;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_TITLE,
            ident: Self::QUALIFIED_NAME.to_string(),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "type";
    const QUALIFIED_NAME: &'static str = "factor/type";
    type Type = Ident;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_TYPE,
            ident: Self::QUALIFIED_NAME.to_string(),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "valueType";
    const QUALIFIED_NAME: &'static str = "factor/valueType";
    type Type = String;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_VALUETYPE,
            ident: Self::QUALIFIED_NAME.to_string(),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "unique";
    const QUALIFIED_NAME: &'static str = "factor/unique";
    type Type = bool;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_UNIQUE,
            ident: Self::QUALIFIED_NAME.to_string(),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "index";
    const QUALIFIED_NAME: &'static str = "factor/index";
    type Type = bool;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_INDEX,
            ident: Self::QUALIFIED_NAME.to_string(),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "description";
    const QUALIFIED_NAME: &'static str = "factor/description";
    type Type = String;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_DESCRIPTION,
            ident: Self::QUALIFIED_NAME.to_string(),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "isStrict";
    const QUALIFIED_NAME: &'static str = "factor/isStrict";
    type Type = bool;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_STRICT,
            ident: Self::QUALIFIED_NAME.to_string(),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "Attribute";
    const QUALIFIED_NAME: &'static str = "factor/Attribute";

    fn schema() -> EntitySchema {
        EntitySchema {
            id: ATTRIBUTE_ID,
            ident: Self::QUALIFIED_NAME.to_string(),
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

// EntitySchema attributes and entity type.

pub struct AttrAttributes;

impl AttributeDescriptor for AttrAttributes {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "entityAttributes";
    const QUALIFIED_NAME: &'static str = "factor/entityAttributes";
    type Type = Vec<EntityAttribute>;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_ATTRIBUTES,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Entity Attributes".into()),
            description: None,
            value_type: ValueType::Object(crate::data::value::ObjectType {
                name: Some("ObjectType".to_string()),
                fields: vec![
                    crate::data::value::ObjectField {
                        name: "attribute".to_string(),
                        value_type: ValueType::Ref,
                    },
                    crate::data::value::ObjectField {
                        name: "cardinality".to_string(),
                        value_type: ValueType::Union(vec![
                            ValueType::Const("Optional".into()),
                            ValueType::Const("Many".into()),
                            ValueType::Const("Required".into()),
                        ]),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "extend";
    const QUALIFIED_NAME: &'static str = "factor/extend";
    type Type = Option<Ident>;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_EXTEND,
            ident: Self::QUALIFIED_NAME.to_string(),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "isRelation";
    const QUALIFIED_NAME: &'static str = "factor/isRelation";
    type Type = bool;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_ISRELATION,
            ident: Self::QUALIFIED_NAME.to_string(),
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
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "Entity";
    const QUALIFIED_NAME: &'static str = "factor/Entity";

    fn schema() -> EntitySchema {
        EntitySchema {
            id: ENTITY_ID,
            ident: Self::QUALIFIED_NAME.to_string(),
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

// IndexSchema attributes and entity type.

pub struct AttrIndexAttributes;

impl AttributeDescriptor for AttrIndexAttributes {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "index_attributes";
    const QUALIFIED_NAME: &'static str = "factor/index_attributes";
    type Type = Vec<Id>;

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_INDEX_ATTRIBUTES,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Indexed Attributes".into()),
            description: None,
            value_type: ValueType::Ref,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct IndexSchemaType;

impl EntityDescriptor for IndexSchemaType {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "Index";
    const QUALIFIED_NAME: &'static str = "factor/Index";

    fn schema() -> EntitySchema {
        EntitySchema {
            id: INDEX_ID,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Entity".into()),
            description: None,
            attributes: vec![
                ATTR_ID.into(),
                ATTR_IDENT.into(),
                EntityAttribute::from(ATTR_TITLE).into_optional(),
                EntityAttribute::from(ATTR_DESCRIPTION).into_optional(),
                EntityAttribute {
                    attribute: ATTR_INDEX_ATTRIBUTES.into(),
                    cardinality: Cardinality::Many,
                },
            ],
            extends: Vec::new(),
            strict: true,
        }
    }
}

fn index_entity_type() -> IndexSchema {
    IndexSchema {
        id: INDEX_ENTITY_TYPE,
        ident: "factor/index_entity_type".into(),
        title: Some("Global entity type attribute index".into()),
        attributes: vec![ATTR_TYPE],
        description: None,
        unique: false,
    }
}

fn index_ident() -> IndexSchema {
    IndexSchema {
        id: INDEX_IDENT,
        ident: "factor/index_ident".into(),
        title: Some("Globabl ident attribute index".into()),
        attributes: vec![ATTR_IDENT],
        description: None,
        unique: true,
    }
}

pub fn builtin_db_schema() -> super::DbSchema {
    super::DbSchema {
        attributes: vec![
            AttrId::schema(),
            AttrIdent::schema(),
            AttrTitle::schema(),
            AttrDescription::schema(),
            AttrType::schema(),
            AttrValueType::schema(),
            AttrUnique::schema(),
            AttrIndex::schema(),
            AttrStrict::schema(),
            AttrAttributes::schema(),
            AttrExtend::schema(),
            AttrIsRelation::schema(),
            AttrIndexAttributes::schema(),
        ],
        entities: vec![
            AttributeSchemaType::schema(),
            EntitySchemaType::schema(),
            IndexSchemaType::schema(),
        ],
        indexes: vec![index_entity_type(), index_ident()],
    }
}

/// Check if an [`Id`] is a builtin entity *type*.
#[inline]
pub fn id_is_builtin_entity_type(id: Id) -> bool {
    match id {
        ATTRIBUTE_ID | ENTITY_ID | INDEX_ID => true,
        _ => false,
    }
}

/// Builds an [`Expr`] filter that excludes builtin entities.
pub fn id_is_builtin_entity_filter() -> crate::query::expr::Expr {
    use crate::query::expr::Expr;
    Expr::not(Expr::in_(
        Expr::attr::<AttrType>(),
        vec![
            AttributeSchemaType::QUALIFIED_NAME,
            EntitySchemaType::QUALIFIED_NAME,
            IndexSchemaType::QUALIFIED_NAME,
        ],
    ))
}
