//! This module defines attributes and entities that are built in to factor.
//! The builtins describe the database schema (attributes, entities, indexes).
//!
//! The code here is collected in a single file rather than split across  the
//! various sibling modules to prevent mistakes with attribute and entity ids,
//! which are statically defined.

use crate::{
    data::{value_type::ConstrainedRefType, Id, IdOrIdent, Ident, ValueType},
    schema::{Attribute, AttributeMeta, Class, ClassAttribute, ClassMeta},
};

use super::IndexSchema;

pub const NS_FACTOR: &str = "factor";

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
pub const ATTR_COUNT: Id = Id::from_u128(14);
pub const ATTR_ATTRIBUTE: Id = Id::from_u128(15);
pub const ATTR_REQUIRED: Id = Id::from_u128(16);
pub const ATTR_CLASSES: Id = Id::from_u128(17);

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

impl AttributeMeta for AttrId {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "id";
    const QUALIFIED_NAME: &'static str = "factor/id";
    type Type = IdOrIdent;

    fn schema() -> Attribute {
        Attribute {
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

impl AttributeMeta for AttrIdent {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "ident";
    const QUALIFIED_NAME: &'static str = "factor/ident";
    type Type = String;

    fn schema() -> Attribute {
        Attribute {
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

impl AttributeMeta for AttrTitle {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "title";
    const QUALIFIED_NAME: &'static str = "factor/title";
    type Type = String;

    fn schema() -> Attribute {
        Attribute {
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

impl AttributeMeta for AttrType {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "type";
    const QUALIFIED_NAME: &'static str = "factor/type";
    type Type = IdOrIdent;

    fn schema() -> Attribute {
        Attribute {
            id: ATTR_TYPE,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Type".into()),
            description: None,
            value_type: ValueType::Ident(ConstrainedRefType {
                allowed_entity_types: vec![Class::IDENT],
            }),
            unique: false,
            index: true,
            strict: true,
        }
    }
}

pub struct AttrValueType;

impl AttributeMeta for AttrValueType {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "valueType";
    const QUALIFIED_NAME: &'static str = "factor/valueType";
    type Type = String;

    fn schema() -> Attribute {
        Attribute {
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

impl AttributeMeta for AttrUnique {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "unique";
    const QUALIFIED_NAME: &'static str = "factor/unique";
    type Type = bool;

    fn schema() -> Attribute {
        Attribute {
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

impl AttributeMeta for AttrIndex {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "index";
    const QUALIFIED_NAME: &'static str = "factor/index";
    type Type = bool;

    fn schema() -> Attribute {
        Attribute {
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

impl AttributeMeta for AttrDescription {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "description";
    const QUALIFIED_NAME: &'static str = "factor/description";
    type Type = String;

    fn schema() -> Attribute {
        Attribute {
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

impl AttributeMeta for AttrStrict {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "isStrict";
    const QUALIFIED_NAME: &'static str = "factor/isStrict";
    type Type = bool;

    fn schema() -> Attribute {
        Attribute {
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

pub struct AttributeConstraint;

impl ClassMeta for AttributeConstraint {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "AttributeConstraint";
    const QUALIFIED_NAME: &'static str = "factor/AttributeConstraint";
    const IDENT: IdOrIdent = IdOrIdent::new_static(Self::QUALIFIED_NAME);

    fn schema() -> Class {
        Class {
            id: Id::nil(),
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Attribute constraint".into()),
            description: None,
            attributes: vec![],
            extends: vec![],
            strict: false,
        }
    }
}

pub struct AttrClasses;

impl AttributeMeta for AttrClasses {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "classes";
    const QUALIFIED_NAME: &'static str = "factor/classes";
    const IDENT: IdOrIdent = IdOrIdent::new_static(Self::QUALIFIED_NAME);

    type Type = Vec<Ident>;

    fn schema() -> Attribute {
        Attribute {
            id: ATTR_CLASSES,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Classes".into()),
            description: None,
            value_type: ValueType::List(Box::new(ValueType::Ref)),
            unique: false,
            index: false,
            strict: false,
        }
    }
}

pub struct AttributeConstraintReferenceClasses {
    pub classes: Vec<Ident>,
}

impl ClassMeta for AttributeConstraintReferenceClasses {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "AttributeConstraintReferenceClasses";
    const QUALIFIED_NAME: &'static str = "factor/AttributeConstraintReferenceClasses";
    const IDENT: IdOrIdent = IdOrIdent::new_static(Self::QUALIFIED_NAME);

    fn schema() -> Class {
        Class {
            id: Id::nil(),
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Reference classes constraint".into()),
            description: None,
            attributes: vec![ClassAttribute {
                attribute: AttrClasses::QUALIFIED_NAME.to_string(),
                required: true,
            }],
            extends: vec![],
            strict: false,
        }
    }
}

impl ClassMeta for super::Attribute {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "Attribute";
    const QUALIFIED_NAME: &'static str = "factor/Attribute";

    fn schema() -> Class {
        Class {
            id: ATTRIBUTE_ID,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Attribute".into()),
            description: None,
            attributes: vec![
                ClassAttribute::from_schema_required::<AttrId>(),
                ClassAttribute::from_schema_required::<AttrIdent>(),
                ClassAttribute::from_schema_optional::<AttrTitle>(),
                ClassAttribute::from_schema_optional::<AttrDescription>(),
                ClassAttribute::from_schema_required::<AttrValueType>(),
                ClassAttribute::from_schema_required::<AttrUnique>(),
                ClassAttribute::from_schema_required::<AttrIndex>(),
                ClassAttribute::from_schema_required::<AttrStrict>(),
            ],
            extends: Vec::new(),
            strict: true,
        }
    }
}

pub struct AttrAttribute;

impl AttributeMeta for AttrAttribute {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "attribute";
    const QUALIFIED_NAME: &'static str = "factor/attribute";
    const IDENT: IdOrIdent = IdOrIdent::new_static(Self::QUALIFIED_NAME);
    type Type = Ident;

    fn schema() -> Attribute {
        Attribute {
            id: ATTR_ATTRIBUTE,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Attribute".to_string()),
            description: Some("Reference to an attribute.".to_string()),
            value_type: ValueType::RefConstrained(ConstrainedRefType::new(vec![Attribute::IDENT])),
            unique: false,
            index: false,
            strict: false,
        }
    }
}

pub struct AttrRequired;

impl AttributeMeta for AttrRequired {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "required";
    const QUALIFIED_NAME: &'static str = "factor/required";
    const IDENT: IdOrIdent = IdOrIdent::new_static(Self::QUALIFIED_NAME);
    type Type = bool;

    fn schema() -> Attribute {
        Attribute {
            id: ATTR_REQUIRED,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Required".to_string()),
            description: None,
            value_type: ValueType::Bool,
            unique: false,
            index: false,
            strict: false,
        }
    }
}

// EntitySchema attributes and entity type.

pub struct AttrClassAttributes;

impl AttributeMeta for AttrClassAttributes {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "classAttributes";
    const QUALIFIED_NAME: &'static str = "factor/classAttributes";
    type Type = Vec<ClassAttribute>;

    fn schema() -> Attribute {
        Attribute {
            id: ATTR_ATTRIBUTES,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Entity Attributes".into()),
            description: None,
            value_type: ValueType::List(Box::new(ValueType::Object(
                crate::data::value_type::ObjectType {
                    name: Some("ObjectType".to_string()),
                    fields: vec![
                        crate::data::value_type::ObjectField {
                            name: "attribute".to_string(),
                            value_type: ValueType::Ident(ConstrainedRefType {
                                allowed_entity_types: vec![Attribute::IDENT],
                            }),
                        },
                        crate::data::value_type::ObjectField {
                            name: "cardinality".to_string(),
                            value_type: ValueType::Union(vec![
                                ValueType::Const("Optional".into()),
                                ValueType::Const("Required".into()),
                            ]),
                        },
                    ],
                },
            ))),
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrExtend;

impl AttributeMeta for AttrExtend {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "extend";
    const QUALIFIED_NAME: &'static str = "factor/extend";
    type Type = Option<IdOrIdent>;

    fn schema() -> Attribute {
        Attribute {
            id: ATTR_EXTEND,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Extends".into()),
            description: None,
            value_type: ValueType::List(Box::new(ValueType::Ident(ConstrainedRefType {
                allowed_entity_types: vec![Class::IDENT],
            }))),
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrIsRelation;

impl AttributeMeta for AttrIsRelation {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "isRelation";
    const QUALIFIED_NAME: &'static str = "factor/isRelation";
    type Type = bool;

    fn schema() -> Attribute {
        Attribute {
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

impl ClassMeta for super::Class {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "Class";
    const QUALIFIED_NAME: &'static str = "factor/Class";

    fn schema() -> Class {
        Class {
            id: ENTITY_ID,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Class".into()),
            description: None,
            attributes: vec![
                ClassAttribute::from_schema_required::<AttrId>(),
                ClassAttribute::from_schema_required::<AttrIdent>(),
                ClassAttribute::from_schema_optional::<AttrTitle>(),
                ClassAttribute::from_schema_optional::<AttrDescription>(),
                ClassAttribute::from_schema_required::<AttrStrict>(),
                ClassAttribute::from_schema_required::<AttrIsRelation>(),
                ClassAttribute::from_schema_required::<AttrExtend>(),
                ClassAttribute::from_schema_required::<AttrClassAttributes>(),
            ],
            extends: Vec::new(),
            strict: true,
        }
    }
}

pub struct AttrCount;

impl AttributeMeta for AttrCount {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "count";
    const QUALIFIED_NAME: &'static str = "factor/count";
    type Type = u64;

    fn schema() -> Attribute {
        Attribute {
            id: ATTR_COUNT,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Count".into()),
            description: None,
            value_type: ValueType::UInt,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

// IndexSchema attributes and entity type.

pub struct AttrIndexAttributes;

impl AttributeMeta for AttrIndexAttributes {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "index_attributes";
    const QUALIFIED_NAME: &'static str = "factor/index_attributes";
    type Type = Vec<Id>;

    fn schema() -> Attribute {
        Attribute {
            id: ATTR_INDEX_ATTRIBUTES,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Indexed Attributes".into()),
            description: None,
            value_type: ValueType::List(Box::new(ValueType::Ref)),
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct IndexSchemaType;

impl ClassMeta for IndexSchemaType {
    const NAMESPACE: &'static str = "factor";
    const PLAIN_NAME: &'static str = "Index";
    const QUALIFIED_NAME: &'static str = "factor/Index";

    fn schema() -> Class {
        Class {
            id: INDEX_ID,
            ident: Self::QUALIFIED_NAME.to_string(),
            title: Some("Index".into()),
            description: None,
            attributes: vec![
                ClassAttribute::from_schema_required::<AttrId>(),
                ClassAttribute::from_schema_required::<AttrIdent>(),
                ClassAttribute::from_schema_optional::<AttrTitle>(),
                ClassAttribute::from_schema_optional::<AttrDescription>(),
                ClassAttribute::from_schema_required::<AttrIndexAttributes>(),
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
        title: Some("Global ident attribute index".into()),
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
            AttrClassAttributes::schema(),
            AttrExtend::schema(),
            AttrIsRelation::schema(),
            AttrIndexAttributes::schema(),
            AttrCount::schema(),
        ],
        classes: vec![
            Attribute::schema(),
            Class::schema(),
            IndexSchemaType::schema(),
        ],
        indexes: vec![index_entity_type(), index_ident()],
    }
}

/// Check if an [`Id`] is a builtin entity *type*.
#[inline]
pub fn id_is_builtin_entity_type(id: Id) -> bool {
    matches!(id, ATTRIBUTE_ID | ENTITY_ID | INDEX_ID)
}
