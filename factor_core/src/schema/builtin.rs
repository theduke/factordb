use crate::{
    data::{Id, ValueType},
    schema::{AttributeDescriptor, AttributeSchema, EntityDescriptor, EntitySchema},
};

pub struct AttrId;

pub const ATTR_ID: Id = Id::from_u128(1);

impl AttributeDescriptor for AttrId {
    const NAME: &'static str = "factor/id";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_ID,
            name: Self::NAME.to_string(),
            description: None,
            value_type: ValueType::Ref,
            unique: true,
            index: true,
            strict: true,
        }
    }
}

pub struct AttrIdent;

pub const ATTR_IDENT: Id = Id::from_u128(2);

impl AttributeDescriptor for AttrIdent {
    const NAME: &'static str = "factor/ident";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_IDENT,
            name: Self::NAME.to_string(),
            description: None,
            value_type: ValueType::String,
            unique: true,
            index: true,
            strict: true,
        }
    }
}

pub struct AttrType;

pub const ATTR_TYPE: Id = Id::from_u128(12);

impl AttributeDescriptor for AttrType {
    const NAME: &'static str = "factor/type";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_TYPE,
            name: Self::NAME.to_string(),
            description: None,
            value_type: ValueType::Ref,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrValueType;

pub const ATTR_VALUETYPE: Id = Id::from_u128(3);

impl AttributeDescriptor for AttrValueType {
    const NAME: &'static str = "factor/valueType";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_VALUETYPE,
            name: Self::NAME.to_string(),
            description: None,
            value_type: ValueType::String,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrUnique;

pub const ATTR_UNIQUE: Id = Id::from_u128(4);

impl AttributeDescriptor for AttrUnique {
    const NAME: &'static str = "factor/unique";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_UNIQUE,
            name: Self::NAME.to_string(),
            description: None,
            value_type: ValueType::Bool,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrIndex;

pub const ATTR_INDEX: Id = Id::from_u128(6);

impl AttributeDescriptor for AttrIndex {
    const NAME: &'static str = "factor/index";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_INDEX,
            name: Self::NAME.to_string(),
            description: None,
            value_type: ValueType::Bool,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrDescription;

pub const ATTR_DESCRIPTION: Id = Id::from_u128(7);

impl AttributeDescriptor for AttrDescription {
    const NAME: &'static str = "factor/description";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_DESCRIPTION,
            name: Self::NAME.to_string(),
            description: None,
            value_type: ValueType::String,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrStrict;

pub const ATTR_STRICT: Id = Id::from_u128(8);

impl AttributeDescriptor for AttrStrict {
    const NAME: &'static str = "factor/isStrict";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_STRICT,
            name: Self::NAME.to_string(),
            description: None,
            value_type: ValueType::Bool,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttributeSchemaType;

pub const ATTRIBUTE_ID: Id = Id::from_u128(1000);

impl EntityDescriptor for AttributeSchemaType {
    const NAME: &'static str = "factor/Attr";

    fn schema() -> EntitySchema {
        EntitySchema {
            id: ATTRIBUTE_ID,
            name: Self::NAME.to_string(),
            description: None,
            attributes: vec![
                ATTR_ID.into(),
                ATTR_IDENT.into(),
                ATTR_DESCRIPTION.into(),
                ATTR_VALUETYPE.into(),
                ATTR_UNIQUE.into(),
                ATTR_INDEX.into(),
                ATTR_STRICT.into(),
            ],
            extend: None,
            strict: true,
            is_relation: false,
            from: None,
            to: None,
        }
    }
}

pub struct AttrAttributes;

const ATTR_ATTRIBUTES: Id = Id::from_u128(9);

impl AttributeDescriptor for AttrAttributes {
    const NAME: &'static str = "factor/attributes";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_ATTRIBUTES,
            name: Self::NAME.to_string(),
            description: None,
            value_type: ValueType::Ref,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrExtend;

const ATTR_EXTEND: Id = Id::from_u128(10);

impl AttributeDescriptor for AttrExtend {
    const NAME: &'static str = "factor/extend";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_EXTEND,
            name: Self::NAME.to_string(),
            description: None,
            value_type: ValueType::Bool,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct AttrIsRelation;

const ATTR_ISRELATION: Id = Id::from_u128(11);

impl AttributeDescriptor for AttrIsRelation {
    const NAME: &'static str = "factor/isRelation";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: ATTR_ISRELATION,
            name: Self::NAME.to_string(),
            description: None,
            value_type: ValueType::Bool,
            unique: false,
            index: false,
            strict: true,
        }
    }
}

pub struct EntitySchemaType;

pub const ENTITY_ID: Id = Id::from_u128(1001);

impl EntityDescriptor for EntitySchemaType {
    const NAME: &'static str = "factor/Entity";

    fn schema() -> EntitySchema {
        EntitySchema {
            id: ENTITY_ID,
            name: Self::NAME.to_string(),
            description: None,
            attributes: vec![
                ATTR_ID.into(),
                ATTR_IDENT.into(),
                ATTR_DESCRIPTION.into(),
                ATTR_ATTRIBUTES.into(),
                ATTR_EXTEND.into(),
                ATTR_STRICT.into(),
                ATTR_ISRELATION.into(),
            ],
            extend: None,
            strict: true,
            is_relation: false,
            from: None,
            to: None,
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
