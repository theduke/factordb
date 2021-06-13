use crate::{
    data::{Id, ValueType},
    schema::{AttributeDescriptor, AttributeSchema, EntityDescriptor, EntitySchema},
};

pub struct AttrId;

impl AttributeDescriptor for AttrId {
    const ID: Id = Id::from_u128(1);
    const NAME: &'static str = "factor/id";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: Self::ID,
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

impl AttributeDescriptor for AttrIdent {
    const ID: Id = Id::from_u128(2);
    const NAME: &'static str = "factor/ident";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: Self::ID,
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

impl AttributeDescriptor for AttrType {
    const ID: Id = Id::from_u128(12);
    const NAME: &'static str = "factor/type";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: Self::ID,
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

impl AttributeDescriptor for AttrValueType {
    const ID: Id = Id::from_u128(3);
    const NAME: &'static str = "factor/valueType";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: Self::ID,
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

impl AttributeDescriptor for AttrUnique {
    const ID: Id = Id::from_u128(4);
    const NAME: &'static str = "factor/unique";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: Self::ID,
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

impl AttributeDescriptor for AttrIndex {
    const ID: Id = Id::from_u128(6);
    const NAME: &'static str = "factor/index";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: Self::ID,
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

impl AttributeDescriptor for AttrDescription {
    const ID: Id = Id::from_u128(7);
    const NAME: &'static str = "factor/description";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: Self::ID,
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

impl AttributeDescriptor for AttrStrict {
    const ID: Id = Id::from_u128(8);
    const NAME: &'static str = "factor/isStrict";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: Self::ID,
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

impl EntityDescriptor for AttributeSchemaType {
    const ID: Id = Id::from_u128(1000);
    const NAME: &'static str = "factor/Attr";

    fn schema() -> EntitySchema {
        EntitySchema {
            id: Self::ID,
            name: Self::NAME.to_string(),
            description: None,
            attributes: vec![
                AttrId::ID.into(),
                AttrIdent::ID.into(),
                AttrDescription::ID.into(),
                AttrValueType::ID.into(),
                AttrUnique::ID.into(),
                AttrIndex::ID.into(),
                AttrStrict::ID.into(),
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

impl AttributeDescriptor for AttrAttributes {
    const ID: Id = Id::from_u128(9);
    const NAME: &'static str = "factor/attributes";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: Self::ID,
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

impl AttributeDescriptor for AttrExtend {
    const ID: Id = Id::from_u128(10);
    const NAME: &'static str = "factor/extend";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: Self::ID,
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

impl AttributeDescriptor for AttrIsRelation {
    const ID: Id = Id::from_u128(11);
    const NAME: &'static str = "factor/isRelation";

    fn schema() -> AttributeSchema {
        AttributeSchema {
            id: Self::ID,
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

impl EntityDescriptor for EntitySchemaType {
    const ID: Id = Id::from_u128(1001);
    const NAME: &'static str = "factor/Entity";

    fn schema() -> EntitySchema {
        EntitySchema {
            id: Self::ID,
            name: Self::NAME.to_string(),
            description: None,
            attributes: vec![
                AttrId::ID.into(),
                AttrIdent::ID.into(),
                AttrDescription::ID.into(),
                AttrAttributes::ID.into(),
                AttrExtend::ID.into(),
                AttrStrict::ID.into(),
                AttrIsRelation::ID.into(),
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
        AttributeSchemaType::ID | EntitySchemaType::ID => true,
        _ => false,
    }
}

pub fn id_is_builtin_entity_filter() -> crate::query::expr::Expr {
    use crate::query::expr::Expr;
    // TODO: use IN query
    let a = Expr::neq(
        Expr::ident(AttrType::ID),
        Expr::literal(EntitySchemaType::ID),
    );
    let b = Expr::neq(
        Expr::ident(AttrType::ID),
        Expr::literal(AttributeSchemaType::ID),
    );

    Expr::or(a, b)
}
