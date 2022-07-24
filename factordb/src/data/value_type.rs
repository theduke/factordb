use super::{IdOrIdent, Value};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub enum ValueType {
    Any,

    Unit,

    Bool,
    Int,
    UInt,
    Float,
    String,
    Bytes,

    // Containers.
    List(Box<Self>),
    Map(Box<MapType>),

    /// A union of different types.
    Union(Vec<Self>),
    Object(ObjectType),

    // Custom types.
    // NOTE: these types may not be directly represented by [`Value`], but
    // rather take the canonical underlying representation.
    DateTime,
    /// Represented as Value::String
    Url,
    /// Reference to an entity id.
    Ref,
    /// Reference to an entity using it's ident.
    Ident(ConstrainedRefType),

    /// Reference to entities with a constrained type.
    // TODO: merge with Ref variant on next format breaking change
    RefConstrained(ConstrainedRefType),

    Const(Value),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct ConstrainedRefType {
    pub allowed_entity_types: Vec<IdOrIdent>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct MapType {
    pub key: ValueType,
    pub value: ValueType,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct ObjectType {
    pub name: Option<String>,
    pub fields: Vec<ObjectField>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct ObjectField {
    pub name: String,
    pub value_type: ValueType,
}

impl ValueType {
    pub fn new_list(inner: ValueType) -> Self {
        ValueType::List(Box::new(inner))
    }

    pub fn is_scalar(&self) -> bool {
        match self {
            Self::Bool
            | Self::Int
            | Self::UInt
            | Self::Float
            | Self::String
            | Self::Bytes
            | Self::DateTime
            | Self::Ident(_)
            | Self::Ref
            | Self::RefConstrained(_)
            | Self::Url
            | Self::Map(..) => {
                // TODO: this is probably not the right thing to do...
                true
            }
            Self::Object(_) => false,
            Self::Union(inner) => inner.iter().all(|t| t.is_scalar()),
            Self::Any | Self::Unit | Self::List(_) => false,
            Self::Const(val) => val.value_type().is_scalar(),
        }
    }

    pub fn is_list(&self) -> bool {
        match self {
            Self::List(_) => true,
            _ => false,
        }
    }

    /// Compute the value type of this value.
    pub fn for_value(value: &Value) -> Self {
        match value {
            Value::Unit => Self::Unit,
            Value::Bool(_) => Self::Bool,
            Value::UInt(_) => Self::UInt,
            Value::Int(_) => Self::Int,
            Value::Float(_) => Self::Float,
            Value::String(_) => Self::String,
            Value::Bytes(_) => Self::Bytes,
            Value::List(items) => Self::List(Box::new(Self::for_list(items.iter()))),
            Value::Map(map) => {
                let key = Self::for_list(map.keys());
                let value = Self::for_list(map.keys());
                Self::Map(Box::new(MapType { key, value }))
            }
            Value::Id(_) => Self::Ref,
        }
    }

    fn for_list<'a, I>(iter: I) -> Self
    where
        I: Iterator<Item = &'a Value>,
    {
        let mut types = Vec::new();
        for item in iter {
            let ty = item.value_type();
            if !types.contains(&ty) {
                types.push(ty);
            }
        }

        let inner_ty = if types.len() == 1 {
            types.pop().unwrap()
        } else {
            Self::Union(types)
        };
        inner_ty
    }
}

/// Trait that allows to statically determine the value type of a Rust type.
pub trait ValueTypeDescriptor {
    fn value_type() -> ValueType;
}

impl ValueTypeDescriptor for bool {
    fn value_type() -> ValueType {
        ValueType::Bool
    }
}

impl ValueTypeDescriptor for i8 {
    fn value_type() -> ValueType {
        ValueType::Int
    }
}

impl ValueTypeDescriptor for i16 {
    fn value_type() -> ValueType {
        ValueType::Int
    }
}

impl ValueTypeDescriptor for i32 {
    fn value_type() -> ValueType {
        ValueType::Int
    }
}

impl ValueTypeDescriptor for i64 {
    fn value_type() -> ValueType {
        ValueType::Int
    }
}

/* impl ValueTypeDescriptor for u8 {
    fn value_type() -> ValueType {
        ValueType::Int
    }
} */

impl ValueTypeDescriptor for u16 {
    fn value_type() -> ValueType {
        ValueType::Int
    }
}

impl ValueTypeDescriptor for u32 {
    fn value_type() -> ValueType {
        ValueType::Int
    }
}

impl ValueTypeDescriptor for u64 {
    fn value_type() -> ValueType {
        ValueType::Int
    }
}

impl ValueTypeDescriptor for f32 {
    fn value_type() -> ValueType {
        ValueType::Float
    }
}

impl ValueTypeDescriptor for f64 {
    fn value_type() -> ValueType {
        ValueType::Float
    }
}

impl ValueTypeDescriptor for String {
    fn value_type() -> ValueType {
        ValueType::String
    }
}

impl<T: ValueTypeDescriptor> ValueTypeDescriptor for Vec<T> {
    fn value_type() -> ValueType {
        ValueType::List(Box::new(T::value_type()))
    }
}

impl ValueTypeDescriptor for super::Timestamp {
    fn value_type() -> ValueType {
        ValueType::DateTime
    }
}

impl ValueTypeDescriptor for url::Url {
    fn value_type() -> ValueType {
        ValueType::Url
    }
}
