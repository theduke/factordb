use super::Value;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
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
    Map,

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

    Const(Value),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectType {
    pub name: Option<String>,
    pub fields: Vec<ObjectField>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectField {
    pub name: String,
    pub value_type: ValueType,
}

impl ValueType {
    pub fn is_scalar(&self) -> bool {
        match self {
            Self::Bool
            | Self::Int
            | Self::UInt
            | Self::Float
            | Self::String
            | Self::Bytes
            | Self::DateTime
            | Self::Ref
            | Self::Url
            | Self::Map => true,
            Self::Object(_) => false,
            Self::Union(inner) => inner.iter().all(|t| t.is_scalar()),
            Self::Any | Self::Unit | Self::List(_) => false,
            Self::Const(val) => val.value_type().is_scalar(),
        }
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

// impl ValueTypeDescriptor for u8 {
//     fn value_type() -> ValueType {
//         ValueType::Int
//     }
// }

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
