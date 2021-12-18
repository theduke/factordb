use fnv::FnvHashMap;
use ordered_float::OrderedFloat;

use crate::{
    data::{Id, Value},
    query::expr,
    registry::{LocalAttributeId, ATTR_ID_LOCAL},
};

// SharedStr

#[derive(Clone, Hash, Debug, PartialOrd, Ord)]
pub(super) struct SharedStr(std::sync::Arc<str>);

impl SharedStr {
    pub fn from_string(value: String) -> Self {
        Self(std::sync::Arc::from(value))
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    // #[inline]
    // pub fn strong_count(&self) -> usize {
    //     std::sync::Arc::strong_count(&self.0)
    // }
}

impl PartialEq for SharedStr {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // self.0.as_ptr() == other.0.as_ptr()
        self.0.as_ref() == other.0.as_ref()
    }
}

impl AsRef<str> for SharedStr {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Eq for SharedStr {}

// MemoryValue

// Value for in-memory storage.
// Uses shared strings to save memory usage.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Clone)]
pub(super) enum MemoryValue {
    Unit,

    Bool(bool),
    UInt(u64),
    Int(i64),
    Float(OrderedFloat<f64>),
    String(SharedStr),
    Bytes(Vec<u8>),

    List(Vec<Self>),
    Map(std::collections::BTreeMap<Self, Self>),

    Id(Id),
}

impl MemoryValue {
    // #[inline]
    // pub fn is_bool(&self) -> bool {
    //     match self {
    //         Self::Bool(_) => true,
    //         _ => false,
    //     }
    // }

    pub fn is_true(&self) -> bool {
        match self {
            Self::Bool(b) => *b,
            _ => false,
        }
    }

    /// Returns the boolean value if this is a [`MemoryValue::Bool`], or false
    /// otherwise.
    #[inline]
    pub fn as_bool_discard_other(&self) -> bool {
        match self {
            Self::Bool(flag) => *flag,
            _ => false,
        }
    }

    pub fn to_value(&self) -> Value {
        use MemoryValue as V;
        match self {
            V::Unit => Value::Unit,
            V::Bool(v) => Value::Bool(*v),
            V::UInt(v) => Value::UInt(*v),
            V::Int(v) => Value::Int(*v),
            V::Float(v) => Value::Float(*v),
            V::String(v) => Value::String(v.to_string()),
            V::Bytes(v) => Value::Bytes(v.clone()),
            V::List(v) => Value::List(v.into_iter().map(Into::into).collect()),
            V::Map(v) => Value::Map(
                v.into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
            ),
            V::Id(v) => Value::Id(*v),
        }
    }

    /// Convert a [`Value`] into a [`MemoryValue`] without any smart string
    /// interning.
    pub fn from_value_standalone(val: Value) -> Self {
        match val {
            Value::Unit => Self::Unit,
            Value::Bool(v) => Self::Bool(v),
            Value::UInt(v) => Self::UInt(v),
            Value::Int(v) => Self::Int(v),
            Value::Float(v) => Self::Float(v),
            Value::String(v) => Self::String(SharedStr::from_string(v)),
            Value::Bytes(v) => Self::Bytes(v),
            Value::List(v) => Self::List(v.into_iter().map(Self::from_value_standalone).collect()),
            Value::Map(v) => Self::Map(
                v.0.into_iter()
                    .map(|(key, value)| {
                        (
                            Self::from_value_standalone(key),
                            Self::from_value_standalone(value),
                        )
                    })
                    .collect(),
            ),
            Value::Id(v) => Self::Id(v),
        }
    }

    // pub fn as_id_ref(&self) -> Option<&Id> {
    //     if let Self::Id(id) = self {
    //         Some(id)
    //     } else {
    //         None
    //     }
    // }

    pub(super) fn as_id(&self) -> Option<Id> {
        if let Self::Id(v) = self {
            Some(*v)
        } else {
            None
        }
    }
}

impl<'a> From<&'a MemoryValue> for Value {
    fn from(v: &'a MemoryValue) -> Self {
        v.to_value()
    }
}

// MemoryTuple

#[derive(Debug, Clone)]
pub(super) struct MemoryTuple(pub FnvHashMap<LocalAttributeId, MemoryValue>);

impl MemoryTuple {
    #[allow(unused)]
    pub fn new() -> Self {
        Self(Default::default())
    }

    pub fn get_id(&self) -> Option<Id> {
        self.0.get(&ATTR_ID_LOCAL).and_then(|v| v.as_id())
    }
}

impl std::ops::Deref for MemoryTuple {
    type Target = FnvHashMap<LocalAttributeId, MemoryValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for MemoryTuple {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// impl crate::backend::Dao for MemoryTuple {
//     fn get(&self, attr: &schema::AttributeSchema) -> Result<Option<Value>, AnyError> {
//         Ok(self.0.get(&attr.id).map(|v| v.into()))
//     }

//     fn set(&mut self, _attr: &schema::AttributeSchema, _value: Value) {
//         todo!()
//     }
// }

#[derive(Debug)]
pub(super) enum MemoryExpr {
    Literal(MemoryValue),
    List(Vec<Self>),
    /// Select the value of an attribute.
    Attr(LocalAttributeId),
    /// Resolve the value of an [`Ident`] into an [`Id`].
    Ident(Id),
    UnaryOp {
        op: expr::UnaryOp,
        expr: Box<Self>,
    },
    BinaryOp {
        left: Box<Self>,
        op: expr::BinaryOp,
        right: Box<Self>,
    },
    If {
        value: Box<Self>,
        then: Box<Self>,
        or: Box<Self>,
    },
}
