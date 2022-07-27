use std::{cmp::Ordering, collections::HashSet};

use fnv::FnvHashMap;
use ordered_float::OrderedFloat;

use factordb::{
    data::{Id, Value},
    query::expr,
};

use crate::registry::{LocalAttributeId, ATTR_ID_LOCAL};

// SharedStr

#[derive(Clone, Hash, Debug, PartialOrd, Ord)]
pub(super) struct SharedStr(std::sync::Arc<str>);

impl SharedStr {
    pub fn from_string(value: String) -> Self {
        Self(std::sync::Arc::from(value))
    }

    // #[inline]
    // pub fn strong_count(&self) -> usize {
    //     std::sync::Arc::strong_count(&self.0)
    // }
}

impl std::fmt::Display for SharedStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
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
#[derive(Hash, Debug, Clone)]
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

impl PartialEq for MemoryValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (MemoryValue::Unit, MemoryValue::Unit) => true,
            (Self::Bool(l0), Self::Bool(r0)) => l0 == r0,
            (Self::UInt(l0), Self::UInt(r0)) => l0 == r0,
            (Self::Int(l0), Self::Int(r0)) => l0 == r0,
            (Self::Float(l0), Self::Float(r0)) => l0 == r0,
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            (Self::Bytes(l0), Self::Bytes(r0)) => l0 == r0,
            (Self::List(l0), Self::List(r0)) => l0 == r0,
            (Self::Map(l0), Self::Map(r0)) => l0 == r0,
            (Self::Id(l0), Self::Id(r0)) => l0 == r0,
            (Self::Int(i), Self::UInt(u)) | (Self::UInt(u), Self::Int(i)) => {
                if let Ok(u2) = i64::try_from(*u) {
                    *i == u2
                } else {
                    false
                }
            }
            (Self::Float(f), Self::UInt(u)) | (Self::UInt(u), Self::Float(f)) => (*u as f64) == **f,
            (Self::Float(f), Self::Int(u)) | (Self::Int(u), Self::Float(f)) => (*u as f64) == **f,
            (_, _) => false,
        }
    }
}

impl Eq for MemoryValue {}

impl PartialOrd for MemoryValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MemoryValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Id
            (MemoryValue::Id(a), MemoryValue::Id(b)) => a.cmp(b),
            (MemoryValue::Id(i), MemoryValue::String(s))
            | (MemoryValue::String(s), MemoryValue::Id(i)) => {
                i.to_string().as_str().cmp(s.as_ref())
            }
            (MemoryValue::Id(_), _) => Ordering::Less,
            (_, MemoryValue::Id(_)) => Ordering::Greater,

            // Unit.
            (MemoryValue::Unit, MemoryValue::Unit) => Ordering::Equal,
            (MemoryValue::Unit, _) => Ordering::Less,
            (_, MemoryValue::Unit) => Ordering::Greater,

            // Bool.
            (MemoryValue::Bool(a), MemoryValue::Bool(b)) => a.cmp(b),
            (MemoryValue::Bool(_), _) => Ordering::Less,
            (_, MemoryValue::Bool(_)) => Ordering::Greater,

            // Int + UInt + Float
            (MemoryValue::UInt(a), MemoryValue::UInt(b)) => a.cmp(b),
            (MemoryValue::Int(a), MemoryValue::Int(b)) => a.cmp(b),
            (MemoryValue::Float(a), MemoryValue::Float(b)) => a.cmp(b),
            (MemoryValue::UInt(a), MemoryValue::Int(b)) => {
                if let Ok(b2) = u64::try_from(*b) {
                    a.cmp(&b2)
                } else {
                    Ordering::Less
                }
            }
            (MemoryValue::Int(b), MemoryValue::UInt(a)) => {
                if let Ok(b2) = u64::try_from(*b) {
                    b2.cmp(a)
                } else {
                    Ordering::Greater
                }
            }
            (MemoryValue::UInt(i), MemoryValue::Float(f)) => {
                let i2 = OrderedFloat::from((*i) as f64);
                i2.cmp(f)
            }
            (MemoryValue::Float(f), MemoryValue::UInt(i)) => {
                let i2 = OrderedFloat::from((*i) as f64);
                f.cmp(&i2)
            }
            (MemoryValue::Int(i), MemoryValue::Float(f)) => {
                let i2 = OrderedFloat::from((*i) as f64);
                i2.cmp(f)
            }
            (MemoryValue::Float(f), MemoryValue::Int(i)) => {
                let i2 = OrderedFloat::from((*i) as f64);
                f.cmp(&i2)
            }
            (MemoryValue::UInt(_) | MemoryValue::Int(_) | MemoryValue::Float(_), _) => {
                Ordering::Less
            }
            (_, MemoryValue::UInt(_) | MemoryValue::Int(_) | MemoryValue::Float(_)) => {
                Ordering::Greater
            }

            // String
            (MemoryValue::String(a), MemoryValue::String(b)) => a.cmp(b),
            (MemoryValue::String(_), _) => Ordering::Less,
            (_, MemoryValue::String(_)) => Ordering::Greater,

            // Bytes.
            (MemoryValue::Bytes(a), MemoryValue::Bytes(b)) => a.cmp(b),
            (MemoryValue::Bytes(_), _) => Ordering::Less,
            (_, MemoryValue::Bytes(_)) => Ordering::Greater,

            // List
            (MemoryValue::List(a), MemoryValue::List(b)) => a.cmp(b),
            (MemoryValue::List(_), _) => Ordering::Less,
            (_, MemoryValue::List(_)) => Ordering::Greater,

            // Map
            (MemoryValue::Map(a), MemoryValue::Map(b)) => a.cmp(b),
        }
    }
}

impl MemoryValue {
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
            V::List(v) => Value::List(v.iter().map(Into::into).collect()),
            V::Map(v) => Value::Map(
                v.iter()
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
    Regex(regex::Regex),
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
    InLiteral {
        value: Box<Self>,
        items: HashSet<MemoryValue>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memoryvalue_ord() {
        // Int.
        assert_eq!(MemoryValue::Int(5), MemoryValue::Int(5));
        assert!(MemoryValue::Int(0) < MemoryValue::Int(6));
        assert!(MemoryValue::Int(0) < MemoryValue::Int(6));
        assert!(MemoryValue::Int(-5) > MemoryValue::Int(-10));
        assert_eq!(MemoryValue::Int(5), MemoryValue::UInt(5));
        assert!(MemoryValue::Int(0) < MemoryValue::UInt(10));

        // UInt.
        assert_eq!(MemoryValue::UInt(5), MemoryValue::UInt(5));
        assert!(MemoryValue::UInt(0) < MemoryValue::UInt(6));
        assert!(MemoryValue::UInt(0) < MemoryValue::UInt(6));
        assert!(MemoryValue::UInt(20) > MemoryValue::UInt(11));
        assert_eq!(MemoryValue::UInt(5), MemoryValue::Int(5));
        assert!(MemoryValue::UInt(0) < MemoryValue::Int(10));

        // Float.
        assert_eq!(
            MemoryValue::Float(5.5.into()),
            MemoryValue::Float(5.5.into())
        );
        assert!(MemoryValue::Float(0.0.into()) < MemoryValue::Float(10.0.into()));
        assert!(MemoryValue::Float((-5.5).into()) < MemoryValue::Float(0.0.into()));
        assert!(MemoryValue::Float(1.0.into()) > MemoryValue::Float(0.5.into()));

        assert_eq!(MemoryValue::Float(5.0.into()), MemoryValue::Int(5));
        assert!(MemoryValue::Float(0.0.into()) < MemoryValue::Int(10));
        assert_eq!(MemoryValue::Float(5.0.into()), MemoryValue::UInt(5));

        // TODO: more tests!
    }
}
