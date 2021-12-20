mod serde_deserialize;
pub use serde_serialize::{to_value, to_value_map, ValueSerializeError};

mod serde_serialize;
pub use serde_deserialize::{from_value, from_value_map, ValueDeserializeError};

use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
};

use anyhow::anyhow;
use ordered_float::OrderedFloat;

use crate::AnyError;

use super::{Id, Ident, ValueMap, ValueType};

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub enum Value {
    Unit,

    Bool(bool),
    UInt(u64),
    Int(i64),
    Float(OrderedFloat<f64>),
    String(String),
    Bytes(Vec<u8>),

    List(Vec<Self>),
    Map(ValueMap<Value>),

    Id(Id),
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Unit => ValueType::Unit,
            Value::Bool(_) => ValueType::Bool,
            Value::UInt(_) => ValueType::UInt,
            Value::Int(_) => ValueType::Int,
            Value::Float(_) => ValueType::Float,
            Value::String(_) => ValueType::String,
            Value::Bytes(_) => ValueType::Bytes,
            Value::List(items) => {
                let mut types = Vec::new();
                for item in items {
                    let ty = item.value_type();
                    if !types.contains(&ty) {
                        types.push(ty);
                    }
                }

                let inner_ty = if types.len() == 1 {
                    types.pop().unwrap()
                } else {
                    ValueType::Union(types)
                };
                ValueType::List(Box::new(inner_ty))
            }
            Value::Map(_) => {
                todo!()
            }
            Value::Id(_) => ValueType::Ref,
        }
    }

    pub fn new_list<V: Into<Self>, I: IntoIterator<Item = V>>(items: I) -> Self {
        Self::List(items.into_iter().map(|v| v.into()).collect())
    }

    /// Coerce this value into the type specified by ValueType.
    /// Returns an error if safe coercion is not possible.
    pub fn coerce_mut(&mut self, ty: &ValueType) -> Result<(), AnyError> {
        match (&self, ty) {
            (Value::Unit, ValueType::Unit) => {
                *self = Value::Unit;
                Ok(())
            }
            (Value::Bool(_), ValueType::Bool) => Ok(()),
            (Value::UInt(_), ValueType::UInt) => Ok(()),
            (Value::UInt(ref x), ValueType::Int) => {
                if *x < i64::MAX as u64 {
                    *self = Value::Int(*x as i64);
                    Ok(())
                } else {
                    Err(anyhow!("Invalid int: exceeds i64 range"))
                }
            }
            (Value::Int(_), ValueType::Int) => Ok(()),
            (Value::Int(ref x), ValueType::UInt) => {
                if *x >= 0 {
                    *self = Value::UInt(*x as u64);
                    Ok(())
                } else {
                    Err(anyhow!("Invalid uint: negative number"))
                }
            }
            (Value::Float(_), ValueType::Float) => Ok(()),
            (Value::Int(v), ValueType::Float) => {
                *self = Value::Float(OrderedFloat::from((*v) as f64));
                Ok(())
            }
            (Value::UInt(v), ValueType::Float) => {
                *self = Value::Float(OrderedFloat::from((*v) as f64));
                Ok(())
            }
            (Value::String(_), ValueType::String) => Ok(()),
            (Value::Bytes(_), ValueType::Bytes) => Ok(()),

            (Value::List(_), ValueType::List(item_ty)) => {
                // This is stupid, but required by the borrow checker.
                let mut items = match self {
                    Value::List(inner) => std::mem::take(inner),
                    _ => unreachable!(),
                };

                for item in &mut items {
                    item.coerce_mut(&*&item_ty)?;
                }
                *self = Value::List(items);
                Ok(())
            }
            (Value::Id(_), ValueType::Ref) => Ok(()),
            (other, _) => Err(anyhow!(
                "Invalid value: expected {:?} but got {:?}",
                other.value_type(),
                ty
            )),
        }
    }

    pub fn as_id(&self) -> Option<Id> {
        match self {
            Value::String(v) => v.parse::<uuid::Uuid>().ok().map(Id::from_uuid),
            Value::Id(v) => Some(*v),
            _ => None,
        }
    }

    pub fn to_ident(&self) -> Option<Ident> {
        self.as_id()
            .map(Ident::from)
            .or_else(|| self.as_str().map(|s| Ident::Name(s.to_string().into())))
    }

    /// Returns `true` if the value is [`Bool`].
    pub fn is_bool(&self) -> bool {
        matches!(self, Self::Bool(..))
    }

    pub fn as_bool(&self) -> Option<bool> {
        if let Self::Bool(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Returns `true` if the value is [`UInt`].
    pub fn is_uint(&self) -> bool {
        matches!(self, Self::UInt(..))
    }

    pub fn as_uint(&self) -> Option<u64> {
        if let Self::UInt(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Returns `true` if the value is [`Int`].
    pub fn is_int(&self) -> bool {
        matches!(self, Self::Int(..))
    }

    pub fn as_int(&self) -> Option<i64> {
        if let Self::Int(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        if let Self::Float(v) = self {
            Some(**v)
        } else {
            None
        }
    }

    /// Returns `true` if the value is [`Float`].
    pub fn is_float(&self) -> bool {
        matches!(self, Self::Float(..))
    }

    /// Returns `true` if the value is [`String`].
    pub fn is_string(&self) -> bool {
        matches!(self, Self::String(..))
    }

    pub fn as_str(&self) -> Option<&str> {
        if let Self::String(v) = self {
            Some(&v)
        } else {
            None
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        if let Self::List(items) = self {
            Some(&*items)
        } else {
            None
        }
    }

    /// Returns `true` if the value is [`Bytes`].
    pub fn is_bytes(&self) -> bool {
        matches!(self, Self::Bytes(..))
    }

    /// Returns `true` if the value is [`Id`].
    pub fn is_id(&self) -> bool {
        matches!(self, Self::Id(..))
    }

    pub fn as_map_mut(&mut self) -> Option<&mut ValueMap<Value>> {
        if let Self::Map(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<u8> for Value {
    fn from(v: u8) -> Self {
        Self::UInt(v.into())
    }
}

impl From<u16> for Value {
    fn from(v: u16) -> Self {
        Self::UInt(v.into())
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Self::UInt(v.into())
    }
}
impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Self::UInt(v.into())
    }
}

impl From<i8> for Value {
    fn from(v: i8) -> Self {
        Self::Int(v.into())
    }
}

impl From<i16> for Value {
    fn from(v: i16) -> Self {
        Self::Int(v.into())
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Self::Int(v.into())
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::Int(v.into())
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Self::Float((v as f64).into())
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Self::Float(v.into())
    }
}

impl<'a> From<&'a str> for Value {
    fn from(v: &'a str) -> Self {
        Self::String(v.to_string())
    }
}

impl<'a> From<&'a String> for Value {
    fn from(v: &'a String) -> Self {
        Self::String(v.to_string())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl<'a> From<&'a [u8]> for Value {
    fn from(v: &'a [u8]) -> Self {
        Self::Bytes(v.into())
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Self {
        let items = v.into_iter().map(Into::into).collect();
        Self::List(items)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(v: Option<T>) -> Self {
        match v {
            Some(v) => v.into(),
            None => Self::Unit,
        }
    }
}

impl<K, V> From<HashMap<K, V>> for Value
where
    K: Into<Value>,
    V: Into<Value>,
{
    fn from(v: HashMap<K, V>) -> Self {
        Self::Map(
            v.into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        )
    }
}

impl<K, V> From<BTreeMap<K, V>> for Value
where
    K: Into<Value>,
    V: Into<Value>,
{
    fn from(v: BTreeMap<K, V>) -> Self {
        Self::Map(
            v.into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        )
    }
}

impl From<Id> for Value {
    fn from(v: Id) -> Self {
        Self::Id(v)
    }
}

impl From<Ident> for Value {
    fn from(ident: Ident) -> Self {
        match ident {
            Ident::Id(id) => id.into(),
            Ident::Name(name) => name.to_string().into(),
        }
    }
}

impl TryFrom<Value> for Id {
    type Error = AnyError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if let Value::Id(v) = value {
            Ok(v)
        } else {
            Err(anyhow::anyhow!("Invalid type: expected a Value::Id"))
        }
    }
}

impl TryFrom<Value> for String {
    type Error = AnyError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if let Value::String(v) = value {
            Ok(v)
        } else {
            Err(anyhow::anyhow!("Invalid type: expected a Value::String"))
        }
    }
}

impl TryFrom<Value> for url::Url {
    type Error = AnyError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if let Value::String(v) = value {
            v.parse().map_err(Into::into)
        } else {
            Err(anyhow::anyhow!("Invalid type: expected a Value::String"))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{from_value, from_value_map, to_value, to_value_map, Id, ValueMap};

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
    struct TestData {
        b: bool,
        int64: i64,
        uint64: u64,
        float64: f64,
        string: String,
        bytes: Vec<u8>,
        id: Id,
        list: Vec<Self>,
    }

    #[test]
    fn test_value_de_serialize_roundtrip() {
        let data = TestData {
            b: true,
            int64: 42,
            uint64: 42,
            float64: 42.42,
            string: "010".into(),
            bytes: b"010".to_vec(),
            id: Id::from_u128(42),
            list: vec![TestData {
                b: true,
                int64: 420,
                uint64: 420,
                float64: 420.420,
                string: "420".into(),
                bytes: b"01001".to_vec(),
                list: Vec::new(),
                id: Id::from_u128(420),
            }],
        };

        let value = to_value(data.clone()).unwrap();
        let data2 = from_value(value).unwrap();

        assert_eq!(data, data2);

        // Now round-trip through a map.
        let map: ValueMap<String> = to_value_map(data.clone()).unwrap();
        let data3: TestData = from_value_map(map).unwrap();
        assert_eq!(data, data3);
    }
}
