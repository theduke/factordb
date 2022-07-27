//! This module contains types for representing data stored in a FactorDB.

mod serde_deserialize;
use anyhow::bail;
pub use serde_serialize::{to_value, to_value_map, ValueSerializeError};

mod serde_serialize;
pub use serde_deserialize::{from_value, from_value_map, ValueDeserializeError};

use std::{
    collections::{BTreeMap, HashMap},
    convert::{TryFrom, TryInto},
};

use ordered_float::OrderedFloat;

use crate::{data::patch::PatchPathElem, AnyError};

use super::{patch::PatchPath, Id, IdOrIdent, ValueMap, ValueType};

/// Generic value type that can represent all data stored in a database.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
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

#[cfg(feature = "typescript-schema")]
impl ts_rs::TS for Value {
    const EXPORT_TO: Option<&'static str> = Some("bindings/value.ts");

    fn name() -> String {
        "Value".to_string()
    }

    fn decl() -> String {
        "type Value = null | boolean | number | string | Value[] | { [property: string]: Value };"
            .to_string()
    }

    fn name_with_type_args(args: Vec<String>) -> String {
        assert!(args.is_empty(), "called name_with_type_args on primitive");
        "Value".to_string()
    }

    fn inline() -> String {
        "Value".to_string()
    }

    fn dependencies() -> Vec<ts_rs::Dependency> {
        vec![]
    }

    fn transparent() -> bool {
        false
    }
}

/// Error for failed value coercions.
#[derive(Debug)]
pub struct ValueCoercionError {
    pub(crate) expected_type: ValueType,
    pub(crate) actual_type: ValueType,
    /// Specifies the nested path to the element that failed the coersion.
    /// This is relevant for nested data structures like lists and maps.
    pub(crate) path: Option<PatchPath>,
}

impl std::fmt::Display for ValueCoercionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Value coercion failed: expected {:?}, got {:?}",
            self.expected_type, self.actual_type
        )?;

        if let Some(p) = &self.path {
            write!(f, " at {}", p)?;
        }

        Ok(())
    }
}

impl std::error::Error for ValueCoercionError {}

impl Value {
    /// Compute the value type of this value.
    pub fn value_type(&self) -> ValueType {
        ValueType::for_value(self)
    }

    /// Build a new [`Value::List`] from an iterator.
    pub fn new_list<V: Into<Self>, I: IntoIterator<Item = V>>(items: I) -> Self {
        Self::List(items.into_iter().map(|v| v.into()).collect())
    }

    /// Try to coerce the value into the specified [`ValueType`].
    /// Returns an error if lossless coercion is not possible.
    // Takes a `&mut Value` to avoid redundant cloning if the types does not
    // need to be changed.
    pub fn coerce_mut(&mut self, ty: &ValueType) -> Result<(), ValueCoercionError> {
        match ty {
            ValueType::Unit | ValueType::Bool => {
                let actual_type = self.value_type();
                if &actual_type == ty {
                    Ok(())
                } else {
                    Err(ValueCoercionError {
                        expected_type: ty.clone(),
                        actual_type,
                        path: None,
                    })
                }
            }
            ValueType::Bytes => match self {
                Self::Bytes(_) => Ok(()),
                Self::List(items) => {
                    let items = std::mem::take(items);

                    let bytes = items
                        .into_iter()
                        .enumerate()
                        .map(|(index, v)| -> Result<u8, ValueCoercionError> {
                            match v {
                                Self::Int(x) => x.try_into().map_err(|_| ValueCoercionError {
                                    expected_type: ValueType::Bytes,
                                    actual_type: ValueType::Int,
                                    path: Some(PatchPath(vec![PatchPathElem::ListIndex(index)])),
                                }),
                                Self::UInt(x) => x.try_into().map_err(|_| ValueCoercionError {
                                    expected_type: ValueType::Bytes,
                                    actual_type: ValueType::UInt,
                                    path: Some(PatchPath(vec![PatchPathElem::ListIndex(index)])),
                                }),
                                other => Err(ValueCoercionError {
                                    expected_type: ValueType::Bytes,
                                    actual_type: other.value_type(),
                                    path: Some(PatchPath(vec![PatchPathElem::ListIndex(index)])),
                                }),
                            }
                        })
                        .collect::<Result<Vec<u8>, ValueCoercionError>>()?;

                    *self = Self::Bytes(bytes);
                    Ok(())
                }
                other => Err(ValueCoercionError {
                    expected_type: ValueType::Bytes,
                    actual_type: other.value_type(),
                    path: None,
                }),
            },
            ValueType::Map(_t) => {
                todo!()
            }
            ValueType::Int => match self {
                Value::Int(_) => Ok(()),
                Value::UInt(x) => {
                    if let Ok(intval) = (*x).try_into() {
                        *self = Value::Int(intval);
                        Ok(())
                    } else {
                        Err(ValueCoercionError {
                            expected_type: ValueType::Int,
                            actual_type: ValueType::Unit,
                            path: None,
                        })
                    }
                }
                Value::Float(floatval) => {
                    // Note: a .try_from() would be nicer, but std doesn't
                    // have an impl, only num-traits.
                    if floatval.fract() == 0.0 && **floatval <= (i64::MAX as f64) {
                        *self = Value::Int((**floatval) as i64);
                        Ok(())
                    } else {
                        Err(ValueCoercionError {
                            expected_type: ValueType::Int,
                            actual_type: ValueType::Float,
                            path: None,
                        })
                    }
                }
                other => Err(ValueCoercionError {
                    expected_type: ValueType::Int,
                    actual_type: other.value_type(),
                    path: None,
                }),
            },
            ValueType::UInt => match self {
                Value::UInt(_) => Ok(()),
                Value::Int(x) => {
                    if let Ok(intval) = (*x).try_into() {
                        *self = Value::UInt(intval);
                        Ok(())
                    } else {
                        Err(ValueCoercionError {
                            expected_type: ValueType::Int,
                            actual_type: ValueType::Unit,
                            path: None,
                        })
                    }
                }
                Value::Float(floatval) => {
                    // Note: a .try_from() would be nicer, but std doesn't
                    // have an impl, only num-traits.
                    if floatval.fract() == 0.0 && **floatval <= (u64::MAX as f64) {
                        *self = Value::Int((**floatval) as i64);
                        Ok(())
                    } else {
                        Err(ValueCoercionError {
                            expected_type: ValueType::Int,
                            actual_type: ValueType::Float,
                            path: None,
                        })
                    }
                }
                other => Err(ValueCoercionError {
                    expected_type: ValueType::Int,
                    actual_type: other.value_type(),
                    path: None,
                }),
            },
            ValueType::Float => match self {
                Value::UInt(x) => {
                    *self = Value::Float((*x as f64).into());
                    Ok(())
                }
                Value::Int(x) => {
                    *self = Value::Float((*x as f64).into());
                    Ok(())
                }
                Value::Float(_) => Ok(()),
                other => Err(ValueCoercionError {
                    expected_type: ValueType::Float,
                    actual_type: other.value_type(),
                    path: None,
                }),
            },
            ValueType::String => match self {
                Value::Int(v) => {
                    *self = Value::String(v.to_string());
                    Ok(())
                }
                Value::UInt(v) => {
                    *self = Value::String(v.to_string());
                    Ok(())
                }
                Value::Float(v) => {
                    *self = Value::String(v.to_string());
                    Ok(())
                }
                Value::String(_) => Ok(()),
                other => Err(ValueCoercionError {
                    expected_type: ValueType::String,
                    actual_type: other.value_type(),
                    path: None,
                }),
            },
            ValueType::List(item_type) => match &mut *self {
                Self::Unit => {
                    *self = Self::List(vec![]);
                    Ok(())
                }
                Self::List(items) => {
                    for item in items {
                        item.coerce_mut(item_type)?;
                    }
                    Ok(())
                }
                other => {
                    other.coerce_mut(item_type)?;
                    let inner = other.clone();
                    *self = Self::List(vec![inner]);
                    Ok(())
                }
            },
            ValueType::Any => {
                // Everything is allowed.
                Ok(())
            }
            ValueType::Union(variants) => {
                for variant_ty in variants {
                    if self.coerce_mut(variant_ty).is_ok() {
                        return Ok(());
                    }
                }

                Err(ValueCoercionError {
                    expected_type: ty.clone(),
                    actual_type: self.value_type(),
                    path: None,
                })
            }
            ValueType::Object(_obj) => {
                // FIXME: coerce objects properly - code below is useless.
                let actual_ty = self.value_type();
                if &actual_ty == ty {
                    Ok(())
                } else {
                    Err(ValueCoercionError {
                        expected_type: ty.clone(),
                        actual_type: self.value_type(),
                        path: None,
                    })
                }
            }
            ValueType::DateTime => {
                // FIXME: coerce from uint/int and convert to special Self::DateTime variant once
                // added.
                match self {
                    Value::UInt(_) => Ok(()),
                    Value::Int(x) => {
                        let x2: u64 = (*x).try_into().map_err(|_| ValueCoercionError {
                            expected_type: ValueType::DateTime,
                            actual_type: self.value_type(),
                            path: None,
                        })?;

                        *self = Value::UInt(x2);
                        Ok(())
                    }
                    Value::String(s) => {
                        if let Ok(x) = s.parse::<u64>() {
                            *self = Value::UInt(x);
                            Ok(())
                        } else if let Ok(t) = chrono::DateTime::parse_from_rfc3339(s) {
                            *self = Value::UInt(t.timestamp().try_into().unwrap());
                            Ok(())
                        } else {
                            Err(ValueCoercionError {
                                expected_type: ValueType::DateTime,
                                actual_type: self.value_type(),
                                path: None,
                            })
                        }
                    }
                    other => Err(ValueCoercionError {
                        expected_type: ValueType::DateTime,
                        actual_type: other.value_type(),
                        path: None,
                    }),
                }
            }
            ValueType::Url => {
                match self {
                    Value::String(v) => {
                        if let Err(_err) = url::Url::parse(v) {
                            // TODO: propagate url parser error message?
                            Err(ValueCoercionError {
                                expected_type: ValueType::Url,
                                actual_type: ValueType::String,
                                path: None,
                            })
                        } else {
                            Ok(())
                        }
                    }
                    other => Err(ValueCoercionError {
                        expected_type: ValueType::Url,
                        actual_type: other.value_type(),
                        path: None,
                    }),
                }
            }
            ValueType::Ref | ValueType::RefConstrained(_) => {
                match self {
                    Value::String(strval) => {
                        // TODO: somehow idents?
                        if let Ok(id) = uuid::Uuid::parse_str(strval) {
                            *self = Self::Id(id.into());
                            Ok(())
                        } else {
                            Err(ValueCoercionError {
                                expected_type: ValueType::Ref,
                                actual_type: ValueType::String,
                                path: None,
                            })
                        }
                    }
                    Value::Id(_) => Ok(()),
                    other => Err(ValueCoercionError {
                        expected_type: ValueType::Ref,
                        actual_type: other.value_type(),
                        path: None,
                    }),
                }
            }
            ValueType::Const(const_val) => {
                if self == const_val {
                    Ok(())
                } else {
                    Err(ValueCoercionError {
                        expected_type: ty.clone(),
                        actual_type: self.value_type(),
                        path: None,
                    })
                }
            }
            ValueType::Ident(_) => match self {
                Value::String(_) => Ok(()),
                Value::Id(id) => {
                    *self = Value::String(id.to_string());
                    Ok(())
                }
                other => Err(ValueCoercionError {
                    expected_type: ty.clone(),
                    actual_type: other.value_type(),
                    path: None,
                }),
            },
        }
    }

    pub fn as_id(&self) -> Option<Id> {
        match self {
            Value::String(v) => v.parse::<uuid::Uuid>().ok().map(Id::from_uuid),
            Value::Id(v) => Some(*v),
            _ => None,
        }
    }

    pub fn to_ident(&self) -> Option<IdOrIdent> {
        self.as_id()
            .map(IdOrIdent::from)
            .or_else(|| self.as_str().map(|s| IdOrIdent::Name(s.to_string().into())))
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
            Some(v)
        } else {
            None
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        if let Self::List(items) = self {
            Some(items)
        } else {
            None
        }
    }

    pub fn try_into_list<T>(self) -> Result<Vec<T>, anyhow::Error>
    where
        T: TryFrom<Value>,
        T::Error: std::error::Error + Send + Sync + 'static,
    {
        if let Self::List(items) = self {
            items
                .into_iter()
                .map(|x| T::try_from(x).map_err(anyhow::Error::from))
                .collect()
        } else {
            bail!("expected list, got {:?}", self.value_type());
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
        Self::UInt(v)
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
        Self::Int(v)
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

impl From<IdOrIdent> for Value {
    fn from(ident: IdOrIdent) -> Self {
        match ident {
            IdOrIdent::Id(id) => id.into(),
            IdOrIdent::Name(name) => name.to_string().into(),
        }
    }
}

impl TryFrom<Value> for u64 {
    type Error = ValueCoercionError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UInt(x) => Ok(x),
            Value::Int(x) if x >= 0 => Ok(x as u64),
            _ => Err(ValueCoercionError {
                expected_type: ValueType::UInt,
                actual_type: value.value_type(),
                path: None,
            }),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = ValueCoercionError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int(x) => Ok(x),
            Value::UInt(x) if x <= i64::MAX as u64 => Ok(x as i64),
            _ => Err(ValueCoercionError {
                expected_type: ValueType::UInt,
                actual_type: value.value_type(),
                path: None,
            }),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = ValueCoercionError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bool(x) => Ok(x),
            _ => Err(ValueCoercionError {
                expected_type: ValueType::Bool,
                actual_type: value.value_type(),
                path: None,
            }),
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
