use ordered_float::OrderedFloat;

use crate::{
    data::{Id, Value},
    schema, AnyError,
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
}

impl PartialEq for SharedStr {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ptr() == other.0.as_ptr()
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

    pub fn as_id(&self) -> Option<Id> {
        if let Self::Id(id) = self {
            Some(*id)
        } else {
            None
        }
    }

    pub fn as_id_ref(&self) -> Option<&Id> {
        if let Self::Id(id) = self {
            Some(id)
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

#[derive(Debug)]
pub(super) struct MemoryTuple(pub fnv::FnvHashMap<Id, MemoryValue>);

impl std::ops::Deref for MemoryTuple {
    type Target = fnv::FnvHashMap<Id, MemoryValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for MemoryTuple {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl crate::backend::Dao for MemoryTuple {
    fn get(&self, attr: &schema::AttributeSchema) -> Result<Option<Value>, AnyError> {
        Ok(self.0.get(&attr.id).map(|v| v.into()))
    }

    fn set(&mut self, _attr: &schema::AttributeSchema, _value: Value) {
        todo!()
    }
}
