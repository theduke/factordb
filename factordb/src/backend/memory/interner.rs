use crate::data::Value;

use super::memory_data::{MemoryValue, SharedStr};

pub(super) struct Interner {
    strings: std::collections::HashMap<SharedStr, SharedStr>,
}

impl Interner {
    pub fn new() -> Self {
        Self {
            strings: std::collections::HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.strings.clear();
    }

    pub fn intern_str(&mut self, value: String) -> SharedStr {
        let shared: SharedStr = SharedStr::from_string(value);
        match self.strings.get(&shared) {
            Some(v) => v.clone(),
            None => {
                self.strings.insert(shared.clone(), shared.clone());
                shared
            }
        }
    }

    pub fn intern_value(&mut self, value: Value) -> MemoryValue {
        use MemoryValue as M;
        match value {
            Value::Unit => M::Unit,
            Value::Bool(v) => M::Bool(v),
            Value::UInt(v) => M::UInt(v),
            Value::Int(v) => M::Int(v),
            Value::Float(v) => M::Float(v),
            Value::String(v) => M::String(self.intern_str(v)),
            Value::Bytes(v) => M::Bytes(v),
            Value::List(v) => M::List(v.into_iter().map(|v| self.intern_value(v)).collect()),
            Value::Map(v) => M::Map(
                v.0.into_iter()
                    .map(|(key, value)| (self.intern_value(key), self.intern_value(value)))
                    .collect(),
            ),
            Value::Id(v) => M::Id(v),
        }
    }
}
