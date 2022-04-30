use factordb::data::Value;

use super::memory_data::{MemoryValue, SharedStr};

pub(super) struct Interner {
    strings: std::collections::HashMap<Box<str>, SharedStr>,
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
        match self.strings.get(value.as_str()) {
            Some(v) => v.clone(),
            None => {
                let shared = SharedStr::from_string(value.clone());
                self.strings.insert(Box::from(value), shared.clone());
                shared
            }
        }
    }

    // /// Try to remove a value from the interner.
    // /// Values will only be removed if no other copies exist.
    // pub fn remove(&mut self, value: SharedStr) {
    //     if value.strong_count() == 2 {
    //         // If the strong count is 2, it means that no other copies exist and
    //         // the value can be removed from the cache.
    //         // The count is 2 because:
    //         //  - the `value` argument is 1
    //         //  - the value stored in the map is 1
    //         self.strings.remove(value.as_ref());
    //     }
    // }

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
