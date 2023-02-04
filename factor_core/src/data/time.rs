use std::convert::TryFrom;

use time::OffsetDateTime;

use super::{value::ValueCoercionError, Value, ValueType};

/// A timestamp stored as UNIX timestamp in milliseconds.
#[derive(
    serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "typescript-schema", derive(ts_rs::TS))]
#[cfg_attr(feature = "typescript-schema", ts(export))]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn now() -> Self {
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        Self(t as u64)
    }

    pub fn as_millis(self) -> u64 {
        self.0
    }

    pub fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    pub fn to_system_time(self) -> Option<std::time::SystemTime> {
        std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_millis(self.0))
    }

    pub fn to_datetime(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(self.0 as i128 * 1_000)
            .or_else(|_| OffsetDateTime::from_unix_timestamp(u32::MAX as i64))
            .unwrap_or(OffsetDateTime::from_unix_timestamp(0).unwrap())
    }
}

impl TryFrom<Value> for Timestamp {
    type Error = ValueCoercionError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UInt(x) => Ok(Self::from_millis(x)),
            Value::Int(x) if x >= 0 => Ok(Self::from_millis(x as u64)),
            _ => Err(ValueCoercionError {
                expected_type: ValueType::DateTime,
                actual_type: value.value_type(),
                path: None,
                message: Some("Invalid timestamp: expected a millisecond number".to_string()),
            }),
        }
    }
}

impl From<OffsetDateTime> for Timestamp {
    fn from(v: OffsetDateTime) -> Self {
        Self(v.unix_timestamp() as u64 * 1_000)
    }
}

impl From<Timestamp> for OffsetDateTime {
    fn from(v: Timestamp) -> Self {
        v.to_datetime()
    }
}
