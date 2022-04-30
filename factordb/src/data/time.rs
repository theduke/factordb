use chrono::TimeZone;

#[derive(
    serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts", ts(export))]
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

    pub fn to_datetime(&self) -> chrono::DateTime<chrono::Utc> {
        let seconds = (self.0 / 1000) as i64;
        let nanos = ((self.0 % 1000) * 1_000_000) as u32;
        chrono::Utc.timestamp(seconds, nanos)
    }

    pub fn from_chrono_utc_datetime(t: chrono::NaiveDateTime) -> Self {
        Self(t.timestamp() as u64)
    }
}

impl From<chrono::DateTime<chrono::Utc>> for Timestamp {
    fn from(v: chrono::DateTime<chrono::Utc>) -> Self {
        Self(v.timestamp() as u64)
    }
}

impl From<chrono::DateTime<chrono::FixedOffset>> for Timestamp {
    fn from(v: chrono::DateTime<chrono::FixedOffset>) -> Self {
        Self(v.timestamp() as u64)
    }
}

impl From<Timestamp> for chrono::DateTime<chrono::Utc> {
    fn from(v: Timestamp) -> Self {
        v.to_datetime()
    }
}
