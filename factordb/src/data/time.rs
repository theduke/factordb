use chrono::TimeZone;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
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
        chrono::Utc.timestamp(self.0 as i64, 0)
    }
}

impl From<chrono::DateTime<chrono::Utc>> for Timestamp {
    fn from(v: chrono::DateTime<chrono::Utc>) -> Self {
        Self(v.timestamp() as u64)
    }
}

impl From<Timestamp> for chrono::DateTime<chrono::Utc> {
    fn from(v: Timestamp) -> Self {
        v.to_datetime()
    }
}
