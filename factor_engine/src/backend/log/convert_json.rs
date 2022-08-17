#[derive(Clone, Copy, Debug)]
pub struct JsonConverter;

impl super::LogConverter for JsonConverter {
    fn serialize(&self, event: &super::LogEvent) -> Result<Vec<u8>, anyhow::Error> {
        serde_json::to_vec(event).map_err(Into::into)
    }

    fn deserialize(&self, data: &[u8]) -> Result<super::LogEvent, anyhow::Error> {
        serde_json::from_slice(data).map_err(Into::into)
    }
}
