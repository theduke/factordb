use crate::AnyError;

pub struct JsonConverter;

impl super::LogConverter for JsonConverter {
    fn serialize(&self, event: &super::LogEvent) -> Result<Vec<u8>, AnyError> {
        serde_json::to_vec(event).map_err(Into::into)
    }

    fn deserialize(&self, data: Vec<u8>) -> Result<super::LogEvent, AnyError> {
        serde_json::from_slice(&data).map_err(Into::into)
    }
}
