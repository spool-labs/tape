use anyhow::{Result, anyhow};

pub fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::serialize(value).map_err(|e| anyhow!("Serialization failed: {}", e))
}

pub fn deserialize<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T> {
    bincode::deserialize(data).map_err(|e| anyhow!("Deserialization failed: {}", e))
}
