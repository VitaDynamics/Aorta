use serde::Serialize;

use crate::error::{AortaError, AortaResult};
use crate::ZBytes;

pub fn to_json_string<T: Serialize>(value: &T) -> AortaResult<String> {
    serde_json::to_string(value)
        .map_err(|err| AortaError::internal(format!("failed to serialize telemetry JSON: {}", err)))
}

pub fn to_json_bytes<T: Serialize>(value: &T) -> AortaResult<Vec<u8>> {
    serde_json::to_vec(value)
        .map_err(|err| AortaError::internal(format!("failed to serialize telemetry JSON: {}", err)))
}

pub fn to_json_zbytes<T: Serialize>(value: &T) -> AortaResult<ZBytes> {
    to_json_bytes(value).map(ZBytes::from)
}
