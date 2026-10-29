use bytes::Bytes;

use crate::error::{AortaError, AortaResult};

/// Transport-agnostic payload container.
#[derive(Clone, Debug)]
pub enum AortaPayload {
    Owned(OwnedPayload),
    Shm(ShmPayload),
}

impl AortaPayload {
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(payload) => payload.len(),
            Self::Shm(payload) => payload.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Heap-owned payload with a validated [offset, len] region.
#[derive(Clone, Debug)]
pub struct OwnedPayload {
    data: Bytes,
    offset: usize,
    len: usize,
}

impl OwnedPayload {
    /// Creates an owned payload with strict bounds validation.
    pub fn new(data: Bytes, offset: usize, len: usize) -> AortaResult<Self> {
        let end = offset.checked_add(len).ok_or_else(|| {
            AortaError::invalid_argument(format!(
                "OwnedPayload overflow: offset({offset}) + len({len}) overflows usize"
            ))
        })?;
        if end > data.len() {
            return Err(AortaError::invalid_argument(format!(
                "OwnedPayload out of bounds: offset({offset}) + len({len}) exceeds data length({})",
                data.len()
            )));
        }
        Ok(Self { data, offset, len })
    }

    /// Wraps the entire Bytes buffer.
    pub fn from_bytes(data: Bytes) -> Self {
        let len = data.len();
        Self {
            data,
            offset: 0,
            len,
        }
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn payload(&self) -> &[u8] {
        let end = self.offset + self.len;
        &self.data[self.offset..end]
    }

    /// Returns the validated payload slice as a Bytes view (no copy).
    pub fn into_bytes(self) -> Bytes {
        let end = self.offset + self.len;
        self.data.slice(self.offset..end)
    }
}

/// SHM payload metadata. The backing memory is identified by `handle`.
#[derive(Clone, Copy, Debug)]
pub struct ShmPayload {
    handle: u64,
    capacity: usize,
    offset: usize,
    len: usize,
}

impl ShmPayload {
    /// Creates a SHM payload descriptor with strict bounds validation.
    pub fn new(handle: u64, capacity: usize, offset: usize, len: usize) -> AortaResult<Self> {
        let end = offset.checked_add(len).ok_or_else(|| {
            AortaError::invalid_argument(format!(
                "ShmPayload overflow: offset({offset}) + len({len}) overflows usize"
            ))
        })?;
        if end > capacity {
            return Err(AortaError::invalid_argument(format!(
                "ShmPayload out of bounds: offset({offset}) + len({len}) exceeds capacity({capacity})"
            )));
        }
        Ok(Self {
            handle,
            capacity,
            offset,
            len,
        })
    }

    pub fn handle(&self) -> u64 {
        self.handle
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
#[path = "../test/payload_tests.rs"]
mod tests;
