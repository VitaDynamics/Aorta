use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntrospectErrorKind {
    InvalidInput,
    Timeout,
    NotFound,
    Ambiguous,
    PermissionDenied,
    CapabilityMissing,
    Transport,
    Decode,
    SchemaMismatch,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectError {
    pub kind: IntrospectErrorKind,
    pub message: String,
}

impl IntrospectError {
    pub fn new(kind: IntrospectErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(IntrospectErrorKind::InvalidInput, message)
    }

    pub fn decode(message: impl Into<String>) -> Self {
        Self::new(IntrospectErrorKind::Decode, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(IntrospectErrorKind::Internal, message)
    }
}

impl fmt::Display for IntrospectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for IntrospectError {}

pub type IntrospectResult<T> = Result<T, IntrospectError>;
