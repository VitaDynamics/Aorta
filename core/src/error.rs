use std::fmt;

pub type AortaResult<T> = std::result::Result<T, AortaError>;

#[derive(Debug)]
pub enum TransportError {
    Backend(String),
}

impl TransportError {
    pub fn backend<T: Into<String>>(msg: T) -> Self {
        Self::Backend(msg.into())
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(msg) => write!(f, "backend transport error: {}", msg),
        }
    }
}

impl std::error::Error for TransportError {}

#[derive(Debug)]
pub enum AortaError {
    InvalidArgument(String),
    NotFound(String),
    Busy(String),
    ShmUnavailable(String),
    CapacityExceeded(String),
    QuotaExceeded(String),
    NotSupported(String),
    Transport(TransportError),
    SchemaConflict(String),
    ConcurrentConflict(String),
    Timeout(String),
    /// The operation was cancelled through a Flow `CancelToken`.
    Cancelled(String),
    AlreadyReplied(String),
    PrefixNotDeclared(String),
    DeclarationNotReady(String),
    KindMismatch(String),
    InstanceIdCollision(String),
    Internal(String),
}

impl AortaError {
    pub fn invalid_argument<T: Into<String>>(msg: T) -> Self {
        Self::InvalidArgument(msg.into())
    }

    pub fn internal<T: Into<String>>(msg: T) -> Self {
        Self::Internal(msg.into())
    }

    pub fn not_found<T: Into<String>>(msg: T) -> Self {
        Self::NotFound(msg.into())
    }

    pub fn busy<T: Into<String>>(msg: T) -> Self {
        Self::Busy(msg.into())
    }

    pub fn shm_unavailable<T: Into<String>>(msg: T) -> Self {
        Self::ShmUnavailable(msg.into())
    }

    pub fn capacity_exceeded<T: Into<String>>(msg: T) -> Self {
        Self::CapacityExceeded(msg.into())
    }

    pub fn quota_exceeded<T: Into<String>>(msg: T) -> Self {
        Self::QuotaExceeded(msg.into())
    }

    pub fn not_supported<T: Into<String>>(msg: T) -> Self {
        Self::NotSupported(msg.into())
    }

    pub fn transport(err: TransportError) -> Self {
        Self::Transport(err)
    }

    pub fn schema_conflict<T: Into<String>>(msg: T) -> Self {
        Self::SchemaConflict(msg.into())
    }

    pub fn concurrent_conflict<T: Into<String>>(msg: T) -> Self {
        Self::ConcurrentConflict(msg.into())
    }

    pub fn timeout<T: Into<String>>(msg: T) -> Self {
        Self::Timeout(msg.into())
    }

    pub fn cancelled<T: Into<String>>(msg: T) -> Self {
        Self::Cancelled(msg.into())
    }

    pub fn already_replied<T: Into<String>>(msg: T) -> Self {
        Self::AlreadyReplied(msg.into())
    }

    pub fn prefix_not_declared<T: Into<String>>(msg: T) -> Self {
        Self::PrefixNotDeclared(msg.into())
    }

    pub fn declaration_not_ready<T: Into<String>>(msg: T) -> Self {
        Self::DeclarationNotReady(msg.into())
    }

    pub fn kind_mismatch<T: Into<String>>(msg: T) -> Self {
        Self::KindMismatch(msg.into())
    }

    pub fn instance_id_collision<T: Into<String>>(msg: T) -> Self {
        Self::InstanceIdCollision(msg.into())
    }

    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::ShmUnavailable(_) | Self::CapacityExceeded(_) | Self::NotSupported(_)
        )
    }
}

impl fmt::Display for AortaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AortaError::InvalidArgument(msg) => write!(f, "invalid argument: {}", msg),
            AortaError::NotFound(msg) => write!(f, "not found: {}", msg),
            AortaError::Busy(msg) => write!(f, "busy: {}", msg),
            AortaError::ShmUnavailable(msg) => write!(f, "shm unavailable: {}", msg),
            AortaError::CapacityExceeded(msg) => write!(f, "capacity exceeded: {}", msg),
            AortaError::QuotaExceeded(msg) => write!(f, "quota exceeded: {}", msg),
            AortaError::NotSupported(msg) => write!(f, "not supported: {}", msg),
            AortaError::Transport(err) => write!(f, "transport error: {}", err),
            AortaError::SchemaConflict(msg) => write!(f, "schema conflict: {}", msg),
            AortaError::ConcurrentConflict(msg) => write!(f, "concurrent conflict: {}", msg),
            AortaError::Timeout(msg) => write!(f, "timeout: {}", msg),
            AortaError::Cancelled(msg) => write!(f, "cancelled: {}", msg),
            AortaError::AlreadyReplied(msg) => write!(f, "already replied: {}", msg),
            AortaError::PrefixNotDeclared(msg) => write!(f, "prefix not declared: {}", msg),
            AortaError::DeclarationNotReady(msg) => write!(f, "declaration not ready: {}", msg),
            AortaError::KindMismatch(msg) => write!(f, "context kind mismatch: {}", msg),
            AortaError::InstanceIdCollision(msg) => write!(f, "instance id collision: {}", msg),
            AortaError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for AortaError {}

impl From<zenoh::Error> for AortaError {
    fn from(err: zenoh::Error) -> Self {
        AortaError::transport(TransportError::backend(err.to_string()))
    }
}

#[cfg(test)]
#[path = "../test/error_tests.rs"]
mod tests;
