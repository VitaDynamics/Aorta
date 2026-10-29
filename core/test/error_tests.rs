use super::*;

#[test]
fn error_invalid_argument_creation() {
    let err = AortaError::invalid_argument("test message");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
    if let AortaError::InvalidArgument(msg) = err {
        assert_eq!(msg, "test message");
    }
}

#[test]
fn error_invalid_argument_from_string() {
    let err = AortaError::invalid_argument(String::from("string message"));
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn error_internal_creation() {
    let err = AortaError::internal("internal error");
    assert!(matches!(err, AortaError::Internal(_)));
    if let AortaError::Internal(msg) = err {
        assert_eq!(msg, "internal error");
    }
}

#[test]
fn error_not_found_creation() {
    let err = AortaError::not_found("resource missing");
    assert!(matches!(err, AortaError::NotFound(_)));
    if let AortaError::NotFound(msg) = err {
        assert_eq!(msg, "resource missing");
    }
}

#[test]
fn error_busy_creation() {
    let err = AortaError::busy("queue full");
    assert!(matches!(err, AortaError::Busy(_)));
    if let AortaError::Busy(msg) = err {
        assert_eq!(msg, "queue full");
    }
}

#[test]
fn error_shm_unavailable_creation() {
    let err = AortaError::shm_unavailable("shm pool exhausted");
    assert!(matches!(err, AortaError::ShmUnavailable(_)));
    if let AortaError::ShmUnavailable(msg) = err {
        assert_eq!(msg, "shm pool exhausted");
    }
}

#[test]
fn error_capacity_exceeded_creation() {
    let err = AortaError::capacity_exceeded("message exceeds max size");
    assert!(matches!(err, AortaError::CapacityExceeded(_)));
    if let AortaError::CapacityExceeded(msg) = err {
        assert_eq!(msg, "message exceeds max size");
    }
}

#[test]
fn error_quota_exceeded_creation() {
    let err = AortaError::quota_exceeded("max loans reached");
    assert!(matches!(err, AortaError::QuotaExceeded(_)));
    if let AortaError::QuotaExceeded(msg) = err {
        assert_eq!(msg, "max loans reached");
    }
}

#[test]
fn error_not_supported_creation() {
    let err = AortaError::not_supported("in-place serializer not implemented");
    assert!(matches!(err, AortaError::NotSupported(_)));
    if let AortaError::NotSupported(msg) = err {
        assert_eq!(msg, "in-place serializer not implemented");
    }
}

#[test]
fn error_already_replied_creation() {
    let err = AortaError::already_replied("request already handled");
    assert!(matches!(err, AortaError::AlreadyReplied(_)));
    if let AortaError::AlreadyReplied(msg) = err {
        assert_eq!(msg, "request already handled");
    }
}

#[test]
fn error_schema_conflict_creation() {
    let err = AortaError::schema_conflict("schema mismatch");
    assert!(matches!(err, AortaError::SchemaConflict(_)));
    if let AortaError::SchemaConflict(msg) = err {
        assert_eq!(msg, "schema mismatch");
    }
}

#[test]
fn error_concurrent_conflict_creation() {
    let err = AortaError::concurrent_conflict("concurrent access");
    assert!(matches!(err, AortaError::ConcurrentConflict(_)));
    if let AortaError::ConcurrentConflict(msg) = err {
        assert_eq!(msg, "concurrent access");
    }
}

#[test]
fn error_display_invalid_argument() {
    let err = AortaError::invalid_argument("bad argument");
    let display = format!("{}", err);
    assert!(display.contains("invalid argument"));
    assert!(display.contains("bad argument"));
}

#[test]
fn error_display_transport() {
    let err = AortaError::transport(TransportError::backend("tx path unavailable"));
    let display = format!("{}", err);
    assert!(display.contains("transport error"));
    assert!(display.contains("tx path unavailable"));
}

#[test]
fn error_display_schema_conflict() {
    let err = AortaError::schema_conflict("hash mismatch");
    let display = format!("{}", err);
    assert!(display.contains("schema conflict"));
    assert!(display.contains("hash mismatch"));
}

#[test]
fn error_display_concurrent_conflict() {
    let err = AortaError::concurrent_conflict("registration failed");
    let display = format!("{}", err);
    assert!(display.contains("concurrent conflict"));
    assert!(display.contains("registration failed"));
}

#[test]
fn error_display_internal() {
    let err = AortaError::internal("unexpected failure");
    let display = format!("{}", err);
    assert!(display.contains("internal error"));
    assert!(display.contains("unexpected failure"));
}

#[test]
fn error_display_not_found() {
    let err = AortaError::not_found("missing handle");
    let display = format!("{}", err);
    assert!(display.contains("not found"));
    assert!(display.contains("missing handle"));
}

#[test]
fn error_display_busy() {
    let err = AortaError::busy("queue saturated");
    let display = format!("{}", err);
    assert!(display.contains("busy"));
    assert!(display.contains("queue saturated"));
}

#[test]
fn error_display_shm_unavailable() {
    let err = AortaError::shm_unavailable("pool not ready");
    let display = format!("{}", err);
    assert!(display.contains("shm unavailable"));
    assert!(display.contains("pool not ready"));
}

#[test]
fn error_display_capacity_exceeded() {
    let err = AortaError::capacity_exceeded("buffer too small");
    let display = format!("{}", err);
    assert!(display.contains("capacity exceeded"));
    assert!(display.contains("buffer too small"));
}

#[test]
fn error_display_quota_exceeded() {
    let err = AortaError::quota_exceeded("loan limit reached");
    let display = format!("{}", err);
    assert!(display.contains("quota exceeded"));
    assert!(display.contains("loan limit reached"));
}

#[test]
fn error_display_not_supported() {
    let err = AortaError::not_supported("policy unsupported");
    let display = format!("{}", err);
    assert!(display.contains("not supported"));
    assert!(display.contains("policy unsupported"));
}

#[test]
fn error_display_already_replied() {
    let err = AortaError::already_replied("duplicate reply");
    let display = format!("{}", err);
    assert!(display.contains("already replied"));
    assert!(display.contains("duplicate reply"));
}

#[test]
fn error_implements_std_error_trait() {
    let err = AortaError::invalid_argument("test");
    let _: &dyn std::error::Error = &err;
}

#[test]
fn error_from_zenoh_error_maps_to_transport() {
    // zenoh::Error is Box<dyn StdError + Send + Sync>, can't construct directly
    // Test that the From trait exists and works correctly
    // In practice, zenoh errors come from zenoh operations
    fn convert_to_aorta_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> AortaError {
        AortaError::from(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    use std::io;
    let io_err = io::Error::other("test");
    let aorta_err = convert_to_aorta_error(io_err);
    assert!(matches!(
        aorta_err,
        AortaError::Transport(TransportError::Backend(_))
    ));
}

#[test]
fn error_debug_format() {
    let err = AortaError::invalid_argument("debug test");
    let debug = format!("{:?}", err);
    assert!(debug.contains("InvalidArgument"));
    assert!(debug.contains("debug test"));
}

#[test]
fn aorta_result_type_alias() {
    // Test that AortaResult works as expected
    fn success_fn() -> AortaResult<i32> {
        Ok(42)
    }

    fn error_fn() -> AortaResult<i32> {
        Err(AortaError::invalid_argument("failed"))
    }

    assert_eq!(success_fn().unwrap(), 42);
    assert!(error_fn().is_err());
}

#[test]
fn error_equality_by_variant() {
    // Note: AortaError doesn't derive PartialEq, but we can check variants
    let err1 = AortaError::invalid_argument("msg1");
    let err2 = AortaError::invalid_argument("msg2");
    let err3 = AortaError::internal("msg1");

    // Same variant type
    assert!(matches!(err1, AortaError::InvalidArgument(_)));
    assert!(matches!(err2, AortaError::InvalidArgument(_)));

    // Different variant
    assert!(matches!(err3, AortaError::Internal(_)));
}

#[test]
fn error_empty_message() {
    let err = AortaError::invalid_argument("");
    let display = format!("{}", err);
    assert!(display.contains("invalid argument"));
    // Should not panic with empty message
}

#[test]
fn error_long_message() {
    let long_msg = "a".repeat(1000);
    let err = AortaError::invalid_argument(long_msg.clone());
    let display = format!("{}", err);
    assert!(display.contains(&long_msg));
}

#[test]
fn error_special_characters_in_message() {
    let special_msg = "Error: special chars \n\t\"'!@#$%";
    let err = AortaError::invalid_argument(special_msg);
    let display = format!("{}", err);
    assert!(display.contains(special_msg));
}

#[test]
fn error_recoverable_classification() {
    assert!(AortaError::shm_unavailable("x").is_recoverable());
    assert!(AortaError::capacity_exceeded("x").is_recoverable());
    assert!(AortaError::not_supported("x").is_recoverable());
    assert!(!AortaError::quota_exceeded("x").is_recoverable());
    assert!(!AortaError::internal("x").is_recoverable());
}

#[test]
fn error_prefix_not_declared_creation_and_display() {
    let err = AortaError::prefix_not_declared("demo/state");
    assert!(matches!(err, AortaError::PrefixNotDeclared(_)));
    assert_eq!(err.to_string(), "prefix not declared: demo/state");
}

#[test]
fn error_declaration_not_ready_creation_and_display() {
    let err = AortaError::declaration_not_ready("demo/**");
    assert!(matches!(err, AortaError::DeclarationNotReady(_)));
    assert_eq!(err.to_string(), "declaration not ready: demo/**");
}

#[test]
fn error_kind_mismatch_creation_and_display() {
    let err = AortaError::kind_mismatch("expected Bool got U64");
    assert!(matches!(err, AortaError::KindMismatch(_)));
    assert_eq!(
        err.to_string(),
        "context kind mismatch: expected Bool got U64"
    );
}

#[test]
fn error_instance_id_collision_creation_and_display() {
    let err = AortaError::instance_id_collision("rcp_node");
    assert!(matches!(err, AortaError::InstanceIdCollision(_)));
    assert_eq!(err.to_string(), "instance id collision: rcp_node");
}

#[test]
fn ctx_errors_not_recoverable() {
    assert!(!AortaError::prefix_not_declared("x").is_recoverable());
    assert!(!AortaError::declaration_not_ready("x").is_recoverable());
    assert!(!AortaError::kind_mismatch("x").is_recoverable());
    assert!(!AortaError::instance_id_collision("x").is_recoverable());
}

// Suppress unused variable warnings for test data
#[allow(unused_variables)]
fn _suppress_warnings() {
    let _ = AortaError::invalid_argument("test");
}
