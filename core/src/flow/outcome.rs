//! The Flow outcome vocabulary:
//! `Outcome<T> = Result<T, Stopped>` — the stopped channel as a value.
//!
//! `Outcome` is a `Result` ALIAS, not a new enum: stable Rust has no user
//! `Try` impl, and the golden path (`sub.recv().await?`) needs
//! `?`. The `{Value | Stopped}` vocabulary maps onto `Ok / Err(Stopped)`.
//!
//! `StopReason` is a NEW flow-layer enum. It deliberately does NOT extend
//! `aorta_sync::CancelReason` — that type is `repr(u8) + Copy` inside the
//! loom-verified sync crate and cannot carry a panic payload.

use aorta_sync::CancelReason;

/// Why a task/combinator stopped instead of producing a value.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopReason {
    /// Cooperative cancellation (explicit, deadline, shutdown, or
    /// fail-fast sibling failure — the wire-level `CancelReason` verbatim).
    Cancelled(CancelReason),
    /// The task panicked, or the awaited operation faulted underneath it
    /// (transport/service failure on `call_in`, a closed channel on
    /// `recv_in`). Both the stable code and the message are preserved
    /// end-to-end.
    Fault {
        /// The stable `aorta_result_t` the fault entered with:
        /// [`crate::ffi::c_api::AORTA_ERR_INTERNAL`] for panics/join
        /// failures; transport/service faults keep their original code
        /// (via the ONE centralized mapping — never a per-site table).
        code: i32,
        message: Box<str>,
    },
}

/// Terminal "no value" outcome of a Flow task, combinator, or awaitable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stopped(pub StopReason);

/// THE Flow result vocabulary: `Ok(value)` or `Err(Stopped)`.
pub type Outcome<T> = std::result::Result<T, Stopped>;

impl Stopped {
    /// Cooperative-cancel constructor (the common case).
    pub fn cancelled(reason: CancelReason) -> Self {
        Stopped(StopReason::Cancelled(reason))
    }

    /// Fault constructor for faults with no originating [`AortaError`]
    /// (panic payloads, join failures, closed-channel descriptions):
    /// the code is `AORTA_ERR_INTERNAL`.
    pub fn fault(msg: impl Into<Box<str>>) -> Self {
        Stopped(StopReason::Fault {
            code: crate::ffi::c_api::AORTA_ERR_INTERNAL,
            message: msg.into(),
        })
    }

    /// Fault carrying an explicit stable code (callers that already ran
    /// the centralized mapping, or wrap an error with added context).
    pub fn fault_with_code(code: i32, msg: impl Into<Box<str>>) -> Self {
        Stopped(StopReason::Fault {
            code,
            message: msg.into(),
        })
    }

    /// Fault from an [`AortaError`](crate::error::AortaError): the stable
    /// code comes from the ONE centralized Core-boundary mapping; the
    /// message is the error's display form.
    pub fn fault_from_error(err: &crate::error::AortaError) -> Self {
        Stopped(StopReason::Fault {
            code: crate::ffi::c_api::aorta_result_code_of(err),
            message: err.to_string().into(),
        })
    }

    pub fn reason(&self) -> &StopReason {
        &self.0
    }

    /// The stable fault code, when this stop is a fault.
    pub fn fault_code(&self) -> Option<i32> {
        match self.0 {
            StopReason::Fault { code, .. } => Some(code),
            _ => None,
        }
    }

    /// Any cooperative cancellation (deadline included).
    pub fn is_cancel(&self) -> bool {
        matches!(self.0, StopReason::Cancelled(_))
    }

    /// Specifically a budget/deadline expiry.
    pub fn is_deadline(&self) -> bool {
        matches!(
            self.0,
            StopReason::Cancelled(CancelReason::DeadlineExceeded)
        )
    }

    /// A panic (fail-fast source), payload preserved.
    pub fn is_fault(&self) -> bool {
        matches!(self.0, StopReason::Fault { .. })
    }
}

impl std::fmt::Display for Stopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            StopReason::Cancelled(reason) => write!(f, "stopped: cancelled ({reason:?})"),
            StopReason::Fault { code, message } => {
                write!(f, "stopped: fault [{code}] ({message})")
            }
        }
    }
}

impl std::error::Error for Stopped {}

/// Keeps `when_all(...).await?` compiling in fns returning `AortaError`
/// (-9: examples and app code lean on this conversion).
impl From<Stopped> for crate::error::AortaError {
    fn from(stopped: Stopped) -> Self {
        match &stopped.0 {
            StopReason::Cancelled(_) => crate::error::AortaError::cancelled(stopped.to_string()),
            StopReason::Fault { .. } => crate::error::AortaError::internal(stopped.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reason_mapping_preserves_cancel_flavors() {
        for reason in [
            CancelReason::Explicit,
            CancelReason::DeadlineExceeded,
            CancelReason::Shutdown,
            CancelReason::ChildFailed,
        ] {
            let stopped = Stopped::cancelled(reason);
            assert_eq!(stopped, Stopped::cancelled(reason));
            assert!(stopped.is_cancel());
            assert!(!stopped.is_fault());
            assert_eq!(
                stopped.is_deadline(),
                reason == CancelReason::DeadlineExceeded
            );
        }
    }

    #[test]
    fn panic_payload_preserved_through_bridge() {
        let stopped = Stopped::fault("boom at line 7");
        assert!(stopped.is_fault());
        assert!(!stopped.is_cancel());
        match stopped.reason() {
            StopReason::Fault { code, message } => {
                assert_eq!(&**message, "boom at line 7");
                // Panics/join failures carry the INTERNAL code.
                assert_eq!(*code, crate::ffi::c_api::AORTA_ERR_INTERNAL);
            }
            other => panic!("expected fault, got {other:?}"),
        }
    }

    #[test]
    fn fault_from_error_preserves_the_stable_code() {
        // A transport/service fault
        // keeps its ORIGINAL stable code through the flow vocabulary — the
        // earlier `Fault(Box<str>)` folded it into prose.
        let err = crate::error::AortaError::timeout("rpc window elapsed");
        let stopped = Stopped::fault_from_error(&err);
        assert_eq!(stopped.fault_code(), Some(-3), "TIMEOUT is -3, stable");
        match stopped.reason() {
            StopReason::Fault { message, .. } => {
                assert!(message.contains("rpc window elapsed"), "got: {message}");
            }
            other => panic!("expected fault, got {other:?}"),
        }
        // Unknown-reason cancellations stay cancellations (not faults) —
        // the two guarantees compose.
        let stopped = Stopped::cancelled(CancelReason::from_raw(200).unwrap());
        assert!(stopped.is_cancel());
        assert_eq!(stopped.fault_code(), None);
    }

    #[test]
    fn question_mark_ergonomics_and_error_conformance() {
        fn inner(fail: bool) -> Outcome<u32> {
            if fail {
                Err(Stopped::cancelled(CancelReason::Explicit))
            } else {
                Ok(7)
            }
        }
        // `?` through Outcome…
        fn caller() -> Outcome<u32> {
            let v = inner(false)?;
            Ok(v + 1)
        }
        assert_eq!(caller(), Ok(8));
        // …and `?` into AortaError-returning fns (the flow_demo shape).
        fn app() -> Result<u32, crate::error::AortaError> {
            Ok(inner(true)?)
        }
        let err = app().unwrap_err();
        let text = err.to_string();
        assert!(text.contains("cancelled"), "got: {text}");
        // Display + Error object-safety.
        let boxed: Box<dyn std::error::Error> = Box::new(Stopped::fault("panic payload"));
        assert!(boxed.to_string().contains("panic payload"));
    }
}
