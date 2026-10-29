use std::marker::PhantomData;

use aorta_core::error::AortaResult;
pub use aorta_core::schema::TypedSubscribeMessage;
use aorta_core::subscriber::MessageContext;

/// Retained typed sample for callbacks that need to keep payload beyond
/// callback lifetime.
///
/// Unlike the borrowed view delivered by
/// [`Node::create_subscriber_typed_view`](crate::Node::create_subscriber_typed_view),
/// a `TypedRetainedSample` owns a copy of the payload, so the sample itself
/// stays valid after the callback returns and can be moved into other tasks
/// or stored. Note that [`decode`](Self::decode) returns a FlatBuffers view
/// that **borrows the sample** — the view cannot outlive the sample, even
/// though the sample can outlive the callback.
///
/// Obtained from
/// [`Node::create_subscriber_typed_retained`](crate::Node::create_subscriber_typed_retained)
/// (and its `_with_options` / `_for` variants).
pub struct TypedRetainedSample<M: TypedSubscribeMessage> {
    payload: Vec<u8>,
    context: MessageContext,
    _marker: PhantomData<M>,
}

impl<M: TypedSubscribeMessage> TypedRetainedSample<M> {
    pub(crate) fn try_new(payload: Vec<u8>, context: MessageContext) -> AortaResult<Self> {
        M::decode_typed_root(&payload)?;
        Ok(Self {
            payload,
            context,
            _marker: PhantomData,
        })
    }

    /// The raw (still-encoded) payload bytes of this sample.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Receive metadata of this sample (topic, timestamps, priority,
    /// publisher info, …).
    pub fn context(&self) -> &MessageContext {
        &self.context
    }

    /// Consume the sample and return the owned payload bytes.
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }

    /// Decode the sample into the schema's root view. The returned view
    /// borrows the sample — keep the sample alive while using it.
    pub fn decode(&self) -> AortaResult<M::Root<'_>> {
        M::decode_typed_root(&self.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::{TypedRetainedSample, TypedSubscribeMessage};
    use crate::{AortaError, AortaResult, SchemaMeta};
    use aorta_core::subscriber::MessageContext;
    use std::sync::Arc;

    struct DummySchema;

    #[derive(Clone, Copy, Debug)]
    struct DummyRoot<'a> {
        marker: u8,
        bytes: &'a [u8],
    }

    impl SchemaMeta for DummySchema {
        fn schema_bfbs() -> &'static [u8] {
            b"dummy_schema_for_typed_subscribe"
        }
    }

    impl TypedSubscribeMessage for DummySchema {
        type Root<'a> = DummyRoot<'a>;

        fn decode_typed_root<'a>(payload: &'a [u8]) -> AortaResult<Self::Root<'a>> {
            let marker = payload
                .first()
                .copied()
                .ok_or_else(|| AortaError::invalid_argument("empty payload"))?;
            if marker != 0x2A {
                return Err(AortaError::invalid_argument("unexpected marker"));
            }
            Ok(DummyRoot {
                marker,
                bytes: payload,
            })
        }
    }

    fn dummy_context() -> MessageContext {
        MessageContext {
            topic: Arc::from("typed/test/topic"),
            receive_stamp_ns: 123,
            priority: aorta_core::qos::Priority::Normal,
            publisher_stamp_ns: Some(456),
            publisher_node: Some("publisher".to_string()),
            sequence: Some(7),
            source_namespace: None,
        }
    }

    #[test]
    fn typed_retained_sample_decode_success() {
        let sample = TypedRetainedSample::<DummySchema>::try_new(vec![0x2A, 1, 2], dummy_context())
            .expect("sample should validate");
        let root = sample.decode().expect("decode should succeed");
        assert_eq!(root.marker, 0x2A);
        assert_eq!(root.bytes, &[0x2A, 1, 2]);
        assert_eq!(sample.context().topic.as_ref(), "typed/test/topic");
    }

    #[test]
    fn typed_retained_sample_decode_failure() {
        let err =
            match TypedRetainedSample::<DummySchema>::try_new(vec![0x00, 1, 2], dummy_context()) {
                Ok(_) => panic!("validation should fail"),
                Err(err) => err,
            };
        let msg = err.to_string();
        assert!(
            msg.contains("unexpected marker"),
            "unexpected error message: {msg}"
        );
    }
}
