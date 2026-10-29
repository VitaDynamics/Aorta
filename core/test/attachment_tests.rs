//! Unit tests for the composite versioned attachment codec.
//!
//! The compatibility invariants under test are what make a mixed-version fleet
//! safe: schema-only stays byte-identical to the legacy bare 64-hex form, the
//! decoder accepts both forms, and unintelligible input degrades to "no parts"
//! instead of an error.

use super::{
    decode, encode, extract_attachment_schema_hash_bytes, AttachmentParts, ATTACHMENT_MAGIC,
    ATTACHMENT_VERSION,
};
use crate::schema::SchemaHash;
use crate::trace::{TraceParent, TRACEPARENT_SAMPLED};

fn schema() -> SchemaHash {
    SchemaHash::from_bfbs_bytes(b"attachment codec test schema")
}

fn traceparent() -> TraceParent {
    TraceParent {
        trace_id: *b"0123456789abcdef",
        span_id: 0xDEAD_BEEF_CAFE_F00D,
        flags: TRACEPARENT_SAMPLED,
    }
}

#[test]
fn empty_parts_encode_to_no_attachment() {
    assert_eq!(encode(&AttachmentParts::default()), None);
}

#[test]
fn schema_only_stays_byte_identical_to_the_legacy_wire_form() {
    let parts = AttachmentParts {
        schema_hash: Some(schema()),
        ..Default::default()
    };
    let bytes = encode(&parts).expect("schema-only encodes");
    // An OLD receiver parses the attachment as the bare 64-hex hash — the
    // encoding must not change underneath it when no trace context is flowing.
    assert_eq!(bytes, schema().to_hex_bytes().to_vec());
    assert_eq!(SchemaHash::from_hex_bytes(&bytes), Some(schema()));
}

#[test]
fn legacy_bare_hash_decodes_as_schema_only() {
    let parts = decode(&schema().to_hex_bytes());
    assert_eq!(parts.schema_hash, Some(schema()));
    assert_eq!(parts.traceparent, None);
    assert_eq!(parts.reply_busy_ns, None);
}

#[test]
fn composite_roundtrips_all_parts() {
    let parts = AttachmentParts {
        schema_hash: Some(schema()),
        traceparent: Some(traceparent()),
        reply_busy_ns: Some(123_456_789),
        deadline_budget_ns: None,
    };
    let bytes = encode(&parts).expect("encodes");
    assert_eq!(bytes[0], ATTACHMENT_MAGIC);
    assert_eq!(bytes[1], ATTACHMENT_VERSION);
    assert_eq!(decode(&bytes), parts);
}

#[test]
fn traceparent_without_schema_roundtrips() {
    let parts = AttachmentParts {
        traceparent: Some(traceparent()),
        ..Default::default()
    };
    let bytes = encode(&parts).expect("encodes");
    assert_eq!(bytes[0], ATTACHMENT_MAGIC);
    assert_eq!(decode(&bytes), parts);
}

#[test]
fn unknown_tlv_types_are_skipped_not_fatal() {
    let parts = AttachmentParts {
        traceparent: Some(traceparent()),
        ..Default::default()
    };
    let mut bytes = encode(&parts).expect("encodes");
    // Append a future TLV type the current decoder has never heard of.
    bytes.extend_from_slice(&[0x7F, 3, 1, 2, 3]);
    assert_eq!(decode(&bytes), parts);

    // Also when the unknown TLV comes FIRST.
    let mut prefixed = vec![ATTACHMENT_MAGIC, ATTACHMENT_VERSION, 0x7F, 2, 9, 9];
    prefixed.extend_from_slice(&bytes[2..]);
    assert_eq!(decode(&prefixed), parts);
}

#[test]
fn truncated_tlv_keeps_the_parts_already_parsed() {
    let parts = AttachmentParts {
        schema_hash: Some(schema()),
        traceparent: Some(traceparent()),
        ..Default::default()
    };
    let bytes = encode(&parts).expect("encodes");
    // Cut into the middle of the traceparent TLV: the schema TLV (which comes
    // first) must survive.
    let cut = &bytes[..bytes.len() - 5];
    let decoded = decode(cut);
    assert_eq!(decoded.schema_hash, Some(schema()));
    assert_eq!(decoded.traceparent, None);
}

#[test]
fn garbage_and_wrong_versions_decode_to_empty() {
    assert!(decode(&[]).is_empty());
    assert!(decode(b"not a hash at all").is_empty());
    // 64 bytes but not hex.
    assert!(decode(&[b'z'; 64]).is_empty());
    // Magic with a FUTURE version: unintelligible by contract.
    assert!(decode(&[ATTACHMENT_MAGIC, ATTACHMENT_VERSION + 1, 0x01, 0]).is_empty());
    // Bare magic, no version byte.
    assert!(decode(&[ATTACHMENT_MAGIC]).is_empty());
}

#[test]
fn zero_trace_id_traceparent_is_rejected_on_decode() {
    let tp = TraceParent {
        trace_id: [0u8; 16],
        span_id: 7,
        flags: TRACEPARENT_SAMPLED,
    };
    let bytes = encode(&AttachmentParts {
        traceparent: Some(tp),
        ..Default::default()
    })
    .expect("encodes");
    // All-zero trace_id is the "no trace" sentinel — never a valid propagation.
    assert_eq!(decode(&bytes).traceparent, None);
}

#[test]
fn traceparent_wire_form_roundtrips_ids_exactly() {
    let tp = traceparent();
    let wire = tp.to_wire_bytes();
    assert_eq!(wire.len(), 25);
    assert_eq!(TraceParent::from_wire_bytes(&wire), Some(tp));
    assert_eq!(TraceParent::from_wire_bytes(&wire[..24]), None);
}

#[test]
fn extract_schema_hash_bytes_requires_64_hex() {
    assert!(extract_attachment_schema_hash_bytes(b"abc").is_none());
    assert!(extract_attachment_schema_hash_bytes(&[b'g'; 64]).is_none());
    // Canonical bare attachments are LOWERCASE 64-hex.
    assert_eq!(
        extract_attachment_schema_hash_bytes(&[b'a'; 64]).as_deref(),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    // Uppercase bare-hex is no longer accepted (consistent with core decode).
    assert!(extract_attachment_schema_hash_bytes(&[b'A'; 64]).is_none());
}

#[test]
fn extract_schema_hash_bytes_decodes_composite_tlv() {
    let hash = SchemaHash::from_bytes([0xABu8; 32]);
    let expected = hash.to_hex();
    let composite = encode(&AttachmentParts {
        schema_hash: Some(hash),
        reply_busy_ns: Some(42),
        ..Default::default()
    })
    .expect("composite attachment");
    // Sanity: this is the composite form, not the legacy 64-hex form.
    assert_ne!(composite.len(), 64);
    assert_eq!(
        extract_attachment_schema_hash_bytes(&composite).as_deref(),
        Some(expected.as_str())
    );
}
