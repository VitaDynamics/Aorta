//! Composite, versioned Zenoh-attachment codec.
//!
//! Historically the attachment was a SINGLE value: the 64-hex-char schema hash.
//! Cross-process trace propagation needs to ride the same attachment, so this
//! module frames multiple parts as TLVs behind a magic + version byte:
//!
//! ```text
//! [0] magic 0xAA   (not an ASCII hex char — never confusable with a bare hash)
//! [1] version 0x01
//! then 0+ TLVs: [type u8][len u8][payload `len` bytes]
//! ```
//!
//! Compatibility invariants (mixed-version fleets MUST stay safe):
//! - **Encode**: with no trace parts, a schema-only attachment is emitted in the
//!   LEGACY bare 64-hex form, byte-identical to the historical wire format — an
//!   old receiver never sees a composite unless trace context is actually
//!   flowing (and a service only echoes a composite reply to a request that
//!   itself carried a traceparent, so old clients always get legacy replies).
//! - **Decode**: a bare 64-hex attachment parses as schema-only (legacy peers);
//!   unknown TLV types are SKIPPED (forward compat); a truncated TLV ends the
//!   walk, keeping the parts parsed so far; anything else parses as empty —
//!   attachments are advisory (schema warn / trace join), never load-bearing
//!   for delivery, so "ignore what you don't understand" is the right failure
//!   mode.
//!
//! Wire size when tracing rides along: 2 (header) + 27 (traceparent TLV)
//! + optionally 34 (schema TLV, raw 32 bytes) / 10 (reply context TLV).

use crate::schema::SchemaHash;
use crate::trace::{TraceParent, TRACEPARENT_WIRE_LEN};

/// First byte of a composite attachment. `0xAA` is outside every ASCII-hex
/// range, so composite vs legacy-bare-hash is decided by one byte.
pub const ATTACHMENT_MAGIC: u8 = 0xAA;

/// Composite format version. Bump on layout changes; decoders reject newer
/// versions (the parts would be unintelligible, and attachments are advisory).
pub const ATTACHMENT_VERSION: u8 = 1;

/// TLV: raw 32-byte schema hash (the legacy payload, un-hexed).
const TLV_SCHEMA_HASH: u8 = 0x01;
/// TLV: 25-byte traceparent (`16B trace_id + 8B span_id LE + 1B flags`).
const TLV_TRACEPARENT: u8 = 0x02;
/// TLV: reply context — `8B server busy duration ns LE` (a single-machine
/// MONOTONIC duration, so the client derives a network estimate without
/// subtracting two unaligned clocks).
const TLV_REPLY_CTX: u8 = 0x03;
/// TLV: deadline budget — `8B RELATIVE remaining-budget ns LE` stamped at
/// send . Relative because clock domains differ
/// across machines; the receiver re-anchors on arrival
/// (`Budget::from_wire_relative_ns` — a cooperative NARROWING hint; the
/// sender's own guard stays authoritative).
const TLV_DEADLINE: u8 = 0x04;

/// The decoded (or to-be-encoded) parts of one attachment.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AttachmentParts {
    /// Schema identity of the payload (legacy single value; TLV 0x01).
    pub schema_hash: Option<SchemaHash>,
    /// Propagated trace context (TLV 0x02).
    pub traceparent: Option<TraceParent>,
    /// Server busy duration echoed on a reply (TLV 0x03).
    pub reply_busy_ns: Option<u64>,
    /// Relative remaining-budget ns stamped at send (TLV 0x04).
    pub deadline_budget_ns: Option<u64>,
}

impl AttachmentParts {
    pub fn is_empty(&self) -> bool {
        self.schema_hash.is_none()
            && self.traceparent.is_none()
            && self.reply_busy_ns.is_none()
            && self.deadline_budget_ns.is_none()
    }
}

/// Encode `parts` for the wire. `None` when there is nothing to attach.
/// Schema-only stays LEGACY bare 64-hex (see module compatibility invariants);
/// any trace part promotes the attachment to the composite form.
pub fn encode(parts: &AttachmentParts) -> Option<Vec<u8>> {
    if parts.is_empty() {
        return None;
    }
    if parts.traceparent.is_none()
        && parts.reply_busy_ns.is_none()
        && parts.deadline_budget_ns.is_none()
    {
        let hash = parts.schema_hash.expect("non-empty parts with only schema");
        return Some(hash.to_hex_bytes().to_vec());
    }
    let mut out = Vec::with_capacity(2 + 34 + 27 + 10);
    out.push(ATTACHMENT_MAGIC);
    out.push(ATTACHMENT_VERSION);
    if let Some(hash) = parts.schema_hash {
        out.push(TLV_SCHEMA_HASH);
        out.push(32);
        out.extend_from_slice(hash.as_bytes());
    }
    if let Some(tp) = parts.traceparent {
        out.push(TLV_TRACEPARENT);
        out.push(TRACEPARENT_WIRE_LEN as u8);
        out.extend_from_slice(&tp.to_wire_bytes());
    }
    if let Some(busy_ns) = parts.reply_busy_ns {
        out.push(TLV_REPLY_CTX);
        out.push(8);
        out.extend_from_slice(&busy_ns.to_le_bytes());
    }
    if let Some(budget_ns) = parts.deadline_budget_ns {
        out.push(TLV_DEADLINE);
        out.push(8);
        out.extend_from_slice(&budget_ns.to_le_bytes());
    }
    Some(out)
}

/// Decode an attachment in either wire form. Never fails: unintelligible input
/// decodes to empty parts (see module compatibility invariants).
pub fn decode(bytes: &[u8]) -> AttachmentParts {
    let mut parts = AttachmentParts::default();
    if bytes.first() != Some(&ATTACHMENT_MAGIC) {
        // Legacy form: exactly the 64-hex schema hash (or something we don't
        // understand, which decodes to empty).
        parts.schema_hash = SchemaHash::from_hex_bytes(bytes);
        return parts;
    }
    if bytes.get(1) != Some(&ATTACHMENT_VERSION) {
        return parts;
    }
    let mut rest = &bytes[2..];
    while rest.len() >= 2 {
        let (ty, len) = (rest[0], rest[1] as usize);
        let Some(payload) = rest.get(2..2 + len) else {
            break; // truncated TLV: keep what already parsed
        };
        match ty {
            TLV_SCHEMA_HASH => {
                if parts.schema_hash.is_none() {
                    if let Ok(raw) = <[u8; 32]>::try_from(payload) {
                        parts.schema_hash = Some(SchemaHash::from_bytes(raw));
                    }
                }
            }
            TLV_TRACEPARENT => {
                if parts.traceparent.is_none() {
                    parts.traceparent = TraceParent::from_wire_bytes(payload);
                }
            }
            TLV_REPLY_CTX => {
                if parts.reply_busy_ns.is_none() {
                    if let Ok(raw) = <[u8; 8]>::try_from(payload) {
                        parts.reply_busy_ns = Some(u64::from_le_bytes(raw));
                    }
                }
            }
            TLV_DEADLINE => {
                if parts.deadline_budget_ns.is_none() {
                    if let Ok(raw) = <[u8; 8]>::try_from(payload) {
                        parts.deadline_budget_ns = Some(u64::from_le_bytes(raw));
                    }
                }
            }
            _ => {} // unknown TLV: skip (forward compat)
        }
        rest = &rest[2 + len..];
    }
    parts
}

/// Extract the schema-hash from a Zenoh sample's attachment.
///
/// Accepts both the legacy bare 64-hex form and the composite TLV form
/// introduced for trace propagation.
pub fn extract_attachment_schema_hash(sample: &zenoh::sample::Sample) -> Option<String> {
    sample
        .attachment()
        .and_then(|a| extract_attachment_schema_hash_bytes(&a.to_bytes()))
}

/// Extract the schema-hash from raw attachment bytes.
///
/// This is the byte-oriented counterpart of [`extract_attachment_schema_hash`];
/// it accepts the same wire forms and is useful when a `zenoh::sample::Sample`
/// is not available.
pub fn extract_attachment_schema_hash_bytes(bytes: &[u8]) -> Option<String> {
    decode(bytes).schema_hash.map(|hash| hash.to_hex())
}

#[cfg(test)]
#[path = "../test/attachment_tests.rs"]
mod tests;

#[cfg(test)]
mod deadline_tlv_tests {
    use super::*;

    #[test]
    fn deadline_budget_roundtrips_and_promotes_composite() {
        let parts = AttachmentParts {
            schema_hash: None,
            traceparent: None,
            reply_busy_ns: None,
            deadline_budget_ns: Some(1_500_000_000),
        };
        let bytes = encode(&parts).expect("non-empty encodes");
        assert_eq!(bytes[0], ATTACHMENT_MAGIC, "deadline promotes composite");
        let back = decode(&bytes);
        assert_eq!(back.deadline_budget_ns, Some(1_500_000_000));

        // Composite with every part.
        let full = AttachmentParts {
            schema_hash: parts.schema_hash,
            traceparent: None,
            reply_busy_ns: Some(7),
            deadline_budget_ns: Some(42),
        };
        let bytes = encode(&full).expect("encodes");
        let back = decode(&bytes);
        assert_eq!(back.reply_busy_ns, Some(7));
        assert_eq!(back.deadline_budget_ns, Some(42));
    }

    #[test]
    fn absent_deadline_stays_v1_shaped() {
        // Schema-only stays LEGACY bare hex (old receivers unaffected).
        let parts = AttachmentParts {
            schema_hash: crate::schema::SchemaHash::from_hex_bytes(&[b'a'; 64]),
            traceparent: None,
            reply_busy_ns: None,
            deadline_budget_ns: None,
        };
        let bytes = encode(&parts).expect("encodes");
        assert_ne!(bytes[0], ATTACHMENT_MAGIC, "legacy form preserved");
        assert_eq!(decode(&bytes).deadline_budget_ns, None);
    }
}
