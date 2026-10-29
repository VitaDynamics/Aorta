//! Schema preflight (MCAP Player Proposal §6.3) — the point-in-time split-brain
//! guard.
//!
//! For every planned **typed** topic the player asks the group's
//! `sys/schema_by_key/<suffix>` queryables what schema currently owns the key,
//! *before* it registers anything, and once more *after* its own registrations:
//!
//! | Observation | Verdict |
//! |---|---|
//! | reply error (not a synthesized query timeout) | hard error |
//! | malformed payload / wrong kind | hard error |
//! | claimed hash ≠ recomputed BFBS hash | hard error |
//! | valid reply, hash ≠ planned | hard error (topic + both hashes) |
//! | clean drain, zero replies | unclaimed — first scan proceeds, second scan is a divergence |
//! | deadline before the drain | inconclusive → hard error |
//!
//! The query mirrors core's multi-responder precedent
//! (`schema_resolve.rs:371`): `QueryTarget::All` so every responder answers,
//! `ConsolidationMode::None` because the default `Auto`→`Latest` would hide a
//! second same-key responder — which is exactly the case this guard exists to
//! catch — and one absolute deadline across the get *and* the drain.

use anyhow::{anyhow, bail, Result};
use aorta_core::key_expr::KeyExprBuilder;
use aorta_core::reply_error_is_zenoh_query_timeout;
use aorta_core::schema::{bfbs_root_type_name, SchemaHash};
use aorta_core::schema_registry::SchemaByKeyValue;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use zenoh::query::{ConsolidationMode, QueryTarget};

/// Slack added to the *zenoh* query timeout on top of the player's own
/// absolute deadline.
///
/// The player's deadline stays `T` (MCAP Player Proposal §6.3). The grace only
/// makes sure the reply channel can close for exactly one reason — the query
/// genuinely finished — so that "cleanly drained" and "deadline before drain"
/// are distinguishable instead of racing zenoh's own timer, which would let an
/// inconclusive scan masquerade as "unclaimed" (the one classification §6.3
/// forbids).
const QUERY_TIMEOUT_GRACE: Duration = Duration::from_millis(250);

/// Which of the two MCAP Player Proposal §6.3 scans is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanPhase {
    /// Before any registration: an unclaimed key is fine.
    First,
    /// After the player's own registrations: the key must be claimed by exactly
    /// the player's own schema.
    Second,
}

impl ScanPhase {
    fn label(self) -> &'static str {
        match self {
            Self::First => "first",
            Self::Second => "second",
        }
    }
}

/// What one scan of one topic observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScanOutcome {
    /// Cleanly drained with no responder.
    Unclaimed,
    /// At least one responder, all agreeing with the planned hash.
    Claimed,
    /// The shutdown token fired mid-scan (MCAP Player Proposal §6.7).
    Cancelled,
}

/// Scan one typed topic's by-key slot. `key_suffix` is the group-relative
/// suffix core registers under (`pub/<topic>`), `expected_hash` the planned
/// schema's canonical hex hash.
pub(crate) async fn scan_topic(
    session: &zenoh::Session,
    key_builder: &KeyExprBuilder,
    topic_key: &str,
    key_suffix: &str,
    expected_hash: &str,
    timeout: Duration,
    phase: ScanPhase,
    token: &CancellationToken,
) -> Result<ScanOutcome> {
    let selector: String = key_builder
        .schema_by_key(key_suffix)
        .map_err(|err| anyhow!("cannot build the schema_by_key selector for '{topic_key}': {err}"))?
        .into();

    // One absolute deadline across get + drain; stamped before the get so the
    // worst case is T, not ~2×T (core's `collect_get_replies` precedent).
    let started = Instant::now();
    let replies = tokio::select! {
        biased;
        _ = token.cancelled() => return Ok(ScanOutcome::Cancelled),
        replies = session
            .get(&selector)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .timeout(timeout + QUERY_TIMEOUT_GRACE) => replies.map_err(|err| {
                anyhow!("schema preflight query '{selector}' failed: {err}")
            })?,
    };

    let mut claimed = false;
    loop {
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            bail!("{}", inconclusive(topic_key, phase, timeout));
        }
        let remaining = timeout - elapsed;
        let received = tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(ScanOutcome::Cancelled),
            received = tokio::time::timeout(remaining, replies.recv_async()) => received,
        };
        let reply = match received {
            // Our own deadline fired before the reply stream drained: the
            // observation is inconclusive and must never be read as
            // "unclaimed" (MCAP Player Proposal §6.3).
            Err(_elapsed) => bail!("{}", inconclusive(topic_key, phase, timeout)),
            // The channel closed: the query is complete — a clean drain.
            Ok(Err(_closed)) => break,
            Ok(Ok(reply)) => reply,
        };

        match reply.result() {
            // Zenoh (or a router's query cleanup) synthesizes a bare `Timeout`
            // error reply when a `get` with a timeout expires. That is a
            // dual-guard timeout firing, not a responder answering, so it is
            // not an MCAP Player Proposal §6.3 "reply error"; the authoritative
            // deadline check is the elapsed-time test above. Use Core's shared
            // protocol classifier.
            Err(err) if reply_error_is_zenoh_query_timeout(err) => continue,
            Err(err) => bail!(
                "schema preflight ({} scan) for topic '{}' got an error reply from a \
                 schema_by_key responder: {} (MCAP Player Proposal §6.3 is fail-closed)",
                phase.label(),
                topic_key,
                aorta_core::reply_error_text(err)
            ),
            Ok(sample) => {
                let payload = sample.payload().to_bytes();
                let hash = decode_topic_schema_hash(payload.as_ref()).map_err(|err| {
                    anyhow!(
                        "schema preflight ({} scan) for topic '{}': {} (MCAP Player Proposal §6.3)",
                        phase.label(),
                        topic_key,
                        err
                    )
                })?;
                if hash != expected_hash {
                    bail!(
                        "topic '{}' is already claimed by a different schema: responder serves \
                         {}, this replay would publish {} — resolve the conflict or exclude the \
                         topic (MCAP Player Proposal §6.3, {} scan)",
                        topic_key,
                        hash,
                        expected_hash,
                        phase.label()
                    );
                }
                claimed = true;
            }
        }
    }

    Ok(if claimed {
        ScanOutcome::Claimed
    } else {
        ScanOutcome::Unclaimed
    })
}

fn inconclusive(topic_key: &str, phase: ScanPhase, timeout: Duration) -> String {
    format!(
        "schema preflight ({} scan) for topic '{}' hit its {:?} deadline before the reply \
         stream drained — the observation is inconclusive, never 'unclaimed'; retry with a \
         larger --preflight-timeout or resolve the conflict (MCAP Player Proposal §6.3)",
        phase.label(),
        topic_key,
        timeout
    )
}

/// Decode a `schema_by_key` reply with core's by-key decoder semantics
/// (`schema_resolve.rs:344`): wrong kind or malformed payload is a failure, and
/// the claimed hash is re-derived from the BFBS bytes before it is believed.
pub(crate) fn decode_topic_schema_hash(payload: &[u8]) -> Result<String> {
    let value = SchemaByKeyValue::decode(payload).ok_or_else(|| {
        anyhow!(
            "reply payload is not a decodable topic schema_by_key value ({} bytes; malformed, \
             or a service/param kind on a topic key)",
            payload.len()
        )
    })?;
    let recomputed = SchemaHash::from_bfbs_bytes(value.bfbs());
    if recomputed != value.hash() {
        bail!(
            "reply claims schema hash {} but its bfbs hashes to {}",
            value.hash().to_hex(),
            recomputed.to_hex()
        );
    }
    bfbs_root_type_name(value.bfbs()).ok_or_else(|| {
        anyhow!(
            "reply schema hash {} has no readable FlatBuffers root type name",
            value.hash().to_hex()
        )
    })?;
    Ok(value.hash().to_hex())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_valid_topic_payload_decodes_to_its_canonical_hash() {
        let bfbs = aorta_core::telemetry::fb::TELEMETRY_NODE_IDENTITY_BFBS.to_vec();
        let hash = SchemaHash::from_bfbs_bytes(&bfbs);
        let payload = SchemaByKeyValue::topic(hash, bfbs, None).encode();
        assert_eq!(
            decode_topic_schema_hash(&payload).unwrap(),
            hash.to_hex(),
            "the decoder returns the recomputed, canonical hash"
        );
    }

    #[test]
    fn a_malformed_payload_is_a_hard_error() {
        let err = decode_topic_schema_hash(b"not a flatbuffer at all").unwrap_err();
        assert!(err.to_string().contains("not a decodable"), "got: {err}");
    }

    #[test]
    fn a_service_kind_reply_on_a_topic_key_is_a_hard_error() {
        use aorta_core::schema_registry::ServiceSchemaByKeyValue;
        let bfbs = b"bfbs-service".to_vec();
        let payload =
            ServiceSchemaByKeyValue::new(SchemaHash::from_bfbs_bytes(&bfbs), bfbs, None, None)
                .encode();
        assert!(
            decode_topic_schema_hash(&payload).is_err(),
            "a service value on a topic by-key slot is the wrong kind (MCAP Player Proposal §6.3)"
        );
    }

    #[test]
    fn a_claimed_hash_that_disagrees_with_the_bfbs_is_a_hard_error() {
        // Forge a payload whose claimed hash belongs to *other* bytes: exactly
        // the case core's decoder recomputes to catch.
        let honest = SchemaHash::from_bfbs_bytes(b"bfbs-a");
        let payload = SchemaByKeyValue::topic(honest, b"bfbs-b".to_vec(), None).encode();
        let err = decode_topic_schema_hash(&payload).unwrap_err();
        assert!(err.to_string().contains("hashes to"), "got: {err}");
    }

    #[test]
    fn a_bfbs_without_a_root_table_is_a_hard_error() {
        let bfbs = b"bfbs-without-a-root-table".to_vec();
        let hash = SchemaHash::from_bfbs_bytes(&bfbs);
        let payload = SchemaByKeyValue::topic(hash, bfbs, None).encode();
        let err = decode_topic_schema_hash(&payload).unwrap_err();
        assert!(err.to_string().contains("root"), "got: {err}");
    }
}
