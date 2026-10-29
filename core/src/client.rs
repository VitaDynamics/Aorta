//! Client (RPC caller) implementation for Aorta middleware.
//!
//! A Client sends requests to a Service and waits for responses.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::OnceCell as TokioOnceCell;
use tokio::time::{timeout, Instant};
use zenoh::query::{ConsolidationMode, Querier, QueryTarget};

use crate::error::{AortaError, AortaResult};
use crate::key_expr::{key_builder_from_group_env, KeyExprBuilder};
use crate::log_prelude::*;
use crate::runtime_env;
use crate::schema::{SchemaAttachPolicy, SchemaHash as SchemaInfo};
use crate::schema_registry::SchemaRegistry;
use crate::session::Session;
use crate::telemetry::{
    RuntimeMetricsTelemetryPublisher, RuntimeTelemetryGuard, TelemetryResourceGuard,
    VitalSnapshotTelemetryPublisher,
};
use crate::time::unix_time_ns;
use crate::transport::{reply_error_is_zenoh_query_timeout, reply_error_text};
use crate::ZBytes;

/// Default timeout for client calls (5 seconds)
const DEFAULT_CALL_TIMEOUT_MS: u64 = 5000;
const DEFAULT_MULTI_REPLY_CHECK_WINDOW_MS: u64 = 5;
/// Grace added to the Zenoh `.get().timeout()` over the application-side tokio
/// deadline (the dual-guard).
///
/// Without an explicit `.timeout()` the get builder defaults `ext_timeout` to
/// the session's `queries_default_timeout` (600 s on robots), and dropping the
/// `replies` receiver does NOT cancel the query in this Zenoh fork
/// (`cancel_query` is `unstable`, local-only, and never notifies the router) —
/// so every abandoned call leaks a pending query on this client's dispatcher
/// AND every router hop until that 600 s timer fires (observed in production:
/// 388 × 600 s `Didn't receive final reply`). Setting `.timeout(remaining +
/// grace)` bounds router cleanup to ~the call deadline.
///
/// The grace (rather than `remaining` exactly) keeps the application-side tokio
/// guard deterministically first: the Zenoh timer is armed at query-send while
/// the tokio timer is armed a hair later, so equal durations let the router's
/// synthesized `Err("Timeout")` reply win the race. `reply_error_is_zenoh_query_timeout`
/// (see [`crate::transport`]) is the backstop that reclassifies that reply when
/// it does arrive; the grace makes it the rare path rather than the common one.
/// Shared with the action client (`action_client.rs`).
pub(crate) const ZENOH_QUERY_TIMEOUT_GRACE: Duration = Duration::from_millis(250);
/// Declare-time timeout for the cached readiness `Querier`. A `Querier` has no
/// per-`get` timeout (only this declare-time value), so set it effectively
/// infinite — it is used only for `matching_status` (a local-table read), never
/// for `get()`, so it never closes a reply stream.
const QUERIER_DECLARE_TIMEOUT: Duration = Duration::from_secs(86_400);
const RPC_ENFORCE_SINGLE_PROVIDER_ENV: &str = "AORTA_RPC_ENFORCE_SINGLE_PROVIDER";
const RPC_MULTI_REPLY_CHECK_WINDOW_ENV: &str = "AORTA_RPC_MULTI_REPLY_CHECK_WINDOW_MS";
/// Opt-in flag that re-enables automatic RPC retries. Default is OFF: an RPC
/// service may be non-idempotent (e.g. step counters, ID allocation, gripper
/// actions), and silently re-sending the same request would invoke the
/// provider 2+ times for a single logical `call()` — a production data-
/// corruption hazard the caller cannot observe. Set to a truthy value only
/// when the caller can prove the target service is idempotent-safe.
const RPC_ALLOW_RETRY_ENV: &str = "AORTA_RPC_ALLOW_RETRY";

fn env_flag_enabled(key: &str) -> bool {
    runtime_env::var(key)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on" | "enable" | "enabled"
            )
        })
        .unwrap_or(false)
}

fn multi_reply_check_window_from_env() -> Duration {
    let ms = runtime_env::var(RPC_MULTI_REPLY_CHECK_WINDOW_ENV)
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_MULTI_REPLY_CHECK_WINDOW_MS);
    Duration::from_millis(ms)
}

fn validate_expected_root_type(
    service_name: &str,
    direction: &str,
    expected: Option<&str>,
    actual: Option<&str>,
) -> bool {
    let Some(expected) = expected else {
        return true;
    };

    match actual {
        Some(found) if found == expected => true,
        Some(found) => {
            aorta_core_warn!(
                "Aorta WARN: service '{}' {} root mismatch: expected '{}', got '{}'",
                service_name,
                direction,
                expected,
                found
            );
            false
        }
        None => {
            aorta_core_warn!(
                "Aorta WARN: service '{}' {} root missing: expected '{}'",
                service_name,
                direction,
                expected
            );
            false
        }
    }
}

/// Runtime client diagnostics counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientStats {
    pub call_count: u64,
    pub success_count: u64,
    pub timeout_count: u64,
    pub concurrent_conflict_count: u64,
    pub error_count: u64,
    pub retry_count: u64,
    pub last_call_started_ns: u64,
    pub last_call_finished_ns: u64,
    pub last_call_latency_ns: u64,
}

#[derive(Debug, Default)]
struct ClientStatsInner {
    call_count: AtomicU64,
    success_count: AtomicU64,
    timeout_count: AtomicU64,
    concurrent_conflict_count: AtomicU64,
    error_count: AtomicU64,
    retry_count: AtomicU64,
    last_call_started_ns: AtomicU64,
    last_call_finished_ns: AtomicU64,
    last_call_latency_ns: AtomicU64,
}

impl ClientStatsInner {
    fn snapshot(&self) -> ClientStats {
        ClientStats {
            call_count: self.call_count.load(Ordering::Relaxed),
            success_count: self.success_count.load(Ordering::Relaxed),
            timeout_count: self.timeout_count.load(Ordering::Relaxed),
            concurrent_conflict_count: self.concurrent_conflict_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            retry_count: self.retry_count.load(Ordering::Relaxed),
            last_call_started_ns: self.last_call_started_ns.load(Ordering::Relaxed),
            last_call_finished_ns: self.last_call_finished_ns.load(Ordering::Relaxed),
            last_call_latency_ns: self.last_call_latency_ns.load(Ordering::Relaxed),
        }
    }

    fn on_call_started(&self, started_ns: u64) {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.last_call_started_ns
            .store(started_ns, Ordering::Relaxed);
    }

    fn on_call_finished(&self, result: &AortaResult<ZBytes>, elapsed: Duration, retries: u64) {
        self.retry_count.fetch_add(retries, Ordering::Relaxed);
        self.last_call_finished_ns
            .store(unix_time_ns(), Ordering::Relaxed);
        let latency_ns = duration_to_u64_nanos(elapsed);
        self.last_call_latency_ns
            .store(latency_ns, Ordering::Relaxed);
        // Tracing moved to `call_with_timeout_internal`: the same monotonic
        // anchor now feeds the per-service waterfall slice AND the aggregate
        // histogram (one site, both tiers).

        match result {
            Ok(_) => {
                self.success_count.fetch_add(1, Ordering::Relaxed);
            }
            Err(AortaError::Timeout(_)) => {
                self.timeout_count.fetch_add(1, Ordering::Relaxed);
            }
            Err(AortaError::ConcurrentConflict(_)) => {
                self.concurrent_conflict_count
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.error_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn duration_to_u64_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Client for calling remote services
pub struct Client {
    session: Session,
    key_builder: KeyExprBuilder,
    service_name: String,
    timeout: Duration,
    service_schema: Option<SchemaInfo>,
    request_root_type: Option<String>,
    response_root_type: Option<String>,
    schema_registry: Option<SchemaRegistry>,
    validate_schema: bool,
    schema_attach_policy: SchemaAttachPolicy,
    enforce_single_provider: bool,
    multi_reply_check_window: Duration,
    allow_retry: bool,
    /// Lazily-declared, Client-lifetime cached `Querier` used only for the
    /// readiness gate (`matching_status`/`wait_for_matching`). The data path
    /// still uses `session.get()`. Undeclared on drop with the Client.
    querier_cell: TokioOnceCell<Querier<'static>>,
    schema_validation_checked: AtomicBool,
    response_attachment_checked: AtomicBool,
    /// Interned per-service waterfall event (`service.client_call:<service>`),
    /// minted once at build (bounded by client count).
    stage_event_client_call: u16,
    stats: Arc<ClientStatsInner>,
    inventory_guard: TelemetryResourceGuard,
    runtime_guard: RuntimeTelemetryGuard,
}

impl Client {
    /// Create a new Client builder
    pub fn builder(session: Session, service_name: &str) -> ClientBuilder {
        ClientBuilder::new(session, service_name)
    }

    /// Get the service name this client connects to
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// Get current runtime diagnostics counters.
    pub fn stats(&self) -> ClientStats {
        self.stats.snapshot()
    }

    /// Lazily declare (once) and return the cached `Querier` for this client's
    /// service key, used only for the readiness gate. Declared with the same
    /// `target=All` / `consolidation=None` semantics as the `session.get()`
    /// data path, plus a large declare-time timeout ([`QUERIER_DECLARE_TIMEOUT`]).
    async fn querier(&self) -> AortaResult<&Querier<'static>> {
        self.querier_cell
            .get_or_try_init(|| async {
                let key_expr = self.key_builder.service_key_client(&self.service_name)?;
                let key_expr_str: String = key_expr.into();
                self.session
                    .inner()
                    .declare_querier(key_expr_str)
                    .target(QueryTarget::All)
                    .consolidation(ConsolidationMode::None)
                    .timeout(QUERIER_DECLARE_TIMEOUT)
                    .await
                    .map_err(AortaError::from)
            })
            .await
    }

    /// One-shot readiness check: is there at least one matching service provider
    /// (queryable) for this client's service key right now? Reads local routing
    /// tables (no network round-trip); resolved via `.await`.
    pub async fn matching_status(&self) -> AortaResult<bool> {
        let querier = self.querier().await?;
        let status = querier.matching_status().await.map_err(AortaError::from)?;
        Ok(status.matching())
    }

    /// Wait (bounded by `timeout`) until a matching service provider appears.
    /// Returns `Ok(true)` once visible, `Ok(false)` on timeout (not an error).
    /// Gate the first `call()` on a fresh client with this to avoid the
    /// publish-before-route-known race on the query leg.
    pub async fn wait_for_matching(&self, timeout: Duration) -> AortaResult<bool> {
        const POLL_INTERVAL: Duration = Duration::from_millis(20);
        let start = Instant::now();
        loop {
            if self.matching_status().await? {
                return Ok(true);
            }
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Ok(false);
            }
            tokio::time::sleep((timeout - elapsed).min(POLL_INTERVAL)).await;
        }
    }

    /// Build the query attachment: the schema hash (per policy) plus the
    /// traceparent when this call is traced. Schema-only stays the legacy
    /// bare-hex form; only a traced call promotes to the composite format
    /// (mixed-version fleet safety — see `crate::attachment`).
    fn request_attachment(
        &self,
        link: Option<&crate::trace::SpanLink>,
        deadline_budget: Option<std::time::Duration>,
    ) -> AortaResult<Option<Vec<u8>>> {
        let schema_hash = match self.schema_attach_policy {
            SchemaAttachPolicy::Always => Some(self.service_schema.ok_or_else(|| {
                AortaError::invalid_argument(
                    "service schema hash is required when rpc schema attach policy is always",
                )
            })?),
            SchemaAttachPolicy::Never | SchemaAttachPolicy::Inherit => None,
        };
        let traceparent = link.map(|l| crate::trace::TraceParent {
            trace_id: l.trace_id,
            span_id: l.span_id,
            flags: crate::trace::TRACEPARENT_SAMPLED,
        });
        // stamp the RELATIVE remaining budget (the same value that
        // bounds the transport timeout) unless the kill-switch disables it.
        let deadline_budget_ns = deadline_budget
            .filter(|_| rpc_deadline_stamping_enabled())
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
        Ok(crate::attachment::encode(
            &crate::attachment::AttachmentParts {
                schema_hash,
                traceparent,
                reply_busy_ns: None,
                deadline_budget_ns,
            },
        ))
    }

    fn maybe_warn_response_attachment_once(&self, sample: &zenoh::sample::Sample) {
        if !matches!(self.schema_attach_policy, SchemaAttachPolicy::Always) {
            return;
        }
        if self
            .response_attachment_checked
            .swap(true, Ordering::Relaxed)
        {
            return;
        }

        let Some(expected) = self.service_schema else {
            return;
        };
        let actual = sample.attachment().and_then(|attachment| {
            let bytes = attachment.to_bytes();
            crate::attachment::decode(bytes.as_ref()).schema_hash
        });

        match actual {
            Some(found) if found == expected => {}
            Some(found) => {
                aorta_core_warn!(
                    "Aorta WARN: service '{}' response schema attachment mismatch: expected {}, got {}",
                    self.service_name,
                    expected.to_hex(),
                    found.to_hex()
                );
            }
            None => {
                aorta_core_warn!(
                    "Aorta WARN: service '{}' response missing schema attachment under always policy",
                    self.service_name
                );
            }
        }
    }

    async fn maybe_validate_schema_once(&self) {
        if self.validate_schema && !self.schema_validation_checked.swap(true, Ordering::Relaxed) {
            if let Some(schema_registry) = &self.schema_registry {
                match schema_registry
                    .fetch_schema_by_key_service(&self.service_name)
                    .await
                {
                    Ok(Some(service_schema)) => {
                        if let Some(expected) = &self.service_schema {
                            if &service_schema.service_hash != expected {
                                aorta_core_warn!(
                                    "Aorta WARN: service schema mismatch for service '{}': expected {}, got {}",
                                    self.service_name,
                                    expected.to_hex(),
                                    service_schema.service_hash.to_hex()
                                );
                            }
                        }
                        let request_root_matched = validate_expected_root_type(
                            &self.service_name,
                            "request",
                            self.request_root_type.as_deref(),
                            service_schema.request_root_type.as_deref(),
                        );
                        let response_root_matched = validate_expected_root_type(
                            &self.service_name,
                            "response",
                            self.response_root_type.as_deref(),
                            service_schema.response_root_type.as_deref(),
                        );
                        if !request_root_matched || !response_root_matched {
                            aorta_core_warn!(
                                "Aorta WARN: service '{}' schema root validation detected mismatch",
                                self.service_name
                            );
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        aorta_core_warn!(
                            "Aorta WARN: schema_by_key validation query failed for service '{}': {}",
                            self.service_name,
                            err
                        );
                    }
                }
            }
        }
    }

    pub async fn call<IntoZBytes>(&self, request: IntoZBytes) -> AortaResult<ZBytes>
    where
        IntoZBytes: Into<ZBytes>,
    {
        self.maybe_validate_schema_once().await;
        self.call_with_timeout_internal(request.into(), self.timeout)
            .await
    }

    /// Call the service inside a Flow [`Scope`](crate::flow::Scope) (Flow
    /// Concepts §6.1 `call_in`): the request inherits the scope's cancel token and
    /// budget — the effective timeout is `min(scope remaining, client
    /// timeout)` (narrow, never widen) and a scope cancellation resolves
    /// the call with `Stopped` immediately. Cooperative: the router-side
    /// query is not force-aborted (the cancel does not cross the hop); the
    /// narrowed timeout bounds it.
    ///
    /// Speaks the Flow completion vocabulary: scope cancel →
    /// `Stopped::cancelled(reason)`; the narrowed deadline expiring →
    /// `Stopped::cancelled(DeadlineExceeded)`; any transport/service
    /// failure → `Stopped::fault(message)` (the `recv_in` precedent:
    /// `Fault` covers infrastructure failure, not only panics). The
    /// non-Flow [`call`](Self::call) keeps `AortaResult` untouched.
    pub async fn call_in<IntoZBytes>(
        &self,
        scope: &crate::flow::Scope,
        request: IntoZBytes,
    ) -> crate::flow::Outcome<ZBytes>
    where
        IntoZBytes: Into<ZBytes>,
    {
        scope.checkpoint()?;
        // the effective deadline = min(scope budget, ambient cap
        // installed by scope.timeout, client timeout) — the narrowed
        // window reaches the WIRE (dual-guard), not just the local race.
        // Plain tokio callers have no ambient (cap None): plain timeout verbatim.
        let mut timeout_duration = match scope.remaining() {
            Some(remaining) => remaining.min(self.timeout),
            None => self.timeout,
        };
        if let Some(cap) = crate::flow::ambient::budget_cap() {
            // NOTE: read before maybe_validate_schema_once(); the wire
            // clock starts after validation, so a slow FIRST call's
            // validation latency is not charged against the cap
            // (acceptable: validation is one-shot and local).
            let cap_remaining = cap.saturating_duration_since(std::time::Instant::now());
            timeout_duration = timeout_duration.min(cap_remaining);
        }
        self.maybe_validate_schema_once().await;
        self.call_with_timeout_scoped(request.into(), timeout_duration, Some(scope))
            .await
            .map_err(|err| match err {
                crate::error::AortaError::Cancelled(_) => crate::flow::Stopped::cancelled(
                    scope
                        .cancel_reason()
                        .unwrap_or(crate::flow::CancelReason::Explicit),
                ),
                crate::error::AortaError::Timeout(_) => {
                    crate::flow::Stopped::cancelled(crate::flow::CancelReason::DeadlineExceeded)
                }
                other => crate::flow::Stopped::fault_from_error(&other),
            })
    }

    /// Call the service with a timeout override
    pub async fn call_with_timeout<IntoZBytes>(
        &self,
        request: IntoZBytes,
        timeout_ms: u64,
    ) -> AortaResult<ZBytes>
    where
        IntoZBytes: Into<ZBytes>,
    {
        self.maybe_validate_schema_once().await;
        let timeout_duration = Duration::from_millis(timeout_ms);
        self.call_with_timeout_internal(request.into(), timeout_duration)
            .await
    }

    async fn call_with_timeout_internal(
        &self,
        request: ZBytes,
        timeout_duration: Duration,
    ) -> AortaResult<ZBytes> {
        self.call_with_timeout_scoped(request, timeout_duration, None)
            .await
    }

    /// Shared call path; with a scope, cancellation races the in-flight
    /// query INSIDE the stats/trace bracket, so `on_call_finished` and the
    /// client-call span record on the cancel branch too (the earlier
    /// outer-select form dropped the branch mid-flight and lost them).
    async fn call_with_timeout_scoped(
        &self,
        request: ZBytes,
        timeout_duration: Duration,
        cancel_scope: Option<&crate::flow::Scope>,
    ) -> AortaResult<ZBytes> {
        let started_ns = unix_time_ns();
        self.stats.on_call_started(started_ns);
        let started_at = Instant::now();
        // Tracing: monotonic call anchor, captured only when enabled. Feeds
        // BOTH tiers below (per-service waterfall slice + aggregate
        // histogram); recorded for every outcome, success or timeout/error,
        // matching the stats semantics.
        let call_start_mono = crate::trace::trace_enabled().then(crate::time::monotonic_ns);
        // Pre-mint the client_call span identity so the query attachment can
        // carry it as the server's parent (cross-process propagation); the
        // span itself is recorded below, after the call, under the SAME id.
        let link = call_start_mono.and_then(|_| crate::trace::outbound_link());
        let mut retries = 0u64;
        let call =
            self.call_with_timeout_impl(request, timeout_duration, &mut retries, link.as_ref());
        let result = match cancel_scope {
            None => call.await,
            Some(scope) => {
                // the cancel race uses the EFFECTIVE (shield-aware)
                // view — a shielded cleanup RPC runs to completion even on
                // a cancelled scope (the narrowed timeout still
                // bounds it).
                tokio::select! {
                    reason = (crate::flow::ambient::EffectiveCancelled {
                        cancelled: scope.token().cancelled(),
                    }) => Err(AortaError::cancelled(format!(
                        "rpc call cancelled by scope: {reason:?}"
                    ))),
                    result = call => result,
                }
            }
        };
        self.stats
            .on_call_finished(&result, started_at.elapsed(), retries);
        if let Some(start) = call_start_mono {
            match link {
                Some(l) => crate::trace::record_stage_elapsed_linked(
                    crate::trace::StageId::ServiceClientCall,
                    start,
                    self.stage_event_client_call,
                    l,
                ),
                None => crate::trace::record_stage_elapsed_as(
                    crate::trace::StageId::ServiceClientCall,
                    start,
                    self.stage_event_client_call,
                ),
            }
        }
        result
    }

    async fn call_with_timeout_impl(
        &self,
        request: ZBytes,
        timeout_duration: Duration,
        retries: &mut u64,
        link: Option<&crate::trace::SpanLink>,
    ) -> AortaResult<ZBytes> {
        // `Client` is the query-side; wildcards in `service_name`
        // (e.g. `sensors/*`) are legitimate fan-out patterns. Use the
        // client-side validator so those patterns reach Zenoh instead
        // of being rejected before the query is sent.
        let key_expr = self.key_builder.service_key_client(&self.service_name)?;
        let key_expr_str: String = key_expr.into();
        let start = Instant::now();
        let mut backoff = Duration::from_millis(50);
        loop {
            let elapsed = start.elapsed();
            if elapsed >= timeout_duration {
                return Err(AortaError::timeout(format!(
                    "service '{}' call timed out after {:?}",
                    self.service_name, timeout_duration
                )));
            }

            let remaining = timeout_duration - elapsed;
            // per-attempt attachment — the stamped budget is THIS
            // attempt's remaining window (a retry stamps the shrunken value).
            let request_attachment = self.request_attachment(link, Some(remaining))?;
            let request_attachment_ref = request_attachment.as_deref();

            let get_builder = self
                .session
                .inner()
                .get(&key_expr_str)
                .target(QueryTarget::All)
                .payload(request.clone())
                // Dual-guard: bound the router-side query lifetime to
                // ~the call deadline so an abandoned call does not leak a pending
                // query for `queries_default_timeout` (600s on robots) on every
                // hop. `remaining + grace` keeps the tokio guard below first; the
                // `reply_error_is_zenoh_query_timeout` arm is the backstop if the
                // router's synthesized timeout still wins. See
                // ZENOH_QUERY_TIMEOUT_GRACE.
                .timeout(remaining + ZENOH_QUERY_TIMEOUT_GRACE)
                // Always disable consolidation for RPC so the client can promptly
                // observe individual provider replies (strict mode needs this,
                // and non-strict mode should return on the first reply).
                .consolidation(ConsolidationMode::None);
            // Per-attempt anchor for the network estimate: captured only
            // when this call injected a traceparent (a plain call reads no
            // extra clock).
            let attempt_start_mono = link.is_some().then(crate::time::monotonic_ns);
            let replies = match request_attachment_ref {
                Some(attachment) => get_builder.attachment(attachment).await?,
                None => get_builder.await?,
            };

            match timeout(remaining, replies.recv_async()).await {
                Ok(Ok(reply)) => {
                    // Measure at FIRST-reply arrival, before the strict-mode
                    // multi-reply window can inflate the estimate.
                    let attempt_elapsed_ns = attempt_start_mono
                        .map(|start| crate::time::monotonic_ns().saturating_sub(start));
                    if self.enforce_single_provider {
                        let check_window = remaining.min(self.multi_reply_check_window);
                        if !check_window.is_zero() {
                            let strict_deadline = Instant::now() + check_window;
                            // Listen ONCE for an extra reply within the check
                            // window; an extra reply means another provider
                            // also answered. The previous form was a `loop` in
                            // anticipation of draining multiple extras, but
                            // both branches break unconditionally so it never
                            // iterates (clippy::never_loop). One extra reply
                            // is sufficient to flag the violation; collapsing
                            // to an `if` makes the actual semantics explicit.
                            // If we ever want to drain ALL extras, restructure
                            // this and remove the early-return on first hit.
                            let now = Instant::now();
                            if now < strict_deadline {
                                let wait_for = strict_deadline - now;
                                if let Ok(Ok(extra_reply)) =
                                    timeout(wait_for, replies.recv_async()).await
                                {
                                    // Our own `.timeout(remaining + grace)` can
                                    // synthesize an `Err("Timeout")` reply that
                                    // lands inside this window when the check
                                    // window is configured larger than the grace
                                    // (e.g. AORTA_RPC_MULTI_REPLY_CHECK_WINDOW_MS=
                                    // 500) and the first reply arrived late. That
                                    // is NOT a second provider — only a genuine
                                    // extra reply is a strict-mode violation.
                                    let is_synthesized_timeout = matches!(
                                        extra_reply.result(),
                                        Err(err) if reply_error_is_zenoh_query_timeout(err)
                                    );
                                    if !is_synthesized_timeout {
                                        return Err(AortaError::concurrent_conflict(format!(
                                            "service '{}' returned multiple replies under strict single-provider mode; \
deployment must enforce single active provider per {{namespace,group,service}}",
                                            self.service_name
                                        )));
                                    }
                                }
                            }
                        }
                    }
                    match reply.result() {
                        Ok(sample) => {
                            self.maybe_warn_response_attachment_once(sample);
                            // Reply context: a traced server echoes its busy
                            // duration; `attempt elapsed − busy` estimates the
                            // network round trip (both MONOTONIC durations —
                            // no cross-host clock subtraction). Old servers
                            // echo nothing → no sample.
                            if let (Some(elapsed), Some(attachment)) =
                                (attempt_elapsed_ns, sample.attachment())
                            {
                                if let Some(busy_ns) =
                                    crate::attachment::decode(attachment.to_bytes().as_ref())
                                        .reply_busy_ns
                                {
                                    crate::trace::record_stage(
                                        crate::trace::StageId::ServiceNetworkEstimate,
                                        elapsed.saturating_sub(busy_ns),
                                    );
                                }
                            }
                            return Ok(sample.payload().clone());
                        }
                        // Backstop for the dual-guard race: the get now sets an
                        // explicit `.timeout(remaining + grace)`, so the router's
                        // synthesized timeout reply normally arrives AFTER the
                        // tokio guard has already fired. If scheduler jitter lets
                        // it win anyway, reclassify it as an attempt timeout (fall
                        // through) instead of misreporting it as a provider fault.
                        Err(err) if reply_error_is_zenoh_query_timeout(err) => {}
                        Err(err) => {
                            return Err(AortaError::internal(format!(
                                "service '{}' returned error: {}",
                                self.service_name,
                                reply_error_text(err)
                            )));
                        }
                    }
                }
                Ok(Err(_)) => {}
                Err(_) => {}
            }

            // Default (fail-safe): do NOT auto-retry. Re-sending the request
            // would silently re-invoke a possibly non-idempotent provider for
            // a single logical `call()`. Treat a closed reply channel
            // (`Ok(Err)`) or an attempt timeout as terminal; only the outer
            // deadline produces a Timeout. Retries are opt-in via
            // `ClientBuilder::allow_retry` (caller asserts idempotent safety).
            if !self.allow_retry {
                return Err(AortaError::timeout(format!(
                    "service '{}' call timed out after {:?}",
                    self.service_name, timeout_duration
                )));
            }

            *retries = retries.saturating_add(1);
            tokio::time::sleep(backoff).await;
            if backoff < Duration::from_millis(200) {
                backoff += Duration::from_millis(25);
            }
        }
    }
}

/// Builder for Client
pub struct ClientBuilder {
    session: Session,
    key_builder: KeyExprBuilder,
    service_name: String,
    timeout: Duration,
    service_schema: Option<SchemaInfo>,
    request_root_type: Option<String>,
    response_root_type: Option<String>,
    schema_registry: Option<SchemaRegistry>,
    validate_schema: bool,
    schema_attach_policy: SchemaAttachPolicy,
    default_schema_attach_policy: SchemaAttachPolicy,
    enforce_single_provider: bool,
    multi_reply_check_window: Duration,
    allow_retry: bool,
    inventory_telemetry: Option<Arc<VitalSnapshotTelemetryPublisher>>,
    runtime_telemetry: Option<Arc<RuntimeMetricsTelemetryPublisher>>,
}

impl ClientBuilder {
    fn new(session: Session, service_name: &str) -> Self {
        let key_builder = key_builder_from_group_env();
        Self {
            session,
            key_builder,
            service_name: service_name.to_string(),
            timeout: Duration::from_millis(DEFAULT_CALL_TIMEOUT_MS),
            service_schema: None,
            request_root_type: None,
            response_root_type: None,
            schema_registry: None,
            validate_schema: false,
            schema_attach_policy: SchemaAttachPolicy::Inherit,
            default_schema_attach_policy: SchemaAttachPolicy::from_env(),
            enforce_single_provider: env_flag_enabled(RPC_ENFORCE_SINGLE_PROVIDER_ENV),
            multi_reply_check_window: multi_reply_check_window_from_env(),
            allow_retry: env_flag_enabled(RPC_ALLOW_RETRY_ENV),
            inventory_telemetry: None,
            runtime_telemetry: None,
        }
    }

    pub fn key_builder(mut self, key_builder: KeyExprBuilder) -> Self {
        self.key_builder = key_builder;
        self
    }

    pub fn default_schema_attach_policy(mut self, policy: SchemaAttachPolicy) -> Self {
        self.default_schema_attach_policy = policy;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn timeout_ms(mut self, ms: u64) -> Self {
        self.timeout = Duration::from_millis(ms);
        self
    }

    pub fn service_schema(mut self, schema: SchemaInfo) -> Self {
        self.service_schema = Some(schema);
        self
    }

    pub fn request_root_type(mut self, root_type: impl Into<String>) -> Self {
        self.request_root_type = Some(root_type.into());
        self
    }

    pub fn response_root_type(mut self, root_type: impl Into<String>) -> Self {
        self.response_root_type = Some(root_type.into());
        self
    }

    pub fn service_root_types(
        mut self,
        request_root_type: Option<String>,
        response_root_type: Option<String>,
    ) -> Self {
        self.request_root_type = request_root_type;
        self.response_root_type = response_root_type;
        self
    }

    pub fn schema_registry(mut self, registry: SchemaRegistry) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    pub fn validate_schema(mut self, validate: bool) -> Self {
        self.validate_schema = validate;
        self
    }

    pub fn schema_attach_policy(mut self, policy: SchemaAttachPolicy) -> Self {
        self.schema_attach_policy = policy;
        self
    }

    pub fn enforce_single_provider(mut self, enforce: bool) -> Self {
        self.enforce_single_provider = enforce;
        self
    }

    /// Opt in to automatic RPC retries on attempt timeout / closed reply
    /// channel. OFF by default because retries silently re-invoke the provider,
    /// which corrupts state for non-idempotent services. Enable ONLY when the
    /// target service is provably idempotent-safe.
    pub fn allow_retry(mut self, allow: bool) -> Self {
        self.allow_retry = allow;
        self
    }

    pub fn multi_reply_check_window(mut self, window: Duration) -> Self {
        self.multi_reply_check_window = window;
        self
    }

    pub fn multi_reply_check_window_ms(mut self, ms: u64) -> Self {
        self.multi_reply_check_window = Duration::from_millis(ms);
        self
    }

    pub fn inventory_telemetry(mut self, telemetry: Arc<VitalSnapshotTelemetryPublisher>) -> Self {
        self.inventory_telemetry = Some(telemetry);
        self
    }

    pub fn runtime_telemetry(mut self, telemetry: Arc<RuntimeMetricsTelemetryPublisher>) -> Self {
        self.runtime_telemetry = Some(telemetry);
        self
    }

    pub fn build(self) -> Client {
        let schema_attach_policy = self
            .schema_attach_policy
            .resolve(self.default_schema_attach_policy);
        let stats = Arc::new(ClientStatsInner::default());
        let inventory_guard = self
            .inventory_telemetry
            .as_ref()
            .map(|inventory| {
                inventory.register_client(
                    &self.key_builder,
                    &self.service_name,
                    self.service_schema,
                    self.request_root_type.clone(),
                    self.response_root_type.clone(),
                )
            })
            .transpose()
            .map_err(|err| {
                aorta_core_warn!(
                    "Aorta WARN: client inventory telemetry disabled for '{}': {}",
                    self.service_name,
                    err
                );
                err
            })
            .ok()
            .flatten()
            .unwrap_or_else(TelemetryResourceGuard::disabled);
        let runtime_guard = self
            .runtime_telemetry
            .as_ref()
            .map(|telemetry| {
                telemetry.register_client(
                    &self.service_name,
                    self.service_schema,
                    Arc::new({
                        let stats = Arc::clone(&stats);
                        move || stats.snapshot()
                    }),
                )
            })
            .transpose()
            .map_err(|err| {
                aorta_core_warn!(
                    "Aorta WARN: client runtime telemetry disabled for '{}': {}",
                    self.service_name,
                    err
                );
                err
            })
            .ok()
            .flatten()
            .unwrap_or_else(RuntimeTelemetryGuard::disabled);
        let stage_event_client_call = crate::trace::stage_event_for(
            crate::trace::StageId::ServiceClientCall,
            &self.service_name,
        );
        Client {
            session: self.session,
            key_builder: self.key_builder,
            service_name: self.service_name,
            timeout: self.timeout,
            service_schema: self.service_schema,
            request_root_type: self.request_root_type,
            response_root_type: self.response_root_type,
            schema_registry: self.schema_registry,
            validate_schema: self.validate_schema,
            schema_attach_policy,
            enforce_single_provider: self.enforce_single_provider,
            multi_reply_check_window: self.multi_reply_check_window,
            allow_retry: self.allow_retry,
            querier_cell: TokioOnceCell::new(),
            schema_validation_checked: AtomicBool::new(false),
            response_attachment_checked: AtomicBool::new(false),
            stage_event_client_call,
            stats,
            inventory_guard,
            runtime_guard,
        }
    }
}

#[cfg(test)]
#[path = "../test/client_tests.rs"]
mod client_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{EnvVarGuard, ENV_LOCK};

    #[test]
    fn test_default_timeout() {
        assert_eq!(DEFAULT_CALL_TIMEOUT_MS, 5000);
    }

    #[test]
    fn enforce_single_provider_env_flag_parsing() {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _unset = EnvVarGuard::remove(RPC_ENFORCE_SINGLE_PROVIDER_ENV);
        assert!(!env_flag_enabled(RPC_ENFORCE_SINGLE_PROVIDER_ENV));

        let _set_true = EnvVarGuard::set(RPC_ENFORCE_SINGLE_PROVIDER_ENV, "true");
        assert!(env_flag_enabled(RPC_ENFORCE_SINGLE_PROVIDER_ENV));
    }

    #[test]
    fn allow_retry_env_flag_parsing() {
        let _guard = ENV_LOCK.lock().expect("lock");
        // Default OFF: retries are opt-in because they can silently
        // re-invoke non-idempotent providers.
        let _unset = EnvVarGuard::remove(RPC_ALLOW_RETRY_ENV);
        assert!(!env_flag_enabled(RPC_ALLOW_RETRY_ENV));

        let _set_true = EnvVarGuard::set(RPC_ALLOW_RETRY_ENV, "true");
        assert!(env_flag_enabled(RPC_ALLOW_RETRY_ENV));
    }

    #[test]
    fn multi_reply_check_window_env_parsing() {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _unset = EnvVarGuard::remove(RPC_MULTI_REPLY_CHECK_WINDOW_ENV);
        assert_eq!(
            multi_reply_check_window_from_env(),
            Duration::from_millis(DEFAULT_MULTI_REPLY_CHECK_WINDOW_MS)
        );

        let _set_custom = EnvVarGuard::set(RPC_MULTI_REPLY_CHECK_WINDOW_ENV, "77");
        assert_eq!(
            multi_reply_check_window_from_env(),
            Duration::from_millis(77)
        );
    }

    #[test]
    fn validate_expected_root_type_behavior() {
        assert!(validate_expected_root_type(
            "svc",
            "request",
            None,
            Some("demo.req")
        ));
        assert!(validate_expected_root_type(
            "svc",
            "request",
            Some("demo.req"),
            Some("demo.req")
        ));
        assert!(!validate_expected_root_type(
            "svc",
            "request",
            Some("demo.req"),
            Some("demo.req.b")
        ));
        assert!(!validate_expected_root_type(
            "svc",
            "request",
            Some("demo.req"),
            None
        ));
    }
}

/// kill-switch: `AORTA_RPC_DEADLINE=0` stops the client stamping
/// wire budgets (the server then treats requests as un-stamped — no budget seeded,
/// nothing dropped). Cached once per process.
pub(crate) fn rpc_deadline_stamping_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        rpc_deadline_env_enabled(std::env::var("AORTA_RPC_DEADLINE").ok().as_deref())
    })
}

/// The pure predicate behind BOTH kill-switch caches
/// (process-global env caching makes the caches untestable — this isn't).
pub(crate) fn rpc_deadline_env_enabled(var: Option<&str>) -> bool {
    !matches!(var, Some("0") | Some("false") | Some("off"))
}

#[cfg(test)]
mod deadline_env_tests {
    #[test]
    fn kill_switch_predicate() {
        use super::rpc_deadline_env_enabled as p;
        assert!(p(None), "unset = enabled");
        assert!(p(Some("1")));
        assert!(p(Some("on")));
        assert!(!p(Some("0")));
        assert!(!p(Some("false")));
        assert!(!p(Some("off")));
    }
}
