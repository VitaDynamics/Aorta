//! The live half of the engine (MCAP Player Proposal §6.3, §6.4, §6.6, §6.7,
//! §6.9).
//!
//! Startup order, and why it is that order:
//!
//! 1. **Queryable gate** — typed replay with the player's own schema
//!    queryables env-disabled fails closed *before any publisher exists*: its
//!    registrations could answer no by-key query and the §6.3 second scan
//!    would be unsatisfiable. Raw-only replay proceeds (nothing to discover).
//! 2. **`AORTA_GROUP` ownership** — installed before Node construction,
//!    because the Node reads the variable while constructing ([`crate::group`]).
//! 3. **Node** — under *managed ownership*: the construction future runs on a
//!    spawned task that retains ownership to completion, so a cancelled
//!    construction is completed-then-closed (or detached at the teardown
//!    deadline), never a dropped future that leaves an unclosable session.
//! 4. **First schema scan** (typed topics, concurrent, per-topic deadline).
//! 5. **Publisher map** — one publisher per planned topic, also managed.
//! 6. **Second schema scan** — the player's registrations must now own every
//!    typed key; divergence is a hard error. Skipped after cancellation, whose
//!    already-emitted schema puts are a documented, un-rolled-back residual.
//! 7. **Matching barrier** — `wait_for_matching` per topic, token-wrapped,
//!    unmatched topics reported and replayed anyway.
//! 8. **Pass 2** — [`crate::replay`].
//! 9. **Teardown** — reader thread, publishers, `Node::close()`, all under one
//!    absolute deadline; expiry is a distinct non-success termination and
//!    taints the process.

use crate::group::{self, GroupGuard};
use crate::plan::{PlannedTopic, PlaybackPlan, TopicFilter, TopicKind};
use crate::preflight::{self, ScanOutcome, ScanPhase};
use crate::reader::ReaderLimits;
use crate::replay::{self, DriveOptions, ReaderThread, ReplayEnd, ReplayInputs, TopicPublisher};
use crate::snapshot::Snapshot;
use crate::timeline::{Scheduler, SystemClock, DEFAULT_LATE_THRESHOLD};
use crate::validate::ValidatedLimits;
use crate::{PlayerConfig, ProgressHandle, RunSummary, Termination};
use anyhow::{anyhow, bail, Result};
use aorta_core::error::{AortaError, AortaResult};
use aorta_core::node::Node;
use aorta_core::publisher::{Publisher, PublisherOptions};
use aorta_core::schema::SchemaAttachPolicy;
use aorta_core::SchemaRegistry;
use aorta_core::Session;
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// One absolute deadline for the whole teardown (MCAP Player Proposal §6.7):
/// `Node::close()` is not internally bounded, so the player bounds it from the
/// outside.
pub(crate) const TEARDOWN_DEADLINE: Duration = Duration::from_secs(10);

// Test-only fault hooks for the managed construction and teardown contracts.
#[cfg(feature = "fault-injection")]
pub(crate) const TEST_STALL_PUBLISHER_ENV: &str = "AORTA_PLAY_TEST_STALL_PUBLISHER";
#[cfg(feature = "fault-injection")]
pub(crate) const TEST_PUBLISHER_DELAY_MS_ENV: &str = "AORTA_PLAY_TEST_PUBLISHER_DELAY_MS";
#[cfg(feature = "fault-injection")]
pub(crate) const TEST_STALL_NODE_OPEN_ENV: &str = "AORTA_PLAY_TEST_STALL_NODE_OPEN";
#[cfg(feature = "fault-injection")]
pub(crate) const TEST_STALL_NODE_CLOSE_ENV: &str = "AORTA_PLAY_TEST_STALL_NODE_CLOSE";
#[cfg(feature = "fault-injection")]
pub(crate) const TEST_TEARDOWN_DEADLINE_MS_ENV: &str = "AORTA_PLAY_TEST_TEARDOWN_DEADLINE_MS";
#[cfg(feature = "fault-injection")]
pub(crate) const TEST_CLOSE_BEFORE_REPLAY_ENV: &str = "AORTA_PLAY_TEST_CLOSE_BEFORE_REPLAY";
#[cfg(feature = "fault-injection")]
pub(crate) const TEST_NODE_ERROR_AFTER_CANCEL_ENV: &str = "AORTA_PLAY_TEST_NODE_ERROR_AFTER_CANCEL";
#[cfg(feature = "fault-injection")]
pub(crate) const TEST_PUBLISHER_ERROR_AFTER_CANCEL_ENV: &str =
    "AORTA_PLAY_TEST_PUBLISHER_ERROR_AFTER_CANCEL";
#[cfg(feature = "fault-injection")]
pub(crate) const TEST_NODE_CLOSE_ERROR_ENV: &str = "AORTA_PLAY_TEST_NODE_CLOSE_ERROR";
#[cfg(feature = "fault-injection")]
pub(crate) static TEST_NODE_ERROR_ARMED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(feature = "fault-injection")]
pub(crate) static TEST_PUBLISHER_ERROR_ARMED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(feature = "fault-injection")]
pub(crate) static TEST_NODE_OPEN_STALL_ARMED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn teardown_deadline() -> Duration {
    #[cfg(feature = "fault-injection")]
    {
        std::env::var(TEST_TEARDOWN_DEADLINE_MS_ENV)
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(TEARDOWN_DEADLINE)
    }
    #[cfg(not(feature = "fault-injection"))]
    {
        TEARDOWN_DEADLINE
    }
}

/// Everything the run acquired that teardown has to give back.
#[derive(Default)]
struct Resources {
    node: Option<Node>,
    publishers: Vec<TopicPublisher>,
    reader: Option<ReaderThread>,
    /// Construction tasks still running when cancellation landed. They retain
    /// ownership of whatever they produce, so teardown can close it properly.
    pending: Vec<(String, PendingConstruction)>,
}

enum PendingConstruction {
    Node(JoinHandle<aorta_core::error::AortaResult<Node>>),
    Publisher(JoinHandle<aorta_core::error::AortaResult<Publisher>>),
}

/// What the live body reached before it handed over to teardown.
enum BodyEnd {
    Completed,
    Cancelled,
}

fn apply_player_client_mode(
    mut config: zenoh::Config,
    client_mode_override: bool,
) -> AortaResult<zenoh::Config> {
    if client_mode_override {
        Session::apply_mode_override(&mut config, "client")?;
    }
    Ok(config)
}

async fn build_player_zenoh_config(
    path: Option<&Path>,
    client_mode_override: bool,
) -> AortaResult<zenoh::Config> {
    let config = if let Some(path) = path {
        zenoh::Config::from_file(path).map_err(|err| {
            AortaError::internal(format!(
                "failed to load zenoh config '{}': {err}",
                path.display()
            ))
        })?
    } else {
        Session::config_from_env_or_local_default().await?
    };
    apply_player_client_mode(config, client_mode_override)
}

/// Run the live half. Returns the [`Termination`] (teardown timeout and publish
/// errors already folded in per MCAP Player Proposal §6.6 precedence) or a hard
/// error; either way the summary carries the counters and the teardown report.
pub(crate) async fn run_live(
    plan: PlaybackPlan,
    snapshots: Vec<Snapshot>,
    config: &PlayerConfig,
    limits: &ValidatedLimits,
    shutdown: &CancellationToken,
    summary: &mut RunSummary,
    progress: &ProgressHandle,
) -> Result<Termination> {
    let typed_topics: Vec<&PlannedTopic> = plan
        .topics
        .iter()
        .filter(|topic| matches!(topic.kind, TopicKind::Typed { .. }))
        .collect();

    // 1. Queryable gate (MCAP Player Proposal §6.3) — before the Node, before
    //    any publisher.
    if !typed_topics.is_empty() && !SchemaRegistry::queryable_enabled_from_env() {
        bail!(
            "AORTA_SCHEMA_QUERYABLE disables this process's schema queryables, but {} of the {} \
             planned topics replay typed messages: the player's registrations could answer no \
             by-key query, subscribers could not resolve schemas, and the post-registration \
             verification scan would be unsatisfiable — typed replay fails closed (MCAP Player Proposal §6.3). \
             Enable the queryables, or restrict the replay to raw topics.",
            typed_topics.len(),
            plan.topics.len()
        );
    }

    // 2. `AORTA_GROUP` ownership (MCAP Player Proposal §6.9).
    let group = group::effective_group(config.group.as_deref(), plan.recorded_group.as_deref())?;
    tracing::info!(
        group = %group,
        topics = plan.topics.len(),
        typed = typed_topics.len(),
        "aorta-play: entering the live engine"
    );
    let guard = GroupGuard::acquire(&group);

    let mut resources = Resources::default();
    let outcome = live_body(
        &plan,
        snapshots,
        guard.group(),
        config,
        limits,
        shutdown,
        summary,
        progress,
        &mut resources,
    )
    .await;

    // 9. Teardown always runs, on every return path.
    let teardown_result = teardown(resources, &mut summary.teardown).await;
    if teardown_result.completed {
        guard.restore();
    } else {
        guard.abandon();
    }

    match outcome {
        Ok(BodyEnd::Completed) => match teardown_result.hard_error {
            Some(err) => Err(err),
            None => Ok(termination_of(false, summary)),
        },
        Ok(BodyEnd::Cancelled) => match teardown_result.hard_error {
            Some(err) => Err(err),
            None => Ok(termination_of(true, summary)),
        },
        Err(err) => Err(err),
    }
}

/// MCAP Player Proposal §6.6 precedence: `Err` > `Interrupted` >
/// `CompletedWithTeardownTimeout` > `CompletedWithPublishErrors` > `Completed`
/// (`Err` is handled by the caller).
fn termination_of(interrupted: bool, summary: &RunSummary) -> Termination {
    if interrupted {
        Termination::Interrupted
    } else if summary.teardown.deadline_expired {
        Termination::CompletedWithTeardownTimeout
    } else if summary.publish_errors > 0 {
        Termination::CompletedWithPublishErrors
    } else {
        Termination::Completed
    }
}

#[allow(clippy::too_many_arguments)]
async fn live_body(
    plan: &PlaybackPlan,
    snapshots: Vec<Snapshot>,
    group: &str,
    config: &PlayerConfig,
    limits: &ValidatedLimits,
    shutdown: &CancellationToken,
    summary: &mut RunSummary,
    progress: &ProgressHandle,
    resources: &mut Resources,
) -> Result<BodyEnd> {
    // 3. Node — managed ownership (MCAP Player Proposal §6.7).
    let node_name = format!("aorta_play_{}", std::process::id());
    let started = Instant::now();
    let node = {
        let name = node_name.clone();
        // `Node` ignores this argument and reads `AORTA_GROUP` (MCAP Player
        // Proposal §6.9, `node.rs:184`); it is passed for the startup log's
        // benefit and to keep the call honest if core ever takes it seriously.
        let group = group.to_string();
        let zenoh_config = config.zenoh_config.clone();
        let client_mode_override = config.client_mode_override;
        #[cfg(feature = "fault-injection")]
        let cancellation = shutdown.clone();
        let construct = async move {
            #[cfg(feature = "fault-injection")]
            if std::env::var(TEST_NODE_ERROR_AFTER_CANCEL_ENV)
                .ok()
                .as_deref()
                == Some("1")
            {
                TEST_NODE_ERROR_ARMED.store(true, std::sync::atomic::Ordering::SeqCst);
                cancellation.cancelled().await;
                return Err(aorta_core::error::AortaError::internal(
                    "test node construction hard error after cancellation",
                ));
            }
            #[cfg(feature = "fault-injection")]
            if std::env::var(TEST_STALL_NODE_OPEN_ENV).ok().as_deref() == Some("1") {
                eprintln!("AORTA_PLAY_TEST_NODE_OPEN_STALLED=1");
                TEST_NODE_OPEN_STALL_ARMED.store(true, std::sync::atomic::Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
            let zenoh_config =
                build_player_zenoh_config(zenoh_config.as_deref(), client_mode_override).await?;
            Node::with_config(name, group, zenoh_config).await
        };
        match managed(construct, shutdown).await {
            Managed::Done(Ok(node)) => node,
            Managed::Done(Err(err)) => {
                return Err(anyhow!("failed to create the player node: {err}"))
            }
            Managed::Cancelled(handle) => {
                resources.pending.push((
                    "node_construction".to_string(),
                    PendingConstruction::Node(handle),
                ));
                tracing::warn!("aorta-play: cancelled during node construction");
                return Ok(BodyEnd::Cancelled);
            }
            Managed::Panicked(join) => {
                return Err(anyhow!("the node construction task panicked: {join}"))
            }
        }
    };
    tracing::info!(
        node = %node_name,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "aorta-play: node ready"
    );
    resources.node = Some(node.clone());

    // 4. First schema scan (MCAP Player Proposal §6.3).
    let preflight_timeout = limits.preflight_timeout;
    let first_scan = Instant::now();
    if scan_all(&node, plan, preflight_timeout, ScanPhase::First, shutdown).await? {
        return Ok(BodyEnd::Cancelled);
    }
    let first_scan_ms = first_scan.elapsed().as_millis() as u64;

    // 5. Publisher map (MCAP Player Proposal §6.3) — managed, all before the
    //    barrier.
    let publisher_batch = Instant::now();
    let mut tasks: Vec<(
        String,
        JoinHandle<aorta_core::error::AortaResult<Publisher>>,
    )> = Vec::with_capacity(plan.topics.len());
    for topic in &plan.topics {
        let node = node.clone();
        let suffix = topic.topic_suffix.clone();
        let options = publisher_options(topic);
        #[cfg(feature = "fault-injection")]
        let cancellation = shutdown.clone();
        tasks.push((
            topic.key.clone(),
            tokio::spawn(async move {
                #[cfg(feature = "fault-injection")]
                if std::env::var(TEST_PUBLISHER_ERROR_AFTER_CANCEL_ENV)
                    .ok()
                    .as_deref()
                    == Some("1")
                {
                    TEST_PUBLISHER_ERROR_ARMED.store(true, std::sync::atomic::Ordering::SeqCst);
                    cancellation.cancelled().await;
                    return Err(aorta_core::error::AortaError::internal(
                        "test publisher construction hard error after cancellation",
                    ));
                }
                let result = node.create_publisher_with_options(&suffix, options).await;
                #[cfg(feature = "fault-injection")]
                if std::env::var(TEST_STALL_PUBLISHER_ENV).ok().as_deref() == Some("1") {
                    eprintln!("AORTA_PLAY_TEST_PUBLISHER_CONSTRUCTION_STALLED=1");
                    std::future::pending::<()>().await;
                }
                #[cfg(feature = "fault-injection")]
                if let Some(delay) = std::env::var(TEST_PUBLISHER_DELAY_MS_ENV)
                    .ok()
                    .and_then(|raw| raw.parse::<u64>().ok())
                {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                result
            }),
        ));
    }
    // A cancelled batch keeps its already-emitted schema puts (MCAP Player
    // Proposal §6.7): core has no compensating delete, so they are listed as a
    // residual.
    let typed_count = plan
        .topics
        .iter()
        .filter(|topic| matches!(topic.kind, TopicKind::Typed { .. }))
        .count() as u64;
    let mut cancelled = false;
    let mut failure: Option<anyhow::Error> = None;
    for (key, mut handle) in tasks {
        // Once the batch is unwinding, the remaining tasks keep ownership of
        // whatever they produce and teardown closes it — dropping their handles
        // instead would detach live publishers behind the player's back.
        if cancelled || failure.is_some() {
            resources
                .pending
                .push((key, PendingConstruction::Publisher(handle)));
            continue;
        }
        let joined = tokio::select! {
            biased;
            _ = shutdown.cancelled() => None,
            joined = &mut handle => Some(joined),
        };
        match joined {
            Some(Ok(Ok(publisher))) => resources.publishers.push(TopicPublisher { key, publisher }),
            Some(Ok(Err(err))) => {
                failure = Some(anyhow!("failed to create a publisher for '{key}': {err}"))
            }
            Some(Err(join)) => {
                failure = Some(anyhow!("publisher creation for '{key}' panicked: {join}"))
            }
            None => {
                cancelled = true;
                resources
                    .pending
                    .push((key, PendingConstruction::Publisher(handle)));
            }
        }
    }
    if let Some(err) = failure {
        return Err(err);
    }
    let publisher_batch_ms = publisher_batch.elapsed().as_millis() as u64;
    if cancelled {
        if typed_count > 0 {
            summary
                .teardown
                .residual
                .insert("schema_puts_not_rolled_back".to_string(), typed_count);
        }
        tracing::warn!("aorta-play: cancelled during publisher creation");
        return Ok(BodyEnd::Cancelled);
    }

    // 6. Second scan: the player's own registrations must now own every typed
    // key (MCAP Player Proposal §6.3). Skipped after cancellation — see above.
    let second_scan = Instant::now();
    if scan_all(&node, plan, preflight_timeout, ScanPhase::Second, shutdown).await? {
        if typed_count > 0 {
            summary
                .teardown
                .residual
                .insert("schema_puts_not_rolled_back".to_string(), typed_count);
        }
        return Ok(BodyEnd::Cancelled);
    }
    let second_scan_ms = second_scan.elapsed().as_millis() as u64;

    // 7. Matching barrier (MCAP Player Proposal §6.4).
    let matching = Instant::now();
    let barrier = match_barrier(&resources.publishers, limits.match_timeout, shutdown).await;
    let matching_ms = matching.elapsed().as_millis() as u64;
    tracing::info!(
        first_scan_ms,
        publisher_batch_ms,
        second_scan_ms,
        matching_ms,
        "aorta-play: startup phases complete (MCAP Player Proposal §6.3 T + P + T + M)"
    );
    let unmatched = match barrier {
        BarrierOutcome::Cancelled => {
            if typed_count > 0 {
                summary
                    .teardown
                    .residual
                    .insert("schema_puts_not_rolled_back".to_string(), typed_count);
            }
            tracing::warn!("aorta-play: cancelled at the matching barrier");
            return Ok(BodyEnd::Cancelled);
        }
        BarrierOutcome::Done(unmatched) => unmatched,
    };
    if !unmatched.is_empty() {
        tracing::warn!(
            "aorta-play: {} topic(s) have no matched subscriber after {:?} — replaying anyway \
             (MCAP Player Proposal §6.4): {}",
            unmatched.len(),
            limits.match_timeout,
            unmatched.join(", ")
        );
    }
    summary.unmatched_topics = unmatched;

    // 8. Pass 2 (MCAP Player Proposal §6.5, §6.7).
    #[cfg(feature = "fault-injection")]
    if std::env::var(TEST_CLOSE_BEFORE_REPLAY_ENV).ok().as_deref() == Some("1") {
        // Close the transport after startup so the next synchronous publish
        // reports an error while teardown can still be independently stalled.
        let _ = node.close().await;
    }
    let Some(t0) = plan.timeline.t0 else {
        // `is_empty_selection()` already handled this upstream; belt and braces.
        return Ok(BodyEnd::Completed);
    };
    let span_ns = plan.timeline.t_last.unwrap_or(t0).saturating_sub(t0).min(
        plan.timeline
            .window
            .and_then(|w| w.end)
            .unwrap_or(u64::MAX)
            .saturating_sub(t0),
    );
    let inputs = ReplayInputs::from_plan(
        plan,
        snapshots,
        TopicFilter::new(config.topics.clone(), config.exclude.clone()),
        limits.start,
        limits.duration,
        ReaderLimits {
            max_chunk_bytes: limits.max_chunk_bytes,
            max_message_bytes: limits.max_message_bytes,
        },
        config.loop_playback,
    );
    let mut reader = ReaderThread::spawn(inputs, limits.buffer_bytes)?;
    let clock = SystemClock;
    // `wall0` is read *after* the barrier (MCAP Player Proposal §6.5).
    let scheduler = Scheduler::start(t0, limits.rate, DEFAULT_LATE_THRESHOLD, &clock);
    progress.begin(limits.rate, span_ns);

    let end = replay::drive(
        &mut reader,
        DriveOptions {
            publishers: &resources.publishers,
            scheduler,
            clock: &clock,
            token: shutdown,
            progress,
            span_ns,
        },
        summary,
    )
    .await;
    reader.request_stop();
    resources.reader = Some(reader);
    progress.finish();

    match end? {
        ReplayEnd::Finished => Ok(BodyEnd::Completed),
        ReplayEnd::Cancelled => {
            tracing::warn!("aorta-play: cancelled during pass-2 replay");
            Ok(BodyEnd::Cancelled)
        }
    }
}

/// Per-publisher schema attachment is always `Never` (MCAP Player Proposal
/// §6.3, §6.11): the player replays verbatim recorded bytes and must not add
/// per-message attachments, and a raw publisher has no schema an inherited
/// `Always` could attach.
fn publisher_options(topic: &PlannedTopic) -> PublisherOptions {
    let mut options = PublisherOptions::new();
    options.schema_attach_policy = SchemaAttachPolicy::Never;
    options.schema_bfbs = match &topic.kind {
        TopicKind::Typed { bfbs, .. } => Some(bfbs.clone()),
        TopicKind::Raw => None,
    };
    options
}

// ---------------------------------------------------------------------------
// Managed ownership (MCAP Player Proposal §6.7)
// ---------------------------------------------------------------------------

enum Managed<T> {
    Done(T),
    /// The token fired first; the task keeps running and retains ownership.
    Cancelled(JoinHandle<T>),
    Panicked(tokio::task::JoinError),
}

/// Run `future` on a spawned task that retains ownership of whatever it
/// produces, and select the task against the shutdown token. Dropping a
/// construction future is *not* a rollback (there is no resource to close yet,
/// and a publisher future may already have emitted schema puts), so the player
/// never drops one.
async fn managed<F, T>(future: F, token: &CancellationToken) -> Managed<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let mut handle = tokio::spawn(future);
    tokio::select! {
        biased;
        _ = token.cancelled() => Managed::Cancelled(handle),
        joined = &mut handle => match joined {
            Ok(value) => Managed::Done(value),
            // A panicked construction task has nothing to hand over.
            Err(join) => Managed::Panicked(join),
        },
    }
}

// ---------------------------------------------------------------------------
// Preflight batches (MCAP Player Proposal §6.3)
// ---------------------------------------------------------------------------

/// Scan every typed topic concurrently. Returns `true` when the token fired.
async fn scan_all(
    node: &Node,
    plan: &PlaybackPlan,
    timeout: Duration,
    phase: ScanPhase,
    token: &CancellationToken,
) -> Result<bool> {
    let typed: Vec<(String, String, String)> = plan
        .topics
        .iter()
        .filter_map(|topic| match &topic.kind {
            TopicKind::Typed { schema_hash, .. } => Some((
                topic.key.clone(),
                topic.key_suffix.clone(),
                schema_hash.clone(),
            )),
            TopicKind::Raw => None,
        })
        .collect();
    if typed.is_empty() {
        return Ok(false);
    }

    // A child token lets the first failure unwind the sibling scans instead of
    // waiting out their deadlines.
    let scan_token = token.child_token();
    let mut handles = Vec::with_capacity(typed.len());
    for (key, key_suffix, hash) in typed {
        let session = node.session().inner().clone();
        let key_builder = node.key_builder().clone();
        let scan_token = scan_token.clone();
        handles.push(tokio::spawn(async move {
            let outcome = preflight::scan_topic(
                &session,
                &key_builder,
                &key,
                &key_suffix,
                &hash,
                timeout,
                phase,
                &scan_token,
            )
            .await;
            (key, outcome)
        }));
    }

    let mut failure: Option<anyhow::Error> = None;
    let mut cancelled = false;
    for handle in handles {
        match handle.await {
            Ok((key, Ok(ScanOutcome::Unclaimed))) => {
                if phase == ScanPhase::Second {
                    scan_token.cancel();
                    failure.get_or_insert_with(|| {
                        anyhow!(
                            "post-registration schema verification failed for topic '{}': no \
                             schema_by_key responder answered, so the player's own registration \
                             is not being served — the by-key slot diverged during startup \
                             (MCAP Player Proposal §6.3)",
                            key
                        )
                    });
                }
            }
            Ok((_, Ok(ScanOutcome::Claimed))) => {}
            Ok((_, Ok(ScanOutcome::Cancelled))) => cancelled = true,
            Ok((_, Err(err))) => {
                scan_token.cancel();
                failure.get_or_insert(err);
            }
            Err(join) => {
                scan_token.cancel();
                failure.get_or_insert_with(|| anyhow!("schema preflight task panicked: {join}"));
            }
        }
    }
    if let Some(err) = failure {
        return Err(err);
    }
    Ok(cancelled || token.is_cancelled())
}

// ---------------------------------------------------------------------------
// Matching barrier (MCAP Player Proposal §6.4)
// ---------------------------------------------------------------------------

enum BarrierOutcome {
    /// Topics with no matched subscriber at the deadline.
    Done(Vec<String>),
    Cancelled,
}

async fn match_barrier(
    publishers: &[TopicPublisher],
    timeout: Duration,
    token: &CancellationToken,
) -> BarrierOutcome {
    if timeout.is_zero() || publishers.is_empty() {
        return BarrierOutcome::Done(Vec::new());
    }
    let mut handles = Vec::with_capacity(publishers.len());
    for topic in publishers {
        let key = topic.key.clone();
        let publisher = topic.publisher.clone();
        let token = token.clone();
        handles.push(tokio::spawn(async move {
            // `wait_for_matching` carries no cancellation token of its own
            // (MCAP Player Proposal §6.4), so it is wrapped in a select here.
            let matched = tokio::select! {
                biased;
                _ = token.cancelled() => None,
                matched = publisher.wait_for_matching(timeout) => Some(matched),
            };
            (key, matched)
        }));
    }

    let mut unmatched = Vec::new();
    let mut cancelled = false;
    for handle in handles {
        match handle.await {
            Ok((_, Some(Ok(true)))) => {}
            Ok((key, Some(Ok(false)))) => unmatched.push(key),
            Ok((key, Some(Err(err)))) => {
                tracing::warn!(
                    "aorta-play: matching check failed for '{}': {} — replaying anyway",
                    key,
                    err
                );
                unmatched.push(key);
            }
            Ok((_, None)) => cancelled = true,
            Err(join) => tracing::warn!("aorta-play: matching task panicked: {join}"),
        }
    }
    if cancelled {
        BarrierOutcome::Cancelled
    } else {
        unmatched.sort();
        BarrierOutcome::Done(unmatched)
    }
}

// ---------------------------------------------------------------------------
// Bounded teardown (MCAP Player Proposal §6.7)
// ---------------------------------------------------------------------------

struct TeardownResult {
    completed: bool,
    hard_error: Option<anyhow::Error>,
}

/// Teardown every owned resource inside one absolute deadline. Every child wait
/// receives only the remaining time, and terminal construction failures are
/// retained until the caller applies the MCAP Player Proposal §6.6 error
/// precedence.
async fn teardown(resources: Resources, report: &mut crate::TeardownReport) -> TeardownResult {
    let deadline = tokio::time::Instant::now() + teardown_deadline();
    let Resources {
        node,
        publishers,
        reader,
        pending,
    } = resources;
    let mut node = node;
    let mut publishers = publishers;
    let mut reader = reader;
    let mut pending = pending;
    let mut hard_error = None;

    if let Some(reader_handle) = reader.take() {
        if let Some(remaining) = teardown_remaining(deadline) {
            if !reader_handle.stop_and_join(remaining).await {
                tracing::warn!(
                    "aorta-play: the pass-2 reader thread did not stop within the teardown \
                     deadline — detaching it (it dies with the process, MCAP Player Proposal §6.7)"
                );
                mark_teardown_expired(
                    report,
                    "reader_thread",
                    1,
                    pending.len(),
                    publishers.len(),
                    node.is_some(),
                );
                return TeardownResult {
                    completed: false,
                    hard_error,
                };
            }
        } else {
            mark_teardown_expired(
                report,
                "reader_thread",
                1,
                pending.len(),
                publishers.len(),
                node.is_some(),
            );
            return TeardownResult {
                completed: false,
                hard_error,
            };
        }
    }

    // Cancelled construction tasks retain ownership: complete-then-close while
    // there is time, otherwise detach and report the exact remaining count.
    while let Some((label, construction)) = pending.pop() {
        let Some(remaining) = teardown_remaining(deadline) else {
            mark_teardown_expired(
                report,
                "pending_construction",
                0,
                pending.len() + 1,
                publishers.len(),
                node.is_some(),
            );
            return TeardownResult {
                completed: false,
                hard_error,
            };
        };
        match construction {
            PendingConstruction::Node(handle) => {
                match tokio::time::timeout(remaining, handle).await {
                    Ok(Ok(Ok(late_node))) => {
                        let Some(remaining) = teardown_remaining(deadline) else {
                            mark_teardown_expired(
                                report,
                                "pending_construction",
                                0,
                                pending.len(),
                                publishers.len(),
                                true,
                            );
                            return TeardownResult {
                                completed: false,
                                hard_error,
                            };
                        };
                        let close = async move {
                            #[cfg(feature = "fault-injection")]
                            if std::env::var(TEST_STALL_NODE_CLOSE_ENV).ok().as_deref() == Some("1")
                            {
                                eprintln!("AORTA_PLAY_TEST_NODE_CLOSE_STALLED=1");
                                std::future::pending::<()>().await;
                            }
                            #[cfg(feature = "fault-injection")]
                            if std::env::var(TEST_NODE_CLOSE_ERROR_ENV).ok().as_deref() == Some("1")
                            {
                                return Err(aorta_core::error::AortaError::internal(
                                    "test node close error",
                                ));
                            }
                            late_node.close().await
                        };
                        match tokio::time::timeout(remaining, close).await {
                            Ok(Ok(())) => {}
                            Ok(Err(err)) => {
                                mark_teardown_error(report, "node_close", &err);
                                if hard_error.is_none() {
                                    hard_error = Some(anyhow!("node close failed: {err}"));
                                }
                            }
                            Err(_) => {
                                mark_teardown_expired(
                                    report,
                                    "node_close",
                                    0,
                                    pending.len(),
                                    publishers.len(),
                                    true,
                                );
                                return TeardownResult {
                                    completed: false,
                                    hard_error,
                                };
                            }
                        }
                    }
                    Ok(Ok(Err(err))) => {
                        hard_error = Some(anyhow!(
                            "the cancelled node construction ('{label}') failed: {err}"
                        ));
                    }
                    Ok(Err(join)) => {
                        hard_error = Some(anyhow!(
                            "the cancelled node construction ('{label}') panicked: {join}"
                        ));
                    }
                    Err(_) => {
                        mark_teardown_expired(
                            report,
                            "pending_construction",
                            0,
                            pending.len() + 1,
                            publishers.len(),
                            node.is_some(),
                        );
                        return TeardownResult {
                            completed: false,
                            hard_error,
                        };
                    }
                }
            }
            PendingConstruction::Publisher(handle) => {
                match tokio::time::timeout(remaining, handle).await {
                    Ok(Ok(Ok(publisher))) => drop(publisher),
                    Ok(Ok(Err(err))) => {
                        hard_error = Some(anyhow!(
                            "the cancelled publisher construction ('{label}') failed: {err}"
                        ));
                    }
                    Ok(Err(join)) => {
                        hard_error = Some(anyhow!(
                            "the cancelled publisher construction ('{label}') panicked: {join}"
                        ));
                    }
                    Err(_) => {
                        mark_teardown_expired(
                            report,
                            "pending_construction",
                            0,
                            pending.len() + 1,
                            publishers.len(),
                            node.is_some(),
                        );
                        return TeardownResult {
                            completed: false,
                            hard_error,
                        };
                    }
                }
            }
        }
    }

    if teardown_remaining(deadline).is_none() {
        mark_teardown_expired(report, "publishers", 0, 0, publishers.len(), node.is_some());
        return TeardownResult {
            completed: false,
            hard_error,
        };
    }
    publishers.clear();

    if let Some(node_handle) = node.take() {
        let Some(remaining) = teardown_remaining(deadline) else {
            mark_teardown_expired(report, "node_close", 0, 0, 0, true);
            return TeardownResult {
                completed: false,
                hard_error,
            };
        };
        let close = async move {
            #[cfg(feature = "fault-injection")]
            if std::env::var(TEST_STALL_NODE_CLOSE_ENV).ok().as_deref() == Some("1") {
                eprintln!("AORTA_PLAY_TEST_NODE_CLOSE_STALLED=1");
                std::future::pending::<()>().await;
            }
            #[cfg(feature = "fault-injection")]
            if std::env::var(TEST_NODE_CLOSE_ERROR_ENV).ok().as_deref() == Some("1") {
                return Err(aorta_core::error::AortaError::internal(
                    "test node close error",
                ));
            }
            node_handle.close().await
        };
        match tokio::time::timeout(remaining, close).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                mark_teardown_error(report, "node_close", &err);
                hard_error = Some(anyhow!("node close failed: {err}"));
            }
            Err(_) => {
                mark_teardown_expired(report, "node_close", 0, 0, 0, true);
                return TeardownResult {
                    completed: false,
                    hard_error,
                };
            }
        }
    }
    TeardownResult {
        completed: !report.deadline_expired,
        hard_error,
    }
}

fn teardown_remaining(deadline: tokio::time::Instant) -> Option<Duration> {
    let now = tokio::time::Instant::now();
    (now < deadline).then(|| deadline - now)
}

fn mark_teardown_expired(
    report: &mut crate::TeardownReport,
    stage: &str,
    reader_threads: usize,
    pending_tasks: usize,
    publishers: usize,
    nodes: bool,
) {
    report.deadline_expired = true;
    report.stuck_stage = Some(stage.to_string());
    *report
        .residual
        .entry("stages_not_completed".to_string())
        .or_insert(0) += 1;
    for (name, count) in [
        ("detached_reader_threads", reader_threads as u64),
        ("pending_construction_tasks", pending_tasks as u64),
        ("detached_publishers", publishers as u64),
        ("detached_node_resources", u64::from(nodes)),
    ] {
        if count > 0 {
            *report.residual.entry(name.to_string()).or_insert(0) += count;
        }
    }
    tracing::error!(
        "aorta-play: teardown exceeded its absolute deadline at stage '{}' — the process is \
         Aorta-tainted and must exit (MCAP Player Proposal §6.7)",
        stage
    );
}

fn mark_teardown_error(
    report: &mut crate::TeardownReport,
    stage: &str,
    error: &dyn std::fmt::Display,
) {
    report.stuck_stage = Some(stage.to_string());
    report.error = Some(format!("{stage}: {error}"));
    *report
        .residual
        .entry("teardown_errors".to_string())
        .or_insert(0) += 1;
    tracing::error!(
        "aorta-play: teardown stage '{}' returned an error: {}",
        stage,
        error
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SkippedChannels, TruncationReport};

    fn summary_with(publish_errors: u64, teardown_expired: bool) -> RunSummary {
        RunSummary {
            publish_errors,
            teardown: crate::TeardownReport {
                deadline_expired: teardown_expired,
                stuck_stage: teardown_expired.then(|| "node_close".to_string()),
                ..crate::TeardownReport::default()
            },
            skipped_channels: SkippedChannels::default(),
            truncation: TruncationReport::default(),
            ..RunSummary::default()
        }
    }

    #[test]
    fn termination_precedence_is_a_total_order() {
        // MCAP Player Proposal §6.6: Err > Interrupted >
        // CompletedWithTeardownTimeout > CompletedWithPublishErrors >
        // Completed.
        assert_eq!(
            termination_of(false, &summary_with(0, false)),
            Termination::Completed
        );
        assert_eq!(
            termination_of(false, &summary_with(3, false)),
            Termination::CompletedWithPublishErrors
        );
        assert_eq!(
            termination_of(false, &summary_with(0, true)),
            Termination::CompletedWithTeardownTimeout
        );
        assert_eq!(
            termination_of(false, &summary_with(3, true)),
            Termination::CompletedWithTeardownTimeout,
            "a teardown timeout outranks publish errors, which stay in the summary"
        );
        assert_eq!(
            termination_of(true, &summary_with(3, true)),
            Termination::Interrupted
        );
    }

    #[test]
    fn player_client_mode_override_changes_only_the_default_path() {
        let peer = zenoh::Config::from_json5(r#"{ mode: "peer" }"#).expect("peer config");
        let client = apply_player_client_mode(peer.clone(), true).expect("client override");
        assert!(client
            .get_json("mode")
            .expect("client mode")
            .contains("client"));

        let preserved = apply_player_client_mode(peer, false).expect("preserve mode");
        assert!(preserved
            .get_json("mode")
            .expect("preserved mode")
            .contains("peer"));
    }

    #[test]
    fn the_combined_case_keeps_the_publish_error_count_in_the_summary() {
        let summary = summary_with(7, true);
        assert_eq!(
            termination_of(false, &summary),
            Termination::CompletedWithTeardownTimeout
        );
        assert_eq!(summary.publish_errors, 7);
        assert_eq!(summary.teardown.stuck_stage.as_deref(), Some("node_close"));
    }

    #[test]
    fn a_typed_publisher_carries_its_bfbs_and_never_attaches() {
        let topic = PlannedTopic {
            key: "aorta/robot/pub/sensor/imu".to_string(),
            key_suffix: "pub/sensor/imu".to_string(),
            topic_suffix: "sensor/imu".to_string(),
            kind: TopicKind::Typed {
                schema_hash: "0".repeat(64),
                schema_name: "Imu".to_string(),
                bfbs: b"bfbs-imu".to_vec(),
            },
            messages: 3,
        };
        let options = publisher_options(&topic);
        assert_eq!(options.schema_attach_policy, SchemaAttachPolicy::Never);
        assert_eq!(options.schema_bfbs.as_deref(), Some(b"bfbs-imu".as_slice()));

        let raw = PlannedTopic {
            kind: TopicKind::Raw,
            ..topic
        };
        let options = publisher_options(&raw);
        assert_eq!(options.schema_attach_policy, SchemaAttachPolicy::Never);
        assert!(
            options.schema_bfbs.is_none(),
            "a raw publisher registers nothing (MCAP Player Proposal §6.3)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_empty_teardown_completes_and_reports_nothing() {
        let mut report = crate::TeardownReport::default();
        assert!(teardown(Resources::default(), &mut report).await.completed);
        assert!(!report.deadline_expired);
        assert!(report.stuck_stage.is_none());
    }
}
