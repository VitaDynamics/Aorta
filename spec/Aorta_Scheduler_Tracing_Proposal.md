# Aorta Scheduler-Layer Tracing — Proposal (self-exoneration & root-cause)

Status: DRAFT v3 (2026-06-13) — revised after TWO adversarial reviews (a first
pass → v2, then a second pass grounded in the code → v3; §9 records what each
folded in). Extends the existing tracing design, reusing its StageId + scope
two-tier model. No wire-format or public-API break is proposed.

## 1. Motivation

When an upstream application misuses Aorta — an ill-designed callback group, a
blocking call inside a callback, a self-deadlock, a slow handler that backs up
the queue — the user's *first* suspicion is "the middleware's comms/scheduling
is broken." Today we cannot quickly **(a) exonerate the middleware** or
**(b) point at the user's root cause**, because the scheduling layer (executor
dispatch, callback-group serialization, queue occupancy) is not on the trace
timeline.

### Principle: a timestamped responsibility seam

A dispatched message's life is segments with distinct owners:

```
network ─▶ [zenoh] ─▶ [Aorta scheduler: enqueue → queue_wait → group_wait → callback-start] ─▶ [user callback] ─▶ return
           (§2)        (ours)                                                                    (user's)
```

Timestamp each handoff and every microsecond is attributable. "Self-
exoneration" is this decomposition, produced on demand — **on the dispatch
paths we instrument (§4.1), with the explicit gaps in §2/§4.8 stated, not
papered over.**

## 2. Non-goals / scope boundary (what this does NOT cover)

- **Zenoh internals** (L0 tx/rx, routing) — black box. We timestamp the seam
  where we hand to / are re-entered by zenoh, not what is inside.
- **Action execute is NOT on the scheduler dispatch path**. The
  action handler runs in its OWN `tokio::spawn` (`action_server.rs:spawn_goal_handler`;
  FFI: `ffi/c_api/mod.rs:spawn_goal_handler` + `spawn_blocking`), bypassing the
  executor `JoinSet` / `EventQueue` / callback-group lock / `AdmissionStats`.
  The shared dispatch helper (§4.1) therefore does NOT cover action execute.
  v1 keeps the existing `ActionUserCallback` span for it; a dedicated
  action-handler in-flight view is a later item (§4.8) — do not imply the
  scheduler layer root-causes actions.
- **Per-span free-form attributes do not exist in the trace record**. `RingRecord` carries only ids, timestamps, flags, and a single `value`
  (`trace/chunk.rs`). So "group / priority / op-name queryable ON the span" is
  not achievable as-is. v1 encodes such facts in the **event NAME suffix**
  (`<stage>:<label>`, the converter already splits on `:`) and in **separate
  instant/counter events** + the live **dump** — NOT as bit-packed span fields.
  See §4.2/§4.3.
- No change to the default-tier hot-path cost beyond the topic-span precedent:
  histograms master-gated, spans `aorta.sched`-gated OFF by default.

## 3. What already exists (reuse, do not rebuild)

- Stage spans `service.queue_wait`/`user_callback`, `action.user_callback`,
  `topic.callback`; `service.network_estimate`. The subscriber callback is
  already wrapped (`subscriber.rs:run_traced_callback` → `topic.callback`);
  service has `ServiceUserCallback`. So a generic scheduler `callback` span is
  REDUNDANT for subscribers/services — v1 emits `SchedCallback` only for the
  EventKinds that have none (timers; see §4.0).
- `Event` already carries `enqueued_at: std::time::Instant`
  (`event_queue.rs`, set in `Event::new*`) + `queue_wait_ns_at(now)`. No new
  field needed. CAVEATS: (a) it is `std::time::Instant`, a DIFFERENT epoch from
  the span/histogram `crate::time::monotonic_ns()` — compute the delta in
  `Instant` math, then place it; in `executor.rs` a bare `Instant::now()` binds
  `tokio::time::Instant`, so qualify `std::time::Instant`. (b) The stamp is at
  Event CONSTRUCTION (in the zenoh/timer callback), so it measures
  construction→dispatch and includes the ingress→executor handoff; admission
  (`EventQueueSender::send`) and the subscriber overflow-relay
  (`spawn_overflow_relay_queue_with_handle`) are LATER hops — see §4.1 on not
  blurring them.
- `thread_dur` capture exists (`AORTA_TRACE_THREAD_TIME`), but the explicit-id
  emit path `emit_completed_span_linked` hardcodes `thread_delta_ns: 0` and the
  RAII span (which CAN capture CPU) does not accept a preset `SpanLink`. v1 adds `emit_completed_span_linked_with_thread_delta` so the
  synchronous `SchedCallback` can carry wall-vs-cpu; the async wait spans do
  not capture CPU (they are await time, not run time).
- Runtime anomaly counters + `AdmissionStats` (queue depth/bytes + 4 drop
  classes). CAVEAT: admission depth does NOT count events that left the queue
  and are parked on a busy group mutex — it understates the real backlog, which
  is why §4.2 adds an explicit waiter counter.
- Reentrancy is already detected and funneled through `aorta_reentrant()`
  (`ffi/c_api/mod.rs`, the single funnel — confirmed). It captures only the
  FFI/C-ABI self-deadlock; a pure-Rust block is caught only by §4.4.

## 4. Design

### 4.0 New scope + StageIds

- New scope **`aorta.sched`**, spans at **Debug** (off at the default
  `always`/Info tier; enabled by `AORTA_TRACE=aorta.sched=standard` or global
  `standard`/`verbose`). Same machinery as `aorta.topic`.
- New StageIds (after the topic ones; STAGE_COUNT 18→22 — update
  `StageId::ALL`, `name()`, `from_u8()`, `STAGE_COUNT` together, 4 sites):
  - `SchedQueueWait` (18) — Event construction → dispatch start. **Async span**
    (`FLAG_ASYNC`): it spans the ingress→executor handoff and may cross worker
    threads, so it goes on an async track, NOT a thread lane.
  - `SchedGroupWait` (19) — `lock.lock().await` for a MutuallyExclusive group.
    Also an **async span** (`FLAG_ASYNC`): the await can resume on another
    worker. Zero/absent for Reentrant groups (no lock).
  - `SchedCallback` (20) — the synchronous `event.run()` for **Timer events
    only** (subscribers/services already have their own callback span). The one
    span that is genuinely a thread-lane sync interval; carries `thread_dur`.
  - `SchedRelayWait` (21) — an event's age when it leaves the subscriber
    overflow-relay queue (construction → relay-dequeue). **Async span**; emitted
    only on the overflow path (§4.1). [SHIPPED]

### 4.1 The shared dispatch helper + the paths it covers

A `dispatch_traced(resolved, run)` helper, called from every executor dispatch
site, brackets queue_wait + (optional) group_wait + the timer callback span.
Covered sites:

1. `MultiThreadedExecutor::spawn_event` (`executor.rs`) — JoinSet path.
2. `MultiThreadedExecutor::run_event_once` — `spin_once` path.
3. `SingleThreadedExecutor::dispatch_ready_batch` — runs `event.run()` with NO
   group lock (serial by construction): queue_wait + callback only, group_wait
   absent. (Documented limit: callback-group misuse there shows only as
   long-prior-callback + rising queue_wait, never group_wait.)
4. `Subscriber::spawn_local_queue` (no executor attached) — local-queue task
   runs `event.run()` directly: queue_wait + callback, no group, no admission.

NOT covered by the helper: action execute (§2), and the overflow-relay /
multi-ingress hops — but those are NOT silently merged into queue_wait.
**[SHIPPED]** The relay task records a distinct `SchedRelayWait` async span at
relay-DEQUEUE (`record_relay_wait` in the `spawn_overflow_relay_queue_with_handle`
loop): the event's age so far (construction → now, via the existing
`Event::queue_wait_ns_at`) is the pre-executor sojourn that `SchedQueueWait`
(construction → dispatch) would otherwise silently absorb. So a slow callback
backing the relay up is attributable, not mis-blamed on scheduler queue_wait. It is the relay's own root (off the dispatch chain); histogram
master-gated, span `aorta.sched`-gated; no new `Event` field or channel-type
change. NOTE: relay_wait ⊂ queue_wait (overlapping, not disjoint) — a consumer
subtracts to recover the executor-queue portion.

**Emission soundness:** the open-span stack is thread-local with a
same-thread contract; a JoinSet task parks at `.await` and may resume on a
different worker. So all sched spans use the EXPLICIT-id emit
(`emit_completed_span_linked` / the new `_with_thread_delta`), never RAII begin/
end across the `.await`. queue_wait/group_wait are `FLAG_ASYNC`; only the
synchronous `SchedCallback` (no `.await` inside `event.run()`) is a thread-lane
span and the only one that captures `thread_dur`.

### 4.2 Callback-group as a resource (contention, occupancy, inversion)

- The dispatch site resolves the group ONCE via a `resolve_for_event(event)
  -> ResolvedGroup { requested_handle, effective_handle, group_type, mutex,
  group_state, fallback }` — `lock_for_event` alone returns only the
  mutex, but the waiter counter, holder slot, fallback instant, and the group
  label all need the resolved entry.
- `SchedGroupWait:<group_label>` (group identity in the name suffix) brackets
  `mutex.lock().await`; emitted only when a mutex exists (MutuallyExclusive).
  `group_label` is the group's **NAME** when one was given via
  `create_callback_group_named(type, Some(name))` (Node/executor, additive — the
  C-ABI keeps the unnamed form), otherwise the numeric handle. The label also
  rides the per-callback `args.group` in the converter — making a multi-group
  deployment legible in Perfetto (e.g. `sched.group_wait:vision` vs
  `sched.group_wait:control`). **Zero per-tick cost:** the group's two event ids
  (`sched.group_wait:<label>` and `sched.callback:timer:<label>`) are interned
  ONCE at group creation (`group_trace_events`) and stored on the group entry, so
  dispatch carries only `u16`s — no per-dispatch `format!`/intern (a ~22 ns/tick
  saving on the sched-on path, measured by `trace_sched_dispatch_overhead`).
- **Timer callback label:** the timer callback is `sched.callback:timer:<task>` where
  `<task>` is the timer's NAME (`TimerOptions::with_name`) when given, else the
  group label, else bare. A named timer is pre-interned ONCE at registration
  (the `Event` carries the resulting `u16`), so labelling costs nothing per tick
  — it threads through every backend (executor-owned, Linux timerfd, the
  non-Linux high-precision thread, the Block path). So `sched.callback:timer:control_loop`
  identifies the task, not just its group.
- **Occupancy [SHIPPED]:** parked-on-mutex events have left the
  EventQueue, so `AdmissionStats` understates the backlog. A per-group **atomic
  waiter counter** (inc before `.lock().await`, dec after acquire) is surfaced as
  the COUNTER `sched.group_waiters:<group>`. It lives in a `GroupContention`
  struct shared (behind an `Arc`) by every dispatch of the group; the
  `DispatchGroup` clones that `Arc` ONLY when `aorta.sched` is enabled at resolve
  time (one relaxed atomic load otherwise), so the default path neither refcounts
  nor accounts. The waiter event id is pre-interned at group creation (zero
  per-tick format!/intern).
- **Priority inversion [SHIPPED]:** `Event` carries `EventPriority`. Acquisition
  takes a `try_lock()` fast path; on a CONTENDED acquire (a holder is in the
  guard right now) the waiter reads the holder's recorded priority and, if it
  outranks the holder (lower `EventPriority as u8` = higher priority, Realtime=0),
  emits a `sched.priority_inversion:<group>` INSTANT (a first-class signal — not a
  per-span attribute, which the record cannot hold). The holder's priority lives
  in the same `GroupContention` (`AtomicU8`, init 0 = highest so nothing is
  flagged until a real holder records itself); the comparison is O(1).
  BEST-EFFORT: a brief stale holder-priority read between release and the next
  acquire's store can only cost a missed/extra instant, never correctness.

### 4.3 Misuse instants (cheap, isolated)

- `sched.reentrant_rejected:<subsystem>` INSTANT at `aorta_reentrant()`
  **[SHIPPED]**. The label rides the **name suffix**; `aorta_reentrant()` gained
  an `op: &str` argument (reading `LAST_ERROR` fails because
  `aorta_reentrant` overwrites it with a generic message). Each call site passes
  `module_path!()`, whose last `::` segment names the FFI SUBSYSTEM the blocking
  call came from (`client`, `contextstore`, `action_server`, …) — auto-derived
  at every site, so no per-call-site string to maintain across the ~44 funnels.
  Direct evidence of a blocking middleware call inside a callback; rare error
  path, so the per-call format!/intern is irrelevant. (Per-function granularity
  was traded for per-subsystem to avoid hand-tagging 44 sites; revisit if a
  finer split is ever needed.)
- `sched.group_fallback:<requested_handle>` INSTANT at the unknown-handle
  fallback (`executor.rs:resolve_entry`, the warn site): a stale/unknown
  `callback_group_handle` silently serializes onto the DEFAULT group — surface
  it as a first-class artifact.

### 4.4 thread_dur blocked-vs-busy (with its limits)

On `SchedCallback` (timer, sync), `wall ≈ cpu` ⇒ compute-bound; `wall ≫ cpu
(cpu≈0)` ⇒ the callback is NOT computing — likely blocked on a lock/IO. The
cheapest deadlock hint and the only one catching pure-Rust self-deadlocks
(which never hit §4.3's FFI funnel). KNOWN FALSE-POSITIVE: a callback that
legitimately offloads/`.await`s work elsewhere also shows `wall≫cpu` while
returning quickly — so this is *suggestive, not conclusive*. Requires the new
`emit_completed_span_linked_with_thread_delta` (§3); without it `SchedCallback`
cannot carry CPU and this signal is unavailable.

### 4.5 Stuck-callback watchdog (on-demand first)

From the always-on holder slot + in-flight registry (§4.6): flag any callback
over a threshold (WARN + `sched.callback_stuck:<group>` instant). v1: surfaced
via the on-demand dump (§4.6), NOT an always-on timer thread (a new failure
surface on a busy RT SoC). A periodic watchdog is a later option.

### 4.6 On-demand scheduler dump (net-new always-on state)

A thread-dump for the scheduler: live, per executor — in-flight callbacks +
elapsed, per-queue depth (`AdmissionStats`), per-group occupancy + waiter
counts + the holder's `(event, priority, start, thread)`, AND for each in-flight
callback both the group it **holds** and the group it is **waiting on** (so
deadlock-cycle detection, §4.7, can be added later WITHOUT a data-model change). This is genuinely NEW always-on state (`JoinSet` exposes none of it);
it must be BOUNDED and maintained independent of the trace gate, so a field hang
is inspectable even with tracing OFF. Only span EMISSION is gated by
`aorta.sched`; the registry itself is always-on (§5).

### 4.7 Deadlock-cycle detection (later)

A true multi-group cycle (A holds G1 waits G2; B holds G2 waits G1) needs the
holds+waits map of §4.6 + a cycle check. Deferred; v1 ships the data (holds +
waits in the registry) so the check is a pure addition.

### 4.8 Action-handler in-flight view (later)

Since action execute bypasses the executor (§2), a separate small facility —
handler spawn/started/finished + an in-flight `(goal, elapsed, thread)` slot in
the action server — would give actions the same hang-naming. Out of v1 scope;
v1 leaves `ActionUserCallback` as the action signal.

## 5. Gating & hot-path cost

- `SchedQueueWait`/`SchedGroupWait` HISTOGRAMS record under the **master switch**
  (one `monotonic_ns()` at dispatch; the start reuses `Event::enqueued_at`). The
  per-event SPANS + the `sched.*` instants/counters gate under `aorta.sched`.
- `SchedCallback` (timer) HISTOGRAM also gates under `aorta.sched` (not the
  master) — `thread_dur` is a syscall (~0.6–1µs) and timers already have an
  aggregate elsewhere; pay it only when asked.
- The in-flight/holder REGISTRY (§4.6) is always-on but bounded and lock-light
  (per-group atomics + a small slot), independent of the trace gate.
- Default busy-executor cost: ~1 `monotonic_ns()` + a few atomics per dispatched
  event — the accepted topic-histogram order.

## 6. Diagnostic artifacts

1. **Responsibility decomposition**: `transit (ours+net) | queue_wait |
   group_wait | callback (wall vs cpu)`. Small transit/queue/group + callback
   cpu≈0 *strongly suggests* (not proves, §4.4) a blocked user callback.
2. **Group contention**: `SchedGroupWait:<group>` async spans +
   `sched.group_waiters:<group>` counter + `sched.priority_inversion:<group>`.
3. **Misuse**: `sched.reentrant_rejected:<op>` + `sched.group_fallback:<handle>`.
4. **Hang naming**: the live dump (§4.6) + `sched.callback_stuck:<group>`.

## 7. Phasing

- **Phase 1 [SHIPPED]**: §4.0 scope/StageIds + `emit_completed_span_linked_with_thread_delta`;
  the `dispatch_traced` helper + `resolve_for_dispatch` across paths #1–#4 with
  `SchedQueueWait`/`SchedGroupWait` (async) + the per-group waiter counter +
  `SchedCallback` (timer) with thread_dur; the `sched.reentrant_rejected` and
  `sched.group_fallback` + `sched.priority_inversion` instants; the
  `SchedRelayWait` relay marker. Initial skeleton + group/timer naming landed in
  an earlier change; the contention signals (waiter counter, priority inversion), the relay
  marker, and the reentrant op suffix followed (this change).
- **Phase 2**: the always-on bounded in-flight/holder registry + the on-demand
  scheduler dump (§4.6); optional periodic watchdog.
- **Phase 3**: deadlock-cycle detection (§4.7); action-handler in-flight view
  (§4.8); timer coalescing/overflow visibility by EventKind.

## 8. Open questions

1. `SchedRelayWait` marker vs surfacing relay depth only in the dump (§4.1) —
   marker preferred for timeline visibility; confirm at implementation.
2. `sched.group_waiters` as a Perfetto COUNTER (needs the converter to map a
   counter event per group) vs dump-only — counter preferred.
3. Registry bound size (§4.6) — cap per executor; pick a number at impl.

## 9. Review items folded in

v1 → v2 (first review): the five-path reality + shared helper (replacing the false
single chokepoint); explicit-id emit instead of RAII across `.await`;
blocked-vs-busy demoted to "suggestive"; the per-group waiter counter.

v2 → v3 (second review; it AGREED v2 fixed the prior P1): action execute is OFF the
dispatch path → scoped out (§2/§4.8); queue_wait/group_wait are `FLAG_ASYNC`
async tracks, only the sync timer callback is a thread lane (§4.0/§4.1);
`thread_dur` needs a new explicit-id+CPU emit API (§3/§4.4); per-span free-form
attributes don't exist → group/priority/op-name move to name suffixes +
instants/counters + the dump, dropping "directly queryable on the span" (§2/§4.2/
§4.3); `resolve_for_event -> ResolvedGroup` instead of a bare mutex (§4.2);
`aorta_reentrant()` takes an `op` arg (not `LAST_ERROR`, §4.3); `SchedCallback`
is Timer-only (§4.0); `Event::enqueued_at` is `std::time::Instant` (§3);
don't blur the relay/ingress hop into queue_wait (§4.1); single-thread executor
has no group_wait — documented (§4.1); the registry is always-on+bounded, only
emission gated (§4.6); the dump records holds+waits so cycle detection is a pure
later addition (§4.6/§4.7).
