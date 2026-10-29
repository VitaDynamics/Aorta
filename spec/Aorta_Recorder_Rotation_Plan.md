# Aorta Recorder Rotation & Splitting Plan

Last updated: 2026-06-16
Status: Proposal (revised after design review 2026-06-16)

Companion to `spec/Aorta_Recorder_Design_Proposal.md` (§9.3 `storage.rotation`,
§12.3 Phase C "Rotation runtime behavior: still reserved"). This plan turns the reserved
`storage.rotation.*` fields into a real, implemented feature.

## 1. Summary

The recorder currently writes exactly **one** MCAP file per invocation, named
verbatim from `--output` / `record.output`. There is no segmentation, no
rotation, and no retention.

This plan adds two orthogonal capabilities:

| Layer | Also called | Trigger | Action |
|---|---|---|---|
| **Segmenting** | splitting | segment exceeds time period (or optional size cap) | close current segment (full footer/summary) → open next |
| **Retention** | rotation | segment count > `max_files`, or total bytes > `max_total_size_mb` | delete the oldest **finalized segment produced by this run** |

Core invariant: **every segment is an independently-readable, self-contained
MCAP file.** A split point must land on an MCAP chunk boundary, i.e. a roll is
`inner.finish()` (writes footer/index) followed by opening a fresh file. MCAP
chunks cannot be split mid-chunk.

## 2. Confirmed Decisions

1. **Segments are named purely by start time, no sequence number.**
   `{stem}_{ts}.mcap`, where `ts = YYYYMMDDThhmmss_fffZ` (segment start,
   UTC, **millisecond** precision, derived from `unix_time_ns()`).
2. **Naming collision defence is layered, not floor-dependent**.
   Millisecond-precision start times make same-name collisions across runs /
   concurrent recorders / clock steps very unlikely; on top of that, segments
   are created with **`create_new` (no-clobber)** and a name collision is a
   hard, loud failure — never a silent overwrite.
3. **Configured time splits use whole minutes.** `split_minutes`, when set, must
   be at least one. A size-only policy has no time floor.
4. **Time and size are independent triggers.** Supplying only
   `max_file_size_mb` creates size-only segments; supplying both rolls when
   either threshold is reached. The finalized file may exceed the size cap by
   the buffered data plus footer/summary overhead.
5. Rotation is **off by default**; when off, behavior is byte-for-byte identical
   to today (literal single file, no template, no `.part`).
6. Retention deletion only ever touches files **this process created in this
   run**, tracked in memory. Never glob-and-delete.

## 3. Filename Scheme

### 3.1 Template

When rotation is enabled, the output path becomes a template. Default template
(derived from `record.output` when `name_template` is null):

```
{stem}_{ts}.{ext}
```

Placeholders (expanded from `unix_time_ns()` + config; no heavy new dep):

- `{ts}` — segment **start** time, `YYYYMMDDThhmmss_fffZ`, UTC, millisecond
  precision (`fff` = 3-digit milliseconds-of-second, `(unix_time_ns() / 1_000_000)
  % 1000`). `timestamp: local` substitutes the local zone but is discouraged
  (DST / zone ambiguity — see §3.4). Sorts lexicographically in chronological
  order.
- `{stem}`, `{ext}` — from the base `record.output`.
- `{group}`, `{pid}` — optional, available for users who want extra
  disambiguation in the stem.
- `{index}` — optional zero-padded segment index (`000`, `001`, ...), useful
  when a session owner needs stable ordered names such as
  `session_20260915_150433_000.mcap`.

The format is forced to UTC by default; millisecond precision is the chosen
collision-resistance mechanism and keeps names pure-time without a
sequence number. The `create_new` hard stop prevents silent overwrite if two
segment starts still receive the same name. Operators wanting even lower odds
may raise precision (e.g. `_ffffffZ` microsecond) via `name_template`.

Example (`record.output = /data/aorta.mcap`, `split_minutes = 1`):

```
/data/aorta_20260616T142530_004Z.mcap   segment 1, started 14:25:30.004
/data/aorta_20260616T142630_018Z.mcap   segment 2, started 14:26:30.018
/data/aorta_20260616T142730_031Z.mcap   segment 3, started 14:27:30.031
```

The default identity remains the start timestamp (millisecond). Consumers that
own a longer-lived session may opt into `{index}` instead.

### 3.2 In-progress (`.part`) + atomic rename + no-clobber

A segment being written is named `…​.mcap.part`. On clean `finish()`
(footer/summary written), it is atomically `rename`d to the final `….mcap`.

- The `.part` file is created with **`create_new` / `O_EXCL` (no-clobber)**
  semantics. The current `McapRecorderWriter::create` uses
  `File::create`, which *truncates* an existing path; the rotating path must not.
  If the `.part` or the final name already exists (prior run, concurrent
  recorder), the recorder **fails loudly** rather than silently overwriting.
- **The `.part`→final promotion is also no-clobber.** `std::fs::rename` *replaces*
  an existing destination on Unix, so the promotion pre-checks that the final
  name does not exist and errors otherwise. This matters for
  a `{ts}`-less custom template (every segment resolves to one name) and for a
  cross-run/same-ms collision. The residual check→rename race is bounded by the
  per-segment `.part` `create_new` reservation and millisecond-unique names.
- Downstream uploaders/watchers treat "a non-`.part` `.mcap` appeared" as
  "this segment is complete and valid" — unambiguous completion signal.
- Library consumers may use `run_with_segment_callback`; it invokes a short,
  synchronous callback after atomic promotion of every segment, including the
  final segment at shutdown. The callback should only persist or enqueue the
  event. Upload runs elsewhere. Callback mode rejects Aorta retention settings
  because the consumer owns upload and reclamation of finalized files.
- On crash, the active `.part` remains. It is **only partially recoverable**: an
  MCAP without a `finish()`-written footer/summary is not guaranteed readable by
  standard tooling and may be truncated; recovery requires an MCAP repair pass.
  Already-renamed historical segments are complete and untouched.

### 3.3 Backward compatibility

When `rotation.enabled = false` (default), `record.output` is the literal single
file. No template expansion, no `.part`, no timestamp injection — identical to
the current implementation.

### 3.4 Cross-run / concurrent / clock-skew collision

Millisecond-precision start times plus `create_new` are the defence
(§2.2, §3.2). Two independent recorders, or a restart, producing a byte-identical
millisecond start time is unlikely; if it ever happens, `create_new` turns it
into a hard error instead of a silent overwrite. Operators wanting
namespace separation beyond the timestamp may add `{group}` / `{pid}` to the
stem. **UTC is the default and recommended** — `timestamp: local` reintroduces
DST/zone collisions (a backward clock step can repeat a wall-clock value) and is
opt-in only.

## 4. Segmenting (splitting)

### 4.1 Triggers

```yaml
split_minutes: null       # optional integer minutes ≥ 1
max_file_size_mb: 2048    # optional size cap (0/null = disabled)
```

- **Time:** segment 1 starts at recorder start T0; roll every
  `split_minutes`. Segment start time → the `{ts}` in its filename. Segments are
  NOT aligned to wall-clock `:00`; they run from T0 + k·period.
- **Size:** if the flushed on-disk segment reaches `max_file_size_mb`, roll it
  without waiting for a minimum duration.

### 4.2 Trigger combinations

If only `max_file_size_mb` is set, rotation is size-only. If
`split_minutes` is also set, either threshold rolls the segment. `--rotate`
with neither threshold preserves the original one-minute default. Final
shutdown always finalizes whatever partial segment exists.

### 4.3 Evaluation timing & chunk boundary

Triggers are evaluated only at **flush boundaries** (the run loop already
flushes on batch threshold + interval tick) plus a coarse roll-check tick for
the idle-but-time-elapsed case. Size source for the **cap decision** =
`fs::metadata(part_path).len()` taken post-flush — i.e. *current flushed on-disk
bytes, excluding the not-yet-written final footer/summary*. It is
an estimate of the active segment, deliberately preferred over `accepted_bytes`
(uncompressed logical payload bytes, which ignore compression and MCAP
overhead). `accepted_bytes` is never used for disk caps. Size source for
**retention accounting** = the finalized `fs::metadata(final_path).len()` taken
*after* `finish()` + rename (then the footer/summary are included). A roll is
`inner.finish()` → rename → open next, so the split always lands on a chunk
boundary.

### 4.4 Empty-segment suppression

If the period elapses but zero messages were written since the last roll, do not
roll an empty file. Two concrete behaviors implement this (both regression-tested):

- **Idle-window reset.** When an empty period elapses, advance the active
  segment's window start to *now* so the next message begins a fresh period
  rather than being rolled immediately into a stale one-message segment.
- **Empty trailing-segment discard.** If a roll opens a new segment and the run
  then stops before any message arrives, `finish()` **discards** that empty
  segment (removes the `.part`, does not promote or retain it) rather than
  finalizing it — provided at least one prior segment already holds data.
  Otherwise retention (e.g. `max_files=1`) would evict the real data segment in
  favour of the empty one. A recording that captured *zero*
  messages still emits one (empty) segment, matching the single-file path.

## 5. Retention (rotation)

```yaml
max_files: 10             # 0/null = keep all
max_total_size_mb: null   # optional aggregate cap (0/null = disabled)
```

After a segment is finalized (renamed off `.part`), push its path onto an
in-memory `VecDeque<PathBuf>`. While `len > max_files` (or summed on-disk bytes
> `max_total_size_mb`), pop and delete from the front (oldest).

Safety rules (non-negotiable):

- Only delete paths in this run's deque — files this process created.
- Never delete via glob/wildcard.
- Never delete the active (currently-writing) segment.
- Deletions are logged and counted in the run summary.

Cross-run / cross-restart retention (cleaning historical files from a prior
process) is **out of scope for v1** — it would require globbing the template and
carries real blast-radius risk.

## 6. Segment Self-Containment

A naive roll produces a new file lacking (a) the current ctx state and (b) the
schema/channel declarations (the writer's `channel_ids`/`schema_ids` caches are
per-file). To keep each segment independently replayable:

1. **ctx latch replay.** The recorder already subscribes `ctx/**`. Maintain an
   in-memory `HashMap<full_key, LatchedCtx>` where `LatchedCtx` holds the latest
   **raw ctx payload** plus its original `log_time_ns` / `publish_time_ns`. Ctx
   messages are written with `schema: None` today (raw channel; see
   `write_ctx_snapshot` and the non-`pub/` live path), so the latch carries **no
   schema**. The map is
   seeded by the t0 wildcard-GET snapshot and updated on every live ctx put. On
   roll, **synchronously** replay it into the new segment (pure memory, no
   network — avoids the stall a fresh wildcard GET per roll would cause).
   Streaming `pub/**` topics are not latched/replayed.

   **Replay timestamp semantics:** each replayed message uses the **new
   segment's start time as `log_time`** (so a time-indexed reader sees the
   state-as-of-segment-start), while **preserving the original `publish_time`**
   from the latch (so provenance — when the value was actually published — is not
   lost). This mirrors the t0-snapshot convention but keeps publish provenance.

   **Counter/cap accounting:** replayed ctx messages are **synthetic re-emits**,
   not new ingress. They do **not** count toward the global `max_messages` cap
   (otherwise a long recording with many ctx keys would prematurely hit the cap
   purely from per-roll replays). The t0 snapshot keeps its existing behavior of
   counting toward `max_messages`. Replayed bytes *do* count toward the active
   segment's size estimate (they are real on-disk bytes) but are tracked in a
   separate `replayed_messages` counter for observability rather than inflating
   `accepted_messages`.
2. **Schema/channel re-emit.** Handled implicitly: the fresh `McapRecorderWriter`
   re-adds schemas/channels lazily on first use (typed `pub/**` channels) and
   raw channels for replayed ctx. No `McapRecorderWriter` change needed for this.
3. **Per-segment metadata.** Each segment's start metadata gains:
   - `aorta.recording_id` — stable id shared by all segments of one recording,
     so downstream can regroup segments into one logical recording.
   - `aorta.segment.start_ns` — authoritative ordering key (replaces a sequence
     number).
   - `aorta.segment.continues` — `true` for segments after the first.
   - `aorta.rotation.reason` — `time | size | final`.

   Bounds: ctx cardinality is assumed bounded (it is canonical robot state, not
   a high-cardinality stream); the latch is O(#ctx keys). If this assumption is
   ever violated, a max-entry bound on the latch is the mitigation (follow-up).

## 7. Configuration Model

Extends `storage.rotation` in `Aorta_Recorder_Design_Proposal.md` §9.3:

```yaml
storage:
  rotation:
    enabled: false           # master switch; off => today's single-file behavior
    split_minutes: null      # optional integer ≥ 1; defaults to 1 if no size cap
    max_file_size_mb: 2048   # optional size cap; alone means size-only
    # naming
    name_template: null      # null => {stem}_{ts}.{ext}; optional {index}
    timestamp: utc           # utc | local
    # retention
    max_files: 10            # 0/null = keep all
    max_total_size_mb: null  # optional aggregate cap
```

Precedence unchanged: `CLI args > env > config file > defaults`.

### 7.1 CLI flags (both `aorta-record` and `aorta record`)

- `--rotate` → `enabled = true`
- `--split-minutes <N>`
- `--rotate-size-mb <N>` → `max_file_size_mb`
- `--rotate-max-files <N>`
- `--rotate-max-total-mb <N>`
- `--rotate-name-template <T>`

Note: the existing global `--duration` / `record.duration_sec` stops the **whole
recording**; `--split-minutes` rolls **within** a recording. They are distinct
and may be combined (the final partial segment is finished + renamed normally on
stop).

## 8. Code Structure

Keep `McapRecorderWriter` as the **single-file primitive**, with two small,
additive API changes:

- `create(...)` gains a `segment: SegmentMeta` parameter (`recording_id`,
  `segment.start_ns`, `continues`, `rotation.reason`) so per-segment start
  metadata can be injected. Existing callers pass a trivial single-segment meta.
- `create(...)` uses `OpenOptions::new().write(true).create_new(true)` instead of
  `File::create` **on the rotating path** (no-clobber, §3.2). The non-rotating
  path keeps `File::create` for byte-for-byte back-compat (it may overwrite a
  user-named output, as today).

`finish(self, ...)` already consumes the writer, so the composition holds the
active segment in an `Option` and `take()`s it on roll:

```
RotatingWriter (new)
  ├─ inner: Option<McapRecorderWriter>  // active segment; take()'n on roll
  ├─ policy: { split: Option<Duration>, size_cap: Option<u64> }
  ├─ naming: NameTemplate               // template + .part + millisecond ts
  ├─ retained: VecDeque<(PathBuf, u64)> // this run's finalized (path, bytes)
  ├─ ctx_latch: HashMap<String, LatchedCtx>
  ├─ recording_id, segment_start: Instant, segment_start_ns: u64
  ├─ msgs_since_roll, part_path
  └─ write_message / update_ctx / flush / maybe_roll(now) / finish
```

No async lock is needed: the writer lives entirely in the single `run()` task;
ingress subscribers only feed it via the existing mpsc channel.

`maybe_roll(now)`:
1. if the optional time period elapsed OR the post-flush `part_path` reached
   the optional size cap:
   - skip if `msgs_since_roll == 0` (empty-segment suppression);
   - else `self.inner.take().unwrap().finish(...)` → atomic rename `.part`→final
     → stat finalized size → push `(final_path, size)` to `retained` → enforce
     retention (delete oldest) → `create_new` next segment (next `{ts}`) →
     replay `ctx_latch` into it → reset `segment_start`, `msgs_since_roll`.
2. on `finish`/rename/`create_new` failure: keep the `.part` (do not lose data),
   log an error, and treat the roll as fatal to the recording (a degraded
   "keep ingesting into a full/over-cap or stale segment" mode is explicitly NOT
   chosen — see §9).

`run()` changes (in `tools/aorta-record/src/lib.rs`) are small and localized:

- Replace the bare `writer` (currently `lib.rs:214`) with `RotatingWriter`
  (when rotation is off it is a single-segment pass-through, equivalent to
  today).
- Call `update_ctx(...)` wherever a ctx message is written (initial snapshot
  loop + live ctx puts).
- After each flush point (`lib.rs:314`, `lib.rs:382-384`) call
  `maybe_roll(now)`; add a coarse roll-check tick to the `select!`.
- At shutdown (`lib.rs:444`) call `finish()` to finalize + rename the last
  segment.
- Extend `RunSummary` with `segment_count`, `total_bytes_all_segments`,
  `deleted_segments`, and (optionally) the segment path list.

DRY: rotation logic lives entirely in `RotatingWriter`; the ingress loop and the
single-file writer stay simple.

## 9. Failure & Edge Semantics

- Global `duration_sec` / `max_messages` / Ctrl-C reached → normal stop; last
  segment finished + renamed (reason = `final`).
- Crash → active `.part` left behind, **partially recoverable only** (no footer/
  summary; needs an MCAP repair pass; may be truncated). Already-renamed segments
  are complete and intact (§3.2).
- Cross-run / concurrent / clock-skew naming → millisecond ts + `create_new`
  (§2.2, §3.4); a true collision is a hard failure, never a silent overwrite.
- Roll-time failure (`finish`/rename/`create_new` errors, e.g. disk full) → keep
  the current segment's `.part` rather than lose it, log an error, and **fail the
  recording** (propagate the error out of `run()` as today's writer errors do
  via `?`). Note: `drop_policy` governs only *queue send* backpressure, not roll
  failures — it is **not** reused here (correcting the earlier draft).
- Retention deletion only after finalize, only from the in-run deque, never the
  active segment; a deletion failure is logged and counted but is non-fatal (the
  cap is best-effort, like the size cap).

## 10. Phased Delivery

- **P1 — Segmenting + naming.** `RotatingWriter`, independent time/size triggers,
  `{stem}_{ts}.mcap` millisecond naming, `.part` + `create_new` no-clobber + atomic
  rename, `max_file_size_mb`. Run-loop wiring. Red/green tests: time roll;
  size-only roll without a time floor;
  empty-segment suppression; rename atomicity; **`create_new` fails loudly on a
  pre-existing final/`.part` name**; **`duration_sec < 60` produces one valid
  sub-floor final segment**; **rotation-disabled path is byte-for-byte identical
  to today** (incl. still using `File::create`).
- **P2 — Segment self-containment.** ctx latch replay, per-segment metadata,
  `recording_id`. Tests: each segment independently Foxglove-readable and carries
  current ctx; **replayed ctx uses segment-start `log_time` and preserves
  original `publish_time`**; **replayed ctx does NOT advance `max_messages`** (a
  capped run stops on real ingress, not on replays); latch reflects last live ctx
  put.
- **P3 — Retention.** `max_files` / `max_total_size_mb` ring deletion + safety
  tests: only this run's deque; never the active segment; never glob; total-bytes
  computed from finalized sizes after rename; deletion failure is non-fatal.
- **P4 — Docs.** Promote §12.3 "rotation: reserved" → implemented; update §9.3
  config schema to match this plan.

Additional edge tests: backward clock step / `timestamp: local` DST
window does not silently overwrite (UTC default + `create_new`); two concurrent
recorders targeting the same base path collide loudly rather than corrupt;
crash-left `.part` is detected as incomplete (no footer) by the validation tool.

## 11. Validation Checklist

- Rotation off → identical bytes to current single-file behavior.
- Size-only rotation has no time trigger or minimum duration.
- Millisecond timestamp names + `create_new` → no silent overwrite across runs,
  concurrent recorders, or clock steps; a real collision fails loudly.
- Each segment is a standalone, Foxglove-readable MCAP carrying current ctx.
- `.part` → final rename is atomic; crash leaves a partially-recoverable `.part`
  (no footer) plus intact prior segments.
- Replayed ctx does not advance `max_messages`.
- Retention deletes only this run's finalized segments, never the active one.
- `aorta-record` and `aorta record` share the same `RotatingWriter` path.
