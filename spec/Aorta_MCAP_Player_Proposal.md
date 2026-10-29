# Aorta MCAP Player Proposal (v10 — no sim time)

Status: DRAFT v10 (round 1: NO-GO/20 → §11; … round 8: NO-GO/1 → §18; round
9 re-gate: round-8 P1 **CLOSED**, 3 implementation interpretations ratified
as errata, 1 rejected (DataEnd probe collision — implementation fix, spec
unchanged), 3 spec errata applied → §19)
Date: 2026-07-30
Author: middleware team
Scope: recorded-topic playback (simulated time, `Clock::Simulation`, explicitly **excluded** from v1)

## 1. Motivation

Developers need to replay recorded MCAP data against live programs to validate
functionality — the `rosbag play` workflow. Today Aorta has a mature recorder
(`tools/aorta-record`, rotation, ctx snapshot, RPC record mirror) but **zero
playback capability**: no playback work had started, and a CLI/TUI gap
review confirmed "no replay primitive exists in core or tools today".

v1 goal: a correct, minimal `aorta-play` that replays recorded `pub/**` topics
with real-time pacing and rate control. Simulated clock distribution
(`Clock::Simulation`) is out of scope; downstream code that reads
embedded message timestamps will see original recorded stamps (§6.8, §10).

## 2. Current state (verified against code)

What the recorder writes (all in `tools/aorta-record/src/writer.rs`):

- **Schema record**: `name` = FlatBuffers root type name when extractable from
  the bfbs, else the schema hash (fallback, `writer.rs:278-290`); `encoding` =
  `"flatbuffer"`, `data` = raw **bfbs** bytes. Keyed by SHA-256 schema hash as
  recorded (caller-supplied `hash_hex`, not recomputed — §6.2).
- **Channel record**: `topic` = full aorta key
  (`aorta/{group}/pub/{category}/{name}`), `message_encoding` = `"flatbuffer"`
  (typed) or `"aorta_raw_bytes"` (raw, schema_id 0). Channel identity is
  `(topic, schema_hash)` — one topic can own multiple channels (raw + typed,
  or schema evolution). Channel metadata: `aorta_key`, `key_suffix`,
  `schema_hash`.
- **Message record**: `log_time` = recorder ingest wall clock, approximately
  but **not strictly** monotonic within a file (per-subscriber stamping ahead
  of the writer queue). `publish_time` = Zenoh sample timestamp when present.
  `data` = verbatim wire payload (AortaHeader, when the schema defines one, is
  an embedded field, not a wire frame) — re-publishable as-is.
- **Summary / indexes**: optional (`--no-summary`). Chunked writing with
  per-chunk CRCs and a data-section CRC is the mcap default and Aorta does not
  disable them (`writer.rs:300-315`, mcap `write.rs:141`); chunk size is
  user-configurable with **no upper bound** (`config.rs:661`). Message-index
  `offset`s are **relative to the decompressed contents of their chunk**
  (mcap `records.rs:366`; `ChunkIndex.chunk_start_offset` gives the chunk's
  file position) — one reason §6.5 orders by the record stream instead.
- **File integrity on crash**: `aorta.recorder.end` and the MCAP
  footer/end-magic are written together in `finish()` (`writer.rs:199-236`).
  The **non-rotating** path writes the user's final `.mcap` path directly
  (`writer.rs:66`), so an *actively recording* file has the same footer-less
  signature as a crashed one; only rotation uses `.part`-then-rename
  (`rotation.rs:200,426`). §6.1's private-copy snapshot exists because of
  this.
- **Metadata records**: `aorta.recorder.start` (`aorta.group`,
  `aorta.started_at_ns`; rotating runs add `aorta.recording_id` +
  `aorta.segment.index` + `aorta.segment.continues`; non-rotating files carry
  no recording/segment identity) and `aorta.recorder.end`. Profile `"aorta"`.
- **ctx channels**: recorded on a typed `aorta.ctx.ContextValue` channel.

What the mcap 0.24 reader offers (drives §6.1/§6.7 API choices):

- High-level `MessageStream` composes `ChunkFlattener` with only ordinary
  chunk-CRC checking (`read.rs:262,380`): it does **not** enable
  `prevalidate_chunk_crcs` (validate a chunk *before* yielding any message),
  `validate_data_section_crc`, or `record_length_limit` — those are
  `sans_io::LinearReader` options (`linear_reader.rs:181`). The low-level
  reader can yield records from a chunk before the chunk-end CRC comparison
  (`linear_reader.rs:668,730`) and reserves the declared decompressed size
  before any queue accounting (`linear_reader.rs:826`). The player therefore
  uses `sans_io::LinearReader` directly with all three options, behind the
  §6.7 adapter.
- **CRC zero is mcap's "do not compare" sentinel — and also a valid CRC32
  output**: prevalidation enters its validating state only when the stored
  chunk CRC is nonzero (`linear_reader.rs:589,623`) — and the whole-chunk
  `record_length_limit` checks live inside that skipped branch; the `DataEnd`
  comparison likewise runs only for a nonzero saved value
  (`linear_reader.rs:509`). Meanwhile the writer serializes the raw hasher
  output verbatim (`write.rs:1519,1549,1049`; `io_utils.rs:40`) and CRC32 can
  legitimately calculate to `0x00000000` — the format cannot distinguish
  "calculated zero" from "not calculated". A stored zero therefore can be a
  valid Aorta recording and must not be rejected outright; §6.1 makes the
  player *recalculate and compare* in the zero case instead, and §6.7
  enforces size limits independently of CRC presence.
- **Allocation precedes the configured limit at the API boundary**: for a
  chunk, the reader consumes the caller-visible header (including the
  attacker-controlled compression-string length) and issues a `ReadRequest`
  before the whole-chunk limit checks; `LinearReader::insert(need)` grows the
  raw input buffer to the requested size (`linear_reader.rs:129,372,560`).
  Parser state and physical offsets are private (`linear_reader.rs:304,790`).
  The §6.7 adapter therefore caps every `ReadRequest` itself and tracks outer
  record framing/offsets on its own.
- `IgnoreEndMagic` only waives the final magic; a torn record/chunk still
  surfaces as `UnexpectedEof` parser state (`linear_reader.rs:392`) — §6.1
  classifies via the adapter's own offsets/framing, not by error type.
- Message indexes are written only after their chunk completes
  (`write.rs:1588`) and are **channel-grouped** — one
  `Vec<MessageIndexEntry>` record per channel per chunk (`records.rs:373`,
  `write.rs:1595`), so reconstructing within-chunk order from indexes would
  require retaining and merging all of a chunk's index records (aggregate
  unbounded by the per-record limit). The player does **not** use indexes
  for ordering (§6.5) — prevalidation already decompresses every retained
  chunk, so the in-chunk record stream is available for free; index records
  are length-validated and skipped. A torn chunk's exact message count is
  unknowable either way (EOF precedes its indexes; §6.1 reports `unknown`).

What the publish side already offers (no core changes needed):

- `Node::create_publisher_with_options(topic, options)` (`node.rs:800`);
  `PublisherOptions` publicly carries schema bfbs **and**
  `schema_attach_policy` (`publisher.rs:30`) — per-publisher, no env writes.
  By-key registration runs `SchemaConflictAction::from_env` (Reject/Warn);
  **Warn is not convergence** — each Node's by-key queryable answers its own
  cache (`schema_registry.rs:534,699`), so a Warn-overwrite leaves the
  original publisher serving the old schema. Schema queryables start only
  when env-enabled (`node.rs:240`, `schema_registry.rs:472`) — with the gate
  off, the player's own registration answers nothing (§6.3 handles this).
- Multi-responder query precedent: `QueryTarget::All` +
  `ConsolidationMode::None` + one absolute get+drain deadline
  (`schema_registry.rs:501`, `schema_resolve.rs:371`, `capture.rs:183`);
  consolidation left at Zenoh's default (`Auto`→`Latest`) can hide same-key
  responders. The by-key reply decoder recomputes the BFBS hash before
  accepting a reply (`schema_resolve.rs:344`).
- `Publisher::publish(bytes)` (`publisher.rs:455`): fire-and-forget, success
  = enqueued. `wait_for_matching(timeout)` (`publisher.rs:380-407`): bounded
  poll loop, **no cancellation token** — the player wraps it in a
  token-select (§6.7).
- `Node::close()` (`node.rs:454-509`) is **not** end-to-end bounded (unbounded
  telemetry/queryable task joins before the bounded session close) — §6.7
  wraps the entire teardown in an outer deadline and §6.6 reports expiry as a
  distinct non-success termination.
- Group: Node ignores its group argument and reads `AORTA_GROUP` from process
  env (`node.rs:184`, `key_expr.rs:778`); `aorta-cli` exports the flag then
  merges flag/ambient (`cli.group.or_else(env)`, `main.rs:538`) — `play`
  captures provenance pre-merge (§6.9).

## 3. Goals / non-goals

Goals (v1):

- G1: Replay one recording (single file or a rotated segment set) with
  original relative timing.
- G2: Rate control, start offset, duration limit, loop mode; all numeric
  inputs validated per §6.12.
- G3: Topic include/exclude filtering.
- G4: Replay into the recorded group by default; `--group` to remap.
- G5: Best-effort schema discovery parity (§6.3; bounded by env gates,
  per-publisher attachment policy, schema policy, and the point-in-time
  nature of the preflight).
- G6: Deterministic, testable scheduler with a fully-specified ordering
  contract (§6.5), including the canonical record-stream order definition.
- G7: Defined error, termination, cancellation, and shutdown contract
  (§6.6, §6.7): cancellation observed at every player-controlled
  await/check point under one stated first-signal budget, with **one
  documented exclusion** — an in-progress synchronous transport publish,
  during which library return is unbounded and the wrapper's second-signal
  process exit is the only bound (§6.7); no `process::exit`, no signal
  handling in the library; teardown-deadline expiry is a distinct
  non-success outcome.
- G8: Bounded resource use (§6.7): a stated peak-memory invariant, private
  immutable snapshots, declared-size validation before allocation.

Non-goals (v1, tracked in §10):

- Sim time; rewriting embedded timestamps or `AortaHeader` identity.
- Replaying `ctx/**`, `sys/**` channels.
- Mixed raw+typed topics (fail closed, §6.3).
- Typed replay with player-local schema queryables env-disabled (fail
  closed, §6.3).
- Interactive pause/step/seek, topic remap, keyexpr globs.
- Blocking congestion control / delivery guarantees.
- Global schema arbitration: single active schema provider per replayed
  topic remains an operational deployment constraint; the player guards but
  cannot enforce it (§6.3).

## 4. Tool placement & layering

- New crate `tools/aorta-play`: `src/lib.rs` exposes
  `run(config) -> Result<RunOutcome, PlayerError>` and
  `run_with_shutdown(config, shutdown: CancellationToken) ->
  Result<RunOutcome, PlayerError>` (§6.6/§6.7);
  `src/main.rs` is a thin clap wrapper owning signals via the §6.7
  scheduler-independent signal thread. `aorta-cli play`
  wires the same flags to the same engine (§6.9).
- Pure L3 tool; `aorta_core` high-level API only; no
  `aorta_core::ffi::*`. **Zero core/ changes in v1.**
- MCAP reading via the in-tree `mcap` 0.24 crate's `sans_io::LinearReader`
  (§2, §6.7) over private snapshot copies (§6.1). No `memmap2` file-backed
  maps of user-writable files (their safety contract makes concurrently
  modified mappings UB — §6.1).

## 5. CLI surface

```
aorta-play [OPTIONS] <INPUT>...        # files and/or directories
aorta-cli play [OPTIONS] <INPUT>...    # same engine

  <INPUT>...            One or more .mcap files, or a directory containing a
                        rotated segment set (non-recursive *.mcap glob).
  -r, --rate <F>        Playback rate multiplier (default 1.0; §6.12).
  -s, --start <SECS>    Skip the first SECS seconds of the effective timeline
                        (§6.5; §6.12).
  -d, --duration <SECS> Stop after SECS seconds of effective recorded time.
      --topics <T>...   Only replay these topics (topic name, key suffix, or
                        full key; exact match against *recorded* keys).
      --exclude <T>...  Skip these topics (applied after --topics).
  -l, --loop            Loop forever (Ctrl-C to stop).
      --match-timeout <SECS>      Matched-subscriber barrier bound
                                  (default 5.0; 0 disables; §6.4, §6.12).
      --preflight-timeout <SECS>  Per-topic schema-preflight get+drain
                                  deadline (default 5.0; must be > 0 — zero
                                  would make every preflight inconclusive by
                                  the §6.3 deadline rule; §6.12).
      --schema-policy <strict|latest>   Multi-typed-schema topics
                                        (default strict = error; §6.3).
      --allow-gaps      Play a rotated set with missing/duplicate segment
                        indices (default: error; §6.1).
      --max-message-bytes <N>   Reject larger message payloads
                                (default 256 MiB; must be ≤ --buffer-bytes).
      --max-chunk-bytes <N>     Reject larger declared compressed or
                                decompressed chunk / generic record sizes
                                (default 512 MiB; wired to the reader's
                                record_length_limit; §6.7).
      --buffer-bytes <N>        In-flight byte budget of the reader queue
                                (default 256 MiB; §6.7).
      --group <G>       Replay into group G instead of the recorded group.
      --zenoh-config <PATH>
      --log-level <L>
```

End-of-run summary (also printed on Ctrl-C): messages published (enqueued —
§6.4), per-channel counts, late count + max lateness, clamp count (§6.5),
publish errors, unmatched-topic list, skipped channels (§6.10),
truncation/recovery report (torn files; discarded tail chunks exactly;
discarded tail *messages* as an exact count where the verified record
stream covers them, else
`unknown` — a torn prevalidated chunk yields no messages to count and its
message index was never written, §6.1), teardown report (stuck stage +
residual counts if the deadline expired — §6.6/§6.7), wall duration.

Progress line (TTY only, 4 Hz): `[t=12.4s / 87.1s] [rate 1.0x] 4821 msgs`.

## 6. Design

### 6.1 Pipeline, snapshot & validation

```
pass 0: private snapshot copies → segment ordering → pass 1: verify + index
+ timeline + selection → publish plan (selected topics only) → schema
preflight → publisher map → matching barrier → pass 2: timed replay
(bounded by the pass-1 verified prefix)
```

**Pass 0 — private immutable snapshot**: for each input file, capture
identity (dev/inode, size, mtime), then **stream-copy** its bytes (ordinary
buffered reads, fixed-size blocks, cancellation checked per block) into a
private temporary file in the player's temp dir; the temp file is opened
once and unlinked immediately (fd-backed, vanishes on process death).
Re-check source identity **after** the copy; any change → hard error
(`input changed during snapshot — is the recorder still writing this
file?`). All later passes read **only** the private copies; the original
files are never touched again. This is a real byte snapshot — no
file-backed mmap of user-writable files (memmap2's contract makes
concurrently modified mappings UB, and post-access metadata checks cannot
retroactively make dereferenced pages safe). Cost: temp disk equal to the
input size; free space is checked up front (hard error with the required
size if insufficient) and documented in `--help`. Copies are created
sequentially at startup and each is retained until its segment finishes
pass 2 (one consistent lifetime for both passes; pass 1 and pass 2 read the
same snapshot fd).

**File opening & crash recovery** (over the snapshot): strict open first; a
file failing only the end-magic check is reopened with `IgnoreEndMagic`,
flagged `truncated` (warning + summary). Pass 1 computes the **verified
safe prefix**, authoritative for pass 2:

- *Unchunked regions*: complete records up to (excluding) the torn final
  record.
- *Chunked regions*: only chunks whose declared lengths pass the §6.7
  bounds, lie fully inside the snapshot, and whose **decompression and
  chunk-CRC validation succeeded in pass 1** (prevalidation — §6.7 reader
  options). The incomplete/unvalidated final chunk is discarded whole.
- *Complete files*: the data-section CRC is validated in pass 1
  (`validate_data_section_crc`); mismatch → hard error (corrupt file).
- **CRC-zero policy (recalculate, don't reject)**: a stored zero is both
  mcap's "do not compare" sentinel *and* a possible legitimate CRC32 result
  (§2), so rejecting it would misclassify valid recorder output. Instead,
  where a stored CRC is zero the **adapter performs the comparison mcap
  skips**: for a zero-CRC chunk it fully decompresses and hashes the chunk
  in pass 1 *before* any inner record may be yielded, and requires the
  calculated CRC to equal the stored zero; for a zero `DataEnd` CRC it
  maintains its own data-section hasher across pass 1 and requires the
  calculated value to equal zero. Mismatch → hard error, exactly as in the
  nonzero path. (Documented caveat: the format cannot prove a matching zero
  was *intentionally* written — calculated-match is the strongest available
  check.) Chunk size limits are enforced by the §6.7 adapter independently
  of CRC presence, so a zero-CRC chunk cannot smuggle an oversize
  allocation either.
- Classification uses the snapshot length plus the §6.7 adapter's own outer
  framing/offset tracking (the reader's parser state is private) — an error
  strictly inside the snapshot with complete framing (including a
  fully-framed chunk with a bad nonzero CRC) is **mid-file corruption →
  hard error**, truncated flag or not. Only the EOF-torn tail is
  recoverable.

Pass 2 replays strictly within the pass-1 verified boundaries. Discarded
tail **chunks** are counted exactly; discarded tail **messages** are exact
where the verified record stream covers them, else reported as `unknown`
(`Option<u64>` in `RunSummary`) — a torn prevalidated chunk never yielded
messages and its index was never written (§2), and the player does not
re-parse unverified bytes to manufacture a count. A
truncated-but-recovered replay terminates `Completed` (truncation reported,
not an error).

**Pass 1 (per snapshot)**: one linear walk of the snapshot through the
§6.7 adapter (prevalidation decompresses every retained chunk — that *is*
the verification cost) producing: channels, schemas,
`aorta.recorder.start` metadata, the verified prefix (above), the
effective timeline and selection counts (§6.5, computed from the record
stream in canonical order), and per-channel last `log_time` (for
`latest`). Summary/message-index records are length-validated and skipped
— not load-bearing (§2, §6.5). **Retained planning state is deliberately
small**: per-channel and per-schema entries plus per-segment scalars
(safe-prefix end offset, t_first, counts, selection bits) — chunk metadata
is *streamed, not retained*; pass 2 re-walks the snapshot with the same
adapter and deterministically recomputes per-message effective times and
selection predicates. Retained planning state runs under a checked byte
budget (default 64 MiB; exceeding it → hard error naming the snapshot —
adversarial channel/schema floods fail loudly rather than growing with
input length). Pass 1 runs on a dedicated thread under the same
stop-flag/sliced protocol as pass 2 (§6.7) so cancellation lands
mid-scan.

**Format validation (hard errors)**: profile `"aorta"`; `message_encoding`
∈ {`"flatbuffer"` (schema_id ≠ 0), `"aorta_raw_bytes"` (schema_id 0)};
schema `encoding` `"flatbuffer"`.

**Segment ordering & set validation**: inputs sharing a `recording_id`
ordered by `segment.index`; indices unique and contiguous from the lowest
present; duplicates/interior gaps → hard error unless `--allow-gaps`. At
most one id-less file per invocation. Mixed `recording_id`s, overlapping
unrelated time ranges, or mixed `aorta.group` → hard error. Only the final
segment may be truncated; a truncated interior segment → hard error.

**Channel consistency check**: `topic` == metadata `aorta_key`;
`key_suffix` must be the group-relative suffix of `topic`; violations →
hard error.

**Publish plan (selected topics only)**: publishers, preflight, and the
matching barrier cover only topics with ≥1 finally publishable message
(window ∩ filters). Excluded/out-of-window topics register nothing, create
nothing, and cannot fail startup; their messages still feed the effective
timeline (§6.5). Empty final selection → zero-count `Completed`, no
publishers, no preflight.

### 6.2 Schema integrity check

Recomputed SHA-256 over each Schema record's bfbs (canonical
`SchemaHash::from_bfbs_bytes`) compared against channel-metadata
`schema_hash`; mismatch → hard error naming the channel.

### 6.3 Topic plan, publisher map & schema preflight

Topic kinds:

- **Typed-only** (one schema, or several under `latest`): one publisher via
  `create_publisher_with_options(topic_suffix, options)` with the planned
  bfbs and per-publisher `schema_attach_policy: Never`.
- **Raw-only**: one schema-less publisher, also per-publisher `Never` (an
  inherited `Always` would fail for lack of schema); registers nothing.
- **Mixed raw + typed**: hard error (one transport key, one by-key schema
  slot — raw bytes would gain a false by-key association; deferred, §10).
- **Multiple typed schemas**: `strict` (default) → hard error; `latest` →
  register only the newest channel's schema (per-channel last `log_time`
  from pass 1); all typed bytes replay through that publisher;
  message-level identity intentionally unasserted — rolling-upgrade
  semantics, opt-in.
- Non-`pub/` suffixes → skipped and counted (§6.10).

**Queryable gate**: if the player's own schema queryables are env-disabled
(`queryable_enabled_from_env()` false), **typed replay fails closed before
any publisher is created** (hard error naming `AORTA_SCHEMA_QUERYABLE`):
the player's registrations could answer no by-key query, subscribers could
not resolve schemas, and the §6.3 second scan would be unsatisfiable.
Raw-only replay remains allowed in that mode (nothing to discover). This
replaces v4's warn-and-continue.

**Schema preflight — exact contract**: per planned typed topic, before any
registration, with `T = --preflight-timeout` (default 5 s; per-topic;
strictly positive — §6.12; each scan batch runs concurrently across
topics). **Startup phase arithmetic** (no single-`T` claim): the schema
phase is `first_scan (≤ T) → publisher creation (P) → second_scan (≤ T)`,
followed by the matching barrier (`≤ --match-timeout = M`). `P` — Core
schema registration (its own env-configurable query timeout,
`schema_registry.rs:264`, plus puts/queryable/publisher declarations) — has
**no hard bound in v1**; normal startup is therefore bounded by
`T + P + T + M` with `P` unbounded-but-observable (per-topic progress
logged), while **cancellation retains its own §6.7 budget throughout**.
The per-phase durations are reported in the startup log:

```
session.get(schema_by_key(topic))
  .target(QueryTarget::All)
  .consolidation(ConsolidationMode::None)   // Auto→Latest hides same-key responders
  .timeout(T)                                // one absolute deadline across get + drain
```

- Reply validation is fail-closed with core's by-key decoder semantics
  (`schema_resolve.rs:344`): reply errors, malformed payloads, wrong kind,
  or claimed-hash/BFBS mismatch (recomputed SHA-256) → hard error.
- Valid reply with a different hash than planned → hard error (topic + both
  hashes).
- Cleanly drained, zero replies → unclaimed, proceed.
- Deadline before drain → inconclusive → hard error (operator retries with
  a larger `--preflight-timeout` or resolves the conflict). Never
  "unclaimed".

**Scope of the guarantee**: a point-in-time, best-effort snapshot — it
cannot see publishers whose queryables are disabled *in their process*, and
a conflicting publisher can register after the drain. The player narrows
(not closes) the race with a **second identical scan after its own
registrations** (executable because of the queryable gate above), expecting
exactly its own schema per topic; divergence → hard error and teardown.
The residual single-active-schema-provider requirement is operational (§3),
like the RPC single-provider rule. Core's `SchemaConflictAction` remains
the inner fence at registration.

All publishers are created before the matching barrier.

### 6.4 Startup barrier & delivery semantics

`Publisher::wait_for_matching(timeout)` concurrently across planned topics,
bounded by `--match-timeout` (default 5 s, 0 = skip), each wrapped in a
cancellation select (§6.7; the core API itself has no token). Unmatched
topics at deadline: reported, replay proceeds ("fire anyway"). The gate
proves ≥1 subscriber per topic, nothing more.

**Delivery**: publish success = enqueued. Core-default congestion (drop)
loses messages to slow subscribers/saturated links with no player-side
signal; summary counts are "published (enqueued)". Blocking congestion is
not offered in v1 (synchronous `.wait()` put would freeze the scheduler at
a non-await point); deferred behind a publish worker (§10).

### 6.5 Time base, ordering contract & scheduler

`log_time` is the scheduling base; `publish_time` unused in v1.

**Canonical file order (stream order)**: the order in which the §6.7
adapter's linear walk yields records — outer records in snapshot order;
within a chunk, the decompressed record stream's order. Prevalidation
already decompresses every retained chunk in pass 1, so this order is
available at no extra cost, for every input shape (chunked, unchunked,
summary-less, truncated), and both passes recompute it identically.
**Message indexes are not used for ordering**: they are channel-grouped
per chunk with chunk-relative offsets (§2) — a global merge on bare inner
offsets is wrong, and a correct cross-channel merge would need unbounded
aggregate index memory; the stream makes both problems moot. Required
fixture: two chunks with non-monotone `log_time` across the chunk
boundary, plus deliberately misleading/duplicated index contents — the
timeline must follow the stream and ignore the indexes.

**Ordering contract**:

1. **Effective timeline first**: over the entire ordered segment set in
   canonical file order, all channels, before filtering:
   `effective_time(m) = max(m.log_time, effective_time(prev))` — a
   streaming `previous_max` accumulator (O(1) memory) over the §6.5
   canonical record stream. Clamp events counted.
2. **Windowing second**: select `effective_time ∈ [t_first + S, t_first +
   S + D]` (checked arithmetic, §6.12).
3. **Topic filtering last**: decides publishing only; filtered-out messages
   still feed step 1 (pacing of the remainder is filter-independent).

Worked example (required test): file-order `log_time = 100, 90, 95` →
effective `100, 100, 100`, replayed back-to-back.

Cost note (documented): pass 1 decompresses and walks every retained
chunk — that is simultaneously the CRC verification and the timeline
source, so there is no cheaper "metadata-only" mode; CPU is O(input
bytes) per pass even under narrow filters; memory stays O(byte budget)
(§6.7).

**Scheduler** — absolute targets:

```
t0        = effective_time of first selected message
wall0     = Instant::now() at replay start (after the matching barrier)
target(m) = wall0 + (effective_time(m) - t0) / rate     // checked
```

`tokio::time::sleep_until(target)`, publish; if `now > target +
late_threshold` (10 ms default) publish immediately and count *late*. No
catch-up skipping. Empty selection → zero-count `Completed`, no publishers
(§6.1); `--loop` + empty exits after one pass. Loop re-derives `wall0`
per iteration.

Testability: timeline pass and `target_for` are pure functions over an
injected clock; mock-clock unit tests, no sleeps.

### 6.6 Error & termination contract

```rust
pub struct RunOutcome { pub summary: RunSummary, pub termination: Termination }
pub enum Termination {
    Completed,
    CompletedWithPublishErrors,
    CompletedWithTeardownTimeout,   // replay finished; Node teardown did not
    Interrupted,
}

pub async fn run(config: PlayerConfig) -> Result<RunOutcome, PlayerError>;
pub async fn run_with_shutdown(config: PlayerConfig, shutdown: CancellationToken)
    -> Result<RunOutcome, PlayerError>;

/// Erratum (impl review round 2): the error type is the typed `PlayerError`
/// rather than `anyhow::Error`, because the §6.6 mid-replay contract requires
/// the summary-so-far to reach the wrappers on `Err`. `PlayerError` carries
/// `summary: RunSummary` (whatever was accumulated before the failure) plus
/// the source error; wrappers print the summary to stderr before the error
/// and exit 1. `impl std::error::Error for PlayerError` keeps `?`/anyhow
/// interop for embedding callers.
```

No `process::exit`, no signal handlers in the library. Wrapper exit-code
mapping: `Completed` → 0; `CompletedWithPublishErrors`,
`CompletedWithTeardownTimeout`, `Err(_)` → 1; `Interrupted` → 130.

**Termination precedence (total order)**: `Err` > `Interrupted` >
`CompletedWithTeardownTimeout` > `CompletedWithPublishErrors` >
`Completed`. A completed run with *both* publish errors and a teardown
timeout therefore reports `CompletedWithTeardownTimeout` (the
fresher/rarer failure); the publish-error count is always in `RunSummary`
regardless of the enum, as is the teardown report (stuck stage + residual
counts) whenever the deadline expired — including under `Interrupted`/
`Err`. The combined case is a required unit test (§7.4).

- **Startup errors** (§6.1–§6.3 validation, snapshot identity change,
  preflight failure, queryable gate, publisher creation, conflict Reject):
  `Err` before any publish.
- **Mid-replay errors** (mid-file corruption, snapshot inconsistency): `Err`
  after printing the summary-so-far to stderr. The verified torn tail of a
  `truncated` file is not an error.
- **Publish errors**: counted, rate-limit-logged, continue →
  `CompletedWithPublishErrors`.

### 6.7 Threads, cancellation, resource bounds & bounded shutdown

**Reader adapter**: both passes drive `mcap::sans_io::LinearReader` — not
`MessageStream` — with `prevalidate_chunk_crcs`, `validate_data_section_crc`
and `record_length_limit = --max-chunk-bytes`, wrapped in a player-owned
adapter that supplies what the reader's public API does not (§2):

1. **Physical framing tracker**: the adapter counts bytes supplied/consumed
   and parses outer record envelopes (opcode + length) itself, so it always
   knows each outer record's/chunk's start and end offset — this is what
   defines the §6.1 safe prefix and torn-tail classification (the reader's
   own parser state is private).
2. **`ReadRequest` cap**: every `ReadRequest(n)` is validated **before**
   `insert(n)`. `n` exceeding the applicable envelope/record/chunk cap
   (`--max-chunk-bytes` + fixed header slack) → hard `Bound` error with no
   allocation — this closes the pre-limit allocation path
   (attacker-controlled compression-string/record lengths reach `insert()`
   before mcap's own checks). `n` exceeding the **remaining snapshot bytes**
   is *not* an error: the adapter supplies `min(n, remaining)` — never
   allocating beyond what exists — and signals exhaustion via
   `notify_read(0)`, at which point §6.1's torn-tail classification applies
   (a torn tail *always* requests bytes that do not remain; treating that as
   fatal would contradict the recovery contract). Declared-size validation
   runs **before** torn-tail classification (§6.1 bounds order), so an
   oversize declared length in a tiny file is a `Bound` error, never
   reclassified as a recoverable tail.
3. **CRC-independent size checks**: outer chunk length, compression-string
   length, and declared compressed/uncompressed sizes are validated by the
   adapter from the chunk header regardless of CRC presence (mcap's own
   size checks live inside the CRC-gated prevalidation branch, §2).
4. **Index handling**: summary and message-index records are
   length-validated and **skipped** — ordering and counts come from the
   prevalidated record stream (§6.5), so no index containers are ever
   retained or merged.
5. **CRC-zero comparison**: where a stored chunk/DataEnd CRC is zero, the
   adapter computes the checksum itself (chunk fully decompressed + hashed
   before any yield; a run-long data-section hasher for DataEnd) and
   requires calculated == stored (§6.1). If the computation is realized by
   probing mcap with a patched nonzero sentinel value, a *successful*
   validation against the sentinel means calculated == sentinel ≠ 0 and is
   therefore a **mismatch → hard error** — the collision case must fail
   closed on both the chunk and DataEnd paths (regression fixture:
   engineered stored-0/calculated-sentinel content).
6. **Walk termination**: semantic record delivery ends at `DataEnd` (which
   mcap emits only after freezing and comparing the data-section hasher);
   the post-`DataEnd` summary/footer region is outer-framed for bounds and
   clean-end classification but never fed to the reader or parsed
   (consistent with items 4 and §6.5).

Each pass runs on a dedicated OS thread under the recorder writer-thread
contract (`writer_thread.rs`): bounded queue, sliced sends checking an
atomic stop flag, terminal `ReaderEvent::Error`/`Eof`, receiver-close as a
second unblocking path, join with deadline, detach + loud warning on join
timeout.

**Queue & memory invariant**: the pass-2 queue admits up to 1024 messages
*or* `--buffer-bytes` of payload, whichever binds first. CLI enforces
`--max-message-bytes ≤ --buffer-bytes` (§6.12), so every accepted message
can be enqueued — no impossible-admission case. Peak memory is bounded by:
snapshot copy buffer (O(block)) + the reader's **raw input buffer**
(≤ `--max-chunk-bytes` + header slack, enforced by the `ReadRequest` cap;
its capacity persists across records) + one decompressed chunk
(≤ `--max-chunk-bytes`; raw and decompressed buffers coexist during
prevalidation) + transient per-record parse allocations
(≤ `record_length_limit` each; index/summary records are skipped, never
parsed into containers) + **during zero-CRC direct verification** the
verifier's own working set (its fixed hash read buffer + the compression
decoder's input buffer — zstd `DCtx::in_size()`-sized, lz4 32 KiB; erratum,
impl review round 3: these coexist with the reader/parser state and must be
stated, not borrowed from other terms) + the current
owned message (≤ `--max-message-bytes`) + queued payloads
(≤ `--buffer-bytes`) + retained planning state (channels + schemas +
per-segment scalars under the §6.1 checked 64 MiB budget; chunk metadata
and index records are streamed/skipped, never retained — no `O(chunks)`
term survives). Every term is a checked numeric bound; nothing in the
formula grows with input length. Oversize declared sizes fail **before the
adapter passes bytes to the reader**; an oversize message is rejected
**before the owned-message copy** (the raw envelope necessarily transits
the capped reader buffer first — that cap, not the message limit, is the
pre-allocation guard).

**Cancellation (first-signal contract)**: the `shutdown` token is observed
at every player-controlled decision point:

- pass-0 copy runs on a managed OS thread (same stop/join/detach protocol);
  the async owner selects the token independently of any blocking syscall —
  a wedged filesystem call is detached and reported, not awaited;
- Node/session and publisher construction — **managed ownership, not
  future-drop "rollback"** (dropping a construction future is not a
  rollback: before `Ok(node)` there is no `Node` to `close()`, and a
  publisher-creation future may already have emitted by-hash/by-key schema
  *puts* that nothing retracts — `Node::close()` stops queryables and the
  session but issues no compensating deletes). Each construction future
  runs on a spawned task that **retains ownership to completion**; the
  player token-selects against the task handle. On cancellation: the task
  keeps running in the background, bounded by the teardown deadline — if it
  completes in time, the returned resource is closed properly
  (`Node::close()` / publisher drop) and normal teardown reporting applies;
  if the deadline expires first, the task is detached, the stuck
  construction stage goes into the residual report, `AORTA_GROUP` is not
  restored, and the process is Aorta-tainted (§6.9). **Documented
  residual**: schema puts already emitted by a cancelled registration are
  network-visible (and storage-retained where a compatible storage
  subscribes) and are *not* rolled back — the second scan is skipped after
  cancellation and the residual is listed in the summary. A
  cancellation-safe transactional registration API is a core follow-up
  (§10). Cancellation tests must observe by-hash/by-key network state, not
  merely the absence of a returned `Publisher`;
- pass-1/pass-2 threads: stop flag + receiver close (sliced sends bound the
  in-thread latency);
- preflight: `select!` around the get and every reply-drain await;
- matching barrier: token-select around each `wait_for_matching` (the core
  API has no token of its own);
- scheduler: the token is **checked explicitly on every iteration**,
  including runs of already-due/clamped messages where `sleep_until` is
  immediately ready and never yields.

**Budget (stated honestly)**: cancellation is detected within ≤ 1 s at
every point above, and teardown adds its ≤ 10 s deadline. Two exclusions:

- *Wedged blocking syscall on a managed thread* (copy/reader): the owning
  async side is not blocked — it detaches the thread at the deadline and
  returns with the residual report (detach-and-return holds).
- *In-progress synchronous publish*: `Publisher::publish` runs Zenoh's
  `.wait()` **on the scheduler's own call stack** (`transport.rs:53`, even
  under Drop congestion) — v1 has no publish worker (deferred, §10), so
  while a publish is wedged the library **cannot** observe the token, start
  teardown, or return; first-signal library return is **unbounded in this
  one state**. The only bound is the wrapper's **scheduler-independent
  signal owner** (§6.7 lifecycle below). G7 is narrowed accordingly; there
  is no detach-and-return claim for this case — the stalled-publish proof
  is a §7.5 child-process test asserting the forced-exit path. (A wedged
  put under Drop
  congestion indicates a broken transport, not backpressure.)

Tests assert a generous end-to-end bound on the covered paths and
fault-inject both exclusions.

**Bounded shutdown**: the entire teardown — reader-thread stops/joins,
publisher drop, `Node::close()` — runs under one absolute deadline (default
10 s) because `Node::close()` is not internally bounded (§2). On expiry:
record the stuck stage + residual counts in the summary, return with the
§6.6 termination rules, and treat the process as **Aorta-tainted**: the
library documents that after a teardown timeout no further Node/player
construction is supported in this process and `AORTA_GROUP` is left as-is
(live core tasks may still read it); an embedding caller must exit. Both
wrappers exit immediately after printing the outcome, which reclaims
detached threads/tasks.

**Lifecycle**: normal completion → `Eof` + last publish → teardown →
`RunOutcome`. Cancellation → per-phase observation above → teardown →
`Interrupted`.

**Signal owner (wrappers; scheduler-independent AND coalescing-proof)**:
each wrapper (`aorta-play` main and `aorta-cli play`) installs its signal
handling **before entering the engine**, via one shared helper, using
`signal-hook` — *not* `tokio::signal::ctrl_c()` (once Tokio's Ctrl-C
future is first polled it installs a process-wide handler that permanently
replaces Unix's default SIGINT-terminates behavior, and a runtime wedged
inside a synchronous `.wait()` may never poll a signal task again).

The forced-exit decision lives **in the OS signal handler itself, not in
any iterator thread**, because a signal iterator is not a reliable
occurrence counter: `signal-hook`'s delivery state is a single flag
(`SignalOnly` exfiltrator — one `AtomicBool`), so two close-spaced SIGINTs
can coalesce into one iterator event and an iterator-local first/second
counter would silently lose the only finite escape. Registration order
(the crate's documented conditional-shutdown pattern):

1. `register_conditional_shutdown(SIGINT, 130, armed_flag)` with
   `armed_flag` initially false — when a SIGINT arrives with the flag
   already set, the handler terminates via the **bare `_exit(130)` path**
   (`signal_hook::low_level::exit`; `std::process::exit` is prohibited for
   this path — it runs `atexit`/exit handlers that may synchronize with
   already-wedged threads, unbounding the "hard" bound);
2. register the action that **arms** `armed_flag` (first delivery arms;
   any subsequent delivery exits inside the handler, regardless of thread
   or runtime state).

A dedicated iterator thread remains, but **only** to translate the first
delivery into the thread-safe `CancellationToken::cancel()` for graceful
teardown — it plays no role in forced exit. `130` is the documented
**direct-exit code** for the forced path, defined here rather than via the
§6.6 `RunOutcome` mapping (which cannot execute while the library is
wedged). Relying on Unix's default second-SIGINT behavior after any Tokio
signal listener has been installed is explicitly prohibited.

### 6.8 Time semantics (three timelines)

1. Embedded application stamps: recorded, untouched (no rescaling).
2. Pacing: effective-timeline deltas ÷ `--rate`.
3. Zenoh sample timestamp: fresh.

Reconciling 1 and 2 is sim time, out of v1 by design.

### 6.9 Group handling & aorta-cli wiring

Default: `aorta.group` from `aorta.recorder.start` (fallback: first channel
key); precedence `--group` flag > recorded metadata; ambient `AORTA_GROUP`
is not a source (info line when it differs).

**Process-ownership & restoration contract**: `run_with_shutdown` owns
`AORTA_GROUP` process-wide from before Node construction until Node close
completes. The prior value is saved and **restored on every return path
where teardown completed** (success, startup error, mid-replay error,
cancellation). If the teardown deadline expired, the variable is **not**
restored (live core tasks may still read it) and the process is
Aorta-tainted per §6.7. Concurrent player runs or concurrent Node
construction in one process are unsupported (documented; save/restore
cannot make concurrent mutation safe). Making group an explicit Node input
is a core follow-up (§10).

**Provenance (aorta-cli)**: capture `explicit_global_group =
cli.group.clone()` **before** the existing flag→env merge (`main.rs:538`);
`play` consults only the explicit value. Tests: explicit-flag /
ambient-env-only / recorded-metadata-only triples. `--zenoh-config` follows
the `record` precedent.

Filters match recorded keys, before remap. Player node name:
`aorta_play_<pid>`.

### 6.10 Skipped-channel policy

`ctx/**`, `sys/telemetry/**`, `sys/record/srv/**` are excluded (ownership
semantics / meaningless without a live server). One summary line:
`skipped N non-pub channels (ctx: a, sys: b) — v1 replays pub/** only`.

### 6.11 What subscribers observe

- Verbatim payload bytes; where the schema embeds an `AortaHeader`,
  subscribers with `enable_header_context` see recorded
  `publisher_node`/`sequence`.
- Coexistence: replay + live original publisher = two writers on one key
  (schema divergence caught by preflight); loop mode repeats sequences
  (dedup keyed on (publisher, sequence) drops repeats). Inherent to
  verbatim replay.
- No per-message schema attachments (per-publisher `Never`).
- Fresh Zenoh sample timestamps.

### 6.12 Numeric & cross-input validation (checked, reject-not-saturate)

- `--rate`: finite, `0 < rate ≤ 1e6`.
- `--start`, `--duration`, `--match-timeout`: finite, `≥ 0`, representable
  — checked float→`Duration`/ns conversion; checked `t_first + start
  (+ duration)` (u64 ns) and `wall0 + Δ/rate`.
- `--preflight-timeout`: finite, **`> 0`**, representable (zero is
  rejected: under the §6.3 deadline-before-drain rule it would make every
  typed preflight immediately inconclusive; there is no "disable" mode —
  disabling the preflight would waive the split-brain guard).
- `--buffer-bytes`, `--max-message-bytes`, `--max-chunk-bytes`: `> 0`;
  **`--max-message-bytes ≤ --buffer-bytes`** (hard CLI error otherwise).
- Any overflow/unrepresentable value → CLI error; never saturate, wrap, or
  panic. Boundary tests at largest-accepted / first-rejected per input.

## 7. Testing plan (TDD)

Fixtures built with the `mcap` crate directly (recorder test helpers are
private). E2E uses the real recorder binary. Crash fixtures truncate real
recorder output at multiple positions (inside compressed chunks, inside
uncompressed chunks, between records) and strip the footer; corruption
fixtures flip bytes inside fully-framed chunks and in the data-section CRC.

1. **Unit — timeline & scheduler**: `100,90,95` example; window-after-clamp;
   filter-independence of clamp state; canonical record-stream order (two
   chunks, non-monotone cross-chunk `log_time`, misleading/duplicated index
   contents ignored — §6.5);
   rates {0.5, 1, 2}; late detection; empty selection; §6.12 boundary
   matrix incl. `max-message-bytes > buffer-bytes` rejection.
2. **Unit — planning/validation**: channel classification incl. mixed
   raw+typed → error; queryable-gate: typed plan + env-disabled queryables
   → error before publisher creation, raw-only plan proceeds; publish plan
   restricted to finally-publishable topics (excluded topic with a
   conflicting live schema neither preflights nor aborts; out-of-window
   topic creates no publisher); schema-hash integrity; format validation;
   multi-schema × {strict, latest} (last-`log_time` from the record
   stream, chunked and unchunked inputs);
   summary-less fallback.
3. **Unit — segment sets & recovery**: contiguity/gaps/duplicates/
   `--allow-gaps`; >1 id-less file; mixed recording_id/group;
   truncated-interior-segment; verified prefix: torn unchunked record, torn
   compressed chunk, torn uncompressed chunk — **zero messages from any
   unverified chunk**; fully-framed bad-nonzero-CRC chunk → hard error; bad
   data-section CRC → hard error; **CRC-zero matrix (stored × calculated)**:
   stored zero + calculated zero → accepted (chunk and DataEnd variants,
   content engineered so the CRC32 genuinely calculates to zero); stored
   zero + calculated nonzero (corrupt body) → error; stored zero + oversize
   declared size → error before decompression (size checks are independent
   of the CRC branch); patching a nonzero field to zero is additionally
   covered as calculated-mismatch → error; torn compressed chunk →
   discarded-message count reported `unknown`; adapter bounds: malicious
   compression-string length,
   `ReadRequest` exceeding the cap → error **before** `insert()`;
   `ReadRequest` exceeding remaining snapshot → bounded short supply
   (`min(n, remaining)`, no over-allocation) then EOF/torn-tail
   classification, simultaneous max-compressed + max-decompressed buffer case
   stays within the stated peak, index/summary records length-validated and
   skipped (misleading index contents never affect ordering — §6.5
   fixture); planning-budget: many tiny chunks stay O(1) retained (streamed
   chunk metadata), channel/schema flood → budget exceeded → hard error;
   oversize declared chunk/record → error before reader allocation;
   oversize message → error before owned-message copy.
4. **Integration (cargo, single process)**: fixture → `run_with_shutdown` →
   in-process subscriber → byte-identical payloads, order, coarse pacing
   (generous bounds only, per the defiant-drain flake rule); summary counts;
   outcome mapping incl. `CompletedWithTeardownTimeout` (fault-injected
   stuck close via a held resource, if injectable — else covered by the
   teardown-report unit test); preflight: two same-key responders with
   different schemas → error even where consolidation would hide one;
   responder appearing during preflight → second-scan divergence → error;
   deadline-before-drain → error; env-disabled queryables → error;
   `AORTA_GROUP` restoration on success/error/cancel paths; combined
   publish-errors + teardown-timeout → `CompletedWithTeardownTimeout` with
   publish-error count in the summary (§6.6 precedence); cancellation
   during: snapshot copy (incl. fault-injected blocked copy → thread
   detached, owner returns), Node/session construction (stalled open →
   spawned task detached at deadline, residual stage reported, taint),
   pass-1 scan, preflight get, preflight drain, mid-publisher-creation
   (stalled declaration → task completes-late-then-closed OR detached at
   deadline; **by-hash/by-key network schema state observed and asserted as
   the documented un-rolled-back residual**), matching, queue backpressure
   (byte budget exhausted), scheduler over a long run of
   same-effective-time already-due messages — each returns within the
   stated generous bound (the stalled-publish forced-exit proof lives in
   §7.5 as a child-process test — a same-process forced-exit (`_exit(130)`) assertion
   would kill the harness); startup phase arithmetic: both scans driven to
   their deadlines + publisher-delay injection (observability of
   `T + P + T + M`), `--preflight-timeout 0` rejected at CLI.
5. **E2E (bazel, multi-process)**: record → play → verify via subscriber
   process (shared exported zenoh config). Variants: `--no-summary`; group
   precedence triple; live non-rotating recorder still writing the input →
   player refuses at snapshot verification; **forced-exit signal tests**
   (child process, one per wrapper — `aorta-play` and `aorta-cli play`,
   since each owns its wiring via the shared helper): deterministic
   wedged-publish injection → first SIGINT → assert still running (library
   wedged as documented) → second SIGINT → assert process exit with
   **exact code 130 within a finite wall-clock bound**; plus a
   **two-handler-delivery variant** proving the decision sits in the OS
   handler (§6.7): deterministically prevent the iterator thread from
   draining (test hook), deliver the first SIGINT, wait for a test-visible
   acknowledgement that the handler has **armed** the shutdown flag, then
   deliver the second SIGINT → exit 130. (Two unsynchronized back-to-back
   SIGINTs are *not* a valid proof: POSIX permits a second occurrence of an
   already-pending standard signal to coalesce before the first handler
   invocation — no handler can count a delivery the kernel never made.)
6. Gates: `cargo clippy --all-targets`, `cargo fmt --check`,
   `bazel test //tools/...`.

## 8. Deliverables

Single PR (self-contained, no core changes): `tools/aorta-play` (lib + bin
+ BUILD.bazel + tests); `aorta-cli play` (incl. §6.9 provenance capture);
docs (this proposal finalized; CLI docs).

## 9. Dissent log

Round 3 accepted the torn-tail direction (warn + verified prefix) and round
4 re-confirmed it ("the policy is sound on its merits") while pinning the
mechanics to prevalidated chunk CRCs over an immutable snapshot — §6.1/§6.7
now specify exactly that. No open dissents.

## 10. Deferred / open items (v2+)

| Item | Note |
|---|---|
| Sim time (`Clock::Simulation`) | Out of v1 by design (§6.8) |
| ctx/** replay | Ownership-aware design needed |
| Mixed raw+typed topic replay | Per-channel discovery identity or key remap |
| Typed replay with disabled queryables | Currently fail-closed (§6.3) |
| Blocking congestion (`block`) | Publish worker + interrupt budget |
| Global schema arbitration | Ownership/lease protocol |
| Bounded `Node::close()` | Core follow-up; outer deadline meanwhile |
| Group as explicit Node input | Core follow-up; removes §6.9 env contract |
| Header rewrite | Requires re-encode |
| Pause/step/seek; topic remap; glob filters | On demand |
| `--use-publish-time` | Alternate time base |
| Delivery accounting | Needs transport-level acks |
| Cross-recording interleave | Currently hard error |

## 11. Review round 1 → resolution map

| Finding | Resolution |
|---|---|
| 1 summary optional | §6.1 linear-scan fallback |
| 2 log_time not monotonic | §6.5 clamp + ordering contract |
| 3 multi-schema per topic | §6.3 topic kinds + `--schema-policy` |
| 4 matching API exists | §6.4 |
| 5 enqueue ≠ delivered | §6.4 |
| 6 no error contract | §6.6 |
| 7 hash/bfbs integrity | §6.2 |
| 8 live schema conflict | §6.3 preflight (rounds 2–4) |
| 9 lifecycle undefined | §6.7 |
| 10 sync reader starves async | §6.7 |
| 11 queryable env-gated | §6.3 queryable gate (round 4 hardened) |
| 12 three timelines | §6.8 |
| 13 live-publisher/loop collision | §6.11 |
| 14 segment-set validation | §6.1 |
| 15 key metadata consistency | §6.1 |
| 16 empty selection | §6.5/§6.1 |
| 17 private test helpers | §7 |
| 18 aorta-cli wiring | §6.9 |
| 19 AortaHeader optional | §6.11 |
| 20 schema name fallback | §2 |

## 12. Review round 2 → resolution map

| Finding | Resolution |
|---|---|
| 1 Warn ≠ convergence | §6.3 |
| 2 publisher forges identity | §6.3 |
| 3 `block` breaks lifecycle | §6.4/§10 |
| 4 reader protocol | §6.7 |
| 5 termination contract | §6.6 |
| 6 clamp/window order | §6.5 |
| 7 cli group provenance | §6.9 |
| 8 crash = missing footer | §6.1 |
| 9 multiple id-less files | §6.1 |
| 10 per-channel last time | §6.1/§6.5 |
| 11 validation gaps | §6.1/§6.12 |

## 13. Review round 3 → resolution map

| Finding | Resolution |
|---|---|
| 1 preflight contract | §6.3 (T defined in round 5: `--preflight-timeout`) |
| 2 preflight ≠ convergence | §6.3 point-in-time + second scan + operational constraint |
| 3 raw+typed share key | §6.3 fail closed |
| 4 crash vs live ambiguity | §6.1 private-copy snapshot (round 5: no mmap) |
| 5 torn-chunk verification | §6.1/§6.7 prevalidated CRCs (round 5: LinearReader) |
| 6 plan before selection | §6.1 |
| 7 queue ≠ memory bound | §6.7 (round 5: full invariant) |
| 8 numeric representability | §6.12 |
| 9 env side effects | §6.3/§6.9 |
| 10 SIGINT ownership | §6.6/§6.7 |
| 11 `Node::close()` unbounded | §6.7 (round 5: distinct termination) |
| 12 stale anchors | §2 |

## 14. Review round 4 → resolution map

| Finding | Resolution |
|---|---|
| 1 (P1) mmap ≠ immutable snapshot; contradictory lifetimes | §6.1 pass-0 private stream-copy (unlinked fd), identity re-check after copy, one lifetime for both passes; no file-backed mmap (§4); mutation tests §7.5 |
| 2 (P1) `MessageStream` can't prevalidate | §6.7 both passes use `sans_io::LinearReader` with `prevalidate_chunk_crcs` + `validate_data_section_crc` + `record_length_limit`; framed bad-CRC chunk & bad DataEnd = hard errors (§6.1); fixtures §7.3 |
| 3 (P1) memory bound gaps; impossible admission | §6.12 `max-message-bytes ≤ buffer-bytes`; §6.7 peak-memory invariant incl. decompression buffer; `--max-chunk-bytes` wired to `record_length_limit`; declared-size checks pre-allocation; tests §7.1/§7.3/§7.4 |
| 4 (P1) cancellation only at scheduler | §6.7 per-phase token observation (copy blocks, pass-1 thread, preflight selects, publisher creation, matching, scheduler) + stated first-signal budget; tests §7.4 |
| 5 (P1) preflight `T` undefined; self-scan vs disabled queryables | §5/§6.3 `--preflight-timeout` (default 5 s, per-topic, concurrent, §6.12 bounds); §6.3 queryable gate: typed replay fails closed when player-local queryables are disabled (raw-only proceeds) |
| 6 (P1) bare index offsets ≠ file order | §6.5 composite `(chunk_start_offset, message_offset)` order + linear fallback; fixture §7.1 |
| 7 (P2) teardown timeout invisible; group restore undefined | §6.6 `CompletedWithTeardownTimeout` → exit 1 + teardown report; §6.9 restore-on-complete-teardown, taint-on-expiry (§6.7); tests §7.4 |

## 15. Review round 5 → resolution map

| Finding | Resolution |
|---|---|
| 1 (P1) CRC-zero bypasses prevalidation & size checks | §6.1 fail-closed zero policy (zero chunk CRC / zero DataEnd = hard error); §6.7 adapter size checks independent of CRC; §2 sentinel semantics documented; fixtures §7.3 |
| 2 (P1) reader seam: pre-limit allocation, private state, peak omissions | §6.7 adapter contract: framing/offset tracker, `ReadRequest` cap before `insert()`, CRC-independent header validation, streamed index handling; peak formula extended (raw buffer + transient parse allocations, raw+decompressed coexistence); message-limit wording narrowed to before-owned-copy; fixtures §7.3 |
| 3 (P1) first-signal budget not end-to-end | §6.7 copy on managed thread + async select; Node construction and each publisher-creation future token-selected (dropped-future state reclaimed by close, second scan skipped); scheduler checks token every iteration incl. already-due runs; wedged syscall + in-progress sync publish explicitly excluded and bounded via detach/taint + wrapper exit; fault-injection tests §7.4 |
| 4 (P1) startup bound arithmetically false; zero `T` undefined | §6.3 explicit `T + P + T + M` phase arithmetic, `P` unbounded-but-observable in v1; §5/§6.12 `--preflight-timeout` strictly positive, zero rejected; timing tests §7.4 |
| 5 (P2) torn-chunk message counts unknowable | §6.1/§5 discarded chunks exact, discarded messages `Option<u64>`/`unknown` for unindexed torn chunks; fixture §7.3 |
| 6 (P2) combined publish-error + teardown-timeout | §6.6 total precedence order; counts always in `RunSummary`; combined-case test §7.4 |

## 16. Review round 6 → resolution map

| Finding | Resolution |
|---|---|
| 1 (P1) zero is a valid calculated CRC32 | §6.1 recalculate-and-compare in the zero case (adapter hashes the chunk pre-yield / keeps a run-long data-section hasher) instead of rejecting; §2 sentinel-vs-value collision documented; calculated-zero fixtures §7.3 |
| 2 (P1) future-drop ≠ rollback | §6.7 managed ownership: construction futures run on spawned tasks retaining ownership; complete-then-close or detach-at-deadline + residual + taint; schema puts explicitly documented as not rolled back (no compensating delete in core; follow-up §10); network-state-observing tests §7.4 |
| 3 (P1) wedged sync publish can't detach itself | §6.7/§3 claim narrowed: publish runs on the scheduler stack, library return unbounded during a wedged `.wait()`, wrapper second-signal exit is the only bound; detach-and-return test replaced accordingly (§7.4); publish worker stays deferred (§10) |
| 4 (P1) unbounded planning/index aggregate state | §6.1 chunk metadata streamed (no `O(chunks)` retention), pass 2 recomputes timeline; retained channel/schema state under a checked 64 MiB budget; §6.5 ordering switched to the prevalidated record stream — indexes length-validated and skipped, no cross-channel merge exists; §6.7 peak formula updated; flood fixtures §7.3 |

## 17. Review round 7 → resolution map

| Finding | Resolution |
|---|---|
| 1 (P1) second-signal escape not scheduler-independent | §6.7 signal owner: dedicated OS signal thread per wrapper (shared helper), installed before the engine; `tokio::signal` explicitly prohibited (its handler permanently captures SIGINT and a wedged runtime may never poll it); first SIGINT → token, second SIGINT → `process::exit(130)` from the signal thread (direct-exit code defined outside the `RunOutcome` mapping); §7.5 child-process forced-exit tests for both wrappers |
| 2 (P2) stale indexed-order / CRC-fixture wording | G6, §5 summary, §6.7 peak formula, §7.1 test renamed to record-stream vocabulary; §7.3 CRC cases restated as an explicit stored×calculated matrix |

## 18. Review round 8 → resolution map

| Finding | Resolution |
|---|---|
| 1 (P1) signal iterator coalesces deliveries; `process::exit` runs exit handlers | §6.7 forced-exit decision moved into the OS handler via `signal-hook`'s `register_conditional_shutdown` (flag registered before the arming action, per the crate's documented order) with bare `_exit(130)` (`low_level::exit`); iterator thread retained only for first-signal token cancellation; `std::process::exit` prohibited on the forced path; §7.5 adds the close-spaced double-SIGINT child-process variant that defeats iterator-local counting |

## 19. Review round 9 → resolution map (re-gate)

| Finding | Resolution |
|---|---|
| Round-8 P1 closure | **CLOSED** — signal-owner design verified against signal-hook 0.3.18 (`flag.rs:148,166,173`, registry order, `low_level::exit` = `libc::_exit`) |
| 1 (P1) §6.7 vs §6.1 ReadRequest contradiction | Interpretation 1 ratified as errata: §6.7 item 2 = cap-only `Bound` error, remaining-shortfall = short supply + EOF/torn-tail; §7.3 case split |
| 2 (P2) back-to-back SIGINT test non-deterministic | §7.5 reworded: block iterator drain, first SIGINT, arm-acknowledgement, then second SIGINT (POSIX pending-signal coalescing noted) |
| 3 (P2) stale `process::exit` name in §7.4 | Replaced with `_exit(130)` wording |
| Interpretation 2 (size-check before tail classification) | Ratified; §6.7 item 2 now states the ordering explicitly |
| Interpretation 3 (walk ends at DataEnd) | Ratified; new §6.7 item 6 |
| Interpretation 4 (CRC-zero sentinel probe) | **Rejected as implemented**: DataEnd path accepts calculated == sentinel (0xFFFFFFFF) collision; spec unchanged (`calculated == stored` already forbids it) — §6.7 item 5 now spells out the fail-closed collision rule; implementation fix + regression fixture required (P2 follow-up) |
