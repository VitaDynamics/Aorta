# Flow TCK — event-record schema (v1, test-only contract)

Status: full Flow scenario matrix. v0 was the initial skeleton (one scenario).
This schema is a TEST-ONLY contract: it versions independently of every
shipped API and never crosses the C ABI. Changing it requires updating both
emitters and every expectation file in the same commit.

## Transport

Each scenario binary writes newline-delimited JSON records to stdout, one
record per observable event, in observation order. Nothing else may be
written to stdout (logs go to stderr). The process exit code is 0 unless the
scenario harness itself is broken — semantic divergence is the DRIVER's
verdict, not the emitter's.

## Record fields

Required on every record:

| field | type | meaning |
|---|---|---|
| `scenario` | string | scenario id, e.g. `"cancel_reason_observation"` |
| `seq` | integer | emitter-assigned observation order, 0-based, dense |
| `op` | string | the operation observed, from the op vocabulary below |
| `event` | string | one of `start`, `value`, `stopped`, `fault`, `counter`, `lifecycle` |

Event-specific fields:

- `event == "value"`: `value` (string form of the delivered value — emitters
  stringify identically, e.g. `"7"`, `"moved:motor-frame"`).
- `event == "stopped"`: `kind` ∈ `cancelled` | `deadline` | `fault`;
  for cancellations additionally `reason` ∈ `explicit` | `deadline` |
  `shutdown` | `child_failed` | `unknown` and `raw_reason` (integer, the
  exact wire byte — REQUIRED even when `reason` is a known name);
  for faults additionally `code` (integer, stable `aorta_result_t`) and
  `message` (exact text).
- `event == "fault"`: `code`, `message` — used when a fault is observed
  outside a stopped-outcome (e.g. supervision notification).
- `event == "counter"`: `name`, `value` (integer) — admission/refusal/
  deadline-drop/detached counts.
- `event == "lifecycle"`: `phase` — free-form ordered lifecycle marker from
  the scenario script (e.g. `"parent_cancelled"`, `"child_observed_cancel"`).

## Op vocabulary (extend per scenario, same string both planes)

`task.spawn`, `task.await`, `task.body`, `task.take_outcome`,
`scope.cancel`, `scope.close`, `scope.shutdown_grace`, `scope.stats`,
`queue.stats`, `service.stats`, `when_all`, `when_any`, `recv`, `call`,
`action.send_goal`, `action.wait_result`, `timer.sleep`,
`budget.checkpoint`, `supervision.restart`.

Counter records under `queue.stats`/`service.stats` use UNIFIED logical
names (`admission_refused`, `deadline_dropped`,
`request_dropped_deadline`) — each plane derives the value from its
OBSERVED native counter (Rust event-queue `AdmissionStats` / service
stats vs C++ lane-pool `LaneStats`); the tier difference is a
qualified, manifest-recorded consequence of the differentiated
scheduler, while the records themselves stay single-file comparable.

## Scenario manifest (`scenarios.json`)

`test/flow_tck/scenarios.json` is the runner's source of truth. Each entry:

- `id` — scenario name (expectation file(s) under `expected/`);
- `family` — the scenario family it covers;
- `planes` — which emitters run. A single-plane row is a plane-qualified
  family: the entry MUST carry a `qualification` string that states the
  qualified cross-plane difference. The runner prints the
  qualification whenever it skips a plane.
- `runs` (optional) — multiple invocations of the SAME plane that must all
  match the one expectation (e.g. the C++ lanes-vs-rollback env variants);
- `wave` — delivery wave; the runner SKIPs (loudly, with a count) waves
  above its current implementation level rather than failing.

## Emission rules (v1)

- Emitters MUST serialize record emission: one mutex (or equivalent)
  around "assign seq + write line". A plain global counter races once
  task bodies emit from runtime threads.
- Every semantic field (`reason`, `raw_reason`, `code`, `message`,
  `value`) MUST be derived from the OBSERVED object (`Outcome`/`Stopped`/
  counter read) — never from a constant that happens to match the
  expectation. A hardcoded emission makes the TCK vacuous.
- When a record emitted by a task body must precede a dependent
  main-thread record (e.g. `child_observed_cancel` before the parent's
  `stopped`), the emitter synchronizes (atomic flag / channel) — the
  ORDER in the expectation file is the contract, and relying on timing
  is a harness bug.

## Normalization (driver-side, before comparison)

- drop fields: `thread`, `ts_ns` (emitters MAY emit them for debugging);
- `seq` is compared as ORDER, not value (re-densified after filtering);
- records with `op` prefixed `debug.` are dropped.

## Plane-qualified expectations

An expectation file is either `expected/<scenario>.json` (both planes must
match it) or the PAIR `expected/<scenario>.rust.json` +
`expected/<scenario>.cpp.json` — the paired form is legal ONLY for an
explicitly qualified cross-plane difference, and the driver prints the
qualification note when it applies the paired form. Silent divergence
(records differing under the single-file form) is a hard failure.

## Baseline-divergence scenario (RESOLVED)

`cancel_reason_observation`: parent scope cancels a parked child with
`Shutdown`; the child's awaiter observes the stop and the consumer emits the
stopped record.

- Rust: `kind=cancelled, reason=shutdown, raw_reason=3` — full fidelity.
- C++: the C ABI's completion record and `Outcome<T>`/`Stopped`
  (`cpp/include/aorta/flow.hpp`) together preserve the exact wire-level
  `CancelReason` end to end, same as Rust: `kind=cancelled, reason=shutdown,
  raw_reason=3`. The scenario now uses the single-file expectation form
  (`expected/cancel_reason_observation.json`) — the original documented
  divergence (`reason=unknown, raw_reason=0` on the C++ plane) was the TCK's
  red evidence for that gap and no longer applies. The paired-form rule
  above still governs future scenarios that carry a genuine, explicitly qualified
  cross-plane difference.
