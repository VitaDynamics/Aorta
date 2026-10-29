# Aorta Flow — Concepts Guide (Shared Across Rust / C++)

> **Audience:** every Aorta SDK user — including programmers who have never
> touched coroutines, async/await, or "structured concurrency". Read this
> once, top to bottom, and the per-language chapters
> ([`rust/README.md` §15](../rust/README.md#15-aorta-flow-structured-concurrency), [`cpp/README.md` §17](../cpp/README.md#17-aorta-flow-c20-coroutines))
> will read as "how do I spell this", not "what is this".
>
> **Contracts:** this guide states the Flow contracts the Rust and C++ SDKs
> implement and teaches them; the per-language chapters give the exact
> spellings. If a chapter and this guide ever disagree, please file the
> discrepancy.

---

## 1. What problem does Flow solve?

Robot software is full of *concurrent activities*: a camera pipeline, a
planner request, a motion command that takes seconds to finish, a battery
poller. Before Flow, Aorta (like ROS 2) gave you **callbacks** — the executor
calls your function when a message arrives — and everything beyond that was
your problem. In practice, four problems kept reappearing in real robot code:

1. **Waiting blocks a whole thread.** "Send a goal, wait for the result" was
   either a blocking call (which freezes the executor thread that every other
   callback needs) or a hand-rolled polling loop ("check every 200 ms"),
   which wastes latency and CPU.
2. **Stopping things is ad-hoc.** Cancelling a navigation goal meant setting
   your own `AtomicBool`, hoping every loop remembered to check it, and
   having no idea when everything actually stopped.
3. **Orphaned work.** A function starts three background jobs and returns
   early on an error — who stops the three jobs? Usually nobody: they keep
   running, holding resources, publishing stale data.
4. **Deadlines don't propagate.** "This whole request must finish in 500 ms"
   had no carrier: each sub-step had its own unrelated timeout, and the sum
   routinely exceeded the intent.

Flow is Aorta's answer: a small set of primitives — **Scope, CancelToken,
Budget, Task, Watch, capabilities, readiness** — that make waiting cheap,
stopping reliable, ownership of work explicit, and deadlines something that
flows through calls instead of something you re-invent per call site.

**What Flow is NOT:** it is not a DAG/pipeline engine, not a distributed
orchestrator, and not a replacement for the callback API. Everything you have
works unchanged; Flow is additive (you can adopt it one function at a time).

---

## 2. A five-minute primer: coroutines and async/await

*Skip this section if you already write tokio/asio code.*

### 2.1 Functions that can pause

A normal function runs from its first line to `return`, occupying its thread
the entire time. A **coroutine** (Rust: `async fn`; C++20: a function using
`co_await`) is a function that can **pause itself** at marked points and
**resume later**, possibly on a different thread. The marked points are
`await` (Rust) / `co_await` (C++).

```text
normal function:     [====== runs, blocking a thread ======]
coroutine:           [==run==]--pause--[==run==]--pause--[==run==]
                              ^ thread is FREE here to run other work
```

The magic is what happens at a pause: the coroutine's local variables are
saved into a small heap object (its **frame**), the thread walks away and
does other work, and when the awaited thing (a reply, a timer, a message) is
ready, an **executor** picks the frame up and continues it. Nothing spins,
nothing sleeps; a paused coroutine costs *zero CPU*.

Two mental-model corrections that save beginners a lot of confusion:

- `await` does **not** mean "block here". It means "if not ready, give the
  thread back; wake me when it is."
- A coroutine does **not** run on its own thread. Many thousands of paused
  coroutines can share one executor thread, because only the *running* one
  occupies it.

### 2.2 Why robots should care

The blocking pattern that starves an executor:

```text
callback for /goal:            (executor thread #1)
    result = action.wait_result()   // blocks thread #1 for 8 seconds
                                    // → every other callback on this
                                    //   executor is FROZEN for 8 seconds
```

The same logic as a coroutine:

```text
task for /goal:                (executor thread #1)
    result = call(...).await        // pauses; thread #1 immediately
                                    //   continues serving other callbacks;
                                    //   this task resumes when the reply
                                    //   arrives
```

Same shape of code, no starvation, no polling loop, no callback pyramid.

### 2.3 The cost: cooperation

A coroutine only pauses at `await` points. Between them it runs like a
normal function — so a CPU-heavy loop with no awaits hogs its executor
thread exactly like a blocking call would. Flow gives you two tools for
that: `offload` (run the heavy closure on a dedicated thread pool and await
its result — R4 in §7) and `checkpoint()` (voluntarily observe cancellation —
§4.3). "Cooperative" is the key word throughout this guide: pausing,
stopping, and deadlines all work because your code touches the well-marked
points; none of them preempt you mid-instruction.

---

## 3. Structured concurrency: work forms a tree

### 3.1 The idea

"Structured programming" replaced `goto` with blocks: control flow enters a
block at the top and leaves at the bottom, so you can reason locally.
**Structured concurrency** applies the same discipline to concurrent work:

> Every task belongs to a **scope**. Closing a scope waits for every one
> of its tasks to end — work never outlives a scope you `close()`. The
> guarantee is tied to *closing*: dropping your last handle without
> closing prints a loud warning instead of silently orphaning (§3.2).

In Flow the scope is a first-class object, `Scope`, and scopes form a
**tree** (a scope can create child scopes). The tree gives three system-wide
guarantees:

1. **No orphan tasks** — closing a scope waits for its children
   (rule 1).
2. **Cancellation cascades downward** — cancelling a scope cancels every
   descendant scope and task, automatically (rule 2).
3. **Deadlines only narrow** — a child's time budget can never exceed its
   parent's remaining budget (rule 4).

```text
node root scope                        ← cancelled when the node closes
 ├─ scope "sensor-pipeline"
 │   ├─ task: camera decode
 │   └─ task: imu fusion
 └─ scope "nav-request"   budget: 500ms
     ├─ task: plan                     ← inherits ≤500ms
     └─ scope "local-avoid"            ← child budget can only be smaller
         └─ task: check obstacles
```

Cancel `"nav-request"` and *everything under it* stops (cooperatively — see
§4); `"sensor-pipeline"` is untouched. Close the node, and the root cascade
stops it all.

### 3.2 The lifecycle contract (memorize this one)

A `Scope` has exactly four lifecycle verbs, and the differences matter:

| Verb | Meaning | Waits? |
|---|---|---|
| `cancel(reason)` | Ask every descendant to stop | **No** — returns immediately |
| `close()` | Refuse new tasks, then **wait** until all children finished | **Yes** |
| `shutdown(reason)` | `cancel` then `close` — the normal teardown | **Yes** |
| `shutdown_with_grace(reason, grace)` | Bounded teardown in four phases: mark the subtree closing (no new spawns) → `cancel` → await the drain up to `grace` → **detach** the stragglers (they stop blocking close, their outcomes are discarded, a one-shot WARN names the count). **Returns the detached count — `0` means a clean drain**; check it. | **Yes** — bounded by `grace` |

*(both SDKs ship the first three: Rust `scope.shutdown(reason).await`,
C++ `scope.shutdown(reason)` — each is exactly `cancel` + `close`.
`shutdown_with_grace` is Rust-only today; a detached straggler is not
killed — it keeps running until its next dispatch boundary, and
`detached_children()` tracks it until then.)*

Three consequences beginners trip over:

- **`cancel` alone does not wait.** Code after `cancel()` runs while children
  are still winding down. If you need "they are gone", you need `close()`.
- **Dropping/destroying a `Scope` object does NOT cancel or wait.** The
  handle is not the scope's life. Dropping an *intermediate* handle while
  ancestors still hold theirs is safe — the subtree stays accounted. But if
  you let your *last* handle go without `close()`, the children keep running
  as orphans — exactly what structured concurrency exists to prevent. (A loud
  warning is printed when you do this — in ALL builds — but it is only a
  warning: nothing is cancelled.) End scopes with `shutdown()`/`close()`,
  deliberately.
- **`close()` is one-way and idempotent.** Once closing, new `spawn`s are
  refused (so a task racing your teardown cannot slip in), and calling it
  again is harmless.

### 3.3 Failure: fail-fast by default, supervised on request

If a task **panics/faults**, its scope cancels all its siblings (reason
`ChildFailed`) and the failure surfaces on the task's handle — the reasoning
is "this scope's collective job can no longer succeed, stop burning time on
it" (rule 5). When you *want* isolation — a flaky experimental
probe that must not take down its siblings — create the child scope with
`supervised`: the failure is then contained to that child and still
observable on its handle.

This is a typed, CROSS-PLANE policy: Rust `Supervision` and C++
`flow::Supervision` (`Scope::Options::supervision`) are the same enum,
carried over the C ABI as a validated byte — an unknown value is
refused, never silently ignored. `FailFast` and `Isolate` (the
`supervised` shorthand above) exist on BOTH planes:

```rust
// Rust: builder shorthand, or the typed form spelled out
let s1 = root.child(ScopeOpts::named("probe").supervised());
let s2 = root.child(ScopeOpts::named("probe").supervision(Supervision::Isolate));
```

```cpp
// C++: Scope::Options::supervision is typed, not a bool
auto s = root.child({.supervision = flow::Supervision::kIsolate,
                      .name = "probe"});
```

A third policy, **`Restart`**, is Rust-plane-only this release —
qualified deliberately, not an oversight: `spawn_supervised`
restarts a faulted child from a fresh call to its factory, with bounded
attempts inside a tumbling time window and exponential backoff;
exhausting the window escalates to `FailFast` (§5 has the code shape).
There is no C++ spelling for `Restart` and no C-ABI wire representation
for it — reach for a Rust-side supervisor task if you need restart
semantics from a C++-driven system.

---

## 4. Cancellation: cooperative, observable, RT-safe

### 4.1 No preemption — and why

Flow never kills a running task — with ONE documented exception, the
`when_any`/`TaskHandle::abort()` loser teardown (§5), which hard-stops a
loser at its *next await point* (never mid-instruction). Everywhere else,
cancellation **sets state**; tasks **observe it and unwind**. This is a
deliberate, load-bearing choice: a preempted task could be killed halfway
through writing a motor command or holding a lock, leaving the robot in an
undefined state. Cooperative unwinding means your cleanup code
(destructors, `Drop`) always runs.

The corollary: **a task that never observes cancellation never stops.** Every
loop you write inside a task must touch one of the observation points below.

### 4.2 The reason travels with the cancel

Cancellation carries a `CancelReason` — `Explicit`, `DeadlineExceeded`,
`Shutdown`, or `ChildFailed` — so handlers and logs can distinguish "operator
aborted" from "budget ran out" from "the process is going down". Treat any
unknown/nonzero value as "cancelled" (new reasons may be added).

### 4.3 The four observation points

| Point | Shape | When to use |
|---|---|---|
| `is_cancelled()` | non-blocking bool | inside hot loops; **RT-safe** (one atomic load — safe in a 1 kHz control loop; the C ABI exposes the flag as a raw pointer you resolve once) |
| `checkpoint()` | returns error-with-reason if cancelled/expired | at natural step boundaries in a task body; **this is also what notices budget expiry** |
| `cancelled().await` / status of an executor hop | suspends until cancelled | to *wait* for cancellation, or race it against work (C++ spells it `co_await flow::cancelled()` — the frame's own scope) |
| `run_until_cancelled(fut)` | races `fut` vs cancellation, `None` on cancel | the recommended loop shape: `while let Some(x) = scope.run_until_cancelled(rx.recv()).await { ... }` (Rust; C++ has no spelling for it — the pull/readiness family already races the scope by default, so a plain `co_await flow::recv(sub)` is cancel-safe out of the box, §8 — but see the C++ delta below: not every ambient verb gets this) |

Start small: **`run_until_cancelled` is the only observation point most
tasks ever need.** Reach for `checkpoint()` in long non-awaiting
stretches, `is_cancelled()` in RT loops, and bare `cancelled().await`
only when cancellation itself is the event you are waiting on — the four
points are one mechanism at four ergonomic grips, not four concepts to
learn up front.

**C++ delta: entry-gated vs. mid-flight-raced.** Every C++ ambient verb
refuses AT ENTRY — one atomic load of the frame's scope, at
`await_suspend` — when the scope is already cancelled before the
`co_await`. The one deliberate exception is `flow::cancelled()`:
resolving immediately on an already-cancelled scope IS its whole
contract, so it does not gate against itself. Entry-gating is a
weaker guarantee than "races the scope", and the two verb families
differ honestly:

- **Mid-flight-raced** — `recv`/`aligned`/`wait_wave` (and their
  `*_for` deadline forms) arm the scope's cancel token as a live second
  branch of the SAME wait; a cancel landing while the wait is parked
  wakes it immediately. The action verbs (`send_goal`/`wait_result`/
  `cancel_goal`) get the same live wake through their own ctx-bridge
  wiring.
- **Entry-gated only** — `sleep`, `join`/`join_all`/`race`/`timeout`,
  and `offload` do not arm a live scope-cancel branch once parked: a
  bare scope cancel that lands mid-flight is only *observed* the next
  time one of these verbs is entered. (An executor/pool teardown still
  wakes a parked one early — that is a separate, unconditional
  mechanism: *resume-into-cancellation*, glossary §10.)

The entry gate is also what bounds a swallowed `catch (...)` (§8): even
if a task's cleanup code eats the unwind instead of rethrowing it, the
scope is still cancelled, so the very next ambient verb the task enters
refuses at the door and unwinds again. A swallow costs you one
synchronous window, never a stuck task.

**C++ delta: ending cleanup with `stop_now()`.** After `co_await
flow::cancelled()` resolves, plain code after it would otherwise need a
follow-up await just to get the unwind that completes the task as
`Cancelled` (returning normally would complete it `Ok` instead — almost
certainly not what you want). `flow::stop_now()` is the direct
spelling: `[[noreturn]]`, unwind-only, and it snapshots the outcome's
cancel reason from the scope at the point of the call. The idiom is:

```cpp
co_await flow::cancelled();   // observe the cancel
release_resources();          // cleanup
flow::stop_now();             // end this task AS CANCELLED, right here
```

Rule of thumb: **any `await` that can be slow should be wrapped in
`run_until_cancelled`, and any long non-awaiting stretch should sprinkle
`checkpoint()`/`is_cancelled()`.** Flow's own blocking points (`call_in`,
`offload_in`, spawn) already check for you at entry.

### 4.4 Budget: deadlines that flow

A `Budget` is a deadline attached to a scope. Two properties make it
different from an ad-hoc timeout:

- **It narrows, never widens.** `child(budget: 2s)` under a parent with
  300 ms left gets 300 ms. The intent "this whole request has 500 ms" is
  enforced against every sub-step automatically.
- **It is observed, not enforced by interruption.** An expired budget makes
  the *next* `checkpoint()` (or `call_in`, which checkpoints on entry) fail
  with `DeadlineExceeded`; it does not preempt (same cooperative philosophy
  as §4.1). Note the precise `spawn` behavior: spawning on an expired scope
  *observes* the expiry (the scope becomes cancelled) but the task is still
  created — only a **closing** scope refuses spawns outright. The new task's
  own first `checkpoint()` is what stops it. There is no safety net that
  makes un-checkpointed code stop.

`remaining()` tells you how much is left — useful for choosing algorithm
variants ("50 ms left → use the cheap planner").

### 4.5 Cleanup that must run *under* cancellation: `Shield`

One subtlety in the §4.3 loop shape bites everyone once. Inside a spawned
task, cancellation is enforced at the **dispatch boundary**: once the
scope is cancelled, the task resolves `Stopped` at that outer boundary
*before* the body is polled again. So in

```rust
scope.spawn_with(|s| async move {
    while let Some(msg) = s.run_until_cancelled(rx.recv()).await {
        process(msg).await;
    }
    flush().await;   // ⚠ NEVER runs on a real cancellation
});
```

the `flush()` line only runs when the loop ends for *other* reasons (the
channel closing, say). On a real cancel the whole task is resolved at the
boundary before the body observes its own graceful exit — ordinary code
placed after the loop is unreachable.

Cleanup that must run under cancellation goes inside a **`Shield`**
(Rust: `scope.shield(fut)`; C++ has no equivalent — put C++ cleanup in
destructors, which the unwind always runs). While a shield is polled, the
task is immune to *cooperative* cancellation: `checkpoint()`,
`run_until_cancelled`, the in-task combinators, and the dispatch boundary
all see an un-stopped scope. The raw token surface (`is_cancelled()` /
`cancelled()`) stays shield-**transparent** by design — which is exactly
what lets a shielded worker decide when to stop:

```rust
scope.spawn_with(|s| async move {
    s.shield(async {
        loop {
            tokio::select! {
                _ = s.cancelled() => break,   // raw token: sees the cancel
                msg = rx.recv() => match msg {
                    Some(m) => process(m).await,
                    None => break,
                },
            }
        }
        flush().await;   // still shielded: completes even under cancel
    })
    .await;
    // past the shield, the next await/boundary resolves the task Stopped
    // — put nothing load-bearing here.
});
```

Two hard edges: the **budget is not maskable** — a deadline pierces
shields, so shielded cleanup still has to fit the scope's budget — and
shields **do not cross `spawn`** (a child task gets a fresh ambient).
Keep shields short and terminal: a shield is for `flush()`/`release()`,
not for opting a worker out of teardown.

---

## 5. Tasks: spawning, handles, results

`scope.spawn(async { ... })` starts a coroutine as a child of the scope and
returns a **TaskHandle** — the task's result channel:

- **Await the handle (or `join()`)** to get `Outcome<T>`: the value, or
  a `Stopped` carrying *why* — `Cancelled(reason)` or `Fault(message)`
  (a panicked/faulted body).
- **A dropped handle does not stop the task** (the scope owns the task's
  lifetime, not the handle) — but it does throw away the result, including a
  supervised child's failure. Both SDKs mark the handle "must use": if you
  really mean fire-and-forget, bind it away explicitly and let `close()` be
  your join point.
- `spawn_with(|scope| async { ... })` hands the body its own **real child
  scope** — the shape you want whenever the body needs
  `checkpoint`/`run_until_cancelled` (which is almost always).

⚠ **`spawn_with` is the canonical form.** The scope it hands the body is
what `TaskHandle::abort()` (and therefore `when_any` loser teardown)
cooperatively cancels before the hard stop. A plain `spawn(fut)` task has
no task scope of its own: `abort()` cannot reach nested children the body
built from *outer* scope handles — they are orphaned with only a warning.
Prefer `spawn_with` and derive children from the scope it gives you.

**`spawn_supervised` (Rust-only, §3.3).** On a scope configured with
`Supervision::Restart{max_restarts, window, backoff}`, `spawn_supervised`
restarts a faulted child from a fresh call to its factory instead of
letting the fault propagate:

```rust
let s = root.child(ScopeOpts::named("probe")
    .supervision(Supervision::Restart {
        max_restarts: 3,
        window: Duration::from_secs(10),
        backoff: Duration::from_millis(200),
    }));
let h = s.spawn_supervised(|| async { flaky_probe().await });
```

Under any OTHER policy `spawn_supervised` behaves like a plain `spawn`
(the factory runs once). There is no C++ equivalent this release.

Combinators: `when_all([h1, h2, ...])` awaits all (failing fast if one
fails); `when_any` returns the first winner and **aborts** the losers — a
hard stop at their next await point, *not* a cooperative cancel of the
loser's own body. What happens to a loser splits three ways:

- a **`spawn_with` loser**: its own child scope is cooperatively cancelled
  *first*, then the hard abort — nested children derived from the given
  scope get an orderly cancel;
- a **plain `spawn` loser**: pure hard abort — the future is dropped at its
  next await point, and children built from outer scope handles are not
  reachable (see the warning above);
- a **lazy `race`/`race2` loser** (in-task combinators, not handles): the
  loser future is simply dropped in place — Rust drop *is* its cancellation.

If losers need orderly cleanup, use `spawn_with` (or give them their own
child scope and cancel it yourself). Both combinators are just conveniences
over handles — nothing magic.

One caveat to internalize: the "winner" is the first **completion**, not
the first success. If the task bodies return `Result`, a fast branch
completing with `Err` wins over a slower sibling that was about to succeed
— when racing a primary against a backup RPC you can get the primary's
error and never see the backup's success (the `select` shape, not
`select_ok`; the in-task `race` has the same semantics). A
`when_any_ok`/`race_ok` companion that skips value-level failures is
planned.

**C++ parity.** The C++ plane has the same shapes:
`spawn_on` / `Runtime::spawn` return `flow::TaskHandle<T>`
(`co_await handle` → `Outcome<T>`; a discarded handle is
fire-and-forget, `[[nodiscard]]`-guarded), plus
`flow::when_all(handles)` / `flow::when_any(handles)`. `Outcome<T>` is
built only through `Outcome<T>::Value(v)` / `Outcome<T>::Stop(stopped)`
(no aggregate `{status, value}`, no default-constructed `T` on the stop
path — move-only, non-default-constructible payloads work); reading it
is `o.ok()` then `*o`/`o->`/`o.value()`, or `o.stopped()` for a
`Stopped` with `.kind()`/`.is_cancel()`/`.is_deadline()`/`.is_fault()`,
`.cancel_reason()`/`.raw_reason()`, and `.fault()` (`code()` +
`message()`). This is full parity
with Rust's `Cancelled(reason)`/`Fault(message)`, not a fold: the C++
plane preserves the *exact* wire-level `CancelReason` byte and the
fault's stable code + diagnostic text end to end — it used to fold every
non-OK completion down to a bare kind with no reason (the Flow TCK's
`cancel_reason_observation` scenario was the tracked red evidence for
this gap; `test/flow_tck/SCHEMA.md` records the resolution). Two
remaining honest deltas: a C++ frame is never destroyed while suspended,
so there is **no hard abort** — `when_any` losers keep draining under
their scope (their registrations are revoked immediately;
cooperative-cancel + grace is a registered follow-up) and `when_all`
reports after every handle settles (scope fail-fast still cancels the
siblings promptly). And for non-coroutine contexts (C++ `main`, tests)
the handle has a synchronous seam Rust doesn't need:
`wait_for(budget)` → `take_outcome()` → **then** `scope.close()` (the
closing bar is subtree-wide — close first and the still-running task's
own combinator spawns refuse). One more edge drift: an **empty input
list** — Rust `when_any(vec![])` resolves as a cancelled *value*
(`Err(Stopped::cancelled(Explicit))`), while C++ `when_any({})` refuses
loudly (`INVALID_ARGUMENT` ERROR log, then the standard unwind).

**Where do tasks run?** On Aorta's executor(s) — the same machinery that
dispatches your callbacks. In Rust the ambient runtime is used (spawning off
a runtime thread panics, like `tokio::spawn`; foreign threads must enter a
runtime handle or use the C ABI). In C++ you name the executor explicitly
(`spawn_on(executor, ...)` — §8). Long CPU work still doesn't belong on
executor threads — that's `offload` (see R4 in §7).

---

## 6. Talking to the robot: Flow × Aorta primitives

### 6.1 RPC inside a scope: `call_in`

`client.call_in(&scope, request)` is the flagship integration: an RPC that

- **suspends** instead of blocking a thread,
- inherits the scope's **budget** (narrowed against the client's own timeout),
- resolves immediately with `Cancelled` if the scope is cancelled mid-call,
- refuses up front (checkpoint-first) if the scope is already dead.

One honest limitation: cancelling the *caller's* scope stops the *waiting*;
whether the remote **server** stops working depends on the service (the
cancel is not teleported to it — server-side cancel arrives with actions /
future protocol work).

### 6.2 Actions ARE scopes

Every action goal execution gets its own scope (`ctx.scope()`): a wire
cancel request cancels the scope, and your handler observes it with the
same four points from §4.3 — no second cancellation vocabulary. The goal
deadline travels on the wire too: a `send_goal` issued under a Flow budget
stamps the caller's remaining window onto the request, and the server
seeds the goal scope's budget from it (re-anchored at receipt — a
cooperative narrowing hint), so `ctx.scope().remaining()` and
every `call_in` under the handler narrow automatically. The stamping
honors the `AORTA_RPC_DEADLINE=0` kill-switch, same as plain RPC; an
un-stamped goal's scope starts unlimited.

### 6.3 Streaming state: the `Watch`

A `Watch` is a **one-value mailbox** per topic: `node.watch(topic)`
subscribes and keeps only the **latest** message ("latest-wins"). Consumers:

- `load()` — the current value (Rust: a zero-copy shared handle), or
  nothing yet; RT-friendly.
- `snapshot()` — the structured view: value plus a monotonic sequence
  number plus the source timestamp, taken from one atomic snapshot (safe
  for change detection: seq and value can never be mismatched).
- `changed(seen).await` — **suspend until a newer value arrives.** This is
  what deletes the `sleep(10ms)`-and-poll loop. (Rust-only — see the C++
  delta below.)
- `store(v)` — foreign-producer injection: a CAN-bus thread can feed a
  detached watch cell without any Zenoh involvement.

**C++ delta.** `aorta::flow::Watch` (a C++17 baseline header, no
coroutines) exposes `load()` / `load_checked()` / `store()` over the C
ABI. Its `load()` **copies** the payload into a `std::vector` (a soft-RT
one-buffer copy, not the Rust zero-copy handle) and returns `{seq,
payload}` from one atomic snapshot — the `snapshot()` equivalent. There
is no awaitable `changed(seen)`: a C++ consumer polls `load()`'s seq for
change detection or wires a subscriber callback as the wake-up.

Use a `Watch` when only the freshest value matters (robot pose, battery,
latest camera frame). Use a normal subscriber when every message matters.
Intermediate values are *skipped by design* — that is the point.

### 6.4 Per-endpoint capabilities

Two runtime-adjustable knobs per subscriber (also via C ABI):

- **Priority override** — which executor bucket this subscriber's events
  dispatch from (see the priority note below).
- **Admission** (`FlowAdmission { max_in_flight, deadline }`) — a counting
  gate (at most N events of this subscriber in flight; the settable form of
  `receive_depth`) plus an optional per-event deadline: an event that sits
  in the queue past its deadline is **dropped at dequeue** instead of being
  processed late ("late data is worse than no data" for control loops).
  Sheds are counted, never silent.

*Two priorities exist — don't confuse them:* `EventPriority` is **local
executor scheduling** (this section); `qos::Priority` is the **Zenoh wire
priority** on publish. A subscriber's wire priority maps to an event
priority at receive unless you override it.

### 6.5 Readiness: fire when a SET of things is true

For multi-input logic ("run fusion when camera AND lidar AND imu all have
fresh data"), polling each input is the anti-pattern. Flow ships the
mechanism layer:

- **`ReadinessSet`** — an AND-gate over up to 64 conditions: sources call
  `satisfy(i)`, a consumer awaits a wave; the completing arrival fires
  exactly one "wave" and the gate resets. Two await forms exist:
  `wait_wave()` awaits the next wave *from the call onward* (the everyday
  loop shape); `wait_wave_after(seen)` awaits a wave numbered above a
  `waves()` snapshot you took **before** arming your trigger — the
  race-free form when a wave may fire between your check and your wait.
  (The baseline-before-arm form is Rust-only: C++ `flow::wait_wave` /
  `wait_wave_for` snapshot the baseline internally at await entry.)
- **`WindowSync`** — approximate-time alignment: feed each sensor's samples
  with timestamps; when every input holds a sample within a time window
  (`slop`), the aligned set is emitted. `aligned().await` — again, no
  polling. (Latest-per-input today; deeper history buffers are a
  calibration follow-up.)
- **`poll_fd`** *(unix)* — wire a raw readable fd (serial port, socket)
  into a `Watch` so external inputs join the same model. Not a hard-RT path.

Wiring is declarative — no glue callbacks:

```rust
let window = WindowSync::new(2, Duration::from_millis(5));
let fresh  = ReadinessSet::with_conditions(
    &[Condition::MessageAvailable, Condition::Periodic])?;
let _cam = node.create_subscriber_with_options("cam", on_cam,
    SubscriberOptions::new().with_window_feed(&window, 0)).await?;
let _det = node.create_subscriber_with_options("det", on_det,
    SubscriberOptions::new()
        .with_window_feed(&window, 1)
        .with_readiness_satisfy(&fresh, 0)).await?;   // condition 0: message
node.create_timer(period, fresh.satisfier(1)?).await?;   // condition 1: periodic
let set = window.aligned().await;                          // zero polling
```

Post-declare binds exist for every language (`Subscriber::bind_window` /
`bind_readiness`; C++ `flow::bind_window(sub, ws, 0)`), and C++ flow
frames await the same surfaces ambiently: `co_await flow::aligned(ws)` /
`co_await flow::wait_wave(set)` — the frame's promise-carried context
supplies the runtime and scope (§8), and explicit forms
(`aligned(ws, rt, scope)` etc. — scope mandatory; the cancellation-blind
scope-less overloads were removed) remain for cross-scope/cross-runtime
waits. Indices are validated when you wire, not
silently dropped per sample. Timestamps: `WindowStamp::Receive` (default)
or `::Publisher` — transport-level only; sensor-capture stamps live in
your payload, fed via `window.feed(...)` (that correlation is policy,
yours).

**Actions from coroutines.** C++ flow frames get first-class action
verbs: `co_await flow::send_goal{client, goal}` →
`Outcome<AcceptedGoal>`, `co_await flow::wait_result{goal, 5s}`
(push-based terminal wait) → `Outcome<ActionGetResultResponse>`,
`co_await flow::cancel_goal{goal}` (checked cancel) →
`Outcome<ActionCancelGoalResponse>` — scope and executor come from the
frame's promise-carried context (§8); the explicit
`{client, exec, goal, scope}` spellings remain for cross-scope use and
for non-flow coroutines (§5.1's parameter-order rule: primary object,
execution plane, operation args, scope last). The three verbs
speak the SAME `Outcome`/`Stopped` vocabulary as every other Flow
result (§5) — a scope cancel or deadline resolves as
`Stopped::Cancelled` carrying the EXACT wire-level cancel-reason byte
(via the C ABI's completion record, not folded to a bare code), while a
goal rejection or transport fault resolves as `Stopped::Fault{original
code, message}`. `flow::GoalGuard` ties the goal's server-side lifetime
to a value — dropping it fires a detached best-effort cancel;
`disarm()` hands the goal over instead. Rust's `send_goal`/`wait_result`
are already async; `GoalHandle::into_guard()` is the matching Rust
guard. Remember the layering: scope cancel stops your LOCAL wait — the
goal keeps running server-side unless you cancel it (that is what the
guard is for).

### 6.6 Receiving every message in a task: pull + `recv`

The single most common task shape is "receive, process, repeat". Its Flow
form uses a **pull subscriber** — no callback: the subscriber owns a
bounded keep-last ring (`depth`, drop-oldest) that *you* drain — plus an
awaitable receive that resolves `Stopped` when the scope cancels:

```rust
// Rust: create_subscriber_pull(topic, depth) + recv_in(&scope)
let sub = node.create_subscriber_pull("cmd/vel", 8).await?;
let scope = node.root_scope().child(ScopeOpts::named("consumer"));
let _worker = scope.spawn_with(|s| async move {
    while let Ok(sample) = sub.recv_in(&s).await {
        process(&sample.payload).await;
    }
    // recv_in resolves Err(Stopped) on scope cancel — the loop needs no
    // separate run_until_cancelled wrapper.
});
```

```cpp
// C++: CreatePullSubscriber + co_await flow::recv(sub) — the frame's
// promise-carried context supplies the runtime and scope (§8), so the
// coroutine body threads neither
flow::Task<void> consume(aorta::PullSubscriber& sub) {
  for (;;) {
    aorta::PullSample s = co_await flow::recv(sub);
    process(s);
  }  // scope cancel / teardown unwinds the frame (§8) — no manual check
}
```

The details that matter:

- **Drain-friendly ordering:** queued samples are delivered *before* the
  cancellation check — an already-cancelled scope still receives what the
  ring holds; new *waits* are what cancellation stops.
- **Per-wait deadlines:** C++ `flow::recv_for(sub, window)` (explicit:
  `recv_for(sub, rt, window, scope)`)
  makes the deadline a branch of the same wait (never wrap the raw
  `timeout()` combinator around `recv` — it is rejected at compile
  time with a message naming `recv_for`); Rust composes
  `scope.timeout(d, sub.recv_in(&scope))` (§7 R5). Mind the delivery
  delta: Rust's `scope.timeout` resolves that one wait with
  `DeadlineExceeded` as a recoverable *value* and the task continues;
  C++ `recv_for` (like `flow::timeout`) delivers the deadline by
  **unwinding the whole frame** — no `.nothrow()` form — so the
  continue-after-a-miss receive loop is Rust-only (§7 R5 has the C++
  escape).
- **Blocking siblings** for non-async code (Rust): `recv()` /
  `try_recv()` / `recv_timeout(d)` on the same subscriber.
- This is the "every message matters" counterpart of the `Watch` (§6.3):
  bounded ring, every queued message delivered, oldest dropped on
  overflow — pick by semantics, don't emulate one with the other.

---

## 7. Best practices — the rules

Distilled from the review of real usage; each rule exists because the
opposite compiled fine and misbehaved.

**R1. End every scope on purpose.** `shutdown(reason)` (or `cancel`+`close`)
is part of the happy path, not just error handling. Never rely on drop.

**R2. Make every loop cancellable.** Slow awaits →
`run_until_cancelled(...)`; compute stretches → `checkpoint()?` /
`is_cancelled()`. If you cannot point at the line where your task observes
cancellation, it doesn't.

**R3. Don't ignore results.** Task handles and (C++) statuses are marked
must-use for a reason: a discarded handle silently swallows a supervised
child's panic; a discarded C++ status can hide "you are being torn down"
(§8, rule C3). Fire-and-forget must be *spelled*, not defaulted into.

**R4. Keep executor threads light.** CPU-heavy or genuinely-blocking
closures go to `offload` / `offload_in(&scope, f)` (the latter refuses to
even start once the scope is cancelled). Inside long offloaded jobs, poll
`is_cancelled()` — the pool never kills a running closure, and its queue is
unbounded (self-restraint required for now).

**R5. Deadline at the top, not per call.** Give the *request scope* a
budget and let narrowing handle sub-steps; per-call timeouts are for
protecting against foreign services, not for expressing intent.

When you genuinely need a *per-message* deadline inside a loop, use the
window-local form — `scope.timeout(d, fut)` (C++:
`flow::timeout(window, task)`, or `flow::recv_for(sub, window)` for
receives — ambient forms; the explicit-argument overloads exist for
cross-scope waits) — which resolves
that one wait with `DeadlineExceeded` **without cancelling the scope**:

```rust
loop {
    match scope.timeout(per_msg, sub.recv_in(&scope)).await {
        Ok(Ok(sample)) => process(sample).await,          // in time
        Ok(Err(_stopped)) => break,                        // subscriber gone
        Err(stopped) if stopped.is_deadline() => continue, // this wait expired
        Err(_) => break,                                   // scope cancelled
    }
}
```

**C++ delta.** The recoverable-value shape above is Rust-only:
`scope.timeout` resolves that one wait with `DeadlineExceeded` as a
*value* and the task continues, but C++ `flow::timeout` / `flow::recv_for`
deliver the deadline by **unwinding the whole frame** (no `.nothrow()`
form) — code after the await does not run. The C++ escape when a loop
must survive a deadline miss: spawn the deadline-bounded wait as its own
subtask (`spawn_on` + `TaskHandle`) and read the `Outcome` at the handle
— the unwind is contained inside the subtask.

⚠ Do **not** express a per-message deadline by giving one long-lived
scope a budget and reusing it across the receive loop. A budget is a
*one-shot* deadline for the whole scope: the first observed expiry
(`checkpoint()`, or any entry check) **permanently cancels the scope's
token**, so every message after the first deadline miss fails
immediately, silently, forever.

**R6. Latest-wins data goes in a Watch; every-message data goes in a
subscriber.** Don't emulate one with the other.

**R7. One node, one root.** Parent long-lived scopes under
`node.root_scope()` so node shutdown reaches them; free-standing roots are
for tests and short-lived tools. (C users: `aorta_node_root_scope`.) Note:
node close **cancels** the root cascade but does not await it — for a
deterministic drain, still `shutdown()`/`close()` your scopes yourself.

**R8. Name your scopes** (static strings). Every diagnostic — the orphan
warning, cancel logs, future tracing — is only as good as the names.

**R9. Serialize shared state with a `SerialDomain`, not a mutex (C++ MT
runtimes).** `auto d = rt->create_serial(klass, "name")` gives you a
task-tier strand — the callback-group successor for the coroutine tier.
The user-facing model is three sentences: (1) tasks spawned into one
domain (`rt->spawn(scope, d, task)`) never run concurrently with each
other — as long as they suspend only through Flow's own awaitables
(hand-rolled awaiters that capture the raw coroutine handle are outside
the contract and best-effort-detected by a WARN canary); (2) `co_await`
— including `join`/`race`/`timeout`/`join_all` — keeps you in the
domain; **explicit spawn + `when_all`** is the parallel spelling; (3) a
bound frame lives on its domain's runtime: cross-executor and
cross-runtime *arms* are refused loudly (`INVALID_ARGUMENT`), never
silently rerouted. Two honesty notes: the domain buys
**non-concurrency, not atomicity across awaits** (invariants must hold
at every suspension point — Swift-actor parity), and on ST or
rollback runtimes a domain is a valid no-op (the single consumer
already serializes). `offload` remains the sanctioned escape: the
closure runs outside, the continuation re-enters the domain.

---

## 8. C++-specific ground rules

C++20 coroutines are powerful and sharp; Aorta's C++ layer
(`aorta::flow`, the opt-in `-std=c++20` target — the base SDK stays C++17)
engineers most of the sharpness away (the hardening plan). What remains:

**The one discipline — route lambda coroutines through
`co_invoke`/`spawn_with`.** A RAW capturing-lambda coroutine still dangles
(the closure dies at end of statement while the frame lives on; the
language offers no post-hoc detection). `flow::co_invoke(lambda, args...)`
copies the closure into a wrapper frame that outlives every suspension —
captures become safe; `flow::spawn_with(exec, scope, fn, args...)`
additionally injects the body's own **real child scope** (a by-value
co-owned `scope.child()`, Rust `spawn_with` parity — §5) as the body's
first parameter. Named coroutine functions with value parameters remain
the simplest form.

**Engineered away (know it, don't carry it):**

- **Scope lifetime**: `Scope` is copyable shared ownership; every SDK
  awaitable stores it by value — frames co-own their scope, so a caller
  dropping handles early cannot dangle them.
- **C++ Flow requires exceptions.** Cancellation and deadline
  delivery ride unwinding — there is no parallel status-return channel
  through a `flow::Task`. Instantiating (defining or awaiting) any
  `flow::Task` coroutine in a `-fno-exceptions` translation unit is a
  COMPILE-TIME teaching rejection, not a silent fallback; merely
  including the Flow headers stays legal. The C++17 baseline SDK
  (callbacks, executors, pull subscribers — everything that predates
  `flow::Task`) is the supported `-fno-exceptions` surface. `.nothrow()`
  opt-outs on individual verbs are ergonomics for exceptions-enabled
  builds that want a status-return escape hatch on ONE await — not a
  `-fno-exceptions` story: a body that needs `-fno-exceptions` end to
  end cannot host a `flow::Task` at all.
- **Teardown cannot be ignored**: a §6.3 teardown delivery UNWINDS the
  frame (an internal `cancelled_unwind` caught by the promise — never
  crossing the C ABI). Code after the await simply does not run;
  destructors do. First, `cancelled_unwind` deliberately does **not**
  derive from `std::exception`, so a `catch (std::exception&)` in user
  code cannot swallow a cancellation by accident. Second —
  **a Flow task body that catches `...` MUST rethrow
  `cancelled_unwind`.** A body that swallows it instead (catches `...`
  and does not rethrow) does not suppress cancellation permanently: it
  gets a BOUNDED window — the task keeps running synchronous code until
  its *next* ambient `co_await`, where the entry cancel-gate (§4.3) sees
  the still-cancelled scope and unwinds again. A swallow costs you one
  synchronous window, never a stuck task — and it is DETECTED regardless:
  a thread-local canary is armed at every unwind throw and cleared when
  the promise catches it; a swallowed unwind is noticed at the frame's
  final suspend and WARNs ("a cancelled_unwind was SWALLOWED…") — loud,
  not silent.
- **Escaped exceptions = task failure, not process death**: an exception
  leaving a Task body resolves as `ChildFailed` fail-fast (supervised
  scopes contain it) — exactly the Rust panic semantics. `std::terminate`
  is retired from the promise; the no-exceptions-across-the-ABI rule is
  enforced inside the SDK (noexcept-audited callbacks), no longer by you.
- **Misuse stays distinguishable from cancellation**: a REFUSED await —
  a second concurrent awaiter on a single-awaiter surface
  (`recv`/`aligned`/`wait_wave`, `INVALID_ARGUMENT`) or a stale/freed
  handle (`NOT_FOUND`) — logs at ERROR with the surface name and the raw
  code (e.g. `flow::recv: await REFUSED (status -1 / INVALID_ARGUMENT)`)
  before the frame unwinds. The unwind itself remains `cancelled_unwind`
  (no new exception type, no new StopReason); primitives with a
  `.nothrow()` form carry the raw status in their return value for
  programmatic handling. Normal lifecycle endings (stop, scope cancel,
  deadline, destroy-wake) stay quiet.

**Promise-carried context — the ambient verb tier.** Every flow coroutine
frame carries a context snapshot `{LaneRuntime* rt, executor handle,
Scope, SerialDomain}` in its promise (the fourth element — the R9
domain binding — inherits at the same links and is read by the resume
router and `co_await flow::this_coro::serial_domain`; invalid when
unbound). `spawn_on` stamps the root — `spawn_on(exec, …)`
stamps an executor-only plane (`rt == nullptr`); `spawn_on(rt, …)`
stamps `{&rt, rt.fallback_executor()}`, *including* a Disabled rollback
runtime (a valid plane, not an absent one) — and every `co_await` link
copies it parent→child. That is what lets every verb in this guide be
spelled context-free inside a flow task: `flow::resume()`,
`flow::sleep(d)`, `flow::call(client, payload)`, `flow::join(a, b)` /
`race` / `timeout(w, t)` / `offload(prio, fn)`, `flow::recv(sub)` /
`recv_for(sub, w)`, `flow::aligned(ws)` / `flow::wait_wave(set)` (+
`*_for`), and the action verbs `send_goal{client, goal}` /
`wait_result{goal[, ms]}` / `cancel_goal{goal}`. The rules:

- **Inheritance is an immutable snapshot.** Children see the parent's
  context at every `co_await` link; "changing context" means calling an
  explicit-argument overload or spawning under a different scope — the
  promise is never mutated.
- **Explicit wins, per call.** The explicit-argument overloads all
  remain — they are the override tier and the unit-test seam. No merge,
  no writeback: an explicit call uses exactly the passed arguments.
- **The explicit parameter order is frozen (§5.1): primary object,
  execution plane, operation args, scope last** — e.g.
  `race(rt, a, b, scope)`, `timeout(rt, window, task, scope)`,
  `call{client, executor, payload..., scope}`,
  `send_goal{client, executor, goal, scope}`,
  `wait_result{goal, executor, timeout, scope}`,
  `cancel_goal{goal, executor, scope}`. Two recorded exemptions: `join`
  (`join(rt, scope, tasks...)`) keeps scope right after the plane —
  C++ deduction cannot place a fixed parameter behind a deduced task
  pack; and the spawn shapes (`spawn_on`/`spawn_with`) keep their
  owner-after-plane shape (`spawn_on(plane, scope, task)`) because the
  scope there is the structural owner (the nursery argument), not a
  cancellation racer.
- **`{rt, exec}` is ONE coupled "plane" element.** An explicit
  `LaneRuntime&` overrides both (exec becomes that runtime's fallback
  executor); an explicit executor argument selects the executor-only
  plane for that call. No API accepts a mixed pair.
- **Readers:** `co_await flow::this_coro::context / scope / runtime /
  executor` — zero-suspend, by value; `this_coro::scope` is an owning
  copy (the in-loop cancellation observation point).
- **`offload` is a plane switch.** After `co_await flow::offload(prio,
  fn)` the frame continues on the lane worker (no auto-rehop, by
  design); ambient `flow::resume()` only re-queues to the frame's own
  plane — on an MT runtime that can be a lane worker again. The
  explicit `resume_on{exec}` is the way back to the executor dispatch
  thread.
- **Null-rt refusal.** On an executor-only frame the ambient
  arbiter-family verbs (`join`/`race`/`timeout`/`offload`/`recv`/
  `aligned`/`wait_wave`) refuse at runtime — an ERROR log names the fix
  ("spawn this task on a LaneRuntime, or use the explicit overload"),
  then the standard unwind. Ambient `resume()`, `sleep()`, `call`, and
  the action verbs work on both planes.
- **Foreign coroutines fail to compile** — awaiting a flow `Task` or an
  ambient verb from a non-flow coroutine is a compile error with a
  teaching message — with one honestly-recorded exception: the ambient
  `call` and action-verb spellings are constructors of the same classes
  as the explicit forms, so they refuse loudly at *runtime* instead
  (ERROR log + `INVALID_ARGUMENT` through the RPC-layer return). The
  explicit forms are the surface for non-flow coroutines.
- **Teardown precondition.** Stop the executor and let its stop-drain
  complete *before* destroying the `LaneRuntime`; destroying it under a
  live executor is a contract violation (a destructor-sweep resume
  would queue behind the live executor and touch the freed runtime).

Also good to know: `Task<T>` is **lazy** (nothing runs until spawned or
awaited); failures/cancels PROPAGATE across nested `co_await` chains (the
stored-exception model) and are classified once, at the spawned root;
`spawn_on`'s returned id is opaque accounting (never call
`aorta_task_finish` on it); `join`/`race`/`timeout`/`offload` live in
`flow_combinators.hpp` (ambient `offload(priority, fn)` — explicit
`offload(rt, priority, fn)` — hops a blocking or
CPU-heavy closure onto a lane class — the Rust `offload` intent with the
lane pool as the substrate). `race` carries §5's caveat verbatim: the
first branch to *complete* wins, including one completing with an error
value.


---

## 9. Mental-model FAQ

**Q: Is `await` the same as blocking?** No. Blocking occupies the thread;
`await` releases it. That single difference is most of Flow's value.

**Q: Who runs my task after a pause?** An Aorta executor — the same
component dispatching your callbacks. That's also why you must not hog it
(R4).

**Q: What happens to a task nobody awaits?** It runs to completion (or
cancellation) — the scope owns it. Only its *result* needs the handle.

**Q: Can cancellation lose data / corrupt state?** Not by preemption —
there is none (§4.1). Your task unwinds at a point *you* chose, with
destructors running. Design those points so unwinding is safe.

**Q: What if I forget `close()`?** Children keep running (orphans), and
dropping the last handle prints a warning naming the scope — in all builds
(§3.2). It is only a warning; nothing is cancelled. Fix the code path —
this is the one guarantee Flow cannot give without you.

**Q: `Watch` skipped some messages — bug?** Feature. Latest-wins is the
contract (§6.3). If every message matters, use a subscriber.

**Q: Does a cancelled `call_in` stop the server?** It stops your *waiting*
immediately; the server learns about it only if the service protocol
carries cancels (actions do; plain RPC today does not — §6.1).

**Q: How does this interact with plain callbacks?** Freely. Callbacks and
tasks share executors; a callback can `store` into a Watch or `satisfy` a
ReadinessSet that a task awaits. Adopt Flow incrementally.

---

## 10. Glossary

| Term | One-liner |
|---|---|
| **Coroutine / task** | A function that can pause at `await` points and resume later, without holding a thread while paused |
| **Frame** | The heap object holding a paused coroutine's state |
| **Executor** | The component that runs callbacks and resumes tasks |
| **Scope** | Owner of a set of tasks/child scopes; the unit of cancellation, deadline, and teardown |
| **CancelToken** | The cancellation tree node behind a scope (usable standalone via C ABI for e-stop-style wiring) |
| **CancelReason** | Why: Explicit / DeadlineExceeded / Shutdown / ChildFailed (+ future values = "cancelled") |
| **Budget** | A deadline that narrows down the scope tree, observed at checkpoints |
| **checkpoint** | A voluntary "am I still supposed to run?" observation point |
| **TaskHandle** | A spawned task's result channel (must-use; dropping it ≠ stopping the task) |
| **Supervision** | Typed, cross-plane fault policy on a scope: `FailFast` (default) / `Isolate` (`supervised`, both planes) / `Restart` (Rust-only) — §3.3 |
| **supervised** | Shorthand for `Supervision::Isolate`: a child scope whose failure is contained instead of cancelling siblings |
| **Restart (supervision)** | Rust-only supervision policy: `spawn_supervised` restarts a faulted child with bounded attempts in a tumbling window; exhaustion escalates to `FailFast` — no C++ spelling (§3.3) |
| **Shield** | (Rust) Masks *cooperative* cancellation while polled so terminal cleanup completes; budgets pierce it; raw token stays visible (§4.5) |
| **Watch** | Per-topic latest-value mailbox: `load` / `snapshot` / `changed` / `store` (`changed(seen).await` + zero-copy `load` are Rust-only; C++ `load()` copies out `{seq, payload}` — §6.3) |
| **latest-wins** | Watch semantics: intermediate values may be skipped; only the freshest matters |
| **Admission** | Per-subscriber counting gate + per-event queue deadline (drop-at-dequeue) |
| **ReadinessSet / wave** | AND-gate over conditions; the completing arrival fires one wave and resets |
| **WindowSync / slop** | Approximate-time multi-sensor alignment within a tolerance window |
| **offload** | Run a blocking/CPU closure on a dedicated pool; await its result |
| **resume_on (C++)** | Awaitable hop onto a named executor's dispatch thread (the ambient `flow::resume()` re-queues on the frame's own plane instead — §8) |
| **resume-into-cancellation** | Teardown rule: queued resumptions run one final time with a cancelled status instead of being dropped |
| **stop_now() (C++)** | `[[noreturn]]`, unwind-only: end the current task now, AS CANCELLED, after observed cleanup — the `flow::cancelled()` idiom's closing move (§4.3) |
| **entry cancel-gate (C++)** | Every ambient verb but `flow::cancelled()` refuses at `await_suspend` if the frame's scope is already cancelled (one atomic load); bounds — but does not replace — mid-flight racing (§4.3) |
