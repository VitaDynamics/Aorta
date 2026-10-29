# Aorta Python SDK — Design Proposal

> Historical design note: describes the design at the time it was written; it is not a v0.1.0 instruction.

- **Status:** FINALIZED v1.0 — deep design review complete (verdict APPROVE-WITH-CHANGES; all 6
  must-fix + medium/nit items verified against source and applied 2026-06-15). Implementation contract.
- **Owner:** Aorta middleware team
- **Date:** 2026-06-15
- **Supersedes / corrects:** an earlier architecture sketch of a PyO3/maturin binding that
  contradicts the L2 C-ABI layer rule (see §2).
- **Related:** `rust/README.md`, `cpp/README.md`, `spec/Aorta_SDK_Common_Concepts.md`.

---

## 1. Goal, Scope, Non-Goals

### 1.1 Goal

Deliver a **complete, reliable, easy-to-use, relatively high-performance Python 3 SDK**
(`aorta` package) that reaches **functional parity with the existing Rust and C++ SDKs** —
no missing functionality — built on the Bazel-authoritative toolchain, validated by 100%
line+branch test coverage plus smoke, regression, and cross-language interop tests.

### 1.2 Scope — the parity contract ("no missing functionality")

The Python SDK must expose, in Pythonic form, the **union of the capabilities present in the
Rust SDK (`rust/`) and C++ SDK (`cpp/`)**. The authoritative capability inventory is the
parity matrix in §9, derived from a full read of `core/src/ffi/c_api/*`, `rust/src/*`, and
`cpp/include/aorta/*`.

**Scope boundary (decided 2026-06-15):** parity is defined as *exactly* the Rust/C++ capability
union — **no Python-only features beyond that union**. Concretely this trims:

- **No Python-only `Node.with_config_file()/with_config_json()`** — the Rust SDK deliberately
  hides `zenoh::Config` and routes deployment config through environment variables
  (`ZENOH_SESSION_CONFIG_URI` / `ZENOH_CONFIG`, `AORTA_*`). Python uses the **same env-var path**
  for router/endpoint configuration. (This resolves open question Q2: it is not a gap; it is the
  established parity behavior.)
- **No asyncio facade as a required deliverable** — the cross-language SDK contract is *uniformly
  synchronous*; neither the C++ SDK nor the Rust SDK surface exposes an async API to
  end users. An asyncio facade would be a Python-only value-add and is therefore **out of scope**
  (may be revisited later as an optional, separately-scoped add-on, but is not part of parity).

### 1.3 Non-Goals (out of scope, with rationale)

| Out of scope | Rationale |
|---|---|
| asyncio / `async def` API | Python-only value-add; not in Rust/C++ SDK contract (sync-uniform). |
| Explicit SHM / loan API | Not surfaced in Rust/C++ SDK public API; core-internal. |
| Tracing registration hooks | No C-ABI surface exists at any layer yet. |
| Action stats getters | Do not exist at any layer (Rust/C++/C-ABI). |
| PyO3 / native-extension binding | Forbidden by the L2 C-ABI layer rule (see §2). |
| Re-implementing protocol logic in Python | DRY: Core is the single source of truth; Python is a thin binding. |

---

## 2. Binding Strategy Decision (locked)

**Decision: pure-Python `ctypes` binding over a Bazel-built `rust_shared_library` cdylib of the
L2 C-ABI.** (Ratified by owner 2026-06-15.)

### 2.1 Why — and the document contradiction it resolves

At the time of writing the design documents contained a **direct contradiction**:

- **The L2 C-ABI layer rule:** the C++ and Python SDKs MUST go through the L2 C-ABI without
  exception; neither may depend directly on any Rust crate.
- **BUT an earlier architecture sketch** proposed a **PyO3 project built with `maturin build`**
  that binds `Node/Publisher/Subscriber` *directly* against the `aorta_core` Rust crate — which
  **violates the layer rule**.

**Resolution:** the layer rule wins; **PyO3-over-crate and maturin are disallowed.** This
proposal **corrects the earlier sketch** to specify ctypes-over-C-ABI. The spec edits land with
Phase 0 (specs are updated alongside API changes).

> Governance note: the CI architecture lint gate currently covers `rust/` and `tools/`
> but **not** `python/`. Phase 0 extends the lint to forbid any Rust-crate dependency under
> `python/`, so the layer rule is mechanically enforced going forward.

### 2.2 Decision matrix

| Criterion | **A: ctypes / cdylib (chosen)** | B: cffi API-mode | C1: PyO3 over C-ABI | C2: PyO3 over crate | D: maturin |
|---|---|---|---|---|---|
| L2 C-ABI layer-rule compliance | ✅ PASS | ✅ PASS | ✅ (narrow) | ❌ VIOLATES | ❌ VIOLATES |
| Bazel-authoritative | ✅ `rust_shared_library`+`py_wheel`, no new toolchain | ⚠️ custom CPython-ext genrule | ⚠️ pyo3 crate + interp wiring | ⚠️ | ❌ outside Bazel |
| aarch64 cross-compile (robot) | ✅ cdylib has no libpython; 1 `.so`/arch | ❌ needs target sysroot | ❌ PYO3_CROSS_* unwired | ❌ | ❌ |
| Wheel packaging | ✅ `py3-none-linux_*` version-agnostic | per-CPython-minor | abi3 | abi3 | per-version |
| Ergonomics (dev) | ⚠️ hand-bind ~113 fns + structs | medium | best | best | best |
| ABI-drift safety | manual + **CI header-parse guard** | best | n/a | n/a | n/a |
| Continues existing code | ✅ extends `python/logging.py` loader | partial | no | no | no |

cffi API-mode (B) is the strongest *DRY* option but **loses the hard aarch64 cross-build gate**;
we adopt its one good idea — parsing the cbindgen header — purely as a **CI ABI-drift test** (§6.4).

### 2.3 What "ctypes over C-ABI" means concretely

- **Runtime:** the `aorta` package is **pure Python**. It loads one shared library
  (`libaorta_core.so` / `.dylib`) via an extended version of the existing
  `python/logging.py:_load_lib` loader, and calls the ~113 `extern "C"` functions in
  `core/src/ffi/c_api/*` through `ctypes`.
- **No compiled Python extension** → one `.so` per architecture serves **all** CPython versions;
  trivial aarch64 cross-compilation; no libpython linkage.
- **The C-ABI is the only contract.** Python never depends on a Rust crate. DRY: one binding
  surface, mirroring C++.

---

## 3. Architecture & Layer Placement

```
L0 Zenoh ── L1 Rust Core (core/, aorta_core) ── L2 C-ABI (core/src/ffi/c_api, cbindgen → aorta.h)
                                                     │
                       ┌─────────────────────────────┼─────────────────────────────┐
                    L3 rust/ (aorta_sdk)        L3 cpp/ (thin C-ABI wrapper)   L3 python/ (THIS — thin ctypes wrapper)
```

The Python SDK sits at **L3**, peer to the Rust and C++ SDKs, consuming **only** L2. It adds:

1. A **loader** + **ctypes ABI mirror** layer (structs, signatures, handles).
2. A **safety/ergonomics** layer (exceptions, callback trampolines, context managers,
   bytes/memoryview policy).
3. A **typed-messaging** layer over generated FlatBuffers Python bindings + a new generated
   Python schema-metadata artifact (§7).
4. The **public Pythonic API** mirroring Rust/C++ (§8).

---

## 4. Build, Packaging & Distribution

### 4.1 Step zero (hard prerequisite, blocks everything)

Today `core/BUILD.bazel` produces only `rlib` and `staticlib` — **there is no cdylib target.**
Phase 0 adds:

```python
# core/BUILD.bazel
rust_shared_library(
    name = "aorta_core_cdylib",
    crate_name = "aorta_core",          # → libaorta_core.so / libaorta_core.dylib
    deps = [...],                        # same deps as the rlib
    # macOS: link //core:aorta_core_linkopts
)
```

The output `.so` name (`libaorta_core`) is already what the existing loader searches for.

### 4.2 Packaging

- **`py_wheel`** (rules_python native) builds a **platform wheel** per architecture.
  Distribution name = **`aorta_sdk`** (index name); import package = **`aorta`**. Tag =
  `aorta_sdk-<ver>-py3-none-linux_x86_64.whl` / `...linux_aarch64.whl` — `py3-none-<platform>`
  (**NOT `any`**: it carries a native `.so`), version-agnostic across CPython **3.10+** (no
  libpython linkage; floor = the repo's `rules_python` pin, `MODULE.bazel:148`). manylinux/auditwheel
  repair is a **separate later gate**; first ship internal `linux_{x86_64,aarch64}` wheels.
- The cdylib is **bundled package-relative** inside the wheel. **Loader order (single source of
  truth — §9.0):** `AORTA_CORE_FFI_LIB` explicit override → **bundled package-local cdylib** →
  `ctypes.util.find_library`. Immediately after load, run the **runtime ABI/version guard** (§6.4)
  before any other call.
- **aarch64 cross-build:** target `--platforms=@rules_rust//rust/platform:aarch64-unknown-linux-gnu`,
  to be surfaced as a `--config=robot` in `.bazelrc` — **not yet defined** (added in the Phase-5
  packaging step). Local dev builds the host-arch cdylib; the linux/aarch64 cross-build + wheel are
  validated in CI / on the robot (Phase 5), not on the macOS dev host.
- **`.pyi` stubs + `py.typed`** ship in the wheel; ctypes `Structure`s and FFI internals are
  kept out of the public stubs (private `_ffi` submodule).

### 4.3 Package layout

```
python/
  aorta/
    __init__.py            # public API re-exports
    _ffi/                  # PRIVATE: loader, ctypes Structures, signatures, error decode
      __init__.py
      loader.py            # extends current logging.py:_load_lib
      structs.py           # ctypes mirrors of every #[repr(C)] struct (layout-locked)
      bindings.py          # argtypes/restype for all extern "C" fns
    errors.py              # AortaError hierarchy + code→exception map
    node.py  publisher.py  subscriber.py  service.py  client.py
    action.py  timer.py  executor.py  contextstore.py
    qos.py  time.py  header.py  logging.py   # logging.py already exists, moves under aorta/
    schema.py              # schema hash + AortaHeader injection helpers
    py.typed  *.pyi
  examples/                # parity example set (§10)
  tests/                   # pytest smoke + regression; 100% coverage target
  BUILD.bazel
```

> Migration note: the existing top-level `python/logging.py` + `python/__init__.py` (already a
> working ctypes binding of the 7 `aorta_log_*` symbols) are **folded into `python/aorta/`** and
> become `aorta.logging`; behavior is preserved (it is already feature-complete — see §9.12).

---

## 5. Cross-Cutting Models

### 5.1 Threading / GIL / callback discipline (the #1 hazard)

All callbacks — subscriber `on_msg`, service handler, timer tick, contextstore `watch`, action
feedback — fire on **Tokio/Zenoh worker threads, not the Python thread, possibly concurrently.**
The binding enforces:

1. **GIL auto-acquire:** ctypes `CFUNCTYPE` trampolines acquire the GIL on entry, so user Python
   code is always GIL-safe. **Honesty in docs:** the GIL serializes Python callbacks — a
   `CONCURRENT`/`Reentrant` callback group gives *no real parallelism for pure-Python callbacks*.
   It only helps when the callback releases the GIL (I/O, NumPy, C-extension). Recommend C++ for
   >1 kHz / sub-ms paths; Python for tooling, prototyping, ≲100 Hz control.
2. **Python exceptions must NEVER cross the C boundary.** Rust ≥1.81 `extern "C"` is `nounwind`;
   the core `ffi_guard*` catches *Rust* panics, not host exceptions → a propagating Python
   exception is UB/abort. **Every trampoline wraps the user callback in `try/except`, logs via
   `aorta.logging`, and swallows** (mirrors C++ `aorta_cpp_on_msg` noexcept + the
   `throwing_callback_listener` test). This is the #1 crash risk and gets a dedicated regression
   test per callback type.
3. **Callback lifetime / GC:** the binding keeps a strong ref to every `CFUNCTYPE` trampoline for
   the full lifetime of the owning object. `user_data` is a **stable integer token**; the real
   Python handler is looked up in a process-global registry dict (never pass a movable PyObject
   pointer to Rust).
4. **Reentrancy:** most FFI calls from inside a callback return `AORTA_ERR_REENTRANT` (-15) →
   raised as `ReentrantError`. **Callback-safe set only:** `publish[_owned]`, `owned_bytes_free`,
   `sample_release`, `stop_handle_stop`, and the spawn-detach `_free`s. **The action `execute_cb`
   is special** — it runs on a dedicated thread per goal (`offload_dedicated` in
   `core/src/flow/offload.rs`; the Flow offload pool before ABI 13), not on a dispatch
   substrate, so `aorta_action_context_*` calls *succeed* there
   (the natural place for long Python work + `succeed`/`abort`/`publish_feedback`). The documented
   pattern for other long work is "defer to a `queue.Queue`, drain from the spin thread."
   **Action-execute exception rule (overrides #2's plain swallow):** if the user execute handler
   raises, the trampoline must catch and explicitly call
   `ctx.abort(ActionReplyCode.HandlerError, message)` **before returning** — never just swallow. A
   bare swallow returns with no terminal state set, and Core then reports
   `HandlerCompletedWithoutResult` (`core/src/ffi/c_api/mod.rs:1653`), masking the real failure.
5. **Teardown drain:** `close()`/`__exit__` replicate the core's "wait for in-flight callbacks
   before freeing" (subscriber/service/timer drain ≤2 s). **Closing from a callback thread is
   forbidden.** Lifetime is managed via explicit `close()` + context managers; a
   `weakref.finalize` guard (not `__del__`, to avoid resurrection) is a backstop only.

### 5.2 Exception model

`aorta_result_t` is `i32`; `AORTA_OK = 0`, errors are negative (codes -1..-15, with -6 and -9
unused). On any non-zero rc the binding **immediately calls `aorta_last_error()` on the same
thread** (thread-local, invalidated by the next FFI call), decodes UTF-8, and raises a mapped
exception carrying `code` and `message`. (Recoverability is **not** exposed: core's
recoverable/non-recoverable variants collapse onto shared FFI codes in `map_aorta_error`, so it
cannot be reliably derived from the FFI code post-collapse.)

```
AortaError(Exception)
├── InvalidArgumentError   (-1)   # also: CapacityExceeded, NotSupported (collapsed by FFI)
├── NotFoundError          (-2)
├── TimeoutError           (-3)
├── BusyError              (-4)   # also: ShmUnavailable, QuotaExceeded (collapsed by FFI)
├── InternalError          (-5)   # also: Transport
├── SchemaConflictError    (-7)
├── ConcurrentConflictError(-8)
├── AlreadyRepliedError    (-10)
├── PrefixNotDeclaredError (-11)
├── DeclarationNotReadyError(-12)
├── KindMismatchError      (-13)
├── InstanceIdCollisionError(-14)
└── ReentrantError         (-15)
```

> Note: several distinct core errors **collapse** onto shared FFI codes (verified in
> `map_aorta_error`, `core/src/ffi/c_api/mod.rs:195`). The Python hierarchy keys off the **13
> distinct FFI codes**; the original granularity is preserved in the message string.

**Control-flow, not exceptions:** `spin_once` Timeout → returned as an enum/sentinel (not raised);
`query_status`/`info`/`get_result` NOT_FOUND → return `None`. The code→exception mapping lives in
**one** module (`aorta/errors.py`) — DRY, never forked per call site.

### 5.3 Bytes / memoryview policy (a UAF-class decision — locked)

- **Inbound** (`on_msg`, `ctx_change`, sample payload): the C-ABI hands a **borrowed**
  `aorta_bytes_view_t` valid *only during the callback*, possibly **multi-slice** (Zenoh
  scattered buffers). **Default = copy-on-deliver:** the user callback receives an owned `bytes`
  (slices concatenated). **Opt-in zero-copy** (`zero_copy=True`) delivers a `memoryview` **only when
  `slice_count == 1`** — a single Python `memoryview` cannot represent a non-contiguous multi-slice
  view (`aorta_bytes_view_t` = slices+slice_count+total_len, `core/src/ffi/c_api/mod.rs:2405`; Core
  can emit multi-slice, `:557`). When `slice_count > 1` the binding **falls back to a copied `bytes`
  even under `zero_copy=True`** (documented, not silent). The zero-copy `memoryview` carries a loud
  "valid only during the callback; do not stash" contract; a `.to_bytes()` helper copies.
- **Outbound** `publish` / `call` / `reply` / action ctx inputs: accept the **buffer protocol**
  (`bytes`/`bytearray`/`memoryview`/NumPy), mirroring Rust `Into<ZBytes>`. v1 default copies via
  the memcpy fallback (single-slice `aorta_owned_bytes_view_t`, `release=None`).
- **`aorta_owned_bytes_t` outputs** (client/action responses): read `slices`/`total_len` into a
  Python `bytes` **before** `aorta_owned_bytes_free(&out)` in a `try/finally` (free is idempotent).
- **Zero-copy publish** (`publish_owned` + release callback): the buffer must outlive the call
  until `release` fires on an arbitrary thread; tracked via a registry keyed by `release_ctx`.
  **Deferred to a late phase** (large sensor frames only); copy path is the safe v1 default.

### 5.4 QoS / express / time

- **QoS** is a frozen dataclass (priority, reliability, congestion_control, `express`).
  Presets come **only** from `aorta_qos_preset()` (which returns the full struct) — never
  hard-coded (single source of truth). The reliability docstring is term-locked: *"hop-to-hop,
  not exactly-once."*
- **`aorta_qos_t` mirrors the QoS struct 1:1** (QoS & buffering redesign): `congestion_control`
  + `express` are folded into the struct (the former `durability`/`history_depth` no-op bytes were
  removed), so `QoS._to_ffi()` carries every field and the plain `aorta_{pub,sub}_new` ctors are
  used — no separate `*_with_express` param.
- **Time:** `now_ns()`, `unix_time_ns()`, simulation-time get/set, time-source get/set,
  `node.now()` — full parity with the C-ABI time surface.

### 5.5 Schema hash & SchemaAttachPolicy

- Schema hash = `hashlib.sha256(bfbs).hexdigest()` → **64 lowercase hex chars**, byte-identical to
  core. Short hashes are rejected; V1 attachment = exactly 64 hex bytes, no prefix.
- `SchemaAttachPolicy` enum: `Inherit=0` / `Always=1` / `Never=2`; honors `AORTA_SCHEMA_ATTACH`.
  Per-primitive defaults match core: Publisher=Inherit, Service=**Never**, Client=Inherit.

---

## 6. ABI Mirroring & Drift Safety

### 6.1 Struct mirrors (layout-locked)

Every `#[repr(C)]` struct gets a ctypes `Structure` mirror. The table below is the **minimum
locked set** (verified against `core/src/ffi/abi_assert.rs`); the binding also mirrors the
remaining structs (action-server codec vtable, timer/executor/node-telemetry options, etc.), and
the §6.4 runtime guard asserts `ctypes.sizeof` for **all** mirrored structs, not just these:

| Struct | size | Struct | size |
|---|---|---|---|
| qos | 24 | msg_context | 56 |
| pub_options | 32 | bytes_view | 24 |
| sub_options | 72 | owned_bytes | 32 |
| service_options | 104 | ctx_read | 88 |
| client_options | 48 | ctx_change | 128 |
| action_client_options | 16 | pub_stats | 56 |
| action_server_options | 32 | service_stats | 88 |
| exec_admission | 64 | client_stats | 72 |

### 6.4 ABI-drift CI test + runtime ABI guard (two layers)

**Static (CI):** a test **parses the cbindgen-generated `aorta.h`** (target
`//core:aorta_ffi_header`) with `cffi.FFI().cdef(...)` (or `pycparser`) and asserts every ctypes
`Structure` size/offset and every function `argtypes`/`restype` matches the header — the Python
analogue of `cpp/test/abi_layout_test.cc`. **Header-parse alone is insufficient**: it cannot catch
loading the *wrong* `.so`, a missing symbol, or *semantic* drift (e.g. the `publisher_get_stats`
stub, `core/src/ffi/c_api/publisher.rs:253`).

**Runtime (in the loader, every process start):** after loading the cdylib the guard (1) verifies
the expected **exported symbol set** is resolvable, (2) asserts `ctypes.sizeof` for every mirrored
struct against a baseline, (3) checks the result-code constants, and (4) probes a cheap call
(`aorta_now_ns`) to confirm the library is live. Any mismatch raises at import — the SDK never runs
against a drifted or wrong library.

---

## 7. FlatBuffers & Schema-Metadata Codegen (the one true build gap)

### 7.1 What already exists

- **Python flatc accessor classes already exist** — the `flatbuffers_python` Bazel rule emits
  them, and ~30 schemas already set `python=True`. flatbuffers is version-locked (`25.9.23` for
  both flatc and the pip runtime). The `flatbuffers` pip dep is already in
  `python/requirements_*.txt`.

### 7.2 What is MISSING and must be built (gating for typed messaging)

The **Aorta schema-metadata layer** for Python. Extend `message/tools/gen_schema_meta.py` with a
`--lang=python` mode that emits, per schema, a `_schema_meta.py` exporting:

```python
SCHEMA_BFBS: bytes      # consumes the SAME `:<stem>_bfbs` Bazel artifact the existing genrule
                        # produces (gen_schema_meta.py:270 reads --bfbs bytes; schema_codegen.bzl:202
                        # exposes `:stem_bfbs`) — Python MUST reuse that target, NOT regenerate, so
                        # byte-identity holds; else SHA-256 diverges and Always-policy peers reject
SCHEMA_HASH_HEX: str    # = hashlib.sha256(SCHEMA_BFBS).hexdigest(), 64 lowercase hex
ROOT_TYPE_NAME: str     # dotted, e.g. "aorta.examples.SensorReading"
FIXED_SIZE: int | None
HAS_AORTA_HEADER: bool
```

Add a `schema_meta_py` Bazel flag + targets/aliases mirroring the existing `schema_meta_cpp`
(consuming the shared `:stem_bfbs`); set it on every `python=True` schema. The
flatbuffers-pin-drift CI check (`python/requirements_*.txt` pin == `MODULE.bazel` tag) **and** a
cross-language **schema-hash byte-identity** gate are **Phase-1 exit gates** (moved earlier from
Phase 5): a hash divergence is a silent wire incompatibility and must be caught the moment typed
messaging lands.

### 7.3 AortaHeader injection (byte-identical to Rust/C++)

```python
def build_aorta_header(builder, *, publish_stamp_ns=None, publisher_node=None, sequence):
    # header table at field id 0, built BEFORE the root table opens (FlatBuffers is bottom-up);
    # publish_stamp_ns defaults to time.time_ns() (matches unix_time_ns());
    # omit publisher_node when empty; per-publisher/per-direction monotonic sequence from 0.
```

Header-layout inference for typed-subscribe context is done by **Core** via BFBS reflection —
Python only supplies the BFBS bytes in `aorta_sub_options_t`; **no Python-side BFBS parsing.**

### 7.4 Actions

Action typed messaging consumes the **generated protocol-wrapper BFBS**
(`action_schema=True` / `gen_action_wrapper.py`), **not** the bare domain BFBS. The action-server
codec (§8.7) is built over the generated FlatBuffers Python bindings.

---

## 8. Public API Design (mirrors Rust/C++)

Naming follows Python conventions (`snake_case` methods, context managers, keyword-only options),
while the *capabilities* mirror Rust/C++ 1:1. Illustrative surface (full method list = §9):

### 8.1 Node
```python
with aorta.Node("my_node", group="default") as node:   # ctx manager; close() drains + joins executor
    node.id(); node.name(); node.group(); node.now()
    cs = node.create_context_store(); eg = node.create_execution_group(ExecutionPolicy.SERIALIZED)
    # telemetry identity auto-set: language="python", sdk_name, sdk_version, labels
    # (no validate_*/liveliness on Node — those are C-ABI-only, not SDK parity; see §9.2-note)
```
A single background spin thread is spawned lazily on the first callback-bearing factory (mirrors
Rust `ensure_executor`); `close()` stops and joins it. Deployment/router config is via env vars
(parity with Rust — §1.2).

### 8.2 Publisher
```python
pub = node.create_publisher(SensorReading, "aorta/.../sensor", qos=QoS.sensor_data())
pub.publish_typed(lambda b: SensorReading.build(b, ...))   # builds AortaHeader + payload, sync
pub.publish_bytes(raw)                                      # raw, no header (bridging)
pub.topic(); pub.schema_hash(); pub.stats()                 # stats via Python-side accounting (parity)
```

### 8.3 Subscriber
```python
sub = node.create_subscriber("aorta/.../sensor", on_msg, options=SubscriberOptions(...))
# on_msg(payload, ctx): payload is bytes (default copy) or memoryview (zero_copy=True);
# ctx exposes topic/receive_stamp_ns/priority/publisher_stamp_ns/publisher_node/sequence (opt-in)
sub_typed = node.create_subscriber_typed_view(SensorReading, topic, on_typed)
sub.undeclare()    # or context-managed
```

### 8.4 Service (RPC server)
```python
svc = node.create_service("aorta/.../echo", handler, options=ServiceOptions(...))
# handler(request_view, req): req.reply(bytes) | req.reply_error(str) | req.release()
svc_typed = node.create_service_typed_view(EchoReq, EchoResp, typed_handler)  # auto error-reply on decode fail
svc.stats()   # real FFI counters (ServiceStats)
```

### 8.5 Client (RPC, sync)
```python
client = node.create_client("aorta/.../echo", options=ClientOptions(timeout_ms=5000))  # allow_retry OFF by default
resp = client.call(request_bytes)                 # blocking; call_with_timeout(b, ms)
typed = node.create_client_typed(EchoReq, EchoResp); r = typed.call(fill); r.decode()
client.stats()   # real FFI counters (ClientStats)
```

### 8.6 Action client
```python
ac = aorta.ActionClient(node, options=ActionClientOptions(...))
h = ac.send_goal(goal)            # auto UUIDv4; or send_goal_with_id(id, goal)
h.goal_id(); h.goal_secret(); h.server_epoch()
ac.query_status(h); ac.cancel(h); ac.get_result(h); ac.wait_result(h, timeout)  # epoch-validated
for fb in ac.feedback_stream(h):  ...   # goal-filtered; sync .recv() + iterator
```

### 8.7 Action server (includes the 8-fn codec vtable — in scope per owner)
```python
server = aorta.ActionServer.serve(node, options, handler)   # handler(goal, ctx)
# ctx runs on a dedicated per-goal thread (not a dispatch thread) → ctx ops succeed here:
#   ctx.goal_id(); ctx.is_cancel_requested(); ctx.publish_feedback(data, msg)
#   ctx.succeed(result); ctx.abort(code, msg); ctx.cancel(code, msg)
#   ctx.cancelled() await-form needs a new C-ABI fn (§9.8); poll is_cancel_requested meanwhile
#   on handler exception: trampoline catches → ctx.abort(HandlerError, msg)  (NOT swallow — §5.1)
# goal: RetainedActionGoal → goal.goal_id(); goal.payload(); goal.decode()
```
The binding implements all **8 codec fn-ptrs** of `aorta_action_server_codec_t` over generated
FlatBuffers Python bindings, using the **confirmed discriminants**: `ActionStatus` is `repr(i8)`
{Unknown=0, Accepted=1, Executing=2, Canceling=3, Succeeded=4, Aborted=5, Canceled=6};
`ActionReplyCode` is `repr(i16)` {None=0, Ok=1, DuplicateAccepted=2, Succeeded=100, Canceled=101,
Busy=200, PayloadConflict=201, Unauthorized=202, NotFound=203, AlreadyTerminal=204, GoalExpired=205,
HandlerError=300, HandlerPanic=301, HandlerCompletedWithoutResult=302, ContextDropped=303,
ServerCrash=304, Conflict=400}.

### 8.8 Timer / Executor / ExecutionGroup / ContextStore
```python
t = node.create_timer(0.1, on_tick, options=TimerOptions(...))  # seconds-float or timedelta
t.pause(); t.resume(); t.stop(); t.is_running(); t.period()

ex = aorta.SingleThreadedExecutor()   # or MultiThreadedExecutor(workers=N)
ex.add_node(node); ex.spin()          # spin() on a dedicated thread, releases GIL
                                      # spin_once(timeout) → Timeout is a control value, not raised
stop = ex.get_stop_handle(); stop.stop()   # callback-safe; signal handlers use a self-pipe

eg = node.create_execution_group(ExecutionPolicy.SERIALIZED)  # or CONCURRENT
                                      # stamps subscriber/timer/service options with the group

cs = node.create_context_store()
cs.declare_owned_prefix("aorta/.../state")    # MUST precede set
cs.set_string(key, value, expires_at_ns=0); r = cs.get_string(key)   # 5 types × {plain, expiry}
cs.watch(prefix, on_change)                    # + watch_with_initial_snapshot(...)
```

---

## 9. Capability Parity Matrix (the "no missing functionality" contract)

**Scope basis (corrected after SDK-surface audit 2026-06-15):** parity is the **union of the
Rust SDK (`rust/src/*`) and C++ SDK (`cpp/include/aorta/*`) *public API*** — the L3 surface — **NOT
the raw C-ABI**. The C-ABI (`core/src/ffi/c_api/*`) exposes more than the SDKs choose to surface;
C-ABI-only functions that **neither** the Rust **nor** the C++ SDK exposes are **NOT parity items**
and are explicitly excluded (listed in §9.2-note). This corrects an earlier draft that derived the
matrix from the C-ABI union and over-claimed several C-ABI-only Node functions as parity.

**[v1]** = interop-gating core; **[full]** = completes parity (delivered across phases, all in
1.0); **[defer]** = explicitly out-of-scope-for-now with rationale (still tracked). Per owner
decision, **action is delivered (not dropped)** and **no Python-only items are added**.

### 9.0 Foundation / ABI
- [ ] cdylib loader (`AORTA_CORE_FFI_LIB` → package path → `find_library`) **[v1]**
- [ ] ctypes struct mirrors for all `#[repr(C)]` structs, layout-locked (§6.1) **[v1]**
- [ ] ABI-drift CI test parsing `aorta.h` (§6.4) **[v1]**
- [ ] Thread-local `aorta_last_error()` retrieval on rc≠0 **[v1]**

### 9.1 Errors
- [ ] Full exception hierarchy (13 FFI codes), message preserved (is_recoverable NOT exposed — collapses at the FFI; see §5.2) **[v1]**
- [ ] Timeout(spin_once)=value; NOT_FOUND(query/info/result)=None **[v1]**

### 9.2 Node
Parity basis: Rust `Node` public API = `name/id/group/now`, `close`, `create_context_store`,
`create_execution_group`, `ensure_executor`, and the `create_*` factories. C++ `Node` = the same
`Create*` factories + `CreateExecutionGroup`/`FreeExecutionGroup`/`EnsureExecutor`. Neither SDK
exposes validate/liveliness/session-zid/raw-callback-group on Node.
- [ ] `Node(name, group)` + `close()` + context manager **[v1]**
- [ ] telemetry identity: language="python", sdk_name, sdk_version, labels **[v1]**
- [ ] accessors: name/id/group/now **[v1]**
- [ ] `create_context_store()`, `create_execution_group(policy)`, `ensure_executor()` (auto) **[v1]**
- [ ] `create_*` factories (publisher/subscriber/service/client/action/timer) — see §9.3-9.10 **[v1]**
- [ ] lazy single background spin thread; close stops+joins **[v1]**
- [ ] router/endpoint config via env vars (parity, no Python-only ctor) **[v1]**

> **NOT parity (C-ABI-only — excluded):** `aorta_node_validate_topic_name/_pattern`,
> `validate_service_name/_pattern`, `has_event_sender`, `session_zid`, `process_start_time_ns`,
> `declare_liveliness`, `has_liveliness`, raw `create/free_callback_group` on Node. These exist in
> `core/src/ffi/c_api/node.rs` but are surfaced by **neither** the Rust nor the C++ SDK, so per the
> scope basis they are not required. (Callback groups ARE reachable — via `ExecutionGroup` (§9.10)
> and the C++ `Executor`, which is how both SDKs expose them.)

### 9.3 Publisher
- [ ] typed `create_publisher` + `publish_typed` (header injection, sync) **[v1]**
- [ ] `publish_bytes` / typed-msg raw **[v1]**
- [ ] `create_publisher_raw` (no header) **[full]**
- [ ] express via `_with_express` ctor / preset (NOT in qos struct) **[full]**
- [ ] topic(), schema_hash() **[v1]**
- [ ] `publish_owned` zero-copy + release callback — **parity** (C++ `PublishOwned`); delivered in a late phase (large-frame optimization), not out-of-scope **[full, late]**
- [ ] PublisherOptions — **C-ABI-mappable fields only**: `aorta_pub_options_t` = schema_attach_policy(Inherit)/schema_bfbs/max_message_size (`core/src/ffi/c_api/mod.rs:2497`), plus `qos` (QoS object) and `express` (routed via the `_with_express` ctor, NOT the struct). `include_publish_timestamp`/`congestion_control` are **Rust/Core-only** (`core/src/publisher.rs:29`), absent from both the C-ABI and the C++ SDK → **demoted** (not v1 Python options; add a C-ABI fn in a later phase only if a use case needs them). **[v1 mappable]**
- [ ] `stats()` → PublisherStats — **parity** (Rust `Publisher::stats()` + C++ `GetStats()`, both **SDK-computed**, not from the C-ABI). The C-ABI `aorta_publisher_get_stats` is a stub (all zeros), so Python mirrors Rust/C++ by **computing stats SDK-side via Python-level accounting** at the publish call site. Do NOT expose the zeroed FFI struct. **[full]**

### 9.4 Subscriber
- [ ] `create_subscriber` raw (bytes/memoryview, MessageContext) **[v1]**
- [ ] `create_subscriber_typed_view` **[v1]**; `_typed_retained` **[full]**; `_with_qos`/`_with_express` **[full]**
- [ ] undeclare() / drain-on-free (2 s) **[v1]**
- [ ] multi-slice payload handling (iterate slices, never assume single) **[v1]**
- [ ] MessageContext opt-in fields (topic/receive_stamp_ns/priority/publisher_stamp_ns/publisher_node/sequence) **[v1]**
- [ ] SubscriberOptions matrix: callback_group/prefetch_schema_on_init/header-context/dedup+window(1024)/reorder/drop_out_of_order/overflow_policy(Drop)+capacity(1024)/history_depth(1024). **`deadline_ns` is RESERVED/no-op** — the field exists in the struct + C++ options but the FFI builder does **not** consume it (`core/src/ffi/c_api/subscriber.rs`); expose as documented no-op until Core implements it, do NOT advertise deadline semantics. **[v1 core / full all]**
- [ ] runtime_stats() (7 counters) — **no C-ABI bridge today** (`core/src/subscriber.rs:1038` exists; no `aorta_subscriber_*stats` fn). Requires a NEW C-ABI fn (Phase 3) or demotion. **[full — needs C-ABI add]**

### 9.5 Service
- [ ] `create_service` raw; ServiceRequest.reply/reply_error/release (consume handle) **[v1]**
- [ ] `create_service_typed_view` (auto error-reply on decode fail) **[v1]**; `_typed_retained` **[full]**
- [ ] TypedServiceResponder.reply(fill)/reply_message/reply_bytes/reply_error (auto-stamp header + per-responder sequence) **[v1]**
- [ ] ServiceOptions: schema_attach_policy(Never)/callback_group/rpc_overflow_policy(FastFailBusy)+block_timeout/schema. **`deadline_ns` is RESERVED/no-op** (struct+C++ have it; the FFI builder does not consume it, `core/src/ffi/c_api/service.rs`) — documented no-op until Core implements. **[v1]**
- [ ] stats() (11 counters, real) **[full]**

### 9.6 Client
- [ ] `create_client` + `create_client_with_timeout`; call/call_with_timeout (blocking) **[v1]**
- [ ] `create_client_typed` → TypedClientResponse.decode()/payload **[v1]**
- [ ] ClientOptions: timeout(5000ms)/schema_attach(Inherit)/service_schema/root_types/validate_schema(false)/enforce_single_provider(bool, Rust SDK)/multi_reply_check_window/**allow_retry** — tri-state: `None`=inherit Core/env default (unset env → OFF, `core/src/client.rs:481`), `False`=force OFF, `True`=on; **default `None`** (`rust/src/client.rs:29`), preserve OFF-by-default. **[v1]**
- [ ] stats() (9 counters, real) **[full]**

### 9.7 Action client  — **delivered**
- [ ] `ActionClient.action_name()` accessor (`rust/src/action.rs:100`) **[v1]**
- [ ] send_goal (auto UUIDv4) + send_goal_with_id **[v1]**
- [ ] GoalHandle: goal_id/goal_secret/server_epoch **[v1]**; from_persisted **[full]**
- [ ] query_status/cancel/get_result **[v1]**; wait_result (poll→subscribe→cv + epoch validation) **[full]**
- [ ] info()/query_info_with_timeout() **[full]**
- [ ] feedback_stream (goal-filtered) **[v1]**; feedback_stream_all + dropped_count **[full]**; ActionFeedbackStream.recv()/close() + ActionFeedbackSample.payload()/decode() (`rust/src/action.rs:159,198`) **[v1]**
- [ ] ActionClientOptions: control_timeout(10s)/info_timeout(5s)/feedback_capacity(256)/feedback_overflow_policy(DropOldest) **[v1]**
- [ ] server_epoch validation everywhere (mismatch→NOT_FOUND) **[v1]**

### 9.8 Action server  — **delivered**
- [ ] serve(node, options, handler) **[v1]**; serve_info_only **[full]**
- [ ] ActionServer accessors: `action_name()`/`info()`/`stop()`/`stop_and_join(timeout)` (`rust/src/action.rs:72,76,80,84`) **[full]**
- [ ] 8-fn codec vtable over FlatBuffers Python bindings; confirmed i8/i16 discriminants **[full, this milestone]**
- [ ] ActionExecutionContext (dedicated per-goal thread; Flow offload pool before ABI 13): goal_id/is_cancel_requested/publish_feedback/succeed/abort/cancel — bridged via `aorta_action_context_*`. **`cancelled()` (await-on-cancel) is NOT bridged** — C-ABI has only the polling `is_cancel_requested` (`core/src/ffi/c_api/action_server.rs:333`); the async `cancelled()` (`core/src/action_server.rs:178`) needs a NEW C-ABI fn (Phase 4), else Python documents polling `is_cancel_requested` as the equivalent. **[v1 of action; cancelled() needs C-ABI add or poll-equivalent]**
- [ ] RetainedActionGoal: goal_id/payload/decode **[v1 of action]**
- [ ] ActionServerOptions: supports_cancel(true)/max_concurrency(1)/result_ttl(60s)/dedup_ttl(300s≥result_ttl) **[v1 of action]**
- [ ] action helpers: goal_id↔key, resolve_action_root_types_from_bfbs, ActionInfo/PublicStatus/SendGoal/Cancel/GetResult types **[full]**

### 9.9 Timer
- [ ] create_timer(period, cb, options); pause/resume/stop/is_running/period **[v1]**
- [ ] TimerOptions: callback_group/auto_start(true)/invoke_on_missed(false)/overflow_policy(CoalesceLatest)/block_timeout **[v1]**
- [ ] 13 diagnostic counter getters **[full]**

### 9.10 Executor / ExecutionGroup
- [ ] Single/MultiThreadedExecutor; new/new_with_options(capacity, workers, ExecutorOptions) **[v1]**
- [ ] spin()/spin_once(timeout→value)/stop()/add_node()/free() (retry off callback thread) **[v1]**
- [ ] create/free_callback_group; get_admission_stats() (REENTRANT-guarded) **[full]**
- [ ] StopHandle: get_stop_handle/stop (callback-safe; signal→self-pipe) **[v1]**
- [ ] ExecutionGroup: create_execution_group(policy); SERIALIZED→MutuallyExclusive / CONCURRENT→Reentrant; option stampers **[v1]**
- [ ] ExecutorOptions (scheduler_mode Fifo/Priority+WDRR, admission, class_policy) **[full]**; AdmissionStats(8) **[full]**

### 9.11 ContextStore
- [ ] node.create_context_store() (mints source_instance_id) **[v1]**
- [ ] declare_owned_prefix (precedes set) **[v1]**
- [ ] set_{bool,i64,u64,f64,string} ±expiry (5×2) **[v1]**
- [ ] get_{...} → ContextRead ±deadline; encapsulate aorta_ctx_read_t lifecycle (zero→get→copy→free) **[v1]**
- [ ] watch + watch_with_initial_snapshot; WatchHandle.shutdown (not reentrant from own cb) **[v1]**
- [ ] ContextRead fields (value/is_fresh/stamp_ns/expires_at_ns/source_node/source_instance_id/sequence) **[v1]**
- [ ] ContextChange + ContextChangeKind(Live/Snapshot/SnapshotDone) + SnapshotDoneReason **[v1]**
- [ ] shutdown (deterministic) **[full]**
- [ ] `is_key_declared` / `declared_prefixes` exist in Core + Rust SDK (`core/src/contextstore.rs:589,1085`) but have **no C-ABI bridge** → require NEW C-ABI fns (Phase 3) or demotion. **[full — needs C-ABI add]**

### 9.12 Logging — **already feature-complete** (existing `python/logging.py`)
- [x] init_from_env_once/configure_logging/flush/set_default_context/is_enabled/write_log[_at] **[done]**
- [x] level fns + if_/every_n; scope default; lazy %% formatting; level-guard; file:line capture **[done]**
- [x] LogLevel/LogFormat/LogConfig **[done]**
- [ ] (optional) stdlib `logging.Handler` bridge → Core sink **[defer]**

### 9.13 Stats / observability
- [ ] service.stats()/client.stats()/executor.admission_stats() (real) → frozen dataclasses **[full]**
- [ ] publisher.stats() → PublisherStats via **Python-side accounting** (parity with Rust `stats()` / C++ `GetStats()`, which are SDK-computed; C-ABI stub bypassed) **[full]**
- [ ] node telemetry pass-through; auto identity/inventory/runtime-metrics/`/log` are env-driven (zero Python code) — document `AORTA_TELEMETRY_*` / `AORTA_LOG_TOPIC*` **[v1]**
- [ ] action stats — none exist anywhere → out of scope **[defer]**
- [ ] tracing registration — no C-ABI → out of scope **[defer]**

### 9.14 QoS / time / schema
- [ ] QoS dataclass + presets via `aorta_qos_preset[_express]()`; express routed correctly **[v1]**
- [ ] time: now/now_ns/unix_time_ns/simulation get-set/time_source get-set/node.now **[full]**
- [ ] SchemaAttachPolicy(Inherit/Always/Never); honor AORTA_SCHEMA_ATTACH **[v1]**
- [ ] schema hash sha256→64 lowercase hex; reject short **[v1]**

### 9.15 FlatBuffers codegen + schema metadata
- [x] Python flatc accessors (`flatbuffers_python`, ~30 schemas python=True) **[done]**
- [ ] `gen_schema_meta.py --lang=python` → SCHEMA_BFBS/HASH_HEX/ROOT_TYPE_NAME/FIXED_SIZE/HAS_AORTA_HEADER + `schema_meta_py` targets **[v1 — gating]**
- [ ] build_aorta_header helper (byte-identical) **[v1]**
- [ ] action wrapper BFBS (action_schema=True) **[action milestone]**
- [ ] CI: flatbuffers pin == MODULE.bazel tag **[v1]**

### 9.16 Build / packaging
- [ ] `rust_shared_library` cdylib in core/BUILD.bazel (+macOS linkopts) **[v1 — step zero]**
- [ ] `py_wheel` per-arch platform wheel bundling the `.so` package-relative — **Phase 5** (needs the aarch64 cross-build). Until then, Phase-0→4 dev/test use **env/`bazel-bin` cdylib discovery** (`AORTA_CORE_FFI_LIB` / package-relative path the loader already supports). **[Phase-5]**
- [ ] aarch64 cross (`--config=robot`, to be defined in `.bazelrc`; CI/robot-validated) **[Phase-5 gate]**
- [ ] `.pyi`/`py.typed`; ctypes internals private **[full]**
- [ ] CI wheel build+publish (per-arch matrix) **[v1]**

### 9.17 Required NEW C-ABI functions (bridge gaps for full parity)
Some Rust/C++ SDK capabilities have **no C-ABI bridge today**; because Python binds via the C-ABI,
reaching parity means **adding** these `extern "C"` functions to Core — the layer-rule-compliant path
(extend L2, never bypass it). Each is gated to a phase; if an add slips, the item is **demoted with
a documented rationale**, not faked.

| New C-ABI fn (proposed) | Backs (Rust/C++ SDK capability) | Phase |
|---|---|---|
| `aorta_ctx_is_key_declared` | `ContextStore.is_key_declared` (`core/src/contextstore.rs:589`) | 3 |
| `aorta_ctx_declared_prefixes` | `ContextStore.declared_prefixes` (`core/src/contextstore.rs:1085`) | 3 |
| `aorta_subscriber_runtime_stats` | `Subscriber.runtime_stats` (`core/src/subscriber.rs:1038`) | 3 |
| `aorta_action_context_cancelled` (await) | `ActionExecutionContext.cancelled` (`core/src/action_server.rs:178`); interim = poll `is_cancel_requested` | 4 |

Publisher Rust-only options (`include_publish_timestamp` / `congestion_control`) and the reserved
`deadline_ns` (subscriber/service) are **not bridged in v1** — Core does not consume `deadline_ns`,
and the publisher fields are absent from the C++ SDK too. They are documented as reserved/demoted,
never advertised as working.

---

## 10. Examples & Test Strategy

### 10.1 Example parity set (mirrors `rust/examples` + `cpp/examples`)
- **Core:** talker, listener, talker_raw, listener_raw, echo_server, echo_client,
  navigation_service_server/client, cli_execute_task_server/client, timer_demo, all_types_talker.
- **Full:** control_loop, sensor_publisher/subscriber, late_joiner, executor_demo,
  contextstore_demo, performance_demo, throwing_callback_listener (the noexcept regression),
  foxglove publisher.

### 10.2 Testing — non-negotiables (per owner)

1. **TDD Red→Green→Refactor** every phase, with boundary contract tests first.
2. **100% line + branch coverage** on the `python/aorta/` SDK package (pytest + `coverage` with
   the thread tracer enabled for callback-thread code). `examples/` excluded (covered by the
   subprocess interop matrix). Documented `# pragma: no cover` only for `__main__` guards,
   finalizer-ordering, and genuinely race-only branches (logged in the coverage report).
3. **In-process pytest smoke suite** (mirrors C++ `NetworkTestFixture`): pubsub, rpc,
   cross-session-rpc, action, timer, contextstore, node-validation, qos. Sessions serialized (no
   xdist), poll-until-count (never fixed sleeps), ~800 ms cross-session discovery budget, <10 s
   per test.
4. **Cross-language interop matrix** (Py↔Rust↔C++) as `test_scripts/*.sh` running **prebuilt
   binaries only** (never invoke Bazel from a test): pub/sub (marker grep), RPC echo, navigation
   service, action (quick/slow/cancel profiles), contextstore. Gate: **byte-identical AortaHeader
   + 64-hex schema hash** across languages.
5. **Negative / regression suite:** invalid topic, empty name, zero timeout, no-provider
   NotFound, callback-raises-survives (per callback type), schema-hash mismatch, reentrancy,
   double-free idempotence, memoryview-escape contract.
6. **CI enforcement.** The pytest suite is *not* a Bazel `py_test` target (it is pytest-based,
   not `unittest`), so `bazel test //...` does **not** cover it. A dedicated CI workflow
   is the authoritative gate: it Bazel-builds the L2
   cdylib + the Rust interop example binaries, then runs the pytest suite (with the 100%
   line+branch coverage gate) and the `test_scripts/*.sh` interop matrix against those prebuilt
   artifacts. `pytest`/`pytest-cov`/`coverage` stay dev-time pip deps (installed in the CI venv),
   never added to the Bazel pip lockfile.

---

## 11. Phased Delivery Plan (each phase independently testable + reviewed)

Per owner: **phased, hardest parts later, but action IS delivered.** Each phase ends with a
review gate before the next begins.

| Phase | Deliverable | Exit gate |
|---|---|---|
| **0 — Build foundation** | `rust_shared_library` cdylib + `aorta_abi_version`; loader + runtime ABI guard (symbols + version + sizeof/offsets); ctypes ABI mirrors; static ABI-drift tests (vs `abi_assert.rs` + `abi_layout_test.cc`); fold `logging.py` into `aorta/`. (env/`bazel-bin` cdylib discovery; `py_wheel` + `--config=robot` deferred to Phase 5) | cdylib builds+loads on host (macOS arm64 verified); ABI sizes+offsets assert-match the Rust/C++ ABI lock; abi_version guard; (linux/aarch64 cross-build is a Phase-5 CI gate) |
| **1 — Core pub/sub + schema-meta codegen** | `gen_schema_meta.py --lang=python` + `schema_meta_py`; `build_aorta_header`; Node; Publisher (typed+raw); Subscriber (typed-view+raw, MessageContext, multi-slice); exception hierarchy + `aorta_last_error`; callback trampoline discipline; auto-executor; **flatbuffers-pin + schema-hash byte-identity gates** | in-process pubsub smoke (6) + **Py↔Rust↔C++ pubsub interop** (schema-attach never/always); byte-identical header + 64-hex hash; pin/hash gates green |
| **2 — RPC** | Service (raw+typed-view); Client (raw+typed, allow_retry OFF, enforce_single_provider, multi_reply_window); ExecutionGroup; service/client stats | rpc + cross-session-rpc smoke + **echo/navigation interop** both directions |
| **3 — Timer + Executor + ContextStore** | Timer (+counters); Single/Multi executor (spin/spin_once/stop, callback groups, StopHandle self-pipe, admission stats); ContextStore (declare/set/get ±expiry/deadline, watch+snapshot, ContextRead lifecycle); **new C-ABI fns: `aorta_ctx_is_key_declared`/`_declared_prefixes` + `aorta_subscriber_runtime_stats`** | timer/contextstore smoke + contextstore interop; snapshot + sequence semantics correct; new C-ABI fns ABI-guard-checked |
| **4 — Action (client + server, full codec)** | ActionClient (send_goal[_with_id], goal_secret/epoch, query/cancel/result/wait_result, feedback stream); ActionServer (serve + 8-fn codec, ExecutionContext on a dedicated per-goal thread, i8/i16 discriminants); wrapper BFBS; **new C-ABI fn `aorta_action_context_cancelled`** (or documented poll-equivalent) | action smoke (goal/feedback/succeed/cancel/abort/timeout, **handler-exception→HandlerError abort**) + **action interop** (quick/slow/cancel); goal_id/secret/epoch/feedback/cancel byte-compatible cross-lang |
| **5 — Hardening / packaging / coverage / docs** | 100% line+branch on `python/aorta/`; negative/regression suite; `.pyi`/`py.typed`; per-arch wheel build+publish CI; remaining examples; flatbuffers-pin-drift CI; spec updates | coverage gate green; full NxN interop matrix green; wheel installs+runs on robot aarch64 |

**Cross-cutting each phase:** TDD; never fork Core/FFI semantics (DRY); update `spec/` in the same
PR; QoS presets via `aorta_qos_preset[_express]()` only; every callback trampoline GIL-aware +
try/except-swallow + strong-ref-kept-alive + token user_data; every handle wrapper = context
manager + idempotent `close()` + `weakref.finalize` guard + forbid close-from-callback-thread.

---

## 12. Open Questions (status after owner ratification 2026-06-15)

| # | Question | Status |
|---|---|---|
| Q1 | PyO3-vs-ctypes spec contradiction | **RESOLVED — ctypes over C-ABI.** |
| Q2 | C-ABI node config-string ctor | **RESOLVED — not a gap; use env-var path (parity with Rust), no Python-only ctor.** |
| Q3 | Publisher stats (C-ABI stub all-zeros) | **RESOLVED (corrected) — publisher.stats() IS parity: Rust `stats()` and C++ `GetStats()` both expose it, SDK-computed. Python implements it via Python-side accounting at the publish call site; the zeroed C-ABI struct is bypassed, never exposed.** |
| Q4 | v1 scope incl. action server codec? | **RESOLVED — phased; action delivered in Phase 4 (not dropped).** |
| Q5 | asyncio in v1? | **RESOLVED — out of scope (Python-only; not in Rust/C++ contract).** |
| Q6 | memoryview retention policy | **RESOLVED — default copy-on-deliver; opt-in zero-copy with lifetime contract.** |
| Q7 | Wheel naming / Python versions / distribution | **RESOLVED — dist name `aorta_sdk`, import `aorta`; CPython 3.10+; `py3-none-linux_{x86_64,aarch64}` (NOT `any`, carries a native `.so`); internal index + robot-image bundle first; manylinux/auditwheel a separate later gate.** |
| Q8 | Where Python schema-meta files land | **RESOLVED — alongside the flatc Python output as `<namespace>/<stem>_schema_meta.py` (lowercase stem, consistent with `*_schema_meta.rs/.h`), importable next to the generated message module.** |
| Q9 | Where interop example binaries live for `test_scripts` | **RESOLVED — examples in `python/examples/` as `py_binary` (mirror rust/cpp); `test_scripts/*.sh` call prebuilt `bazel-bin/{python,rust,cpp}/examples/...`; loopback config reuses the C++ convention (127.0.0.1 fixed port, multicast/gossip/shm disabled — `cpp/examples/local_zenoh_config.h:40`).** |
| Q10 | Coverage gate boundary | **RESOLVED — 100% line+branch on hand-written `python/aorta/` only (loader, ctypes structs/signatures, errors, callbacks, lifetime wrappers); EXCLUDE generated flatc modules, generated `*_schema_meta.py`, examples, test scripts, bundled cdylib; documented `# pragma: no cover` for `__main__`/finalizer-ordering/race-only branches.** |
| Q11 | express routing on custom QoS | **RESOLVED — route via `_with_express`/preset; never into `aorta_qos_t` (don't copy C++ `ToFfi()` omission).** |

---

## 13. Risks & Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| Python exception crosses C boundary → abort | **Critical** | Mandatory try/except-swallow trampoline + per-callback regression test (throwing_callback_listener parity). |
| ABI drift core↔ctypes mirror | High | CI test parses `aorta.h`, asserts sizes/offsets/signatures (§6.4). |
| Callback object GC'd → dangling Rust call | High | Strong-ref kept on owner; token-based `user_data` + global registry. |
| Schema BFBS byte-mismatch → silent wire incompat | High | Reuse the shared `:stem_bfbs` artifact (never regenerate); flatbuffers-pin + schema-hash gates at **Phase-1 exit**. |
| C-ABI bridge gap (SDK capability has no `extern "C"` fn) | High | §9.17 enumerates the adds per phase; runtime ABI guard verifies the symbol set; demote-with-rationale if an add slips. |
| Action handler exception masked as "completed without result" | High | Action-execute trampoline catches → `ctx.abort(HandlerError, msg)` (§5.1); dedicated regression test. |
| `deadline_ns` advertised but no-op in Core | Medium | Documented reserved/no-op (§9.4/§9.5); not surfaced as a working option until Core consumes it. |
| Action codec vtable complexity | Medium | Discriminants confirmed (i8/i16); built over generated FB bindings; isolated Phase 4 with interop gate. |
| GIL stalls high-rate callbacks | Medium | Documented honestly; recommend C++ for sub-ms paths; deferral-queue pattern. |
| aarch64 wheel runtime divergence | Medium | Wheel install+run smoke on the robot in Phase 5 exit gate. |

---

## 14. Acceptance (definition of done for 1.0)

1. Every **[v1]** and **[full]** box in §9 implemented (action included).
2. 100% line+branch coverage on `python/aorta/`.
3. In-process pytest smoke suite green; full Py↔Rust↔C++ interop matrix green (byte-identical
   header + 64-hex schema hash).
4. Per-arch wheels build in CI and install+run on the robot (linux/aarch64).
5. `spec/` updated (this doc finalized).
6. Each phase passed its review gate.
