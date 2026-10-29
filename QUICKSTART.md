# Aorta Quickstart

> Supported platforms: Linux x86_64 and aarch64.

Goal: from a fresh clone to a live talker/listener pair, a working RPC call, and
an Aorta recording open in Foxglove Studio — in about 30 minutes, most of which
is the first Bazel build.

The commands below use the public Bazel SDK and example targets.

The initial public release does not accept external code contributions or pull
requests; external pull requests will be closed without review. Use GitHub
Issues for bugs and feature requests, and [SECURITY.md](SECURITY.md) for private
vulnerability reports. We plan to open contributions later with an individual
or corporate CLA; see [CONTRIBUTING.md](CONTRIBUTING.md) for future instructions.

## Contents

1. [Prerequisites](#1-prerequisites)
2. [Clone and build](#2-clone-and-build)
3. [Start a router](#3-start-a-router)
4. [Point nodes at the router](#4-point-nodes-at-the-router)
5. [Run talker and listener](#5-run-talker-and-listener)
6. [Inspect the system with the CLI](#6-inspect-the-system-with-the-cli)
7. [Make an RPC call](#7-make-an-rpc-call)
8. [Record to MCAP and open it in Foxglove](#8-record-to-mcap-and-open-it-in-foxglove)
9. [Troubleshooting](#9-troubleshooting)
10. [Where to go next](#10-where-to-go-next)

---

## 1. Prerequisites

| Requirement | Notes |
|:---|:---|
| **Linux, x86_64 or aarch64** | Linux is what the release tooling validates and supports for consumers (`x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`). |
| **[bazelisk](https://github.com/bazelbuild/bazelisk)** | The repo pins Bazel via `.bazelversion` (currently `7.6.1`); bazelisk reads it and fetches the right release. Commands below say `bazel`, which is how bazelisk is normally installed. |
| **A C++ toolchain** | Whatever your distro ships (`build-essential` on Debian/Ubuntu). |
| **Working network access** | The first build downloads the Bazel module graph, the Rust toolchain, and the crate universe. |
| **[Foxglove Studio](https://foxglove.dev)** | Only needed for step 8. Desktop or web. |

You do **not** need a preinstalled Rust toolchain, Cargo, or `flatc`: Aorta's
`MODULE.bazel` pins `rules_rust` 0.68.1, declares `rust.toolchain(versions =
["1.92.0"])`, and calls `register_toolchains("@rust_toolchains//:all")`, and it
pins `flatbuffers` 25.12.19 as a Bazel module, so Bazel builds `flatc` for you.

The complete `bazel build //...` and SDK wheel targets also collect third-party
licenses. For those builds, put cargo-about 0.9.2 and a Cargo that can read the lockfile on PATH
(`cargo install cargo-about --version 0.9.2 --locked`). Use the local spawn strategy
already selected by `.bazelrc`; the collector needs the complete Cargo workspace,
including targets across Bazel subpackages. Its filegroups are not a sandboxed
workspace manifest. These extra tools are not required by the focused Quickstart
SDK and example targets. Public wheel CI installs and verifies the pinned
cargo-about archive before invoking Bazel.

### Note for Bazel consumers embedding Aorta

The toolchain registration above only applies when **Aorta is the root module**,
which is the case for this quickstart. Bzlmod does not inherit
`register_toolchains` from a dependency, so if you later add Aorta to your own
project as a `bazel_dep`, **your root `MODULE.bazel` must declare `rules_rust`
and register a Rust toolchain itself** — otherwise the Rust half of the SDK will
not build. The exact snippet to copy (including the `rules_rust` `git_override`
and patch that Aorta's own build depends on) is published in the
`## Consumer snippet` section of every Aorta GitHub Release. The supported root toolchains are
`x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`.

---

## 2. Clone and build

```bash
git clone --branch v0.1.0 https://github.com/VitaDynamics/Aorta.git
cd Aorta
```

Public source snapshots include `MIRROR_RELEASE.json` and
`MIRROR_RELEASE_NOTES.md`, which record the public version and included schema
names.

Build the four binaries this quickstart uses. Expect the first build to take a
while — it fetches and compiles the Rust toolchain, Zenoh, and FlatBuffers.

```bash
bazel build \
  //tools/zenohd:zenohd \
  //tools/aorta-cli:aorta \
  //cpp/examples:talker \
  //rust/examples:listener
```

The RPC pair in step 7 needs two more:

```bash
bazel build //cpp/examples:echo_server //rust/examples:echo_client
```

Note that the talker is C++ and the listener is Rust. That is deliberate: they
share one FlatBuffers schema (`//message:demo_chat_msg_cc` and
`//message:demo_chat_msg_rs`) and talk over the wire with no language-specific
glue.

---

## 3. Start a router

Aorta rides on Zenoh. The `aorta` CLI connects in **client** mode by default and
reads its discovery snapshots from the router's admin space, so a router has to
be running for `topic list` / `service list` / `nodes list` to return anything.

Open terminal 1 and leave it running:

```bash
bazel run //tools/zenohd:zenohd -- \
  --listen tcp/127.0.0.1:7447 \
  --no-multicast-scouting
```

`--no-multicast-scouting` keeps the quickstart to one machine and off your LAN.
Admin-space read permission is on by default, which is all the CLI needs.

---

## 4. Point nodes at the router

Aorta reads its Zenoh session config from the `ZENOH_SESSION_CONFIG_URI`
environment variable. With that variable unset, a node falls back to stock Zenoh
peer defaults — multicast scouting on, no authentication, no access control —
and logs a warning saying so. Give it an explicit client config instead:

```bash
cat > /tmp/aorta_quickstart_client.json5 <<'EOF'
{
  mode: "client",
  connect: {
    endpoints: ["tcp/127.0.0.1:7447"]
  },
  scouting: {
    multicast: { enabled: false },
    gossip: { enabled: false }
  },
  transport: {
    shared_memory: { enabled: false }
  }
}
EOF
```

Export it in **every terminal that runs an Aorta node** from here on:

```bash
export ZENOH_SESSION_CONFIG_URI=/tmp/aorta_quickstart_client.json5
```

The CLI takes the same file through a flag rather than the environment:
`aorta --zenoh-config /tmp/aorta_quickstart_client.json5 ...`.

---

## 5. Run talker and listener

Terminal 2 — the C++ talker publishes a `demo.Chat` message on `/demo/chatter`
every 500 ms:

```bash
export ZENOH_SESSION_CONFIG_URI=/tmp/aorta_quickstart_client.json5
bazel run //cpp/examples:talker
```

Terminal 3 — the Rust listener subscribes to the same topic:

```bash
export ZENOH_SESSION_CONFIG_URI=/tmp/aorta_quickstart_client.json5
bazel run //rust/examples:listener -- --header-context
```

For every message, the listener prints the receive metadata followed by the
sequence number and publisher node name that Aorta injected into the message
header — representative output looks like:

```
[aorta/default/pub/demo/chatter] Received at priority Normal, local_ts=..., pub_ts=...
  ctx header: seq=12 node=cpp_talker
  seq=12 node=cpp_talker text=hello 12 from cpp_talker
```

Both binaries accept `--name` and `--topic` if you want to rename the nodes or
move them onto a different topic. To run them in a different group, set the
`AORTA_GROUP` environment variable before starting the process: the runtime
resolves the effective group from `AORTA_GROUP` only (falling back to
`default`). The examples also parse a `--group` flag, but it is currently
ignored by the runtime, so do not rely on it. `--header-context` is what makes
the listener surface the `AortaHeader` fields; drop it and you still get the
payload.

Stop either one with `Ctrl+C`.

---

## 6. Inspect the system with the CLI

Leave the talker and listener running, and use terminal 4. The `aorta` CLI is
the diagnostic tool — it needs no Aorta node of its own, just a Zenoh session.

The CLI takes flags of its own, so call the built binary directly rather than
fighting `bazel run` over argument parsing:

```bash
alias aorta='bazel-bin/tools/aorta-cli/aorta --zenoh-config /tmp/aorta_quickstart_client.json5'
```

(`bazel-bin/tools/aorta-cli/aorta` is the binary that `bazel build
//tools/aorta-cli:aorta` produced in step 2. Run the alias from the repo root.)

List what is online:

```bash
aorta nodes list          # nodes holding a liveliness token
aorta topic list          # declared publishers, from the router admin space
aorta topic info /demo/chatter
```

Watch the traffic, decoded from FlatBuffers to JSON using the schema the
publisher registered:

```bash
aorta topic echo /demo/chatter --count 5
```

Measure the publish rate — you should see roughly 2 Hz, matching the talker's
500 ms timer:

```bash
aorta topic hz /demo/chatter
```

And read the schema itself:

```bash
aorta schema get /demo/chatter --describe
```

`topic echo` and `topic hz` stream until you interrupt them (`topic echo` also
stops at `--count`). Every discovery command takes `--group`; everything in this
quickstart stays in the default group, `default`.

---

## 7. Make an RPC call

Aorta services are typed request/response over Zenoh queries. Start the C++ echo
server in terminal 5:

```bash
export ZENOH_SESSION_CONFIG_URI=/tmp/aorta_quickstart_client.json5
bazel run //cpp/examples:echo_server
```

It serves `/echo`, taking a `demo.EchoRequest { message: string }` and replying
with a `demo.EchoResponse { echoed_message: string }`.

Call it from the CLI — the CLI encodes your JSON into FlatBuffers using the
registered schema and decodes the reply back to JSON:

```bash
aorta service list
aorta service info /echo
aorta service call /echo '{"message":"hello from the CLI"}'
```

Then call it from a real client. Terminal 6 runs the Rust `echo_client`, which
sends one request per second and logs each round trip:

```bash
export ZENOH_SESSION_CONFIG_URI=/tmp/aorta_quickstart_client.json5
bazel run //rust/examples:echo_client -- --interval-ms 1000
```

Again: C++ server, Rust client, one schema.

---

## 8. Record to MCAP and open it in Foxglove

With the talker still publishing, record 20 seconds of traffic to an MCAP file.
`aorta record` is a subcommand of the same CLI binary:

```bash
aorta record -o /tmp/aorta_quickstart.mcap --duration 20
```

For a typed topic like this quickstart's `/demo/chatter`, the recorder resolves
the publisher's registered schema and writes the FlatBuffers schema (BFBS) into
the MCAP alongside the messages, so the file is self-describing. (Schema
resolution is best-effort: a topic whose schema cannot be resolved is still
recorded, but as raw bytes — `aorta_raw_bytes` encoding with no embedded
schema — and such channels cannot be decoded field by field.)

Open `/tmp/aorta_quickstart.mcap` in Foxglove Studio (**Open local file**). The
**Raw Messages**, **Plot**, and **State Transitions** panels decode any custom
FlatBuffers channel that carries its BFBS schema — `demo.Chat` here — so you can
inspect `/demo/chatter` field by field and plot the header's sequence number
over time.

One honest limitation: Foxglove panels that key off well-known schema names —
Image, 3D — only light up for schemas that follow the `foxglove.*` convention.
`demo.Chat` is not one of them. Aorta does ship the official Foxglove schemas
(`RawImage`, `CompressedVideo`, `PointCloud`, `LaserScan`, `FrameTransforms`,
and more, under `message/schemas/topic/foxglove_schema/`), and topics published
with those render in the full panel set.

To replay a recording back onto the wire instead of viewing it statically:

```bash
aorta play /tmp/aorta_quickstart.mcap
```

If you would rather watch live than record first, the repo also contains
`tools/aorta-foxglove-bridge`, a WebSocket bridge that advertises every Aorta
topic straight into Foxglove Studio. It is outside the validated public target
set this quickstart sticks to — see
[`tools/aorta-foxglove-bridge/README.md`](tools/aorta-foxglove-bridge/README.md)
for how to run it and what it does and does not support yet.

---

## 9. Troubleshooting

**`topic list` / `service list` / `nodes list` come back empty.**
These read the router's admin space. Check that `zenohd` from step 3 is still
running, that every process is using the same config file, and that the CLI got
`--zenoh-config`. If your router runs with admin space disabled or write-only,
start it with `--adminspace-permissions r`. The direct commands (`topic echo`,
`service call`, `schema get`) do not need the admin space and will keep working
either way.

**Nodes log a warning about "stock Zenoh peer defaults".**
`ZENOH_SESSION_CONFIG_URI` is unset in that terminal. Re-export it (step 4).
The node still runs, but it is scouting your LAN rather than talking to your
router.

**`error: flatc compiler not found`.**
This only comes up outside the Bazel flow (for example, running `cargo` directly:
`core/build.rs` invokes `flatc`). Do not install a distro/homebrew `flatc` — it
will not match the FlatBuffers version the repo pins (25.12.19 via
`MODULE.bazel`). Build the pinned one with Bazel and point the `FLATC`
environment variable at it:

```bash
bazel build @flatbuffers//:flatc
export FLATC="$PWD/$(bazel cquery --output=files @flatbuffers//:flatc 2>/dev/null | head -1)"
export AORTA_FLATBUFFERS_ROOT="$(bazel info output_base)/external/flatbuffers~"
```

Cargo builds require Rust 1.92.0 and the pinned `FLATC` above.

**Tests or examples hang or fail to discover each other.**
Multiple concurrent Zenoh sessions can conflict on the same machine. Shut down
stray example processes before starting new ones, and run test suites serially.

**Stale shared-memory segments after a crash.**
`aorta clean shm` — a local-only operation, no router needed.

**The build runs out of disk.**
The Bazel repository cache for this project is large. Check `df -h` before a
cold build.

---

## 10. Where to go next

- [`cpp/README.md`](cpp/README.md) — the C++ SDK guide: nodes, publishers,
  services, executors, QoS, actions, ContextStore, tracing, and the C++20 Flow
  coroutine surface.
- [`rust/README.md`](rust/README.md) — the Rust SDK guide, including how to
  depend on `@aorta//rust:aorta_sdk` from your own Bazel module.
- [`tools/aorta-cli/README.md`](tools/aorta-cli/README.md) — the full CLI
  surface, including the action workflow.
- [`message/README.md`](message/README.md) — defining your own schemas and
  wiring up codegen.
- [`README.md`](README.md#build-and-test) — running the test suite.
