//! Test-only ContextStore owner used by `tools/aorta-cli/tests/cli_ctx_e2e.sh`.
//!
//! Declares one or more owned prefixes and writes a fixed set of canonical
//! values, then either exits (oneshot) or holds them and re-writes on a
//! ticker until SIGTERM. Output is a single READY line on stdout so the
//! test driver knows the owner has completed declare bootstrap.
//!
//! Flags:
//!   --name <node-name>            (default: cli_ctx_owner)
//!   --group <group>               (default: default)
//!   --prefix <p> [--prefix <q>]   (default: demo/**)
//!   --set <key>=<kind>:<value>    (repeatable)
//!   --tick-ms <ms>                (default: 0 — write once and hold)
//!   --duration-ms <ms>            (default: 0 — run until ctrl-c / sigterm)

use std::time::Duration;

use aorta_sdk::contextstore::ContextStore;
use aorta_sdk::Node;

mod cli_args;
use cli_args::ExampleArgs;

#[derive(Debug, Clone)]
enum SetSpec {
    Bool(String, bool),
    I64(String, i64),
    U64(String, u64),
    F64(String, f64),
    Str(String, String),
}

fn parse_set(spec: &str) -> Result<SetSpec, String> {
    // `key=kind:value`. The first `=` splits the key from the rest;
    // the first `:` after that splits kind from value.
    let (key, rest) = spec
        .split_once('=')
        .ok_or_else(|| format!("--set '{}' missing '='", spec))?;
    let (kind, value) = rest
        .split_once(':')
        .ok_or_else(|| format!("--set '{}' missing kind:value", spec))?;
    let key = key.trim().to_string();
    match kind.trim() {
        "bool" => Ok(SetSpec::Bool(
            key,
            value.parse().map_err(|_| format!("bad bool: {}", value))?,
        )),
        "i64" => Ok(SetSpec::I64(
            key,
            value.parse().map_err(|_| format!("bad i64: {}", value))?,
        )),
        "u64" => Ok(SetSpec::U64(
            key,
            value.parse().map_err(|_| format!("bad u64: {}", value))?,
        )),
        "f64" => Ok(SetSpec::F64(
            key,
            value.parse().map_err(|_| format!("bad f64: {}", value))?,
        )),
        "string" => Ok(SetSpec::Str(key, value.to_string())),
        other => Err(format!("unknown kind: {}", other)),
    }
}

async fn apply_set(store: &ContextStore, spec: &SetSpec) -> Result<(), Box<dyn std::error::Error>> {
    match spec {
        SetSpec::Bool(k, v) => store.set_bool(k, *v).await?,
        SetSpec::I64(k, v) => store.set_i64(k, *v).await?,
        SetSpec::U64(k, v) => store.set_u64(k, *v).await?,
        SetSpec::F64(k, v) => store.set_f64(k, *v).await?,
        SetSpec::Str(k, v) => store.set_string(k, v).await?,
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "cli_ctx_owner".to_string();
    let mut group = "default".to_string();
    let mut prefixes: Vec<String> = Vec::new();
    let mut sets: Vec<SetSpec> = Vec::new();
    let mut tick_ms: u64 = 0;
    let mut duration_ms: u64 = 0;

    let mut args = ExampleArgs::new();
    while let Some(arg) = args.next_flag() {
        match arg.as_str() {
            "--name" => {
                if let Some(value) = args.next_value() {
                    node_name = value;
                }
            }
            "--group" => {
                if let Some(value) = args.next_value() {
                    group = value;
                }
            }
            "--prefix" => {
                if let Some(value) = args.next_value() {
                    prefixes.push(value);
                }
            }
            "--set" => {
                if let Some(value) = args.next_value() {
                    sets.push(parse_set(&value)?);
                }
            }
            "--tick-ms" => {
                if let Some(value) = args.next_value() {
                    tick_ms = value.parse()?;
                }
            }
            "--duration-ms" => {
                if let Some(value) = args.next_value() {
                    duration_ms = value.parse()?;
                }
            }
            other => {
                eprintln!("unknown arg: {}", other);
                std::process::exit(2);
            }
        }
    }

    if prefixes.is_empty() {
        prefixes.push("demo/**".to_string());
    }

    let node = Node::new(node_name, group).await?;
    let store = node.create_context_store();
    for p in &prefixes {
        store.declare_owned_prefix(p).await?;
    }
    for s in &sets {
        apply_set(&store, s).await?;
    }
    println!("READY source_instance_id={}", store.source_instance_id());

    let deadline = if duration_ms > 0 {
        Some(tokio::time::Instant::now() + Duration::from_millis(duration_ms))
    } else {
        None
    };
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    if tick_ms > 0 {
        let mut tick = tokio::time::interval(Duration::from_millis(tick_ms));
        tick.tick().await; // skip the immediate first tick
        loop {
            let timed_out = match deadline {
                Some(dl) => tokio::time::Instant::now() >= dl,
                None => false,
            };
            if timed_out {
                break;
            }
            tokio::select! {
                _ = &mut shutdown => break,
                _ = tick.tick() => {
                    for s in &sets { apply_set(&store, s).await?; }
                }
            }
        }
    } else {
        match deadline {
            Some(dl) => {
                tokio::select! {
                    _ = &mut shutdown => {},
                    _ = tokio::time::sleep_until(dl) => {},
                }
            }
            None => {
                let _ = (&mut shutdown).await;
            }
        }
    }

    store.shutdown().await?;
    Ok(())
}
