use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Directory {
    topics: Vec<String>,
    services: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    name: String,
    live: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    topics: Vec<Entry>,
    services: Vec<Entry>,
}

/// Canonicalise interface names to the one spelling the rest of the CLI uses:
/// `/`-prefixed, `normalize_path`-normalised, no trailing slash — exactly what
/// `admin_public_name` emits and what `KeyExprBuilder` normalises to. Duplicate
/// spellings of the same interface collapse to a single row; first-seen order
/// is preserved so the served snapshot still follows the allowlist file.
fn canonical_allowlist(names: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    names
        .iter()
        .map(|name| crate::util::to_user_key(name))
        .filter(|name| seen.insert(name.clone()))
        .collect()
}

fn project(allowed: &[String], live: &BTreeSet<String>) -> Vec<Entry> {
    allowed
        .iter()
        .map(|name| Entry {
            name: name.clone(),
            live: live.contains(name),
        })
        .collect()
}

/// Router adminspace publisher/queryable declarations.
const SOURCE_ADMINSPACE: &str = "adminspace";
/// Node telemetry publisher/subscriber and provider/client inventories.
const SOURCE_TELEMETRY: &str = "telemetry";
/// The trusted interface directory (`sys/edu_directory`).
const SOURCE_DIRECTORY: &str = "directory";

#[derive(Debug, Serialize)]
pub struct DiscoveryEntry {
    pub name: String,
    pub live: bool,
    pub sources: Vec<&'static str>,
}

impl DiscoveryEntry {
    fn absent(name: String) -> Self {
        Self {
            name,
            live: false,
            sources: Vec::new(),
        }
    }

    fn note_source(&mut self, source: &'static str) {
        if !self.sources.contains(&source) {
            self.sources.push(source);
        }
    }
}

async fn open_directory_session(group: &str) -> Result<(zenoh::Session, String)> {
    let mut config = aorta_core::Session::config_from_env_or_local_default().await?;
    let namespace: Option<String> =
        serde_json::from_str(&config.get_json("namespace").map_err(|e| anyhow!("{e}"))?)?;
    // Router adminspace is global even when application keys have a namespace.
    config
        .insert_json5("namespace", "null")
        .map_err(|e| anyhow!("{e}"))?;
    let session = crate::cmd::open_session_with_config(config).await?;
    let wire_prefix = match namespace.filter(|value| !value.is_empty()) {
        Some(namespace) => format!("{namespace}/aorta/{group}/"),
        None => format!("aorta/{group}/"),
    };
    Ok((session, wire_prefix))
}

pub(super) async fn fetch(
    session: &zenoh::Session,
    group: &str,
    kind: &str,
    timeout: Duration,
) -> Result<Vec<Entry>> {
    aorta_introspect::ids::GroupName::new(group)?;
    let key = format!("aorta/{group}/sys/edu_directory");
    // Gather from every directory server rather than the single best-matching
    // one, and keep each reply distinct: consolidation would collapse replies
    // that share the directory key, letting one rogue queryable hide the real
    // directory's answer. Same reasoning as the core schema queryable.
    let replies = session
        .get(key)
        .target(zenoh::query::QueryTarget::All)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(timeout)
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let mut entries = Vec::new();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            // An unparsable reply is treated exactly like an error reply:
            // skipped with a warning. Otherwise any node able to declare the
            // directory key — and any corrupt frame from the real directory —
            // could deny topic/service discovery to every client.
            match serde_json::from_slice::<Snapshot>(&sample.payload().to_bytes()) {
                Ok(snapshot) => entries.extend(if kind == "topic" {
                    snapshot.topics
                } else {
                    snapshot.services
                }),
                Err(err) => eprintln!(
                    "Ignoring malformed interface directory reply on {}: {}",
                    sample.key_expr(),
                    err
                ),
            }
        }
    }
    Ok(entries)
}

pub(super) fn merge(
    admin: BTreeSet<String>,
    telemetry: BTreeSet<String>,
    directory: Vec<Entry>,
) -> Vec<DiscoveryEntry> {
    let mut rows: BTreeMap<String, DiscoveryEntry> = BTreeMap::new();
    // Adminspace and telemetry both observe interfaces that exist right now,
    // so either one marks the row live; the directory contributes allowlisted
    // names that may legitimately be absent.
    for (names, source) in [(admin, SOURCE_ADMINSPACE), (telemetry, SOURCE_TELEMETRY)] {
        for name in names {
            let row = rows
                .entry(name.clone())
                .or_insert_with(|| DiscoveryEntry::absent(name));
            row.live = true;
            row.note_source(source);
        }
    }
    for entry in directory {
        // A directory may have been served from a non-canonical allowlist
        // spelling; canonicalise on the way in so it lands on the same row as
        // the adminspace/telemetry name instead of duplicating it as absent.
        let name = crate::util::to_user_key(&entry.name);
        let row = rows
            .entry(name.clone())
            .or_insert_with(|| DiscoveryEntry::absent(name));
        row.live |= entry.live;
        row.note_source(SOURCE_DIRECTORY);
    }
    rows.into_values().collect()
}

/// One JSON row of `topic list` / `service list`.
///
/// `name` / `live` / `sources` is the current contract. The legacy rows
/// carried a single `topic` (resp. `service`) field; it is still emitted as a
/// deprecated alias of `name` so existing scripts and downstream tooling do
/// not break silently on the rename.
pub fn discovery_row(entry: &DiscoveryEntry, kind: &str) -> serde_json::Value {
    let mut row = serde_json::to_value(entry).expect("discovery entry serializes");
    row[legacy_name_field(kind)] = serde_json::Value::String(entry.name.clone());
    row
}

fn legacy_name_field(kind: &str) -> &'static str {
    if kind == "topic" {
        "topic"
    } else {
        "service"
    }
}

pub fn empty_hint(kind: &str) {
    eprintln!("No {kind} entries found in adminspace, telemetry or the interface directory; check providers or the trusted directory service.");
}

fn admin_public_name(key: &str, prefix: &str) -> Option<String> {
    let (_, path) = key.strip_prefix("@/")?.split_once('/')?;
    Some(format!("/{}", path.strip_prefix(prefix)?))
}

async fn declared_names(
    session: &zenoh::Session,
    wire_prefix: &str,
    kind: &str,
    primitive: &str,
    timeout: Duration,
) -> Result<BTreeSet<String>> {
    let prefix = format!("router/{kind}/{wire_prefix}{primitive}/");
    let replies = session
        .get(format!("@/**/{prefix}**"))
        .timeout(timeout)
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let mut names = BTreeSet::new();
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            if let Some(name) = admin_public_name(sample.key_expr().as_str(), &prefix) {
                names.insert(name);
            }
        }
    }
    Ok(names)
}

/// Read, validate and canonicalise an allowlist file.
///
/// Validation goes through the same `KeyExprBuilder` the publish/serve side
/// uses, so a name this accepts is exactly a name a provider could declare;
/// canonicalisation then stores the single spelling `project()` and `merge()`
/// compare against.
fn load_directory(path: &Path, group: &str) -> Result<Directory> {
    let directory: Directory = serde_json::from_slice(&std::fs::read(path)?)?;
    let builder = aorta_core::KeyExprBuilder::new(group)?;
    for name in &directory.topics {
        builder.topic_key_pub(name)?;
    }
    for name in &directory.services {
        builder.service_key_server(name)?;
    }
    Ok(Directory {
        topics: canonical_allowlist(&directory.topics),
        services: canonical_allowlist(&directory.services),
    })
}

pub async fn serve(path: &Path, group: Option<&str>) -> Result<()> {
    let group = group.unwrap_or("default");
    let directory = load_directory(path, group)?;
    let (session, wire_prefix) = open_directory_session(group).await?;
    let session = Arc::new(session);
    let key = format!("{wire_prefix}sys/edu_directory");
    let admin_session = session.clone();
    let snapshot = Arc::new(tokio::sync::RwLock::new(None::<Vec<u8>>));
    let cache = snapshot.clone();
    let refresh = tokio::spawn(async move {
        loop {
            let topics = declared_names(
                &admin_session,
                &wire_prefix,
                "publisher",
                "pub",
                Duration::from_secs(1),
            )
            .await;
            let services = declared_names(
                &admin_session,
                &wire_prefix,
                "queryable",
                "srv",
                Duration::from_secs(1),
            )
            .await;
            let payload = match (topics, services) {
                (Ok(topics), Ok(services)) => serde_json::to_vec(&Snapshot {
                    topics: project(&directory.topics, &topics),
                    services: project(&directory.services, &services),
                })
                .ok(),
                _ => None,
            };
            *cache.write().await = payload;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    let queryable = session
        .declare_queryable(key.clone())
        .await
        .map_err(|e| anyhow!("{e}"))?;
    eprintln!("Serving interface directory on {key}");
    serve_queries(queryable, snapshot, key).await;
    refresh.abort();
    Ok(())
}

async fn serve_queries(
    queryable: zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>,
    snapshot: Arc<tokio::sync::RwLock<Option<Vec<u8>>>>,
    key: String,
) {
    while let Ok(query) = queryable.recv_async().await {
        let reply_result = match snapshot.read().await.clone() {
            Some(payload) => query.reply(key.clone(), payload).await,
            None => query.reply_err("directory snapshot unavailable").await,
        };
        if let Err(err) = reply_result {
            eprintln!("Interface directory reply failed: {err}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn discovery_reuses_fixed_peer_listener() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("tcp/{}", listener.local_addr().unwrap());
        drop(listener);
        let config = zenoh::Config::from_json5(
            &serde_json::json!({
                "mode": "peer",
                "listen": {"endpoints": [endpoint], "exit_on_failure": true},
                "scouting": {"multicast": {"enabled": false}}
            })
            .to_string(),
        )
        .unwrap();
        crate::cmd::set_client_mode_override(false);
        let session = crate::cmd::open_session_with_config(config).await.unwrap();
        let result = fetch(&session, "default", "topic", Duration::from_millis(100)).await;
        session.close().await.unwrap();
        crate::cmd::set_client_mode_override(true);
        assert!(
            result.is_ok(),
            "discovery must reuse the bound peer session: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn directory_survives_reply_failure() {
        let mut config = zenoh::Config::default();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();
        let session = zenoh::open(config).await.unwrap();
        let key = "aorta/default/sys/edu_directory";
        let queryable = session.declare_queryable(key).await.unwrap();
        let snapshot = Arc::new(tokio::sync::RwLock::new(Some(b"{}".to_vec())));
        // A mismatched reply key makes Zenoh reject a real reply deterministically.
        let task = tokio::spawn(serve_queries(
            queryable,
            snapshot.clone(),
            "wrong/key".into(),
        ));
        let replies = session
            .get(key)
            .timeout(Duration::from_millis(100))
            .await
            .unwrap();
        assert!(replies.recv_async().await.is_err());
        *snapshot.write().await = None;
        let replies = session
            .get(key)
            .timeout(Duration::from_secs(1))
            .await
            .unwrap();
        let reply = replies
            .recv_async()
            .await
            .expect("server must answer the next query after a reply failure");
        assert!(
            reply.result().is_err(),
            "the next query reports the unavailable snapshot"
        );
        task.abort();
        let _ = task.await;
        session.close().await.unwrap();
    }

    fn write_allowlist(name: &str, body: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "aorta_allowlist_{}_{}_{}.json",
            std::process::id(),
            name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn discovery_rows_keep_the_pre_wp7b_name_field_as_an_alias() {
        let entry = DiscoveryEntry {
            name: "/chatter".into(),
            live: true,
            sources: vec![SOURCE_ADMINSPACE],
        };
        assert_eq!(
            discovery_row(&entry, "topic"),
            serde_json::json!({
                "name": "/chatter",
                "topic": "/chatter",
                "live": true,
                "sources": ["adminspace"]
            })
        );
        assert_eq!(
            discovery_row(&entry, "service"),
            serde_json::json!({
                "name": "/chatter",
                "service": "/chatter",
                "live": true,
                "sources": ["adminspace"]
            })
        );
    }

    #[test]
    fn merge_with_no_source_is_empty_and_drives_the_hint() {
        assert!(merge(BTreeSet::new(), BTreeSet::new(), Vec::new()).is_empty());
    }

    #[test]
    fn merge_keeps_a_directory_row_absent_when_no_live_source_saw_it() {
        let rows = merge(
            BTreeSet::new(),
            BTreeSet::new(),
            vec![Entry {
                name: "/demo/lights".into(),
                live: false,
            }],
        );
        assert_eq!(
            serde_json::to_value(&rows).unwrap(),
            serde_json::json!([
                {"name":"/demo/lights","live":false,"sources":["directory"]}
            ])
        );
    }

    #[test]
    fn merge_takes_live_from_the_directory_when_adminspace_missed_it() {
        // A directory built by a trusted session can see an interface a
        // client's own adminspace query cannot reach.
        let rows = merge(
            BTreeSet::new(),
            BTreeSet::new(),
            vec![Entry {
                name: "/chatter".into(),
                live: true,
            }],
        );
        assert!(rows[0].live);
        assert_eq!(rows[0].sources, vec!["directory"]);
    }

    #[test]
    fn load_directory_canonicalises_and_deduplicates_accepted_spellings() {
        let path = write_allowlist(
            "canon",
            r#"{"topics":["chatter","/chatter","/demo/lights/"],"services":["demo/version"]}"#,
        );
        let directory = load_directory(&path, "default").unwrap();
        std::fs::remove_file(&path).ok();
        assert_eq!(directory.topics, vec!["/chatter", "/demo/lights"]);
        assert_eq!(directory.services, vec!["/demo/version"]);
    }

    #[test]
    fn load_directory_rejects_wildcards_and_unknown_fields() {
        let wildcard = write_allowlist("wild", r#"{"topics":["**"],"services":[]}"#);
        assert!(load_directory(&wildcard, "default").is_err());
        std::fs::remove_file(&wildcard).ok();

        let unknown = write_allowlist(
            "unknown",
            r#"{"topics":[],"services":[],"actions":["/demo/execute_task"]}"#,
        );
        assert!(load_directory(&unknown, "default").is_err());
        std::fs::remove_file(&unknown).ok();

        let missing = write_allowlist("missing", r#"{"topics":[]}"#);
        assert!(load_directory(&missing, "default").is_err());
        std::fs::remove_file(&missing).ok();
    }

    /// Open a loopback-free peer session: no scouting, no endpoints, so the
    /// queryables below are answered locally and the test needs no network.
    async fn isolated_session() -> zenoh::Session {
        let mut config = zenoh::Config::default();
        config.insert_json5("mode", "\"peer\"").unwrap();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();
        config
            .insert_json5("scouting/gossip/enabled", "false")
            .unwrap();
        config.insert_json5("listen/endpoints", "[]").unwrap();
        config.insert_json5("connect/endpoints", "[]").unwrap();
        zenoh::open(config).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_skips_unparsable_directory_replies() {
        // Any node that can declare `sys/edu_directory` — or one corrupt frame
        // from the real directory — must not be able to deny discovery to
        // everyone. A discovery aid may not take down discovery.
        let session = isolated_session().await;
        let group = format!("directory_bad_{}", std::process::id());
        let key = format!("aorta/{group}/sys/edu_directory");

        let rogue = session.declare_queryable(&key).await.unwrap();
        let rogue_key = key.clone();
        let rogue_task = tokio::spawn(async move {
            while let Ok(query) = rogue.recv_async().await {
                let _ = query.reply(rogue_key.clone(), b"not-json".to_vec()).await;
            }
        });

        let good = session.declare_queryable(&key).await.unwrap();
        let good_key = key.clone();
        let payload = serde_json::to_vec(&Snapshot {
            topics: vec![Entry {
                name: "/chatter".into(),
                live: true,
            }],
            services: Vec::new(),
        })
        .unwrap();
        let good_task = tokio::spawn(async move {
            while let Ok(query) = good.recv_async().await {
                let _ = query.reply(good_key.clone(), payload.clone()).await;
            }
        });

        let entries = fetch(&session, &group, "topic", Duration::from_millis(500))
            .await
            .expect("a malformed directory reply must not fail discovery");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["/chatter"],
            "the well-formed directory reply must survive the rogue one"
        );

        rogue_task.abort();
        good_task.abort();
        session.close().await.unwrap();
    }

    #[test]
    fn allowlist_spellings_the_key_builder_accepts_project_onto_one_canonical_row() {
        // `serve()` validates names with `topic_key_pub` / `service_key_server`,
        // which accept unprefixed and trailing-slash spellings and normalise
        // them to the same key. Projecting the raw strings made those spellings
        // permanently `absent` and duplicated the row.
        let allowed = canonical_allowlist(&[
            "/chatter".to_string(),
            "chatter".to_string(),
            "demo/version/".to_string(),
        ]);
        let live = BTreeSet::from(["/chatter".to_string(), "/demo/version".to_string()]);
        let json = serde_json::to_value(project(&allowed, &live)).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"name":"/chatter","live":true},
                {"name":"/demo/version","live":true}
            ])
        );
    }

    #[test]
    fn merge_canonicalises_directory_names_onto_adminspace_rows() {
        let rows = merge(
            BTreeSet::from(["/chatter".to_string()]),
            BTreeSet::new(),
            vec![Entry {
                name: "chatter".into(),
                live: false,
            }],
        );
        let json = serde_json::to_value(&rows).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"name":"/chatter","live":true,"sources":["adminspace","directory"]}
            ])
        );
    }

    #[test]
    fn merge_keeps_subscriber_only_telemetry_interfaces() {
        // Repro for the `topic list` regression: `/drive/joy` is a
        // subscriber-only topic, so it never appears in the router
        // `router/publisher/**` adminspace declarations and only telemetry
        // knows about it.
        let admin = BTreeSet::from(["/chatter".to_string()]);
        let telemetry = BTreeSet::from(["/chatter".to_string(), "/drive/joy".to_string()]);
        let rows = merge(admin, telemetry, Vec::new());
        let json = serde_json::to_value(&rows).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"name":"/chatter","live":true,"sources":["adminspace","telemetry"]},
                {"name":"/drive/joy","live":true,"sources":["telemetry"]}
            ])
        );
    }

    #[test]
    fn merge_keeps_telemetry_rows_when_adminspace_is_unavailable() {
        // Telemetry-only mode (no reachable router adminspace) must still list
        // every interface telemetry knows about.
        let rows = merge(
            BTreeSet::new(),
            BTreeSet::from(["/chatter".to_string(), "/log".to_string()]),
            Vec::new(),
        );
        assert_eq!(
            rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
            vec!["/chatter", "/log"]
        );
        assert!(rows.iter().all(|row| row.live));
    }

    #[test]
    fn merge_records_every_source_that_saw_an_interface() {
        let rows = merge(
            BTreeSet::from(["/chatter".to_string()]),
            BTreeSet::from(["/chatter".to_string()]),
            vec![
                Entry {
                    name: "/chatter".into(),
                    live: true,
                },
                Entry {
                    name: "/demo/lights".into(),
                    live: false,
                },
            ],
        );
        let json = serde_json::to_value(&rows).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"name":"/chatter","live":true,"sources":["adminspace","telemetry","directory"]},
                {"name":"/demo/lights","live":false,"sources":["directory"]}
            ])
        );
    }

    #[test]
    fn admin_names_are_scoped_to_exact_namespace_and_resource_kind() {
        let prefix = "router/publisher/SN/aorta/default/pub/";
        assert_eq!(
            admin_public_name(
                "@/abcd/router/publisher/SN/aorta/default/pub/chatter",
                prefix
            ),
            Some("/chatter".into())
        );
        assert_eq!(
            admin_public_name(
                "@/abcd/router/publisher/OTHER/aorta/default/pub/chatter",
                prefix
            ),
            None
        );
        assert_eq!(
            admin_public_name(
                "@/abcd/router/queryable/SN/aorta/default/pub/chatter",
                prefix
            ),
            None
        );
        assert_eq!(
            admin_public_name(
                "@/abcd/router/publisher/aorta/default/pub/SN/aorta/default/pub/chatter",
                prefix
            ),
            None
        );
    }

    #[test]
    fn directory_filters_internal_names_and_keeps_absent_and_edu_commands() {
        let allowed = vec![
            "/chatter".into(),
            "/demo/lights".into(),
            "/drive/joy".into(),
        ];
        let live = BTreeSet::from(["/chatter".into(), "/drive/joy".into(), "/private".into()]);
        let json = serde_json::to_value(project(&allowed, &live)).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                {"name":"/chatter","live":true},
                {"name":"/demo/lights","live":false},
                {"name":"/drive/joy","live":true}
            ])
        );
    }
}
