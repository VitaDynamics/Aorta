use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use aorta_introspect::client::{TopicPreviewDecodeStatus, TopicPreviewFrame};
use serde_json::{json, Value};

use crate::app::{BookmarkKind, ResourceBookmark, RuntimeSectionPreferences, TopicPreviewState};
use crate::data::ServiceCallResponseData;

const LOCAL_STATE_VERSION: u64 = 2;
const APP_STATE_DIR_NAME: &str = "aorta-tui";
const LOCAL_STATE_FILE_NAME: &str = "state.json";
const EXPORTS_DIR_NAME: &str = "exports";
const MAX_EXPORT_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalState {
    pub runtime_sections: RuntimeSectionPreferences,
    pub bookmarks: Vec<ResourceBookmark>,
}

impl Default for LocalState {
    fn default() -> Self {
        Self {
            runtime_sections: RuntimeSectionPreferences::default(),
            bookmarks: Vec::new(),
        }
    }
}

impl LocalState {
    fn to_json_value(self) -> Value {
        json!({
            "version": LOCAL_STATE_VERSION,
            "runtime_sections": {
                "topics_expanded": self.runtime_sections.topics_expanded,
                "services_expanded": self.runtime_sections.services_expanded,
                "nodes_expanded": self.runtime_sections.nodes_expanded,
            },
            "bookmarks": self
                .bookmarks
                .into_iter()
                .map(bookmark_to_json_value)
                .collect::<Vec<_>>(),
        })
    }

    fn from_json_value(value: Value) -> Result<Self, String> {
        let version = value
            .get("version")
            .and_then(Value::as_u64)
            .ok_or_else(|| "local state is missing a numeric version".to_string())?;
        match version {
            1 => Ok(Self {
                runtime_sections: parse_runtime_sections(&value)?,
                bookmarks: Vec::new(),
            }),
            LOCAL_STATE_VERSION => {
                let bookmarks = value
                    .get("bookmarks")
                    .and_then(Value::as_array)
                    .map(|entries| {
                        entries
                            .iter()
                            .map(bookmark_from_json_value)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self {
                    runtime_sections: parse_runtime_sections(&value)?,
                    bookmarks,
                })
            }
            _ => Err(format!(
                "unsupported local state version {} (expected 1 or {})",
                version, LOCAL_STATE_VERSION
            )),
        }
    }
}

fn parse_runtime_sections(value: &Value) -> Result<RuntimeSectionPreferences, String> {
    let runtime_sections = value
        .get("runtime_sections")
        .ok_or_else(|| "local state is missing runtime_sections".to_string())?;
    let topics_expanded = runtime_sections
        .get("topics_expanded")
        .and_then(Value::as_bool)
        .ok_or_else(|| "runtime_sections.topics_expanded is missing".to_string())?;
    let services_expanded = runtime_sections
        .get("services_expanded")
        .and_then(Value::as_bool)
        .ok_or_else(|| "runtime_sections.services_expanded is missing".to_string())?;
    let nodes_expanded = runtime_sections
        .get("nodes_expanded")
        .and_then(Value::as_bool)
        .ok_or_else(|| "runtime_sections.nodes_expanded is missing".to_string())?;
    Ok(RuntimeSectionPreferences {
        topics_expanded,
        services_expanded,
        nodes_expanded,
    })
}

fn bookmark_to_json_value(bookmark: ResourceBookmark) -> Value {
    json!({
        "group": bookmark.group,
        "kind": bookmark.kind.as_str(),
        "key": bookmark.key,
        "label": bookmark.label,
    })
}

fn bookmark_from_json_value(value: &Value) -> Result<ResourceBookmark, String> {
    let group = value
        .get("group")
        .and_then(Value::as_str)
        .ok_or_else(|| "bookmark.group is missing".to_string())?;
    let kind = value
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| "bookmark.kind is missing".to_string())?;
    let key = value
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| "bookmark.key is missing".to_string())?;
    let label = value
        .get("label")
        .and_then(Value::as_str)
        .ok_or_else(|| "bookmark.label is missing".to_string())?;
    let kind = match kind {
        "topic" => BookmarkKind::Topic,
        "service" => BookmarkKind::Service,
        "node" => BookmarkKind::Node,
        other => return Err(format!("unsupported bookmark kind '{}'", other)),
    };
    Ok(ResourceBookmark {
        group: group.to_string(),
        kind,
        key: key.to_string(),
        label: label.to_string(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPaths {
    root_dir: PathBuf,
    state_file: PathBuf,
    exports_dir: PathBuf,
}

impl LocalPaths {
    pub fn from_env() -> Self {
        let root_dir = if let Ok(root) = env::var("AORTA_TUI_STATE_DIR") {
            PathBuf::from(root)
        } else if let Ok(xdg_state_home) = env::var("XDG_STATE_HOME") {
            PathBuf::from(xdg_state_home).join(APP_STATE_DIR_NAME)
        } else if let Ok(home) = env::var("HOME") {
            PathBuf::from(home)
                .join(".local")
                .join("state")
                .join(APP_STATE_DIR_NAME)
        } else {
            env::temp_dir().join(APP_STATE_DIR_NAME)
        };
        Self::from_root(root_dir)
    }

    pub fn from_root(root_dir: PathBuf) -> Self {
        Self {
            state_file: root_dir.join(LOCAL_STATE_FILE_NAME),
            exports_dir: root_dir.join(EXPORTS_DIR_NAME),
            root_dir,
        }
    }

    pub fn state_file(&self) -> &Path {
        &self.state_file
    }

    pub fn exports_dir(&self) -> &Path {
        &self.exports_dir
    }
}

pub fn load_local_state(paths: &LocalPaths) -> Result<LocalState, String> {
    if !paths.state_file().exists() {
        return Ok(LocalState::default());
    }

    let raw = fs::read_to_string(paths.state_file())
        .map_err(|err| format!("failed to read local state: {}", err))?;
    let value: Value =
        serde_json::from_str(&raw).map_err(|err| format!("invalid local state JSON: {}", err))?;
    LocalState::from_json_value(value)
}

pub fn save_local_state(paths: &LocalPaths, state: LocalState) -> Result<(), String> {
    fs::create_dir_all(&paths.root_dir)
        .map_err(|err| format!("failed to create local state directory: {}", err))?;
    let payload = serde_json::to_string_pretty(&state.to_json_value())
        .map_err(|err| format!("failed to serialize local state: {}", err))?;
    fs::write(paths.state_file(), payload)
        .map_err(|err| format!("failed to write local state: {}", err))
}

pub fn build_topic_preview_export(
    group: &str,
    preview: &TopicPreviewState,
) -> Result<String, String> {
    let frame = preview
        .selected_frame()
        .ok_or_else(|| "no topic preview frame is currently selected".to_string())?;
    let export = json!({
        "version": 1,
        "kind": "topic_preview_payload",
        "group": group,
        "topic": frame.topic.as_str(),
        "key_expr": frame.key_expr,
        "selected_history_position": preview.history_position().map(|(position, _)| position),
        "history_total": preview.history_position().map(|(_, total)| total),
        "viewing_latest": preview.is_viewing_latest(),
        "decode": build_topic_preview_decode_metadata(frame),
        "payload": build_topic_preview_payload(frame),
        "exported_unix_ms": unix_timestamp_millis(),
    });
    serde_json::to_string_pretty(&export)
        .map_err(|err| format!("failed to serialize topic preview export: {}", err))
}

pub fn build_service_response_export(
    group: &str,
    service_name: &str,
    response: &ServiceCallResponseData,
) -> Result<String, String> {
    let response_mode = if response.response_json.is_some() {
        "schema_decoded_json"
    } else if response.response_utf8.is_some() {
        "utf8_fallback"
    } else {
        "hex_only"
    };
    let export = json!({
        "version": 1,
        "kind": "service_call_response_payload",
        "group": group,
        "service": service_name,
        "response_mode": response_mode,
        "decoded_with_schema": response.decoded_with_schema,
        "schema_hash": response.schema_hash,
        "response_root_type": response.response_root_type,
        "payload": {
            "decoded_json": response
                .response_json
                .as_deref()
                .map(parse_json_or_string)
                .unwrap_or(Value::Null),
            "response_utf8": response
                .response_utf8
                .as_deref()
                .map(|text| Value::String(text.to_string()))
                .unwrap_or(Value::Null),
            "response_hex": response.response_hex,
        },
        "exported_unix_ms": unix_timestamp_millis(),
    });
    serde_json::to_string_pretty(&export)
        .map_err(|err| format!("failed to serialize service response export: {}", err))
}

pub fn write_export_payload(
    paths: &LocalPaths,
    category: &str,
    subject: &str,
    payload: &str,
) -> Result<PathBuf, String> {
    write_export_file(paths, category, subject, "json", payload)
}

pub fn write_text_export(
    paths: &LocalPaths,
    category: &str,
    subject: &str,
    extension: &str,
    payload: &str,
) -> Result<PathBuf, String> {
    write_export_file(paths, category, subject, extension, payload)
}

fn write_export_file(
    paths: &LocalPaths,
    category: &str,
    subject: &str,
    extension: &str,
    payload: &str,
) -> Result<PathBuf, String> {
    if payload.len() > MAX_EXPORT_BYTES {
        return Err(format!(
            "export payload exceeds the {} byte safety limit",
            MAX_EXPORT_BYTES
        ));
    }

    fs::create_dir_all(paths.exports_dir())
        .map_err(|err| format!("failed to create export directory: {}", err))?;
    let filename = format!(
        "{}-{}-{}.{}",
        sanitize_filename_component(category),
        sanitize_filename_component(subject),
        unix_timestamp_millis(),
        sanitize_export_extension(extension)
    );
    let path = paths.exports_dir().join(filename);
    fs::write(&path, payload).map_err(|err| format!("failed to write export file: {}", err))?;
    Ok(path)
}

fn build_topic_preview_decode_metadata(frame: &TopicPreviewFrame) -> Value {
    match &frame.decode_status {
        TopicPreviewDecodeStatus::DecodedFromTopicSchema => json!({
            "mode": "topic_schema_decoded",
            "schema_hash": frame.schema_hash,
            "error": Value::Null,
        }),
        TopicPreviewDecodeStatus::DecodedFromAttachmentSchema => json!({
            "mode": "attachment_schema_decoded",
            "schema_hash": frame.schema_hash,
            "error": Value::Null,
        }),
        TopicPreviewDecodeStatus::RawOnly => json!({
            "mode": "raw_hex_only",
            "schema_hash": frame.schema_hash,
            "error": Value::Null,
        }),
        TopicPreviewDecodeStatus::DecodeError(error) => json!({
            "mode": "decode_error_hex_only",
            "schema_hash": frame.schema_hash,
            "error": error,
        }),
    }
}

fn build_topic_preview_payload(frame: &TopicPreviewFrame) -> Value {
    json!({
        "decoded_json": frame
            .decoded_json
            .as_deref()
            .map(parse_json_or_string)
            .unwrap_or(Value::Null),
        "payload_hex": frame.payload_hex,
    })
}

fn parse_json_or_string(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

fn sanitize_filename_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' => ch,
            _ => '-',
        })
        .collect();
    sanitized.trim_matches('-').to_string()
}

fn sanitize_export_extension(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .filter(|ch| matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9'))
        .collect();
    if sanitized.is_empty() {
        "txt".to_string()
    } else {
        sanitized.to_ascii_lowercase()
    }
}

fn unix_timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use aorta_introspect::client::{TopicPreviewDecodeStatus, TopicPreviewFrame};
    use aorta_introspect::ids::TopicSuffix;
    use serde_json::Value;

    use super::{
        build_service_response_export, build_topic_preview_export, load_local_state,
        save_local_state, write_export_payload, write_text_export, LocalPaths, LocalState,
    };
    use crate::app::{App, ResourceBookmark, RuntimeSectionPreferences};
    use crate::data::ServiceCallResponseData;

    #[test]
    fn local_state_round_trip_preserves_runtime_preferences() {
        let root = unique_test_root("local-state");
        let paths = LocalPaths::from_root(root.clone());
        let state = LocalState {
            runtime_sections: RuntimeSectionPreferences {
                topics_expanded: false,
                services_expanded: true,
                nodes_expanded: false,
            },
            bookmarks: vec![
                ResourceBookmark::topic("robot_a", "imu/data"),
                ResourceBookmark::service("robot_a", "nav/plan"),
            ],
        };

        save_local_state(&paths, state.clone()).expect("save local state");
        let loaded = load_local_state(&paths).expect("load local state");

        assert_eq!(loaded, state);
        let raw = fs::read_to_string(paths.state_file()).expect("read state file");
        assert!(raw.contains("\"version\": 2"));
        assert!(raw.contains("\"bookmarks\""));
        cleanup_test_root(root);
    }

    #[test]
    fn local_state_v1_migrates_with_empty_bookmarks() {
        let root = unique_test_root("local-state-v1");
        let paths = LocalPaths::from_root(root.clone());
        fs::create_dir_all(root.clone()).expect("create root");
        fs::write(
            paths.state_file(),
            r#"{
  "version": 1,
  "runtime_sections": {
    "topics_expanded": false,
    "services_expanded": true,
    "nodes_expanded": false
  }
}"#,
        )
        .expect("write v1 state");

        let loaded = load_local_state(&paths).expect("load migrated local state");
        assert_eq!(
            loaded,
            LocalState {
                runtime_sections: RuntimeSectionPreferences {
                    topics_expanded: false,
                    services_expanded: true,
                    nodes_expanded: false,
                },
                bookmarks: Vec::new(),
            }
        );
        cleanup_test_root(root);
    }

    #[test]
    fn topic_preview_export_marks_decoded_mode_and_payload_body() {
        let mut app = App::default();
        app.begin_topic_preview("imu/data".to_string());
        app.apply_topic_preview_frame(TopicPreviewFrame {
            topic: TopicSuffix::new("imu/data").expect("topic"),
            key_expr: "aorta/default/pub/imu/data".to_string(),
            payload_hex: "7b226f6b223a747275657d".to_string(),
            decoded_json: Some("{\"ok\":true}".to_string()),
            schema_hash: Some("abcdef01".to_string()),
            decode_status: TopicPreviewDecodeStatus::DecodedFromTopicSchema,
        });

        let exported =
            build_topic_preview_export("default", app.topic_preview()).expect("export preview");
        let value: Value = serde_json::from_str(&exported).expect("valid export json");

        assert_eq!(value["kind"], "topic_preview_payload");
        assert_eq!(value["decode"]["mode"], "topic_schema_decoded");
        assert_eq!(value["payload"]["decoded_json"]["ok"], true);
        assert_eq!(value["payload"]["payload_hex"], "7b226f6b223a747275657d");
    }

    #[test]
    fn service_response_export_marks_utf8_fallback_mode() {
        let response = ServiceCallResponseData {
            response_json: None,
            response_utf8: Some("planner timeout".to_string()),
            response_hex: "706c616e6e65722074696d656f7574".to_string(),
            decoded_with_schema: false,
            schema_hash: "01234567".to_string(),
            response_root_type: Some("demo.Response".to_string()),
        };

        let exported = build_service_response_export("default", "nav/plan", &response)
            .expect("export response");
        let value: Value = serde_json::from_str(&exported).expect("valid export json");

        assert_eq!(value["response_mode"], "utf8_fallback");
        assert_eq!(value["payload"]["response_utf8"], "planner timeout");
        assert_eq!(
            value["payload"]["response_hex"],
            "706c616e6e65722074696d656f7574"
        );
    }

    #[test]
    fn service_response_export_marks_hex_only_mode() {
        let response = ServiceCallResponseData {
            response_json: None,
            response_utf8: None,
            response_hex: "00ff".to_string(),
            decoded_with_schema: false,
            schema_hash: "89abcdef".to_string(),
            response_root_type: None,
        };

        let exported = build_service_response_export("default", "nav/plan", &response)
            .expect("export response");
        let value: Value = serde_json::from_str(&exported).expect("valid export json");

        assert_eq!(value["response_mode"], "hex_only");
        assert_eq!(value["payload"]["decoded_json"], Value::Null);
        assert_eq!(value["payload"]["response_utf8"], Value::Null);
        assert_eq!(value["payload"]["response_hex"], "00ff");
    }

    #[test]
    fn write_export_payload_writes_json_file_under_exports_dir() {
        let root = unique_test_root("exports");
        let paths = LocalPaths::from_root(root.clone());
        let payload = "{\"kind\":\"demo\"}";

        let path = write_export_payload(&paths, "topic-preview", "imu/data", payload)
            .expect("write export");
        let raw = fs::read_to_string(&path).expect("read export file");

        assert!(path.starts_with(paths.exports_dir()));
        assert_eq!(raw, payload);
        cleanup_test_root(root);
    }

    #[test]
    fn write_text_export_preserves_requested_extension() {
        let root = unique_test_root("text-export");
        let paths = LocalPaths::from_root(root.clone());
        let payload = "#!/usr/bin/env bash\n";

        let path = write_text_export(&paths, "recorder-launchpad", "imu/data", "sh", payload)
            .expect("write text export");

        assert!(path.starts_with(paths.exports_dir()));
        assert_eq!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("sh")
        );
        let raw = fs::read_to_string(&path).expect("read text export");
        assert_eq!(raw, payload);
        cleanup_test_root(root);
    }

    fn unique_test_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "aorta-tui-{}-{}",
            label,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ))
    }

    fn cleanup_test_root(root: PathBuf) {
        let _ = fs::remove_dir_all(root);
    }
}
