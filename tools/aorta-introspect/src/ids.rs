use std::fmt;

use aorta_core::key_expr::{normalize_path, normalize_path_with_wildcards};
use aorta_core::KeyExprBuilder;

use crate::error::{IntrospectError, IntrospectResult};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupName(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicSuffix(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceSuffix(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeName(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EndpointId(String);

impl GroupName {
    pub fn new(value: impl AsRef<str>) -> IntrospectResult<Self> {
        let raw = value.as_ref().trim();
        KeyExprBuilder::new(raw).map_err(|err| {
            IntrospectError::invalid_input(format!("invalid group '{}': {}", raw, err))
        })?;
        Ok(Self(raw.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TopicSuffix {
    /// Publish-side / user-input constructor: rejects wildcard segments. Use
    /// for CLI arguments and any concrete topic key.
    pub fn new(value: impl AsRef<str>) -> IntrospectResult<Self> {
        Ok(Self(normalize_user_path(value.as_ref(), "topic")?))
    }

    /// Subscribe-side constructor: accepts whole-segment Zenoh wildcards
    /// (`*` / `**`). Telemetry-derived subscriber resource names may be
    /// wildcard patterns (e.g. a `*/task_report` fan-in), so validating
    /// them with the strict publish-side [`TopicSuffix::new`] hard-fails the
    /// whole introspect listing. Represent the pattern instead.
    pub fn new_subscribe_side(value: impl AsRef<str>) -> IntrospectResult<Self> {
        Ok(Self(normalize_subscribe_side_path(
            value.as_ref(),
            "topic",
        )?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// True when this suffix carries a whole-segment wildcard (`*` / `**`),
    /// i.e. it is a subscribe-side pattern rather than a concrete topic.
    pub fn is_pattern(&self) -> bool {
        suffix_is_pattern(&self.0)
    }
}

impl ServiceSuffix {
    /// Publish-side / user-input constructor: rejects wildcard segments.
    pub fn new(value: impl AsRef<str>) -> IntrospectResult<Self> {
        Ok(Self(normalize_user_path(value.as_ref(), "service")?))
    }

    /// Subscribe-side constructor: accepts whole-segment Zenoh wildcards.
    /// Telemetry-derived service *client* resource names may be wildcard
    /// patterns, mirroring [`TopicSuffix::new_subscribe_side`].
    pub fn new_subscribe_side(value: impl AsRef<str>) -> IntrospectResult<Self> {
        Ok(Self(normalize_subscribe_side_path(
            value.as_ref(),
            "service",
        )?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// True when this suffix carries a whole-segment wildcard (`*` / `**`).
    pub fn is_pattern(&self) -> bool {
        suffix_is_pattern(&self.0)
    }
}

impl NodeName {
    pub fn new(value: impl AsRef<str>) -> IntrospectResult<Self> {
        let raw = value.as_ref().trim();
        if raw.is_empty() {
            return Err(IntrospectError::invalid_input("node name cannot be empty"));
        }
        let normalized = normalize_path(raw).map_err(|err| {
            IntrospectError::invalid_input(format!("invalid node name '{}': {}", raw, err))
        })?;
        Ok(Self(normalized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl NodeId {
    pub fn new(value: impl AsRef<str>) -> IntrospectResult<Self> {
        let normalized = value.as_ref().trim().to_ascii_lowercase();
        if normalized.len() != 32 || !normalized.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(IntrospectError::invalid_input(format!(
                "invalid node id '{}': expected 32 hex characters",
                value.as_ref().trim()
            )));
        }
        Ok(Self(normalized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl EndpointId {
    pub fn new(value: impl AsRef<str>) -> IntrospectResult<Self> {
        let normalized = value.as_ref().trim();
        if normalized.is_empty() {
            return Err(IntrospectError::invalid_input(
                "endpoint id cannot be empty",
            ));
        }
        Ok(Self(normalized.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn normalize_user_path(value: &str, kind: &str) -> IntrospectResult<String> {
    let normalized = normalize_path(value).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid {} '{}': {}", kind, value.trim(), err))
    })?;
    Ok(normalized)
}

/// Subscribe-side normalization: same rules as [`normalize_user_path`] but
/// whole-segment wildcards (`*` / `**`) are accepted verbatim.
fn normalize_subscribe_side_path(value: &str, kind: &str) -> IntrospectResult<String> {
    let normalized = normalize_path_with_wildcards(value).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid {} '{}': {}", kind, value.trim(), err))
    })?;
    Ok(normalized)
}

/// A normalized suffix is a pattern when any segment is a whole-segment Zenoh
/// wildcard. Chunk-wildcards (`$*`) are not supported by the validator, so
/// `*` / `**` are the only pattern segments that can appear here.
fn suffix_is_pattern(normalized: &str) -> bool {
    normalized
        .split('/')
        .any(|segment| segment == "*" || segment == "**")
}

impl fmt::Display for GroupName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for TopicSuffix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for ServiceSuffix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for EndpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::{EndpointId, GroupName, NodeId, NodeName, ServiceSuffix, TopicSuffix};

    #[test]
    fn topic_suffix_normalizes_user_input() {
        // Validator strips outer slashes and ASCII whitespace; the
        // middle `//` collapse the test used to exercise is now a
        // hard error, so use a clean
        // path here.
        let suffix = TopicSuffix::new(" /demo/camera/ ").expect("topic suffix");
        assert_eq!(suffix.as_str(), "demo/camera");
    }

    #[test]
    fn topic_suffix_rejects_empty_middle_segment() {
        // Post-relaxation: `//` is no longer silently collapsed.
        assert!(TopicSuffix::new("demo//camera").is_err());
    }

    #[test]
    fn service_suffix_normalizes_user_input() {
        let suffix = ServiceSuffix::new("/rpc/echo").expect("service suffix");
        assert_eq!(suffix.as_str(), "rpc/echo");
    }

    #[test]
    fn topic_suffix_new_rejects_wildcard_but_subscribe_side_accepts_it() {
        // Publish-side / user input stays concrete.
        assert!(TopicSuffix::new("*/task_report").is_err());
        assert!(TopicSuffix::new("camera/**").is_err());

        // Subscribe-side tolerates the wildcard subscriber pattern.
        let pattern = TopicSuffix::new_subscribe_side("/*/task_report/").expect("wildcard sub");
        assert_eq!(pattern.as_str(), "*/task_report");
        assert!(pattern.is_pattern());

        let concrete = TopicSuffix::new_subscribe_side("camera/front").expect("concrete topic");
        assert!(!concrete.is_pattern());
    }

    #[test]
    fn service_suffix_subscribe_side_accepts_wildcard_client_pattern() {
        assert!(ServiceSuffix::new("*/task_report").is_err());

        let pattern = ServiceSuffix::new_subscribe_side("**").expect("wildcard client");
        assert!(pattern.is_pattern());

        let concrete = ServiceSuffix::new_subscribe_side("nav/plan").expect("concrete service");
        assert!(!concrete.is_pattern());
    }

    #[test]
    fn node_id_lowercases_hex() {
        let node_id = NodeId::new("ABCDEF0123456789ABCDEF0123456789").expect("node id");
        assert_eq!(node_id.as_str(), "abcdef0123456789abcdef0123456789");
    }

    #[test]
    fn node_name_normalizes_user_input() {
        // Outer-slash strip + whitespace trim still works; middle `//`
        // collapse is no longer silent (it is now a hard error).
        let node_name = NodeName::new(" /demo/talker ").expect("node name");
        assert_eq!(node_name.as_str(), "demo/talker");
    }

    #[test]
    fn group_name_validates_with_core_builder() {
        let group = GroupName::new("default").expect("group");
        assert_eq!(group.as_str(), "default");
    }

    #[test]
    fn endpoint_id_rejects_empty_input() {
        let err = EndpointId::new(" ").expect_err("empty endpoint");
        assert!(err.to_string().contains("cannot be empty"));
    }
}
