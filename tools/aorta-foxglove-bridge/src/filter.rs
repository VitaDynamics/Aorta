use anyhow::{anyhow, Result};

use crate::config::TopicsConfig;

/// Topic filter with `aorta-record` semantics: `blacklist_suffix` patterns are
/// rooted under `pub/` (pattern `demo/private/**` blocks
/// `aorta/{group}/pub/demo/private/**`), while `blacklist_full` patterns match
/// the FULL wire key — including `ctx/**` and `sys/telemetry/**` streams.
/// Callers pass the `pub/`-stripped topic suffix (empty for non-`pub` keys, in
/// which case the suffix path is a no-op) so the same YAML knobs mean the same
/// thing in the recorder and the bridge.
#[derive(Debug, Clone)]
pub struct TopicFilter {
    /// Pub-relative include patterns; empty = accept every `pub/` topic.
    /// The data plane narrows via subscription selectors already, but the
    /// admin-space discovery loop sees EVERY declared publisher — without
    /// this gate a `--include demo/**` bridge would still pre-advertise
    /// channels for topics no subscription will ever feed.
    include_suffix: Vec<String>,
    blacklist_suffix: Vec<String>,
    blacklist_full: Vec<String>,
}

impl TopicFilter {
    pub fn from_config(topics: &TopicsConfig) -> Result<Self> {
        let include_suffix = topics
            .include_suffix
            .iter()
            .map(|p| normalize_suffix_pattern(p))
            .collect::<Result<Vec<_>>>()?;
        let blacklist_suffix = topics
            .blacklist_suffix
            .iter()
            .map(|p| normalize_suffix_pattern(p))
            .collect::<Result<Vec<_>>>()?;
        let blacklist_full = topics
            .blacklist_full
            .iter()
            .map(|p| {
                let trimmed = p.trim();
                if trimmed.is_empty() {
                    Err(anyhow!("blacklist full keyexpr cannot be empty"))
                } else {
                    Ok(trimmed.to_string())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            include_suffix,
            blacklist_suffix,
            blacklist_full,
        })
    }

    /// `topic_suffix` is relative to the `pub/` root (empty for non-`pub`
    /// keys); `full_key` is the complete wire key. Non-`pub` keys (empty
    /// `topic_suffix`) skip the include gate — the include set narrows
    /// `pub/**` only, matching `build_wildcard_subscriptions`.
    pub fn accepts(&self, topic_suffix: &str, full_key: &str) -> bool {
        if !topic_suffix.is_empty()
            && !self.include_suffix.is_empty()
            && !self
                .include_suffix
                .iter()
                .any(|pattern| keyexpr_match(pattern, topic_suffix))
        {
            return false;
        }
        for pattern in &self.blacklist_suffix {
            if keyexpr_match(pattern, topic_suffix) {
                return false;
            }
        }
        for pattern in &self.blacklist_full {
            if keyexpr_match(pattern, full_key) {
                return false;
            }
        }
        true
    }
}

pub fn normalize_suffix_pattern(input: &str) -> Result<String> {
    let trimmed = input.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        return Err(anyhow!("topic suffix pattern cannot be empty"));
    }
    if trimmed.contains(' ') {
        return Err(anyhow!("topic suffix pattern cannot contain spaces"));
    }
    Ok(trimmed.to_string())
}

/// Builds the wildcard Zenoh selector for a user-supplied include
/// pattern. The pattern is treated as a topic suffix relative to the
/// `pub/` root, matching `aorta-record::filter::build_subscription_key`:
/// `--include demo/**` → `aorta/{group}/pub/demo/**`.
pub fn build_subscription_key(group: &str, suffix_pattern: &str) -> Result<String> {
    let suffix = normalize_suffix_pattern(suffix_pattern)?;
    Ok(format!("aorta/{}/pub/{}", group, suffix))
}

pub fn keyexpr_match(pattern: &str, value: &str) -> bool {
    let pattern_segments: Vec<&str> = pattern
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let value_segments: Vec<&str> = value
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    keyexpr_match_segments(&pattern_segments, &value_segments)
}

fn keyexpr_match_segments(pattern: &[&str], value: &[&str]) -> bool {
    if pattern.is_empty() {
        return value.is_empty();
    }
    if pattern[0] == "**" {
        for skip in 0..=value.len() {
            if keyexpr_match_segments(&pattern[1..], &value[skip..]) {
                return true;
            }
        }
        return false;
    }
    if value.is_empty() {
        return false;
    }
    segment_match(pattern[0], value[0]) && keyexpr_match_segments(&pattern[1..], &value[1..])
}

// Mirrors `aorta-record::filter::segment_match` — embedded wildcards
// (`cam*` matching `camera`) must behave identically in both tools or the
// same blacklist YAML silently filters different sets.
fn segment_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return pattern == value;
    }
    wildcard_segment_match(pattern.as_bytes(), value.as_bytes())
}

fn wildcard_segment_match(pattern: &[u8], value: &[u8]) -> bool {
    let mut dp = vec![vec![false; value.len() + 1]; pattern.len() + 1];
    dp[0][0] = true;

    for i in 1..=pattern.len() {
        if pattern[i - 1] == b'*' {
            dp[i][0] = dp[i - 1][0];
        }
    }

    for i in 1..=pattern.len() {
        for j in 1..=value.len() {
            if pattern[i - 1] == b'*' {
                dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
            } else if pattern[i - 1] == value[j - 1] {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }

    dp[pattern.len()][value.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn double_star_matches_multiple_segments() {
        assert!(keyexpr_match("demo/**", "demo/cam/front"));
        assert!(keyexpr_match("demo/**", "demo"));
        assert!(!keyexpr_match("demo/**", "other/cam/front"));
    }

    #[test]
    fn star_matches_single_segment() {
        assert!(keyexpr_match("demo/*", "demo/cam"));
        assert!(!keyexpr_match("demo/*", "demo/cam/front"));
    }

    #[test]
    fn embedded_wildcard_matches_within_segment() {
        // Recorder parity: `cam*` must match `camera`.
        assert!(keyexpr_match("demo/cam*", "demo/camera"));
        assert!(keyexpr_match("demo/cam*", "demo/cam"));
        assert!(!keyexpr_match("demo/cam*", "demo/lidar"));
        assert!(keyexpr_match("demo/*_raw", "demo/img_raw"));
        assert!(!keyexpr_match("demo/*_raw", "demo/img"));
    }

    #[test]
    fn suffix_blacklist_is_rooted_under_pub() {
        // Recorder semantics: the pattern does NOT include the `pub/` prefix.
        let topics = TopicsConfig {
            include_suffix: vec![],
            blacklist_suffix: vec!["demo/private/**".to_string()],
            blacklist_full: vec![],
        };
        let filter = TopicFilter::from_config(&topics).expect("filter");
        assert!(!filter.accepts(
            "demo/private/chatter",
            "aorta/default/pub/demo/private/chatter"
        ));
        assert!(filter.accepts(
            "demo/public/chatter",
            "aorta/default/pub/demo/public/chatter"
        ));
        // Non-pub keys pass an empty topic_suffix: the suffix path is a no-op.
        assert!(filter.accepts("", "aorta/default/ctx/robot/state"));
    }

    #[test]
    fn include_gate_narrows_pub_but_not_ctx() {
        // Regression: the admin-space discovery loop must not pre-advertise
        // pub topics outside the --include set (Foxglove Bridge Design §4.1:
        // patterns apply on
        // both paths).
        let topics = TopicsConfig {
            include_suffix: vec!["demo/**".to_string()],
            blacklist_suffix: vec![],
            blacklist_full: vec![],
        };
        let filter = TopicFilter::from_config(&topics).expect("filter");
        assert!(filter.accepts("demo/chatter", "aorta/default/pub/demo/chatter"));
        assert!(!filter.accepts("cam/front", "aorta/default/pub/cam/front"));
        // Non-pub keys (empty topic_suffix) bypass the include gate.
        assert!(filter.accepts("", "aorta/default/ctx/robot/state"));
    }

    #[test]
    fn filter_rejects_full_key_blacklist() {
        let topics = TopicsConfig {
            include_suffix: vec![],
            blacklist_suffix: vec![],
            blacklist_full: vec!["aorta/default/ctx/**".to_string()],
        };
        let filter = TopicFilter::from_config(&topics).expect("filter");
        assert!(!filter.accepts("", "aorta/default/ctx/robot/state"));
        assert!(filter.accepts("demo/chatter", "aorta/default/pub/demo/chatter"));
    }

    #[test]
    fn build_subscription_key_adds_pub_prefix_implicitly() {
        let key = build_subscription_key("default", "demo/**").expect("key");
        assert_eq!(key, "aorta/default/pub/demo/**");
    }

    #[test]
    fn build_subscription_key_strips_leading_slash() {
        let key = build_subscription_key("default", "/demo/cam/*").expect("key");
        assert_eq!(key, "aorta/default/pub/demo/cam/*");
    }
}
