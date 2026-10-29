use anyhow::{anyhow, Result};

#[derive(Debug, Clone)]
pub struct TopicFilter {
    blacklist_suffix_patterns: Vec<String>,
    blacklist_full_patterns: Vec<String>,
}

impl TopicFilter {
    pub fn new(
        blacklist_suffix_patterns: Vec<String>,
        blacklist_full_patterns: Vec<String>,
    ) -> Self {
        Self {
            blacklist_suffix_patterns,
            blacklist_full_patterns,
        }
    }

    pub fn accepts(&self, topic_suffix: &str, full_key: &str) -> bool {
        !self
            .blacklist_suffix_patterns
            .iter()
            .any(|pattern| keyexpr_match(pattern, topic_suffix))
            && !self
                .blacklist_full_patterns
                .iter()
                .any(|pattern| keyexpr_match(pattern, full_key))
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
    use super::{build_subscription_key, keyexpr_match, TopicFilter};

    #[test]
    fn keyexpr_double_star_matches_multiple_segments() {
        assert!(keyexpr_match("demo/**", "demo/cam/front"));
        assert!(keyexpr_match("demo/**", "demo"));
        assert!(!keyexpr_match("demo/**", "other/cam/front"));
    }

    #[test]
    fn keyexpr_star_matches_single_segment() {
        assert!(keyexpr_match("demo/*", "demo/cam"));
        assert!(!keyexpr_match("demo/*", "demo/cam/front"));
    }

    #[test]
    fn topic_filter_rejects_blacklist() {
        let filter = TopicFilter::new(vec!["demo/private/**".to_string()], vec![]);
        assert!(!filter.accepts(
            "demo/private/chatter",
            "aorta/default/pub/demo/private/chatter"
        ));
        assert!(filter.accepts(
            "demo/public/chatter",
            "aorta/default/pub/demo/public/chatter"
        ));
    }

    #[test]
    fn build_subscription_key_builds_pub_prefix() {
        let key = build_subscription_key("default", "demo/**").expect("subscription key");
        assert_eq!(key, "aorta/default/pub/demo/**");
    }
}
