//! Host identity + the placeholder normalization algorithm.
//!
//! Per design, raw `{host_name}` / `{host_id}` are preserved in the JSON
//! payload for display; only the substituted-into-key form goes through
//! the segment normalizer. `KeyExprBuilder` validation runs against the
//! full assembled suffix at daemon start-up.
//!
//! This module only exposes the normalization helpers; the actual key
//! assembly happens in the daemon publish path (`publisher`).

use std::ffi::CStr;

const MAX_SEGMENT_LEN: usize = 64;
const HOST_PREFIX: &str = "host_";

/// Resolves the hostname via `libc::gethostname`. Returns the raw,
/// un-normalized hostname (the form a human would type).
pub fn resolve_hostname() -> String {
    // 255 + NUL is what POSIX promises; pad a little for safety.
    let mut buf = vec![0u8; 256];
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if rc != 0 {
        return "localhost".to_string();
    }
    // gethostname may not NUL-terminate when the buffer is exactly large
    // enough; force a sentinel just in case.
    *buf.last_mut().expect("non-empty buffer") = 0;
    let cstr = unsafe { CStr::from_ptr(buf.as_ptr() as *const libc::c_char) };
    cstr.to_string_lossy().into_owned()
}

/// Apply the normalization algorithm to a single substituted value.
///
/// 1. ASCII lowercase.
/// 2. Replace every char not in `[a-z0-9_]` with `_`.
/// 3. Collapse runs of `_`, strip leading/trailing `_`.
/// 4. If empty or first char is not `[a-z]`, prepend `host_`.
/// 5. Truncate to 64 bytes from the end (keeping the optional `host_` prefix).
pub fn normalize_placeholder(input: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let replaced: String = lower
        .chars()
        .map(|c| if is_keyseg_char(c) { c } else { '_' })
        .collect();

    let mut collapsed = String::with_capacity(replaced.len());
    let mut prev_underscore = false;
    for c in replaced.chars() {
        if c == '_' {
            if !prev_underscore {
                collapsed.push('_');
            }
            prev_underscore = true;
        } else {
            collapsed.push(c);
            prev_underscore = false;
        }
    }
    let trimmed = collapsed.trim_matches('_');

    let needs_prefix = trimmed
        .chars()
        .next()
        .map(|c| !c.is_ascii_lowercase())
        .unwrap_or(true);
    let body = if needs_prefix {
        let mut s = String::with_capacity(HOST_PREFIX.len() + trimmed.len());
        s.push_str(HOST_PREFIX);
        s.push_str(trimmed);
        s
    } else {
        trimmed.to_string()
    };

    truncate_to(&body, MAX_SEGMENT_LEN)
}

fn is_keyseg_char(c: char) -> bool {
    matches!(c, 'a'..='z' | '0'..='9' | '_')
}

fn truncate_to(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    // Normalization step 5: truncate to 64 bytes from the end (keeping the
    // optional `host_` prefix). We anchor the result at the END of the
    // body so two long ids that share a common prefix (e.g. two robots
    // with serials differing only in the suffix) remain distinct after
    // truncation.
    let (prefix, body) = if let Some(rest) = s.strip_prefix(HOST_PREFIX) {
        (HOST_PREFIX, rest)
    } else {
        ("", s)
    };
    // If max is too small to fit even the prefix, fall back to the
    // prefix itself so we never return something starting with a digit.
    if max <= prefix.len() {
        return prefix.to_string();
    }
    let body_budget = max - prefix.len();
    let body_bytes = body.as_bytes();
    if body_bytes.len() <= body_budget {
        return format!("{}{}", prefix, body);
    }
    // Walk forward (drop bytes from the head) to a char boundary so the
    // tail we keep is `body_budget` bytes long.
    let mut start = body_bytes.len() - body_budget;
    while !body.is_char_boundary(start) && start < body_bytes.len() {
        start += 1;
    }
    format!("{}{}", prefix, &body[start..])
}

/// Resolved identity used by the snapshot pipeline.
#[derive(Debug, Clone)]
pub struct HostIdentity {
    /// Raw, un-normalized host name (for display).
    pub host_name: String,
    /// Raw, un-normalized host id (defaults to `host_name`).
    pub host_id: String,
    /// `{host_name}` after normalization (for key substitution).
    pub host_name_key: String,
    /// `{host_id}` after normalization (for key substitution).
    pub host_id_key: String,
}

impl HostIdentity {
    pub fn resolve(host_name_override: Option<&str>, host_id_override: Option<&str>) -> Self {
        let host_name = host_name_override
            .map(str::to_string)
            .filter(|s| !s.is_empty())
            .unwrap_or_else(resolve_hostname);

        let host_id = host_id_override
            .map(str::to_string)
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| host_name.clone());

        let host_name_key = normalize_placeholder(&host_name);
        let host_id_key = normalize_placeholder(&host_id);

        Self {
            host_name,
            host_id,
            host_name_key,
            host_id_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowercase_simple() {
        assert_eq!(normalize_placeholder("ubuntu"), "ubuntu");
    }

    #[test]
    fn dashes_and_dots_become_underscore() {
        assert_eq!(normalize_placeholder("dev-host.local"), "dev_host_local");
    }

    #[test]
    fn digit_prefixed_serial_gets_host_prefix() {
        assert_eq!(normalize_placeholder("0EXAMPLESN42"), "host_0examplesn42");
    }

    #[test]
    fn collapses_runs_of_underscore() {
        assert_eq!(normalize_placeholder("a..b__c"), "a_b_c");
    }

    #[test]
    fn empty_input_yields_host_prefix_only() {
        assert_eq!(normalize_placeholder(""), "host_");
    }

    #[test]
    fn truncates_to_64_bytes() {
        let long = "a".repeat(200);
        let out = normalize_placeholder(&long);
        assert_eq!(out.len(), MAX_SEGMENT_LEN);
        assert!(out.chars().all(|c| c == 'a'));
    }

    #[test]
    fn truncation_keeps_tail_to_preserve_uniqueness() {
        // Two long ids sharing a common prefix should remain distinct
        // after normalization — the differentiating suffix lives at the
        // end, so we must truncate FROM the front (keep the tail).
        let a = format!("{}xxxx", "0".repeat(70));
        let b = format!("{}yyyy", "0".repeat(70));
        let na = normalize_placeholder(&a);
        let nb = normalize_placeholder(&b);
        assert_eq!(na.len(), MAX_SEGMENT_LEN);
        assert_eq!(nb.len(), MAX_SEGMENT_LEN);
        assert_ne!(na, nb, "long ids must not collide after truncation");
        // Both still start with `host_` because step 4 prepends it for
        // digit-leading inputs.
        assert!(na.starts_with(HOST_PREFIX));
        assert!(nb.starts_with(HOST_PREFIX));
        // The distinguishing suffix is preserved.
        assert!(na.ends_with("xxxx"));
        assert!(nb.ends_with("yyyy"));
    }

    #[test]
    fn identity_uses_override_when_present() {
        let id = HostIdentity::resolve(Some("dev-1"), None);
        assert_eq!(id.host_name, "dev-1");
        assert_eq!(id.host_id, "dev-1");
        assert_eq!(id.host_name_key, "dev_1");
        assert_eq!(id.host_id_key, "dev_1");
    }

    #[test]
    fn identity_empty_override_falls_through() {
        let id = HostIdentity::resolve(Some(""), Some(""));
        // Should fall back to gethostname; we only check that the override
        // did not silently become an empty raw host_name.
        assert!(!id.host_name.is_empty());
    }
}
