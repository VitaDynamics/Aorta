//! Small parsing helpers shared by collectors.

use anyhow::{anyhow, Context, Result};

/// Parse a whitespace-separated u64 at column `idx` (0-based).
pub fn parse_u64_col(line: &str, idx: usize) -> Result<u64> {
    line.split_whitespace()
        .nth(idx)
        .ok_or_else(|| anyhow!("missing column {} in {:?}", idx, line))?
        .parse()
        .with_context(|| format!("parse u64 col {} in {:?}", idx, line))
}

/// Saturating delta on a cumulative counter — counters reset to 0 on
/// kernel rollover (procfs uses u64, so rollover is essentially never;
/// but we still defensively saturate to keep callers simple).
pub fn delta_u64(prev: u64, curr: u64) -> u64 {
    curr.saturating_sub(prev)
}

/// Convert a per-interval delta into a "per second" rate.
pub fn per_sec(delta: u64, interval_ms: u64) -> u64 {
    if interval_ms == 0 {
        return 0;
    }
    (delta as u128 * 1000 / interval_ms as u128) as u64
}

/// Simple shell-style glob matcher used by include/exclude filters
/// (only `*` is supported — covers `loop*`, `nvme*`, `dm-*`, etc).
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let mut pi = 0usize;
    let mut ni = 0usize;
    let pb = pattern.as_bytes();
    let nb = name.as_bytes();
    let mut star = None;
    let mut match_idx = 0;
    while ni < nb.len() {
        if pi < pb.len() && (pb[pi] == nb[ni] || pb[pi] == b'?') {
            pi += 1;
            ni += 1;
        } else if pi < pb.len() && pb[pi] == b'*' {
            star = Some(pi);
            match_idx = ni;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            match_idx += 1;
            ni = match_idx;
        } else {
            return false;
        }
    }
    while pi < pb.len() && pb[pi] == b'*' {
        pi += 1;
    }
    pi == pb.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_basic() {
        assert!(glob_match("loop*", "loop0"));
        assert!(glob_match("nvme*", "nvme0n1"));
        assert!(!glob_match("loop*", "lp0"));
        assert!(glob_match("dm-*", "dm-0"));
        assert!(glob_match("lo", "lo"));
        assert!(!glob_match("lo", "lo0"));
        assert!(glob_match("*", "anything"));
    }

    #[test]
    fn per_sec_rounds_down() {
        assert_eq!(per_sec(1000, 1000), 1000);
        assert_eq!(per_sec(500, 1000), 500);
        assert_eq!(per_sec(500, 2000), 250);
        assert_eq!(per_sec(0, 1000), 0);
        assert_eq!(per_sec(10, 0), 0);
    }
}
