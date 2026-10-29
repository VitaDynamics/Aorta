use aorta_core::key_expr::normalize_path;

pub fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Format a byte count as a human-readable size with binary (1024-based) units.
///
/// `512 -> "512 B"`, `1536 -> "1.50 KiB"`, `1048576 -> "1.00 MiB"`. Used by the
/// `stats` and `topic bw` readouts; pair it with the exact byte count when
/// precision matters.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{} B", bytes);
    }
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{:.2} {}", value, UNITS[unit])
}

/// Format a nanosecond **duration** with an adaptive ns/us/ms/s/m unit.
///
/// `999 -> "999ns"`, `1_500 -> "1.500us"`, `2_500_000 -> "2.500ms"`. Promoted here
/// from `cmd::action` so `service`/`nodes`/`stats` can share one latency formatter.
pub fn format_duration_ns(duration_ns: u64) -> String {
    if duration_ns < 1_000 {
        return format!("{}ns", duration_ns);
    }
    if duration_ns < 1_000_000 {
        return format!("{:.3}us", duration_ns as f64 / 1_000.0);
    }
    if duration_ns < 1_000_000_000 {
        return format!("{:.3}ms", duration_ns as f64 / 1_000_000.0);
    }
    if duration_ns < 60_000_000_000 {
        return format!("{:.3}s", duration_ns as f64 / 1_000_000_000.0);
    }
    format!("{:.3}m", duration_ns as f64 / 60_000_000_000.0)
}

/// Format a latency **duration** in ns, rendering a zero (no call recorded yet)
/// as `n/a` rather than `0ns`.
pub fn format_latency_ns(latency_ns: u64) -> String {
    if latency_ns == 0 {
        return "n/a".to_string();
    }
    format_duration_ns(latency_ns)
}

/// Format the age of a wall-clock `unix_time_ns` stamp relative to `now_ns`.
/// A zero stamp (never set) renders as `n/a`; a future stamp clamps to `0ns`.
pub fn format_stamp_age_ns(now_ns: u64, stamp_ns: u64) -> String {
    if stamp_ns == 0 {
        return "n/a".to_string();
    }
    format_duration_ns(now_ns.saturating_sub(stamp_ns))
}

/// Render a user-facing topic/service path with a leading slash.
///
/// Internal suffixes are usually normalized as `foo/bar`; for CLI output we
/// present them as `/foo/bar` to keep user-facing behavior consistent.
pub fn to_user_key(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return "/".to_string();
    }

    if let Ok(normalized) = normalize_path(trimmed) {
        return format!("/{}", normalized);
    }

    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{}", trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        format_duration_ns, format_latency_ns, format_stamp_age_ns, human_bytes, to_user_key,
    };

    #[test]
    fn format_duration_ns_formats_common_ranges() {
        assert_eq!(format_duration_ns(999), "999ns");
        assert_eq!(format_duration_ns(1_500), "1.500us");
        assert_eq!(format_duration_ns(2_500_000), "2.500ms");
        assert_eq!(format_duration_ns(2_000_000_000), "2.000s");
        assert_eq!(format_duration_ns(120_000_000_000), "2.000m");
    }

    #[test]
    fn format_latency_ns_renders_zero_as_na() {
        assert_eq!(format_latency_ns(0), "n/a");
        assert_eq!(format_latency_ns(1_500), "1.500us");
    }

    #[test]
    fn format_stamp_age_ns_handles_zero_and_future() {
        assert_eq!(format_stamp_age_ns(1_000, 0), "n/a");
        assert_eq!(format_stamp_age_ns(3_000, 1_000), "2.000us");
        // Future stamp (clock skew) clamps to 0ns rather than underflowing.
        assert_eq!(format_stamp_age_ns(1_000, 5_000), "0ns");
    }

    #[test]
    fn human_bytes_formats_binary_units() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(1024), "1.00 KiB");
        assert_eq!(human_bytes(1536), "1.50 KiB");
        assert_eq!(human_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.00 GiB");
    }

    #[test]
    fn to_user_key_normalizes_without_leading_slash() {
        assert_eq!(to_user_key("demo/chatter"), "/demo/chatter");
    }

    #[test]
    fn to_user_key_preserves_normalized_leading_slash() {
        assert_eq!(to_user_key("/demo/chatter"), "/demo/chatter");
    }

    #[test]
    fn to_user_key_passes_through_malformed_input_after_pr_90() {
        // Pre-PR-#90 normalize_path silently collapsed `//`. Now it
        // rejects empty middle segments, so to_user_key's fallback
        // path preserves the original input verbatim (with whitespace
        // trim and leading-slash injection). Showing the malformed key
        // as-is helps the user spot the typo instead of silently
        // hiding it.
        assert_eq!(to_user_key(" /demo//chatter/ "), "/demo//chatter/");
    }
}
