//! Numeric & cross-input validation (MCAP Player Proposal §6.12).
//!
//! Every conversion here is *checked*: an out-of-range or unrepresentable
//! input is rejected with an error, never saturated, wrapped, or panicked on.
//! The player's timeline arithmetic is `u64` nanoseconds, so a duration-shaped
//! CLI input must additionally be representable in that unit.

use crate::PlayerConfig;
use anyhow::{anyhow, Result};
use std::time::Duration;

/// Playback-rate default and hard ceiling (MCAP Player Proposal §6.12:
/// `0 < rate <= 1e6`).
pub const DEFAULT_RATE: f64 = 1.0;
/// Largest accepted `--rate`.
pub const MAX_RATE: f64 = 1e6;
/// Matched-subscriber barrier default in seconds (MCAP Player Proposal §5; `0`
/// disables).
pub const DEFAULT_MATCH_TIMEOUT_SECS: f64 = 5.0;
/// Per-topic schema-preflight deadline default in seconds (MCAP Player Proposal
/// §5, §6.3).
pub const DEFAULT_PREFLIGHT_TIMEOUT_SECS: f64 = 5.0;
/// Largest accepted message payload (MCAP Player Proposal §5: 256 MiB).
pub const DEFAULT_MAX_MESSAGE_BYTES: u64 = 256 * 1024 * 1024;
/// Largest accepted declared chunk / generic record size (MCAP Player Proposal
/// §5: 512 MiB).
pub const DEFAULT_MAX_CHUNK_BYTES: u64 = 512 * 1024 * 1024;
/// In-flight byte budget of the pass-2 reader queue (MCAP Player Proposal §5:
/// 256 MiB).
pub const DEFAULT_BUFFER_BYTES: u64 = 256 * 1024 * 1024;

const NANOS_PER_SEC_F64: f64 = 1_000_000_000.0;
/// Exclusive upper bound for a nanosecond count that must fit in `u64`.
/// `2^64` is exactly representable in `f64`, so the comparison is exact.
const MAX_NANOS_EXCLUSIVE_F64: f64 = 18_446_744_073_709_551_616.0;

/// The validated, converted form of every numeric CLI input (MCAP Player
/// Proposal §6.12).
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedLimits {
    pub rate: f64,
    pub start: Duration,
    pub duration: Option<Duration>,
    pub match_timeout: Duration,
    pub preflight_timeout: Duration,
    pub max_message_bytes: u64,
    pub max_chunk_bytes: u64,
    pub buffer_bytes: u64,
}

/// Validate every numeric and cross-input constraint of MCAP Player Proposal
/// §6.12.
///
/// The remaining §6.12 obligations — checked `t_first + start (+ duration)` and
/// `wall0 + Δ / rate` — need the pass-1 timeline and belong to the scheduler
/// phase; they consume the [`ValidatedLimits`] produced here.
pub fn validate(config: &PlayerConfig) -> Result<ValidatedLimits> {
    let rate = validate_rate(config.rate)?;
    let start = checked_secs_to_duration("--start", config.start_secs)?;
    let duration = config
        .duration_secs
        .map(|secs| checked_secs_to_duration("--duration", secs))
        .transpose()?;
    let match_timeout = checked_secs_to_duration("--match-timeout", config.match_timeout_secs)?;
    let preflight_timeout = validate_preflight_timeout(config.preflight_timeout_secs)?;

    let max_message_bytes =
        validate_positive_bytes("--max-message-bytes", config.max_message_bytes)?;
    let max_chunk_bytes = validate_positive_bytes("--max-chunk-bytes", config.max_chunk_bytes)?;
    let buffer_bytes = validate_positive_bytes("--buffer-bytes", config.buffer_bytes)?;

    // Every accepted message must fit the queue budget, so the reader can
    // never face an impossible admission (MCAP Player Proposal §6.7).
    if max_message_bytes > buffer_bytes {
        return Err(anyhow!(
            "--max-message-bytes ({}) must be <= --buffer-bytes ({})",
            max_message_bytes,
            buffer_bytes
        ));
    }

    Ok(ValidatedLimits {
        rate,
        start,
        duration,
        match_timeout,
        preflight_timeout,
        max_message_bytes,
        max_chunk_bytes,
        buffer_bytes,
    })
}

/// `--rate`: finite, `0 < rate <= 1e6`.
pub(crate) fn validate_rate(rate: f64) -> Result<f64> {
    if !rate.is_finite() {
        return Err(anyhow!("--rate must be a finite number, got {}", rate));
    }
    if rate <= 0.0 || rate > MAX_RATE {
        return Err(anyhow!(
            "--rate must satisfy 0 < rate <= {}, got {}",
            MAX_RATE,
            rate
        ));
    }
    Ok(rate)
}

/// Checked seconds → [`Duration`] for inputs that allow zero
/// (`--start`, `--duration`, `--match-timeout`): finite, `>= 0`, and
/// representable as a `u64` nanosecond count.
pub(crate) fn checked_secs_to_duration(name: &str, secs: f64) -> Result<Duration> {
    if !secs.is_finite() {
        return Err(anyhow!(
            "{} must be a finite number of seconds, got {}",
            name,
            secs
        ));
    }
    if secs < 0.0 {
        return Err(anyhow!("{} must be >= 0 seconds, got {}", name, secs));
    }
    secs_to_duration_checked(name, secs)
}

/// `--preflight-timeout`: finite, strictly `> 0`, representable. Zero is
/// rejected rather than treated as "disabled": under the MCAP Player Proposal
/// §6.3 deadline rule it would make every typed preflight immediately
/// inconclusive.
pub(crate) fn validate_preflight_timeout(secs: f64) -> Result<Duration> {
    if !secs.is_finite() {
        return Err(anyhow!(
            "--preflight-timeout must be a finite number of seconds, got {}",
            secs
        ));
    }
    if secs <= 0.0 {
        return Err(anyhow!(
            "--preflight-timeout must be > 0 seconds (zero would make every typed \
             preflight inconclusive), got {}",
            secs
        ));
    }
    secs_to_duration_checked("--preflight-timeout", secs)
}

// ---------------------------------------------------------------------------
// Runtime checked arithmetic (MCAP Player Proposal §6.12, consumed by §6.5)
// ---------------------------------------------------------------------------

/// Checked `t_first + --start` — the inclusive start of the effective-time
/// window (MCAP Player Proposal §6.5 step 2). Overflow is an error, never a
/// saturating clamp.
pub fn checked_window_start(t_first_ns: u64, start: Duration) -> Result<u64> {
    let start_ns = duration_nanos("--start", start)?;
    t_first_ns.checked_add(start_ns).ok_or_else(|| {
        anyhow!(
            "--start = {} ns past the first recorded timestamp ({} ns) overflows the \
             u64 nanosecond timeline",
            start_ns,
            t_first_ns
        )
    })
}

/// Checked `window_start + --duration` — the inclusive window end. `None`
/// duration means an open-ended window.
pub fn checked_window_end(window_start_ns: u64, duration: Option<Duration>) -> Result<Option<u64>> {
    let Some(duration) = duration else {
        return Ok(None);
    };
    let duration_ns = duration_nanos("--duration", duration)?;
    let end = window_start_ns.checked_add(duration_ns).ok_or_else(|| {
        anyhow!(
            "--duration = {} ns past the window start ({} ns) overflows the u64 \
             nanosecond timeline",
            duration_ns,
            window_start_ns
        )
    })?;
    Ok(Some(end))
}

/// Checked `Δ / rate` — the scheduler's `target(m) = wall0 + (effective − t0) /
/// rate` (MCAP Player Proposal §6.5, §6.12).
pub fn checked_scaled_duration(delta_ns: u64, rate: f64) -> Result<Duration> {
    let rate = validate_rate(rate)?;
    let scaled = delta_ns as f64 / rate;
    if !scaled.is_finite() || !(0.0..MAX_NANOS_EXCLUSIVE_F64).contains(&scaled) {
        return Err(anyhow!(
            "replay offset {} ns at --rate {} is not representable as a u64 \
             nanosecond count",
            delta_ns,
            rate
        ));
    }
    Ok(Duration::from_nanos(scaled as u64))
}

/// Checked [`Duration`] → `u64` nanoseconds. `Duration` counts nanoseconds in
/// a `u128`, so the player's `u64` timeline needs this narrowing check.
pub(crate) fn duration_nanos(name: &str, value: Duration) -> Result<u64> {
    u64::try_from(value.as_nanos()).map_err(|_| {
        anyhow!(
            "{} = {:?} is not representable as a u64 nanosecond count",
            name,
            value
        )
    })
}

/// Byte-budget inputs must be strictly positive.
pub(crate) fn validate_positive_bytes(name: &str, value: u64) -> Result<u64> {
    if value == 0 {
        return Err(anyhow!("{} must be > 0 bytes", name));
    }
    Ok(value)
}

/// Shared representability check: the product must stay strictly below `2^64`
/// nanoseconds, so the `f64 → u64` conversion can neither saturate nor wrap.
fn secs_to_duration_checked(name: &str, secs: f64) -> Result<Duration> {
    let nanos = secs * NANOS_PER_SEC_F64;
    if nanos >= MAX_NANOS_EXCLUSIVE_F64 {
        return Err(anyhow!(
            "{} = {} s is not representable as a u64 nanosecond count",
            name,
            secs
        ));
    }
    Ok(Duration::from_nanos(nanos as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The next representable `f64` above `value` (positive values only) —
    /// used to pin the "first rejected" side of a boundary.
    fn next_up(value: f64) -> f64 {
        f64::from_bits(value.to_bits() + 1)
    }

    #[test]
    fn rate_accepts_positive_values_up_to_the_ceiling() {
        assert_eq!(validate_rate(DEFAULT_RATE).unwrap(), 1.0);
        assert_eq!(validate_rate(0.5).unwrap(), 0.5);
        assert_eq!(validate_rate(f64::MIN_POSITIVE).unwrap(), f64::MIN_POSITIVE);
        assert_eq!(validate_rate(MAX_RATE).unwrap(), MAX_RATE);
    }

    #[test]
    fn rate_rejects_zero_negative_nan_inf_and_above_ceiling() {
        for bad in [
            0.0,
            -0.0,
            -1.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            next_up(MAX_RATE),
        ] {
            assert!(validate_rate(bad).is_err(), "rate {bad} must be rejected");
        }
    }

    #[test]
    fn secs_conversion_accepts_zero_and_fractional_values() {
        assert_eq!(
            checked_secs_to_duration("--start", 0.0).unwrap(),
            Duration::ZERO
        );
        assert_eq!(
            checked_secs_to_duration("--start", 1.5).unwrap(),
            Duration::from_nanos(1_500_000_000)
        );
        assert_eq!(
            checked_secs_to_duration("--duration", 0.000_000_001).unwrap(),
            Duration::from_nanos(1)
        );
    }

    #[test]
    fn secs_conversion_boundary_is_u64_nanoseconds() {
        // Largest accepted: 18_446_744_073 s * 1e9 < 2^64 ns.
        assert!(checked_secs_to_duration("--start", 18_446_744_073.0).is_ok());
        // First rejected: the next whole second overflows the u64 ns range.
        assert!(checked_secs_to_duration("--start", 18_446_744_074.0).is_err());
    }

    #[test]
    fn secs_conversion_rejects_negative_nan_and_inf() {
        for bad in [-0.5, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                checked_secs_to_duration("--start", bad).is_err(),
                "start {bad} must be rejected"
            );
        }
    }

    #[test]
    fn secs_conversion_error_names_the_flag() {
        let err = checked_secs_to_duration("--match-timeout", -1.0).unwrap_err();
        assert!(
            err.to_string().contains("--match-timeout"),
            "error must name the offending flag, got: {err}"
        );
    }

    #[test]
    fn preflight_timeout_requires_strictly_positive() {
        assert!(validate_preflight_timeout(0.0).is_err());
        assert!(validate_preflight_timeout(-0.0).is_err());
        assert!(validate_preflight_timeout(-1.0).is_err());
        assert!(validate_preflight_timeout(f64::NAN).is_err());
        assert!(validate_preflight_timeout(f64::INFINITY).is_err());
        assert!(validate_preflight_timeout(18_446_744_074.0).is_err());
        assert_eq!(
            validate_preflight_timeout(0.000_000_001).unwrap(),
            Duration::from_nanos(1)
        );
        assert_eq!(
            validate_preflight_timeout(DEFAULT_PREFLIGHT_TIMEOUT_SECS).unwrap(),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn byte_budgets_must_be_positive() {
        assert!(validate_positive_bytes("--buffer-bytes", 0).is_err());
        assert_eq!(validate_positive_bytes("--buffer-bytes", 1).unwrap(), 1);
        assert_eq!(
            validate_positive_bytes("--buffer-bytes", u64::MAX).unwrap(),
            u64::MAX
        );
    }

    #[test]
    fn default_config_validates_to_documented_defaults() {
        let config = PlayerConfig::default();
        let limits = validate(&config).expect("defaults must validate");
        assert_eq!(limits.rate, DEFAULT_RATE);
        assert_eq!(limits.start, Duration::ZERO);
        assert_eq!(limits.duration, None);
        assert_eq!(limits.match_timeout, Duration::from_secs(5));
        assert_eq!(limits.preflight_timeout, Duration::from_secs(5));
        assert_eq!(limits.max_message_bytes, DEFAULT_MAX_MESSAGE_BYTES);
        assert_eq!(limits.max_chunk_bytes, DEFAULT_MAX_CHUNK_BYTES);
        assert_eq!(limits.buffer_bytes, DEFAULT_BUFFER_BYTES);
    }

    #[test]
    fn match_timeout_zero_is_accepted_and_disables_the_barrier() {
        let config = PlayerConfig {
            match_timeout_secs: 0.0,
            ..PlayerConfig::default()
        };
        let limits = validate(&config).expect("zero match timeout disables the barrier");
        assert_eq!(limits.match_timeout, Duration::ZERO);
    }

    #[test]
    fn max_message_bytes_must_not_exceed_buffer_bytes() {
        let config = PlayerConfig {
            max_message_bytes: 1024,
            buffer_bytes: 1023,
            ..PlayerConfig::default()
        };
        let err = validate(&config).unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("--max-message-bytes") && text.contains("--buffer-bytes"),
            "error must name both flags, got: {text}"
        );

        let equal = PlayerConfig {
            max_message_bytes: 1024,
            buffer_bytes: 1024,
            ..PlayerConfig::default()
        };
        assert!(
            validate(&equal).is_ok(),
            "max-message-bytes == buffer-bytes is accepted"
        );
    }

    #[test]
    fn validate_rejects_each_zero_byte_budget() {
        for config in [
            PlayerConfig {
                buffer_bytes: 0,
                ..PlayerConfig::default()
            },
            PlayerConfig {
                max_message_bytes: 0,
                ..PlayerConfig::default()
            },
            PlayerConfig {
                max_chunk_bytes: 0,
                ..PlayerConfig::default()
            },
        ] {
            assert!(validate(&config).is_err());
        }
    }

    #[test]
    fn validate_propagates_duration_and_start_conversion() {
        let config = PlayerConfig {
            start_secs: 2.5,
            duration_secs: Some(0.25),
            ..PlayerConfig::default()
        };
        let limits = validate(&config).expect("valid window");
        assert_eq!(limits.start, Duration::from_millis(2500));
        assert_eq!(limits.duration, Some(Duration::from_millis(250)));
    }

    // -- runtime checked arithmetic (MCAP Player Proposal §6.12) -----------

    #[test]
    fn window_start_boundary_is_the_end_of_the_nanosecond_timeline() {
        assert_eq!(checked_window_start(0, Duration::ZERO).unwrap(), 0);
        assert_eq!(
            checked_window_start(10, Duration::from_nanos(5)).unwrap(),
            15
        );
        // Largest accepted, then first rejected.
        assert_eq!(
            checked_window_start(u64::MAX - 1, Duration::from_nanos(1)).unwrap(),
            u64::MAX
        );
        let err = checked_window_start(u64::MAX, Duration::from_nanos(1)).unwrap_err();
        assert!(err.to_string().contains("--start"), "got: {err}");
    }

    #[test]
    fn window_end_is_optional_and_checked() {
        assert_eq!(checked_window_end(10, None).unwrap(), None);
        assert_eq!(
            checked_window_end(10, Some(Duration::from_nanos(5))).unwrap(),
            Some(15)
        );
        assert_eq!(
            checked_window_end(u64::MAX - 1, Some(Duration::from_nanos(1))).unwrap(),
            Some(u64::MAX)
        );
        let err = checked_window_end(u64::MAX, Some(Duration::from_nanos(1))).unwrap_err();
        assert!(err.to_string().contains("--duration"), "got: {err}");
    }

    #[test]
    fn duration_nanos_rejects_values_above_the_u64_range() {
        assert_eq!(
            duration_nanos("--start", Duration::from_nanos(u64::MAX)).unwrap(),
            u64::MAX
        );
        // `Duration` counts nanoseconds in a u128, so this is representable
        // there but not on the player's u64 timeline.
        let err = duration_nanos("--start", Duration::new(u64::MAX, 0)).unwrap_err();
        assert!(err.to_string().contains("--start"), "got: {err}");
    }

    #[test]
    fn scaled_duration_divides_by_the_rate_and_rejects_the_unrepresentable() {
        assert_eq!(
            checked_scaled_duration(1_000_000_000, 1.0).unwrap(),
            Duration::from_secs(1)
        );
        assert_eq!(
            checked_scaled_duration(1_000_000_000, 2.0).unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(
            checked_scaled_duration(1_000_000_000, 0.5).unwrap(),
            Duration::from_secs(2)
        );
        assert_eq!(checked_scaled_duration(0, 1e6).unwrap(), Duration::ZERO);
        // A slow rate can push a representable delta past the u64 ns ceiling.
        assert!(checked_scaled_duration(u64::MAX, 0.5).is_err());
        // An invalid rate is rejected here too, not silently saturated.
        assert!(checked_scaled_duration(1, 0.0).is_err());
        assert!(checked_scaled_duration(1, f64::NAN).is_err());
    }

    #[test]
    fn validate_rejects_bad_rate_start_duration_and_timeouts() {
        let cases = [
            PlayerConfig {
                rate: 0.0,
                ..PlayerConfig::default()
            },
            PlayerConfig {
                start_secs: -1.0,
                ..PlayerConfig::default()
            },
            PlayerConfig {
                duration_secs: Some(f64::NAN),
                ..PlayerConfig::default()
            },
            PlayerConfig {
                match_timeout_secs: f64::INFINITY,
                ..PlayerConfig::default()
            },
            PlayerConfig {
                preflight_timeout_secs: 0.0,
                ..PlayerConfig::default()
            },
        ];
        for config in cases {
            assert!(validate(&config).is_err());
        }
    }
}
