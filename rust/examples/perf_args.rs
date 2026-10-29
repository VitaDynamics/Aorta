use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub enum Pattern {
    Zero,
    Increment,
    Random,
}

pub fn parse_pattern(value: &str) -> Option<Pattern> {
    match value.to_ascii_lowercase().as_str() {
        "zero" => Some(Pattern::Zero),
        "increment" => Some(Pattern::Increment),
        "random" => Some(Pattern::Random),
        _ => None,
    }
}

pub fn parse_size(value: &str) -> Option<usize> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }

    let mut idx = value.len();
    while idx > 0 && value.as_bytes()[idx - 1].is_ascii_alphabetic() {
        idx -= 1;
    }
    let (num_part, suffix_part) = value.split_at(idx);
    let multiplier = match suffix_part.to_ascii_lowercase().as_str() {
        "" | "b" => 1u64,
        "k" | "kb" => 1024u64,
        "m" | "mb" => 1024u64 * 1024u64,
        "g" | "gb" => 1024u64 * 1024u64 * 1024u64,
        _ => return None,
    };
    let number: u64 = num_part.parse().ok()?;
    number
        .checked_mul(multiplier)
        .and_then(|v| usize::try_from(v).ok())
}

pub fn parse_interval(
    interval_ms: Option<u64>,
    rate_hz: Option<f64>,
    default_ms: u64,
) -> Result<Duration, String> {
    if let Some(ms) = interval_ms {
        return Ok(Duration::from_millis(ms));
    }
    if let Some(rate) = rate_hz {
        if rate <= 0.0 {
            return Err("rate-hz must be > 0".to_string());
        }
        return Ok(Duration::from_secs_f64(1.0 / rate));
    }
    Ok(Duration::from_millis(default_ms))
}

pub fn build_payload_body(
    size: usize,
    pattern: Pattern,
    increment_offset: usize,
    random_seed: u64,
) -> Vec<u8> {
    let mut data = vec![0u8; size];
    match pattern {
        Pattern::Zero => {}
        Pattern::Increment => {
            for (idx, byte) in data.iter_mut().enumerate() {
                *byte = ((increment_offset + idx) & 0xFF) as u8;
            }
        }
        Pattern::Random => {
            let mut seed = random_seed.max(1);
            for byte in data.iter_mut() {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                *byte = (seed & 0xFF) as u8;
            }
        }
    }
    data
}
