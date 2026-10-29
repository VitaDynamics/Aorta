//! PSI collector.

use std::fs;

use serde::{Deserialize, Serialize};

use crate::procfs_root::ProcfsRoot;

use super::delta_u64;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PsiResourceSample {
    pub resource: String,
    /// `some` line totals (always present when PSI is available).
    pub some_total_us: u64,
    /// `full` line totals — `None` for CPU (kernel only emits `some` for cpu pressure).
    pub full_total_us: Option<u64>,
    pub some_avg10: f32,
    pub some_avg60: f32,
    pub some_avg300: f32,
    pub full_avg10: Option<f32>,
    pub full_avg60: Option<f32>,
    pub full_avg300: Option<f32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PsiSample {
    pub resources: Vec<PsiResourceSample>,
}

impl PsiSample {
    pub fn read(root: &ProcfsRoot) -> Self {
        let mut resources = Vec::new();
        for r in ["cpu", "memory", "io"] {
            if let Ok(body) = fs::read_to_string(root.proc_path(format!("pressure/{}", r))) {
                if let Some(sample) = parse_one(r, &body) {
                    resources.push(sample);
                }
            }
        }
        Self { resources }
    }
}

fn parse_one(resource: &str, body: &str) -> Option<PsiResourceSample> {
    let mut some_total_us = 0;
    let mut full_total_us = None;
    let mut some_avg10 = 0.0;
    let mut some_avg60 = 0.0;
    let mut some_avg300 = 0.0;
    let mut full_avg10 = None;
    let mut full_avg60 = None;
    let mut full_avg300 = None;
    let mut any = false;

    for line in body.lines() {
        let mut it = line.split_whitespace();
        let kind = match it.next() {
            Some(k) => k,
            None => continue,
        };
        let mut avg10 = 0.0_f32;
        let mut avg60 = 0.0_f32;
        let mut avg300 = 0.0_f32;
        let mut total: u64 = 0;
        // If any value is present but unparsable the kernel format has
        // drifted — drop the whole resource sample rather than silently
        // pretending the field was 0. The caller treats `None` as
        // "PSI unavailable for this resource on this tick", which the
        // capability layer already handles cleanly.
        let mut malformed = false;
        for kv in it {
            let ok = if let Some(v) = kv.strip_prefix("avg10=") {
                match v.parse::<f32>() {
                    Ok(f) => {
                        avg10 = f;
                        true
                    }
                    Err(_) => false,
                }
            } else if let Some(v) = kv.strip_prefix("avg60=") {
                match v.parse::<f32>() {
                    Ok(f) => {
                        avg60 = f;
                        true
                    }
                    Err(_) => false,
                }
            } else if let Some(v) = kv.strip_prefix("avg300=") {
                match v.parse::<f32>() {
                    Ok(f) => {
                        avg300 = f;
                        true
                    }
                    Err(_) => false,
                }
            } else if let Some(v) = kv.strip_prefix("total=") {
                match v.parse::<u64>() {
                    Ok(n) => {
                        total = n;
                        true
                    }
                    Err(_) => false,
                }
            } else {
                // Unknown key — ignore for forward-compat with kernel
                // additions; not a malformed signal.
                true
            };
            if !ok {
                malformed = true;
                break;
            }
        }
        if malformed {
            return None;
        }
        match kind {
            "some" => {
                some_total_us = total;
                some_avg10 = avg10;
                some_avg60 = avg60;
                some_avg300 = avg300;
                any = true;
            }
            "full" => {
                full_total_us = Some(total);
                full_avg10 = Some(avg10);
                full_avg60 = Some(avg60);
                full_avg300 = Some(avg300);
                any = true;
            }
            _ => {}
        }
    }

    if !any {
        return None;
    }
    Some(PsiResourceSample {
        resource: resource.into(),
        some_total_us,
        full_total_us,
        some_avg10,
        some_avg60,
        some_avg300,
        full_avg10,
        full_avg60,
        full_avg300,
    })
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PsiLine {
    pub resource: String,
    pub line: String,
    pub avg10: f32,
    pub avg60: f32,
    pub avg300: f32,
    pub total_us: u64,
    pub total_delta_us: u64,
    /// `total_delta_us / (interval_ms * 1000)` — interval-independent.
    pub stall_ratio: f32,
}

pub fn delta(prev: &PsiSample, curr: &PsiSample, interval_ms: u64) -> Vec<PsiLine> {
    let window_us = interval_ms.saturating_mul(1000);
    let mut out = Vec::new();
    for cur in &curr.resources {
        let prev_match = prev.resources.iter().find(|p| p.resource == cur.resource);
        let some_delta = match prev_match {
            Some(p) => delta_u64(p.some_total_us, cur.some_total_us),
            None => 0,
        };
        out.push(PsiLine {
            resource: cur.resource.clone(),
            line: "some".into(),
            avg10: cur.some_avg10,
            avg60: cur.some_avg60,
            avg300: cur.some_avg300,
            total_us: cur.some_total_us,
            total_delta_us: some_delta,
            stall_ratio: ratio(some_delta, window_us),
        });
        if let Some(full_total) = cur.full_total_us {
            let full_delta = match prev_match.and_then(|p| p.full_total_us) {
                Some(prev_total) => delta_u64(prev_total, full_total),
                None => 0,
            };
            out.push(PsiLine {
                resource: cur.resource.clone(),
                line: "full".into(),
                avg10: cur.full_avg10.unwrap_or(0.0),
                avg60: cur.full_avg60.unwrap_or(0.0),
                avg300: cur.full_avg300.unwrap_or(0.0),
                total_us: full_total,
                total_delta_us: full_delta,
                stall_ratio: ratio(full_delta, window_us),
            });
        }
    }
    out
}

fn ratio(delta_us: u64, window_us: u64) -> f32 {
    if window_us == 0 {
        return 0.0;
    }
    (delta_us as f32 / window_us as f32).min(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PSI_IO: &str = "\
some avg10=10.00 avg60=5.00 avg300=1.00 total=1000000
full avg10=5.00  avg60=2.00 avg300=0.50 total=500000
";

    #[test]
    fn malformed_value_drops_resource_sample() {
        // total= field is present but not a number — must NOT silently
        // pretend it was 0; the whole resource sample should be dropped
        // so capability layer can surface the drift.
        let bad = "some avg10=10.00 avg60=5.00 avg300=1.00 total=NOPE\n";
        assert!(parse_one("io", bad).is_none());
    }

    #[test]
    fn parses_some_and_full() {
        let s = parse_one("io", PSI_IO).expect("io psi sample");
        assert_eq!(s.some_total_us, 1_000_000);
        assert_eq!(s.full_total_us, Some(500_000));
        assert!((s.some_avg10 - 10.0).abs() < 0.001);
    }

    #[test]
    fn stall_ratio_independent_of_interval() {
        let prev = PsiSample {
            resources: vec![PsiResourceSample {
                resource: "io".into(),
                some_total_us: 0,
                full_total_us: Some(0),
                ..Default::default()
            }],
        };
        let curr = PsiSample {
            resources: vec![PsiResourceSample {
                resource: "io".into(),
                some_total_us: 250_000, // 250 ms of "some" pressure
                full_total_us: Some(100_000),
                ..Default::default()
            }],
        };
        let lines = delta(&prev, &curr, 1000);
        let some = lines.iter().find(|l| l.line == "some").expect("some line");
        // 250 ms of pressure in a 1 s window = 0.25 ratio.
        assert!((some.stall_ratio - 0.25).abs() < 0.001);
    }
}
