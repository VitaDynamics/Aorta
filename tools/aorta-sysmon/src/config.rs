//! Configuration for `aorta-sysmon`.
//!
//! Precedence: CLI > `AORTA_SYSMON_*` env > config file > built-in defaults.
//! The `snapshot` subcommand consumes only a subset of these fields;
//! daemon-only fields (alert topics, PSI thresholds, etc.) are parsed and
//! round-tripped there but driven only by the daemon path.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Three-state metric switch matching the `configured_mode` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum FamilyMode {
    On,
    Off,
    #[default]
    Auto,
}

impl std::str::FromStr for FamilyMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "on" => Ok(Self::On),
            "off" => Ok(Self::Off),
            "auto" => Ok(Self::Auto),
            other => Err(anyhow::anyhow!("expected on|off|auto, got {:?}", other)),
        }
    }
}

impl std::fmt::Display for FamilyMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::On => "on",
            Self::Off => "off",
            Self::Auto => "auto",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricFamilies {
    pub cpu: FamilyMode,
    pub memory: FamilyMode,
    pub disk: FamilyMode,
    pub network: FamilyMode,
    pub psi: FamilyMode,
    pub perf: FamilyMode,
    pub zenoh_topology: FamilyMode,
    pub process: FamilyMode,
}

impl Default for MetricFamilies {
    fn default() -> Self {
        Self {
            cpu: FamilyMode::On,
            memory: FamilyMode::On,
            disk: FamilyMode::On,
            network: FamilyMode::On,
            psi: FamilyMode::Auto,
            perf: FamilyMode::Auto,
            // Explicitly off until a topology collector exists.
            zenoh_topology: FamilyMode::Off,
            process: FamilyMode::On,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Filters {
    pub disk_include: Vec<String>,
    pub disk_exclude: Vec<String>,
    pub net_include: Vec<String>,
    pub net_exclude: Vec<String>,
}

impl Default for Filters {
    fn default() -> Self {
        Self {
            disk_include: vec!["sd*".into(), "nvme*".into(), "mmcblk*".into()],
            disk_exclude: vec![
                "loop*".into(),
                "ram*".into(),
                "dm-*".into(),
                "sr*".into(),
                "zram*".into(),
            ],
            net_include: vec![
                "eth*".into(),
                "wlan*".into(),
                "enp*".into(),
                "wlp*".into(),
                "usb*".into(),
                "can*".into(),
            ],
            net_exclude: vec![
                "lo".into(),
                "docker*".into(),
                "veth*".into(),
                "br-*".into(),
                "virbr*".into(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProcessConfig {
    pub include_self: bool,
    pub include_aorta_nodes: bool,
    pub include_pids: Vec<i32>,
    pub include_executables: Vec<String>,
    pub max_listed: u32,
}

impl Default for ProcessConfig {
    fn default() -> Self {
        Self {
            include_self: true,
            // Process discovery does not query aorta_core telemetry yet;
            // the field is accepted in config and round-tripped only.
            include_aorta_nodes: true,
            include_pids: Vec::new(),
            include_executables: Vec::new(),
            max_listed: 32,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PsiThreshold {
    pub resource: String,
    pub line: String,
    pub stall_ratio: f32,
    pub window_ms: u32,
}

impl Default for PsiThreshold {
    fn default() -> Self {
        Self {
            resource: "io".into(),
            line: "full".into(),
            stall_ratio: 0.10,
            window_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProcessThreshold {
    pub metric: String,
    pub threshold: f64,
    pub top_k: u32,
    pub severity: String,
}

impl Default for ProcessThreshold {
    fn default() -> Self {
        Self {
            metric: "cpu_percent_host".into(),
            threshold: 90.0,
            top_k: 3,
            severity: "warn".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub node_name: String,
    pub host_name: String,
    pub host_id: String,
    pub topic_suffix: String,
    pub alert_topic_suffix: String,
    pub interval_ms: u64,
    pub capability_refresh_ms: u64,
    pub metrics: MetricFamilies,
    pub filter: Filters,
    pub process: ProcessConfig,
    #[serde(default, rename = "psi_threshold")]
    pub psi_thresholds: Vec<PsiThreshold>,
    #[serde(default, rename = "process_threshold")]
    pub process_thresholds: Vec<ProcessThreshold>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Default node name — must satisfy KeyExprBuilder normalization.
            node_name: "aorta_sysmon".into(),
            host_name: String::new(),
            host_id: String::new(),
            topic_suffix: "sysmon/{host_name}/snapshot".into(),
            alert_topic_suffix: "sysmon/{host_name}/alerts".into(),
            interval_ms: 5000,
            capability_refresh_ms: 60_000,
            metrics: MetricFamilies::default(),
            filter: Filters::default(),
            process: ProcessConfig::default(),
            psi_thresholds: Vec::new(),
            process_thresholds: Vec::new(),
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut cfg = match path {
            Some(p) => {
                let raw = fs::read_to_string(p)
                    .with_context(|| format!("read config file {}", p.display()))?;
                toml::from_str(&raw).with_context(|| format!("parse TOML {}", p.display()))?
            }
            None => Self::default(),
        };
        cfg.apply_env();
        Ok(cfg)
    }

    /// `AORTA_SYSMON_*` env overrides applied between file and CLI.
    pub fn apply_env(&mut self) {
        if let Ok(v) = std::env::var("AORTA_SYSMON_NODE_NAME") {
            self.node_name = v;
        }
        if let Ok(v) = std::env::var("AORTA_SYSMON_HOST_NAME") {
            self.host_name = v;
        }
        if let Ok(v) = std::env::var("AORTA_SYSMON_HOST_ID") {
            self.host_id = v;
        }
        if let Ok(v) = std::env::var("AORTA_SYSMON_TOPIC_SUFFIX") {
            self.topic_suffix = v;
        }
        if let Ok(v) = std::env::var("AORTA_SYSMON_ALERT_TOPIC_SUFFIX") {
            self.alert_topic_suffix = v;
        }
        if let Ok(v) = std::env::var("AORTA_SYSMON_INTERVAL_MS") {
            if let Ok(n) = v.parse() {
                self.interval_ms = n;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_round_trips_through_toml() {
        let cfg = Config::default();
        let s = toml::to_string(&cfg).expect("serialize");
        let back: Config = toml::from_str(&s).expect("parse");
        assert_eq!(back.node_name, cfg.node_name);
        assert_eq!(back.interval_ms, cfg.interval_ms);
        assert_eq!(back.metrics.cpu, FamilyMode::On);
        assert_eq!(back.metrics.zenoh_topology, FamilyMode::Off);
    }

    #[test]
    fn family_mode_parses_case_insensitive() {
        use std::str::FromStr;
        assert_eq!(FamilyMode::from_str("ON").unwrap(), FamilyMode::On);
        assert_eq!(FamilyMode::from_str("Auto").unwrap(), FamilyMode::Auto);
        assert!(FamilyMode::from_str("nope").is_err());
    }

    #[test]
    fn config_has_phase4_off_by_default() {
        // zenoh_topology must be Off until a topology collector exists.
        let cfg = Config::default();
        assert_eq!(cfg.metrics.zenoh_topology, FamilyMode::Off);
    }
}
