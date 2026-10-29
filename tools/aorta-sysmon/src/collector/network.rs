//! Network collector.

use std::collections::HashMap;
use std::fs;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::Filters;
use crate::procfs_root::ProcfsRoot;

use super::{delta_u64, glob_match, per_sec};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetCounters {
    pub rx_bytes: u64,
    pub rx_packets: u64,
    pub rx_errors: u64,
    pub rx_drop: u64,
    pub rx_fifo: u64,
    pub rx_frame: u64,
    pub rx_compressed: u64,
    pub rx_multicast: u64,
    pub tx_bytes: u64,
    pub tx_packets: u64,
    pub tx_errors: u64,
    pub tx_drop: u64,
    pub tx_fifo: u64,
    pub tx_colls: u64,
    pub tx_carrier: u64,
    pub tx_compressed: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetSample {
    pub interfaces: HashMap<String, NetCounters>,
}

impl NetSample {
    pub fn read(root: &ProcfsRoot) -> Result<Self> {
        let body = fs::read_to_string(root.proc_path("net/dev")).context("read /proc/net/dev")?;
        Ok(Self::parse(&body))
    }

    pub fn parse(body: &str) -> Self {
        // /proc/net/dev format (skip 2 header lines):
        //   ifname: rx_bytes rx_packets rx_errors rx_drop rx_fifo rx_frame
        //           rx_compressed rx_multicast tx_bytes tx_packets tx_errors
        //           tx_drop tx_fifo tx_colls tx_carrier tx_compressed
        let mut interfaces = HashMap::new();
        for line in body.lines().skip(2) {
            let mut parts = line.split(':');
            let name = match parts.next() {
                Some(n) => n.trim().to_string(),
                None => continue,
            };
            if name.is_empty() {
                continue;
            }
            let rest = match parts.next() {
                Some(r) => r,
                None => continue,
            };
            let cols: Vec<u64> = rest
                .split_whitespace()
                .map(|s| s.parse().unwrap_or(0))
                .collect();
            if cols.len() < 16 {
                continue;
            }
            let counters = NetCounters {
                rx_bytes: cols[0],
                rx_packets: cols[1],
                rx_errors: cols[2],
                rx_drop: cols[3],
                rx_fifo: cols[4],
                rx_frame: cols[5],
                rx_compressed: cols[6],
                rx_multicast: cols[7],
                tx_bytes: cols[8],
                tx_packets: cols[9],
                tx_errors: cols[10],
                tx_drop: cols[11],
                tx_fifo: cols[12],
                tx_colls: cols[13],
                tx_carrier: cols[14],
                tx_compressed: cols[15],
            };
            interfaces.insert(name, counters);
        }
        Self { interfaces }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetIfStat {
    pub ifname: String,
    pub rx_bytes_per_sec: u64,
    pub rx_packets_per_sec: u64,
    pub tx_bytes_per_sec: u64,
    pub tx_packets_per_sec: u64,
    pub rx_errors: u64,
    pub rx_drop: u64,
    pub tx_errors: u64,
    pub tx_drop: u64,
    pub oper_state: String,
    pub mtu: u32,
    /// `-1` for virtual or down interfaces.
    pub speed_mbps: i32,
}

pub fn delta(
    prev: &NetSample,
    curr: &NetSample,
    interval_ms: u64,
    filters: &Filters,
    root: &ProcfsRoot,
) -> Vec<NetIfStat> {
    let mut out = Vec::new();
    for (ifname, c) in &curr.interfaces {
        if !accept(ifname, filters) {
            continue;
        }
        let p = match prev.interfaces.get(ifname) {
            Some(p) => p,
            None => continue,
        };
        out.push(compute(ifname, p, c, interval_ms, root));
    }
    out.sort_by(|a, b| a.ifname.cmp(&b.ifname));
    out
}

fn accept(ifname: &str, filters: &Filters) -> bool {
    let included =
        filters.net_include.is_empty() || filters.net_include.iter().any(|p| glob_match(p, ifname));
    let excluded = filters.net_exclude.iter().any(|p| glob_match(p, ifname));
    included && !excluded
}

fn compute(
    ifname: &str,
    prev: &NetCounters,
    curr: &NetCounters,
    interval_ms: u64,
    root: &ProcfsRoot,
) -> NetIfStat {
    let rx_bytes = delta_u64(prev.rx_bytes, curr.rx_bytes);
    let rx_pkts = delta_u64(prev.rx_packets, curr.rx_packets);
    let tx_bytes = delta_u64(prev.tx_bytes, curr.tx_bytes);
    let tx_pkts = delta_u64(prev.tx_packets, curr.tx_packets);

    let (oper_state, mtu, speed_mbps) = read_sysfs_attrs(root, ifname);

    NetIfStat {
        ifname: ifname.to_string(),
        rx_bytes_per_sec: per_sec(rx_bytes, interval_ms),
        rx_packets_per_sec: per_sec(rx_pkts, interval_ms),
        tx_bytes_per_sec: per_sec(tx_bytes, interval_ms),
        tx_packets_per_sec: per_sec(tx_pkts, interval_ms),
        rx_errors: curr.rx_errors,
        rx_drop: curr.rx_drop,
        tx_errors: curr.tx_errors,
        tx_drop: curr.tx_drop,
        oper_state,
        mtu,
        speed_mbps,
    }
}

fn read_sysfs_attrs(root: &ProcfsRoot, ifname: &str) -> (String, u32, i32) {
    let base = root.sys_path(format!("class/net/{}", ifname));
    let oper = fs::read_to_string(base.join("operstate"))
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".into());
    let mtu = fs::read_to_string(base.join("mtu"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    // Reading `speed` on a down interface returns EINVAL; treat that as -1.
    let speed_mbps = fs::read_to_string(base.join("speed"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(-1);
    (oper, mtu, speed_mbps)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NET_DEV_PREV: &str = "\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:       0       0    0    0    0     0          0         0       0       0    0    0    0     0       0          0
  eth0:    1000      10    0    0    0     0          0         0    2000      20    0    0    0     0       0          0
";

    const NET_DEV_CURR: &str = "\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:       0       0    0    0    0     0          0         0       0       0    0    0    0     0       0          0
  eth0:    2000      20    0    0    0     0          0         0    4000      40    0    0    0     0       0          0
";

    #[test]
    fn rx_tx_rates_per_sec() {
        let p = NetSample::parse(NET_DEV_PREV);
        let c = NetSample::parse(NET_DEV_CURR);
        let filters = Filters::default();
        let stats = delta(&p, &c, 1000, &filters, &ProcfsRoot::fixture("/tmp/nope"));
        let eth0 = stats.iter().find(|s| s.ifname == "eth0").expect("eth0");
        assert_eq!(eth0.rx_bytes_per_sec, 1000);
        assert_eq!(eth0.tx_bytes_per_sec, 2000);
        // lo should be filtered out by default.
        assert!(!stats.iter().any(|s| s.ifname == "lo"));
    }

    #[test]
    fn filter_excludes_lo_docker_veth() {
        let filters = Filters::default();
        assert!(accept("eth0", &filters));
        assert!(accept("wlan0", &filters));
        assert!(accept("enp0s3", &filters));
        assert!(!accept("lo", &filters));
        assert!(!accept("docker0", &filters));
        assert!(!accept("veth1234", &filters));
        assert!(!accept("br-abc", &filters));
    }
}
