//! `aorta stats` — router-level traffic counters.
//!
//! Surfaces the Zenoh `stats`-feature byte/message counters that the introspect
//! client already fetches from `@/<zid>/router/metrics`. Counters are cumulative
//! since router start (a single read is a total, not a rate).

use crate::output::{emit_json, output_format, OutputFormat};
use crate::util::human_bytes;
use anyhow::Result;
use std::sync::Arc;

/// Print per-router tx/rx byte and message counters.
pub async fn routers(group: Option<&str>) -> Result<()> {
    let group = group.unwrap_or("default");
    let session = Arc::new(crate::cmd::open_session().await?);
    let client = crate::cmd::build_introspect_client(session, group)?;

    let overview = client
        .fetch_zenoh_admin_overview()
        .await
        .map_err(|err| anyhow::anyhow!("failed to fetch router metrics: {}", err))?;

    if output_format() == OutputFormat::Json {
        let rows: Vec<_> = overview
            .routers
            .iter()
            .map(|router| {
                let metrics = router.metrics.as_ref().map(|m| {
                    serde_json::json!({
                        "tx_bytes_total": m.tx_bytes_total,
                        "tx_msgs_total": m.tx_msgs_total,
                        "rx_bytes_total": m.rx_bytes_total,
                        "rx_msgs_total": m.rx_msgs_total,
                        "transports_opened": m.transports_opened,
                        "links_opened": m.links_opened,
                    })
                });
                serde_json::json!({
                    "zid": router.zid,
                    "whatami": router.whatami,
                    "metrics_available": router.metrics_available,
                    "metrics": metrics,
                })
            })
            .collect();
        emit_json(&rows)?;
        return Ok(());
    }

    eprintln!(
        "Zenoh router traffic counters (cumulative since router start) — {} router(s):\n",
        overview.routers.len()
    );

    for router in &overview.routers {
        println!("Router {} [{}]", router.zid, router.whatami);
        match router.metrics.as_ref() {
            Some(metrics) => {
                println!(
                    "  tx: {} ({} B), {} msgs",
                    human_bytes(metrics.tx_bytes_total),
                    metrics.tx_bytes_total,
                    metrics.tx_msgs_total,
                );
                println!(
                    "  rx: {} ({} B), {} msgs",
                    human_bytes(metrics.rx_bytes_total),
                    metrics.rx_bytes_total,
                    metrics.rx_msgs_total,
                );
                if let Some(transports) = metrics.transports_opened {
                    println!("  transports opened: {}", transports);
                }
                if let Some(links) = metrics.links_opened {
                    println!("  links opened: {}", links);
                }
            }
            None if router.metrics_available => {
                println!(
                    "  metrics: capability present but body unreadable \
                     (router did not honor compression=false, or ACL-gated)"
                );
            }
            None => {
                println!(
                    "  metrics: unavailable \
                     (router built without the `stats` feature, or hidden by admin-space ACL)"
                );
            }
        }
        println!();
    }

    Ok(())
}
