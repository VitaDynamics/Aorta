//! Context put QoS tests: every context put must carry
//! `CongestionControl::Block`.
//!
//! Honest semantics: the pinned zenoh fork bounds the Block wait
//! (`wait_before_close`, default 5 s); on overflow the message is dropped
//! and the transport closed, and `session.put` returns Ok regardless.
//! Residual loss therefore coincides with transport disruption and is
//! healed reader-side (bounded watch re-creation + TTL/heartbeat), NOT by
//! this flag.
//!
//! Observability: `Sample::congestion_control()` on a subscriber reflects
//! the sender's setting end-to-end (the QoS ext travels with the push), so
//! a raw zenoh subscriber on the context prefix can assert the flag
//! without a congested-channel harness (the fan-in benchmark, item G,
//! covers the deep behavioral cells later).

use std::time::Duration;

use zenoh::qos::CongestionControl;

use crate::contextstore::ContextStore;
use crate::error::{AortaError, AortaResult};
use crate::node::Node;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn context_puts_carry_block_congestion_control() -> AortaResult<()> {
    let node = Node::new("ctxB_qos_block", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_qos_block/**").await?;

    // Raw subscriber on the same session observes the wire samples and
    // reports the congestion control each put carried.
    let prefix = node.key_builder().context_prefix("system_qos_block/**")?;
    let (tx, rx) = std::sync::mpsc::channel();
    let sub = node
        .session()
        .inner()
        .declare_subscriber(prefix)
        .callback(move |sample: zenoh::sample::Sample| {
            let _ = tx.send(sample.congestion_control());
        })
        .await
        .map_err(|err| {
            AortaError::transport(crate::error::TransportError::backend(format!(
                "qos probe subscribe failed: {}",
                err
            )))
        })?;

    store.set_u64("system_qos_block/state", 1).await?;

    // Poll non-blockingly: the sample callback runs on zenoh's own
    // threads, and blocking a tokio worker from an async task is
    // forbidden.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let cc = loop {
        match rx.try_recv() {
            Ok(cc) => break cc,
            Err(std::sync::mpsc::TryRecvError::Empty) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                panic!("no context sample observed within 2s")
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                panic!("qos probe channel closed")
            }
        }
    };
    assert_eq!(
        cc,
        CongestionControl::Block,
        "context puts must use CongestionControl::Block (not the Drop default)"
    );

    sub.undeclare().await.map_err(|err| {
        AortaError::transport(crate::error::TransportError::backend(format!(
            "qos probe undeclare failed: {}",
            err
        )))
    })?;
    Ok(())
}
