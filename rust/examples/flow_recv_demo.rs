//! Flow: the async pull surface — `recv_in` completes
//! `Err(Stopped)` on scope cancel (Flow Concepts §6.6).
//!
//! Run: cargo run --example flow_recv_demo --features examples

use aorta_sdk::Node;
use std::time::Duration;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = Node::new("flow_recv_demo", "default").await?;
    let sub = node.create_subscriber_pull("demo/flow_recv", 4).await?;
    let publisher = node.create_publisher_raw("demo/flow_recv").await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let scope = aorta_sdk::Scope::new_root(Default::default());

    // Value flow: publish, then await the sample.
    publisher.publish(b"hello".to_vec())?;
    match sub.recv_in(&scope).await {
        Ok(sample) => println!("[flow-recv] got {} bytes", sample.payload.to_bytes().len()),
        Err(stopped) => println!("[flow-recv] stopped early: {stopped:?}"),
    }

    // Stopped on cancel: a parked recv_in resolves promptly.
    let scope2 = scope.clone();
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        scope2.cancel(aorta_sdk::CancelReason::Explicit);
    });
    match sub.recv_in(&scope).await {
        Ok(_) => println!("[flow-recv] unexpected sample"),
        Err(stopped) => println!("[flow-recv] parked recv stopped: {stopped:?}"),
    }
    canceller.await?;

    node.close().await?;
    println!("[flow-recv] demo complete");
    Ok(())
}
