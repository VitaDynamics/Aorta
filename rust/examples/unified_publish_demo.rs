use aorta_sdk::log_prelude::*;

/// Unified Publish API Demo
///
/// Demonstrates the new synchronous fire-and-forget publish() API that works
/// in any context: async functions, sync functions, and timer callbacks.
///
/// No .await needed - publish() is always synchronous!
///
/// Run with: bazel run //rust/examples:unified_publish_demo
use aorta_core::publisher::Publisher as CorePublisher;
use aorta_sdk::{AortaResult, Node};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> AortaResult<()> {
    aorta_info!("=== Unified Publish API Demo ===\n");

    let node = Node::new("demo_node", "default").await?;
    aorta_info!("✓ Created node: {}\n", node.name());

    let publisher = node.create_publisher_raw("/demo/unified").await?;
    aorta_info!("✓ Created publisher on topic: {}\n", publisher.topic());

    // Demo 1: Publish from async context
    aorta_info!("Demo 1: Publishing from async context");
    publish_from_async(&publisher).await?;

    // Demo 2: Publish from timer callback (sync context)
    aorta_info!("\nDemo 2: Publishing from timer callback (sync context)");
    publish_from_timer(&node, publisher.clone()).await?;

    // Demo 3: Publish from sync closure
    aorta_info!("\nDemo 3: Publishing from sync closure");
    publish_from_sync_closure(publisher.clone())?;

    aorta_info!("\n=== All demos completed successfully! ===");
    aorta_info!("\nKey takeaway: publish() works the same way everywhere - no .await needed!");

    node.close().await?;
    Ok(())
}

/// Demo 1: Publish from async function
async fn publish_from_async(publisher: &CorePublisher) -> AortaResult<()> {
    for i in 0..5 {
        let data = format!("async-message-{}", i);
        let bytes = data.as_bytes().to_vec();

        // No .await - publish() is synchronous fire-and-forget!
        publisher.publish(bytes)?;

        aorta_info!("  ✓ Published: {}", data);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

/// Demo 2: Publish from timer callback (sync context)
async fn publish_from_timer(node: &Node, publisher: CorePublisher) -> AortaResult<()> {
    let counter = Arc::new(AtomicU64::new(0));
    let counter_clone = Arc::clone(&counter);

    let timer = node
        .create_timer(Duration::from_millis(50), move || {
            let count = counter_clone.fetch_add(1, Ordering::Relaxed);
            let data = format!("timer-message-{}", count);

            // Works in sync timer callback - no .await!
            if let Err(e) = publisher.publish(data.as_bytes().to_vec()) {
                aorta_error!("  ✗ Timer publish error: {}", e);
            } else {
                aorta_info!("  ✓ Timer published: {}", data);
            }

            // Stop after 5 messages
            if count >= 4 {
                // Timer will be cleaned up when dropped
            }
        })
        .await?;

    // Run timer for a bit
    tokio::time::sleep(Duration::from_millis(300)).await;
    timer.pause();

    let final_count = counter.load(Ordering::Relaxed);
    aorta_info!("  → Timer published {} messages", final_count);

    Ok(())
}

/// Demo 3: Publish from sync closure
fn publish_from_sync_closure(publisher: CorePublisher) -> AortaResult<()> {
    // Simulate sync context (like processing callback data)
    let process_and_publish = || -> AortaResult<()> {
        for i in 0..3 {
            let data = format!("sync-closure-{}", i);
            let bytes = data.as_bytes().to_vec();

            // Works in sync closure - no .await!
            publisher.publish(bytes)?;

            aorta_info!("  ✓ Sync published: {}", data);

            // Simulate some processing
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    };

    process_and_publish()?;
    Ok(())
}
