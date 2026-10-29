use aorta_sdk::log_prelude::*;

use aorta_sdk::{AortaResult, Node, TimerOptions};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> AortaResult<()> {
    aorta_info!("=== Aorta Timer Demo ===\n");

    // Create a node
    let node = Node::new("timer_demo", "default").await?;
    aorta_info!("Created node: {}", node.name());

    // Shared counter for demonstrating timer functionality
    let counter = Arc::new(AtomicU64::new(0));

    // Example 1: Basic timer with default options
    aorta_info!("\n1. Creating basic timer (100ms period)...");
    let counter_clone = Arc::clone(&counter);
    let _timer1 = node
        .create_timer(Duration::from_millis(100), move || {
            let count = counter_clone.fetch_add(1, Ordering::Relaxed) + 1;
            aorta_info!("  Timer 1 tick: count = {}", count);
        })
        .await?;

    // Wait for a few ticks
    tokio::time::sleep(Duration::from_millis(350)).await;
    aorta_info!("  Timer 1 executed ~3 times");

    // Example 2: Timer with custom options (auto_start = false)
    aorta_info!("\n2. Creating timer with auto_start=false...");
    let counter2 = Arc::new(AtomicU64::new(0));
    let counter2_clone = Arc::clone(&counter2);
    let options = TimerOptions::new().with_auto_start(false);
    let timer2 = node
        .create_timer_with_options(
            Duration::from_millis(100),
            move || {
                let count = counter2_clone.fetch_add(1, Ordering::Relaxed) + 1;
                aorta_info!("  Timer 2 tick: count = {}", count);
            },
            options,
        )
        .await?;

    aorta_info!("  Timer 2 created but not running");
    tokio::time::sleep(Duration::from_millis(250)).await;
    aorta_info!(
        "  Timer 2 count after 250ms: {} (should be 0)",
        counter2.load(Ordering::Relaxed)
    );

    aorta_info!("  Resuming timer 2...");
    timer2.resume();
    tokio::time::sleep(Duration::from_millis(250)).await;
    aorta_info!(
        "  Timer 2 count after resuming: {} (should be ~2)",
        counter2.load(Ordering::Relaxed)
    );

    // Example 3: Pause and resume
    aorta_info!("\n3. Demonstrating pause/resume...");
    let counter3 = Arc::new(AtomicU64::new(0));
    let counter3_clone = Arc::clone(&counter3);
    let timer3 = node
        .create_timer(Duration::from_millis(100), move || {
            let count = counter3_clone.fetch_add(1, Ordering::Relaxed) + 1;
            aorta_info!("  Timer 3 tick: count = {}", count);
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(250)).await;
    aorta_info!("  Pausing timer 3...");
    timer3.pause();

    let count_before_pause = counter3.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let count_after_pause = counter3.load(Ordering::Relaxed);
    aorta_info!(
        "  Count before pause: {}, after pause: {} (should be same)",
        count_before_pause,
        count_after_pause
    );

    aorta_info!("  Resuming timer 3...");
    timer3.resume();
    tokio::time::sleep(Duration::from_millis(250)).await;
    aorta_info!(
        "  Count after resume: {} (should be higher)",
        counter3.load(Ordering::Relaxed)
    );

    // Example 4: Multiple concurrent timers
    aorta_info!("\n4. Creating multiple concurrent timers...");
    let _timer_fast = node
        .create_timer(Duration::from_millis(50), || {
            aorta_info!("  Fast timer (50ms)");
        })
        .await?;

    let _timer_slow = node
        .create_timer(Duration::from_millis(150), || {
            aorta_info!("  Slow timer (150ms)");
        })
        .await?;

    aorta_info!("  Running multiple timers for 500ms...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Example 5: Timer with invoke_on_missed
    aorta_info!("\n5. Timer with invoke_on_missed=true...");
    let counter5 = Arc::new(AtomicU64::new(0));
    let counter5_clone = Arc::clone(&counter5);
    let options = TimerOptions::new().with_invoke_on_missed(true);
    let _timer5 = node
        .create_timer_with_options(
            Duration::from_millis(50),
            move || {
                let count = counter5_clone.fetch_add(1, Ordering::Relaxed) + 1;
                aorta_info!("  Timer 5 tick: count = {}", count);
                // Simulate slow callback
                std::thread::sleep(Duration::from_millis(80));
            },
            options,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(300)).await;
    aorta_info!(
        "  Timer 5 final count: {} (may show delayed execution)",
        counter5.load(Ordering::Relaxed)
    );

    aorta_info!("\n=== Timer Demo Complete ===");
    aorta_info!("Total timer 1 ticks: {}", counter.load(Ordering::Relaxed));

    // Cleanup
    node.close().await?;
    Ok(())
}
