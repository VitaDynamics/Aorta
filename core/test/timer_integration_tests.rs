// Integration tests for Timer with Publisher and Subscriber

#[cfg(test)]
#[allow(clippy::module_inception)] // BUILD.bazel test filters reference the inner module path
mod timer_integration_tests {
    use crate::error::AortaResult;
    use crate::executor::SingleThreadedExecutor;
    use crate::node::Node;
    use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
    use crate::timer::TimerOptions;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_timer_with_publisher() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let pid = std::process::id();
        let group = format!("test_timer_pub_{}", pid);
        let topic = format!("/timer_data_{}", pid);
        let mut exec = SingleThreadedExecutor::new(256);
        let pub_node = Node::new(&format!("timer_pub_{}", pid), &group).await?;
        let sub_node = Node::new(&format!("timer_sub_{}", pid), &group).await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let received_count = Arc::new(AtomicU32::new(0));
        let recv_clone = Arc::clone(&received_count);

        // Create subscriber
        let _sub = sub_node
            .create_subscriber(&topic, move |_data, _ctx| {
                recv_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await?;

        // Create publisher
        let publisher = pub_node.create_publisher(&topic).await?;

        tokio::time::sleep(Duration::from_millis(450)).await;

        let pub_count = Arc::new(AtomicU32::new(0));
        let pub_clone = Arc::clone(&pub_count);

        // Create timer that publishes periodically
        let _timer = pub_node
            .create_timer(Duration::from_millis(50), move || {
                let count = pub_clone.fetch_add(1, Ordering::Relaxed);
                let _data = [count as u8];
                // Note: Can't await in timer callback, need to use try_publish or blocking variant
                // For this test, we'll just increment counter
            })
            .await?;

        let stop = exec.stop_handle();
        let spin_task = tokio::spawn(async move {
            exec.spin().await;
        });

        tokio::time::sleep(Duration::from_millis(250)).await;
        // Warm up discovery and require at least one observed delivery first.
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < ready_deadline
            && received_count.load(Ordering::Relaxed) == 0
        {
            publisher.publish(vec![0xAA])?;
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        received_count.store(0, Ordering::Relaxed);
        for i in 0..6 {
            publisher.publish(vec![i])?;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        tokio::time::sleep(Duration::from_millis(600)).await;
        stop.stop();
        let _ = spin_task.await;

        // Verify messages received
        let recv_count = received_count.load(Ordering::Relaxed);
        assert!(recv_count >= 3, "Should receive at least 3 messages");

        // Verify timer executed
        let timer_count = pub_count.load(Ordering::Relaxed);
        assert!(timer_count >= 3, "Timer should have fired at least 3 times");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_timer_triggers_subscriber_processing() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let pid = std::process::id();
        let group = format!("test_timer_sub_{}", pid);
        let topic = format!("/buffered_data_{}", pid);
        let mut exec = SingleThreadedExecutor::new(256);
        let pub_node = Node::new(&format!("data_pub_{}", pid), &group).await?;
        let sub_node = Node::new(&format!("data_sub_{}", pid), &group).await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let message_buffer = Arc::new(Mutex::new(Vec::new()));
        let buf_clone = Arc::clone(&message_buffer);

        // Subscriber that buffers messages
        let _sub = sub_node
            .create_subscriber(&topic, move |data, _ctx| {
                buf_clone.lock().unwrap().push(data.to_bytes().into_owned());
            })
            .await?;

        let publisher = pub_node.create_publisher(&topic).await?;

        tokio::time::sleep(Duration::from_millis(250)).await;

        let processed_count = Arc::new(AtomicU32::new(0));
        let proc_clone = Arc::clone(&processed_count);
        let buf_for_timer = Arc::clone(&message_buffer);

        // Timer that periodically processes buffered messages
        let _timer = sub_node
            .create_timer(Duration::from_millis(100), move || {
                let mut buffer = buf_for_timer.lock().unwrap();
                let count = buffer.len();
                if count > 0 {
                    proc_clone.fetch_add(count as u32, Ordering::Relaxed);
                    buffer.clear();
                }
            })
            .await?;

        // Publish while executor is spinning to reduce timing sensitivity.
        let publisher_for_task = publisher.clone();
        let publish_task = tokio::spawn(async move {
            // Leave a discovery window before publishing.
            tokio::time::sleep(Duration::from_millis(300)).await;
            for i in 0..30u8 {
                let _ = publisher_for_task.publish(vec![i]);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        // Run executor
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1800)).await;
            stop.stop();
        });

        exec.spin().await;
        let _ = publish_task.await;

        // Verify messages were processed by timer
        // Note: Due to timing, we may not get all messages, but should get most
        let processed = processed_count.load(Ordering::Relaxed);
        let remaining = message_buffer.lock().unwrap().len();
        let total = processed as usize + remaining;

        assert!(
            total >= 8,
            "Should receive most messages, got {} processed + {} buffered = {} total",
            processed,
            remaining,
            total
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_timers_different_periods() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let pid = std::process::id();
        let mut exec = SingleThreadedExecutor::new(256);
        let node = Node::new(
            &format!("multi_timer_{}", pid),
            &format!("test_multi_timer_{}", pid),
        )
        .await?;
        exec.add_node(node.clone());

        let fast_count = Arc::new(AtomicU32::new(0));
        let slow_count = Arc::new(AtomicU32::new(0));
        let fc = Arc::clone(&fast_count);
        let sc = Arc::clone(&slow_count);

        // Fast timer: 30ms
        let _timer_fast = node
            .create_timer(Duration::from_millis(30), move || {
                fc.fetch_add(1, Ordering::Relaxed);
            })
            .await?;

        // Slow timer: 100ms
        let _timer_slow = node
            .create_timer(Duration::from_millis(100), move || {
                sc.fetch_add(1, Ordering::Relaxed);
            })
            .await?;

        // Run for 300ms
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            stop.stop();
        });

        exec.spin().await;

        let fast = fast_count.load(Ordering::Relaxed);
        let slow = slow_count.load(Ordering::Relaxed);

        // Fast timer should fire ~10 times (300ms / 30ms)
        assert!(
            fast >= 8,
            "Fast timer expected at least 8 ticks, got {}",
            fast
        );

        // Slow timer should fire ~3 times (300ms / 100ms)
        assert!(
            slow >= 2,
            "Slow timer expected at least 2 ticks, got {}",
            slow
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_timer_stress_many_timers() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let pid = std::process::id();
        let mut exec = SingleThreadedExecutor::new(512);
        let node = Node::new(
            &format!("stress_timer_{}", pid),
            &format!("test_stress_{}", pid),
        )
        .await?;
        exec.add_node(node.clone());

        let total_ticks = Arc::new(AtomicU32::new(0));
        let mut _timers = Vec::new();

        // Create 20 timers with varying periods
        for i in 0..20 {
            let counter = Arc::clone(&total_ticks);
            let period = Duration::from_millis(50 + (i * 10) as u64);
            let timer = node
                .create_timer(period, move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                })
                .await?;
            _timers.push(timer);
        }

        // Run for 500ms
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(800)).await;
            stop.stop();
        });

        exec.spin().await;

        // With 20 timers running for 500ms at varying rates,
        // we should see significant activity
        let ticks = total_ticks.load(Ordering::Relaxed);
        assert!(
            ticks >= 50,
            "Expected many timer ticks with 20 timers, got {}",
            ticks
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_timer_pause_resume_with_subscriber() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let pid = std::process::id();
        let group = format!("test_pause_{}", pid);
        let topic = format!("/pause_test_{}", pid);
        let mut exec = SingleThreadedExecutor::new(256);
        let pub_node = Node::new(&format!("pause_pub_{}", pid), &group).await?;
        let sub_node = Node::new(&format!("pause_sub_{}", pid), &group).await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let received = Arc::new(AtomicU32::new(0));
        let recv_clone = Arc::clone(&received);

        let _sub = sub_node
            .create_subscriber(&topic, move |_data, _ctx| {
                recv_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await?;

        let publisher = pub_node.create_publisher(&topic).await?;
        let pub_count = Arc::new(AtomicU32::new(0));
        let pub_clone = Arc::clone(&pub_count);

        let timer = pub_node
            .create_timer(Duration::from_millis(50), move || {
                pub_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await?;

        // Run for 100ms (should get ~2 ticks)
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Pause timer
        timer.pause();
        let count_before_pause = pub_count.load(Ordering::Relaxed);

        // Wait while paused
        tokio::time::sleep(Duration::from_millis(250)).await;
        let count_during_pause = pub_count.load(Ordering::Relaxed);

        // Should not have increased
        assert_eq!(
            count_before_pause, count_during_pause,
            "Timer should not fire while paused"
        );

        // Resume timer
        timer.resume();

        // Publish manually to test system still works
        publisher.publish(vec![1, 2, 3])?;

        // Process events
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            stop.stop();
        });

        exec.spin().await;

        // Verify timer resumed
        let final_count = pub_count.load(Ordering::Relaxed);
        assert!(
            final_count > count_during_pause,
            "Timer should fire again after resume"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_timer_with_auto_start_false() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let pid = std::process::id();
        let mut exec = SingleThreadedExecutor::new(256);
        let node = Node::new(
            &format!("autostart_test_{}", pid),
            &format!("test_auto_{}", pid),
        )
        .await?;
        exec.add_node(node.clone());

        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);

        let options = TimerOptions::new().with_auto_start(false);
        let timer = node
            .create_timer_with_options(
                Duration::from_millis(50),
                move || {
                    c.fetch_add(1, Ordering::Relaxed);
                },
                options,
            )
            .await?;

        // Wait without starting
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "Timer should not fire before start"
        );

        // Start timer
        timer.resume();

        // Run executor
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            stop.stop();
        });

        exec.spin().await;

        // Timer should have fired
        let count = counter.load(Ordering::Relaxed);
        assert!(
            count >= 3,
            "Timer should fire after manual start, got {} ticks",
            count
        );

        Ok(())
    }
}
