// Performance benchmark tests for Aorta Middleware
// These tests are marked #[ignore] by default because they take longer to run
// Run with: bazel test //core:aorta_core_test -- --test-arg=--include-ignored

#[cfg(test)]
#[allow(clippy::module_inception)] // BUILD.bazel test filters reference the inner module path
mod performance_tests {
    use crate::error::AortaResult;
    use crate::executor::SingleThreadedExecutor;
    use crate::node::Node;
    use crate::publisher::PublisherOptions;
    use crate::qos::QoS;
    use crate::subscriber::SubscriberOptions;
    use crate::test_support::ENV_LOCK;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore] // Run with --include-ignored
    async fn benchmark_pub_sub_latency() -> AortaResult<()> {
        let mut exec = SingleThreadedExecutor::new(1024);
        let pub_node = Node::new("lat_pub", "bench_lat").await?;
        let sub_node = Node::new("lat_sub", "bench_lat").await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let latencies = Arc::new(Mutex::new(Vec::new()));
        let lat_clone = Arc::clone(&latencies);

        // Subscriber that measures receive time (opt in to timestamps)
        let options = SubscriberOptions::new()
            .with_receive_timestamp(true)
            .with_publisher_timestamp(true);
        let _sub = sub_node
            .create_subscriber_with_options(
                "/latency_test",
                move |_data, ctx| {
                    let receive_time = ctx.receive_stamp_ns;
                    if let Some(pub_time) = ctx.publisher_stamp_ns {
                        let latency_ns = receive_time - pub_time;
                        lat_clone.lock().unwrap().push(latency_ns);
                    }
                },
                options,
            )
            .await?;

        let publisher = pub_node.create_publisher("/latency_test").await?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish 100 messages
        for i in 0..100u8 {
            publisher.publish(vec![i])?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Process events
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            stop.stop();
        });

        exec.spin().await;

        // Calculate statistics
        let lats = latencies.lock().unwrap();
        if !lats.is_empty() {
            let sum: i64 = lats.iter().sum();
            let avg = sum / lats.len() as i64;
            let min = *lats.iter().min().unwrap();
            let max = *lats.iter().max().unwrap();

            println!(
                "\n=== Pub/Sub Latency Benchmark ===\n\
                 Messages: {}\n\
                 Avg Latency: {:.2} µs\n\
                 Min Latency: {:.2} µs\n\
                 Max Latency: {:.2} µs",
                lats.len(),
                avg as f64 / 1000.0,
                min as f64 / 1000.0,
                max as f64 / 1000.0
            );

            // Sanity check: local latency should be reasonable
            assert!(avg < 10_000_000, "Average latency too high: {} ns", avg); // < 10ms
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore] // Run with --include-ignored
    async fn benchmark_pub_sub_throughput() -> AortaResult<()> {
        let mut exec = SingleThreadedExecutor::new(2048);
        let pub_node = Node::new("thr_pub", "bench_thr").await?;
        let sub_node = Node::new("thr_sub", "bench_thr").await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let received_count = Arc::new(AtomicU32::new(0));
        let recv_clone = Arc::clone(&received_count);

        let _sub = sub_node
            .create_subscriber("/throughput_test", move |_data, _ctx| {
                recv_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await?;

        let publisher = pub_node.create_publisher("/throughput_test").await?;

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Measure throughput
        let num_messages = 1000;
        let start = Instant::now();

        // Publish as fast as possible
        for i in 0..num_messages {
            let data = vec![(i % 256) as u8; 100]; // 100-byte messages
            publisher.publish(data)?;
        }

        let publish_duration = start.elapsed();

        // Process events
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(3000)).await;
            stop.stop();
        });

        exec.spin().await;

        let total_duration = start.elapsed();
        let received = received_count.load(Ordering::Relaxed);

        let publish_throughput = num_messages as f64 / publish_duration.as_secs_f64();
        let total_throughput = received as f64 / total_duration.as_secs_f64();

        println!(
            "\n=== Pub/Sub Throughput Benchmark ===\n\
             Messages Published: {}\n\
             Messages Received: {}\n\
             Publish Throughput: {:.0} msg/s\n\
             Total Throughput: {:.0} msg/s\n\
             Publish Duration: {:.2} ms\n\
             Total Duration: {:.2} ms",
            num_messages,
            received,
            publish_throughput,
            total_throughput,
            publish_duration.as_millis(),
            total_duration.as_millis()
        );

        // Sanity check: should receive most messages
        assert!(
            received >= (num_messages * 90 / 100),
            "Should receive at least 90% of messages, got {}/{}",
            received,
            num_messages
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore] // Run with --include-ignored
    async fn benchmark_timer_precision() -> AortaResult<()> {
        let mut exec = SingleThreadedExecutor::new(256);
        let node = Node::new("timer_bench", "bench_timer").await?;
        exec.add_node(node.clone());

        let timestamps = Arc::new(Mutex::new(Vec::new()));
        let ts_clone = Arc::clone(&timestamps);

        let timer_period = Duration::from_millis(50);
        let _timer = node
            .create_timer(timer_period, move || {
                ts_clone.lock().unwrap().push(Instant::now());
            })
            .await?;

        // Run for 1 second
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            stop.stop();
        });

        exec.spin().await;

        // Analyze timer precision
        let times = timestamps.lock().unwrap();
        if times.len() >= 2 {
            let mut jitters = Vec::new();
            for i in 1..times.len() {
                let actual_interval = times[i].duration_since(times[i - 1]);
                let jitter =
                    (actual_interval.as_micros() as i64 - timer_period.as_micros() as i64).abs();
                jitters.push(jitter);
            }

            let avg_jitter = jitters.iter().sum::<i64>() / jitters.len() as i64;
            let max_jitter = *jitters.iter().max().unwrap();

            println!(
                "\n=== Timer Precision Benchmark ===\n\
                 Period: {:?}\n\
                 Ticks: {}\n\
                 Avg Jitter: {:.2} µs\n\
                 Max Jitter: {:.2} µs",
                timer_period,
                times.len(),
                avg_jitter as f64 / 1000.0,
                max_jitter as f64 / 1000.0
            );

            // Sanity check: jitter should be reasonable
            assert!(
                avg_jitter < 10_000,
                "Average jitter too high: {} µs",
                avg_jitter
            );
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore] // Run with --include-ignored
    async fn benchmark_qos_overhead() -> AortaResult<()> {
        let mut exec = SingleThreadedExecutor::new(2048);
        let pub_node = Node::new("qos_pub", "bench_qos").await?;
        let sub_node = Node::new("qos_sub", "bench_qos").await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        // Test different QoS settings
        let qos_configs = vec![
            ("BestEffort", QoS::sensor_data()),
            ("Reliable", QoS::realtime_control()),
            ("TransientLocal", QoS::state_update()),
        ];

        for (name, qos) in qos_configs {
            let received = Arc::new(AtomicU32::new(0));
            let recv_clone = Arc::clone(&received);

            let _sub = sub_node
                .create_subscriber(&format!("/qos_bench_{}", name), move |_data, _ctx| {
                    recv_clone.fetch_add(1, Ordering::Relaxed);
                })
                .await?;

            let options = PublisherOptions {
                qos,
                ..Default::default()
            };
            let publisher = pub_node
                .create_publisher_with_options(&format!("/qos_bench_{}", name), options)
                .await?;

            tokio::time::sleep(Duration::from_millis(100)).await;

            let num_msgs = 100;
            let start = Instant::now();

            for i in 0..num_msgs {
                publisher.publish(vec![i])?;
            }

            let duration = start.elapsed();
            let throughput = num_msgs as f64 / duration.as_secs_f64();

            println!(
                "QoS {}: {:.0} msg/s ({:.2} ms for {} msgs)",
                name,
                throughput,
                duration.as_millis(),
                num_msgs
            );
        }

        // Process events
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            stop.stop();
        });

        exec.spin().await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore] // Run with --include-ignored
    async fn benchmark_message_sizes() -> AortaResult<()> {
        let mut exec = SingleThreadedExecutor::new(2048);
        let pub_node = Node::new("size_pub", "bench_size").await?;
        let sub_node = Node::new("size_sub", "bench_size").await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let message_sizes = vec![10, 100, 1024, 10240, 102400]; // 10B to 100KB

        for size in message_sizes {
            let received = Arc::new(AtomicU32::new(0));
            let recv_clone = Arc::clone(&received);

            let _sub = sub_node
                .create_subscriber(&format!("/size_bench_{}", size), move |_data, _ctx| {
                    recv_clone.fetch_add(1, Ordering::Relaxed);
                })
                .await?;

            let publisher = pub_node
                .create_publisher(&format!("/size_bench_{}", size))
                .await?;

            tokio::time::sleep(Duration::from_millis(100)).await;

            let num_msgs = 100;
            let start = Instant::now();

            for _ in 0..num_msgs {
                let data = vec![0u8; size];
                publisher.publish(data)?;
            }

            let duration = start.elapsed();
            let throughput_msgs = num_msgs as f64 / duration.as_secs_f64();
            let throughput_mb = (num_msgs * size) as f64 / duration.as_secs_f64() / 1_000_000.0;

            println!(
                "Message size {: >6} bytes: {: >6.0} msg/s, {: >6.2} MB/s",
                size, throughput_msgs, throughput_mb
            );
        }

        // Process events
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(3000)).await;
            stop.stop();
        });

        exec.spin().await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore] // Run with --include-ignored
    async fn benchmark_subscriber_count_scalability() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let mut exec = SingleThreadedExecutor::new(4096);
        let pub_node = Node::new("scale_pub", "bench_scale").await?;
        exec.add_node(pub_node.clone());

        let long = matches!(
            std::env::var("AORTA_PERF_LONG")
                .unwrap_or_default()
                .to_lowercase()
                .as_str(),
            "1" | "true" | "yes"
        );
        let subscriber_counts = if long { vec![1, 5, 10, 20] } else { vec![1, 5] };
        let settle_delay = if long {
            Duration::from_millis(200)
        } else {
            Duration::from_millis(60)
        };
        let num_msgs = if long { 50 } else { 20 };
        let post_delay = if long {
            Duration::from_millis(500)
        } else {
            Duration::from_millis(150)
        };
        let stop_delay = if long {
            Duration::from_millis(1000)
        } else {
            Duration::from_millis(400)
        };

        for count in subscriber_counts {
            let mut subscribers = Vec::new();
            let total_received = Arc::new(AtomicU32::new(0));

            // Create N subscribers
            for i in 0..count {
                let sub_node = Node::new(&format!("scale_sub_{}", i), "bench_scale").await?;
                exec.add_node(sub_node.clone());

                let recv_clone = Arc::clone(&total_received);
                let sub = sub_node
                    .create_subscriber("/scale_test", move |_data, _ctx| {
                        recv_clone.fetch_add(1, Ordering::Relaxed);
                    })
                    .await?;
                subscribers.push(sub);
            }

            let publisher = pub_node.create_publisher("/scale_test").await?;
            tokio::time::sleep(settle_delay).await;

            let start = Instant::now();

            for i in 0..num_msgs {
                publisher.publish(vec![i])?;
            }

            tokio::time::sleep(post_delay).await;

            let duration = start.elapsed();
            let throughput = num_msgs as f64 / duration.as_secs_f64();
            let received = total_received.load(Ordering::Relaxed);
            let expected = num_msgs * count;

            println!(
                "Subscribers: {: >2}, Throughput: {: >6.0} msg/s, Received: {}/{} ({:.1}%)",
                count,
                throughput,
                received,
                expected,
                received as f64 / expected as f64 * 100.0
            );
        }

        // Process events
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(stop_delay).await;
            stop.stop();
        });

        exec.spin().await;

        Ok(())
    }
}
