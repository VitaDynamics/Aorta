use crate::{
    CallbackGroupType, ExecutorAdmissionOptions, ExecutorClassPolicyOptions, ExecutorNodeAttachExt,
    ExecutorOptions, MultiThreadedExecutor, Node, PrioritySchedulerOptions, SchedulerMode,
    SingleThreadedExecutor,
};
use aorta_core::event_queue::{Event, EventKind};
use std::time::Duration;

#[test]
fn executor_options_default_is_fifo() {
    let options = ExecutorOptions::default();
    assert!(matches!(options.mode, SchedulerMode::Fifo));
}

#[test]
fn priority_scheduler_options_default_values() {
    let options = PrioritySchedulerOptions::default();
    assert_eq!(options.max_drain_per_cycle, 32);
    assert_eq!(options.weights, [8, 4, 2, 1, 1]);
    assert!(options.map_subscription_qos);
}

#[test]
fn executor_admission_options_default_values() {
    let admission = ExecutorAdmissionOptions::default();
    assert_eq!(admission.max_queued_events, 0);
    assert_eq!(admission.max_queued_bytes, 0);
}

#[test]
fn executor_class_policy_options_default_values() {
    let class_policy = ExecutorClassPolicyOptions::default();
    assert_eq!(
        class_policy.subscriber_overflow_policy,
        aorta_core::subscriber::SubscriberOverflowPolicy::Drop
    );
    assert_eq!(class_policy.subscriber_overflow_relay_capacity, 1024);
    assert_eq!(
        class_policy.rpc_overflow_policy,
        aorta_core::service::RpcOverflowPolicy::FastFailBusy
    );
    assert_eq!(
        class_policy.rpc_overflow_block_timeout,
        Duration::from_millis(0)
    );
    assert_eq!(
        class_policy.timer_overflow_policy,
        aorta_core::timer::TimerOverflowPolicy::CoalesceLatest
    );
    assert_eq!(
        class_policy.timer_overflow_block_timeout,
        Duration::from_millis(0)
    );
}

#[tokio::test]
async fn sdk_executor_new_with_priority_options_constructs() {
    let options = ExecutorOptions {
        mode: SchedulerMode::Priority,
        priority: PrioritySchedulerOptions {
            max_drain_per_cycle: 16,
            weights: [9, 5, 3, 2, 1],
            map_subscription_qos: false,
        },
        admission: ExecutorAdmissionOptions {
            max_queued_events: 128,
            max_queued_bytes: 4096,
        },
        class_policy: ExecutorClassPolicyOptions {
            subscriber_overflow_policy: aorta_core::subscriber::SubscriberOverflowPolicy::Relay,
            subscriber_overflow_relay_capacity: 2048,
            rpc_overflow_policy: aorta_core::service::RpcOverflowPolicy::Block,
            rpc_overflow_block_timeout: Duration::from_millis(10),
            timer_overflow_policy: aorta_core::timer::TimerOverflowPolicy::Block,
            timer_overflow_block_timeout: Duration::from_millis(12),
        },
        ..ExecutorOptions::default()
    };

    let mut exec = SingleThreadedExecutor::new_with_options(32, options);
    let outcome = exec.spin_once(Duration::from_millis(1)).await;
    assert_eq!(format!("{:?}", outcome), "Timeout");
}

#[tokio::test]
async fn sdk_multi_executor_constructs_and_spins_once() {
    let mut exec = MultiThreadedExecutor::new(32);
    let outcome = exec.spin_once(Duration::from_millis(1)).await;
    assert_eq!(format!("{:?}", outcome), "Timeout");
}

#[test]
fn sdk_multi_executor_supports_callback_group_lifecycle() {
    let exec = MultiThreadedExecutor::new(8);
    let handle = exec.create_callback_group(CallbackGroupType::Reentrant);
    assert!(handle > 0);
    assert!(exec.remove_callback_group(handle));
}

#[test]
fn sdk_executor_exposes_admission_stats() {
    let exec = SingleThreadedExecutor::new(4);
    let sender = exec.queue().sender();
    sender
        .try_send(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(32))
        .expect("enqueue one event");

    let stats = exec.admission_stats();
    assert_eq!(stats.queued_events, 1);
    assert_eq!(stats.queued_bytes, 32);
    // Admission observability is disabled by default to reduce hot-path overhead.
    assert_eq!(stats.peak_queued_events, 0);
    assert_eq!(stats.peak_queued_bytes, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn sdk_single_executor_can_attach_sdk_node_directly() {
    let node = Node::new("sdk_attach_single_exec", "sdk_test")
        .await
        .expect("create node");
    let mut exec = SingleThreadedExecutor::new(8);

    exec.attach_sdk_node(&node);

    assert!(node.inner().has_event_sender());
    node.close().await.expect("close node");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn sdk_multi_executor_can_attach_sdk_node_directly() {
    let node = Node::new("sdk_attach_multi_exec", "sdk_test")
        .await
        .expect("create node");
    let mut exec = MultiThreadedExecutor::new(8);

    exec.attach_sdk_node(&node);

    assert!(node.inner().has_event_sender());
    node.close().await.expect("close node");
}

// Attaching a node that already has a live executor
// must be refused (previously it silently split callbacks across two
// running executors).
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn attach_sdk_node_refuses_node_with_live_executor() {
    let node = Node::new("sdk_attach_refused", "sdk_test")
        .await
        .expect("create node");
    // First attach wins.
    let mut first = SingleThreadedExecutor::new(8);
    first.attach_sdk_node(&node);
    assert!(node.inner().has_event_sender());

    // Infallible variant: refused (ERROR log), node keeps its executor.
    let mut second = SingleThreadedExecutor::new(8);
    second.attach_sdk_node(&node);
    assert!(node.inner().has_event_sender());

    // Fallible variant: surfaces the refusal as InvalidArgument.
    let err = second
        .try_attach_sdk_node(&node)
        .expect_err("re-attach must be refused");
    assert!(
        err.to_string().contains("already has a live executor"),
        "unexpected error: {}",
        err
    );

    node.close().await.expect("close node");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn try_attach_sdk_node_succeeds_on_fresh_node() {
    let node = Node::new("sdk_try_attach_fresh", "sdk_test")
        .await
        .expect("create node");
    let mut exec = MultiThreadedExecutor::new(8);
    exec.try_attach_sdk_node(&node).expect("fresh attach");
    assert!(node.inner().has_event_sender());
    node.close().await.expect("close node");
}
