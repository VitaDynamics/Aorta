use std::sync::Arc;

use aorta_core::event_queue::{CallbackGroupHandle, CallbackGroupType};
use aorta_core::node::Node as CoreNode;
use aorta_core::subscriber::SubscriberOptions;
use aorta_core::timer::TimerOptions;

use crate::service::ServiceOptions;

/// Execution ordering policy for an [`ExecutionGroup`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionPolicy {
    /// Callbacks in the group run one at a time (mutually exclusive
    /// callback group) — no two handlers of this group overlap.
    Serialized,
    /// Callbacks in the group may run concurrently (reentrant callback
    /// group).
    Concurrent,
}

impl ExecutionPolicy {
    pub(crate) fn callback_group_type(self) -> CallbackGroupType {
        match self {
            Self::Serialized => CallbackGroupType::MutuallyExclusive,
            Self::Concurrent => CallbackGroupType::Reentrant,
        }
    }
}

/// A callback domain: bind subscribers, services, and timers to one
/// [`ExecutionPolicy`] so their handlers share ordering guarantees.
///
/// Created via [`Node::create_execution_group`](crate::Node::create_execution_group):
///
/// ```no_run
/// use aorta_sdk::prelude::*;
///
/// # async fn demo(node: &Node) -> AortaResult<()> {
/// let group = node.create_execution_group(ExecutionPolicy::Serialized)?;
/// let sub_options = group.subscriber_options(SubscriberOptions::new());
/// let service_options = group.service_options(ServiceOptions::new());
/// let timer_options = group.timer_options(TimerOptions::new());
/// # Ok(())
/// # }
/// ```
///
/// The group is `Clone`; the underlying callback group handle is released
/// when the **last** clone is dropped. Keep the group alive **at least as
/// long as** the primitives created with its `*_options` helpers: once the
/// handle is gone, their callbacks fall back to the executor-wide default
/// group (with a warning), silently losing the intended isolation.
#[derive(Clone)]
#[must_use = "dropping an ExecutionGroup releases its callback group; bind it for its lifetime"]
pub struct ExecutionGroup {
    inner: Arc<ExecutionGroupInner>,
}

struct ExecutionGroupInner {
    node: CoreNode,
    handle: CallbackGroupHandle,
    policy: ExecutionPolicy,
}

impl Drop for ExecutionGroupInner {
    fn drop(&mut self) {
        let _ = self.node.remove_callback_group(self.handle);
    }
}

impl ExecutionGroup {
    pub(crate) fn new(
        node: CoreNode,
        policy: ExecutionPolicy,
        handle: CallbackGroupHandle,
    ) -> Self {
        Self {
            inner: Arc::new(ExecutionGroupInner {
                node,
                handle,
                policy,
            }),
        }
    }

    /// The raw callback group handle (for advanced options plumbing).
    pub fn handle(&self) -> CallbackGroupHandle {
        self.inner.handle
    }

    /// The policy this group was created with.
    pub fn policy(&self) -> ExecutionPolicy {
        self.inner.policy
    }

    /// Rewrite `options` to route subscriber callbacks through this group.
    pub fn subscriber_options(&self, mut options: SubscriberOptions) -> SubscriberOptions {
        options.callback_group = self.inner.policy.callback_group_type();
        options.callback_group_handle = Some(self.inner.handle);
        options
    }

    /// Rewrite `options` to route timer callbacks through this group.
    pub fn timer_options(&self, mut options: TimerOptions) -> TimerOptions {
        options.callback_group = self.inner.policy.callback_group_type();
        options.callback_group_handle = Some(self.inner.handle);
        options
    }

    /// Rewrite `options` to route service handlers through this group.
    pub fn service_options(&self, mut options: ServiceOptions) -> ServiceOptions {
        options.callback_group = self.inner.policy.callback_group_type();
        options.callback_group_handle = Some(self.inner.handle);
        options
    }
}
