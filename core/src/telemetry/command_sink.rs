use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use tokio::sync::mpsc;

use crate::telemetry::PendingCommand;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandDispatch {
    Enqueued,
    Deferred,
    ShuttingDown,
    ClosedUnexpected,
    WorkerSpawnFailed,
}

pub(crate) struct PendingCommands<T> {
    next_sequence: u64,
    priority_resource_id: Option<&'static str>,
    priority: Option<PendingCommand<T>>,
    pub(crate) resources: BTreeMap<String, PendingCommand<T>>,
}

impl<T> Default for PendingCommands<T> {
    fn default() -> Self {
        Self {
            next_sequence: 0,
            priority_resource_id: None,
            priority: None,
            resources: BTreeMap::new(),
        }
    }
}

impl<T: Clone> PendingCommands<T> {
    pub(crate) fn with_priority_resource(priority_resource_id: &'static str) -> Self {
        Self {
            priority_resource_id: Some(priority_resource_id),
            ..Self::default()
        }
    }

    pub(crate) fn push(&mut self, resource_instance_id: String, command: T) {
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let pending = PendingCommand {
            sequence: self.next_sequence,
            command,
        };

        if self
            .priority_resource_id
            .map(|priority| priority == resource_instance_id)
            .unwrap_or(false)
        {
            self.priority = Some(pending);
            return;
        }

        self.resources.insert(resource_instance_id, pending);
    }

    pub(crate) fn contains_resource(&self, resource_instance_id: &str) -> bool {
        if self
            .priority_resource_id
            .map(|priority| priority == resource_instance_id)
            .unwrap_or(false)
        {
            return self.priority.is_some();
        }
        self.resources.contains_key(resource_instance_id)
    }

    pub(crate) fn peek_next(&self) -> Option<(String, PendingCommand<T>)> {
        if let Some(priority) = self.priority.as_ref() {
            return Some((
                self.priority_resource_id
                    .expect("priority resource id must exist when priority command is queued")
                    .to_string(),
                priority.clone(),
            ));
        }

        let (resource_instance_id, pending) = self.resources.iter().next()?;
        Some((resource_instance_id.clone(), pending.clone()))
    }

    pub(crate) fn acknowledge_sent(&mut self, resource_instance_id: &str, sequence: u64) {
        if self
            .priority_resource_id
            .map(|priority| priority == resource_instance_id)
            .unwrap_or(false)
        {
            let should_clear = self
                .priority
                .as_ref()
                .map(|pending| pending.sequence == sequence)
                .unwrap_or(false);
            if should_clear {
                self.priority = None;
            }
            return;
        }

        let should_remove = self
            .resources
            .get(resource_instance_id)
            .map(|pending| pending.sequence == sequence)
            .unwrap_or(false);
        if should_remove {
            self.resources.remove(resource_instance_id);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.priority.is_none() && self.resources.is_empty()
    }

    pub(crate) fn clear(&mut self) {
        self.priority = None;
        self.resources.clear();
    }

    #[cfg(test)]
    pub(crate) fn has_priority(&self) -> bool {
        self.priority.is_some()
    }
}

pub(crate) struct CommandSink<T> {
    tx: mpsc::Sender<T>,
    pub(crate) pending: Mutex<PendingCommands<T>>,
    pub(crate) flush_worker_scheduled: AtomicBool,
    accepting: AtomicBool,
    resource_instance_id: fn(&T) -> String,
    worker_name: &'static str,
}

impl<T: Clone + Send + 'static> CommandSink<T> {
    pub(crate) fn new(
        tx: mpsc::Sender<T>,
        pending: PendingCommands<T>,
        resource_instance_id: fn(&T) -> String,
        worker_name: &'static str,
    ) -> Arc<Self> {
        Arc::new(Self {
            tx,
            pending: Mutex::new(pending),
            flush_worker_scheduled: AtomicBool::new(false),
            accepting: AtomicBool::new(true),
            resource_instance_id,
            worker_name,
        })
    }

    pub(crate) fn close(&self) {
        self.accepting.store(false, Ordering::Release);
        if let Ok(mut pending) = self.pending.lock() {
            pending.clear();
        }
    }

    pub(crate) fn enqueue(self: &Arc<Self>, command: T) -> CommandDispatch {
        if !self.accepting.load(Ordering::Acquire) {
            return CommandDispatch::ShuttingDown;
        }

        let resource_instance_id = (self.resource_instance_id)(&command);
        if let Ok(mut pending) = self.pending.lock() {
            if pending.contains_resource(&resource_instance_id) {
                pending.push(resource_instance_id, command);
                drop(pending);
                return self.ensure_flush_worker();
            }
        } else {
            return CommandDispatch::ClosedUnexpected;
        }

        match self.tx.try_send(command) {
            Ok(()) => CommandDispatch::Enqueued,
            Err(mpsc::error::TrySendError::Full(command)) => {
                if let Ok(mut pending) = self.pending.lock() {
                    pending.push(resource_instance_id, command);
                } else {
                    return CommandDispatch::ClosedUnexpected;
                }
                self.ensure_flush_worker()
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                if self.accepting.load(Ordering::Acquire) {
                    CommandDispatch::ClosedUnexpected
                } else {
                    CommandDispatch::ShuttingDown
                }
            }
        }
    }

    fn ensure_flush_worker(self: &Arc<Self>) -> CommandDispatch {
        if self
            .flush_worker_scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return CommandDispatch::Deferred;
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let sink = Arc::clone(self);
            handle.spawn(async move {
                sink.flush_async().await;
            });
            return CommandDispatch::Deferred;
        }

        let sink = Arc::clone(self);
        match thread::Builder::new()
            .name(self.worker_name.to_string())
            .spawn(move || sink.flush_blocking())
        {
            Ok(_) => CommandDispatch::Deferred,
            Err(_) => {
                self.flush_worker_scheduled.store(false, Ordering::Release);
                CommandDispatch::WorkerSpawnFailed
            }
        }
    }

    async fn flush_async(self: Arc<Self>) {
        loop {
            if !self.accepting.load(Ordering::Acquire) {
                self.finish_worker();
                return;
            }

            let Some((resource_instance_id, pending_command)) = self.peek_pending() else {
                if self.finish_idle_cycle() {
                    return;
                }
                continue;
            };

            if self.tx.send(pending_command.command).await.is_err() {
                self.finish_worker();
                return;
            }

            self.acknowledge_pending(&resource_instance_id, pending_command.sequence);
        }
    }

    fn flush_blocking(self: Arc<Self>) {
        loop {
            if !self.accepting.load(Ordering::Acquire) {
                self.finish_worker();
                return;
            }

            let Some((resource_instance_id, pending_command)) = self.peek_pending() else {
                if self.finish_idle_cycle() {
                    return;
                }
                continue;
            };

            if self.tx.blocking_send(pending_command.command).is_err() {
                self.finish_worker();
                return;
            }

            self.acknowledge_pending(&resource_instance_id, pending_command.sequence);
        }
    }

    fn peek_pending(&self) -> Option<(String, PendingCommand<T>)> {
        self.pending
            .lock()
            .ok()
            .and_then(|pending| pending.peek_next())
    }

    fn acknowledge_pending(&self, resource_instance_id: &str, sequence: u64) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.acknowledge_sent(resource_instance_id, sequence);
        }
    }

    fn finish_idle_cycle(&self) -> bool {
        self.flush_worker_scheduled.store(false, Ordering::Release);
        let has_pending = self
            .pending
            .lock()
            .map(|pending| !pending.is_empty())
            .unwrap_or(false);
        if !has_pending {
            return true;
        }
        self.flush_worker_scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
    }

    fn finish_worker(&self) {
        self.flush_worker_scheduled.store(false, Ordering::Release);
    }
}
