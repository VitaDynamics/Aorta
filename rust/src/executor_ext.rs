use aorta_core::error::{AortaError, AortaResult};
use aorta_core::executor::{MultiThreadedExecutor, SingleThreadedExecutor, StopHandle};
use std::thread::{self, JoinHandle};

use crate::Node;

/// SDK-facing helpers for attaching a Rust SDK `Node` to an external executor.
///
/// The core executors operate on `aorta_core::node::Node`. This extension trait
/// keeps the common SDK path explicit so application code does not need to
/// reach through `node.inner().clone()`.
pub trait ExecutorNodeAttachExt {
    /// Attach an SDK `Node` to this executor, logging a refusal instead of
    /// returning it — see [`Self::try_attach_sdk_node`].
    fn attach_sdk_node(&mut self, node: &Node) {
        if let Err(err) = self.try_attach_sdk_node(node) {
            crate::aorta_error!("Aorta ERROR: attach_sdk_node refused: {}", err);
        }
    }

    /// Attach an SDK `Node` to this executor.
    ///
    /// After attachment, callback-capable primitives created from `node`
    /// (`Subscriber`, `Service`, `Timer`, `ExecutionGroup`) will reuse this
    /// executor instead of spawning the SDK-managed single-threaded fallback.
    ///
    /// The FIRST executor attached to a node OWNS it: a node that already has a
    /// live executor — because a callback-capable primitive was created first
    /// and started the auto-executor, or because of a previous attach — is
    /// REFUSED with `InvalidArgument` and left untouched. Re-attaching would
    /// split that node's callbacks across two running executors, which used to
    /// happen silently.
    ///
    /// Attach before the first callback-capable primitive, and prefer
    /// [`NodeOptions::with_auto_executor(false)`](crate::NodeOptions::with_auto_executor)
    /// so the ordering requirement is enforced rather than remembered.
    fn try_attach_sdk_node(&mut self, node: &Node) -> AortaResult<()>;
}

impl ExecutorNodeAttachExt for SingleThreadedExecutor {
    fn try_attach_sdk_node(&mut self, node: &Node) -> AortaResult<()> {
        self.try_add_node(node.inner().clone())
    }
}

impl ExecutorNodeAttachExt for MultiThreadedExecutor {
    fn try_attach_sdk_node(&mut self, node: &Node) -> AortaResult<()> {
        self.try_add_node(node.inner().clone())
    }
}

/// A [`SingleThreadedExecutor`] spinning on its own OS thread.
///
/// Returned by [`spawn_executor_thread`]. Dropping it requests the spin loop
/// to stop and joins the thread.
pub struct ExecutorThread {
    stop: StopHandle,
    join: Option<JoinHandle<()>>,
    name: String,
}

impl ExecutorThread {
    /// Ask the spin loop to stop without waiting for it.
    pub fn request_stop(&self) {
        self.stop.stop();
    }

    /// Stop the spin loop and join the thread. Idempotent.
    ///
    /// Joining is SKIPPED when called from the spin thread itself (possible
    /// when the handle is dropped inside a callback the executor is running):
    /// a self-join deadlocks, and the stop signal alone is enough there.
    pub fn stop(&mut self) {
        self.stop.stop();
        if let Some(join) = self.join.take() {
            if thread::current().id() == join.thread().id() {
                return;
            }
            if join.join().is_err() {
                crate::aorta_error!(
                    "Aorta ERROR: executor thread '{}' panicked during spin",
                    self.name
                );
            }
        }
    }

    /// A handle that can stop this executor from another thread.
    pub fn stop_handle(&self) -> StopHandle {
        self.stop.clone()
    }
}

impl Drop for ExecutorThread {
    fn drop(&mut self) {
        self.stop();
    }
}

impl std::fmt::Debug for ExecutorThread {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorThread")
            .field("name", &self.name)
            .field("running", &self.join.is_some())
            .finish()
    }
}

/// Run `executor` to completion on a DEDICATED OS thread with its own
/// `current_thread` tokio runtime.
///
/// This is the runtime placement the SDK auto-executor uses, exposed so an
/// application that supplies its own executor can reproduce it. Awaiting
/// `executor.spin()` on a shared multi-threaded runtime instead puts the
/// dispatch loop on the same workers as Zenoh's RX tasks; the 64 B @ 1 kHz
/// measurement put that at roughly +96 µs of p50 receive latency
/// and 5× subscriber-thread CPU.
///
/// `executor` must have been built with `ExecutorOptions::force_tokio_queue`,
/// because a `current_thread` runtime cannot serve the crossbeam+eventfd
/// queue's `block_in_place` park; otherwise this returns `InvalidArgument`.
///
/// ```no_run
/// use aorta_sdk::prelude::*;
/// use aorta_sdk::{spawn_executor_thread, ExecutorNodeAttachExt, ExecutorOptions,
///                 SingleThreadedExecutor};
///
/// # async fn demo(node: &Node) -> AortaResult<()> {
/// let mut executor = SingleThreadedExecutor::new_with_options(
///     8192,
///     ExecutorOptions {
///         force_tokio_queue: true,
///         ..ExecutorOptions::default()
///     },
/// );
/// executor.try_attach_sdk_node(node)?;
/// let spin = spawn_executor_thread("app-exec", executor)?;
/// # let _ = spin;
/// # Ok(())
/// # }
/// ```
pub fn spawn_executor_thread(
    name: &str,
    mut executor: SingleThreadedExecutor,
) -> AortaResult<ExecutorThread> {
    if !executor.forces_tokio_queue() {
        return Err(AortaError::invalid_argument(format!(
            "executor for thread '{}' must be built with \
             ExecutorOptions::force_tokio_queue = true: a current_thread tokio \
             runtime cannot serve the crossbeam queue's block_in_place park",
            name
        )));
    }
    let stop = executor.stop_handle();
    let join = thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    crate::aorta_error!(
                        "Aorta ERROR: executor thread runtime build failed: {}",
                        err
                    );
                    return;
                }
            };
            runtime.block_on(async move {
                executor.spin().await;
            });
        })
        .map_err(|err| {
            AortaError::internal(format!("spawn executor thread '{}' failed: {}", name, err))
        })?;
    Ok(ExecutorThread {
        stop,
        join: Some(join),
        name: name.to_string(),
    })
}
