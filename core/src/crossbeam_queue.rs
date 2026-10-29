//! Lock-free event queue backed by crossbeam-channel + Linux eventfd.
//!
//! The sender side performs a lock-free `try_send()` into a crossbeam bounded
//! channel, then signals the consumer via a raw Linux `eventfd`.  The consumer
//! performs a short spin-poll (`try_recv()`) before falling back to blocking on
//! the eventfd.  This eliminates the tokio task-scheduler overhead that adds
//! ~20-25 µs of latency in the standard tokio mpsc path.
//!
//! # Platform
//!
//! This module is Linux-only (`cfg(target_os = "linux")`).  On other platforms
//! the executor falls back to the tokio mpsc path.

use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel as xbeam;

use crate::event_queue::Event;

// ---------------------------------------------------------------------------
// eventfd helpers (Linux-only)
// ---------------------------------------------------------------------------

/// Thin wrapper around a Linux `eventfd` file descriptor.
#[derive(Debug)]
pub(crate) struct EventFd {
    fd: RawFd,
}

impl EventFd {
    /// Create a new `eventfd` in non-blocking mode (counter-mode, not semaphore).
    ///
    /// In counter mode each `read()` drains the entire accumulated counter
    /// back to zero in a single syscall. We never need per-signal counting
    /// (a single wake is enough to drive the consumer loop, which then
    /// drains the queue), so counter mode avoids a redundant wakeup cycle
    /// when many signals accumulate during a busy consumption burst.
    pub fn new() -> std::io::Result<Self> {
        // SAFETY: `libc::eventfd` is the bare Linux syscall wrapper; both
        // args are scalar ints. Failure is signaled via the return value
        // (`fd < 0`) — checked immediately below.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { fd })
    }

    /// Signal the eventfd (increment counter by 1).
    #[inline]
    pub fn signal(&self) {
        let val: u64 = 1;
        // Best-effort write; EAGAIN is fine (counter is already non-zero).
        // SAFETY: `self.fd` is an owned eventfd descriptor (constructed by
        // `Self::new`, closed only in `Drop`), `&val` is a stack u64 valid
        // for the syscall duration, and the byte count matches the buffer.
        // Return value is intentionally ignored (best-effort write).
        unsafe {
            libc::write(
                self.fd,
                &val as *const u64 as *const libc::c_void,
                std::mem::size_of::<u64>(),
            );
        }
    }

    /// Drain the full accumulated count from the eventfd (non-blocking).
    ///
    /// In counter mode a single `read()` returns the current counter value
    /// and resets it to zero. Returns `true` if any count was consumed,
    /// `false` if the counter was zero (EAGAIN).
    #[inline]
    pub fn try_drain(&self) -> bool {
        let mut val: u64 = 0;
        // SAFETY: `self.fd` is an owned eventfd descriptor (see `new`),
        // `&mut val` is a stack u64 with capacity for the syscall's 8-byte
        // counter read, and the byte count matches the buffer size.
        let rc = unsafe {
            libc::read(
                self.fd,
                &mut val as *mut u64 as *mut libc::c_void,
                std::mem::size_of::<u64>(),
            )
        };
        rc > 0
    }

    /// Block until the eventfd becomes readable (counter > 0), with optional
    /// timeout.  Returns `true` if readable, `false` if timed out.
    pub fn wait(&self, timeout: Option<Duration>) -> bool {
        let mut pfd = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let timeout_ms = match timeout {
            Some(d) => d.as_millis().min(i32::MAX as u128) as i32,
            None => -1, // infinite
        };
        // SAFETY: `&mut pfd` is a stack `pollfd` valid for the syscall
        // duration; the count `1` matches the single-element array (just
        // `pfd`). `timeout_ms` is a scalar int.
        let rc = unsafe { libc::poll(&mut pfd as *mut libc::pollfd, 1, timeout_ms) };
        rc > 0
    }
}

impl AsRawFd for EventFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl Drop for EventFd {
    fn drop(&mut self) {
        // SAFETY: this is the unique owner of `self.fd` (constructed by
        // `Self::new` and never duplicated — `EventFd` is not `Clone`); the
        // descriptor is closed exactly once here on drop.
        unsafe {
            libc::close(self.fd);
        }
    }
}

// SAFETY: The eventfd fd is safe to signal/drain from multiple threads.
unsafe impl Send for EventFd {}
// SAFETY: same justification as `Send` above — the underlying eventfd
// syscalls (write/read/poll) are inherently safe to invoke concurrently
// from multiple threads on the same fd; the kernel serializes them.
unsafe impl Sync for EventFd {}

// ---------------------------------------------------------------------------
// CrossbeamEventfdSender / Receiver
// ---------------------------------------------------------------------------

/// Default number of spin iterations before falling back to eventfd blocking.
const DEFAULT_SPIN_ITERATIONS: u32 = 128;

#[derive(Clone)]
pub(crate) struct CrossbeamEventfdWaker {
    efd: Arc<EventFd>,
}

impl CrossbeamEventfdWaker {
    pub fn wake(&self) {
        self.efd.signal();
    }
}

/// Sender half of the crossbeam+eventfd queue.
#[derive(Clone)]
pub(crate) struct CrossbeamEventfdSender {
    tx: xbeam::Sender<Event>,
    efd: Arc<EventFd>,
    closed: Arc<AtomicBool>,
}

impl std::fmt::Debug for CrossbeamEventfdSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossbeamEventfdSender")
            .field("capacity", &self.tx.capacity().unwrap_or(0))
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish()
    }
}

impl CrossbeamEventfdSender {
    /// Async blocking send. Retries on `Full` until capacity becomes available.
    pub async fn send(&self, mut event: Event) -> Result<(), Event> {
        loop {
            match self.try_send(event) {
                Ok(()) => return Ok(()),
                Err(xbeam::TrySendError::Full(returned)) => {
                    event = returned;
                    tokio::task::yield_now().await;
                }
                Err(xbeam::TrySendError::Disconnected(returned)) => {
                    return Err(returned);
                }
            }
        }
    }

    /// Non-blocking send.  Returns `Err` if the channel is full or closed.
    #[inline]
    pub fn try_send(&self, event: Event) -> Result<(), xbeam::TrySendError<Event>> {
        let result = self.tx.try_send(event);
        if result.is_ok() {
            self.efd.signal();
        }
        result
    }

    /// Check if the channel is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> usize {
        self.tx.capacity().unwrap_or(0)
    }

    pub fn waker(&self) -> CrossbeamEventfdWaker {
        CrossbeamEventfdWaker {
            efd: Arc::clone(&self.efd),
        }
    }
}

impl Drop for CrossbeamEventfdSender {
    fn drop(&mut self) {
        self.efd.signal();
    }
}

/// Receiver half of the crossbeam+eventfd queue.
pub(crate) struct CrossbeamEventfdReceiver {
    rx: xbeam::Receiver<Event>,
    efd: Arc<EventFd>,
    closed: Arc<AtomicBool>,
    spin_iterations: u32,
}

impl CrossbeamEventfdReceiver {
    /// Non-blocking receive.
    #[inline]
    pub fn try_recv(&self) -> Result<Event, xbeam::TryRecvError> {
        match self.rx.try_recv() {
            Ok(event) => Ok(event),
            Err(xbeam::TryRecvError::Empty) => Err(xbeam::TryRecvError::Empty),
            Err(xbeam::TryRecvError::Disconnected) => {
                self.closed.store(true, Ordering::Relaxed);
                Err(xbeam::TryRecvError::Disconnected)
            }
        }
    }

    /// Block until an event is available or the channel is closed.
    ///
    /// Uses a spin-then-block strategy:
    /// 1. Spin `try_recv()` for N iterations (~1-3 µs).
    /// 2. Fall back to `eventfd` blocking via `poll()`.
    pub fn recv(&self) -> Option<Event> {
        // Phase 1: spin-poll
        for _ in 0..self.spin_iterations {
            match self.try_recv() {
                Ok(event) => return Some(event),
                Err(xbeam::TryRecvError::Empty) => {}
                Err(xbeam::TryRecvError::Disconnected) => return None,
            }
            std::hint::spin_loop();
        }

        // Phase 2: eventfd-blocking loop
        loop {
            // Drain accumulated eventfd signals.
            self.efd.try_drain();

            // Check for items that may have arrived.
            match self.try_recv() {
                Ok(event) => return Some(event),
                Err(xbeam::TryRecvError::Empty) => {}
                Err(xbeam::TryRecvError::Disconnected) => return None,
            }

            if self.closed.load(Ordering::Relaxed) {
                return None;
            }

            // Block on eventfd until signaled or timeout.
            // Use a 10ms poll timeout to periodically re-check closed status.
            if self.efd.wait(Some(Duration::from_millis(10))) {
                self.efd.try_drain();
                match self.try_recv() {
                    Ok(event) => return Some(event),
                    Err(xbeam::TryRecvError::Empty) => {}
                    Err(xbeam::TryRecvError::Disconnected) => return None,
                }
            }
        }
    }

    /// Block until an event is available, with a timeout.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<Event> {
        use std::time::Instant;
        let deadline = Instant::now() + timeout;

        // Phase 1: spin-poll (bounded to remaining time, but spin is fast)
        for _ in 0..self.spin_iterations {
            match self.try_recv() {
                Ok(event) => return Some(event),
                Err(xbeam::TryRecvError::Empty) => {}
                Err(xbeam::TryRecvError::Disconnected) => return None,
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::hint::spin_loop();
        }

        // Phase 2: eventfd-blocking loop with deadline
        loop {
            let drained_signal = self.efd.try_drain();

            match self.try_recv() {
                Ok(event) => return Some(event),
                Err(xbeam::TryRecvError::Empty) => {}
                Err(xbeam::TryRecvError::Disconnected) => return None,
            }

            if drained_signal {
                // A signal accumulated BEFORE we started blocking — either an
                // external wake (CrossbeamEventfdWaker) that raced ahead of
                // this call / landed during the spin phase, or a stale signal
                // from an item already consumed. With an empty channel, honor
                // the same external-wake contract as the post-wait branch
                // below: return None NOW so the caller re-runs its
                // top-of-loop state checks (reserved lane, stop flag, timers)
                // instead of silently riding out the full deadline with the
                // wake swallowed.
                return None;
            }

            let now = Instant::now();
            if now >= deadline {
                return None;
            }

            if self.closed.load(Ordering::Relaxed) {
                return None;
            }

            let remaining = deadline - now;
            // Use the full remaining duration instead of a 10ms cap.
            // The closed-status check above handles shutdown, and the eventfd
            // is signaled on close(), so we don't need periodic re-polling.
            if self.efd.wait(Some(remaining)) {
                self.efd.try_drain();
                match self.try_recv() {
                    Ok(event) => return Some(event),
                    // Empty after a wake means the eventfd was signaled
                    // externally (via CrossbeamEventfdWaker::wake), not by an
                    // enqueued event. The executor uses this as its
                    // "re-check state" interrupt — for example, when a timer
                    // is registered/paused or stop is requested. Return None
                    // here so the executor spin loop wakes up and re-runs
                    // its top-of-loop state checks. See
                    // `crossbeam_waker_unblocks_sync_timeout` test for the
                    // contract this preserves.
                    Err(xbeam::TryRecvError::Empty) => return None,
                    Err(xbeam::TryRecvError::Disconnected) => return None,
                }
            }
        }
    }

    /// Mark the channel as closed.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        // Signal the eventfd to wake any blocked recv().
        self.efd.signal();
    }
}

/// Create a bounded crossbeam+eventfd channel pair.
pub(crate) fn channel(
    capacity: usize,
) -> std::io::Result<(CrossbeamEventfdSender, CrossbeamEventfdReceiver)> {
    let (tx, rx) = xbeam::bounded(capacity);
    let efd = Arc::new(EventFd::new()?);
    let closed = Arc::new(AtomicBool::new(false));
    Ok((
        CrossbeamEventfdSender {
            tx,
            efd: Arc::clone(&efd),
            closed: Arc::clone(&closed),
        },
        CrossbeamEventfdReceiver {
            rx,
            efd,
            closed,
            spin_iterations: DEFAULT_SPIN_ITERATIONS,
        },
    ))
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use super::*;

    #[test]
    fn try_recv_marks_disconnected_as_closed() {
        let (tx, rx) = channel(1).expect("create crossbeam eventfd queue");
        drop(tx);

        assert!(matches!(
            rx.try_recv(),
            Err(xbeam::TryRecvError::Disconnected)
        ));
        assert!(rx.closed.load(Ordering::Relaxed));
    }

    #[test]
    fn recv_timeout_honors_a_wake_signal_that_precedes_the_call() {
        // A CrossbeamEventfdWaker::wake() that lands BEFORE recv_timeout
        // starts blocking (e.g. a reserved-lane push racing the call) must
        // not be swallowed by the top-of-loop drain: with an empty channel
        // the call returns None promptly (the external-wake contract), so
        // the caller re-runs its state checks instead of riding out the
        // full deadline.
        let (tx, rx) = channel(1).expect("create crossbeam eventfd queue");
        tx.waker().wake();

        let start = std::time::Instant::now();
        let got = rx.recv_timeout(Duration::from_secs(2));
        assert!(got.is_none(), "no channel item was enqueued");
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "a pre-existing wake signal must interrupt the wait promptly, \
             not be drained-and-ignored (elapsed: {:?})",
            start.elapsed()
        );
    }

    #[test]
    fn recv_timeout_wakes_when_last_sender_is_dropped() {
        let (tx, rx) = channel(1).expect("create crossbeam eventfd queue");
        let (done_tx, done_rx) = std::sync::mpsc::channel();

        let handle = std::thread::spawn(move || {
            let closed = rx.recv_timeout(Duration::from_secs(1)).is_none();
            done_tx.send(closed).expect("send completion");
        });

        std::thread::sleep(Duration::from_millis(20));
        drop(tx);

        let closed = done_rx
            .recv_timeout(Duration::from_millis(200))
            .expect("receiver should wake after sender drop");
        assert!(closed);
        handle.join().expect("join receiver thread");
    }
}
