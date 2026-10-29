#pragma once

/*
 * REENTRANCY CONTRACT
 * ===================
 * Aorta FFI entry points are NOT reentrant from inside Aorta callbacks.
 * A blocking call made from a callback context returns
 * AORTA_ERR_REENTRANT instead of panicking (Tokio's "Cannot start a
 * runtime from within a runtime" panic would otherwise abort the process
 * across the extern "C" boundary under Rust >= 1.81).
 *
 * Whether a call is refused is DYNAMIC — it depends on the thread the
 * callback runs on, not on the API being called:
 *
 *   Substrate x entry legality
 *   - Executor dispatch thread / Flow lane worker: reentrancy flags
 *     SET -> blocking entries return AORTA_ERR_REENTRANT. Pure-C++ waits
 *     (GoalHandle::WaitResult, FeedbackStream::Recv, scope shutdown,
 *     destructor teardown) have NO core backstop — they self-deadlock or
 *     stall, and the detector below is the only signal.
 *   - Raw action `execute` handler (a dedicated thread per goal) and
 *     action `cancel_requested` callback (Flow offload worker): flags
 *     CLEAR -> blocking FFI calls are LEGAL; core documents that raw
 *     execute handlers MAY block until their goal is terminal.
 *   - Action feedback2 delivery: node BOUND to an executor -> reserved
 *     lane, flags SET (blocking refused); UNBOUND -> dedicated plain
 *     thread, flags CLEAR (blocking legal).
 *   - Foreign runtime worker (application's own Tokio): flags CLEAR;
 *     rejected only via the panic-substring classifier on ffi_block_on!
 *     entries. Explicit-only guards (aorta_executor_detach_lane_pool)
 *     have NO foreign-runtime coverage — documented gap.
 *   - Plain user thread / spawn_blocking: flags CLEAR, blocking legal.
 *
 * The exact detector is exported for C++ call-site checks:
 *
 *   uint8_t aorta_reentrant_context(void);
 *
 * Returns 1 iff a blocking Aorta operation on the current thread right
 * now would be refused as reentrant by the explicit detector (thread is
 * inside a dispatched Aorta callback or nested inside an aorta-owned
 * blocking entry). Exact, allocation-free, never initializes the
 * runtime. Returns 0 on plain threads, spawn_blocking threads, Flow
 * offload workers, unbound feedback2 delivery threads, and
 * foreign-runtime workers.
 *
 * AORTA_REENTRANT_ASSERT (opt-in diagnostics; default OFF):
 *   0 or unset -> off; 1 -> warn once per call site; 2 -> abort.
 *   The mode is parsed once at the first evaluation of any
 *   reentrancy diagnostic and is immutable thereafter — a mode
 *   change requires a process restart.
 *
 * To create or query resources in response to a callback, either:
 *   (a) post the request to a queue and process it from the main
 *       thread, OR
 *   (b) construct the resources before entering the callback path and
 *       reuse them, OR
 *   (c) for handles you already own, call non-reentrant helpers like
 *       aorta_publisher_publish / aorta_stop_handle_stop, OR
 *   (d) use the C++20 Flow layer — co_await instead of blocking where
 *       an awaiter exists.
 *
 * The following entry points ARE safe to call from inside a callback:
 *   - aorta_publisher_publish, aorta_publisher_publish_owned
 *     (fire-and-forget hot path)
 *   - aorta_owned_bytes_free, aorta_sample_release,
 *     aorta_owned_string_free
 *   - aorta_stop_handle_stop (signaling only, no async work)
 *   - class-F teardown (spawn-and-detach; returns AORTA_OK immediately
 *     from a callback; deterministic teardown is only guaranteed when
 *     called from outside the runtime):
 *     aorta_node_free / aorta_publisher_free / aorta_subscriber_free /
 *     aorta_ctx_free / aorta_action_feedback_sub_free /
 *     aorta_service_free / aorta_capture_subscriber_free /
 *     aorta_action_server_free.
 *     Exception — BUSY variant: aorta_action_result_sub_free returns
 *     AORTA_ERR_BUSY (detached) from its own delivery or any async
 *     context.
 *
 * Teardown entries that do NOT follow class F:
 *   - class T (REENTRANT-refused from callbacks):
 *     aorta_executor_free, aorta_executor_detach_lane_pool,
 *     aorta_ctx_watch_free.
 *   - class W (blocking teardown with NO core refusal):
 *     aorta_timer_free unconditionally drains synchronously (up to
 *     2 s); aorta_action_feedback_sub2_free as a whole — lane arm
 *     returns BUSY from its own delivery or any async context; thread
 *     arm returns BUSY only from its own drain thread and joins that
 *     drain thread synchronously (un-timeboxed) from any other
 *     callback context.
 */

// IWYU pragma: begin_exports
#include "aorta/action.h"
#include "aorta/capture.h"
#include "aorta/client.h"
#include "aorta/convenience.h"
#include "aorta/executor.h"
#include "aorta/flatbuffers.h"
#include "aorta/header.h"
#include "aorta/logging.h"
#include "aorta/node.h"
#include "aorta/options.h"
#include "aorta/publisher.h"
#include "aorta/pull_subscriber.h"
#include "aorta/qos.h"
#include "aorta/schema_meta.h"
#include "aorta/service.h"
#include "aorta/status.h"
#include "aorta/subscriber.h"
#include "aorta/time.h"
#include "aorta/timer.h"
#include "aorta/typed_subscribe.h"
#include "aorta/typed_service.h"
#include "aorta/types.h"
#include "aorta/uuid.h"
// IWYU pragma: end_exports
