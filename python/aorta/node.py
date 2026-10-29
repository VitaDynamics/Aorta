"""Node — the entry point: owns the Zenoh session and creates pub/sub/etc.

Exposes the name/id/group/now accessors plus the raw and typed pub/sub, RPC,
timer, action and ContextStore factories. Deployment/router config is via
environment variables (parity with the Rust
SDK, which hides zenoh::Config) — Python SDK design proposal §1.2.
"""

from __future__ import annotations

import ctypes
import functools
import threading
from typing import Any, Callable

from . import errors
from . import logging as _log
from . import service as _service
from . import subscriber as _subscriber
from ._ffi import bindings, loader, structs
from .action import ActionClient, ActionInfoServer, ActionServer
from .client import Client, TypedClient
from .contextstore import ContextStore
from .executiongroup import ExecutionGroup
from .executor import Executor, ExecutorType
from .options import (
    ClientOptions,
    ExecutionPolicy,
    ServiceOptions,
    SubscriberOptions,
    TimerOptions,
    inject_client_schema,
    inject_service_schema,
)
from .schema import SchemaMeta
from .publisher import Publisher
from .qos import QoS
from .schema import SchemaAttachPolicy
from .service import Service, ServiceHandler
from .subscriber import MessageHandler, Subscriber
from .timer import Period, Timer, TimerCallback

# root_decoder(payload: bytes) -> decoded root; handler(decoded, ctx)
RootDecoder = Callable[[bytes], Any]
TypedHandler = Callable[[Any, "object"], None]

# Telemetry identity the Python SDK stamps on every node so Core does not
# default it to language="c". sdk_version tracks aorta.__version__.
_SDK_LANGUAGE = b"python"
_SDK_NAME = b"aorta_python_sdk"
_SDK_VERSION = b"0.0.0"


class Node:
    """An Aorta node. Use as a context manager or call ``close()`` explicitly."""

    def __init__(self, name: str, group: str = "default") -> None:
        self._lib = bindings.configure(loader.library())
        self._name = name
        self._closed = False
        self._children = []  # Publisher/Subscriber/Service/Client/Timer
        # Lazy auto-executor (created on first create_timer); see ensure_executor.
        # _exec_lock guards create/teardown so concurrent factory calls cannot
        # each spin up an executor (parity with the C++/Rust executor mutex).
        self._executor = None
        self._stop_handle = None
        self._spin_thread = None
        self._exec_lock = threading.Lock()
        handle = bindings.aorta_node_t(0)
        # Stamp the Python SDK telemetry identity (language="python") so Core
        # does not record the node as language="c". The structs stay alive on
        # the stack for the duration of this synchronous call.
        telemetry = structs.AortaNodeTelemetryOptions()
        telemetry.language = _SDK_LANGUAGE
        telemetry.sdk_name = _SDK_NAME
        telemetry.sdk_version = _SDK_VERSION
        options = structs.AortaNodeOptions()
        options.telemetry = ctypes.cast(ctypes.byref(telemetry), ctypes.c_void_p)
        # NB: Core IGNORES the `group` argument (the effective group comes from
        # AORTA_GROUP/default); we still pass it for signature parity and expose
        # the resolved value via group(). See aorta_node_get_group.
        rc = self._lib.aorta_node_new_with_options(
            name.encode("utf-8"),
            group.encode("utf-8"),
            ctypes.byref(options),
            ctypes.byref(handle),
        )
        errors.check_rc(self._lib, rc, "aorta_node_new_with_options")
        self._handle = handle.value

    def name(self) -> str:
        return self._name

    def _read_out_string(self, fn, op: str) -> str:
        self._ensure_open()
        buf = ctypes.create_string_buffer(128)
        out_len = ctypes.c_size_t(0)
        rc = fn(self._handle, buf, len(buf), ctypes.byref(out_len))
        errors.check_rc(self._lib, rc, op)
        return buf.raw[: out_len.value].decode("utf-8", "replace")

    def id(self) -> str:
        """The node's unique id (UUID string)."""
        return self._read_out_string(self._lib.aorta_node_get_id, "aorta_node_get_id")

    def group(self) -> str:
        """The node's **effective** group (resolved from ``AORTA_GROUP``/default).

        Core ignores the constructor ``group`` argument, so this returns the
        real group the node's keys live under — matching Rust/C++.
        """
        return self._read_out_string(
            self._lib.aorta_node_get_group, "aorta_node_get_group"
        )

    def now(self) -> int:
        """Current node time in nanoseconds (System or Simulation time source)."""
        self._ensure_open()
        out = ctypes.c_int64(0)
        rc = self._lib.aorta_node_now(self._handle, ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_node_now")
        return int(out.value)

    def create_publisher(self, topic: str, *, qos: "QoS" = None) -> Publisher:
        """Create a raw byte publisher on ``topic``. ``qos`` (optional) sets
        priority/reliability/congestion_control/express; default = core defaults."""
        self._ensure_open()
        pub = Publisher(self, topic, qos=qos)
        self._children.append(pub)
        return pub

    def create_publisher_typed(
        self,
        schema_meta,
        topic: str,
        *,
        qos: "QoS" = None,
        schema_attach_policy: SchemaAttachPolicy = SchemaAttachPolicy.INHERIT,
    ) -> Publisher:
        """Create a publisher that attaches ``schema_meta.SCHEMA_BFBS`` so peers
        can discover/validate the message schema (its 64-hex hash rides with the
        message per the attach policy). Use ``publish_typed`` on the result."""
        self._ensure_open()
        pub = Publisher(
            self,
            topic,
            qos=qos,
            schema_bfbs=schema_meta.SCHEMA_BFBS,
            # ROOT_TYPE_NAME is optional on duck-typed metas (generated schema
            # modules always carry it); absent keeps the legacy no-root-type
            # registration, matching the optional wire field.
            schema_root_type=getattr(schema_meta, "ROOT_TYPE_NAME", None),
            schema_attach_policy=schema_attach_policy,
        )
        self._children.append(pub)
        return pub

    def create_subscriber(
        self,
        topic: str,
        handler: MessageHandler,
        *,
        qos: "QoS" = None,
        options: "SubscriberOptions" = None,
    ) -> Subscriber:
        """Create a raw byte subscriber; ``handler(payload, ctx)`` runs on a
        worker thread (exceptions are logged and swallowed). ``qos`` sets
        priority/reliability/congestion_control/express; ``options`` tunes
        receive_depth / callback group / dedup / reorder / MessageContext fields."""
        self._ensure_open()
        sub = Subscriber(self, topic, handler, qos=qos, options=options)
        self._children.append(sub)
        return sub

    def create_subscriber_pull(
        self,
        topic: str,
        depth: int,
        *,
        qos: "QoS" = None,
        options: "SubscriberOptions" = None,
    ) -> "PullSubscriber":
        """Create a pull-mode (Tier B-pull) subscriber: a Zenoh-native keep-last
        ``depth`` ring with no callback. Drain it via ``recv``/``try_recv`` from
        your own thread — the latest-wins sensor idiom."""
        from .pull_subscriber import PullSubscriber

        self._ensure_open()
        sub = PullSubscriber(self, topic, depth, qos=qos, options=options)
        self._children.append(sub)
        return sub

    def create_subscriber_typed(
        self,
        root_decoder: RootDecoder,
        topic: str,
        handler: TypedHandler,
        *,
        qos: "QoS" = None,
        options: "SubscriberOptions" = None,
    ) -> Subscriber:
        """Typed-view subscriber: decode each payload with ``root_decoder`` and
        deliver ``handler(decoded, ctx)``. A decode failure is logged and the
        message skipped (parity with the C++ typed-view auto-error path)."""
        self._ensure_open()
        adapter = functools.partial(
            _subscriber.typed_decode_dispatch, root_decoder, handler
        )
        return self.create_subscriber(topic, adapter, qos=qos, options=options)

    # ── RPC ──────────────────────────────────────────────────────────────────
    def create_service(
        self,
        service_name: str,
        handler: ServiceHandler,
        *,
        options: ServiceOptions = None,
    ) -> Service:
        """Create a raw RPC service. ``handler(request_bytes, req)`` runs on a
        worker thread; reply via ``req.reply(bytes)`` / ``req.reply_error(msg)``,
        or drop the request with ``req.release()``. If the handler raises, the
        request is released so the caller's ``call`` fails fast."""
        self._ensure_open()
        svc = _service.Service(self, service_name, handler, options=options)
        self._children.append(svc)
        return svc

    def create_service_typed_view(
        self,
        root_decoder: RootDecoder,
        service_name: str,
        handler: Callable[[Any, "object"], None],
        *,
        request_schema_meta: SchemaMeta = None,
        response_schema_meta: SchemaMeta = None,
        options: ServiceOptions = None,
    ) -> Service:
        """Typed-view service: decode each request with ``root_decoder`` and call
        ``handler(decoded, responder)`` (a :class:`~aorta.service.TypedServiceResponder`).
        A decode failure sends an error reply (parity with the C++ typed-view
        auto-error path).

        Pass ``request_schema_meta`` (and optionally ``response_schema_meta``) — a
        generated ``*_schema_meta`` module — to **auto-inject** the schema
        BFBS/hash + request/response root-type names so cross-language peers can
        discover/validate the schema (parity with the Rust/C++ typed service)."""
        self._ensure_open()
        options = inject_service_schema(options, request_schema_meta, response_schema_meta)
        stamper = _service.make_header_stamper(self._lib, self._name)
        raw_handler = functools.partial(
            _service.typed_service_decode_dispatch, root_decoder, handler, stamper
        )
        svc = _service.Service(self, service_name, raw_handler, options=options)
        self._children.append(svc)
        return svc

    def create_client(
        self, service_name: str, *, options: ClientOptions = None
    ) -> Client:
        """Create a synchronous RPC client (``allow_retry`` OFF by default)."""
        self._ensure_open()
        client = Client(self, service_name, options=options)
        self._children.append(client)
        return client

    def create_client_with_timeout(
        self, service_name: str, timeout_ms: int
    ) -> Client:
        """Create a client with only the call timeout set (C++ ``CreateClient(name, ms)``)."""
        return self.create_client(
            service_name, options=ClientOptions(timeout_ms=timeout_ms)
        )

    def create_client_typed(
        self,
        service_name: str,
        response_decoder: RootDecoder,
        *,
        request_schema_meta: SchemaMeta = None,
        options: ClientOptions = None,
    ) -> TypedClient:
        """Create a typed RPC client; ``response_decoder(bytes)`` decodes replies.
        ``TypedClient.call(fill)`` injects an AortaHeader into the request.

        Pass ``request_schema_meta`` (a generated ``*_schema_meta`` module) to
        auto-inject ``service_schema_hash_hex`` so the client matches the
        provider's schema (parity with the Rust/C++ typed client)."""
        options = inject_client_schema(options, request_schema_meta)
        client = self.create_client(service_name, options=options)
        return TypedClient(self, client, response_decoder)

    def ensure_executor(self) -> None:
        """Lazily create + spin a single-threaded executor on a background thread.

        Timers need a running executor with an event queue (the C-ABI does not
        auto-create one — that convenience lives in the Rust/C++ SDK wrappers).
        Idempotent: the first call creates it; :meth:`close` tears it down.
        ``aorta_executor_spin`` blocks, and ctypes releases the GIL for its
        duration, so worker callbacks can re-acquire the GIL and run."""
        if self._executor is not None:
            return
        with self._exec_lock:
            if self._executor is not None:  # re-check under the lock (DCL)
                return
            # Parity with C++ Node::EnsureExecutor (cpp/src/node.cc): if the node
            # already has an event sender — e.g. the user attached it to an
            # explicit Executor — do NOT spin a second auto-executor. The explicit
            # one owns dispatch; two event loops pulling the same node would race.
            if self._has_event_sender():
                return
            # Reuse the explicit Executor wrapper (DRY) for create/add_node/
            # get_stop_handle; the Node owns the spin thread + the special
            # leak-on-timeout teardown the standalone class does not need.
            ex = Executor(ExecutorType.SINGLE_THREADED)
            try:
                ex.add_node(self)
                stop = ex.get_stop_handle()
            except errors.AortaError:  # pragma: no cover - cannot fail for a fresh node
                ex.close()
                raise
            # Spin the raw C fn (rc ignored): a benign stop returns OK and a real
            # error just ends the daemon thread — no exception to surface.
            spin = threading.Thread(
                target=self._lib.aorta_executor_spin,
                args=(ex.handle(),),
                daemon=True,
            )
            spin.start()
            # Set state last so `_executor is not None` always implies the stop
            # handle + spin thread are present (teardown keys off `_executor`).
            self._stop_handle = stop
            self._spin_thread = spin
            self._executor = ex

    def _teardown_executor(self) -> None:
        with self._exec_lock:
            if self._executor is None:
                return
            # Stop the spin loop and join. `Executor.close` re-acquires the spin
            # lock to free, so if the thread is still alive (a hung callback)
            # freeing would block — leak the executor instead (parity with the
            # C++ destructor's wait+leak-on-timeout).
            # Best-effort: teardown runs on the close() path, so a non-OK rc must
            # NOT escape (it would skip aorta_node_free below). A valid handle
            # won't error, but StopHandle.stop() runs check_rc, so guard it.
            try:
                self._stop_handle.stop()
            except errors.AortaError:  # pragma: no cover - a live handle won't error
                pass
            self._spin_thread.join(timeout=2.0)
            # Freeing the stop handle only removes a registry entry (no spin
            # lock), so it is always safe — do it on both paths.
            self._stop_handle.close()
            if self._spin_thread.is_alive():  # pragma: no cover - needs a hung cb
                _log.error(
                    "executor spin thread did not exit within 2s; leaking the "
                    "executor handle to avoid blocking close()",
                    scope="aorta_sdk",
                )
            else:
                self._executor.close()  # stop()+free(); spin has already returned
            self._executor = None
            self._stop_handle = None
            self._spin_thread = None

    # ── Timer ────────────────────────────────────────────────────────────────
    def create_timer(
        self,
        period: Period,
        callback: TimerCallback,
        *,
        options: "TimerOptions" = None,
    ) -> Timer:
        """Create a periodic timer; ``callback()`` runs on a worker thread.

        ``period`` is seconds (float/int) or a :class:`datetime.timedelta`.
        ``auto_start`` (TimerOptions, default True) fires it immediately."""
        self._ensure_open()
        self.ensure_executor()  # timers need a running executor event queue
        t = Timer(self, period, callback, options=options)
        self._children.append(t)
        return t

    # ── ContextStore ─────────────────────────────────────────────────────────
    def create_context_store(self) -> ContextStore:
        """Create a :class:`~aorta.contextstore.ContextStore` (mints a fresh
        source instance id). Declare a prefix before setting keys under it."""
        self._ensure_open()
        cs = ContextStore(self)
        self._children.append(cs)
        return cs

    # ── ExecutionGroup ───────────────────────────────────────────────────────
    def create_execution_group(
        self, policy: ExecutionPolicy = ExecutionPolicy.SERIALIZED
    ) -> ExecutionGroup:
        """Create an :class:`~aorta.executiongroup.ExecutionGroup` (a shared
        callback group). Stamp it onto timer/service options to serialize or
        parallelize their callbacks."""
        self._ensure_open()
        self.ensure_executor()  # callback groups live in the executor event queue
        eg = ExecutionGroup(self, policy)
        self._children.append(eg)
        return eg

    # ── Action ───────────────────────────────────────────────────────────────
    def create_action_client(
        self, action_name: str, codec, *, options=None
    ) -> ActionClient:
        """Create an action client. ``codec`` is an
        :class:`~aorta.action.ActionCodec` for this action's wrapper schema."""
        self._ensure_open()
        client = ActionClient(self, action_name, codec, options=options)
        self._children.append(client)
        return client

    def create_action_server(
        self, action_name: str, schema_bfbs, codec, handler, *, options=None, goal_decoder=None
    ) -> ActionServer:
        """Create an action server. ``handler(goal, ctx)`` runs on a worker
        thread; drive the goal terminal with ``ctx.succeed/abort/cancel`` (a
        handler exception auto-aborts with ``HANDLER_ERROR``)."""
        self._ensure_open()
        server = ActionServer(
            self, action_name, schema_bfbs, codec, handler,
            options=options, goal_decoder=goal_decoder,
        )
        self._children.append(server)
        return server

    def create_action_info_server(
        self, action_name: str, schema_bfbs, *, options=None
    ) -> ActionInfoServer:
        """Create an **info-only** action server (no execute handler): it answers
        the action's discovery query so clients' ``info()`` resolves, but accepts
        no goals. Mirror of C++ ``ActionServer::ServeInfoOnly`` / Rust
        ``serve_info_only``."""
        self._ensure_open()
        server = ActionInfoServer(self, action_name, schema_bfbs, options=options)
        self._children.append(server)
        return server

    def close(self) -> None:
        """Close children (draining callbacks), then free the node. Idempotent."""
        if self._closed:
            return
        self._closed = True
        for child in reversed(self._children):
            try:
                child.close()
            except errors.AortaError:  # pragma: no cover - best-effort teardown
                pass
        self._children.clear()
        # try/finally so a freed node is never leaked even if teardown raises
        # unexpectedly (teardown is itself best-effort; this is belt-and-braces).
        try:
            self._teardown_executor()  # stop+join the spin thread before freeing node
        finally:
            self._lib.aorta_node_free(self._handle)

    def _has_event_sender(self) -> bool:
        """Whether the node is already attached to an executor (auto or explicit)."""
        out = ctypes.c_uint8(0)
        rc = self._lib.aorta_node_has_event_sender(self._handle, ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_node_has_event_sender")
        return bool(out.value)

    def _ensure_open(self) -> None:
        if self._closed:
            raise errors.AortaError("operation on a closed Node")

    def __enter__(self) -> "Node":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
