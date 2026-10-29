"""Aorta middleware — Python 3 SDK.

A pure-Python ctypes binding over the Aorta L2 C-ABI (``libaorta_core``), at
functional parity with the Rust and C++ SDKs. See
``spec/Aorta_Python_SDK_Design_Proposal.md`` for the design contract.

Phase 0 exposes the foundation: the library loader/ABI guard and the error
model. Node / Publisher / Subscriber / Service / Client / Action / Timer /
Executor / ContextStore land in later phases.
"""

from __future__ import annotations

# Make ``aorta`` a path-extending package so generated FlatBuffers modules for
# ``namespace aorta.*`` schemas (e.g. ``aorta.sys.AortaHeader``, ``aorta.topic.*``)
# placed elsewhere on ``sys.path`` co-exist with this SDK package instead of being
# shadowed by it. ``aorta.__path__`` then spans every
# ``aorta/`` directory on ``sys.path``.
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from . import errors
from . import logging
from . import logging as log
from ._ffi.loader import AbiGuardError, AortaLoadError, library, load_library
from .action import (
    ActionClient,
    ActionCodec,
    ActionExecutionContext,
    ActionFeedbackSubscription,
    ActionInfo,
    ActionInfoServer,
    ActionReplyCode,
    ActionServer,
    ActionServerOptions,
    ActionStatus,
    CancelResponse,
    FeedbackSample,
    FlatbuffersActionCodec,
    GetResultResponse,
    GoalHandle,
    PublicStatus,
    RetainedActionGoal,
    SendGoalResponse,
)
from .action import ActionClientOptions as ActionClientOptions
from .capture import (
    CaptureKind,
    CapturedSample,
    CaptureSelectorKind,
    CaptureSubscriber,
    ResolvedSchema,
    SchemaResolver,
)
from .client import Client, TypedClient, TypedClientResponse
from .context import MessageContext
from .executiongroup import ExecutionGroup
from .contextstore import (
    ContextChange,
    ContextChangeKind,
    ContextKind,
    ContextRead,
    ContextStore,
    OwnerLivelinessEvent,
    OwnerLivelinessWatchHandle,
    SnapshotDoneReason,
    WatchHandle,
    WatchSnapshotOptions,
)
from .executor import (
    Executor,
    ExecutorAdmissionStats,
    ExecutorType,
    StopHandle,
)
from .node import Node
from .options import (
    CallbackGroup,
    ClientOptions,
    ClientStats,
    DeliveryMode,
    ExecutionPolicy,
    PublisherStats,
    ReceiveOverflow,
    RpcOverflowPolicy,
    ServiceOptions,
    ServiceStats,
    SubscriberOptions,
    SubscriberOverflowPolicy,
    SubscriberRuntimeStats,
    TimerOptions,
    TimerOverflowPolicy,
    TimerStats,
)
from .publisher import Publisher
from .pull_subscriber import PullSample, PullSubscriber
from .qos import QoS, CongestionControl, Priority, Reliability
from .schema import SchemaAttachPolicy, SchemaMeta
from .service import Service, ServiceRequest, TypedServiceResponder
from .subscriber import Subscriber
from .timer import Timer
from .uuid_v7 import new_uuid_v7
from .errors import (
    AlreadyRepliedError,
    AortaError,
    BusyError,
    ConcurrentConflictError,
    DeclarationNotReadyError,
    InstanceIdCollisionError,
    InternalError,
    InvalidArgumentError,
    KindMismatchError,
    NotFoundError,
    PrefixNotDeclaredError,
    ReentrantError,
    ResultCode,
    SchemaConflictError,
    TimeoutError,
)

__version__ = "0.0.0"

__all__ = [
    "__version__",
    "errors",
    "logging",
    "log",
    "Node",
    "Publisher",
    "Subscriber",
    "PullSubscriber",
    "PullSample",
    "CaptureSubscriber",
    "SchemaResolver",
    "CapturedSample",
    "ResolvedSchema",
    "CaptureSelectorKind",
    "CaptureKind",
    "QoS",
    "Priority",
    "Reliability",
    "CongestionControl",
    "Service",
    "ServiceRequest",
    "TypedServiceResponder",
    "Client",
    "TypedClient",
    "TypedClientResponse",
    "ServiceOptions",
    "ClientOptions",
    "ServiceStats",
    "ClientStats",
    "PublisherStats",
    "SubscriberOptions",
    "SubscriberOverflowPolicy",
    "SubscriberRuntimeStats",
    "DeliveryMode",
    "ReceiveOverflow",
    "CallbackGroup",
    "RpcOverflowPolicy",
    "Timer",
    "new_uuid_v7",
    "TimerOptions",
    "TimerStats",
    "TimerOverflowPolicy",
    "ContextStore",
    "ContextRead",
    "ContextChange",
    "ContextKind",
    "ContextChangeKind",
    "SnapshotDoneReason",
    "WatchSnapshotOptions",
    "WatchHandle",
    "OwnerLivelinessEvent",
    "OwnerLivelinessWatchHandle",
    "ExecutionGroup",
    "ExecutionPolicy",
    "Executor",
    "ExecutorType",
    "ExecutorAdmissionStats",
    "StopHandle",
    "ActionClient",
    "ActionServer",
    "ActionInfoServer",
    "ActionCodec",
    "FlatbuffersActionCodec",
    "ActionExecutionContext",
    "ActionFeedbackSubscription",
    "GoalHandle",
    "RetainedActionGoal",
    "ActionStatus",
    "ActionReplyCode",
    "ActionClientOptions",
    "ActionServerOptions",
    "SendGoalResponse",
    "CancelResponse",
    "GetResultResponse",
    "PublicStatus",
    "FeedbackSample",
    "ActionInfo",
    "MessageContext",
    "SchemaAttachPolicy",
    "SchemaMeta",
    "load_library",
    "library",
    "AortaLoadError",
    "AbiGuardError",
    "AortaError",
    "ResultCode",
    "InvalidArgumentError",
    "NotFoundError",
    "TimeoutError",
    "BusyError",
    "InternalError",
    "SchemaConflictError",
    "ConcurrentConflictError",
    "AlreadyRepliedError",
    "PrefixNotDeclaredError",
    "DeclarationNotReadyError",
    "KindMismatchError",
    "InstanceIdCollisionError",
    "ReentrantError",
]
