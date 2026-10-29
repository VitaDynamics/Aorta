use std::str;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use crate::action::{
    ActionCancelGoalResponse, ActionGetResultResponse, ActionInfo, ActionPublicStatus,
    ActionReplyCode, ActionSchema, ActionSendGoalResponse, ActionStatus, ActionWire,
};
use crate::action_client::{ActionClient, ActionClientOptions, GoalHandle};
use crate::action_server::{
    fetch_action_info, ActionExecutionContext, ActionServer, ActionServerOptions,
    DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS,
};
use crate::error::{AortaError, AortaResult};
use crate::node::Node;
use crate::schema::{SchemaHash, SchemaMeta, TypedSubscribeMessage};

fn unique_node_name(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_p{}_{}", base, std::process::id(), id)
}

// `CancelAction` (action `test/action_server_cancel`) is served by 3 tests. The
// action system enforces one active server per {group,action}, and the
// ActionInfo discovery query surfaces every server on the shared local-test
// router (gossip crosses groups), so two of these in parallel make the client
// see "multiple ActionInfo replies" -> ConcurrentConflict. Serialize them so at
// most one cancel server is live at a time. (Unique-action tests stay parallel.)
static CANCEL_ACTION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DummyGoalRoot<'a> {
    value: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DummyFeedbackRoot<'a> {
    goal_id: &'a str,
    seq: u64,
    stamp_ns: u64,
    status: ActionStatus,
    value: &'a str,
    message: &'a str,
}

#[derive(Clone, Debug, Default)]
struct DummyGoalMsg<'a> {
    value: &'a str,
}

#[derive(Clone, Debug, Default)]
struct DummyFeedbackDataMsg<'a> {
    value: &'a str,
}

#[derive(Clone, Debug, Default)]
struct DummyResultMsg<'a> {
    value: &'a str,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct OwnedDummyFeedbackDataMsg {
    value: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct OwnedDummyResultMsg {
    value: String,
}

struct DummyGoalSchema;
struct DummyResultSchema;
struct DummyFeedbackDataSchema;
struct DummySendGoalRequestSchema;
struct DummySendGoalResponseSchema;
struct DummyCancelGoalRequestSchema;
struct DummyCancelGoalResponseSchema;
struct DummyFeedbackSchema;
struct DummyPublicStatusSchema;
struct DummyGetResultRequestSchema;
struct DummyGetResultResponseSchema;

macro_rules! impl_dummy_schema {
    ($ty:ty, $root:literal) => {
        impl SchemaMeta for $ty {
            fn schema_bfbs() -> &'static [u8] {
                b"dummy-action-bfbs"
            }

            fn root_type() -> Option<&'static str> {
                Some($root)
            }
        }
    };
}

impl_dummy_schema!(DummyGoalSchema, "test.action.DummyGoal");
impl_dummy_schema!(DummyResultSchema, "test.action.DummyResult");
impl_dummy_schema!(DummyFeedbackDataSchema, "test.action.DummyFeedbackData");
impl_dummy_schema!(
    DummySendGoalRequestSchema,
    "test.action.DummySendGoalRequest"
);
impl_dummy_schema!(
    DummySendGoalResponseSchema,
    "test.action.DummySendGoalResponse"
);
impl_dummy_schema!(
    DummyCancelGoalRequestSchema,
    "test.action.DummyCancelGoalRequest"
);
impl_dummy_schema!(
    DummyCancelGoalResponseSchema,
    "test.action.DummyCancelGoalResponse"
);
impl_dummy_schema!(DummyFeedbackSchema, "test.action.DummyFeedback");
impl_dummy_schema!(DummyPublicStatusSchema, "test.action.DummyPublicStatus");
impl_dummy_schema!(
    DummyGetResultRequestSchema,
    "test.action.DummyGetResultRequest"
);
impl_dummy_schema!(
    DummyGetResultResponseSchema,
    "test.action.DummyGetResultResponse"
);

impl TypedSubscribeMessage for DummyGoalSchema {
    type Root<'a> = DummyGoalRoot<'a>;

    fn decode_typed_root<'a>(payload: &'a [u8]) -> AortaResult<Self::Root<'a>> {
        let value = str::from_utf8(payload)
            .map_err(|err| AortaError::invalid_argument(format!("dummy goal utf8: {}", err)))?;
        Ok(DummyGoalRoot { value })
    }
}

impl TypedSubscribeMessage for DummyFeedbackSchema {
    type Root<'a> = DummyFeedbackRoot<'a>;

    fn decode_typed_root<'a>(payload: &'a [u8]) -> AortaResult<Self::Root<'a>> {
        let text = str::from_utf8(payload)
            .map_err(|err| AortaError::invalid_argument(format!("dummy feedback utf8: {}", err)))?;
        let parts = text.split('|').collect::<Vec<_>>();
        if parts.len() != 6 {
            return Err(AortaError::invalid_argument("dummy feedback field count"));
        }
        Ok(DummyFeedbackRoot {
            goal_id: parts[0],
            seq: parts[1].parse().map_err(|err| {
                AortaError::invalid_argument(format!("dummy feedback seq: {}", err))
            })?,
            stamp_ns: parts[2].parse().map_err(|err| {
                AortaError::invalid_argument(format!("dummy feedback stamp_ns: {}", err))
            })?,
            status: ActionStatus::try_from(parts[3].parse::<i8>().map_err(|err| {
                AortaError::invalid_argument(format!("dummy feedback status: {}", err))
            })?)?,
            value: parts[4],
            message: parts[5],
        })
    }
}

fn parse_request_goal_id(payload: &[u8]) -> AortaResult<String> {
    let text = str::from_utf8(payload)
        .map_err(|err| AortaError::invalid_argument(format!("dummy request utf8: {}", err)))?;
    let (goal_id, _) = text
        .split_once('\n')
        .ok_or_else(|| AortaError::invalid_argument("dummy request missing delimiter"))?;
    Ok(goal_id.to_string())
}

fn parse_request_goal<'a>(payload: &'a [u8]) -> AortaResult<DummyGoalRoot<'a>> {
    let text = str::from_utf8(payload)
        .map_err(|err| AortaError::invalid_argument(format!("dummy request utf8: {}", err)))?;
    let (_, goal_value) = text
        .split_once('\n')
        .ok_or_else(|| AortaError::invalid_argument("dummy request missing goal payload"))?;
    Ok(DummyGoalRoot { value: goal_value })
}

fn encode_response(response: &ActionSendGoalResponse) -> Vec<u8> {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        response.accepted,
        response.goal_id,
        response.goal_secret,
        response.code as i16,
        response.status as i8,
        response.message,
        response.server_epoch,
    )
    .into_bytes()
}

fn decode_response(payload: &[u8]) -> AortaResult<ActionSendGoalResponse> {
    let text = str::from_utf8(payload)
        .map_err(|err| AortaError::invalid_argument(format!("dummy response utf8: {}", err)))?;
    let parts = text.split('|').collect::<Vec<_>>();
    if parts.len() != 7 {
        return Err(AortaError::invalid_argument("dummy response field count"));
    }
    Ok(ActionSendGoalResponse {
        accepted: parts[0]
            .parse()
            .map_err(|err| AortaError::invalid_argument(format!("dummy accepted: {}", err)))?,
        goal_id: parts[1].to_string(),
        goal_secret: parts[2].to_string(),
        code: ActionReplyCode::try_from(parts[3].parse::<i16>().map_err(|err| {
            AortaError::invalid_argument(format!("dummy response code: {}", err))
        })?)?,
        status: ActionStatus::try_from(parts[4].parse::<i8>().map_err(|err| {
            AortaError::invalid_argument(format!("dummy response status: {}", err))
        })?)?,
        message: parts[5].to_string(),
        server_epoch: parts[6].to_string(),
    })
}

fn parse_cancel_request(payload: &[u8]) -> AortaResult<(String, String)> {
    let text = str::from_utf8(payload)
        .map_err(|err| AortaError::invalid_argument(format!("dummy cancel utf8: {}", err)))?;
    let (goal_id, goal_secret) = text
        .split_once('|')
        .ok_or_else(|| AortaError::invalid_argument("dummy cancel missing delimiter"))?;
    Ok((goal_id.to_string(), goal_secret.to_string()))
}

fn encode_cancel_response(response: &ActionCancelGoalResponse) -> Vec<u8> {
    format!(
        "{}|{}|{}|{}|{}",
        response.accepted,
        response.goal_id,
        response.code as i16,
        response.status as i8,
        response.message,
    )
    .into_bytes()
}

fn decode_cancel_response(payload: &[u8]) -> AortaResult<ActionCancelGoalResponse> {
    let text = str::from_utf8(payload).map_err(|err| {
        AortaError::invalid_argument(format!("dummy cancel response utf8: {}", err))
    })?;
    let parts = text.split('|').collect::<Vec<_>>();
    if parts.len() != 5 {
        return Err(AortaError::invalid_argument(
            "dummy cancel response field count",
        ));
    }
    Ok(ActionCancelGoalResponse {
        accepted: parts[0].parse().map_err(|err| {
            AortaError::invalid_argument(format!("dummy cancel accepted: {}", err))
        })?,
        goal_id: parts[1].to_string(),
        code: ActionReplyCode::try_from(
            parts[2].parse::<i16>().map_err(|err| {
                AortaError::invalid_argument(format!("dummy cancel code: {}", err))
            })?,
        )?,
        status: ActionStatus::try_from(parts[3].parse::<i8>().map_err(|err| {
            AortaError::invalid_argument(format!("dummy cancel status: {}", err))
        })?)?,
        message: parts[4].to_string(),
    })
}

fn encode_get_result_response(
    response: &ActionGetResultResponse<OwnedDummyFeedbackDataMsg, OwnedDummyResultMsg>,
) -> Vec<u8> {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        response.goal_id,
        response.status as i8,
        response.accepted_stamp_ns,
        response.started_stamp_ns,
        response.finished_stamp_ns,
        response.last_feedback_seq,
        response.feedback_snapshot.value,
        response
            .result
            .as_ref()
            .map(|value| value.value.as_str())
            .unwrap_or(""),
        response.code as i16,
        response.message,
        response.server_epoch,
    )
    .into_bytes()
}

fn decode_get_result_response(
    payload: &[u8],
) -> AortaResult<ActionGetResultResponse<OwnedDummyFeedbackDataMsg, OwnedDummyResultMsg>> {
    let text = str::from_utf8(payload)
        .map_err(|err| AortaError::invalid_argument(format!("dummy get_result utf8: {}", err)))?;
    let parts = text.split('|').collect::<Vec<_>>();
    if parts.len() != 11 {
        return Err(AortaError::invalid_argument(
            "dummy get_result response field count",
        ));
    }
    Ok(ActionGetResultResponse {
        goal_id: parts[0].to_string(),
        status: ActionStatus::try_from(parts[1].parse::<i8>().map_err(|err| {
            AortaError::invalid_argument(format!("dummy get_result status: {}", err))
        })?)?,
        accepted_stamp_ns: parts[2].parse().map_err(|err| {
            AortaError::invalid_argument(format!("dummy get_result accepted stamp: {}", err))
        })?,
        started_stamp_ns: parts[3].parse().map_err(|err| {
            AortaError::invalid_argument(format!("dummy get_result started stamp: {}", err))
        })?,
        finished_stamp_ns: parts[4].parse().map_err(|err| {
            AortaError::invalid_argument(format!("dummy get_result finished stamp: {}", err))
        })?,
        last_feedback_seq: parts[5].parse().map_err(|err| {
            AortaError::invalid_argument(format!("dummy get_result feedback seq: {}", err))
        })?,
        feedback_snapshot: OwnedDummyFeedbackDataMsg {
            value: parts[6].to_string(),
        },
        result: if parts[7].is_empty() {
            None
        } else {
            Some(OwnedDummyResultMsg {
                value: parts[7].to_string(),
            })
        },
        code: ActionReplyCode::try_from(parts[8].parse::<i16>().map_err(|err| {
            AortaError::invalid_argument(format!("dummy get_result code: {}", err))
        })?)?,
        message: parts[9].to_string(),
        server_epoch: parts[10].to_string(),
    })
}

fn encode_status(status: &ActionPublicStatus) -> Vec<u8> {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        status.goal_id,
        status.status as i8,
        status.code as i16,
        status.accepted_stamp_ns,
        status.started_stamp_ns,
        status.finished_stamp_ns,
        status.last_feedback_seq,
        status.server_epoch,
    )
    .into_bytes()
}

fn decode_status(payload: &[u8]) -> AortaResult<ActionPublicStatus> {
    let text = str::from_utf8(payload)
        .map_err(|err| AortaError::invalid_argument(format!("dummy status utf8: {}", err)))?;
    let parts = text.split('|').collect::<Vec<_>>();
    if parts.len() != 8 {
        return Err(AortaError::invalid_argument("dummy status field count"));
    }
    Ok(ActionPublicStatus {
        goal_id: parts[0].to_string(),
        status: ActionStatus::try_from(
            parts[1].parse::<i8>().map_err(|err| {
                AortaError::invalid_argument(format!("dummy status code: {}", err))
            })?,
        )?,
        code: ActionReplyCode::try_from(
            parts[2].parse::<i16>().map_err(|err| {
                AortaError::invalid_argument(format!("dummy reply code: {}", err))
            })?,
        )?,
        accepted_stamp_ns: parts[3].parse().map_err(|err| {
            AortaError::invalid_argument(format!("dummy accepted stamp: {}", err))
        })?,
        started_stamp_ns: parts[4]
            .parse()
            .map_err(|err| AortaError::invalid_argument(format!("dummy started stamp: {}", err)))?,
        finished_stamp_ns: parts[5].parse().map_err(|err| {
            AortaError::invalid_argument(format!("dummy finished stamp: {}", err))
        })?,
        last_feedback_seq: parts[6]
            .parse()
            .map_err(|err| AortaError::invalid_argument(format!("dummy feedback seq: {}", err)))?,
        server_epoch: parts[7].to_string(),
    })
}

macro_rules! define_dummy_action {
    ($name:ident, $action_name:literal) => {
        struct $name;

        impl ActionSchema for $name {
            type Goal = DummyGoalSchema;
            type Result = DummyResultSchema;
            type FeedbackData = DummyFeedbackDataSchema;
            type SendGoalRequest = DummySendGoalRequestSchema;
            type SendGoalResponse = DummySendGoalResponseSchema;
            type CancelGoalRequest = DummyCancelGoalRequestSchema;
            type CancelGoalResponse = DummyCancelGoalResponseSchema;
            type Feedback = DummyFeedbackSchema;
            type PublicStatus = DummyPublicStatusSchema;
            type GetResultRequest = DummyGetResultRequestSchema;
            type GetResultResponse = DummyGetResultResponseSchema;

            const ACTION_NAME: &'static str = $action_name;
        }

        impl ActionWire for $name {
            type GoalMsg<'a> = DummyGoalMsg<'a>;
            type OwnedFeedbackDataMsg = OwnedDummyFeedbackDataMsg;
            type OwnedResultMsg = OwnedDummyResultMsg;
            type ResultMsg<'a> = DummyResultMsg<'a>;
            type FeedbackDataMsg<'a> = DummyFeedbackDataMsg<'a>;

            fn encode_send_goal_request(
                goal_id: &str,
                goal: &Self::GoalMsg<'_>,
            ) -> AortaResult<Vec<u8>> {
                Ok(format!("{}\n{}", goal_id, goal.value).into_bytes())
            }

            fn decode_send_goal_request_goal_id(payload: &[u8]) -> AortaResult<String> {
                parse_request_goal_id(payload)
            }

            fn decode_goal_from_send_goal_request<'a>(
                payload: &'a [u8],
            ) -> AortaResult<<Self::Goal as TypedSubscribeMessage>::Root<'a>> {
                parse_request_goal(payload)
            }

            fn encode_send_goal_response(
                response: &ActionSendGoalResponse,
            ) -> AortaResult<Vec<u8>> {
                Ok(encode_response(response))
            }

            fn decode_send_goal_response(payload: &[u8]) -> AortaResult<ActionSendGoalResponse> {
                decode_response(payload)
            }

            fn encode_cancel_goal_request(
                goal_id: &str,
                goal_secret: &str,
            ) -> AortaResult<Vec<u8>> {
                Ok(format!("{}|{}", goal_id, goal_secret).into_bytes())
            }

            fn decode_cancel_goal_request(payload: &[u8]) -> AortaResult<(String, String)> {
                parse_cancel_request(payload)
            }

            fn encode_cancel_goal_response(
                response: &ActionCancelGoalResponse,
            ) -> AortaResult<Vec<u8>> {
                Ok(encode_cancel_response(response))
            }

            fn decode_cancel_goal_response(
                payload: &[u8],
            ) -> AortaResult<ActionCancelGoalResponse> {
                decode_cancel_response(payload)
            }

            fn encode_get_result_request(goal_id: &str, goal_secret: &str) -> AortaResult<Vec<u8>> {
                Ok(format!("{}|{}", goal_id, goal_secret).into_bytes())
            }

            fn decode_get_result_request(payload: &[u8]) -> AortaResult<(String, String)> {
                parse_cancel_request(payload)
            }

            fn clone_feedback_data_owned(
                data: &Self::FeedbackDataMsg<'_>,
            ) -> AortaResult<Self::OwnedFeedbackDataMsg> {
                Ok(OwnedDummyFeedbackDataMsg {
                    value: data.value.to_string(),
                })
            }

            fn clone_result_owned(
                result: &Self::ResultMsg<'_>,
            ) -> AortaResult<Self::OwnedResultMsg> {
                Ok(OwnedDummyResultMsg {
                    value: result.value.to_string(),
                })
            }

            fn encode_get_result_response(
                response: &ActionGetResultResponse<
                    Self::OwnedFeedbackDataMsg,
                    Self::OwnedResultMsg,
                >,
            ) -> AortaResult<Vec<u8>> {
                Ok(encode_get_result_response(response))
            }

            fn decode_get_result_response(
                payload: &[u8],
            ) -> AortaResult<
                ActionGetResultResponse<Self::OwnedFeedbackDataMsg, Self::OwnedResultMsg>,
            > {
                decode_get_result_response(payload)
            }

            fn encode_feedback(
                goal_id: &str,
                seq: u64,
                stamp_ns: u64,
                status: ActionStatus,
                data: &Self::FeedbackDataMsg<'_>,
                message: Option<&str>,
            ) -> AortaResult<Vec<u8>> {
                Ok(format!(
                    "{}|{}|{}|{}|{}|{}",
                    goal_id,
                    seq,
                    stamp_ns,
                    status as i8,
                    data.value,
                    message.unwrap_or_default()
                )
                .into_bytes())
            }

            fn decode_feedback_goal_id(payload: &[u8]) -> AortaResult<String> {
                let text = str::from_utf8(payload).map_err(|err| {
                    AortaError::invalid_argument(format!("dummy feedback utf8: {}", err))
                })?;
                let (goal_id, _) = text.split_once('|').ok_or_else(|| {
                    AortaError::invalid_argument("dummy feedback missing goal_id delimiter")
                })?;
                Ok(goal_id.to_string())
            }

            fn encode_feedback_data(data: &Self::FeedbackDataMsg<'_>) -> AortaResult<Vec<u8>> {
                Ok(data.value.as_bytes().to_vec())
            }

            fn encode_result(result: &Self::ResultMsg<'_>) -> AortaResult<Vec<u8>> {
                Ok(result.value.as_bytes().to_vec())
            }

            fn encode_public_status(status: &ActionPublicStatus) -> AortaResult<Vec<u8>> {
                Ok(encode_status(status))
            }

            fn decode_public_status(payload: &[u8]) -> AortaResult<ActionPublicStatus> {
                decode_status(payload)
            }
        }
    };
}

define_dummy_action!(InfoAction, "test/action_server_info");
define_dummy_action!(RestartAction, "test/action_server_restart");
define_dummy_action!(ConflictAction, "test/action_server_conflict");
define_dummy_action!(SuccessAction, "test/action_server_success");
define_dummy_action!(SendGoalAction, "test/action_server_send_goal");
define_dummy_action!(GetResultAction, "test/action_server_get_result");
define_dummy_action!(GetResultAuthAction, "test/action_server_get_result_auth");
define_dummy_action!(FeedbackAllAction, "test/action_server_feedback_all");
define_dummy_action!(FeedbackFilterAction, "test/action_server_feedback_filter");
define_dummy_action!(TtlRemoveAction, "test/action_server_ttl_remove");
define_dummy_action!(TtlDupAction, "test/action_server_ttl_dup");
define_dummy_action!(TtlReuseAction, "test/action_server_ttl_reuse");
define_dummy_action!(
    PersistedValidateAction,
    "test/action_server_persisted_validate"
);
define_dummy_action!(PersistedWaitAction, "test/action_server_persisted_wait");
define_dummy_action!(WaitDoneAction, "test/action_server_wait_done");
define_dummy_action!(WaitRunningAction, "test/action_server_wait_running");
define_dummy_action!(WaitTimeoutAction, "test/action_server_wait_timeout");
define_dummy_action!(HandlerPanicAction, "test/action_server_handler_panic");
define_dummy_action!(HandlerDrainAction, "test/action_server_handler_drain");
define_dummy_action!(RestartReuseAction, "test/action_server_restart_reuse");
define_dummy_action!(DuplicateAction, "test/action_server_duplicate");
define_dummy_action!(BusyAction, "test/action_server_busy");
define_dummy_action!(CancelAction, "test/action_server_cancel");
define_dummy_action!(
    ZenohTimeoutRetryAction,
    "test/action_server_zenoh_timeout_retry"
);

async fn wait_for_action_info(node: &Node, action_name: &str) -> AortaResult<ActionInfo> {
    let started = tokio::time::Instant::now();
    let timeout_duration = Duration::from_millis(DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS);

    loop {
        if let Some(info) = fetch_action_info(
            node.session(),
            node.key_builder(),
            action_name,
            Duration::from_millis(250),
        )
        .await?
        {
            return Ok(info);
        }

        if started.elapsed() >= timeout_duration {
            return Err(AortaError::timeout(format!(
                "timed out waiting for action info '{}'",
                action_name
            )));
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_terminal_status<TAction: ActionWire>(
    handle: &GoalHandle<TAction>,
) -> AortaResult<ActionPublicStatus> {
    let started = tokio::time::Instant::now();
    let timeout_duration = Duration::from_secs(5);

    loop {
        if let Some(status) = handle.query_status().await? {
            if status.status.is_terminal() {
                return Ok(status);
            }
        }

        if started.elapsed() >= timeout_duration {
            return Err(AortaError::timeout(format!(
                "timed out waiting for terminal action status '{}'",
                handle.goal_id()
            )));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_status<TAction: ActionWire>(
    handle: &GoalHandle<TAction>,
    expected: ActionStatus,
) -> AortaResult<ActionPublicStatus> {
    let started = tokio::time::Instant::now();
    let timeout_duration = Duration::from_secs(5);

    loop {
        if let Some(status) = handle.query_status().await? {
            if status.status == expected {
                return Ok(status);
            }
        }

        if started.elapsed() >= timeout_duration {
            return Err(AortaError::timeout(format!(
                "timed out waiting for action status '{:?}' for '{}'",
                expected,
                handle.goal_id()
            )));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_status_absent<TAction: ActionWire>(
    handle: &GoalHandle<TAction>,
) -> AortaResult<()> {
    let started = tokio::time::Instant::now();
    let timeout_duration = Duration::from_secs(5);

    loop {
        if handle.query_status().await?.is_none() {
            return Ok(());
        }

        if started.elapsed() >= timeout_duration {
            return Err(AortaError::timeout(format!(
                "timed out waiting for action status removal '{}'",
                handle.goal_id()
            )));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_count(counter: &AtomicUsize, expected: usize) -> AortaResult<()> {
    let started = tokio::time::Instant::now();
    let timeout_duration = Duration::from_secs(5);

    loop {
        let observed = counter.load(Ordering::Relaxed);
        if observed == expected {
            return Ok(());
        }

        if started.elapsed() >= timeout_duration {
            return Err(AortaError::timeout(format!(
                "timed out waiting for counter to reach {} (observed {})",
                expected, observed
            )));
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_action_info_returns_none_when_server_absent() -> AortaResult<()> {
    let node = Node::new(unique_node_name("action_info_none"), "default").await?;
    let info = fetch_action_info(
        node.session(),
        node.key_builder(),
        "test/action_absent",
        Duration::from_millis(200),
    )
    .await?;
    assert!(info.is_none());
    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_server_serves_info_and_registers_schema() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_server_info_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_server_info_cli"), "default").await?;
    let expected_hash = SchemaHash::from_bfbs_bytes(b"dummy-action-bfbs");

    let server = ActionServer::<InfoAction>::serve_info_only(
        &server_node,
        InfoAction::ACTION_NAME,
        ActionServerOptions {
            supports_cancel: true,
            max_concurrency: 4,
            result_ttl: Duration::from_secs(45),
            dedup_ttl: Duration::from_secs(90),
        },
    )
    .await?;

    assert_eq!(server.action_name(), InfoAction::ACTION_NAME);
    assert_eq!(server.info().schema_hash, expected_hash);
    assert_eq!(server.info().server_node_name, server_node.name());
    assert_eq!(
        server_node.schema_registry().get_local(expected_hash).await,
        Some(b"dummy-action-bfbs".to_vec())
    );

    let info = wait_for_action_info(&client_node, InfoAction::ACTION_NAME).await?;
    assert_eq!(info.action_name, InfoAction::ACTION_NAME);
    assert_eq!(info.schema_hash, expected_hash);
    assert!(info.supports_cancel);
    assert_eq!(info.max_concurrency, 4);
    assert_eq!(info.result_ttl_ms, 45_000);
    assert_eq!(info.dedup_ttl_ms, 90_000);
    assert_eq!(info.server_node_name, server_node.name());

    let replies = client_node
        .session()
        .inner()
        .get(format!("{}?schema=bfbs", server.info_key()))
        .timeout(Duration::from_secs(1))
        .await?;
    let reply = replies.recv_async().await.expect("action schema reply");
    assert_eq!(
        reply
            .result()
            .expect("schema payload")
            .payload()
            .to_bytes()
            .as_ref(),
        b"dummy-action-bfbs",
        "the allowed action info key must serve BFBS without a global hash query"
    );

    let bfbs = crate::action_server::fetch_action_schema_bfbs(
        client_node.session().inner(),
        client_node.key_builder(),
        &info,
        Duration::from_secs(1),
    )
    .await?;
    assert_eq!(bfbs, b"dummy-action-bfbs");

    // Mixed-version matrix, old client x new server: a pre-PR client issues a
    // bare info query (no `schema` parameter) and resolves the schema from the
    // global hash key. Both must keep working against a current server.
    let replies = client_node
        .session()
        .inner()
        .get(server.info_key())
        .timeout(Duration::from_secs(1))
        .await?;
    let reply = replies.recv_async().await.expect("bare action info reply");
    let bare = ActionInfo::decode(
        reply
            .result()
            .expect("bare info payload")
            .payload()
            .to_bytes()
            .as_ref(),
    )?;
    assert_eq!(bare.schema_hash, expected_hash);
    assert_eq!(
        crate::schema_resolve::fetch_bfbs_by_hash(
            client_node.session().inner(),
            &expected_hash,
            Duration::from_secs(1),
        )
        .await?,
        b"dummy-action-bfbs",
        "the global hash key must stay readable for pre-PR clients"
    );
    let mut mismatched_info = info.clone();
    mismatched_info.schema_hash = SchemaHash::from_bytes([0xab; 32]);
    assert!(crate::action_server::fetch_action_schema_bfbs(
        client_node.session().inner(),
        client_node.key_builder(),
        &mismatched_info,
        Duration::from_secs(1)
    )
    .await
    .is_err());

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

/// Stand up the pre-PR action info queryable: `spawn_single_key_snapshot_queryable`
/// ignores query parameters entirely and answers `?schema=bfbs` with the same
/// ActionInfo payload it answers a bare query with. This is the exact
/// "old server" a staged rollout leaves in place.
async fn spawn_legacy_action_info_queryable(
    node: &Node,
    info: &ActionInfo,
) -> AortaResult<(
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
)> {
    let key: String = node
        .key_builder()
        .action_info_key(&info.action_name)?
        .into();
    let payload = Arc::new(tokio::sync::RwLock::new(Some(crate::ZBytes::from(
        info.encode()?,
    ))));
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let task = crate::snapshot_queryable::spawn_single_key_snapshot_queryable(
        node.session().clone(),
        key,
        payload,
        stop_rx,
    )
    .await?;
    Ok((stop_tx, task))
}

fn legacy_action_info(action_name: &str, schema_hash: SchemaHash, server: &str) -> ActionInfo {
    ActionInfo {
        action_name: action_name.to_string(),
        schema_hash,
        supports_cancel: true,
        max_concurrency: 1,
        result_ttl_ms: 30_000,
        dedup_ttl_ms: 60_000,
        server_node_name: server.to_string(),
        server_epoch: uuid::Uuid::new_v4().hyphenated().to_string(),
    }
}

/// Mixed-version matrix, new client x old server x hash readable.
///
/// A pre-PR action server replies with ActionInfo for `?schema=bfbs`, so the
/// info-key hash check fails. The client must then fall back to the global
/// content-addressed key `aorta/sys/schema/<hash>` — the pre-PR resolution
/// path — instead of aborting every `aorta action *` subcommand.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_client_resolves_legacy_action_server_schema_via_hash_key() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_legacy_hash_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_legacy_hash_cli"), "default").await?;

    let bfbs = b"legacy-action-bfbs".to_vec();
    let schema_hash = SchemaHash::from_bfbs_bytes(&bfbs);
    server_node
        .schema_registry()
        .ensure_schema(schema_hash, bfbs.clone())
        .await?;
    let action_name = format!("test/legacy_hash_{}", std::process::id());
    let info = legacy_action_info(&action_name, schema_hash, server_node.name());
    let (stop_tx, task) = spawn_legacy_action_info_queryable(&server_node, &info).await?;

    // Wait for cross-session declaration propagation before testing the old protocol.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fetch_action_info(
                client_node.session(),
                client_node.key_builder(),
                &action_name,
                Duration::from_millis(250),
            )
            .await?
            .is_some()
            {
                return Ok::<(), AortaError>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("legacy action info readiness deadline")?;

    // The old server really does answer `?schema=bfbs` with ActionInfo bytes.
    let replies = client_node
        .session()
        .inner()
        .get(format!(
            "{}?schema=bfbs",
            client_node.key_builder().action_info_key(&action_name)?
        ))
        .timeout(Duration::from_secs(2))
        .await?;
    let reply = replies.recv_async().await.expect("legacy info reply");
    let payload = reply
        .result()
        .expect("legacy info payload")
        .payload()
        .to_bytes()
        .to_vec();
    assert_ne!(
        SchemaHash::from_bfbs_bytes(&payload),
        schema_hash,
        "the legacy server replies with ActionInfo, not BFBS"
    );

    let resolved = crate::action_server::fetch_action_schema_bfbs(
        client_node.session().inner(),
        client_node.key_builder(),
        &info,
        Duration::from_secs(2),
    )
    .await?;
    assert_eq!(
        resolved, bfbs,
        "a new client must fall back to the global hash key for a pre-PR server"
    );

    let _ = stop_tx.send(());
    let _ = task.await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

/// Mixed-version matrix, new client x old server x hash denied.
///
/// With the EDU ACL the global hash key is unreadable, so neither path can
/// produce a schema. The client must fail with a clean `not_found` naming the
/// info key rather than hanging or panicking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_client_reports_not_found_when_legacy_server_and_hash_key_are_both_unavailable(
) -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_legacy_deny_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_legacy_deny_cli"), "default").await?;

    // Nothing registers this schema anywhere, which is what a denied (or
    // simply absent) `aorta/sys/schema/<hash>` looks like to the client.
    let schema_hash = SchemaHash::from_bfbs_bytes(b"never-registered-action-bfbs");
    let action_name = format!("test/legacy_deny_{}", std::process::id());
    let info = legacy_action_info(&action_name, schema_hash, server_node.name());
    let (stop_tx, task) = spawn_legacy_action_info_queryable(&server_node, &info).await?;

    let err = crate::action_server::fetch_action_schema_bfbs(
        client_node.session().inner(),
        client_node.key_builder(),
        &info,
        Duration::from_millis(500),
    )
    .await
    .expect_err("no schema is reachable on either path");
    assert!(
        err.to_string().contains("action schema not found"),
        "unexpected error: {err}"
    );

    let _ = stop_tx.send(());
    let _ = task.await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_server_without_cancel_support_does_not_accept_cancel_requests() -> AortaResult<()> {
    let _serial = CANCEL_ACTION_LOCK.lock().await;
    let server_node = Node::new(unique_node_name("action_no_cancel_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_no_cancel_cli"), "default").await?;
    let allow_finish = Arc::new(Notify::new());

    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<CancelAction>::serve_with_handler(
        &server_node,
        CancelAction::ACTION_NAME,
        ActionServerOptions {
            supports_cancel: false,
            ..Default::default()
        },
        move |_goal, ctx: ActionExecutionContext<CancelAction>| {
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                allow_finish.notified().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;

    let info = wait_for_action_info(&client_node, CancelAction::ACTION_NAME).await?;
    assert!(!info.supports_cancel);

    let client = ActionClient::<CancelAction>::new(
        &client_node,
        CancelAction::ACTION_NAME,
        ActionClientOptions {
            control_timeout: Duration::from_millis(200),
            info_timeout: Duration::from_millis(200),
            ..Default::default()
        },
    );

    let handle = client.send_goal(&DummyGoalMsg { value: "run" }).await?;
    match handle.cancel().await {
        Err(AortaError::Timeout(msg)) => {
            assert!(msg.contains("timed out"));
        }
        Err(other) => panic!("expected Timeout, got {}", other),
        Ok(response) => panic!("expected cancel timeout, got {:?}", response),
    }

    allow_finish.notify_one();
    let terminal = wait_for_terminal_status(&handle).await?;
    assert_eq!(terminal.status, ActionStatus::Succeeded);

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_server_rejects_duplicate_provider_for_same_action() -> AortaResult<()> {
    let node1 = Node::new(unique_node_name("action_dup_srv1"), "default").await?;
    let node2 = Node::new(unique_node_name("action_dup_srv2"), "default").await?;

    let server = ActionServer::<ConflictAction>::serve_info_only(
        &node1,
        ConflictAction::ACTION_NAME,
        ActionServerOptions::default(),
    )
    .await?;

    let _ = wait_for_action_info(&node2, ConflictAction::ACTION_NAME).await?;

    let result = ActionServer::<ConflictAction>::serve_info_only(
        &node2,
        ConflictAction::ACTION_NAME,
        ActionServerOptions::default(),
    )
    .await;
    match result {
        Err(AortaError::ConcurrentConflict(msg)) => {
            assert!(msg.contains("already has an active server"));
        }
        Ok(_) => panic!("expected ConcurrentConflict, got Ok(_)"),
        Err(err) => panic!("expected ConcurrentConflict, got {}", err),
    }

    server.stop_and_join(Duration::from_secs(1)).await;
    node1.close().await?;
    node2.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_goal_updates_public_status_and_completes() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_success_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_success_cli"), "default").await?;
    let server = ActionServer::<SendGoalAction>::serve_with_handler(
        &server_node,
        SendGoalAction::ACTION_NAME,
        ActionServerOptions::default(),
        |goal, ctx: ActionExecutionContext<SendGoalAction>| async move {
            let decoded = goal.decode()?;
            assert_eq!(decoded.value, "run");
            let feedback = DummyFeedbackDataMsg { value: "half" };
            ctx.publish_feedback(&feedback).await?;
            let result = DummyResultMsg { value: "done" };
            ctx.succeed(&result).await
        },
    )
    .await?;
    let client = ActionClient::<SendGoalAction>::new(
        &client_node,
        SendGoalAction::ACTION_NAME,
        ActionClientOptions::default(),
    );
    let goal = DummyGoalMsg { value: "run" };
    let handle = client.send_goal(&goal).await?;

    let status = wait_for_terminal_status(&handle).await?;
    assert_eq!(status.status, ActionStatus::Succeeded);
    assert_eq!(status.code, ActionReplyCode::Succeeded);
    assert_eq!(status.last_feedback_seq, 1);
    assert_eq!(status.server_epoch, handle.server_epoch());
    assert!(status.started_stamp_ns >= status.accepted_stamp_ns);
    assert!(status.finished_stamp_ns >= status.started_stamp_ns);

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_send_goal_is_idempotent_and_does_not_reexecute_handler() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_dupgoal_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_dupgoal_cli"), "default").await?;
    let started = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());

    let started_for_handler = Arc::clone(&started);
    let release_for_handler = Arc::clone(&release);
    let server = ActionServer::<DuplicateAction>::serve_with_handler(
        &server_node,
        DuplicateAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<DuplicateAction>| {
            let started = Arc::clone(&started_for_handler);
            let release = Arc::clone(&release_for_handler);
            async move {
                started.fetch_add(1, Ordering::Relaxed);
                release.notified().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<DuplicateAction>::new(
        &client_node,
        DuplicateAction::ACTION_NAME,
        ActionClientOptions::default(),
    );
    let goal_id = "550e8400-e29b-41d4-a716-446655440000";
    let goal = DummyGoalMsg { value: "same" };

    let first = client.send_goal_with_id(goal_id, &goal).await?;
    let second = client.send_goal_with_id(goal_id, &goal).await?;

    assert_eq!(first.goal_id(), goal_id);
    assert_eq!(second.goal_id(), goal_id);
    assert_eq!(first.goal_secret(), second.goal_secret());
    wait_for_count(started.as_ref(), 1).await?;

    release.notify_one();
    let status = wait_for_terminal_status(&first).await?;
    assert_eq!(status.status, ActionStatus::Succeeded);

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_goal_rejects_when_server_is_busy() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_busy_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_busy_cli"), "default").await?;
    let release = Arc::new(Notify::new());

    let release_for_handler = Arc::clone(&release);
    let server = ActionServer::<BusyAction>::serve_with_handler(
        &server_node,
        BusyAction::ACTION_NAME,
        ActionServerOptions {
            max_concurrency: 1,
            ..Default::default()
        },
        move |_goal, ctx: ActionExecutionContext<BusyAction>| {
            let release = Arc::clone(&release_for_handler);
            async move {
                release.notified().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<BusyAction>::new(
        &client_node,
        BusyAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let first_goal = DummyGoalMsg { value: "first" };
    let _first = client
        .send_goal_with_id("550e8400-e29b-41d4-a716-446655440001", &first_goal)
        .await?;

    let second_goal = DummyGoalMsg { value: "second" };
    let result = client
        .send_goal_with_id("550e8400-e29b-41d4-a716-446655440002", &second_goal)
        .await;
    match result {
        Err(err) => match err {
            AortaError::Busy(msg) => assert!(msg.contains("send_goal rejected")),
            other => panic!("expected Busy, got {}", other),
        },
        Ok(_) => panic!("second goal should be rejected as busy"),
    }

    release.notify_one();
    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_goal_transitions_to_canceling_and_handler_can_finish_canceled() -> AortaResult<()> {
    let _serial = CANCEL_ACTION_LOCK.lock().await;
    let server_node = Node::new(unique_node_name("action_cancel_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_cancel_cli"), "default").await?;
    let observed_cancel = Arc::new(Notify::new());
    let allow_finish = Arc::new(Notify::new());

    let observed_cancel_for_handler = Arc::clone(&observed_cancel);
    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<CancelAction>::serve_with_handler(
        &server_node,
        CancelAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<CancelAction>| {
            let observed_cancel = Arc::clone(&observed_cancel_for_handler);
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                ctx.cancelled().await;
                assert!(ctx.is_cancel_requested());
                observed_cancel.notify_waiters();
                allow_finish.notified().await;
                ctx.cancel(ActionReplyCode::Canceled, "client requested cancel")
                    .await
            }
        },
    )
    .await?;
    let client = ActionClient::<CancelAction>::new(
        &client_node,
        CancelAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let goal = DummyGoalMsg { value: "cancel-me" };
    let handle = client.send_goal(&goal).await?;
    let observed = observed_cancel.notified();
    let cancel = handle.cancel().await?;
    assert!(cancel.accepted);
    assert_eq!(cancel.goal_id, handle.goal_id());
    assert_eq!(cancel.code, ActionReplyCode::Ok);
    assert_eq!(cancel.status, ActionStatus::Canceling);

    observed.await;
    let canceling = wait_for_status(&handle, ActionStatus::Canceling).await?;
    assert_eq!(canceling.code, ActionReplyCode::Ok);

    allow_finish.notify_one();
    let terminal = wait_for_terminal_status(&handle).await?;
    assert_eq!(terminal.status, ActionStatus::Canceled);
    assert_eq!(terminal.code, ActionReplyCode::Canceled);

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_goal_rejects_when_goal_secret_is_invalid() -> AortaResult<()> {
    let _serial = CANCEL_ACTION_LOCK.lock().await;
    let server_node = Node::new(unique_node_name("action_cancel_auth_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_cancel_auth_cli"), "default").await?;
    let release = Arc::new(Notify::new());

    let release_for_handler = Arc::clone(&release);
    let server = ActionServer::<CancelAction>::serve_with_handler(
        &server_node,
        CancelAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<CancelAction>| {
            let release = Arc::clone(&release_for_handler);
            async move {
                release.notified().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<CancelAction>::new(
        &client_node,
        CancelAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let goal = DummyGoalMsg {
        value: "keep-running",
    };
    let handle = client.send_goal(&goal).await?;
    let result = client.cancel(handle.goal_id(), "wrong-secret").await;
    match result {
        Err(AortaError::InvalidArgument(msg)) => {
            assert!(msg.contains("Unauthorized"));
        }
        Err(other) => panic!("expected InvalidArgument, got {}", other),
        Ok(_) => panic!("cancel should reject invalid goal_secret"),
    }

    release.notify_one();
    let terminal = wait_for_terminal_status(&handle).await?;
    assert_eq!(terminal.status, ActionStatus::Succeeded);

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_result_returns_snapshot_while_running_and_result_after_completion() -> AortaResult<()>
{
    let server_node = Node::new(unique_node_name("action_result_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_result_cli"), "default").await?;
    let feedback_published = Arc::new(Notify::new());
    let allow_finish = Arc::new(Notify::new());

    let feedback_published_for_handler = Arc::clone(&feedback_published);
    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<GetResultAction>::serve_with_handler(
        &server_node,
        GetResultAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<GetResultAction>| {
            let feedback_published = Arc::clone(&feedback_published_for_handler);
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                let feedback = DummyFeedbackDataMsg { value: "half" };
                ctx.publish_feedback(&feedback).await?;
                feedback_published.notify_waiters();
                allow_finish.notified().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<GetResultAction>::new(
        &client_node,
        GetResultAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let feedback_ready = feedback_published.notified();
    let goal = DummyGoalMsg { value: "run" };
    let handle = client.send_goal(&goal).await?;
    feedback_ready.await;

    let running = handle.get_result().await?;
    assert_eq!(running.status, ActionStatus::Executing);
    assert_eq!(running.code, ActionReplyCode::Ok);
    assert_eq!(running.last_feedback_seq, 1);
    assert_eq!(running.feedback_snapshot.value, "half");
    assert!(running.result.is_none());

    allow_finish.notify_one();
    let terminal_status = wait_for_terminal_status(&handle).await?;
    assert_eq!(terminal_status.status, ActionStatus::Succeeded);

    let terminal = handle.get_result().await?;
    assert_eq!(terminal.status, ActionStatus::Succeeded);
    assert_eq!(terminal.code, ActionReplyCode::Succeeded);
    assert_eq!(terminal.feedback_snapshot.value, "half");
    assert_eq!(terminal.result.unwrap().value, "done");

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_result_rejects_when_goal_secret_is_invalid() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_result_auth_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_result_auth_cli"), "default").await?;
    let release = Arc::new(Notify::new());

    let release_for_handler = Arc::clone(&release);
    let server = ActionServer::<GetResultAuthAction>::serve_with_handler(
        &server_node,
        GetResultAuthAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<GetResultAuthAction>| {
            let release = Arc::clone(&release_for_handler);
            async move {
                release.notified().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<GetResultAuthAction>::new(
        &client_node,
        GetResultAuthAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let goal = DummyGoalMsg { value: "run" };
    let handle = client.send_goal(&goal).await?;
    let result = client.get_result(handle.goal_id(), "wrong-secret").await;
    match result {
        Err(AortaError::InvalidArgument(msg)) => {
            assert!(msg.contains("Unauthorized"));
        }
        Err(other) => panic!("expected InvalidArgument, got {}", other),
        Ok(_) => panic!("get_result should reject invalid goal_secret"),
    }

    release.notify_one();
    let _ = wait_for_terminal_status(&handle).await?;

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feedback_stream_all_receives_published_feedback() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_feedback_all_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_feedback_all_cli"), "default").await?;
    let allow_finish = Arc::new(Notify::new());

    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<FeedbackAllAction>::serve_with_handler(
        &server_node,
        FeedbackAllAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<FeedbackAllAction>| {
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                let feedback = DummyFeedbackDataMsg { value: "half" };
                ctx.publish_feedback_with_message(&feedback, Some("working"))
                    .await?;
                allow_finish.notified().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<FeedbackAllAction>::new(
        &client_node,
        FeedbackAllAction::ACTION_NAME,
        ActionClientOptions::default(),
    );
    let mut feedback_stream = client.feedback_stream_all().await?;

    let goal = DummyGoalMsg { value: "run" };
    let handle = client.send_goal(&goal).await?;
    let sample = tokio::time::timeout(Duration::from_secs(5), feedback_stream.recv())
        .await
        .map_err(|_| AortaError::timeout("timed out waiting for action feedback"))?
        .ok_or_else(|| AortaError::not_found("feedback stream closed unexpectedly"))??;
    let feedback = sample.decode()?;

    assert_eq!(feedback.goal_id, handle.goal_id());
    assert_eq!(feedback.seq, 1);
    assert_eq!(feedback.status, ActionStatus::Executing);
    assert_eq!(feedback.value, "half");
    assert_eq!(feedback.message, "working");

    feedback_stream.close().await?;
    allow_finish.notify_one();
    let _ = wait_for_terminal_status(&handle).await?;

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn goal_feedback_stream_filters_other_goals() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_feedback_filter_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_feedback_filter_cli"), "default").await?;
    // `tokio::sync::Notify::notify_waiters` only wakes tasks that are
    // already inside `notified().await`. The "second" goal handler is
    // racy: it might still be between `publish_feedback().await` and
    // its own `notified().await` when the test calls
    // `notify_waiters`, in which case the signal is dropped and the
    // handler stays parked → 5s `wait_for_terminal_status` timeout.
    // Use a `Semaphore` instead so each permit is durable: every
    // handler eventually drains its permit no matter when it arrives
    // at `acquire`.
    let allow_finish = Arc::new(tokio::sync::Semaphore::new(0));

    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<FeedbackFilterAction>::serve_with_handler(
        &server_node,
        FeedbackFilterAction::ACTION_NAME,
        ActionServerOptions {
            max_concurrency: 2,
            ..Default::default()
        },
        move |goal, ctx: ActionExecutionContext<FeedbackFilterAction>| {
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                let decoded = goal.decode()?;
                if decoded.value == "first" {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                }
                let feedback = DummyFeedbackDataMsg {
                    value: decoded.value,
                };
                ctx.publish_feedback(&feedback).await?;
                let permit = allow_finish
                    .acquire()
                    .await
                    .expect("allow_finish semaphore should not be closed");
                permit.forget();
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<FeedbackFilterAction>::new(
        &client_node,
        FeedbackFilterAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let first = client.send_goal(&DummyGoalMsg { value: "first" }).await?;
    let mut feedback_stream = first.feedback_stream().await?;
    let second = client.send_goal(&DummyGoalMsg { value: "second" }).await?;

    let sample = tokio::time::timeout(Duration::from_secs(5), feedback_stream.recv())
        .await
        .map_err(|_| AortaError::timeout("timed out waiting for filtered action feedback"))?
        .ok_or_else(|| AortaError::not_found("feedback stream closed unexpectedly"))??;
    let feedback = sample.decode()?;

    assert_eq!(feedback.goal_id, first.goal_id());
    assert_eq!(feedback.value, "first");
    assert_ne!(feedback.goal_id, second.goal_id());

    feedback_stream.close().await?;
    allow_finish.add_permits(2);
    let _ = wait_for_terminal_status(&first).await?;
    let _ = wait_for_terminal_status(&second).await?;

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_goal_is_removed_after_result_ttl_and_get_result_returns_goal_expired(
) -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_ttl_remove_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_ttl_remove_cli"), "default").await?;
    let server = ActionServer::<TtlRemoveAction>::serve_with_handler(
        &server_node,
        TtlRemoveAction::ACTION_NAME,
        ActionServerOptions {
            result_ttl: Duration::from_millis(150),
            dedup_ttl: Duration::from_millis(400),
            ..Default::default()
        },
        move |_goal, ctx: ActionExecutionContext<TtlRemoveAction>| async move {
            let result = DummyResultMsg { value: "done" };
            ctx.succeed(&result).await
        },
    )
    .await?;
    let client = ActionClient::<TtlRemoveAction>::new(
        &client_node,
        TtlRemoveAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let goal = DummyGoalMsg { value: "run" };
    let handle = client.send_goal(&goal).await?;
    let terminal_status = wait_for_terminal_status(&handle).await?;
    assert_eq!(terminal_status.status, ActionStatus::Succeeded);

    wait_for_status_absent(&handle).await?;
    let result = handle.get_result().await;
    match result {
        Err(AortaError::NotFound(msg)) => {
            assert!(msg.contains("GoalExpired"));
        }
        Err(other) => panic!("expected NotFound, got {}", other),
        Ok(_) => panic!("get_result should report GoalExpired after result_ttl"),
    }

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_send_goal_returns_goal_expired_during_dedup_window() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_ttl_dup_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_ttl_dup_cli"), "default").await?;
    let server = ActionServer::<TtlDupAction>::serve_with_handler(
        &server_node,
        TtlDupAction::ACTION_NAME,
        ActionServerOptions {
            result_ttl: Duration::from_millis(150),
            dedup_ttl: Duration::from_millis(500),
            ..Default::default()
        },
        move |_goal, ctx: ActionExecutionContext<TtlDupAction>| async move {
            let result = DummyResultMsg { value: "done" };
            ctx.succeed(&result).await
        },
    )
    .await?;
    let client = ActionClient::<TtlDupAction>::new(
        &client_node,
        TtlDupAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let goal_id = "550e8400-e29b-41d4-a716-446655440010";
    let goal = DummyGoalMsg { value: "same" };
    let handle = client.send_goal_with_id(goal_id, &goal).await?;
    let _ = wait_for_terminal_status(&handle).await?;
    wait_for_status_absent(&handle).await?;

    let duplicate = client.send_goal_with_id(goal_id, &goal).await;
    match duplicate {
        Err(AortaError::NotFound(msg)) => {
            assert!(msg.contains("GoalExpired"));
        }
        Err(other) => panic!("expected NotFound, got {}", other),
        Ok(_) => panic!("duplicate goal_id should be rejected during dedup window"),
    }

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

// Regression test — deterministic repro of the action_client cleanup-window race.
//
// The bug: during `GoalRecord::request_stop`, the action server clears the per-goal status
// payload *before* the queryable is torn down. Any in-flight `query_status` call landing in
// that window observes a `reply_err("snapshot unavailable")`, which the action client used
// to surface as `AortaError::Internal(...)` instead of `Ok(None)`.
//
// This test reproduces the cleanup window deterministically without timing: we bind a
// queryable on the action status key with a permanently-None payload (so every reply is
// `reply_err`). We then call `ActionClient::query_status` and assert it returns `Ok(None)`.
//
// Without the fix this assertion fails with `AortaError::Internal(...)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_status_treats_reply_err_as_absent_snapshot() -> AortaResult<()> {
    use std::sync::Arc;
    use tokio::sync::{oneshot, RwLock as TokioRwLock};

    let server_node = Node::new(unique_node_name("action_reply_err_absent_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_reply_err_absent_cli"), "default").await?;

    let goal_id = "550e8400-e29b-41d4-a716-446655440011";
    let key: String = server_node
        .key_builder()
        .action_status_key(TtlDupAction::ACTION_NAME, goal_id)?
        .into();

    let payload = Arc::new(TokioRwLock::new(None));
    let (stop_tx, stop_rx) = oneshot::channel();
    let queryable_task = crate::snapshot_queryable::spawn_single_key_snapshot_queryable(
        server_node.session().clone(),
        key,
        payload,
        stop_rx,
    )
    .await?;

    let client = ActionClient::<TtlDupAction>::new(
        &client_node,
        TtlDupAction::ACTION_NAME,
        ActionClientOptions::default(),
    );
    let result = client.query_status(goal_id).await;

    let _ = stop_tx.send(());
    queryable_task.await.expect("queryable task join");
    server_node.close().await?;
    client_node.close().await?;

    match result {
        Ok(None) => Ok(()),
        Ok(Some(status)) => panic!("expected Ok(None), got Ok(Some({:?}))", status),
        Err(err) => panic!(
            "expected Ok(None) — reply_err from a snapshot queryable must map to absent, got {}",
            err
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_goal_id_can_be_reused_after_dedup_window_expires() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_ttl_reuse_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_ttl_reuse_cli"), "default").await?;
    let started = Arc::new(AtomicUsize::new(0));

    let started_for_handler = Arc::clone(&started);
    let server = ActionServer::<TtlReuseAction>::serve_with_handler(
        &server_node,
        TtlReuseAction::ACTION_NAME,
        ActionServerOptions {
            result_ttl: Duration::from_millis(100),
            dedup_ttl: Duration::from_millis(220),
            ..Default::default()
        },
        move |_goal, ctx: ActionExecutionContext<TtlReuseAction>| {
            let started = Arc::clone(&started_for_handler);
            async move {
                started.fetch_add(1, Ordering::Relaxed);
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<TtlReuseAction>::new(
        &client_node,
        TtlReuseAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let goal_id = "550e8400-e29b-41d4-a716-446655440011";
    let goal = DummyGoalMsg { value: "same" };
    let first = client.send_goal_with_id(goal_id, &goal).await?;
    let _ = wait_for_terminal_status(&first).await?;
    wait_for_status_absent(&first).await?;
    tokio::time::sleep(Duration::from_millis(180)).await;

    let second = client.send_goal_with_id(goal_id, &goal).await?;
    let terminal = wait_for_terminal_status(&second).await?;
    assert_eq!(terminal.status, ActionStatus::Succeeded);
    assert_eq!(started.load(Ordering::Relaxed), 2);

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn from_persisted_validates_goal_id_and_goal_secret() -> AortaResult<()> {
    let node = Node::new(unique_node_name("action_persisted_validate"), "default").await?;
    let client = ActionClient::<PersistedValidateAction>::new(
        &node,
        PersistedValidateAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let invalid_goal_id =
        GoalHandle::<PersistedValidateAction>::from_persisted(&client, "bad-goal-id", "secret");
    match invalid_goal_id {
        Err(AortaError::InvalidArgument(msg)) => {
            assert!(msg.contains("invalid action goal_id"));
        }
        Err(other) => panic!("expected InvalidArgument, got {}", other),
        Ok(_) => panic!("invalid goal_id should be rejected"),
    }

    let empty_secret = GoalHandle::<PersistedValidateAction>::from_persisted(
        &client,
        "550e8400-e29b-41d4-a716-446655440012",
        "",
    );
    match empty_secret {
        Err(AortaError::InvalidArgument(msg)) => {
            assert!(msg.contains("goal_secret"));
        }
        Err(other) => panic!("expected InvalidArgument, got {}", other),
        Ok(_) => panic!("empty goal_secret should be rejected"),
    }

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn from_persisted_wait_result_succeeds_without_server_epoch() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_persisted_wait_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_persisted_wait_cli"), "default").await?;
    let allow_finish = Arc::new(Notify::new());

    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<PersistedWaitAction>::serve_with_handler(
        &server_node,
        PersistedWaitAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<PersistedWaitAction>| {
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                let feedback = DummyFeedbackDataMsg { value: "half" };
                ctx.publish_feedback(&feedback).await?;
                allow_finish.notified().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<PersistedWaitAction>::new(
        &client_node,
        PersistedWaitAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let goal = DummyGoalMsg { value: "run" };
    let accepted = client.send_goal(&goal).await?;
    let restored = GoalHandle::<PersistedWaitAction>::from_persisted(
        &client,
        accepted.goal_id(),
        accepted.goal_secret(),
    )?;
    assert_eq!(restored.goal_id(), accepted.goal_id());
    assert_eq!(restored.goal_secret(), accepted.goal_secret());
    assert!(restored.server_epoch().is_empty());

    tokio::spawn({
        let allow_finish = Arc::clone(&allow_finish);
        async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            allow_finish.notify_one();
        }
    });

    let result = restored.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(result.status, ActionStatus::Succeeded);
    assert_eq!(result.code, ActionReplyCode::Succeeded);
    assert_eq!(result.feedback_snapshot.value, "half");
    assert_eq!(result.result.unwrap().value, "done");

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_result_returns_terminal_result_when_goal_is_already_completed() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_wait_done_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_wait_done_cli"), "default").await?;
    let server = ActionServer::<WaitDoneAction>::serve_with_handler(
        &server_node,
        WaitDoneAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<WaitDoneAction>| async move {
            let result = DummyResultMsg { value: "done" };
            ctx.succeed(&result).await
        },
    )
    .await?;
    let client = ActionClient::<WaitDoneAction>::new(
        &client_node,
        WaitDoneAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let goal = DummyGoalMsg { value: "run" };
    let handle = client.send_goal(&goal).await?;
    let terminal_status = wait_for_terminal_status(&handle).await?;
    assert_eq!(terminal_status.status, ActionStatus::Succeeded);

    let result = handle.wait_result(Some(Duration::from_secs(1))).await?;
    assert_eq!(result.status, ActionStatus::Succeeded);
    assert_eq!(result.code, ActionReplyCode::Succeeded);
    assert_eq!(result.result.unwrap().value, "done");

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_result_blocks_until_goal_completes() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_wait_running_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_wait_running_cli"), "default").await?;
    let feedback_published = Arc::new(Notify::new());

    let feedback_published_for_handler = Arc::clone(&feedback_published);
    let server = ActionServer::<WaitRunningAction>::serve_with_handler(
        &server_node,
        WaitRunningAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<WaitRunningAction>| {
            let feedback_published = Arc::clone(&feedback_published_for_handler);
            async move {
                let feedback = DummyFeedbackDataMsg { value: "half" };
                ctx.publish_feedback(&feedback).await?;
                feedback_published.notify_waiters();
                tokio::time::sleep(Duration::from_millis(200)).await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<WaitRunningAction>::new(
        &client_node,
        WaitRunningAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let feedback_ready = feedback_published.notified();
    let goal = DummyGoalMsg { value: "run" };
    let handle = client.send_goal(&goal).await?;
    feedback_ready.await;

    let result = handle.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(result.status, ActionStatus::Succeeded);
    assert_eq!(result.code, ActionReplyCode::Succeeded);
    assert_eq!(result.last_feedback_seq, 1);
    assert_eq!(result.feedback_snapshot.value, "half");
    assert_eq!(result.result.unwrap().value, "done");

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_result_returns_timeout_when_goal_does_not_finish() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_wait_timeout_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_wait_timeout_cli"), "default").await?;
    let allow_finish = Arc::new(Notify::new());

    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<WaitTimeoutAction>::serve_with_handler(
        &server_node,
        WaitTimeoutAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<WaitTimeoutAction>| {
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                allow_finish.notified().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<WaitTimeoutAction>::new(
        &client_node,
        WaitTimeoutAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let goal = DummyGoalMsg { value: "run" };
    let handle = client.send_goal(&goal).await?;
    let wait_started = std::time::Instant::now();
    let wait_result = handle.wait_result(Some(Duration::from_millis(500))).await;
    // A bounded wait must respect its budget — in particular
    // the liveliness seed query must be capped by the remaining deadline, not
    // run its full 2s window.
    assert!(
        wait_started.elapsed() < Duration::from_millis(1900),
        "bounded wait_result(500ms) took {:?}",
        wait_started.elapsed()
    );
    match wait_result {
        Err(AortaError::Timeout(msg)) => {
            assert!(msg.contains("timed out waiting for terminal action status"));
        }
        Err(other) => panic!("expected Timeout, got {}", other),
        Ok(_) => panic!("wait_result should time out for unfinished goal"),
    }

    allow_finish.notify_one();
    let _ = wait_for_terminal_status(&handle).await?;

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_goal_id_can_be_reused_after_server_restart() -> AortaResult<()> {
    let server_node1 = Node::new(unique_node_name("action_restart_reuse_srv1"), "default").await?;
    let client_node = Node::new(unique_node_name("action_restart_reuse_cli"), "default").await?;
    let starts = Arc::new(AtomicUsize::new(0));

    let starts_for_handler = Arc::clone(&starts);
    let server1 = ActionServer::<RestartReuseAction>::serve_with_handler(
        &server_node1,
        RestartReuseAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<RestartReuseAction>| {
            let starts = Arc::clone(&starts_for_handler);
            async move {
                starts.fetch_add(1, Ordering::Relaxed);
                let result = DummyResultMsg { value: "done-1" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<RestartReuseAction>::new(
        &client_node,
        RestartReuseAction::ACTION_NAME,
        ActionClientOptions::default(),
    );
    let goal_id = "550e8400-e29b-41d4-a716-446655440333";
    let goal = DummyGoalMsg { value: "run" };

    let first = client.send_goal_with_id(goal_id, &goal).await?;
    let first_result = first.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(first_result.status, ActionStatus::Succeeded);
    assert_eq!(starts.load(Ordering::Relaxed), 1);

    server1.stop_and_join(Duration::from_secs(1)).await;
    server_node1.close().await?;

    let server_node2 = Node::new(unique_node_name("action_restart_reuse_srv2"), "default").await?;
    let starts_for_handler = Arc::clone(&starts);
    let server2 = ActionServer::<RestartReuseAction>::serve_with_handler(
        &server_node2,
        RestartReuseAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<RestartReuseAction>| {
            let starts = Arc::clone(&starts_for_handler);
            async move {
                starts.fetch_add(1, Ordering::Relaxed);
                let result = DummyResultMsg { value: "done-2" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;

    let second = client.send_goal_with_id(goal_id, &goal).await?;
    let second_result = second.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(second_result.status, ActionStatus::Succeeded);
    assert_eq!(starts.load(Ordering::Relaxed), 2);
    assert_ne!(first.server_epoch(), second.server_epoch());

    server2.stop_and_join(Duration::from_secs(1)).await;
    server_node2.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_panic_transitions_goal_to_handler_panic_terminal_state() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_handler_panic_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_handler_panic_cli"), "default").await?;
    let server = ActionServer::<HandlerPanicAction>::serve_with_handler(
        &server_node,
        HandlerPanicAction::ACTION_NAME,
        ActionServerOptions::default(),
        |_goal, _ctx: ActionExecutionContext<HandlerPanicAction>| async move {
            panic!("boom");
        },
    )
    .await?;
    let client = ActionClient::<HandlerPanicAction>::new(
        &client_node,
        HandlerPanicAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let handle = client.send_goal(&DummyGoalMsg { value: "run" }).await?;
    let terminal = wait_for_terminal_status(&handle).await?;
    assert_eq!(terminal.status, ActionStatus::Aborted);
    assert_eq!(terminal.code, ActionReplyCode::HandlerPanic);

    let result = handle.get_result().await?;
    assert_eq!(result.status, ActionStatus::Aborted);
    assert_eq!(result.code, ActionReplyCode::HandlerPanic);
    assert!(result.message.contains("handler panicked"));

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

// Regression: `stop_and_join` must
// await per-goal handler driver tasks, not just the queryable tasks.
// Before the fix, `spawn_goal_handler`'s outer `tokio::spawn` dropped
// its JoinHandle, so a handler still in mid-`.await` when `stop_and_join`
// returned would keep running in the background. The handler below
// sleeps past the `running` notify, then bumps `finished`. If the
// drain is hooked up correctly, `finished` MUST be true after
// `stop_and_join` returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_and_join_drains_in_flight_handler_task() -> AortaResult<()> {
    use std::sync::atomic::AtomicBool;

    let server_node = Node::new(unique_node_name("action_handler_drain_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_handler_drain_cli"), "default").await?;

    let started = Arc::new(Notify::new());
    let finished = Arc::new(AtomicBool::new(false));
    let started_for_handler = Arc::clone(&started);
    let finished_for_handler = Arc::clone(&finished);

    let server = ActionServer::<HandlerDrainAction>::serve_with_handler(
        &server_node,
        HandlerDrainAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<HandlerDrainAction>| {
            let started = Arc::clone(&started_for_handler);
            let finished = Arc::clone(&finished_for_handler);
            async move {
                started.notify_one();
                // Long enough that the test's stop_and_join wins the
                // race against natural completion — without the fix
                // stop_and_join returns first and `finished` stays
                // false. With the fix the drain pins us here.
                tokio::time::sleep(Duration::from_millis(300)).await;
                let result = DummyResultMsg { value: "drained" };
                ctx.succeed(&result).await?;
                finished.store(true, Ordering::Release);
                Ok(())
            }
        },
    )
    .await?;
    let client = ActionClient::<HandlerDrainAction>::new(
        &client_node,
        HandlerDrainAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let _handle = client.send_goal(&DummyGoalMsg { value: "drain" }).await?;
    // Wait until the handler has actually entered the sleep — otherwise
    // a fast stop_and_join could finish before send_goal_queryable even
    // dispatches, which would be a passing test for the wrong reason.
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .map_err(|_| AortaError::timeout("handler never started"))?;

    server.stop_and_join(Duration::from_secs(5)).await;

    assert!(
        finished.load(Ordering::Acquire),
        "handler task must have finished by the time stop_and_join returns; \
         this is the H-2 second-class drain contract"
    );

    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

define_dummy_action!(TraceLinkAction, "test/action_server_trace_link");

/// Action-path trace propagation, exercised over the REAL wire: the send_goal
/// query attachment is the only channel between the client task and the
/// action server, so the goal lifecycle spans sharing the client's trace —
/// and parenting to the goal_sent_to_accepted span — proves the traceparent
/// rode the attachment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_goal_links_goal_lifecycle_spans_to_the_client_trace() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_trace_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_trace_cli"), "default").await?;
    let callback_saw_trace = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_saw_trace_for_handler = Arc::clone(&callback_saw_trace);
    let release_feedback = Arc::new(Notify::new());
    let release_feedback_for_handler = Arc::clone(&release_feedback);
    let server = ActionServer::<TraceLinkAction>::serve_with_handler(
        &server_node,
        TraceLinkAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |goal, ctx: ActionExecutionContext<TraceLinkAction>| {
            let saw = Arc::clone(&callback_saw_trace_for_handler);
            let release = Arc::clone(&release_feedback_for_handler);
            async move {
                if goal.decode()?.value == "cancel_me" {
                    // Cancel path: wait for the client's cancel request.
                    ctx.cancelled().await;
                    return ctx
                        .cancel(ActionReplyCode::Canceled, "client requested cancel")
                        .await;
                }
                saw.store(ctx.inbound_trace().is_some(), Ordering::Release);
                // Publish feedback REPEATEDLY until the client confirms
                // receipt: a single publish can race the freshly-declared
                // subscriber's routing propagation through the test router
                // and be dropped unseen (CI flake: "feedback within
                // deadline"). The retry loop is deterministic — the client
                // catches one as soon as matching is up, then releases us.
                loop {
                    let feedback = DummyFeedbackDataMsg { value: "step" };
                    ctx.publish_feedback(&feedback).await?;
                    if tokio::time::timeout(Duration::from_millis(50), release.notified())
                        .await
                        .is_ok()
                    {
                        break;
                    }
                }
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<TraceLinkAction>::new(
        &client_node,
        TraceLinkAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    // Serialize the gate toggle + drain window against other trace tests.
    // Held across awaits — sound because the test body runs via `block_on`
    // (no `Send` bound), matching the trace_service_tests pattern.
    let _trace_guard = crate::trace_test_support::lock();
    crate::trace::set_trace_enabled(true);
    let stage_scope = crate::trace::intern_scope(crate::trace::stage::STAGE_SCOPE_NAME);
    crate::trace::set_scope_enabled(stage_scope, true);

    let handle = client.send_goal(&DummyGoalMsg { value: "run" }).await?;
    let mut feedback_stream = handle.feedback_stream().await?;
    let sample = tokio::time::timeout(Duration::from_secs(5), feedback_stream.recv())
        .await
        .expect("feedback within deadline")
        .expect("stream open")?;
    assert_eq!(sample.decode()?.value, "step");
    // Confirmed receipt: stop the handler's feedback retry loop.
    release_feedback.notify_one();
    // Result-delivery client half: wait_result joins the goal's trace.
    let result = handle.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(result.status, ActionStatus::Succeeded);
    let status = wait_for_terminal_status(&handle).await?;
    assert_eq!(status.status, ActionStatus::Succeeded);

    // Cancel path: a second goal that terminates via cancel.
    let cancel_handle = client
        .send_goal(&DummyGoalMsg { value: "cancel_me" })
        .await?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let cancel_response = cancel_handle.cancel().await?;
    assert!(cancel_response.accepted);
    let cancel_terminal = wait_for_terminal_status(&cancel_handle).await?;
    assert_eq!(cancel_terminal.status, ActionStatus::Canceled);

    use crate::trace::StageId;
    let name = "test/action_server_trace_link";
    let ev_sent = crate::trace::stage_event_for(StageId::ActionGoalSentToAccepted, name);
    let ev_exec = crate::trace::stage_event_for(StageId::ActionAcceptedToExecutionStarted, name);
    let ev_fin = crate::trace::stage_event_for(StageId::ActionAcceptedToFinished, name);
    let ev_cb = crate::trace::stage_event_for(StageId::ActionUserCallback, name);
    let ev_result_wait = crate::trace::stage_event_for(StageId::ActionResultWait, name);
    let ev_goal_lifetime = crate::trace::stage_event_for(StageId::ActionGoalLifetime, name);
    let ev_cancel = crate::trace::stage_event_for(StageId::ActionCancelRequestedToCanceled, name);
    let ev_fb_pub = crate::trace::marker_event_for("action.feedback_pub", name);
    let ev_fb_recv = crate::trace::marker_event_for("action.feedback_recv", name);
    let ev_admission = crate::trace::stage_event_for(StageId::ActionGoalAdmission, name);
    let ev_get_result = crate::trace::stage_event_for(StageId::ActionGetResultQuery, name);

    // The terminal stage records before the status flips, but drain with a
    // bounded retry anyway (executor scheduling slack).
    let mut recs = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        crate::trace::ring::drain_all(&mut recs);
        let have = |ev: u16| recs.iter().any(|r| r.event_id == ev);
        if (have(ev_sent)
            && have(ev_exec)
            && have(ev_fin)
            && have(ev_cb)
            && have(ev_result_wait)
            && have(ev_goal_lifetime)
            && have(ev_cancel)
            && have(ev_fb_pub)
            && have(ev_fb_recv)
            && have(ev_admission)
            && have(ev_get_result))
            || tokio::time::Instant::now() > deadline
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    crate::trace::set_trace_enabled(false);

    use crate::trace::chunk::ChunkEventType;
    // TWO goals ran and their records interleave in arbitrary drain order —
    // a bare "first match" can pick the wrong goal. Anchor goal 2 by its
    // UNIQUE cancel segment, then scope every other lookup by trace id.
    let begin = |ev: u16| {
        recs.iter().find(|r| {
            r.event_id == ev && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        })
    };
    let begin_in = |ev: u16, trace: [u8; 16]| {
        recs.iter().find(|r| {
            r.event_id == ev
                && r.trace_id == trace
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        })
    };
    let cancel_seg =
        begin(ev_cancel).expect("server must emit a cancel_requested_to_canceled slice");
    let sent = recs
        .iter()
        .find(|r| {
            r.event_id == ev_sent
                && r.trace_id != cancel_seg.trace_id
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        })
        .expect("the SUCCESS goal must emit a goal_sent_to_accepted slice");
    let exec = begin_in(ev_exec, sent.trace_id)
        .expect("server must emit an accepted_to_execution_started slice on the goal's trace");
    let fin = begin_in(ev_fin, sent.trace_id)
        .expect("server must emit an accepted_to_finished slice on the goal's trace");
    let cb = begin_in(ev_cb, sent.trace_id)
        .expect("server must emit an action user_callback slice on the goal's trace");

    // Hierarchy: accepted_to_finished is the GOAL ROOT (the one cross-process
    // link to the caller's send_goal span); the other stages nest beneath it.
    assert_ne!(sent.trace_id, [0u8; 16]);
    assert_eq!(
        fin.trace_id, sent.trace_id,
        "goal lifecycle spans must ride the client's trace (traceparent on the send_goal attachment)"
    );
    assert_eq!(
        fin.parent_span_id, sent.span_id,
        "goal root (accepted_to_finished) must parent to the goal_sent_to_accepted span"
    );
    assert_eq!(exec.trace_id, sent.trace_id);
    assert_eq!(
        exec.parent_span_id, fin.span_id,
        "execution-started must nest under the goal root"
    );
    assert_eq!(cb.trace_id, sent.trace_id);
    assert_eq!(
        cb.parent_span_id, fin.span_id,
        "action user_callback must nest under the goal root"
    );
    assert!(
        callback_saw_trace.load(Ordering::Acquire),
        "the execute callback must see the propagated traceparent via ctx.inbound_trace()"
    );

    // Goal admission: caller-parented (covers rejects, which never mint a
    // root) — sibling of the goal root in the caller's trace.
    let admission = begin_in(ev_admission, sent.trace_id)
        .expect("server must emit a goal_admission slice on the goal's trace");
    assert_eq!(
        admission.parent_span_id, sent.span_id,
        "goal_admission must parent to the caller's goal_sent span"
    );
    // Result delivery, server half: nests under the live goal's root.
    let get_result_q = begin_in(ev_get_result, sent.trace_id)
        .expect("server must emit a get_result_query slice on the goal's trace");
    assert_eq!(
        get_result_q.parent_span_id, fin.span_id,
        "get_result_query must nest under the goal root"
    );

    // Result wait + feedback markers on the FIRST goal's trace.
    let result_wait = begin_in(ev_result_wait, sent.trace_id)
        .expect("client must emit an action result_wait slice on the goal's trace");
    assert_eq!(result_wait.trace_id, sent.trace_id);
    assert_eq!(
        result_wait.parent_span_id, sent.span_id,
        "result_wait must join the goal trace under the goal_sent span"
    );

    // Client goal-lifetime span: send_goal (request) → terminal result, the one
    // client slice spanning the whole action. Joins the goal trace under the
    // goal_sent span and is anchored at the EXACT send instant, so the short
    // handshake nests INSIDE it (rather than clamping it to 5 ms on a shared
    // worker thread). Exactly one is minted — the once-guard holds across the
    // wait/status calls, and the cancel goal (no result fetched) mints none.
    let lifetime = begin_in(ev_goal_lifetime, sent.trace_id)
        .expect("client must emit an action goal_lifetime slice on the goal's trace");
    assert_eq!(
        lifetime.parent_span_id, sent.span_id,
        "goal_lifetime must join the goal trace under the goal_sent span"
    );
    assert_eq!(
        lifetime.mono_ns, sent.mono_ns,
        "goal_lifetime must start at the EXACT send_goal instant so goal_sent nests inside it"
    );
    let lifetime_begins = recs
        .iter()
        .filter(|r| {
            r.event_id == ev_goal_lifetime
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        })
        .count();
    assert_eq!(
        lifetime_begins, 1,
        "goal_lifetime is recorded once per goal (once-guard); cancel goal mints none"
    );
    let sent_end = recs
        .iter()
        .find(|r| {
            r.span_id == sent.span_id
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceEnd
        })
        .expect("goal_sent must have a SliceEnd");
    let lifetime_end = recs
        .iter()
        .find(|r| {
            r.event_id == ev_goal_lifetime
                && r.trace_id == sent.trace_id
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceEnd
        })
        .expect("goal_lifetime must have a SliceEnd");
    assert!(
        lifetime_end.mono_ns >= sent_end.mono_ns,
        "goal_lifetime must outlive the goal_sent handshake (it spans through to the result)"
    );
    // The handler may publish several feedbacks before the client's
    // subscriber is routed — pair pub ↔ recv by EXACT span ids, not by
    // ordinal (the first RECEIVED one may be the Nth PUBLISHED one).
    let fb_recv = begin_in(ev_fb_recv, sent.trace_id)
        .expect("client must emit a feedback_recv marker on the goal's trace");
    assert_eq!(fb_recv.trace_id, sent.trace_id);
    let fb_pub = recs
        .iter()
        .find(|r| {
            r.event_id == ev_fb_pub
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
                && r.span_id == fb_recv.parent_span_id
        })
        .expect(
            "the recv marker's parent must be one of the feedback_pub spans \
             (pub → recv = the transit hop)",
        );
    assert_eq!(fb_pub.trace_id, sent.trace_id);
    assert_eq!(
        fb_pub.parent_span_id, fin.span_id,
        "feedback publish must nest under the goal root"
    );

    // The cancel segment nests under the CANCELLED goal's own
    // root (its trace is distinct from the success goal's by construction).
    let cancel_root = begin_in(ev_fin, cancel_seg.trace_id)
        .expect("cancelled goal has its own accepted_to_finished root");
    assert_eq!(
        cancel_seg.parent_span_id, cancel_root.span_id,
        "cancel segment must nest under the cancelled goal's root"
    );

    drop(_trace_guard);
    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

define_dummy_action!(GoalLifetimeAction, "test/action_goal_lifetime");

/// The `get_result()` polling path (the direct API — NOT `wait_result()`) must
/// still emit the client goal-lifetime span, exactly once, even across repeated
/// terminal polls and handle clones. This is the path the downstream
/// app_agent uses; without this instrumentation its trace shows only the ~5 ms
/// `goal_sent_to_accepted` handshake and never a slice for the whole action.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_result_polling_emits_one_goal_lifetime_span() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_gl_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_gl_cli"), "default").await?;
    let server = ActionServer::<GoalLifetimeAction>::serve_with_handler(
        &server_node,
        GoalLifetimeAction::ACTION_NAME,
        ActionServerOptions::default(),
        |_goal, ctx: ActionExecutionContext<GoalLifetimeAction>| async move {
            let result = DummyResultMsg { value: "done" };
            ctx.succeed(&result).await
        },
    )
    .await?;
    let client = ActionClient::<GoalLifetimeAction>::new(
        &client_node,
        GoalLifetimeAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let _trace_guard = crate::trace_test_support::lock();
    crate::trace::set_trace_enabled(true);
    let stage_scope = crate::trace::intern_scope(crate::trace::stage::STAGE_SCOPE_NAME);
    crate::trace::set_scope_enabled(stage_scope, true);

    let handle = client.send_goal(&DummyGoalMsg { value: "run" }).await?;
    // Poll get_result() until terminal, exactly as the app_agent client does.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let resp = handle.get_result().await?;
        if resp.status.is_terminal() {
            assert_eq!(resp.status, ActionStatus::Succeeded);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "goal never finished"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Repeat terminal poll + a cloned handle: the once-guard is shared, so
    // neither mints a second lifetime span.
    let _ = handle.get_result().await?;
    let _ = handle.clone().get_result().await?;

    use crate::trace::StageId;
    let name = "test/action_goal_lifetime";
    let ev_goal_lifetime = crate::trace::stage_event_for(StageId::ActionGoalLifetime, name);
    let ev_sent = crate::trace::stage_event_for(StageId::ActionGoalSentToAccepted, name);
    let ev_result_wait = crate::trace::stage_event_for(StageId::ActionResultWait, name);

    let mut recs = Vec::new();
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        crate::trace::ring::drain_all(&mut recs);
        let have = |ev: u16| recs.iter().any(|r| r.event_id == ev);
        if (have(ev_sent) && have(ev_goal_lifetime)) || tokio::time::Instant::now() > drain_deadline
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    crate::trace::set_trace_enabled(false);

    use crate::trace::chunk::ChunkEventType;
    let count_begin = |ev: u16| {
        recs.iter()
            .filter(|r| {
                r.event_id == ev
                    && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
            })
            .count()
    };
    // Exactly one lifetime span despite three get_result() calls + a clone.
    assert_eq!(
        count_begin(ev_goal_lifetime),
        1,
        "get_result path must emit the goal_lifetime span exactly once (once-guard across polls/clones)"
    );
    // The get_result path is independently instrumented: NO wait_result was
    // called, yet the lifetime span is present.
    assert_eq!(
        count_begin(ev_result_wait),
        0,
        "wait_result was never called; the lifetime span comes from the get_result path alone"
    );
    // Linked into the goal trace under the goal_sent span, at the send instant.
    let sent = recs
        .iter()
        .find(|r| {
            r.event_id == ev_sent
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        })
        .expect("goal_sent_to_accepted slice");
    let lifetime = recs
        .iter()
        .find(|r| {
            r.event_id == ev_goal_lifetime
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        })
        .expect("goal_lifetime slice");
    assert_eq!(lifetime.trace_id, sent.trace_id);
    assert_eq!(lifetime.parent_span_id, sent.span_id);
    assert_eq!(
        lifetime.mono_ns, sent.mono_ns,
        "goal_lifetime anchors at the exact send instant"
    );

    drop(_trace_guard);
    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

/// Contract test pinning the `"Timeout"` payload zenoh synthesizes when a query
/// times out. Depending on whose deadline fires first this is either the client
/// session's `ReplyError::new("Timeout", Encoding::ZENOH_STRING)`
/// (`zenoh/src/api/session.rs`) or the router `QueryCleanup` timer's `"Timeout"`
/// payload with `Encoding::default()`
/// (`zenoh/src/net/routing/dispatcher/queries.rs`); both must classify as a
/// timeout. `reply_error_is_zenoh_query_timeout` matches the `"Timeout"`
/// payload under either site's encoding *base* (`zenoh/bytes` / `zenoh/string`,
/// the `type/subtype` before any `;schema`) — not the whole `Encoding`, whose
/// `schema` field is not a stable wire contract. This test guards that the fork
/// keeps emitting `"Timeout"` under one of those two encoding bases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zenoh_synthesized_query_timeout_reply_is_classified_as_timeout() -> AortaResult<()> {
    let node = Node::new(unique_node_name("action_ztimeout_contract"), "default").await?;
    let key = format!(
        "aorta/default/test/ztimeout_contract/p{}",
        std::process::id()
    );

    // Hold every incoming query unanswered so the only way the client-side
    // reply channel produces anything is zenoh's own query timeout.
    let queryable = node
        .session()
        .inner()
        .declare_queryable(key.clone())
        .await?;
    let holder = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok(query) = queryable.recv_async().await {
            held.push(query);
        }
    });

    let replies = node
        .session()
        .inner()
        .get(&key)
        .timeout(Duration::from_millis(100))
        .await?;
    let reply = replies.recv_async().await.map_err(|_| {
        AortaError::internal("expected zenoh synthesized timeout reply, got closed channel")
    })?;
    let err = reply
        .result()
        .expect_err("zenoh query timeout must surface as a reply error");
    assert_eq!(err.payload().to_bytes().as_ref(), b"Timeout".as_slice());
    assert!(crate::transport::reply_error_is_zenoh_query_timeout(err));

    holder.abort();
    node.close().await?;
    Ok(())
}

/// `send_goal` arms zenoh's query timeout and the tokio recv timeout with the
/// same per-attempt duration; when zenoh's fires first it delivers a
/// synthesized "Timeout" reply error. That must count as a retryable attempt
/// timeout — not abort the retry loop as `AortaError::Internal` (an observed
/// CI flake on
/// `action_sdk_feedback_streams_and_cancel_roundtrip`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_goal_retries_past_zenoh_synthesized_timeout_reply() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_ztimeout_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_ztimeout_cli"), "default").await?;

    let send_goal_key: String = server_node
        .key_builder()
        .action_send_goal_key(ZenohTimeoutRetryAction::ACTION_NAME)?
        .into();
    let queryable = server_node
        .session()
        .inner()
        .declare_queryable(send_goal_key)
        .await?;
    let responder = tokio::spawn(async move {
        // Hold the first two queries unanswered past the client's 250 ms
        // attempt timeout, then answer the retry attempts normally. Each held
        // attempt races zenoh's query timeout (the synthesized "Timeout"
        // reply error) against the client's tokio recv timeout; only the
        // zenoh side exercised the old bug, so holding two attempts raises
        // the odds a regression turns this red while the fixed path stays
        // deterministically green.
        let mut held = Vec::new();
        for _ in 0..2 {
            if let Ok(query) = queryable.recv_async().await {
                held.push(query);
            }
        }
        while let Ok(query) = queryable.recv_async().await {
            let payload = query
                .payload()
                .map(|payload| payload.to_bytes().to_vec())
                .unwrap_or_default();
            let goal_id = match parse_request_goal_id(&payload) {
                Ok(goal_id) => goal_id,
                Err(_) => continue,
            };
            let response = ActionSendGoalResponse {
                accepted: true,
                goal_id,
                goal_secret: "ztimeout-secret".to_string(),
                code: ActionReplyCode::Ok,
                status: ActionStatus::Accepted,
                message: String::new(),
                server_epoch: "ztimeout-epoch".to_string(),
            };
            let _ = query
                .reply(query.key_expr().clone(), encode_response(&response))
                .await;
        }
    });

    let client = ActionClient::<ZenohTimeoutRetryAction>::new(
        &client_node,
        ZenohTimeoutRetryAction::ACTION_NAME,
        ActionClientOptions::default(),
    );
    let handle = client
        .send_goal(&DummyGoalMsg { value: "retry-me" })
        .await?;
    assert!(!handle.goal_id().is_empty());

    responder.abort();
    client_node.close().await?;
    server_node.close().await?;
    Ok(())
}

/// Same race inside `fetch_action_info`: a held info query must resolve to
/// `Ok(None)` ("no server") whichever timeout fires first, never `Internal`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_action_info_treats_zenoh_timeout_reply_as_absent() -> AortaResult<()> {
    let node = Node::new(unique_node_name("action_ztimeout_info"), "default").await?;
    let action_name = "test/action_server_ztimeout_info";
    let info_key: String = node.key_builder().action_info_key(action_name)?.into();
    let queryable = node.session().inner().declare_queryable(info_key).await?;
    let holder = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok(query) = queryable.recv_async().await {
            held.push(query);
        }
    });

    let info = fetch_action_info(
        node.session(),
        node.key_builder(),
        action_name,
        Duration::from_millis(100),
    )
    .await?;
    assert!(info.is_none());

    holder.abort();
    node.close().await?;
    Ok(())
}

// Regression tests for the info-key half of the action_client cleanup-window race.
//
// The action info key is served by `snapshot_queryable`, which replies with the
// absent-snapshot marker whenever its payload slot is empty. `serve` fills the
// payload BEFORE declaring the queryable, so the marker never appears at
// startup — it means exactly one thing: a server is tearing down
// (`request_stop_nowait` clears the payload, then the queryable tasks undeclare
// asynchronously).
//
// `fetch_action_info` used to surface that marker as `AortaError::Internal`, so
// `ensure_single_action_provider` — and therefore `ActionServer::serve` —
// FAILED outright whenever a just-stopped server's info queryable had not
// finished undeclaring. Any process that serves one action after another hits
// it; downstream action-server restart tests flake with
// `action '<name>' info query returned error: snapshot unavailable`.

/// Caller-facing view: an absent-snapshot reply is absence of *info*, not a
/// provider fault. Deterministic, no timing — the payload is permanently `None`
/// so every reply is the marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_action_info_treats_absent_snapshot_reply_as_absent() -> AortaResult<()> {
    use std::sync::Arc;
    use tokio::sync::{oneshot, RwLock as TokioRwLock};

    let node = Node::new(unique_node_name("action_absent_snap_info"), "default").await?;
    let action_name = "test/action_server_absent_snapshot_info";
    let info_key: String = node.key_builder().action_info_key(action_name)?.into();

    let payload = Arc::new(TokioRwLock::new(None));
    let (stop_tx, stop_rx) = oneshot::channel();
    let queryable_task = crate::snapshot_queryable::spawn_single_key_snapshot_queryable(
        node.session().clone(),
        info_key,
        payload,
        stop_rx,
    )
    .await?;

    let info = fetch_action_info(
        node.session(),
        node.key_builder(),
        action_name,
        Duration::from_millis(200),
    )
    .await;

    let _ = stop_tx.send(());
    queryable_task.await.expect("queryable task join");
    node.close().await?;

    match info {
        Ok(None) => Ok(()),
        Ok(Some(found)) => panic!("expected Ok(None), got Ok(Some({:?}))", found),
        Err(err) => panic!(
            "expected Ok(None) — an absent-snapshot reply_err on the info key must map to \
             absent, got {}",
            err
        ),
    }
}

/// Admission: a marker-answering info queryable means the previous server is
/// still tearing down, and its `send_goal` surface may still accept goals — so
/// the replacement must WAIT for the teardown instead of being admitted into a
/// two-provider window. Here the teardown finishes
/// shortly after the check starts, which is the realistic restart shape: the
/// call must succeed, and only after the queryable is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_waits_for_a_tearing_down_provider_then_succeeds() -> AortaResult<()> {
    use std::sync::Arc;
    use tokio::sync::{oneshot, RwLock as TokioRwLock};

    let node = Node::new(unique_node_name("action_teardown_wait"), "default").await?;
    let action_name = "test/action_server_teardown_wait";
    let info_key: String = node.key_builder().action_info_key(action_name)?.into();

    let payload = Arc::new(TokioRwLock::new(None));
    let (stop_tx, stop_rx) = oneshot::channel();
    let queryable_task = crate::snapshot_queryable::spawn_single_key_snapshot_queryable(
        node.session().clone(),
        info_key,
        payload,
        stop_rx,
    )
    .await?;

    // Finish the "teardown" while the admission check is polling.
    let stopper = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = stop_tx.send(());
    });

    let started = tokio::time::Instant::now();
    let admitted = crate::action_server::ensure_single_action_provider(
        node.session(),
        node.key_builder(),
        action_name,
        Duration::from_millis(200),
    )
    .await;
    let waited = started.elapsed();

    stopper.await.expect("stopper join");
    queryable_task.await.expect("queryable task join");
    node.close().await?;

    if let Err(err) = admitted {
        panic!(
            "admission must succeed once the predecessor's teardown completes, got {}",
            err
        );
    }
    assert!(
        waited >= Duration::from_millis(250),
        "admission must have WAITED for the teardown, returned after {:?}",
        waited
    );
    Ok(())
}

/// End-to-end shape of the field flake: one process serves an action, calls
/// `stop()` (the nowait form — no `stop_and_join`), then immediately serves the
/// same action again. That is what a supervisor restarting a surface does, and
/// what downstream action-server restart tests do between sub-cases.
///
/// This is a SMOKE test of that shape, not the reproduction: whether the second
/// `serve` lands inside the predecessor's teardown window is a race, and it
/// usually does not. The deterministic reproductions are the marker-queryable
/// tests above/below, which pin the window open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serving_an_action_again_right_after_stop_succeeds() -> AortaResult<()> {
    let node = Node::new(unique_node_name("action_restart_after_stop"), "default").await?;
    let options = ActionServerOptions {
        supports_cancel: true,
        max_concurrency: 1,
        result_ttl: Duration::from_secs(5),
        dedup_ttl: Duration::from_secs(5),
    };

    let mut first = ActionServer::<RestartAction>::serve_info_only(
        &node,
        RestartAction::ACTION_NAME,
        options.clone(),
    )
    .await?;
    first.stop();
    drop(first);

    let second =
        ActionServer::<RestartAction>::serve_info_only(&node, RestartAction::ACTION_NAME, options)
            .await;
    let second = match second {
        Ok(server) => server,
        Err(err) => panic!(
            "re-serving an action right after stop() must succeed, got {}",
            err
        ),
    };

    second.stop_and_join(Duration::from_secs(1)).await;
    node.close().await?;
    Ok(())
}

/// The fail-closed end of the same rule: a marker that never goes away means a
/// provider that never finishes tearing down, so no replacement is admitted —
/// a bounded wait, then a conflict naming the reason (never the old opaque
/// `info query returned error: snapshot unavailable`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_fails_closed_when_a_provider_never_finishes_tearing_down() -> AortaResult<()> {
    use std::sync::Arc;
    use tokio::sync::{oneshot, RwLock as TokioRwLock};

    let node = Node::new(unique_node_name("action_teardown_stuck"), "default").await?;
    let action_name = "test/action_server_teardown_stuck";
    let info_key: String = node.key_builder().action_info_key(action_name)?.into();

    let payload = Arc::new(TokioRwLock::new(None));
    let (stop_tx, stop_rx) = oneshot::channel();
    let queryable_task = crate::snapshot_queryable::spawn_single_key_snapshot_queryable(
        node.session().clone(),
        info_key,
        payload,
        stop_rx,
    )
    .await?;

    let admitted = crate::action_server::ensure_single_action_provider(
        node.session(),
        node.key_builder(),
        action_name,
        Duration::from_millis(200),
    )
    .await;

    let _ = stop_tx.send(());
    queryable_task.await.expect("queryable task join");
    node.close().await?;

    match admitted {
        Ok(()) => panic!(
            "a replacement must NOT be admitted while a provider is still tearing down — its \
             control queryables can still accept goals"
        ),
        Err(err) => {
            let text = err.to_string();
            assert!(
                text.contains("tearing down"),
                "expected a teardown conflict, got {}",
                text
            );
            Ok(())
        }
    }
}

// =====================================================================
// DEMO: cloud cross-namespace action addressing
//
// Two action servers run on DIFFERENT Zenoh session namespaces (R1, R2) — the
// per-robot deployment model. A single namespace-free "cloud" node calls BOTH
// actions concurrently, targeting each robot by namespace, and recovers each
// robot's result. Run it and watch the narrative with:
//
//   cargo test -p aorta_core --lib \
//     cross_namespace_action_demo -- --nocapture
//
// See spec/Aorta_Cloud_Cross_Namespace_Addressing_Proposal.md §5.
// =====================================================================

define_dummy_action!(DemoExecuteAction, "demo/cross_ns_execute");

const DEMO_ROUTER_CONFIG: &str = include_str!("zenoh_router_config.json5");

async fn demo_open_router() -> AortaResult<(crate::session::Session, zenoh::config::EndPoint)> {
    use zenoh::config::WhatAmI;
    let mut config = zenoh::Config::from_json5(DEMO_ROUTER_CONFIG)
        .map_err(|err| AortaError::internal(format!("router config error: {:?}", err)))?;
    config
        .set_mode(Some(WhatAmI::Router))
        .map_err(|err| AortaError::internal(format!("router mode error: {:?}", err)))?;
    let session = crate::session::Session::open(config).await?;
    let started = tokio::time::Instant::now();
    loop {
        let locators = session.inner().info().locators().await;
        if let Some(loc) = locators
            .iter()
            .find(|loc| loc.to_string().starts_with("tcp/"))
        {
            return Ok((session, zenoh::config::EndPoint::from(loc.clone())));
        }
        if started.elapsed() >= Duration::from_secs(5) {
            return Err(AortaError::timeout("router locator not found"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A client-mode node config connected to `endpoint`; when `namespace` is set,
/// the Zenoh session prefixes every key with it on the wire (= a robot's SN).
fn demo_peer_config(
    endpoint: &zenoh::config::EndPoint,
    namespace: Option<&str>,
) -> AortaResult<zenoh::Config> {
    let ns_line = namespace
        .map(|ns| format!("  namespace: \"{ns}\",\n"))
        .unwrap_or_default();
    let json5 = format!(
        r#"{{
  mode: "client",
{ns_line}  scouting: {{ multicast: {{ enabled: false }} }},
  transport: {{ shared_memory: {{ enabled: false }} }}
}}"#
    );
    let mut config = zenoh::Config::from_json5(&json5)
        .map_err(|err| AortaError::internal(format!("peer config error: {:?}", err)))?;
    config
        .connect
        .endpoints
        .set(vec![endpoint.clone().into()])
        .map_err(|err| AortaError::internal(format!("peer connect error: {:?}", err)))?;
    Ok(config)
}

/// Spin up one robot: a node on Zenoh namespace `ns` serving DemoExecuteAction
/// with a handler that succeeds with a result tagged by the robot's name.
async fn demo_spawn_robot(
    endpoint: &zenoh::config::EndPoint,
    ns: &'static str,
) -> AortaResult<(Node, ActionServer<DemoExecuteAction>)> {
    let node = Node::with_zenoh_config(
        unique_node_name(&format!("demo_robot_{ns}")),
        "default",
        demo_peer_config(endpoint, Some(ns))?,
    )
    .await?;
    let server = ActionServer::<DemoExecuteAction>::serve_with_handler(
        &node,
        DemoExecuteAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |goal, ctx: ActionExecutionContext<DemoExecuteAction>| async move {
            let decoded = goal.decode()?;
            println!(
                "  [robot {ns}] accepted goal_id={} value={}",
                ctx.goal_id(),
                decoded.value
            );
            // Tag the result with this robot's name so the cloud can prove which
            // robot actually handled its goal.
            let result = DummyResultMsg { value: ns };
            ctx.succeed(&result).await
        },
    )
    .await?;
    Ok((node, server))
}

/// Send a goal, retrying until the (namespaced) server queryable is discovered.
async fn demo_send_goal(
    client: &ActionClient<DemoExecuteAction>,
    goal: &DummyGoalMsg<'_>,
) -> AortaResult<GoalHandle<DemoExecuteAction>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        match client.send_goal(goal).await {
            Ok(handle) => return Ok(handle),
            Err(err) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(err);
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_namespace_action_demo() -> AortaResult<()> {
    println!("\n=== Cloud cross-namespace action demo ===");
    let (router_session, endpoint) = demo_open_router().await?;
    println!("router up at {endpoint}");

    // Two robots, each on its own Zenoh namespace (its SN).
    let (robot1, server1) = demo_spawn_robot(&endpoint, "R1").await?;
    let (robot2, server2) = demo_spawn_robot(&endpoint, "R2").await?;
    println!(
        "robot servers ready: R1 ({}), R2 ({}) — both serving action '{}'",
        robot1.name(),
        robot2.name(),
        DemoExecuteAction::ACTION_NAME
    );

    // ONE namespace-free cloud node addresses any robot by namespace.
    let cloud = Node::with_zenoh_config(
        unique_node_name("demo_cloud"),
        "default",
        demo_peer_config(&endpoint, None)?,
    )
    .await?;
    let client_r1 = ActionClient::<DemoExecuteAction>::try_new(
        &cloud,
        DemoExecuteAction::ACTION_NAME,
        ActionClientOptions {
            namespace: Some("R1".to_string()),
            ..Default::default()
        },
    )?;
    let client_r2 = ActionClient::<DemoExecuteAction>::try_new(
        &cloud,
        DemoExecuteAction::ACTION_NAME,
        ActionClientOptions {
            namespace: Some("R2".to_string()),
            ..Default::default()
        },
    )?;
    println!(
        "cloud node '{}' (no namespace) created clients for R1 and R2",
        cloud.name()
    );

    // Fire both goals CONCURRENTLY from the single cloud session.
    println!("cloud sending goals to R1 and R2 concurrently...");
    let goal = DummyGoalMsg { value: "run" };
    let (h1, h2) = tokio::join!(
        demo_send_goal(&client_r1, &goal),
        demo_send_goal(&client_r2, &goal),
    );
    let h1 = h1?;
    let h2 = h2?;
    println!("  R1 goal_id={}  R2 goal_id={}", h1.goal_id(), h2.goal_id());

    // Wait for both to finish, then recover each robot's result.
    let _ = wait_for_terminal_status(&h1).await?;
    let _ = wait_for_terminal_status(&h2).await?;
    let r1 = h1.get_result().await?;
    let r2 = h2.get_result().await?;

    let r1_value = r1.result.as_ref().expect("R1 result").value.clone();
    let r2_value = r2.result.as_ref().expect("R2 result").value.clone();
    println!("  R1 result: status={:?} value={}", r1.status, r1_value);
    println!("  R2 result: status={:?} value={}", r2.status, r2_value);

    // Proof of isolation: each cloud client reached exactly its target robot.
    assert_eq!(r1.status, ActionStatus::Succeeded);
    assert_eq!(r2.status, ActionStatus::Succeeded);
    assert_eq!(r1_value, "R1", "cloud->R1 must be handled by robot R1");
    assert_eq!(r2_value, "R2", "cloud->R2 must be handled by robot R2");
    println!("OK: one cloud session reached both robots by namespace, results isolated.\n");

    server1.stop_and_join(Duration::from_secs(1)).await;
    server2.stop_and_join(Duration::from_secs(1)).await;
    cloud.close().await?;
    robot1.close().await?;
    robot2.close().await?;
    drop(router_session);
    Ok(())
}

/// An action IS a Scope — the wire cancel fires the goal
/// scope's token, sub-activities spawned on `ctx.scope()` observe it
/// structurally, and the driver drains them before the goal task ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_goal_scope_cascades_wire_cancel_to_sub_tasks() -> AortaResult<()> {
    let _serial = CANCEL_ACTION_LOCK.lock().await;
    let server_node = Node::new(unique_node_name("action_scope_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_scope_cli"), "default").await?;
    let sub_task_cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let goal_started = Arc::new(Notify::new());

    let sub_flag = Arc::clone(&sub_task_cancelled);
    let started = Arc::clone(&goal_started);
    let server = ActionServer::<CancelAction>::serve_with_handler(
        &server_node,
        CancelAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<CancelAction>| {
            let sub_flag = Arc::clone(&sub_flag);
            let started = Arc::clone(&started);
            async move {
                assert!(!ctx.scope().is_cancelled(), "scope live at goal start");
                // A goal sub-activity, structurally tied to the goal scope.
                // Flow: the dispatch boundary resolves it on cancel —
                // its OUTCOME is the observation probe (in-body post-cancel
                // code is preempted by design).
                let sub = ctx.scope().spawn(async move {
                    std::future::pending::<()>().await;
                });
                started.notify_waiters();
                // The handler itself waits for the wire cancel...
                ctx.cancelled().await;
                assert!(
                    ctx.scope().is_cancelled(),
                    "the wire cancel must fire the goal scope token"
                );
                assert!(ctx.is_cancel_requested());
                // ...joins its sub-activity (structured teardown): the
                // boundary resolved it Stopped(cancel).
                let stopped = sub
                    .join()
                    .await
                    .expect_err("the dispatch boundary resolves the sub-task");
                if stopped.is_cancel() {
                    sub_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                // ...and finishes the goal as canceled.
                ctx.cancel(ActionReplyCode::Canceled, "cancelled via scope")
                    .await
            }
        },
    )
    .await?;
    let client = ActionClient::<CancelAction>::new(
        &client_node,
        CancelAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let goal = DummyGoalMsg {
        value: "scope-cancel",
    };
    let ready = goal_started.notified();
    let handle = client.send_goal(&goal).await?;
    ready.await;
    let cancel = handle.cancel().await?;
    assert!(cancel.accepted);

    let terminal = wait_for_terminal_status(&handle).await?;
    assert_eq!(terminal.status, ActionStatus::Canceled);
    assert!(
        sub_task_cancelled.load(std::sync::atomic::Ordering::SeqCst),
        "the sub-task observed the goal scope cancellation"
    );

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

define_dummy_action!(StopCancelAction, "test/action_server_stop_cancel");
define_dummy_action!(StopDefiantAction, "test/action_server_stop_defiant");

// ---- Server stop cancels executing goal scopes ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_stop_signals_cooperative_handlers_promptly() -> AortaResult<()> {
    // A cooperatively-written handler (awaiting ctx.cancelled()) must
    // observe server stop and exit WELL before the join timeout.
    // Observation via Atomic + channel only (feedback is silently dropped
    // once terminal, so it is not a valid probe).
    let server_node = Node::new(unique_node_name("arch04_coop_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("arch04_coop_cli"), "default").await?;

    let observed_cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let entered = Arc::new(tokio::sync::Notify::new());
    let observed_for_handler = Arc::clone(&observed_cancel);
    let entered_for_handler = Arc::clone(&entered);

    let server = ActionServer::<StopCancelAction>::serve_with_handler(
        &server_node,
        StopCancelAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<StopCancelAction>| {
            let observed = Arc::clone(&observed_for_handler);
            let entered = Arc::clone(&entered_for_handler);
            async move {
                entered.notify_one();
                // The documented cooperative pattern: park on cancellation.
                ctx.cancelled().await;
                observed.store(true, std::sync::atomic::Ordering::SeqCst);
                ctx.cancel(ActionReplyCode::Canceled, "cancelled by stop")
                    .await
            }
        },
    )
    .await?;

    let _info = wait_for_action_info(&client_node, StopCancelAction::ACTION_NAME).await?;
    let client = ActionClient::<StopCancelAction>::new(
        &client_node,
        StopCancelAction::ACTION_NAME,
        ActionClientOptions::default(),
    );
    let _handle = client.send_goal(&DummyGoalMsg { value: "run" }).await?;
    entered.notified().await; // handler is executing and parked

    let join_timeout = Duration::from_secs(10);
    let started = tokio::time::Instant::now();
    server.stop_and_join(join_timeout).await;
    let elapsed = started.elapsed();

    assert!(
        observed_cancel.load(std::sync::atomic::Ordering::SeqCst),
        "handler must observe the stop-cancel signal"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "cooperative handler must not eat the join timeout (took {elapsed:?})"
    );

    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_stop_reclaims_defiant_handlers_after_timeout() -> AortaResult<()> {
    // A handler that never yields to cancellation: stop must, after the
    // (short) join timeout, abort the INNER task and cancel+close the goal
    // scope — sub-tasks spawned on ctx.scope() observe cancellation and
    // nothing outlives stop_and_join.
    let server_node = Node::new(unique_node_name("arch04_defiant_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("arch04_defiant_cli"), "default").await?;

    let subtask_cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let entered = Arc::new(tokio::sync::Notify::new());
    let subtask_probe = Arc::clone(&subtask_cancelled);
    let entered_for_handler = Arc::clone(&entered);

    let server = ActionServer::<StopDefiantAction>::serve_with_handler(
        &server_node,
        StopDefiantAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<StopDefiantAction>| {
            let probe = Arc::clone(&subtask_probe);
            let entered = Arc::clone(&entered_for_handler);
            async move {
                // Sub-task on the goal scope: the reclamation target.
                // Probe via Drop guard — set when the sub-task
                // is torn down (boundary resolution or abort), which is
                // exactly "did not outlive stop".
                struct SetOnDrop(Arc<std::sync::atomic::AtomicBool>);
                impl Drop for SetOnDrop {
                    fn drop(&mut self) {
                        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                }
                // Constructed OUTSIDE the async block (the Depart
                // lesson): a REFUSED spawn drops the body unpolled — a
                // guard constructed inside it would never exist and the
                // probe would silently miss that interleaving (seen on
                // cold-start runs where stop races the goal).
                let guard = SetOnDrop(probe);
                let _sub = ctx.scope().spawn(async move {
                    let _guard = guard;
                    std::future::pending::<()>().await;
                });
                entered.notify_one();
                // DEFIANT: never observes cancellation.
                loop {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        },
    )
    .await?;

    let _info = wait_for_action_info(&client_node, StopDefiantAction::ACTION_NAME).await?;
    let client = ActionClient::<StopDefiantAction>::new(
        &client_node,
        StopDefiantAction::ACTION_NAME,
        ActionClientOptions::default(),
    );
    let _handle = client.send_goal(&DummyGoalMsg { value: "run" }).await?;
    entered.notified().await;

    // Short join timeout: the defiant handler forces the timeout arm.
    server.stop_and_join(Duration::from_millis(300)).await;

    // The timeout arm's close is BOUNDED — the sub-task's teardown can
    // land a beat after stop_and_join returns; poll briefly (flake fix).
    let teardown_deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !subtask_cancelled.load(std::sync::atomic::Ordering::SeqCst)
        && std::time::Instant::now() < teardown_deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        subtask_cancelled.load(std::sync::atomic::Ordering::SeqCst),
        "goal-scope sub-task must not outlive stop_and_join (torn down via \
         boundary cancel or abort — the Drop guard cannot distinguish; \
         was: detached past shutdown)"
    );

    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn goal_guard_drop_fires_a_detached_cancel_and_disarm_does_not() -> AortaResult<()> {
    let _serial = CANCEL_ACTION_LOCK.lock().await;
    let server_node = Node::new(unique_node_name("goal_guard_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("goal_guard_cli"), "default").await?;
    let allow_finish = Arc::new(Notify::new());

    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<CancelAction>::serve_with_handler(
        &server_node,
        CancelAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<CancelAction>| {
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                // Finish CANCELED when cancelled, SUCCEEDED when released.
                tokio::select! {
                    _ = ctx.cancelled() => {
                        ctx.cancel(ActionReplyCode::Canceled, "guard cancel").await
                    }
                    _ = allow_finish.notified() => {
                        ctx.succeed(&DummyResultMsg { value: "done" }).await
                    }
                }
            }
        },
    )
    .await?;
    let _ = wait_for_action_info(&client_node, CancelAction::ACTION_NAME).await?;
    let client = ActionClient::<CancelAction>::new(
        &client_node,
        CancelAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    // 1) Dropping an ARMED guard cancels the goal server-side.
    let handle = client.send_goal(&DummyGoalMsg { value: "guarded" }).await?;
    let probe = handle.clone();
    drop(handle.into_guard()); // detached fire-and-forget cancel
    let terminal = wait_for_terminal_status(&probe).await?;
    assert_eq!(terminal.status, ActionStatus::Canceled);

    // 2) disarm() releases WITHOUT cancelling — the goal runs to success.
    let handle = client
        .send_goal(&DummyGoalMsg {
            value: "handed-over",
        })
        .await?;
    let guard = handle.into_guard();
    let handle = guard.disarm();
    allow_finish.notify_one();
    let terminal = wait_for_terminal_status(&handle).await?;
    assert_eq!(terminal.status, ActionStatus::Succeeded);

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

// ── Server death mid-goal must not hang wait_result ───

define_dummy_action!(DeadServerAction, "test/action_server_dead_unbounded");
define_dummy_action!(DeadServerBoundedAction, "test/action_server_dead_bounded");

/// Serve an action whose handler never finishes, send a goal, then kill the
/// server (stop + close its node, dropping the liveliness token and every
/// queryable). Returns the still-pending goal handle and the client node.
macro_rules! spawn_goal_then_kill_server {
    ($action:ty, $srv:expr, $cli:expr) => {{
        let server_node = Node::new(unique_node_name($srv), "default").await?;
        let client_node = Node::new(unique_node_name($cli), "default").await?;
        let server = ActionServer::<$action>::serve_with_handler(
            &server_node,
            <$action>::ACTION_NAME,
            ActionServerOptions::default(),
            move |_goal, ctx: ActionExecutionContext<$action>| async move {
                // Simulate a wedged goal: never finishes on its own.
                std::future::pending::<()>().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            },
        )
        .await?;
        let client = ActionClient::<$action>::new(
            &client_node,
            <$action>::ACTION_NAME,
            ActionClientOptions::default(),
        );
        let goal = DummyGoalMsg { value: "run" };
        let handle = client.send_goal(&goal).await?;

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            // Simulate an UNGRACEFUL death (crash / power loss): a plain drop
            // never publishes terminal abort statuses (that broadcast is the
            // graceful stop_and_join path); closing the session then kills
            // the queryables and the liveliness token exactly like a dead
            // process would.
            drop(server);
            let _ = server_node.close().await;
        });

        (handle, client_node)
    }};
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_result_unbounded_errors_when_server_dies_mid_goal() -> AortaResult<()> {
    let (handle, client_node) =
        spawn_goal_then_kill_server!(DeadServerAction, "action_dead_srv", "action_dead_cli");

    // The unbounded wait must surface server death as an error
    // instead of hanging forever. The outer timeout only bounds the TEST.
    let wait = tokio::time::timeout(Duration::from_secs(15), handle.wait_result(None)).await;
    let result = wait.expect("wait_result(None) must not hang after the server dies");
    let err = result.expect_err("wait_result must error once the server is gone");
    let msg = err.to_string();
    assert!(
        msg.contains("alive") || msg.contains("liveliness"),
        "error should identify server death, got: {}",
        msg
    );

    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_result_bounded_fails_fast_when_server_dies_mid_goal() -> AortaResult<()> {
    let (handle, client_node) = spawn_goal_then_kill_server!(
        DeadServerBoundedAction,
        "action_dead_b_srv",
        "action_dead_b_cli"
    );

    // A bounded wait must fail fast on server death instead of burning the
    // whole 60s budget waiting for a status sample that can never arrive.
    let started = std::time::Instant::now();
    let result = handle.wait_result(Some(Duration::from_secs(60))).await;
    let elapsed = started.elapsed();
    let err = result.expect_err("wait_result must error once the server is gone");
    let msg = err.to_string();
    assert!(
        msg.contains("alive") || msg.contains("liveliness"),
        "error should identify server death, got: {}",
        msg
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "death detection took {:?}, expected well under the 60s budget",
        elapsed
    );

    client_node.close().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-goal feedback subscription BEFORE send_goal — the client can
// open a goal-scoped stream against a pre-generated goal_id so feedback the
// server publishes as its first handler action (e.g. an ACCEPTED business
// frame) is never lost to the subscribe-after-send window.
// Per-goal end-of-stream — a goal-scoped stream closes itself once
// the client observes the goal's terminal state, so `recv()` drains the buffer
// and then returns `None` instead of parking forever.
// ---------------------------------------------------------------------------

define_dummy_action!(EarlyFeedbackAction, "test/action_server_early_feedback");
define_dummy_action!(EosCloseAction, "test/action_server_eos_close");
define_dummy_action!(EosTwoGoalsAction, "test/action_server_eos_two_goals");

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feedback_stream_for_receives_feedback_published_before_handle_exists() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_early_fb_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_early_fb_cli"), "default").await?;
    let allow_finish = Arc::new(Notify::new());

    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<EarlyFeedbackAction>::serve_with_handler(
        &server_node,
        EarlyFeedbackAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<EarlyFeedbackAction>| {
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                // First handler action: publish feedback with zero delay —
                // the exact shape of the subscribe-after-send race (business ACCEPTED frame).
                let feedback = DummyFeedbackDataMsg { value: "accepted" };
                ctx.publish_feedback(&feedback).await?;
                allow_finish.notified().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<EarlyFeedbackAction>::new(
        &client_node,
        EarlyFeedbackAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    // Subscribe BEFORE the goal exists, against the pre-generated goal_id.
    let goal_id = uuid::Uuid::new_v4().hyphenated().to_string();
    let mut stream = client.feedback_stream_for(&goal_id).await?;

    let goal = DummyGoalMsg { value: "run" };
    let handle = client.send_goal_with_id(&goal_id, &goal).await?;

    let sample = tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .map_err(|_| AortaError::timeout("early feedback lost: pre-send stream saw nothing"))?
        .ok_or_else(|| AortaError::not_found("feedback stream closed unexpectedly"))??;
    let feedback = sample.decode()?;
    assert_eq!(feedback.goal_id, goal_id);
    assert_eq!(feedback.value, "accepted");

    allow_finish.notify_one();
    let _ = wait_for_terminal_status(&handle).await?;

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn goal_scoped_feedback_stream_closes_after_terminal_result() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_eos_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_eos_cli"), "default").await?;
    let allow_feedback = Arc::new(Notify::new());
    let allow_finish = Arc::new(Notify::new());

    let allow_feedback_for_handler = Arc::clone(&allow_feedback);
    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<EosCloseAction>::serve_with_handler(
        &server_node,
        EosCloseAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<EosCloseAction>| {
            let allow_feedback = Arc::clone(&allow_feedback_for_handler);
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                allow_feedback.notified().await;
                let feedback = DummyFeedbackDataMsg { value: "half" };
                ctx.publish_feedback(&feedback).await?;
                allow_finish.notified().await;
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<EosCloseAction>::new(
        &client_node,
        EosCloseAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let mut all_stream = client.feedback_stream_all().await?;
    let goal = DummyGoalMsg { value: "run" };
    let handle = client.send_goal(&goal).await?;
    let mut goal_stream = handle.feedback_stream().await?;

    // Deterministic ordering: feedback is published only after both streams
    // exist, and the goal finishes only after the client drained the sample.
    allow_feedback.notify_one();
    let sample = tokio::time::timeout(Duration::from_secs(5), goal_stream.recv())
        .await
        .map_err(|_| AortaError::timeout("timed out waiting for goal feedback"))?
        .ok_or_else(|| AortaError::not_found("goal stream closed before terminal"))??;
    assert_eq!(sample.decode()?.value, "half");

    allow_finish.notify_one();
    let result = handle.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(result.status, ActionStatus::Succeeded);

    // Acceptance: after the client observed the terminal result, the
    // goal-scoped loop terminates on its own — recv() returns None with no
    // consumer-side timer. (Pre-fix this parks forever; the outer timeout is
    // the test harness catching the regression, not consumer protocol.)
    let eos = tokio::time::timeout(Duration::from_secs(2), goal_stream.recv()).await;
    match eos {
        Ok(None) => {}
        Ok(Some(other)) => panic!(
            "expected end-of-stream after terminal, got a sample: {:?}",
            other.map(|s| s.decode().map(|f| f.value.to_string()))
        ),
        Err(_) => panic!("goal-scoped stream still parked 2s after terminal result (no EOS)"),
    }

    // feedback_stream_all is NOT goal-scoped and must stay open: it yields the
    // published sample and then keeps waiting (timeout, not None).
    let all_sample = tokio::time::timeout(Duration::from_secs(5), all_stream.recv())
        .await
        .map_err(|_| AortaError::timeout("all-stream lost the feedback sample"))?
        .ok_or_else(|| AortaError::not_found("feedback_stream_all closed unexpectedly"))??;
    assert_eq!(all_sample.decode()?.value, "half");
    let still_open = tokio::time::timeout(Duration::from_millis(300), all_stream.recv()).await;
    assert!(
        still_open.is_err(),
        "feedback_stream_all must stay open after a goal turns terminal"
    );

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_result_closes_only_that_goals_stream() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("action_eos2_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_eos2_cli"), "default").await?;
    let allow_slow_finish = Arc::new(Notify::new());

    let allow_slow_finish_for_handler = Arc::clone(&allow_slow_finish);
    let server = ActionServer::<EosTwoGoalsAction>::serve_with_handler(
        &server_node,
        EosTwoGoalsAction::ACTION_NAME,
        ActionServerOptions {
            max_concurrency: 2,
            ..ActionServerOptions::default()
        },
        move |goal, ctx: ActionExecutionContext<EosTwoGoalsAction>| {
            let allow_slow_finish = Arc::clone(&allow_slow_finish_for_handler);
            async move {
                let finish_now = goal.decode()?.value == "finish-now";
                let feedback = DummyFeedbackDataMsg { value: "fb" };
                ctx.publish_feedback(&feedback).await?;
                if !finish_now {
                    allow_slow_finish.notified().await;
                }
                let result = DummyResultMsg { value: "done" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<EosTwoGoalsAction>::new(
        &client_node,
        EosTwoGoalsAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let fast_goal_id = uuid::Uuid::new_v4().hyphenated().to_string();
    let slow_goal_id = uuid::Uuid::new_v4().hyphenated().to_string();
    let mut fast_stream = client.feedback_stream_for(&fast_goal_id).await?;
    let mut slow_stream = client.feedback_stream_for(&slow_goal_id).await?;

    let slow_handle = client
        .send_goal_with_id(&slow_goal_id, &DummyGoalMsg { value: "wait" })
        .await?;
    let fast_handle = client
        .send_goal_with_id(
            &fast_goal_id,
            &DummyGoalMsg {
                value: "finish-now",
            },
        )
        .await?;

    let fast_result = fast_handle
        .wait_result(Some(Duration::from_secs(5)))
        .await?;
    assert_eq!(fast_result.status, ActionStatus::Succeeded);

    // The fast goal's stream drains its sample then closes...
    let fast_fb = tokio::time::timeout(Duration::from_secs(2), fast_stream.recv())
        .await
        .map_err(|_| AortaError::timeout("fast goal feedback lost"))?
        .ok_or_else(|| AortaError::not_found("fast stream closed before draining"))??;
    assert_eq!(fast_fb.decode()?.goal_id, fast_goal_id);
    let fast_eos = tokio::time::timeout(Duration::from_secs(2), fast_stream.recv()).await;
    assert!(
        matches!(fast_eos, Ok(None)),
        "fast goal stream must reach EOS after its terminal result"
    );

    // ...while the slow goal's stream is untouched: yields its sample and
    // stays open.
    let slow_fb = tokio::time::timeout(Duration::from_secs(5), slow_stream.recv())
        .await
        .map_err(|_| AortaError::timeout("slow goal feedback lost"))?
        .ok_or_else(|| AortaError::not_found("slow stream closed by another goal's terminal"))??;
    assert_eq!(slow_fb.decode()?.goal_id, slow_goal_id);
    let slow_open = tokio::time::timeout(Duration::from_millis(300), slow_stream.recv()).await;
    assert!(
        slow_open.is_err(),
        "slow goal stream must stay open until ITS terminal is observed"
    );

    allow_slow_finish.notify_one();
    let slow_result = slow_handle
        .wait_result(Some(Duration::from_secs(5)))
        .await?;
    assert_eq!(slow_result.status, ActionStatus::Succeeded);
    let slow_eos = tokio::time::timeout(Duration::from_secs(2), slow_stream.recv()).await;
    assert!(
        matches!(slow_eos, Ok(None)),
        "slow goal stream must reach EOS after its own terminal result"
    );

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feedback_stream_opened_after_terminal_is_born_closed() -> AortaResult<()> {
    // Terminal-before-open ordering: a goal-scoped
    // stream opened AFTER the client already observed the goal's terminal
    // result must be born closed (recv -> None immediately), not park
    // forever waiting for feedback that can never arrive.
    let server_node = Node::new(unique_node_name("action_born_closed_srv"), "default").await?;
    let client_node = Node::new(unique_node_name("action_born_closed_cli"), "default").await?;

    let server = ActionServer::<EosCloseAction>::serve_with_handler(
        &server_node,
        "test/action_born_closed",
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<EosCloseAction>| async move {
            let result = DummyResultMsg { value: "done" };
            ctx.succeed(&result).await
        },
    )
    .await?;
    let client = ActionClient::<EosCloseAction>::new(
        &client_node,
        "test/action_born_closed",
        ActionClientOptions::default(),
    );

    let handle = client.send_goal(&DummyGoalMsg { value: "run" }).await?;
    let result = handle.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(result.status, ActionStatus::Succeeded);

    // Terminal already observed — BOTH goal-scoped openers must yield a
    // born-closed stream.
    let mut late_for = client.feedback_stream_for(handle.goal_id()).await?;
    let eos = tokio::time::timeout(Duration::from_secs(2), late_for.recv()).await;
    assert!(
        matches!(eos, Ok(None)),
        "feedback_stream_for after terminal must be born closed, got {:?}",
        eos.map(|s| s.is_some())
    );

    let mut late_handle = handle.feedback_stream().await?;
    let eos = tokio::time::timeout(Duration::from_secs(2), late_handle.recv()).await;
    assert!(
        matches!(eos, Ok(None)),
        "handle.feedback_stream after terminal must be born closed"
    );

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}
