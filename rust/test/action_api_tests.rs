use std::str;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::{
    ActionClient, ActionClientOptions, ActionExecutionContext, ActionGetResultResponse, ActionInfo,
    ActionPublicStatus, ActionReplyCode, ActionSchema, ActionSendGoalResponse, ActionServer,
    ActionServerOptions, ActionStatus, ActionWire, AortaError, AortaResult, GoalHandle, Node,
    SchemaMeta, TypedSubscribeMessage,
};

fn unique_node_name(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_p{}_{}", base, std::process::id(), id)
}

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
                b"sdk-action-bfbs"
            }

            fn root_type() -> Option<&'static str> {
                Some($root)
            }
        }
    };
}

impl_dummy_schema!(DummyGoalSchema, "sdk.action.DummyGoal");
impl_dummy_schema!(DummyResultSchema, "sdk.action.DummyResult");
impl_dummy_schema!(DummyFeedbackDataSchema, "sdk.action.DummyFeedbackData");
impl_dummy_schema!(
    DummySendGoalRequestSchema,
    "sdk.action.DummySendGoalRequest"
);
impl_dummy_schema!(
    DummySendGoalResponseSchema,
    "sdk.action.DummySendGoalResponse"
);
impl_dummy_schema!(
    DummyCancelGoalRequestSchema,
    "sdk.action.DummyCancelGoalRequest"
);
impl_dummy_schema!(
    DummyCancelGoalResponseSchema,
    "sdk.action.DummyCancelGoalResponse"
);
impl_dummy_schema!(DummyFeedbackSchema, "sdk.action.DummyFeedback");
impl_dummy_schema!(DummyPublicStatusSchema, "sdk.action.DummyPublicStatus");
impl_dummy_schema!(
    DummyGetResultRequestSchema,
    "sdk.action.DummyGetResultRequest"
);
impl_dummy_schema!(
    DummyGetResultResponseSchema,
    "sdk.action.DummyGetResultResponse"
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
                AortaError::invalid_argument(format!("dummy feedback stamp: {}", err))
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

fn encode_send_goal_response(response: &ActionSendGoalResponse) -> Vec<u8> {
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

fn decode_send_goal_response(payload: &[u8]) -> AortaResult<ActionSendGoalResponse> {
    let text = str::from_utf8(payload).map_err(|err| {
        AortaError::invalid_argument(format!("dummy send_goal response utf8: {}", err))
    })?;
    let parts = text.split('|').collect::<Vec<_>>();
    if parts.len() != 7 {
        return Err(AortaError::invalid_argument(
            "dummy send_goal response field count",
        ));
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

fn parse_goal_secret_request(payload: &[u8]) -> AortaResult<(String, String)> {
    let text = str::from_utf8(payload)
        .map_err(|err| AortaError::invalid_argument(format!("dummy request utf8: {}", err)))?;
    let (goal_id, goal_secret) = text
        .split_once('|')
        .ok_or_else(|| AortaError::invalid_argument("dummy request missing delimiter"))?;
    Ok((goal_id.to_string(), goal_secret.to_string()))
}

fn encode_cancel_goal_response(response: &crate::ActionCancelGoalResponse) -> Vec<u8> {
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

fn decode_cancel_goal_response(payload: &[u8]) -> AortaResult<crate::ActionCancelGoalResponse> {
    let text = str::from_utf8(payload).map_err(|err| {
        AortaError::invalid_argument(format!("dummy cancel response utf8: {}", err))
    })?;
    let parts = text.split('|').collect::<Vec<_>>();
    if parts.len() != 5 {
        return Err(AortaError::invalid_argument(
            "dummy cancel response field count",
        ));
    }
    Ok(crate::ActionCancelGoalResponse {
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

fn encode_public_status(status: &ActionPublicStatus) -> Vec<u8> {
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

fn decode_public_status(payload: &[u8]) -> AortaResult<ActionPublicStatus> {
    let text = str::from_utf8(payload)
        .map_err(|err| AortaError::invalid_argument(format!("dummy status utf8: {}", err)))?;
    let parts = text.split('|').collect::<Vec<_>>();
    if parts.len() != 8 {
        return Err(AortaError::invalid_argument("dummy status field count"));
    }
    Ok(ActionPublicStatus {
        goal_id: parts[0].to_string(),
        status: ActionStatus::try_from(
            parts[1]
                .parse::<i8>()
                .map_err(|err| AortaError::invalid_argument(format!("dummy status: {}", err)))?,
        )?,
        code: ActionReplyCode::try_from(
            parts[2].parse::<i16>().map_err(|err| {
                AortaError::invalid_argument(format!("dummy status code: {}", err))
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
                Ok(encode_send_goal_response(response))
            }

            fn decode_send_goal_response(payload: &[u8]) -> AortaResult<ActionSendGoalResponse> {
                decode_send_goal_response(payload)
            }

            fn encode_cancel_goal_request(
                goal_id: &str,
                goal_secret: &str,
            ) -> AortaResult<Vec<u8>> {
                Ok(format!("{}|{}", goal_id, goal_secret).into_bytes())
            }

            fn decode_cancel_goal_request(payload: &[u8]) -> AortaResult<(String, String)> {
                parse_goal_secret_request(payload)
            }

            fn encode_cancel_goal_response(
                response: &crate::ActionCancelGoalResponse,
            ) -> AortaResult<Vec<u8>> {
                Ok(encode_cancel_goal_response(response))
            }

            fn decode_cancel_goal_response(
                payload: &[u8],
            ) -> AortaResult<crate::ActionCancelGoalResponse> {
                decode_cancel_goal_response(payload)
            }

            fn encode_get_result_request(goal_id: &str, goal_secret: &str) -> AortaResult<Vec<u8>> {
                Ok(format!("{}|{}", goal_id, goal_secret).into_bytes())
            }

            fn decode_get_result_request(payload: &[u8]) -> AortaResult<(String, String)> {
                parse_goal_secret_request(payload)
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
                Ok(encode_public_status(status))
            }

            fn decode_public_status(payload: &[u8]) -> AortaResult<ActionPublicStatus> {
                decode_public_status(payload)
            }
        }
    };
}

define_dummy_action!(SdkRoundtripAction, "sdk/action_roundtrip");
define_dummy_action!(SdkFeedbackAction, "sdk/action_feedback_cancel");
define_dummy_action!(SdkRestartReuseAction, "sdk/action_restart_reuse");
define_dummy_action!(SdkInfoOnlyAction, "sdk/action_info_only");
define_dummy_action!(SdkEarlyEosAction, "sdk/action_early_eos");

async fn wait_for_action_info<TAction: ActionWire + Send + Sync + 'static>(
    node: &Node,
) -> AortaResult<ActionInfo> {
    let started = tokio::time::Instant::now();
    let timeout_duration = Duration::from_secs(5);

    loop {
        if let Some(info) = ActionServer::<TAction>::query_info(node, TAction::ACTION_NAME).await? {
            return Ok(info);
        }

        if started.elapsed() >= timeout_duration {
            return Err(AortaError::timeout("timed out waiting for action info"));
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
            return Err(AortaError::timeout("timed out waiting for terminal status"));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_sdk_info_only_server_advertises_action_info() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("sdk_action_info_only_srv"), "sdk_test").await?;
    let client_node = Node::new(unique_node_name("sdk_action_info_only_cli"), "sdk_test").await?;
    let server = ActionServer::<SdkInfoOnlyAction>::serve_info_only(
        &server_node,
        SdkInfoOnlyAction::ACTION_NAME,
        ActionServerOptions::default(),
    )
    .await?;

    let info = wait_for_action_info::<SdkInfoOnlyAction>(&client_node).await?;
    assert_eq!(info.action_name, SdkInfoOnlyAction::ACTION_NAME);
    assert_eq!(info.server_node_name, server_node.name());
    assert_eq!(info.max_concurrency, 1);
    assert!(info.supports_cancel);

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_sdk_roundtrip_supports_info_and_persisted_handle() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("sdk_action_roundtrip_srv"), "sdk_test").await?;
    let client_node = Node::new(unique_node_name("sdk_action_roundtrip_cli"), "sdk_test").await?;
    let server = ActionServer::<SdkRoundtripAction>::serve(
        &server_node,
        SdkRoundtripAction::ACTION_NAME,
        ActionServerOptions::default(),
        |goal, ctx: ActionExecutionContext<SdkRoundtripAction>| async move {
            let decoded = goal.decode()?;
            assert_eq!(decoded.value, "run");
            let result = DummyResultMsg { value: "done" };
            ctx.succeed(&result).await
        },
    )
    .await?;
    let client = ActionClient::<SdkRoundtripAction>::new(
        &client_node,
        SdkRoundtripAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let info = wait_for_action_info::<SdkRoundtripAction>(&client_node).await?;
    assert_eq!(info.action_name, SdkRoundtripAction::ACTION_NAME);
    assert_eq!(info.server_node_name, server_node.name());

    let handle = client.send_goal(&DummyGoalMsg { value: "run" }).await?;
    let restored = GoalHandle::<SdkRoundtripAction>::from_persisted(
        &client,
        handle.goal_id(),
        handle.goal_secret(),
    )?;

    let terminal = restored.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(terminal.status, ActionStatus::Succeeded);
    assert_eq!(terminal.code, ActionReplyCode::Succeeded);
    assert_eq!(terminal.result.unwrap().value, "done");

    let public_status = wait_for_terminal_status(&handle).await?;
    assert_eq!(public_status.status, ActionStatus::Succeeded);
    assert_eq!(public_status.server_epoch, handle.server_epoch());

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_sdk_observe_result_and_on_result_deliver_terminal() -> AortaResult<()> {
    // A unique runtime name keeps this test's server/client off the shared
    // `SdkRoundtripAction::ACTION_NAME` bus, so concurrent sibling tests can't
    // cross-talk into our goals.
    let action_name = "sdk/observe/result";
    let server_node = Node::new(unique_node_name("sdk_action_observe_srv"), "sdk_test").await?;
    let client_node = Node::new(unique_node_name("sdk_action_observe_cli"), "sdk_test").await?;
    let server = ActionServer::<SdkRoundtripAction>::serve(
        &server_node,
        action_name,
        ActionServerOptions::default(),
        |goal, ctx: ActionExecutionContext<SdkRoundtripAction>| async move {
            let _ = goal.decode()?;
            ctx.succeed(&DummyResultMsg { value: "done" }).await
        },
    )
    .await?;
    let client = ActionClient::<SdkRoundtripAction>::new(
        &client_node,
        action_name,
        ActionClientOptions::default(),
    );
    // Wait until our uniquely-named server is discoverable before sending goals.
    loop {
        if ActionServer::<SdkRoundtripAction>::query_info(&client_node, action_name)
            .await?
            .is_some()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // observe_result: a cheap lazy handle that does no work until recv() is
    // awaited, then drives the same event-driven wait as wait_result.
    let handle = client.send_goal(&DummyGoalMsg { value: "run" }).await?;
    let observer = handle.observe_result(Some(Duration::from_secs(5)));
    let terminal = observer.recv().await?;
    assert_eq!(terminal.status, ActionStatus::Succeeded);
    assert_eq!(terminal.result.unwrap().value, "done");

    // on_result: fire-and-forget; the callback is invoked off-task with the
    // terminal outcome. A std mpsc bridges it back to the test.
    let handle2 = client.send_goal(&DummyGoalMsg { value: "run" }).await?;
    let (tx, rx) = std::sync::mpsc::channel();
    handle2.on_result(Some(Duration::from_secs(5)), move |res| {
        let summary = res.map(|r| (r.status, r.result.map(|v| v.value)));
        let _ = tx.send(summary);
    });
    let delivered = tokio::task::spawn_blocking(move || rx.recv())
        .await
        .expect("spawn_blocking join")
        .expect("on_result callback fired");
    let (status, value) = delivered?;
    assert_eq!(status, ActionStatus::Succeeded);
    assert_eq!(value.as_deref(), Some("done"));

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_same_schema_supports_multiple_runtime_names() -> AortaResult<()> {
    // One ActionSchema backs two independently-named actions on one node,
    // mirroring how one message schema backs many topics. The keyexpr name is
    // a runtime argument decoupled from the schema (custom-name proposal).
    let server_node = Node::new(unique_node_name("sdk_action_multiname_srv"), "sdk_test").await?;
    let client_node = Node::new(unique_node_name("sdk_action_multiname_cli"), "sdk_test").await?;

    let name_a = "sdk/multiname/alpha";
    let name_b = "sdk/multiname/beta";

    let server_a = ActionServer::<SdkRoundtripAction>::serve(
        &server_node,
        name_a,
        ActionServerOptions::default(),
        |goal, ctx: ActionExecutionContext<SdkRoundtripAction>| async move {
            let _ = goal.decode()?;
            ctx.succeed(&DummyResultMsg {
                value: "from-alpha",
            })
            .await
        },
    )
    .await?;
    let server_b = ActionServer::<SdkRoundtripAction>::serve(
        &server_node,
        name_b,
        ActionServerOptions::default(),
        |goal, ctx: ActionExecutionContext<SdkRoundtripAction>| async move {
            let _ = goal.decode()?;
            ctx.succeed(&DummyResultMsg { value: "from-beta" }).await
        },
    )
    .await?;

    assert_eq!(server_a.action_name(), name_a);
    assert_eq!(server_b.action_name(), name_b);

    // Discovery is per-name: each name resolves to its own provider.
    let info_a = ActionServer::<SdkRoundtripAction>::query_info(&client_node, name_a)
        .await?
        .ok_or_else(|| AortaError::not_found("no provider for alpha"))?;
    let info_b = ActionServer::<SdkRoundtripAction>::query_info(&client_node, name_b)
        .await?
        .ok_or_else(|| AortaError::not_found("no provider for beta"))?;
    assert_eq!(info_a.action_name, name_a);
    assert_eq!(info_b.action_name, name_b);
    assert_eq!(info_a.schema_hash, info_b.schema_hash);

    // Each client targets one name and reaches only its own server.
    let client_a = ActionClient::<SdkRoundtripAction>::new(
        &client_node,
        name_a,
        ActionClientOptions::default(),
    );
    let client_b = ActionClient::<SdkRoundtripAction>::new(
        &client_node,
        name_b,
        ActionClientOptions::default(),
    );

    let handle_a = client_a.send_goal(&DummyGoalMsg { value: "run" }).await?;
    let res_a = handle_a.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(res_a.result.unwrap().value, "from-alpha");

    let handle_b = client_b.send_goal(&DummyGoalMsg { value: "run" }).await?;
    let res_b = handle_b.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(res_b.result.unwrap().value, "from-beta");

    // A name with no server returns no provider (no silent corruption).
    let missing =
        ActionServer::<SdkRoundtripAction>::query_info(&client_node, "sdk/multiname/nonexistent")
            .await?;
    assert!(missing.is_none());

    server_a.stop_and_join(Duration::from_secs(1)).await;
    server_b.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_same_goal_id_can_be_reused_after_server_restart_via_sdk() -> AortaResult<()> {
    let goal_id = "550e8400-e29b-41d4-a716-446655440444";
    let goal = DummyGoalMsg { value: "run" };
    let starts = Arc::new(AtomicUsize::new(0));

    let server_node1 = Node::new(
        unique_node_name("sdk_action_restart_reuse_srv1"),
        "sdk_test",
    )
    .await?;
    let client_node =
        Node::new(unique_node_name("sdk_action_restart_reuse_cli"), "sdk_test").await?;
    let starts_for_handler = Arc::clone(&starts);
    let server1 = ActionServer::<SdkRestartReuseAction>::serve(
        &server_node1,
        SdkRestartReuseAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<SdkRestartReuseAction>| {
            let starts = Arc::clone(&starts_for_handler);
            async move {
                starts.fetch_add(1, Ordering::Relaxed);
                let result = DummyResultMsg { value: "done-1" };
                ctx.succeed(&result).await
            }
        },
    )
    .await?;
    let client = ActionClient::<SdkRestartReuseAction>::new(
        &client_node,
        SdkRestartReuseAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let first = client.send_goal_with_id(goal_id, &goal).await?;
    let first_result = first.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(first_result.status, ActionStatus::Succeeded);
    assert_eq!(starts.load(Ordering::Relaxed), 1);

    server1.stop_and_join(Duration::from_secs(1)).await;
    server_node1.close().await?;

    let server_node2 = Node::new(
        unique_node_name("sdk_action_restart_reuse_srv2"),
        "sdk_test",
    )
    .await?;
    let starts_for_handler = Arc::clone(&starts);
    let server2 = ActionServer::<SdkRestartReuseAction>::serve(
        &server_node2,
        SdkRestartReuseAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<SdkRestartReuseAction>| {
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
async fn action_sdk_feedback_streams_and_cancel_roundtrip() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("sdk_action_feedback_srv"), "sdk_test").await?;
    let client_node = Node::new(unique_node_name("sdk_action_feedback_cli"), "sdk_test").await?;
    let allow_feedback = Arc::new(tokio::sync::Notify::new());
    let observed_cancel = Arc::new(tokio::sync::Notify::new());
    let allow_finish = Arc::new(tokio::sync::Notify::new());

    let allow_feedback_for_handler = Arc::clone(&allow_feedback);
    let observed_cancel_for_handler = Arc::clone(&observed_cancel);
    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<SdkFeedbackAction>::serve(
        &server_node,
        SdkFeedbackAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<SdkFeedbackAction>| {
            let allow_feedback = Arc::clone(&allow_feedback_for_handler);
            let observed_cancel = Arc::clone(&observed_cancel_for_handler);
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                allow_feedback.notified().await;
                ctx.publish_feedback(&DummyFeedbackDataMsg { value: "half" })
                    .await?;
                ctx.cancelled().await;
                observed_cancel.notify_waiters();
                allow_finish.notified().await;
                ctx.cancel(ActionReplyCode::Canceled, "client requested cancel")
                    .await
            }
        },
    )
    .await?;
    let client = ActionClient::<SdkFeedbackAction>::new(
        &client_node,
        SdkFeedbackAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let mut all_feedback = client.feedback_stream_all().await?;
    let handle = client
        .send_goal(&DummyGoalMsg { value: "cancel-me" })
        .await?;
    let mut goal_feedback = handle.feedback_stream().await?;

    allow_feedback.notify_one();

    let goal_sample = tokio::time::timeout(Duration::from_secs(5), goal_feedback.recv())
        .await
        .map_err(|_| AortaError::timeout("timed out waiting for goal feedback"))?
        .transpose()?
        .ok_or_else(|| AortaError::internal("goal feedback stream closed unexpectedly"))?;
    let goal_feedback_root = goal_sample.decode()?;
    assert_eq!(goal_feedback_root.goal_id, handle.goal_id());
    assert_eq!(goal_feedback_root.seq, 1);
    assert_eq!(goal_feedback_root.status, ActionStatus::Executing);
    assert_eq!(goal_feedback_root.value, "half");
    assert_eq!(goal_feedback_root.message, "");

    let all_sample = tokio::time::timeout(Duration::from_secs(5), all_feedback.recv())
        .await
        .map_err(|_| AortaError::timeout("timed out waiting for action feedback"))?
        .transpose()?
        .ok_or_else(|| AortaError::internal("action feedback stream closed unexpectedly"))?;
    let all_feedback_root = all_sample.decode()?;
    assert_eq!(all_feedback_root.goal_id, handle.goal_id());
    assert_eq!(all_feedback_root.value, "half");

    let observed = observed_cancel.notified();
    let cancel = handle.cancel().await?;
    assert!(cancel.accepted);
    assert_eq!(cancel.goal_id, handle.goal_id());
    assert_eq!(cancel.code, ActionReplyCode::Ok);
    assert_eq!(cancel.status, ActionStatus::Canceling);

    observed.await;
    allow_finish.notify_one();

    let terminal = handle.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(terminal.status, ActionStatus::Canceled);
    assert_eq!(terminal.code, ActionReplyCode::Canceled);
    assert!(terminal.result.is_none());
    assert_eq!(terminal.feedback_snapshot.value, "half");

    goal_feedback.close().await?;
    all_feedback.close().await?;
    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}

// ActionClient::new panics on a malformed action
// name; try_new must return the error instead — matching C++ (StatusOr) and
// Python (raises AortaError) for identical bad input.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_client_try_new_rejects_malformed_name() -> AortaResult<()> {
    let node = crate::Node::new(unique_node_name("sdk_try_new_bad"), "default").await?;
    let err = match ActionClient::<SdkRoundtripAction>::try_new(
        &node,
        "bad//name#",
        ActionClientOptions::default(),
    ) {
        Ok(_) => panic!("malformed action name must be rejected, not accepted"),
        Err(err) => err,
    };
    assert!(matches!(err, AortaError::InvalidArgument(_)), "got: {err}");
    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_client_try_new_accepts_valid_name() -> AortaResult<()> {
    let node = crate::Node::new(unique_node_name("sdk_try_new_ok"), "default").await?;
    let client = ActionClient::<SdkRoundtripAction>::try_new(
        &node,
        "test/sdk_try_new_ok",
        ActionClientOptions::default(),
    )?;
    assert_eq!(client.action_name(), "test/sdk_try_new_ok");
    node.close().await?;
    Ok(())
}

// Issues #341 + #256 at the SDK surface: a goal-scoped stream opened BEFORE
// send (pre-generated goal_id) sees feedback the server publishes as its
// first handler action, and ends itself (recv -> None) once the client
// observes the terminal result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_sdk_pre_send_feedback_stream_and_terminal_eos() -> AortaResult<()> {
    let server_node = Node::new(unique_node_name("sdk_action_early_eos_srv"), "sdk_test").await?;
    let client_node = Node::new(unique_node_name("sdk_action_early_eos_cli"), "sdk_test").await?;
    let allow_finish = Arc::new(tokio::sync::Notify::new());

    let allow_finish_for_handler = Arc::clone(&allow_finish);
    let server = ActionServer::<SdkEarlyEosAction>::serve(
        &server_node,
        SdkEarlyEosAction::ACTION_NAME,
        ActionServerOptions::default(),
        move |_goal, ctx: ActionExecutionContext<SdkEarlyEosAction>| {
            let allow_finish = Arc::clone(&allow_finish_for_handler);
            async move {
                // Zero-delay first action: the #341 race shape.
                ctx.publish_feedback(&DummyFeedbackDataMsg { value: "accepted" })
                    .await?;
                allow_finish.notified().await;
                ctx.succeed(&DummyResultMsg { value: "done" }).await
            }
        },
    )
    .await?;
    let client = ActionClient::<SdkEarlyEosAction>::new(
        &client_node,
        SdkEarlyEosAction::ACTION_NAME,
        ActionClientOptions::default(),
    );

    let goal_id = ActionClient::<SdkEarlyEosAction>::generate_goal_id();
    let mut stream = client.feedback_stream_for(&goal_id).await?;
    let handle = client
        .send_goal_with_id(&goal_id, &DummyGoalMsg { value: "run" })
        .await?;

    let sample = tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .map_err(|_| AortaError::timeout("early feedback lost: pre-send stream saw nothing"))?
        .transpose()?
        .ok_or_else(|| AortaError::not_found("stream closed before terminal"))?;
    let feedback = sample.decode()?;
    assert_eq!(feedback.goal_id, goal_id);
    assert_eq!(feedback.value, "accepted");

    allow_finish.notify_one();
    let result = handle.wait_result(Some(Duration::from_secs(5))).await?;
    assert_eq!(result.status, ActionStatus::Succeeded);

    let eos = tokio::time::timeout(Duration::from_secs(2), stream.recv()).await;
    assert!(
        matches!(eos, Ok(None)),
        "goal-scoped stream must end (recv -> None) after the terminal result"
    );

    server.stop_and_join(Duration::from_secs(1)).await;
    server_node.close().await?;
    client_node.close().await?;
    Ok(())
}
