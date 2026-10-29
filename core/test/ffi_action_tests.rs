// Lint exception: integration tests deliberately
// exercise the FFI extern "C" surface with locally-constructed arguments
// (CStr/CString builders, stack-allocated `aorta_*_t` structs, registry
// handles from the API under test). Per-block SAFETY comments would be
// dozens of copies of the same prose. The lib-root `warn`/`deny` keeps
// production code honest; integration-test files opt out as a class.
#![allow(clippy::undocumented_unsafe_blocks)]

use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;
use std::str;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use crate::action::{
    ActionCancelGoalResponse, ActionGetResultResponse, ActionInfo, ActionPublicStatus,
    ActionReplyCode, ActionSchema, ActionSendGoalResponse, ActionStatus, ActionWire,
};
use crate::action_server::{ActionServer, ActionServerOptions};
use crate::error::{AortaError, AortaResult};
use crate::ffi::c_api::{
    aorta_action_client_cancel, aorta_action_client_free, aorta_action_client_get_result,
    aorta_action_client_info, aorta_action_client_new_with_options, aorta_action_client_options_t,
    aorta_action_client_query_status, aorta_action_client_send_goal,
    aorta_action_client_send_goal_traced, aorta_action_client_subscribe_feedback,
    aorta_action_client_subscribe_feedback2, aorta_action_client_subscribe_result,
    aorta_action_client_t, aorta_action_context_cancel, aorta_action_context_free,
    aorta_action_context_publish_feedback, aorta_action_context_succeed,
    aorta_action_context_trace_context, aorta_action_feedback_sub2_dropped_count,
    aorta_action_feedback_sub2_free, aorta_action_feedback_sub_free, aorta_action_feedback_sub_t,
    aorta_action_result_sub_free, aorta_action_result_sub_t, aorta_action_result_view_t,
    aorta_action_server_callbacks_t, aorta_action_server_codec_t, aorta_action_server_free,
    aorta_action_server_info_new_with_options, aorta_action_server_new_with_callbacks,
    aorta_action_server_new_with_options, aorta_action_server_options_t, aorta_action_server_t,
    aorta_admission_t, aorta_budget_remaining_ns, aorta_bytes_slice_t, aorta_bytes_view_t,
    aorta_executor_add_node, aorta_executor_free, aorta_executor_new, aorta_executor_spin,
    aorta_executor_stop, aorta_executor_type_t, aorta_msg_context_t, aorta_node_free,
    aorta_node_new, aorta_node_root_scope, aorta_node_t, aorta_owned_bytes_free,
    aorta_owned_bytes_slice_t, aorta_owned_bytes_t, aorta_owned_bytes_view_t, aorta_result_t,
    aorta_schema_hash_view_t, aorta_scope_begin_task, aorta_scope_cancel,
    aorta_scope_cancelled_flag_ptr, aorta_scope_checkpoint, aorta_scope_child,
    aorta_scope_child_of_token, aorta_scope_close, aorta_scope_free, aorta_scope_is_cancelled,
    aorta_scope_opts_t, aorta_string_view_t, aorta_sub_t, aorta_subscriber_free,
    aorta_subscriber_new, aorta_subscriber_set_admission, aorta_task_finish_failed,
    aorta_token_cancel, aorta_token_child, aorta_token_free, aorta_trace_current_context,
    aorta_trace_monotonic_ns, aorta_trace_record_stage_at, aorta_trace_record_stage_under,
    aorta_watch_create, aorta_watch_free, aorta_watch_load, aorta_watch_store,
    aorta_watch_stored_total, AORTA_ACTION_STATUS_KIND_PUBLIC_STATUS,
    AORTA_CANCEL_REASON_CHILD_FAILED, AORTA_CANCEL_REASON_DEADLINE, AORTA_CANCEL_REASON_SHUTDOWN,
    AORTA_ERR_BUFFER_TOO_SMALL, AORTA_ERR_CANCELLED, AORTA_ERR_CONCURRENT_CONFLICT,
    AORTA_ERR_INVALID_ARGUMENT, AORTA_ERR_NOT_FOUND, AORTA_ERR_TIMEOUT, AORTA_FB_DROP_OLDEST,
    AORTA_OK,
};
use crate::node::Node;
use crate::schema::{SchemaMeta, TypedSubscribeMessage};

fn unique_name(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_p{}_{}", base, std::process::id(), id)
}

fn action_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn lock_action_test() -> MutexGuard<'static, ()> {
    action_test_lock()
        .lock()
        .unwrap_or_else(|err| err.into_inner())
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
                b"ffi-dummy-action-bfbs"
            }

            fn root_type() -> Option<&'static str> {
                Some($root)
            }
        }
    };
}

impl_dummy_schema!(DummyGoalSchema, "ffi.action.DummyGoal");
impl_dummy_schema!(DummyResultSchema, "ffi.action.DummyResult");
impl_dummy_schema!(DummyFeedbackDataSchema, "ffi.action.DummyFeedbackData");
impl_dummy_schema!(
    DummySendGoalRequestSchema,
    "ffi.action.DummySendGoalRequest"
);
impl_dummy_schema!(
    DummySendGoalResponseSchema,
    "ffi.action.DummySendGoalResponse"
);
impl_dummy_schema!(
    DummyCancelGoalRequestSchema,
    "ffi.action.DummyCancelGoalRequest"
);
impl_dummy_schema!(
    DummyCancelGoalResponseSchema,
    "ffi.action.DummyCancelGoalResponse"
);
impl_dummy_schema!(DummyFeedbackSchema, "ffi.action.DummyFeedback");
impl_dummy_schema!(DummyPublicStatusSchema, "ffi.action.DummyPublicStatus");
impl_dummy_schema!(
    DummyGetResultRequestSchema,
    "ffi.action.DummyGetResultRequest"
);
impl_dummy_schema!(
    DummyGetResultResponseSchema,
    "ffi.action.DummyGetResultResponse"
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

fn encode_cancel_goal_response(response: &ActionCancelGoalResponse) -> Vec<u8> {
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

fn decode_cancel_goal_response(payload: &[u8]) -> AortaResult<ActionCancelGoalResponse> {
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

struct DummyAction;

impl ActionSchema for DummyAction {
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

    const ACTION_NAME: &'static str = "ffi/test_action";
}

impl ActionWire for DummyAction {
    type GoalMsg<'a> = DummyGoalMsg<'a>;
    type OwnedFeedbackDataMsg = OwnedDummyFeedbackDataMsg;
    type OwnedResultMsg = OwnedDummyResultMsg;
    type ResultMsg<'a> = DummyResultMsg<'a>;
    type FeedbackDataMsg<'a> = DummyFeedbackDataMsg<'a>;

    fn encode_send_goal_request(goal_id: &str, goal: &Self::GoalMsg<'_>) -> AortaResult<Vec<u8>> {
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

    fn encode_send_goal_response(response: &ActionSendGoalResponse) -> AortaResult<Vec<u8>> {
        Ok(encode_send_goal_response(response))
    }

    fn decode_send_goal_response(payload: &[u8]) -> AortaResult<ActionSendGoalResponse> {
        decode_send_goal_response(payload)
    }

    fn encode_cancel_goal_request(goal_id: &str, goal_secret: &str) -> AortaResult<Vec<u8>> {
        Ok(format!("{}|{}", goal_id, goal_secret).into_bytes())
    }

    fn decode_cancel_goal_request(payload: &[u8]) -> AortaResult<(String, String)> {
        let text = str::from_utf8(payload)
            .map_err(|err| AortaError::invalid_argument(format!("dummy cancel utf8: {}", err)))?;
        let (goal_id, goal_secret) = text
            .split_once('|')
            .ok_or_else(|| AortaError::invalid_argument("dummy cancel missing delimiter"))?;
        Ok((goal_id.to_string(), goal_secret.to_string()))
    }

    fn encode_cancel_goal_response(response: &ActionCancelGoalResponse) -> AortaResult<Vec<u8>> {
        Ok(encode_cancel_goal_response(response))
    }

    fn decode_cancel_goal_response(payload: &[u8]) -> AortaResult<ActionCancelGoalResponse> {
        decode_cancel_goal_response(payload)
    }

    fn encode_get_result_request(goal_id: &str, goal_secret: &str) -> AortaResult<Vec<u8>> {
        Ok(format!("{}|{}", goal_id, goal_secret).into_bytes())
    }

    fn decode_get_result_request(payload: &[u8]) -> AortaResult<(String, String)> {
        Self::decode_cancel_goal_request(payload)
    }

    fn clone_feedback_data_owned(
        data: &Self::FeedbackDataMsg<'_>,
    ) -> AortaResult<Self::OwnedFeedbackDataMsg> {
        Ok(OwnedDummyFeedbackDataMsg {
            value: data.value.to_string(),
        })
    }

    fn clone_result_owned(result: &Self::ResultMsg<'_>) -> AortaResult<Self::OwnedResultMsg> {
        Ok(OwnedDummyResultMsg {
            value: result.value.to_string(),
        })
    }

    fn encode_get_result_response(
        response: &ActionGetResultResponse<Self::OwnedFeedbackDataMsg, Self::OwnedResultMsg>,
    ) -> AortaResult<Vec<u8>> {
        Ok(encode_get_result_response(response))
    }

    fn decode_get_result_response(
        payload: &[u8],
    ) -> AortaResult<ActionGetResultResponse<Self::OwnedFeedbackDataMsg, Self::OwnedResultMsg>>
    {
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
            message.unwrap_or("")
        )
        .into_bytes())
    }

    fn decode_feedback_goal_id(payload: &[u8]) -> AortaResult<String> {
        let text = str::from_utf8(payload)
            .map_err(|err| AortaError::invalid_argument(format!("dummy feedback utf8: {}", err)))?;
        text.split('|')
            .next()
            .map(|value| value.to_string())
            .ok_or_else(|| AortaError::invalid_argument("dummy feedback missing goal_id"))
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

fn view_from_bytes(bytes: &[u8], slice: &mut aorta_bytes_slice_t) -> aorta_bytes_view_t {
    *slice = aorta_bytes_slice_t {
        data: bytes.as_ptr(),
        len: bytes.len(),
    };
    aorta_bytes_view_t {
        slices: ptr::addr_of!(*slice),
        slice_count: 1,
        total_len: bytes.len(),
    }
}

fn owned_bytes_to_vec(bytes: &aorta_owned_bytes_t) -> Vec<u8> {
    if bytes.view.slice_count == 0 || bytes.view.slices.is_null() {
        return Vec::new();
    }
    let slices = unsafe { std::slice::from_raw_parts(bytes.view.slices, bytes.view.slice_count) };
    let mut out = Vec::with_capacity(bytes.view.total_len);
    for slice in slices {
        if slice.len == 0 || slice.data.is_null() {
            continue;
        }
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(slice.data, slice.len) });
    }
    out
}

fn bytes_view_to_vec(view: *const aorta_bytes_view_t) -> Vec<u8> {
    if view.is_null() {
        return Vec::new();
    }
    let view = unsafe { &*view };
    if view.slice_count == 0 || view.slices.is_null() {
        return Vec::new();
    }
    let slices = unsafe { std::slice::from_raw_parts(view.slices, view.slice_count) };
    let mut out = Vec::with_capacity(view.total_len);
    for slice in slices {
        if slice.len == 0 || slice.data.is_null() {
            continue;
        }
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(slice.data, slice.len) });
    }
    out
}

unsafe fn string_view_to_owned(view: aorta_string_view_t) -> String {
    if view.data.is_null() || view.len == 0 {
        return String::new();
    }
    std::str::from_utf8(std::slice::from_raw_parts(view.data, view.len))
        .expect("string_view_to_owned utf8")
        .to_string()
}

unsafe fn single_slice_view<'a>(view: *const aorta_bytes_view_t) -> &'a [u8] {
    let view = &*view;
    assert_eq!(view.slice_count, 1);
    assert!(!view.slices.is_null());
    let slice = &*view.slices;
    assert_eq!(slice.len, view.total_len);
    std::slice::from_raw_parts(slice.data, slice.len)
}

fn string_view_from_range(payload: &[u8], start: usize, len: usize) -> aorta_string_view_t {
    aorta_string_view_t {
        data: payload[start..].as_ptr(),
        len,
    }
}

struct CallbackOwnedBytes {
    payload: Vec<u8>,
    slice: aorta_owned_bytes_slice_t,
}

unsafe extern "C" fn release_callback_owned_bytes(ctx: *mut c_void, _data: *const u8, _len: usize) {
    if !ctx.is_null() {
        drop(Box::from_raw(ctx as *mut CallbackOwnedBytes));
    }
}

fn fill_owned_view(payload: Vec<u8>, out: *mut aorta_owned_bytes_view_t) -> aorta_result_t {
    if out.is_null() {
        return AORTA_ERR_INVALID_ARGUMENT;
    }
    let len = payload.len();
    let mut storage = Box::new(CallbackOwnedBytes {
        payload,
        slice: aorta_owned_bytes_slice_t::default(),
    });
    storage.slice = aorta_owned_bytes_slice_t {
        data: if len == 0 {
            ptr::null()
        } else {
            storage.payload.as_ptr()
        },
        len,
        release: Some(release_callback_owned_bytes),
        release_ctx: ptr::null_mut(),
    };
    let storage = Box::into_raw(storage);
    unsafe {
        (*storage).slice.release_ctx = storage as *mut c_void;
        *out = aorta_owned_bytes_view_t {
            slices: if len == 0 {
                ptr::null()
            } else {
                ptr::addr_of!((*storage).slice)
            },
            slice_count: if len == 0 { 0 } else { 1 },
            total_len: len,
        };
    }
    AORTA_OK
}

unsafe extern "C" fn ffi_decode_send_goal_request_goal_id(
    request: *const aorta_bytes_view_t,
    out_goal_id: *mut aorta_string_view_t,
) -> i32 {
    if request.is_null() || out_goal_id.is_null() {
        return AORTA_ERR_INVALID_ARGUMENT;
    }
    let payload = single_slice_view(request);
    let Some(goal_id_len) = payload.iter().position(|byte| *byte == b'\n') else {
        return AORTA_ERR_INVALID_ARGUMENT;
    };
    *out_goal_id = string_view_from_range(payload, 0, goal_id_len);
    AORTA_OK
}

unsafe fn decode_goal_secret_views(
    request: *const aorta_bytes_view_t,
    out_goal_id: *mut aorta_string_view_t,
    out_goal_secret: *mut aorta_string_view_t,
) -> i32 {
    if request.is_null() || out_goal_id.is_null() || out_goal_secret.is_null() {
        return AORTA_ERR_INVALID_ARGUMENT;
    }
    let payload = single_slice_view(request);
    let Some(goal_id_len) = payload.iter().position(|byte| *byte == b'|') else {
        return AORTA_ERR_INVALID_ARGUMENT;
    };
    *out_goal_id = string_view_from_range(payload, 0, goal_id_len);
    *out_goal_secret = string_view_from_range(
        payload,
        goal_id_len + 1,
        payload.len().saturating_sub(goal_id_len + 1),
    );
    AORTA_OK
}

unsafe extern "C" fn ffi_decode_cancel_goal_request(
    request: *const aorta_bytes_view_t,
    out_goal_id: *mut aorta_string_view_t,
    out_goal_secret: *mut aorta_string_view_t,
) -> i32 {
    decode_goal_secret_views(request, out_goal_id, out_goal_secret)
}

unsafe extern "C" fn ffi_decode_get_result_request(
    request: *const aorta_bytes_view_t,
    out_goal_id: *mut aorta_string_view_t,
    out_goal_secret: *mut aorta_string_view_t,
) -> i32 {
    decode_goal_secret_views(request, out_goal_id, out_goal_secret)
}

unsafe extern "C" fn ffi_encode_send_goal_response(
    accepted: u8,
    goal_id: aorta_string_view_t,
    goal_secret: aorta_string_view_t,
    code: i16,
    status: i8,
    message: aorta_string_view_t,
    server_epoch: aorta_string_view_t,
    out: *mut aorta_owned_bytes_view_t,
) -> i32 {
    let response = ActionSendGoalResponse {
        accepted: accepted != 0,
        goal_id: string_view_to_owned(goal_id),
        goal_secret: string_view_to_owned(goal_secret),
        code: ActionReplyCode::try_from(code).expect("send_goal response code"),
        status: ActionStatus::try_from(status).expect("send_goal response status"),
        message: string_view_to_owned(message),
        server_epoch: string_view_to_owned(server_epoch),
    };
    fill_owned_view(
        DummyAction::encode_send_goal_response(&response).expect("encode"),
        out,
    )
}

unsafe extern "C" fn ffi_encode_cancel_goal_response(
    accepted: u8,
    goal_id: aorta_string_view_t,
    code: i16,
    status: i8,
    message: aorta_string_view_t,
    out: *mut aorta_owned_bytes_view_t,
) -> i32 {
    let response = ActionCancelGoalResponse {
        accepted: accepted != 0,
        goal_id: string_view_to_owned(goal_id),
        code: ActionReplyCode::try_from(code).expect("cancel response code"),
        status: ActionStatus::try_from(status).expect("cancel response status"),
        message: string_view_to_owned(message),
    };
    fill_owned_view(
        DummyAction::encode_cancel_goal_response(&response).expect("encode"),
        out,
    )
}

unsafe extern "C" fn ffi_encode_public_status(
    goal_id: aorta_string_view_t,
    status: i8,
    code: i16,
    accepted_stamp_ns: u64,
    started_stamp_ns: u64,
    finished_stamp_ns: u64,
    last_feedback_seq: u64,
    server_epoch: aorta_string_view_t,
    out: *mut aorta_owned_bytes_view_t,
) -> i32 {
    let response = ActionPublicStatus {
        goal_id: string_view_to_owned(goal_id),
        status: ActionStatus::try_from(status).expect("public status"),
        code: ActionReplyCode::try_from(code).expect("public status code"),
        accepted_stamp_ns,
        started_stamp_ns,
        finished_stamp_ns,
        last_feedback_seq,
        server_epoch: string_view_to_owned(server_epoch),
    };
    fill_owned_view(
        DummyAction::encode_public_status(&response).expect("encode"),
        out,
    )
}

unsafe extern "C" fn ffi_encode_feedback(
    goal_id: aorta_string_view_t,
    seq: u64,
    stamp_ns: u64,
    status: i8,
    feedback_data: *const aorta_bytes_view_t,
    message: aorta_string_view_t,
    out: *mut aorta_owned_bytes_view_t,
) -> i32 {
    let payload = bytes_view_to_vec(feedback_data);
    let response = DummyAction::encode_feedback(
        &string_view_to_owned(goal_id),
        seq,
        stamp_ns,
        ActionStatus::try_from(status).expect("feedback status"),
        &DummyFeedbackDataMsg {
            value: str::from_utf8(&payload).expect("feedback utf8"),
        },
        Some(&string_view_to_owned(message)),
    )
    .expect("encode");
    fill_owned_view(response, out)
}

unsafe extern "C" fn ffi_encode_get_result_response(
    goal_id: aorta_string_view_t,
    status: i8,
    accepted_stamp_ns: u64,
    started_stamp_ns: u64,
    finished_stamp_ns: u64,
    last_feedback_seq: u64,
    feedback_snapshot: *const aorta_bytes_view_t,
    result_payload: *const aorta_bytes_view_t,
    code: i16,
    message: aorta_string_view_t,
    server_epoch: aorta_string_view_t,
    out: *mut aorta_owned_bytes_view_t,
) -> i32 {
    let feedback_payload = bytes_view_to_vec(feedback_snapshot);
    let result_payload = bytes_view_to_vec(result_payload);
    let response = ActionGetResultResponse {
        goal_id: string_view_to_owned(goal_id),
        status: ActionStatus::try_from(status).expect("get_result status"),
        accepted_stamp_ns,
        started_stamp_ns,
        finished_stamp_ns,
        last_feedback_seq,
        feedback_snapshot: OwnedDummyFeedbackDataMsg {
            value: str::from_utf8(&feedback_payload)
                .expect("feedback snapshot utf8")
                .to_string(),
        },
        result: if result_payload.is_empty() {
            None
        } else {
            Some(OwnedDummyResultMsg {
                value: str::from_utf8(&result_payload)
                    .expect("result utf8")
                    .to_string(),
            })
        },
        code: ActionReplyCode::try_from(code).expect("get_result code"),
        message: string_view_to_owned(message),
        server_epoch: string_view_to_owned(server_epoch),
    };
    fill_owned_view(
        DummyAction::encode_get_result_response(&response).expect("encode"),
        out,
    )
}

unsafe extern "C" fn ffi_execute_action(
    request: *const aorta_bytes_view_t,
    ctx: crate::ffi::c_api::aorta_action_context_t,
    _user_data: *mut c_void,
) {
    let payload = bytes_view_to_vec(request);
    let goal = DummyAction::decode_goal_from_send_goal_request(&payload).expect("decode goal");
    assert_eq!(goal.value, "run");

    let feedback = DummyAction::encode_feedback_data(&DummyFeedbackDataMsg { value: "progress" })
        .expect("encode feedback data");
    let mut feedback_slice = aorta_bytes_slice_t::default();
    let feedback_view = view_from_bytes(&feedback, &mut feedback_slice);
    let message = CString::new("working").unwrap();
    let rc =
        aorta_action_context_publish_feedback(ctx, ptr::addr_of!(feedback_view), message.as_ptr());
    assert_eq!(rc, AORTA_OK);

    let result = DummyAction::encode_result(&DummyResultMsg { value: "done" }).expect("result");
    let mut result_slice = aorta_bytes_slice_t::default();
    let result_view = view_from_bytes(&result, &mut result_slice);
    let rc = aorta_action_context_succeed(ctx, ptr::addr_of!(result_view));
    assert_eq!(rc, AORTA_OK);

    let rc = aorta_action_context_free(ctx);
    assert_eq!(rc, AORTA_OK);
}

extern "C" fn feedback_callback(
    payload: *const aorta_bytes_view_t,
    _schema: *const aorta_schema_hash_view_t,
    _ctx: *const aorta_msg_context_t,
    user_data: *mut c_void,
) {
    if user_data.is_null() {
        return;
    }
    let sender = unsafe { &*(user_data as *const mpsc::Sender<Vec<u8>>) };
    let _ = sender.send(bytes_view_to_vec(payload));
}

#[test]
fn ffi_action_server_info_only_advertises_info_and_enforces_single_provider() {
    let _action_guard = lock_action_test();
    let group_name = unique_name("ffi_action_group");
    let group = CString::new(group_name.clone()).unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();

    let server1_name = CString::new(unique_name("ffi_action_info_server1")).unwrap();
    let mut server1_node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(server1_name.as_ptr(), group.as_ptr(), &mut server1_node) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(server1_node, 0);

    let server_opts = aorta_action_server_options_t {
        supports_cancel: 0,
        max_concurrency: 3,
        result_ttl_ms: 1234,
        dedup_ttl_ms: 5678,
    };
    let mut server1: aorta_action_server_t = 0;
    let rc = unsafe {
        aorta_action_server_info_new_with_options(
            server1_node,
            action_name.as_ptr(),
            DummyAction::schema_bfbs().as_ptr(),
            DummyAction::schema_bfbs().len(),
            ptr::addr_of!(server_opts),
            &mut server1,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(server1, 0);

    let client_name = CString::new(unique_name("ffi_action_info_client")).unwrap();
    let mut client_node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(client_name.as_ptr(), group.as_ptr(), &mut client_node) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(client_node, 0);

    let client_opts = aorta_action_client_options_t {
        control_timeout_ms: 3000,
        info_timeout_ms: 1000,
    };
    let mut client: aorta_action_client_t = 0;
    let rc = unsafe {
        aorta_action_client_new_with_options(
            client_node,
            action_name.as_ptr(),
            ptr::addr_of!(client_opts),
            &mut client,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(client, 0);

    let mut info_bytes = aorta_owned_bytes_t::default();
    let mut rc = AORTA_ERR_NOT_FOUND;
    for _ in 0..20 {
        rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
        if rc == AORTA_OK {
            break;
        }
        assert_eq!(rc, AORTA_ERR_NOT_FOUND);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(rc, AORTA_OK);

    let info = ActionInfo::decode(&owned_bytes_to_vec(&info_bytes)).expect("decode info");
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };
    assert_eq!(info.action_name, DummyAction::ACTION_NAME);
    assert!(!info.supports_cancel);
    assert_eq!(info.max_concurrency, 3);
    assert_eq!(info.result_ttl_ms, 1234);
    assert_eq!(info.dedup_ttl_ms, 5678);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let reader = Node::new(
            unique_name("ffi_action_schema_reader"),
            group.to_str().unwrap(),
        )
        .await
        .expect("schema reader");
        let bfbs = crate::action_server::fetch_action_schema_bfbs(
            reader.session().inner(),
            reader.key_builder(),
            &info,
            Duration::from_secs(1),
        )
        .await
        .expect("BFBS on the FFI action info key");
        assert_eq!(bfbs, DummyAction::schema_bfbs());
    });

    let server2_name = CString::new(unique_name("ffi_action_info_server2")).unwrap();
    let mut server2_node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(server2_name.as_ptr(), group.as_ptr(), &mut server2_node) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(server2_node, 0);

    let mut conflicting_server: aorta_action_server_t = 0;
    let rc = unsafe {
        aorta_action_server_info_new_with_options(
            server2_node,
            action_name.as_ptr(),
            DummyAction::schema_bfbs().as_ptr(),
            DummyAction::schema_bfbs().len(),
            ptr::addr_of!(server_opts),
            &mut conflicting_server,
        )
    };
    assert_eq!(rc, AORTA_ERR_CONCURRENT_CONFLICT);
    assert_eq!(conflicting_server, 0);

    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(client_node), AORTA_OK);
    assert_eq!(aorta_action_server_free(server1), AORTA_OK);

    let mut server2: aorta_action_server_t = 0;
    let rc = unsafe {
        aorta_action_server_info_new_with_options(
            server2_node,
            action_name.as_ptr(),
            DummyAction::schema_bfbs().as_ptr(),
            DummyAction::schema_bfbs().len(),
            ptr::addr_of!(server_opts),
            &mut server2,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(server2, 0);

    assert_eq!(aorta_action_server_free(server2), AORTA_OK);
    assert_eq!(aorta_node_free(server2_node), AORTA_OK);
    assert_eq!(aorta_node_free(server1_node), AORTA_OK);
}

#[test]
fn ffi_action_client_roundtrip() {
    let _action_guard = lock_action_test();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let (server_node, server) = runtime.block_on(async {
        let server_node = Node::new(unique_name("ffi_action_server"), "default")
            .await
            .expect("server node");
        let server = ActionServer::<DummyAction>::serve_with_handler(
            &server_node,
            DummyAction::ACTION_NAME,
            ActionServerOptions::default(),
            |_goal, ctx| async move { ctx.succeed(&DummyResultMsg { value: "done" }).await },
        )
        .await
        .expect("serve action");
        (server_node, server)
    });

    let node_name = CString::new(unique_name("ffi_action_client")).unwrap();
    let group = CString::new("default").unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();
    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(node, 0);

    let opts = aorta_action_client_options_t {
        control_timeout_ms: 3000,
        info_timeout_ms: 1000,
    };
    let mut client: aorta_action_client_t = 0;
    let rc = unsafe {
        aorta_action_client_new_with_options(
            node,
            action_name.as_ptr(),
            ptr::addr_of!(opts),
            &mut client,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(client, 0);

    let mut info_bytes = aorta_owned_bytes_t::default();
    let mut rc = AORTA_ERR_NOT_FOUND;
    for _ in 0..20 {
        rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
        if rc == AORTA_OK {
            break;
        }
        assert_eq!(rc, AORTA_ERR_NOT_FOUND);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(rc, AORTA_OK);
    let info = ActionInfo::decode(&owned_bytes_to_vec(&info_bytes)).expect("decode info");
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };
    assert_eq!(info.action_name, DummyAction::ACTION_NAME);

    let goal_id = "550e8400-e29b-41d4-a716-446655440000";
    let request = DummyAction::encode_send_goal_request(goal_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request_slice = aorta_bytes_slice_t::default();
    let request_view = view_from_bytes(&request, &mut request_slice);
    let mut send_goal_bytes = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_action_client_send_goal(
            client,
            ptr::addr_of!(request_view),
            ptr::addr_of_mut!(send_goal_bytes),
        )
    };
    assert_eq!(rc, AORTA_OK);
    let send_goal = decode_send_goal_response(&owned_bytes_to_vec(&send_goal_bytes))
        .expect("decode send_goal response");
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(send_goal_bytes)) };
    assert!(send_goal.accepted);
    assert_eq!(send_goal.goal_id, goal_id);
    assert!(!send_goal.goal_secret.is_empty());

    let goal_id_c = CString::new(goal_id).unwrap();
    let mut status_bytes = aorta_owned_bytes_t::default();
    let mut seen_terminal = false;
    for _ in 0..20 {
        let rc = unsafe {
            aorta_action_client_query_status(
                client,
                goal_id_c.as_ptr(),
                ptr::addr_of_mut!(status_bytes),
            )
        };
        if rc == AORTA_ERR_NOT_FOUND {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        }
        assert_eq!(rc, AORTA_OK);
        let status =
            decode_public_status(&owned_bytes_to_vec(&status_bytes)).expect("decode public status");
        unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(status_bytes)) };
        if status.status.is_terminal() {
            seen_terminal = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(seen_terminal);

    let get_result_request =
        DummyAction::encode_get_result_request(goal_id, &send_goal.goal_secret)
            .expect("encode get_result request");
    let mut result_request_slice = aorta_bytes_slice_t::default();
    let result_request_view = view_from_bytes(&get_result_request, &mut result_request_slice);
    let mut result_bytes = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_action_client_get_result(
            client,
            ptr::addr_of!(result_request_view),
            ptr::addr_of_mut!(result_bytes),
        )
    };
    assert_eq!(rc, AORTA_OK);
    let result = decode_get_result_response(&owned_bytes_to_vec(&result_bytes))
        .expect("decode get_result response");
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(result_bytes)) };
    assert_eq!(result.goal_id, goal_id);
    assert_eq!(result.status, ActionStatus::Succeeded);
    assert_eq!(
        result.result.as_ref().map(|value| value.value.as_str()),
        Some("done")
    );

    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    runtime.block_on(async {
        server.stop_and_join(Duration::from_secs(1)).await;
        server_node.close().await.expect("close server node");
    });
}

#[test]
fn ffi_action_feedback_subscription_receives_raw_payloads() {
    let _action_guard = lock_action_test();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let (server_node, server) = runtime.block_on(async {
        let server_node = Node::new(unique_name("ffi_action_feedback_server"), "default")
            .await
            .expect("server node");
        let server = ActionServer::<DummyAction>::serve_with_handler(
            &server_node,
            DummyAction::ACTION_NAME,
            ActionServerOptions::default(),
            |_goal, ctx| async move {
                ctx.publish_feedback(&DummyFeedbackDataMsg { value: "progress" })
                    .await?;
                ctx.succeed(&DummyResultMsg { value: "done" }).await
            },
        )
        .await
        .expect("serve action");
        (server_node, server)
    });

    let node_name = CString::new(unique_name("ffi_action_feedback_client")).unwrap();
    let group = CString::new("default").unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();
    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(node, 0);

    let opts = aorta_action_client_options_t {
        control_timeout_ms: 3000,
        info_timeout_ms: 1000,
    };
    let mut client: aorta_action_client_t = 0;
    let rc = unsafe {
        aorta_action_client_new_with_options(
            node,
            action_name.as_ptr(),
            ptr::addr_of!(opts),
            &mut client,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(client, 0);

    let mut info_bytes = aorta_owned_bytes_t::default();
    let mut rc = AORTA_ERR_NOT_FOUND;
    for _ in 0..20 {
        rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
        if rc == AORTA_OK {
            break;
        }
        assert_eq!(rc, AORTA_ERR_NOT_FOUND);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let sender_ptr = Box::into_raw(Box::new(tx));
    let mut feedback_sub: aorta_action_feedback_sub_t = 0;
    let rc = unsafe {
        aorta_action_client_subscribe_feedback(
            client,
            Some(feedback_callback),
            sender_ptr as *mut c_void,
            &mut feedback_sub,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(feedback_sub, 0);

    let goal_id = "550e8400-e29b-41d4-a716-446655440000";
    let request = DummyAction::encode_send_goal_request(goal_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request_slice = aorta_bytes_slice_t::default();
    let request_view = view_from_bytes(&request, &mut request_slice);
    let mut send_goal_bytes = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_action_client_send_goal(
            client,
            ptr::addr_of!(request_view),
            ptr::addr_of_mut!(send_goal_bytes),
        )
    };
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(send_goal_bytes)) };

    let feedback_payload = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive action feedback");
    let feedback = DummyFeedbackSchema::decode_typed_root(&feedback_payload).expect("decode");
    assert_eq!(feedback.goal_id, goal_id);
    assert_eq!(feedback.status, ActionStatus::Executing);
    assert_eq!(feedback.value, "progress");

    assert_eq!(aorta_action_feedback_sub_free(feedback_sub), AORTA_OK);
    unsafe {
        drop(Box::from_raw(sender_ptr));
    }
    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    runtime.block_on(async {
        server.stop_and_join(Duration::from_secs(1)).await;
        server_node.close().await.expect("close server node");
    });
}

#[test]
fn ffi_raw_action_server_roundtrip() {
    let _action_guard = lock_action_test();
    let group_name = unique_name("ffi_raw_action_group");
    let group = CString::new(group_name.clone()).unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();

    let server_name = CString::new(unique_name("ffi_raw_action_server")).unwrap();
    let mut server_node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(server_name.as_ptr(), group.as_ptr(), &mut server_node) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(server_node, 0);

    let server_opts = aorta_action_server_options_t {
        supports_cancel: 1,
        max_concurrency: 4,
        result_ttl_ms: 1500,
        dedup_ttl_ms: 3000,
    };
    let codec = aorta_action_server_codec_t {
        decode_send_goal_request_goal_id: Some(
            ffi_decode_send_goal_request_goal_id as unsafe extern "C" fn(_, _) -> _,
        ),
        decode_cancel_goal_request: Some(
            ffi_decode_cancel_goal_request as unsafe extern "C" fn(_, _, _) -> _,
        ),
        decode_get_result_request: Some(
            ffi_decode_get_result_request as unsafe extern "C" fn(_, _, _) -> _,
        ),
        encode_send_goal_response: Some(
            ffi_encode_send_goal_response as unsafe extern "C" fn(_, _, _, _, _, _, _, _) -> _,
        ),
        encode_cancel_goal_response: Some(
            ffi_encode_cancel_goal_response as unsafe extern "C" fn(_, _, _, _, _, _) -> _,
        ),
        encode_public_status: Some(
            ffi_encode_public_status as unsafe extern "C" fn(_, _, _, _, _, _, _, _, _) -> _,
        ),
        encode_feedback: Some(
            ffi_encode_feedback as unsafe extern "C" fn(_, _, _, _, _, _, _) -> _,
        ),
        encode_get_result_response: Some(
            ffi_encode_get_result_response
                as unsafe extern "C" fn(_, _, _, _, _, _, _, _, _, _, _, _) -> _,
        ),
    };
    let mut server: aorta_action_server_t = 0;
    let rc = unsafe {
        aorta_action_server_new_with_options(
            server_node,
            action_name.as_ptr(),
            DummyAction::schema_bfbs().as_ptr(),
            DummyAction::schema_bfbs().len(),
            ptr::addr_of!(server_opts),
            ptr::addr_of!(codec),
            Some(ffi_execute_action as unsafe extern "C" fn(_, _, _)),
            ptr::null_mut(),
            &mut server,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(server, 0);

    let client_name = CString::new(unique_name("ffi_raw_action_client")).unwrap();
    let mut client_node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(client_name.as_ptr(), group.as_ptr(), &mut client_node) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(client_node, 0);

    let client_opts = aorta_action_client_options_t {
        control_timeout_ms: 3000,
        info_timeout_ms: 1000,
    };
    let mut client: aorta_action_client_t = 0;
    let rc = unsafe {
        aorta_action_client_new_with_options(
            client_node,
            action_name.as_ptr(),
            ptr::addr_of!(client_opts),
            &mut client,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(client, 0);

    let mut info_bytes = aorta_owned_bytes_t::default();
    let mut rc = AORTA_ERR_NOT_FOUND;
    for _ in 0..20 {
        rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
        if rc == AORTA_OK {
            break;
        }
        assert_eq!(rc, AORTA_ERR_NOT_FOUND);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(rc, AORTA_OK);
    let info = ActionInfo::decode(&owned_bytes_to_vec(&info_bytes)).expect("decode info");
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };
    assert_eq!(info.action_name, DummyAction::ACTION_NAME);
    assert!(info.supports_cancel);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let reader = Node::new(
            unique_name("ffi_action_schema_reader"),
            group.to_str().unwrap(),
        )
        .await
        .expect("schema reader");
        let bfbs = crate::action_server::fetch_action_schema_bfbs(
            reader.session().inner(),
            reader.key_builder(),
            &info,
            Duration::from_secs(1),
        )
        .await
        .expect("BFBS on the FFI action info key");
        assert_eq!(bfbs, DummyAction::schema_bfbs());
    });

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let sender_ptr = Box::into_raw(Box::new(tx));
    let mut feedback_sub: aorta_action_feedback_sub_t = 0;
    let rc = unsafe {
        aorta_action_client_subscribe_feedback(
            client,
            Some(feedback_callback),
            sender_ptr as *mut c_void,
            &mut feedback_sub,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(feedback_sub, 0);

    let goal_id = "550e8400-e29b-41d4-a716-446655440123";
    let request = DummyAction::encode_send_goal_request(goal_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request_slice = aorta_bytes_slice_t::default();
    let request_view = view_from_bytes(&request, &mut request_slice);
    let mut send_goal_bytes = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_action_client_send_goal(
            client,
            ptr::addr_of!(request_view),
            ptr::addr_of_mut!(send_goal_bytes),
        )
    };
    assert_eq!(rc, AORTA_OK);
    let send_goal = decode_send_goal_response(&owned_bytes_to_vec(&send_goal_bytes))
        .expect("decode send_goal response");
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(send_goal_bytes)) };
    assert!(send_goal.accepted);
    assert_eq!(send_goal.goal_id, goal_id);
    assert!(!send_goal.goal_secret.is_empty());

    let feedback_payload = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive action feedback");
    let feedback = DummyFeedbackSchema::decode_typed_root(&feedback_payload).expect("decode");
    assert_eq!(feedback.goal_id, goal_id);
    assert_eq!(feedback.status, ActionStatus::Executing);
    assert_eq!(feedback.value, "progress");
    assert_eq!(feedback.message, "working");

    let goal_id_c = CString::new(goal_id).unwrap();
    let mut status_bytes = aorta_owned_bytes_t::default();
    let mut terminal_status = None;
    for _ in 0..20 {
        let rc = unsafe {
            aorta_action_client_query_status(
                client,
                goal_id_c.as_ptr(),
                ptr::addr_of_mut!(status_bytes),
            )
        };
        if rc == AORTA_ERR_NOT_FOUND {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        }
        assert_eq!(rc, AORTA_OK);
        let status =
            decode_public_status(&owned_bytes_to_vec(&status_bytes)).expect("decode status");
        unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(status_bytes)) };
        if status.status.is_terminal() {
            terminal_status = Some(status);
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let status = terminal_status.expect("terminal status");
    assert_eq!(status.goal_id, goal_id);
    assert_eq!(status.status, ActionStatus::Succeeded);

    let get_result_request =
        DummyAction::encode_get_result_request(goal_id, &send_goal.goal_secret)
            .expect("encode get_result request");
    let mut result_request_slice = aorta_bytes_slice_t::default();
    let result_request_view = view_from_bytes(&get_result_request, &mut result_request_slice);
    let mut result_bytes = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_action_client_get_result(
            client,
            ptr::addr_of!(result_request_view),
            ptr::addr_of_mut!(result_bytes),
        )
    };
    assert_eq!(rc, AORTA_OK);
    let result = decode_get_result_response(&owned_bytes_to_vec(&result_bytes))
        .expect("decode get_result response");
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(result_bytes)) };
    assert_eq!(result.goal_id, goal_id);
    assert_eq!(result.status, ActionStatus::Succeeded);
    assert_eq!(result.feedback_snapshot.value, "progress");
    assert_eq!(
        result.result.as_ref().map(|value| value.value.as_str()),
        Some("done")
    );

    let cancel_request = DummyAction::encode_cancel_goal_request(goal_id, &send_goal.goal_secret)
        .expect("encode cancel request");
    let mut cancel_request_slice = aorta_bytes_slice_t::default();
    let cancel_request_view = view_from_bytes(&cancel_request, &mut cancel_request_slice);
    let mut cancel_bytes = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_action_client_cancel(
            client,
            ptr::addr_of!(cancel_request_view),
            ptr::addr_of_mut!(cancel_bytes),
        )
    };
    assert_eq!(rc, AORTA_OK);
    let cancel =
        decode_cancel_goal_response(&owned_bytes_to_vec(&cancel_bytes)).expect("decode cancel");
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(cancel_bytes)) };
    assert!(!cancel.accepted);
    assert_eq!(cancel.code, ActionReplyCode::AlreadyTerminal);
    assert_eq!(cancel.status, ActionStatus::Succeeded);

    assert_eq!(aorta_action_feedback_sub_free(feedback_sub), AORTA_OK);
    unsafe {
        drop(Box::from_raw(sender_ptr));
    }
    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(client_node), AORTA_OK);
    assert_eq!(aorta_action_server_free(server), AORTA_OK);
    assert_eq!(aorta_node_free(server_node), AORTA_OK);
}

/// A held `send_goal` query must surface as `AORTA_ERR_TIMEOUT` whichever
/// dual-guard side fires first: zenoh's synthesized "Timeout" reply error must
/// not map to `AORTA_ERR_INTERNAL`. C-ABI mirror of
/// `send_goal_retries_past_zenoh_synthesized_timeout_reply` in
/// `action_server_tests.rs` — C++/Python action calls go through this path.
#[test]
fn ffi_action_client_send_goal_maps_zenoh_timeout_to_timeout() {
    let _action_guard = lock_action_test();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let action_name_str = unique_name("test/ffi_action_ztimeout");
    let (holder_node, holder) = runtime.block_on(async {
        let node = Node::new(unique_name("ffi_action_ztimeout_srv"), "default")
            .await
            .expect("holder node");
        let key: String = node
            .key_builder()
            .action_send_goal_key(&action_name_str)
            .expect("send_goal key")
            .into();
        let queryable = node
            .session()
            .inner()
            .declare_queryable(key)
            .await
            .expect("queryable");
        // Hold every query unanswered so the client's dual-guard timeout is
        // the only way the reply channel resolves.
        let holder = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok(query) = queryable.recv_async().await {
                held.push(query);
            }
        });
        (node, holder)
    });

    let node_name = CString::new(unique_name("ffi_action_ztimeout_cli")).unwrap();
    let group = CString::new("default").unwrap();
    let action_name = CString::new(action_name_str.clone()).unwrap();
    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) };
    assert_eq!(rc, AORTA_OK);

    let opts = aorta_action_client_options_t {
        control_timeout_ms: 500,
        info_timeout_ms: 1000,
    };
    let mut client: aorta_action_client_t = 0;
    let rc = unsafe {
        aorta_action_client_new_with_options(
            node,
            action_name.as_ptr(),
            ptr::addr_of!(opts),
            &mut client,
        )
    };
    assert_eq!(rc, AORTA_OK);

    let request = DummyAction::encode_send_goal_request(
        "550e8400-e29b-41d4-a716-446655440042",
        &DummyGoalMsg { value: "hold-me" },
    )
    .expect("encode send_goal request");
    let mut request_slice = aorta_bytes_slice_t::default();
    let request_view = view_from_bytes(&request, &mut request_slice);
    let mut send_goal_bytes = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_action_client_send_goal(
            client,
            ptr::addr_of!(request_view),
            ptr::addr_of_mut!(send_goal_bytes),
        )
    };
    assert_eq!(
        rc, AORTA_ERR_TIMEOUT,
        "held send_goal must surface as timeout, not a provider fault"
    );

    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    holder.abort();
    runtime.block_on(async {
        holder_node.close().await.expect("close holder node");
    });
}

// ---- action FFI tracing (the C++/Python SDK path) ----

static TRACED_EXEC_ADOPTED: AtomicU64 = AtomicU64::new(0);
static TRACED_EXEC_ADOPTED_SPAN: AtomicU64 = AtomicU64::new(0);
static TRACED_EXEC_GOAL_CTX_SPAN: AtomicU64 = AtomicU64::new(0);

/// Like `ffi_execute_action`, additionally capturing what the C++ SDK would
/// observe inside the execute callback: the ADOPTED thread context (so
/// AORTA_TRACE_SPAN spans nest) and the goal's trace accessor.
unsafe extern "C" fn ffi_execute_action_traced(
    request: *const aorta_bytes_view_t,
    ctx: crate::ffi::c_api::aorta_action_context_t,
    _user_data: *mut c_void,
) {
    let mut adopted_trace = [0u8; 16];
    let mut adopted_span: u64 = 0;
    let adopted = aorta_trace_current_context(adopted_trace.as_mut_ptr(), &mut adopted_span);
    TRACED_EXEC_ADOPTED.store(u64::from(adopted), Ordering::SeqCst);
    TRACED_EXEC_ADOPTED_SPAN.store(adopted_span, Ordering::SeqCst);

    let mut goal_trace = [0u8; 16];
    let mut goal_span: u64 = 0;
    let got = aorta_action_context_trace_context(ctx, goal_trace.as_mut_ptr(), &mut goal_span);
    TRACED_EXEC_GOAL_CTX_SPAN.store(if got == 1 { goal_span } else { 0 }, Ordering::SeqCst);

    ffi_execute_action(request, ctx, _user_data);
}

/// The full trace-propagation contract over the FFI action path (what the C++ SDK drives):
/// send_goal_traced returns the minted goal_sent identity; the server's goal
/// lifecycle spans (root, execution_started, user_callback, feedback_pub,
/// the adopted execute context) ride the SAME trace, one cross-process link
/// to the caller; the feedback receive marker parents to the publish span;
/// and the SDK-side stage recorders (goal_sent / result_wait) join the trace.
#[test]
fn ffi_action_goal_lifecycle_joins_the_callers_trace() {
    let _action_guard = lock_action_test();
    let group_name = unique_name("ffi_traced_action_group");
    let group = CString::new(group_name.clone()).unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();

    let server_name = CString::new(unique_name("ffi_traced_action_server")).unwrap();
    let mut server_node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(server_name.as_ptr(), group.as_ptr(), &mut server_node) };
    assert_eq!(rc, AORTA_OK);

    let server_opts = aorta_action_server_options_t {
        supports_cancel: 1,
        max_concurrency: 4,
        result_ttl_ms: 1500,
        dedup_ttl_ms: 3000,
    };
    let codec = aorta_action_server_codec_t {
        decode_send_goal_request_goal_id: Some(
            ffi_decode_send_goal_request_goal_id as unsafe extern "C" fn(_, _) -> _,
        ),
        decode_cancel_goal_request: Some(
            ffi_decode_cancel_goal_request as unsafe extern "C" fn(_, _, _) -> _,
        ),
        decode_get_result_request: Some(
            ffi_decode_get_result_request as unsafe extern "C" fn(_, _, _) -> _,
        ),
        encode_send_goal_response: Some(
            ffi_encode_send_goal_response as unsafe extern "C" fn(_, _, _, _, _, _, _, _) -> _,
        ),
        encode_cancel_goal_response: Some(
            ffi_encode_cancel_goal_response as unsafe extern "C" fn(_, _, _, _, _, _) -> _,
        ),
        encode_public_status: Some(
            ffi_encode_public_status as unsafe extern "C" fn(_, _, _, _, _, _, _, _, _) -> _,
        ),
        encode_feedback: Some(
            ffi_encode_feedback as unsafe extern "C" fn(_, _, _, _, _, _, _) -> _,
        ),
        encode_get_result_response: Some(
            ffi_encode_get_result_response
                as unsafe extern "C" fn(_, _, _, _, _, _, _, _, _, _, _, _) -> _,
        ),
    };
    let mut server: aorta_action_server_t = 0;
    let rc = unsafe {
        aorta_action_server_new_with_options(
            server_node,
            action_name.as_ptr(),
            DummyAction::schema_bfbs().as_ptr(),
            DummyAction::schema_bfbs().len(),
            ptr::addr_of!(server_opts),
            ptr::addr_of!(codec),
            Some(ffi_execute_action_traced as unsafe extern "C" fn(_, _, _)),
            ptr::null_mut(),
            &mut server,
        )
    };
    assert_eq!(rc, AORTA_OK);

    let client_name = CString::new(unique_name("ffi_traced_action_client")).unwrap();
    let mut client_node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(client_name.as_ptr(), group.as_ptr(), &mut client_node) };
    assert_eq!(rc, AORTA_OK);

    let client_opts = aorta_action_client_options_t {
        control_timeout_ms: 3000,
        info_timeout_ms: 1000,
    };
    let mut client: aorta_action_client_t = 0;
    let rc = unsafe {
        aorta_action_client_new_with_options(
            client_node,
            action_name.as_ptr(),
            ptr::addr_of!(client_opts),
            &mut client,
        )
    };
    assert_eq!(rc, AORTA_OK);

    // Wait for the server's queryables to be reachable.
    let mut info_bytes = aorta_owned_bytes_t::default();
    let mut rc = AORTA_ERR_NOT_FOUND;
    for _ in 0..20 {
        rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
        if rc == AORTA_OK {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let sender_ptr = Box::into_raw(Box::new(tx));
    let mut feedback_sub: aorta_action_feedback_sub_t = 0;
    let rc = unsafe {
        aorta_action_client_subscribe_feedback(
            client,
            Some(feedback_callback),
            sender_ptr as *mut c_void,
            &mut feedback_sub,
        )
    };
    assert_eq!(rc, AORTA_OK);

    TRACED_EXEC_ADOPTED.store(0, Ordering::SeqCst);
    TRACED_EXEC_ADOPTED_SPAN.store(0, Ordering::SeqCst);
    TRACED_EXEC_GOAL_CTX_SPAN.store(0, Ordering::SeqCst);

    // Serialize the master-gate toggle against the rest of the trace suite.
    let _trace_guard = crate::trace_test_support::lock();
    crate::trace::set_trace_enabled(true);
    let stage_scope = crate::trace::intern_scope(crate::trace::stage::STAGE_SCOPE_NAME);
    crate::trace::set_scope_enabled(stage_scope, true);

    // ---- send_goal_traced: what the C++ SDK's SendGoalWithId does ----
    let sent_mono_ns = aorta_trace_monotonic_ns();
    let goal_id = "550e8400-e29b-41d4-a716-446655440777";
    let request = DummyAction::encode_send_goal_request(goal_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request_slice = aorta_bytes_slice_t::default();
    let request_view = view_from_bytes(&request, &mut request_slice);
    let mut send_goal_bytes = aorta_owned_bytes_t::default();
    let mut sent_trace = [0u8; 16];
    let mut sent_span: u64 = 0;
    let mut sent_parent: u64 = 0;
    let rc = unsafe {
        aorta_action_client_send_goal_traced(
            client,
            ptr::addr_of!(request_view),
            sent_trace.as_mut_ptr(),
            &mut sent_span,
            &mut sent_parent,
            ptr::addr_of_mut!(send_goal_bytes),
        )
    };
    assert_eq!(rc, AORTA_OK);
    let send_goal = decode_send_goal_response(&owned_bytes_to_vec(&send_goal_bytes))
        .expect("decode send_goal response");
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(send_goal_bytes)) };
    assert!(send_goal.accepted);
    assert_ne!(
        sent_span, 0,
        "tracing on: the goal_sent span id must be minted"
    );
    assert_ne!(sent_trace, [0u8; 16]);

    // The C++ SDK records goal_sent_to_accepted itself after decoding ACCEPT.
    let name_c = CString::new(DummyAction::ACTION_NAME).unwrap();
    unsafe {
        aorta_trace_record_stage_at(
            crate::trace::StageId::ActionGoalSentToAccepted as u8,
            name_c.as_ptr(),
            sent_mono_ns,
            sent_trace.as_ptr(),
            sent_span,
            sent_parent,
        );
    }

    let _feedback_payload = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive action feedback");

    // Wait for the goal to reach its terminal state (succeed in the callback),
    // then record result_wait the way the C++ GoalHandle::WaitResult does.
    let wait_start_ns = aorta_trace_monotonic_ns();
    let goal_id_c = CString::new(goal_id).unwrap();
    let mut reached_terminal = false;
    for _ in 0..50 {
        let mut status_bytes = aorta_owned_bytes_t::default();
        let rc = unsafe {
            aorta_action_client_query_status(
                client,
                goal_id_c.as_ptr(),
                ptr::addr_of_mut!(status_bytes),
            )
        };
        if rc == AORTA_OK {
            let status =
                decode_public_status(&owned_bytes_to_vec(&status_bytes)).expect("decode status");
            unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(status_bytes)) };
            if status.status.is_terminal() {
                assert_eq!(status.status, ActionStatus::Succeeded);
                reached_terminal = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(reached_terminal, "goal must reach a terminal state");

    // get_result drives the server-half delivery stage (get_result_query).
    let get_result_request =
        DummyAction::encode_get_result_request(goal_id, &send_goal.goal_secret)
            .expect("encode get_result request");
    let mut result_request_slice = aorta_bytes_slice_t::default();
    let result_request_view = view_from_bytes(&get_result_request, &mut result_request_slice);
    let mut result_bytes = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_action_client_get_result(
            client,
            ptr::addr_of!(result_request_view),
            ptr::addr_of_mut!(result_bytes),
        )
    };
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(result_bytes)) };

    unsafe {
        aorta_trace_record_stage_under(
            crate::trace::StageId::ActionResultWait as u8,
            name_c.as_ptr(),
            wait_start_ns,
            sent_trace.as_ptr(),
            sent_span,
        );
    }

    // ---- drain the trace ring and assert the single-trace tree ----
    let ev_goal_sent = crate::trace::stage_event_for(
        crate::trace::StageId::ActionGoalSentToAccepted,
        DummyAction::ACTION_NAME,
    );
    let ev_exec_started = crate::trace::stage_event_for(
        crate::trace::StageId::ActionAcceptedToExecutionStarted,
        DummyAction::ACTION_NAME,
    );
    let ev_finished = crate::trace::stage_event_for(
        crate::trace::StageId::ActionAcceptedToFinished,
        DummyAction::ACTION_NAME,
    );
    let ev_user_callback = crate::trace::stage_event_for(
        crate::trace::StageId::ActionUserCallback,
        DummyAction::ACTION_NAME,
    );
    let ev_result_wait = crate::trace::stage_event_for(
        crate::trace::StageId::ActionResultWait,
        DummyAction::ACTION_NAME,
    );
    let ev_feedback_pub =
        crate::trace::marker_event_for("action.feedback_pub", DummyAction::ACTION_NAME);
    let ev_feedback_recv =
        crate::trace::marker_event_for("action.feedback_recv", DummyAction::ACTION_NAME);
    let ev_goal_admission = crate::trace::stage_event_for(
        crate::trace::StageId::ActionGoalAdmission,
        DummyAction::ACTION_NAME,
    );
    let ev_get_result_query = crate::trace::stage_event_for(
        crate::trace::StageId::ActionGetResultQuery,
        DummyAction::ACTION_NAME,
    );

    let wanted = [
        ev_goal_sent,
        ev_exec_started,
        ev_finished,
        ev_user_callback,
        ev_result_wait,
        ev_feedback_pub,
        ev_feedback_recv,
        ev_goal_admission,
        ev_get_result_query,
    ];
    let mut recs = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        crate::trace::ring::drain_all(&mut recs);
        let have_all = wanted
            .iter()
            .all(|ev| recs.iter().any(|r| r.event_id == *ev));
        if have_all || std::time::Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    crate::trace::set_trace_enabled(false);

    use crate::trace::chunk::ChunkEventType;
    let begin_of = |ev: u16| {
        recs.iter()
            .find(|r| {
                r.event_id == ev
                    && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
            })
            .unwrap_or_else(|| panic!("missing SliceBegin for event {ev}"))
    };

    let root = begin_of(ev_finished);
    assert_eq!(
        root.trace_id, sent_trace,
        "the goal ROOT must ride the caller's trace (traceparent on the send_goal attachment)"
    );
    assert_eq!(
        root.parent_span_id, sent_span,
        "the goal root must parent to the minted goal_sent span (ONE cross-process link)"
    );

    let exec_started = begin_of(ev_exec_started);
    assert_eq!(exec_started.trace_id, sent_trace);
    assert_eq!(
        exec_started.parent_span_id, root.span_id,
        "execution_started must nest under the goal root"
    );

    let user_cb = begin_of(ev_user_callback);
    assert_eq!(user_cb.trace_id, sent_trace);
    assert_eq!(
        user_cb.parent_span_id, root.span_id,
        "user_callback must nest under the goal root"
    );
    assert_eq!(
        TRACED_EXEC_ADOPTED.load(Ordering::SeqCst),
        1,
        "the execute callback must run under an adopted context"
    );
    assert_eq!(
        TRACED_EXEC_ADOPTED_SPAN.load(Ordering::SeqCst),
        user_cb.span_id,
        "the adopted context must be the pre-minted user_callback span (so user spans nest)"
    );
    assert_eq!(
        TRACED_EXEC_GOAL_CTX_SPAN.load(Ordering::SeqCst),
        root.span_id,
        "aorta_action_context_trace_context must hand out the goal root"
    );

    let fb_pub = begin_of(ev_feedback_pub);
    assert_eq!(fb_pub.trace_id, sent_trace);
    assert_eq!(fb_pub.parent_span_id, root.span_id);
    let fb_recv = begin_of(ev_feedback_recv);
    assert_eq!(fb_recv.trace_id, sent_trace);
    assert_eq!(
        fb_recv.parent_span_id, fb_pub.span_id,
        "feedback_recv must parent to the publish span (pub → recv transit hop)"
    );

    let goal_sent = begin_of(ev_goal_sent);
    assert_eq!(goal_sent.trace_id, sent_trace);
    assert_eq!(
        goal_sent.span_id, sent_span,
        "the SDK-side goal_sent record must reuse the wire-shipped identity"
    );

    let result_wait = begin_of(ev_result_wait);
    assert_eq!(result_wait.trace_id, sent_trace);
    assert_eq!(
        result_wait.parent_span_id, sent_span,
        "result_wait must nest under the goal_sent span"
    );

    let admission = begin_of(ev_goal_admission);
    assert_eq!(admission.trace_id, sent_trace);
    assert_eq!(
        admission.parent_span_id, sent_span,
        "goal_admission must parent to the caller's goal_sent span (covers rejects too)"
    );

    let get_result_query = begin_of(ev_get_result_query);
    assert_eq!(get_result_query.trace_id, sent_trace);
    assert_eq!(
        get_result_query.parent_span_id, root.span_id,
        "get_result_query must nest under the goal root while the goal is live"
    );

    // ---- legacy ABI: the un-traced send_goal must attach the
    // thread's CURRENT context (an already-recorded span) when one is active,
    // and NOTHING when none is — never a pre-minted id nobody will record. ----
    crate::trace::set_trace_enabled(true);

    let legacy_ctx_trace = [0x42u8; 16];
    let legacy_ctx_span: u64 = 0xABCD;
    crate::trace::set_trace_context(legacy_ctx_trace, legacy_ctx_span);
    let goal2_id = "550e8400-e29b-41d4-a716-446655440778";
    let request2 = DummyAction::encode_send_goal_request(goal2_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request2_slice = aorta_bytes_slice_t::default();
    let request2_view = view_from_bytes(&request2, &mut request2_slice);
    let mut goal2_bytes = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_action_client_send_goal(
            client,
            ptr::addr_of!(request2_view),
            ptr::addr_of_mut!(goal2_bytes),
        )
    };
    crate::trace::clear_trace_context();
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(goal2_bytes)) };

    let goal3_id = "550e8400-e29b-41d4-a716-446655440779";
    let request3 = DummyAction::encode_send_goal_request(goal3_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request3_slice = aorta_bytes_slice_t::default();
    let request3_view = view_from_bytes(&request3, &mut request3_slice);
    let mut goal3_bytes = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_action_client_send_goal(
            client,
            ptr::addr_of!(request3_view),
            ptr::addr_of_mut!(goal3_bytes),
        )
    };
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(goal3_bytes)) };

    // Both dummy goals succeed immediately; wait for both roots.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        crate::trace::ring::drain_all(&mut recs);
        let have_ctx_root = recs.iter().any(|r| {
            r.event_id == ev_finished
                && r.trace_id == legacy_ctx_trace
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        });
        let have_fresh_root = recs.iter().any(|r| {
            r.event_id == ev_finished
                && r.trace_id != legacy_ctx_trace
                && r.trace_id != sent_trace
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        });
        if (have_ctx_root && have_fresh_root) || std::time::Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    crate::trace::set_trace_enabled(false);

    let goal2_root = recs
        .iter()
        .find(|r| {
            r.event_id == ev_finished
                && r.trace_id == legacy_ctx_trace
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        })
        .expect("legacy send_goal under an active context must propagate that trace");
    assert_eq!(
        goal2_root.parent_span_id, legacy_ctx_span,
        "the goal root must parent to the ALREADY-RECORDED context span, not a minted orphan"
    );
    let goal3_root = recs
        .iter()
        .find(|r| {
            r.event_id == ev_finished
                && r.trace_id != legacy_ctx_trace
                && r.trace_id != sent_trace
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        })
        .expect("legacy send_goal without a context still gets a fresh server-root tree");
    assert_eq!(
        goal3_root.parent_span_id, 0,
        "no active context => no traceparent on the wire => a parentless fresh root"
    );

    assert_eq!(aorta_action_feedback_sub_free(feedback_sub), AORTA_OK);
    unsafe {
        drop(Box::from_raw(sender_ptr));
    }
    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(client_node), AORTA_OK);
    assert_eq!(aorta_action_server_free(server), AORTA_OK);
    assert_eq!(aorta_node_free(server_node), AORTA_OK);
}

// ---------------------------------------------------------------------------
// Event-driven result subscription (aorta_action_client_subscribe_result).
// ---------------------------------------------------------------------------

unsafe extern "C" fn result_decode_status(
    kind: u8,
    bytes: *const u8,
    len: usize,
    _user_data: *mut c_void,
) -> i8 {
    let payload = unsafe { std::slice::from_raw_parts(bytes, len) };
    let status = if kind == AORTA_ACTION_STATUS_KIND_PUBLIC_STATUS {
        decode_public_status(payload).map(|s| s.status)
    } else {
        decode_get_result_response(payload).map(|r| r.status)
    };
    match status {
        Ok(s) => s as i8,
        Err(_) => -1,
    }
}

struct ResultCapture {
    tx: mpsc::Sender<(aorta_result_t, Vec<u8>)>,
}

unsafe extern "C" fn result_callback(
    ev: *const aorta_action_result_view_t,
    user_data: *mut c_void,
) {
    if ev.is_null() || user_data.is_null() {
        return;
    }
    let ev = unsafe { &*ev };
    let capture = unsafe { &*(user_data as *const ResultCapture) };
    let payload = bytes_view_to_vec(ptr::addr_of!(ev.result_payload));
    let _ = capture.tx.send((ev.error, payload));
}

#[test]
fn ffi_action_subscribe_result_delivers_terminal_result() {
    let _action_guard = lock_action_test();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let (server_node, server) = runtime.block_on(async {
        let server_node = Node::new(unique_name("ffi_action_result_server"), "default")
            .await
            .expect("server node");
        let server = ActionServer::<DummyAction>::serve_with_handler(
            &server_node,
            DummyAction::ACTION_NAME,
            ActionServerOptions::default(),
            |_goal, ctx| async move { ctx.succeed(&DummyResultMsg { value: "done" }).await },
        )
        .await
        .expect("serve action");
        (server_node, server)
    });

    let node_name = CString::new(unique_name("ffi_action_result_client")).unwrap();
    let group = CString::new("default").unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );

    let opts = aorta_action_client_options_t {
        control_timeout_ms: 3000,
        info_timeout_ms: 1000,
    };
    let mut client: aorta_action_client_t = 0;
    assert_eq!(
        unsafe {
            aorta_action_client_new_with_options(
                node,
                action_name.as_ptr(),
                ptr::addr_of!(opts),
                &mut client,
            )
        },
        AORTA_OK
    );

    // Wait for server discovery.
    let mut info_bytes = aorta_owned_bytes_t::default();
    let mut rc = AORTA_ERR_NOT_FOUND;
    for _ in 0..50 {
        rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
        if rc == AORTA_OK {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };

    // Send the goal and capture the issued goal_secret.
    let goal_id = "550e8400-e29b-41d4-a716-446655440000";
    let request = DummyAction::encode_send_goal_request(goal_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request_slice = aorta_bytes_slice_t::default();
    let request_view = view_from_bytes(&request, &mut request_slice);
    let mut send_goal_bytes = aorta_owned_bytes_t::default();
    assert_eq!(
        unsafe {
            aorta_action_client_send_goal(
                client,
                ptr::addr_of!(request_view),
                ptr::addr_of_mut!(send_goal_bytes),
            )
        },
        AORTA_OK
    );
    let send_goal = decode_send_goal_response(&owned_bytes_to_vec(&send_goal_bytes))
        .expect("decode send_goal response");
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(send_goal_bytes)) };
    assert!(send_goal.accepted);

    // Subscribe for the terminal result (event-driven, non-blocking).
    let (tx, rx) = mpsc::channel::<(aorta_result_t, Vec<u8>)>();
    let capture_ptr = Box::into_raw(Box::new(ResultCapture { tx }));
    let get_result_request =
        DummyAction::encode_get_result_request(goal_id, &send_goal.goal_secret)
            .expect("encode get_result request");
    let goal_id_c = CString::new(goal_id).unwrap();
    let mut result_sub: aorta_action_result_sub_t = 0;
    let rc = unsafe {
        aorta_action_client_subscribe_result(
            client,
            goal_id_c.as_ptr(),
            get_result_request.as_ptr(),
            get_result_request.len(),
            5000,
            Some(result_decode_status),
            Some(result_callback),
            capture_ptr as *mut c_void,
            &mut result_sub,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(result_sub, 0);

    // The watcher must deliver the terminal result exactly once.
    let (error, payload) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("receive terminal result");
    assert_eq!(error, AORTA_OK);
    let result = decode_get_result_response(&payload).expect("decode get_result response");
    assert_eq!(result.goal_id, goal_id);
    assert_eq!(result.status, ActionStatus::Succeeded);
    assert_eq!(
        result.result.as_ref().map(|value| value.value.as_str()),
        Some("done")
    );

    assert_eq!(aorta_action_result_sub_free(result_sub), AORTA_OK);
    unsafe {
        drop(Box::from_raw(capture_ptr));
    }
    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    runtime.block_on(async {
        server.stop_and_join(Duration::from_secs(1)).await;
        server_node.close().await.expect("close server node");
    });
}

struct LaneResultCapture {
    tx: mpsc::Sender<(aorta_result_t, Vec<u8>, std::thread::ThreadId)>,
}

unsafe extern "C" fn lane_result_callback(
    ev: *const aorta_action_result_view_t,
    user_data: *mut c_void,
) {
    if ev.is_null() || user_data.is_null() {
        return;
    }
    let ev = unsafe { &*ev };
    let capture = unsafe { &*(user_data as *const LaneResultCapture) };
    let payload = bytes_view_to_vec(ptr::addr_of!(ev.result_payload));
    let _ = capture
        .tx
        .send((ev.error, payload, std::thread::current().id()));
}

/// With the client's node attached to a spinning executor,
/// the terminal result must be delivered as a `CoroutineResume` event on the
/// executor's reserved lane — i.e. the C callback runs ON the executor's
/// dispatch (spin) thread, not on a runtime worker or a private thread.
#[test]
fn ffi_action_subscribe_result_delivers_on_the_executor_lane() {
    let _action_guard = lock_action_test();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let (server_node, server) = runtime.block_on(async {
        let server_node = Node::new(unique_name("ffi_action_lane_server"), "default")
            .await
            .expect("server node");
        let server = ActionServer::<DummyAction>::serve_with_handler(
            &server_node,
            DummyAction::ACTION_NAME,
            ActionServerOptions::default(),
            |_goal, ctx| async move { ctx.succeed(&DummyResultMsg { value: "done" }).await },
        )
        .await
        .expect("serve action");
        (server_node, server)
    });

    let node_name = CString::new(unique_name("ffi_action_lane_client")).unwrap();
    let group = CString::new("default").unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );

    // Attach the node to a single-threaded executor and spin it on a
    // dedicated thread: dispatch runs inline on that thread.
    let mut exec: u64 = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );
    assert_eq!(aorta_executor_add_node(exec, node), AORTA_OK);
    let (spin_tid_tx, spin_tid_rx) = mpsc::channel::<std::thread::ThreadId>();
    let spin_exec = exec;
    let spin_thread = std::thread::spawn(move || {
        spin_tid_tx
            .send(std::thread::current().id())
            .expect("send spin thread id");
        aorta_executor_spin(spin_exec)
    });
    let spin_tid = spin_tid_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("spin thread id");

    let opts = aorta_action_client_options_t {
        control_timeout_ms: 3000,
        info_timeout_ms: 1000,
    };
    let mut client: aorta_action_client_t = 0;
    assert_eq!(
        unsafe {
            aorta_action_client_new_with_options(
                node,
                action_name.as_ptr(),
                ptr::addr_of!(opts),
                &mut client,
            )
        },
        AORTA_OK
    );

    // Wait for server discovery.
    let mut info_bytes = aorta_owned_bytes_t::default();
    let mut rc = AORTA_ERR_NOT_FOUND;
    for _ in 0..50 {
        rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
        if rc == AORTA_OK {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };

    let goal_id = "550e8400-e29b-41d4-a716-446655440001";
    let request = DummyAction::encode_send_goal_request(goal_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request_slice = aorta_bytes_slice_t::default();
    let request_view = view_from_bytes(&request, &mut request_slice);
    let mut send_goal_bytes = aorta_owned_bytes_t::default();
    assert_eq!(
        unsafe {
            aorta_action_client_send_goal(
                client,
                ptr::addr_of!(request_view),
                ptr::addr_of_mut!(send_goal_bytes),
            )
        },
        AORTA_OK
    );
    let send_goal = decode_send_goal_response(&owned_bytes_to_vec(&send_goal_bytes))
        .expect("decode send_goal response");
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(send_goal_bytes)) };
    assert!(send_goal.accepted);

    let (tx, rx) = mpsc::channel::<(aorta_result_t, Vec<u8>, std::thread::ThreadId)>();
    let capture_ptr = Box::into_raw(Box::new(LaneResultCapture { tx }));
    let get_result_request =
        DummyAction::encode_get_result_request(goal_id, &send_goal.goal_secret)
            .expect("encode get_result request");
    let goal_id_c = CString::new(goal_id).unwrap();
    let mut result_sub: aorta_action_result_sub_t = 0;
    let rc = unsafe {
        aorta_action_client_subscribe_result(
            client,
            goal_id_c.as_ptr(),
            get_result_request.as_ptr(),
            get_result_request.len(),
            5000,
            Some(result_decode_status),
            Some(lane_result_callback),
            capture_ptr as *mut c_void,
            &mut result_sub,
        )
    };
    assert_eq!(rc, AORTA_OK);

    let (error, payload, delivery_tid) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("receive terminal result via the executor lane");
    assert_eq!(error, AORTA_OK);
    let result = decode_get_result_response(&payload).expect("decode get_result response");
    assert_eq!(result.status, ActionStatus::Succeeded);
    assert_eq!(
        delivery_tid, spin_tid,
        "with an executor attached, the result callback must be dispatched \
         on the executor's spin thread (CoroutineResume lane), not a runtime \
         worker or a private delivery thread"
    );

    assert_eq!(aorta_action_result_sub_free(result_sub), AORTA_OK);
    unsafe {
        drop(Box::from_raw(capture_ptr));
    }
    assert_eq!(aorta_executor_stop(exec), AORTA_OK);
    assert_eq!(
        spin_thread.join().expect("join spin thread"),
        AORTA_OK,
        "executor spin must exit cleanly on stop"
    );
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    runtime.block_on(async {
        server.stop_and_join(Duration::from_secs(1)).await;
        server_node.close().await.expect("close server node");
    });
}

#[test]
fn ffi_action_subscribe_feedback2_delivers_off_rx_with_dropped_count() {
    let _action_guard = lock_action_test();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let (server_node, server) = runtime.block_on(async {
        let server_node = Node::new(unique_name("ffi_action_fb2_server"), "default")
            .await
            .expect("server node");
        let server = ActionServer::<DummyAction>::serve_with_handler(
            &server_node,
            DummyAction::ACTION_NAME,
            ActionServerOptions::default(),
            |_goal, ctx| async move {
                ctx.publish_feedback(&DummyFeedbackDataMsg { value: "progress" })
                    .await?;
                ctx.succeed(&DummyResultMsg { value: "done" }).await
            },
        )
        .await
        .expect("serve action");
        (server_node, server)
    });

    let node_name = CString::new(unique_name("ffi_action_fb2_client")).unwrap();
    let group = CString::new("default").unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );

    let opts = aorta_action_client_options_t {
        control_timeout_ms: 3000,
        info_timeout_ms: 1000,
    };
    let mut client: aorta_action_client_t = 0;
    assert_eq!(
        unsafe {
            aorta_action_client_new_with_options(
                node,
                action_name.as_ptr(),
                ptr::addr_of!(opts),
                &mut client,
            )
        },
        AORTA_OK
    );

    let mut info_bytes = aorta_owned_bytes_t::default();
    let mut rc = AORTA_ERR_NOT_FOUND;
    for _ in 0..50 {
        rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
        if rc == AORTA_OK {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };

    // Bounded, off-RX feedback subscription.
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let sender_ptr = Box::into_raw(Box::new(tx));
    let mut fb2_sub: aorta_action_feedback_sub_t = 0;
    let rc = unsafe {
        aorta_action_client_subscribe_feedback2(
            client,
            64,
            AORTA_FB_DROP_OLDEST,
            Some(feedback_callback),
            sender_ptr as *mut c_void,
            &mut fb2_sub,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(fb2_sub, 0);

    let goal_id = "550e8400-e29b-41d4-a716-446655440000";
    let request = DummyAction::encode_send_goal_request(goal_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request_slice = aorta_bytes_slice_t::default();
    let request_view = view_from_bytes(&request, &mut request_slice);
    let mut send_goal_bytes = aorta_owned_bytes_t::default();
    assert_eq!(
        unsafe {
            aorta_action_client_send_goal(
                client,
                ptr::addr_of!(request_view),
                ptr::addr_of_mut!(send_goal_bytes),
            )
        },
        AORTA_OK
    );
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(send_goal_bytes)) };

    // The feedback must arrive via the drain thread (off RX).
    let feedback_payload = rx
        .recv_timeout(Duration::from_secs(3))
        .expect("receive action feedback");
    let feedback = DummyFeedbackSchema::decode_typed_root(&feedback_payload).expect("decode");
    assert_eq!(feedback.goal_id, goal_id);
    assert_eq!(feedback.value, "progress");

    // No drops at capacity 64 for a single feedback sample.
    let mut dropped: u64 = u64::MAX;
    assert_eq!(
        unsafe { aorta_action_feedback_sub2_dropped_count(fb2_sub, &mut dropped) },
        AORTA_OK
    );
    assert_eq!(dropped, 0);

    assert_eq!(aorta_action_feedback_sub2_free(fb2_sub), AORTA_OK);
    unsafe {
        drop(Box::from_raw(sender_ptr));
    }
    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    runtime.block_on(async {
        server.stop_and_join(Duration::from_secs(1)).await;
        server_node.close().await.expect("close server node");
    });
}

extern "C" fn lane_feedback_callback(
    payload: *const aorta_bytes_view_t,
    _schema: *const aorta_schema_hash_view_t,
    _ctx: *const aorta_msg_context_t,
    user_data: *mut c_void,
) {
    if user_data.is_null() {
        return;
    }
    let sender = unsafe { &*(user_data as *const mpsc::Sender<(Vec<u8>, std::thread::ThreadId)>) };
    let _ = sender.send((bytes_view_to_vec(payload), std::thread::current().id()));
}

/// With the client's node attached to a spinning executor
/// at subscribe time, feedback2 drains are dispatched as `CoroutineResume` events
/// on the executor's reserved lane — the C callback runs ON the executor's
/// dispatch (spin) thread, and no `aorta-act-feedback2` drain thread exists.
#[test]
fn ffi_action_subscribe_feedback2_delivers_on_the_executor_lane() {
    let _action_guard = lock_action_test();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let (server_node, server) = runtime.block_on(async {
        let server_node = Node::new(unique_name("ffi_action_fb2lane_server"), "default")
            .await
            .expect("server node");
        let server = ActionServer::<DummyAction>::serve_with_handler(
            &server_node,
            DummyAction::ACTION_NAME,
            ActionServerOptions::default(),
            |_goal, ctx| async move {
                for _ in 0..3 {
                    ctx.publish_feedback(&DummyFeedbackDataMsg { value: "progress" })
                        .await?;
                }
                ctx.succeed(&DummyResultMsg { value: "done" }).await
            },
        )
        .await
        .expect("serve action");
        (server_node, server)
    });

    let node_name = CString::new(unique_name("ffi_action_fb2lane_client")).unwrap();
    let group = CString::new("default").unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );

    // Attach + spin an executor BEFORE subscribing: feedback2 fixes its
    // delivery mode at subscribe time.
    let mut exec: u64 = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );
    assert_eq!(aorta_executor_add_node(exec, node), AORTA_OK);
    let (spin_tid_tx, spin_tid_rx) = mpsc::channel::<std::thread::ThreadId>();
    let spin_exec = exec;
    let spin_thread = std::thread::spawn(move || {
        spin_tid_tx
            .send(std::thread::current().id())
            .expect("send spin thread id");
        aorta_executor_spin(spin_exec)
    });
    let spin_tid = spin_tid_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("spin thread id");

    let opts = aorta_action_client_options_t {
        control_timeout_ms: 3000,
        info_timeout_ms: 1000,
    };
    let mut client: aorta_action_client_t = 0;
    assert_eq!(
        unsafe {
            aorta_action_client_new_with_options(
                node,
                action_name.as_ptr(),
                ptr::addr_of!(opts),
                &mut client,
            )
        },
        AORTA_OK
    );

    let mut info_bytes = aorta_owned_bytes_t::default();
    let mut rc = AORTA_ERR_NOT_FOUND;
    for _ in 0..50 {
        rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
        if rc == AORTA_OK {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };

    let (tx, rx) = mpsc::channel::<(Vec<u8>, std::thread::ThreadId)>();
    let sender_ptr = Box::into_raw(Box::new(tx));
    let mut fb2_sub: aorta_action_feedback_sub_t = 0;
    let rc = unsafe {
        aorta_action_client_subscribe_feedback2(
            client,
            64,
            AORTA_FB_DROP_OLDEST,
            Some(lane_feedback_callback),
            sender_ptr as *mut c_void,
            &mut fb2_sub,
        )
    };
    assert_eq!(rc, AORTA_OK);

    let goal_id = "550e8400-e29b-41d4-a716-446655440002";
    let request = DummyAction::encode_send_goal_request(goal_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request_slice = aorta_bytes_slice_t::default();
    let request_view = view_from_bytes(&request, &mut request_slice);
    let mut send_goal_bytes = aorta_owned_bytes_t::default();
    assert_eq!(
        unsafe {
            aorta_action_client_send_goal(
                client,
                ptr::addr_of!(request_view),
                ptr::addr_of_mut!(send_goal_bytes),
            )
        },
        AORTA_OK
    );
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(send_goal_bytes)) };

    // All three feedback samples must arrive, each on the executor's spin
    // thread (the CoroutineResume lane dispatch), none on a drain thread.
    for i in 0..3 {
        let (payload, delivery_tid) = rx
            .recv_timeout(Duration::from_secs(3))
            .unwrap_or_else(|_| panic!("receive feedback sample {i}"));
        let feedback = DummyFeedbackSchema::decode_typed_root(&payload).expect("decode");
        assert_eq!(feedback.goal_id, goal_id);
        assert_eq!(feedback.value, "progress");
        assert_eq!(
            delivery_tid, spin_tid,
            "feedback2 with an executor must deliver on the spin thread"
        );
    }

    let mut dropped: u64 = u64::MAX;
    assert_eq!(
        unsafe { aorta_action_feedback_sub2_dropped_count(fb2_sub, &mut dropped) },
        AORTA_OK
    );
    assert_eq!(dropped, 0);

    assert_eq!(aorta_action_feedback_sub2_free(fb2_sub), AORTA_OK);
    unsafe {
        drop(Box::from_raw(sender_ptr));
    }
    assert_eq!(aorta_executor_stop(exec), AORTA_OK);
    assert_eq!(
        spin_thread.join().expect("join spin thread"),
        AORTA_OK,
        "executor spin must exit cleanly on stop"
    );
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    runtime.block_on(async {
        server.stop_and_join(Duration::from_secs(1)).await;
        server_node.close().await.expect("close server node");
    });
}

/// The watch C-ABI over a real zenoh pub/sub path.
#[test]
fn ffi_stream_ring_load_returns_the_latest_value() {
    let _action_guard = lock_action_test();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let node_name = CString::new(unique_name("ffi_ring_node")).unwrap();
    let group = CString::new("default").unwrap();
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );

    let topic = unique_name("flow_ring_topic");
    let topic_c = CString::new(topic.clone()).unwrap();
    let mut watch: u64 = 0;
    assert_eq!(
        unsafe { aorta_watch_create(node, topic_c.as_ptr(), &mut watch) },
        AORTA_OK
    );

    // Empty watch: has=0, OK.
    let mut buf = [0u8; 16];
    let (mut len, mut has, mut seq) = (0usize, 9u8, 0u64);
    assert_eq!(
        unsafe {
            aorta_watch_load(
                watch,
                buf.as_mut_ptr(),
                buf.len(),
                &mut len,
                &mut has,
                &mut seq,
            )
        },
        AORTA_OK
    );
    assert_eq!(has, 0);

    // Foreign-producer store + load roundtrip.
    let value = [7u8, 8, 9];
    assert_eq!(
        unsafe { aorta_watch_store(watch, value.as_ptr(), value.len()) },
        AORTA_OK
    );
    assert_eq!(
        unsafe {
            aorta_watch_load(
                watch,
                buf.as_mut_ptr(),
                buf.len(),
                &mut len,
                &mut has,
                &mut seq,
            )
        },
        AORTA_OK
    );
    assert_eq!((has, len), (1, 3));
    assert_eq!(&buf[..3], &value);

    // Too-small buffer: reports the required size.
    let mut tiny = [0u8; 1];
    let rc = unsafe {
        aorta_watch_load(
            watch,
            tiny.as_mut_ptr(),
            tiny.len(),
            &mut len,
            &mut has,
            &mut seq,
        )
    };
    assert_eq!(rc, AORTA_ERR_BUFFER_TOO_SMALL);
    assert_eq!(len, 3);

    // Zenoh-fed path: publish from a Rust node; the watch converges.
    let publisher_node = runtime.block_on(async {
        let n = Node::new(unique_name("ffi_ring_pub"), "default")
            .await
            .expect("pub node");
        let p = n.create_publisher(&topic).await.expect("publisher");
        p.publish(vec![42u8; 4]).expect("publish");
        n
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        assert_eq!(
            unsafe {
                aorta_watch_load(
                    watch,
                    buf.as_mut_ptr(),
                    buf.len(),
                    &mut len,
                    &mut has,
                    &mut seq,
                )
            },
            AORTA_OK
        );
        if has == 1 && len == 4 && buf[..4] == [42u8; 4] {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "watch must converge to the published value"
        );
        // Fire-and-forget publish can lose the pre-discovery burst —
        // republish while waiting (latest-wins keeps the assertion valid).
        runtime.block_on(async {
            let p = publisher_node
                .create_publisher(&topic)
                .await
                .expect("republisher");
            p.publish(vec![42u8; 4]).expect("republish");
        });
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut total = 0u64;
    assert_eq!(
        unsafe { aorta_watch_stored_total(watch, &mut total) },
        AORTA_OK
    );
    assert!(total >= 2);

    assert_eq!(aorta_watch_free(watch), AORTA_OK);
    assert_eq!(aorta_watch_free(watch), AORTA_ERR_NOT_FOUND);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    runtime.block_on(async {
        publisher_node.close().await.expect("close pub node");
    });
}

/// The CHILD_FAILED cancel reason is a NAMED constant surfacing through
/// the flag byte a C consumer polls (the fail-fast path — the one most
/// worth telemetry — previously decoded as an unnamed value).
#[test]
fn ffi_child_failed_reason_is_named() {
    let _action_guard = lock_action_test();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let scope = crate::flow::Scope::new_root(crate::flow::ScopeOpts::named("ffi-fail-fast"));
        let child: crate::flow::TaskHandle<()> = scope.spawn(async { panic!("fail-fast probe") });
        let _ = child.await;
        // The cascade wrote ChildFailed; the C constant matches it.
        assert_eq!(
            scope.cancel_reason().map(|r| r.as_u8()),
            Some(AORTA_CANCEL_REASON_CHILD_FAILED)
        );
        scope.close().await;
    });
}

/// out_seq comes from the SAME snapshot as the payload (monotonic,
/// never mismatched) and node_root_scope/token-bridge handles work.
#[test]
fn ffi_ring_seq_and_scope_bridges() {
    let _action_guard = lock_action_test();
    let node_name = CString::new(unique_name("ffi_wp4_node")).unwrap();
    let group = CString::new("default").unwrap();
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );

    // Watch: seq is 1-based and monotonic with the paired payload.
    let topic = CString::new(unique_name("wp4_ring")).unwrap();
    let mut watch: u64 = 0;
    assert_eq!(
        unsafe { aorta_watch_create(node, topic.as_ptr(), &mut watch) },
        AORTA_OK
    );
    let mut buf = [0u8; 8];
    let (mut len, mut has, mut seq) = (0usize, 0u8, 7u64);
    assert_eq!(
        unsafe {
            aorta_watch_load(
                watch,
                buf.as_mut_ptr(),
                buf.len(),
                &mut len,
                &mut has,
                &mut seq,
            )
        },
        AORTA_OK
    );
    assert_eq!((has, seq), (0, 0), "empty watch reports seq 0");
    for i in 1..=3u8 {
        assert_eq!(
            unsafe { aorta_watch_store(watch, [i].as_ptr(), 1) },
            AORTA_OK
        );
        assert_eq!(
            unsafe {
                aorta_watch_load(
                    watch,
                    buf.as_mut_ptr(),
                    buf.len(),
                    &mut len,
                    &mut has,
                    &mut seq,
                )
            },
            AORTA_OK
        );
        assert_eq!((has, len, buf[0], seq), (1, 1, i, u64::from(i)));
    }
    // Nullable out_seq stays supported.
    assert_eq!(
        unsafe {
            aorta_watch_load(
                watch,
                buf.as_mut_ptr(),
                buf.len(),
                &mut len,
                &mut has,
                std::ptr::null_mut(),
            )
        },
        AORTA_OK
    );
    assert_eq!(aorta_watch_free(watch), AORTA_OK);

    // node_root_scope: a reference handle; freeing it never kills the root.
    let mut root1: u64 = 0;
    let mut root2: u64 = 0;
    assert_eq!(unsafe { aorta_node_root_scope(node, &mut root1) }, AORTA_OK);
    assert_eq!(unsafe { aorta_node_root_scope(node, &mut root2) }, AORTA_OK);
    let mut cancelled: u8 = 9;
    assert_eq!(aorta_scope_free(root1), AORTA_OK);
    assert_eq!(
        unsafe { aorta_scope_is_cancelled(root2, &mut cancelled) },
        AORTA_OK
    );
    assert_eq!(cancelled, 0, "freeing one reference never cancels the root");

    // Token → scope bridge: token cancel cascades into the scope.
    let mut token: u64 = 0;
    assert_eq!(unsafe { aorta_token_child(0, &mut token) }, AORTA_OK);
    let mut bridged: u64 = 0;
    assert_eq!(
        unsafe { aorta_scope_child_of_token(token, std::ptr::null(), &mut bridged) },
        AORTA_OK
    );
    assert_eq!(
        aorta_token_cancel(token, AORTA_CANCEL_REASON_SHUTDOWN),
        AORTA_OK
    );
    assert_eq!(
        unsafe { aorta_scope_is_cancelled(bridged, &mut cancelled) },
        AORTA_OK
    );
    assert_eq!(cancelled, 1, "token cancel reaches the bridged scope");
    assert_eq!(aorta_scope_free(bridged), AORTA_OK);
    assert_eq!(aorta_token_free(token), AORTA_OK);
    assert_eq!(aorta_scope_free(root2), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

/// Covers a LIVE admission roundtrip,
/// the resume-event status field, and the root-scope cascade on node close.
#[test]
fn ffi_wp4_delta_gaps() {
    let _action_guard = lock_action_test();
    let node_name = CString::new(unique_name("ffi_delta_node")).unwrap();
    let group = CString::new("default").unwrap();
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );

    // Live admission roundtrip through the reshaped 16-byte struct.
    let topic = CString::new(unique_name("delta_adm")).unwrap();
    let mut sub: aorta_sub_t = 0;
    extern "C" fn delta_noop_cb(
        _payload: *const aorta_bytes_view_t,
        _schema: *const aorta_schema_hash_view_t,
        _ctx: *const aorta_msg_context_t,
        _ud: *mut c_void,
    ) {
    }
    assert_eq!(
        aorta_subscriber_new(
            node,
            topic.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            Some(delta_noop_cb),
            std::ptr::null_mut(),
            &mut sub,
        ),
        AORTA_OK
    );
    let admission = aorta_admission_t {
        max_in_flight: 4,
        deadline_ns: 2_000_000,
    };
    assert_eq!(
        unsafe { aorta_subscriber_set_admission(sub, std::ptr::addr_of!(admission)) },
        AORTA_OK,
        "a live subscriber accepts the reshaped struct"
    );
    assert_eq!(aorta_subscriber_free(sub), AORTA_OK);

    // Root-scope cascade: node close cancels the root (observed via a
    // still-held reference handle).
    let mut root: u64 = 0;
    assert_eq!(unsafe { aorta_node_root_scope(node, &mut root) }, AORTA_OK);
    let mut cancelled: u8 = 0;
    assert_eq!(
        unsafe { aorta_scope_is_cancelled(root, &mut cancelled) },
        AORTA_OK
    );
    assert_eq!(cancelled, 0);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    assert_eq!(
        unsafe { aorta_scope_is_cancelled(root, &mut cancelled) },
        AORTA_OK
    );
    assert_eq!(
        cancelled, 1,
        "node close cascades Shutdown into the root scope"
    );
    assert_eq!(aorta_scope_free(root), AORTA_OK);
}

/// aorta_task_finish_failed applies the Rust-panic
/// semantics — fail-fast ChildFailed unless supervised or already
/// cancelled; accounting balances; double-finish is NOT_FOUND.
#[test]
fn ffi_task_finish_failed_semantics() {
    let _action_guard = lock_action_test();
    // Non-supervised: failure cancels the scope with CHILD_FAILED.
    let mut scope: u64 = 0;
    assert_eq!(
        unsafe { aorta_scope_child(0, std::ptr::null(), &mut scope) },
        AORTA_OK
    );
    let mut task: u64 = 0;
    assert_eq!(
        unsafe { aorta_scope_begin_task(scope, &mut task) },
        AORTA_OK
    );
    assert_eq!(aorta_task_finish_failed(task), AORTA_OK);
    assert_eq!(
        aorta_task_finish_failed(task),
        AORTA_ERR_NOT_FOUND,
        "double finish"
    );
    let mut cancelled = 0u8;
    assert_eq!(
        unsafe { aorta_scope_is_cancelled(scope, &mut cancelled) },
        AORTA_OK
    );
    assert_eq!(cancelled, 1);
    // The RT flag byte CARRIES the reason (nonzero = reason value).
    let mut flag: *const u8 = std::ptr::null();
    assert_eq!(
        unsafe { aorta_scope_cancelled_flag_ptr(scope, &mut flag) },
        AORTA_OK
    );
    assert_eq!(unsafe { *flag }, AORTA_CANCEL_REASON_CHILD_FAILED);
    assert_eq!(aorta_scope_close(scope), AORTA_OK, "accounting balanced");
    assert_eq!(aorta_scope_free(scope), AORTA_OK);

    // Already-cancelled scope: failure is NOT relabeled (the Rust guard).
    let mut scope2: u64 = 0;
    assert_eq!(
        unsafe { aorta_scope_child(0, std::ptr::null(), &mut scope2) },
        AORTA_OK
    );
    let mut task2: u64 = 0;
    assert_eq!(
        unsafe { aorta_scope_begin_task(scope2, &mut task2) },
        AORTA_OK
    );
    assert_eq!(
        aorta_scope_cancel(scope2, AORTA_CANCEL_REASON_SHUTDOWN),
        AORTA_OK
    );
    assert_eq!(aorta_task_finish_failed(task2), AORTA_OK);
    let mut flag2: *const u8 = std::ptr::null();
    assert_eq!(
        unsafe { aorta_scope_cancelled_flag_ptr(scope2, &mut flag2) },
        AORTA_OK
    );
    assert_eq!(
        unsafe { *flag2 },
        AORTA_CANCEL_REASON_SHUTDOWN,
        "teardown must not be relabeled as ChildFailed"
    );
    assert_eq!(aorta_scope_free(scope2), AORTA_OK);

    // Supervised scope: the failure is CONTAINED.
    let sup_opts = aorta_scope_opts_t {
        priority: 255,
        supervision: crate::ffi::c_api::flow::AORTA_SUPERVISION_ISOLATE,
        _pad: [0; 2],
        budget_ns: 0,
        name: std::ptr::null(),
    };
    let mut parent: u64 = 0;
    assert_eq!(
        unsafe { aorta_scope_child(0, std::ptr::null(), &mut parent) },
        AORTA_OK
    );
    let mut probe: u64 = 0;
    assert_eq!(
        unsafe { aorta_scope_child(parent, std::ptr::addr_of!(sup_opts), &mut probe) },
        AORTA_OK
    );
    let mut task3: u64 = 0;
    assert_eq!(
        unsafe { aorta_scope_begin_task(probe, &mut task3) },
        AORTA_OK
    );
    assert_eq!(aorta_task_finish_failed(task3), AORTA_OK);
    let mut cancelled2 = 9u8;
    assert_eq!(
        unsafe { aorta_scope_is_cancelled(parent, &mut cancelled2) },
        AORTA_OK
    );
    assert_eq!(cancelled2, 0, "supervised containment: parent untouched");
    assert_eq!(
        unsafe { aorta_scope_is_cancelled(probe, &mut cancelled2) },
        AORTA_OK
    );
    assert_eq!(
        cancelled2, 0,
        "supervised: even the probe scope stays uncancelled"
    );
    assert_eq!(aorta_scope_free(probe), AORTA_OK);
    assert_eq!(aorta_scope_free(parent), AORTA_OK);
}

// ---------------------------------------------------------------------------
// Flow action ctx bridge: send_goal_ctx / wait_result_ctx / cancel_ctx
// (one-shot, scope-aware, callback delivery — the co_await substrate).
// ---------------------------------------------------------------------------

struct CtxCapture {
    tx: mpsc::Sender<(aorta_result_t, Vec<u8>)>,
}

unsafe extern "C" fn ctx_done_callback(
    code: aorta_result_t,
    payload: *const aorta_bytes_view_t,
    _completion: *const crate::ffi::c_api::flow::aorta_flow_completion_t,
    user_data: *mut c_void,
) {
    if user_data.is_null() {
        return;
    }
    let capture = unsafe { &*(user_data as *const CtxCapture) };
    let payload = if payload.is_null() {
        Vec::new()
    } else {
        bytes_view_to_vec(payload)
    };
    let _ = capture.tx.send((code, payload));
}

#[test]
fn ffi_action_ctx_bridge_round_trips_a_goal() {
    let _action_guard = lock_action_test();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let (server_node, server) = runtime.block_on(async {
        let server_node = Node::new(unique_name("ffi_ctx_bridge_server"), "default")
            .await
            .expect("server node");
        let server = ActionServer::<DummyAction>::serve_with_handler(
            &server_node,
            DummyAction::ACTION_NAME,
            ActionServerOptions::default(),
            |_goal, ctx| async move { ctx.succeed(&DummyResultMsg { value: "done" }).await },
        )
        .await
        .expect("serve action");
        (server_node, server)
    });

    let node_name = CString::new(unique_name("ffi_ctx_bridge_client")).unwrap();
    let group = CString::new("default").unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );
    let opts = aorta_action_client_options_t {
        control_timeout_ms: 3000,
        info_timeout_ms: 1000,
    };
    let mut client: aorta_action_client_t = 0;
    assert_eq!(
        unsafe {
            aorta_action_client_new_with_options(
                node,
                action_name.as_ptr(),
                ptr::addr_of!(opts),
                &mut client,
            )
        },
        AORTA_OK
    );

    // Wait for server discovery.
    let mut info_bytes = aorta_owned_bytes_t::default();
    let mut rc = AORTA_ERR_NOT_FOUND;
    for _ in 0..50 {
        rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
        if rc == AORTA_OK {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };

    // 1) send_goal_ctx (no scope): accepted response via on_done.
    let goal_id = "550e8400-e29b-41d4-a716-446655440042";
    let request = DummyAction::encode_send_goal_request(goal_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request_slice = aorta_bytes_slice_t::default();
    let request_view = view_from_bytes(&request, &mut request_slice);
    let (tx, rx) = mpsc::channel::<(aorta_result_t, Vec<u8>)>();
    let capture = Box::into_raw(Box::new(CtxCapture { tx }));
    assert_eq!(
        unsafe {
            crate::ffi::c_api::action_client::aorta_action_client_send_goal_ctx(
                client,
                0,
                ptr::addr_of!(request_view),
                Some(ctx_done_callback),
                capture as *mut c_void,
            )
        },
        AORTA_OK
    );
    let (code, payload) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("send_goal_ctx delivers");
    assert_eq!(code, AORTA_OK);
    let send_goal = decode_send_goal_response(&payload).expect("decode send_goal response");
    assert!(send_goal.accepted);
    assert_eq!(send_goal.goal_id, goal_id);

    // 2) wait_result_ctx (no scope): parks until terminal, delivers payload.
    let get_result_request =
        DummyAction::encode_get_result_request(goal_id, &send_goal.goal_secret)
            .expect("encode get_result request");
    let goal_id_c = CString::new(goal_id).unwrap();
    let (tx2, rx2) = mpsc::channel::<(aorta_result_t, Vec<u8>)>();
    let capture2 = Box::into_raw(Box::new(CtxCapture { tx: tx2 }));
    assert_eq!(
        unsafe {
            crate::ffi::c_api::action_client::aorta_action_client_wait_result_ctx(
                client,
                0,
                goal_id_c.as_ptr(),
                get_result_request.as_ptr(),
                get_result_request.len(),
                5000,
                Some(result_decode_status),
                Some(ctx_done_callback),
                capture2 as *mut c_void,
            )
        },
        AORTA_OK
    );
    let (code, payload) = rx2
        .recv_timeout(Duration::from_secs(5))
        .expect("wait_result_ctx delivers the terminal");
    assert_eq!(code, AORTA_OK);
    let result = decode_get_result_response(&payload).expect("decode get_result response");
    assert_eq!(result.status, ActionStatus::Succeeded);
    assert_eq!(
        result.result.as_ref().map(|value| value.value.as_str()),
        Some("done")
    );

    // 3) cancel_ctx with a callback: a finished goal answers (decodable
    //    response, delivery OK) — the checked-cancel path.
    let cancel_request = DummyAction::encode_cancel_goal_request(goal_id, &send_goal.goal_secret)
        .expect("encode cancel request");
    let mut cancel_slice = aorta_bytes_slice_t::default();
    let cancel_view = view_from_bytes(&cancel_request, &mut cancel_slice);
    let (tx3, rx3) = mpsc::channel::<(aorta_result_t, Vec<u8>)>();
    let capture3 = Box::into_raw(Box::new(CtxCapture { tx: tx3 }));
    assert_eq!(
        unsafe {
            crate::ffi::c_api::action_client::aorta_action_client_cancel_ctx(
                client,
                0,
                ptr::addr_of!(cancel_view),
                Some(ctx_done_callback),
                capture3 as *mut c_void,
            )
        },
        AORTA_OK
    );
    let (code, payload) = rx3
        .recv_timeout(Duration::from_secs(5))
        .expect("cancel_ctx delivers");
    assert_eq!(code, AORTA_OK);
    let _ = decode_cancel_goal_response(&payload).expect("decode cancel response");

    // 4) scope refusal: a CANCELLED scope refuses entry — on_done fires
    //    promptly with CANCELLED, no network round trip.
    let mut scope: u64 = 0;
    assert_eq!(
        unsafe { aorta_scope_child(0, ptr::null(), &mut scope) },
        AORTA_OK
    );
    assert_eq!(aorta_scope_cancel(scope, 1), AORTA_OK);
    let (tx4, rx4) = mpsc::channel::<(aorta_result_t, Vec<u8>)>();
    let capture4 = Box::into_raw(Box::new(CtxCapture { tx: tx4 }));
    assert_eq!(
        unsafe {
            crate::ffi::c_api::action_client::aorta_action_client_send_goal_ctx(
                client,
                scope,
                ptr::addr_of!(request_view),
                Some(ctx_done_callback),
                capture4 as *mut c_void,
            )
        },
        AORTA_OK
    );
    let (code, _) = rx4
        .recv_timeout(Duration::from_secs(2))
        .expect("cancelled-scope refusal delivers");
    assert_eq!(
        code,
        crate::ffi::c_api::AORTA_ERR_CANCELLED,
        "entry gate: no goal is sent"
    );
    assert_eq!(crate::ffi::c_api::flow::aorta_scope_free(scope), AORTA_OK);

    // 5) fire-and-forget cancel (null on_done) must be accepted.
    assert_eq!(
        unsafe {
            crate::ffi::c_api::action_client::aorta_action_client_cancel_ctx(
                client,
                0,
                ptr::addr_of!(cancel_view),
                None,
                ptr::null_mut(),
            )
        },
        AORTA_OK
    );

    unsafe {
        drop(Box::from_raw(capture));
        drop(Box::from_raw(capture2));
        drop(Box::from_raw(capture3));
        drop(Box::from_raw(capture4));
    }
    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    runtime.block_on(async {
        server.stop_and_join(Duration::from_secs(1)).await;
        server_node.close().await.expect("close server node");
    });
}

/// Every ctx-bridge scope
/// argument above is either 0 or cancelled BEFORE the call — no test covered
/// a real, still-open budget expiring WHILE the query is in flight. Two
/// mid-flight legs, both driven through the public C-ABI:
///
/// 1. send_goal_ctx against a held (never-answered) query: the scope's
///    remaining budget must (a) ride the wire as `deadline_budget_ns` and
///    (b) NARROW the in-flight timeout, so the call fails with
///    `AORTA_ERR_TIMEOUT` at ~budget — far below the 10s control timeout.
/// 2. wait_result_ctx parked on a slow (5s) handler: the budget expires
///    mid-park; a cooperative `aorta_scope_checkpoint` (the budget-expiry
///    observation the C++ caller performs) fires `DeadlineExceeded`, and the
///    in-flight call resolves with `AORTA_ERR_CANCELLED` long before the
///    terminal. The handler also proves the wire stamp seeded the goal
///    scope server-side (`ctx.scope().remaining()` = Some(≤ client budget)).
#[test]
fn ffi_action_ctx_bridge_open_budget_expires_mid_flight() {
    let _action_guard = lock_action_test();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    // ---- Leg 1: send_goal_ctx — the open budget narrows the wire flight ---

    let held_action_str = unique_name("test/ffi_ctx_budget_hold");
    let (stamp_tx, stamp_rx) = mpsc::channel::<Option<u64>>();
    let (holder_node, holder) = runtime.block_on(async {
        let node = Node::new(unique_name("ffi_ctx_budget_holder"), "default")
            .await
            .expect("holder node");
        let key: String = node
            .key_builder()
            .action_send_goal_key(&held_action_str)
            .expect("send_goal key")
            .into();
        let queryable = node
            .session()
            .inner()
            .declare_queryable(key)
            .await
            .expect("queryable");
        // Hold every query unanswered; report the wire-stamped budget so the
        // test can assert the attachment carried `deadline_budget_ns`.
        let holder = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok(query) = queryable.recv_async().await {
                let stamped = query
                    .attachment()
                    .map(|a| crate::attachment::decode(a.to_bytes().as_ref()))
                    .and_then(|parts| parts.deadline_budget_ns);
                let _ = stamp_tx.send(stamped);
                held.push(query);
            }
        });
        (node, holder)
    });

    let node_name = CString::new(unique_name("ffi_ctx_budget_cli")).unwrap();
    let group = CString::new("default").unwrap();
    let held_action_name = CString::new(held_action_str.clone()).unwrap();
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );
    // Control timeout 10s: only the scope budget can end the held query early.
    let opts = aorta_action_client_options_t {
        control_timeout_ms: 10_000,
        info_timeout_ms: 1000,
    };
    let mut held_client: aorta_action_client_t = 0;
    assert_eq!(
        unsafe {
            aorta_action_client_new_with_options(
                node,
                held_action_name.as_ptr(),
                ptr::addr_of!(opts),
                &mut held_client,
            )
        },
        AORTA_OK
    );

    const LEG1_BUDGET: Duration = Duration::from_millis(500);
    let scope_opts = aorta_scope_opts_t {
        priority: 255,
        supervision: crate::ffi::c_api::flow::AORTA_SUPERVISION_FAIL_FAST,
        _pad: [0; 2],
        budget_ns: LEG1_BUDGET.as_nanos() as u64,
        name: ptr::null(),
    };
    let mut scope1: u64 = 0;
    assert_eq!(
        unsafe { aorta_scope_child(0, ptr::addr_of!(scope_opts), &mut scope1) },
        AORTA_OK
    );

    let goal_id = "550e8400-e29b-41d4-a716-446655440077";
    let request = DummyAction::encode_send_goal_request(goal_id, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request_slice = aorta_bytes_slice_t::default();
    let request_view = view_from_bytes(&request, &mut request_slice);
    let (tx1, rx1) = mpsc::channel::<(aorta_result_t, Vec<u8>)>();
    let capture1 = Box::into_raw(Box::new(CtxCapture { tx: tx1 }));
    let leg1_start = std::time::Instant::now();
    assert_eq!(
        unsafe {
            crate::ffi::c_api::action_client::aorta_action_client_send_goal_ctx(
                held_client,
                scope1,
                ptr::addr_of!(request_view),
                Some(ctx_done_callback),
                capture1 as *mut c_void,
            )
        },
        AORTA_OK,
        "an OPEN budget must pass the entry gate"
    );

    // The query genuinely reached the wire (an entry refusal never sends),
    // stamped with the scope's remaining budget, narrowed to ≤ the budget.
    let stamped = stamp_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("held server received the send_goal query");
    let stamped_ns = stamped.expect("attachment must carry deadline_budget_ns");
    assert!(
        stamped_ns > 0 && stamped_ns <= LEG1_BUDGET.as_nanos() as u64,
        "wire budget must be the (narrowed) remaining, got {stamped_ns}ns"
    );

    let (code, _) = rx1
        .recv_timeout(Duration::from_secs(6))
        .expect("mid-flight budget expiry delivers");
    let leg1_elapsed = leg1_start.elapsed();
    assert_eq!(
        code, AORTA_ERR_TIMEOUT,
        "budget-narrowed in-flight timeout, not the 10s control timeout"
    );
    assert!(
        leg1_elapsed < Duration::from_secs(4),
        "delivery must come at ~budget (500ms), well under the 10s control \
         timeout; took {leg1_elapsed:?}"
    );
    assert_eq!(aorta_scope_free(scope1), AORTA_OK);
    assert_eq!(aorta_action_client_free(held_client), AORTA_OK);
    holder.abort();
    runtime.block_on(async {
        holder_node.close().await.expect("close holder node");
    });

    // ---- Leg 2: wait_result_ctx — budget expires while parked in flight ---

    // Handler: record the goal scope's seeded budget (the server half of the
    // wire stamp), then outlast the client budget by a wide margin.
    let seeded: Arc<Mutex<Option<Duration>>> = Arc::new(Mutex::new(None));
    let seeded_for_handler = Arc::clone(&seeded);
    let (server_node, server) = runtime.block_on(async {
        let server_node = Node::new(unique_name("ffi_ctx_budget_server"), "default")
            .await
            .expect("server node");
        let server = ActionServer::<DummyAction>::serve_with_handler(
            &server_node,
            DummyAction::ACTION_NAME,
            ActionServerOptions::default(),
            move |_goal, ctx| {
                let seeded = Arc::clone(&seeded_for_handler);
                async move {
                    *seeded.lock().unwrap() = ctx.scope().remaining();
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    ctx.succeed(&DummyResultMsg { value: "done" }).await
                }
            },
        )
        .await
        .expect("serve action");
        (server_node, server)
    });

    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();
    let mut client: aorta_action_client_t = 0;
    let opts2 = aorta_action_client_options_t {
        control_timeout_ms: 3000,
        info_timeout_ms: 1000,
    };
    assert_eq!(
        unsafe {
            aorta_action_client_new_with_options(
                node,
                action_name.as_ptr(),
                ptr::addr_of!(opts2),
                &mut client,
            )
        },
        AORTA_OK
    );
    // Discovery BEFORE the budgeted scope exists, so none of it is spent here.
    let mut info_bytes = aorta_owned_bytes_t::default();
    let mut rc = AORTA_ERR_NOT_FOUND;
    for _ in 0..50 {
        rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
        if rc == AORTA_OK {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };

    const LEG2_BUDGET: Duration = Duration::from_millis(1500);
    let scope_opts2 = aorta_scope_opts_t {
        priority: 255,
        supervision: crate::ffi::c_api::flow::AORTA_SUPERVISION_FAIL_FAST,
        _pad: [0; 2],
        budget_ns: LEG2_BUDGET.as_nanos() as u64,
        name: ptr::null(),
    };
    let mut scope2: u64 = 0;
    assert_eq!(
        unsafe { aorta_scope_child(0, ptr::addr_of!(scope_opts2), &mut scope2) },
        AORTA_OK
    );

    // send_goal under the open-budget scope: accepted fast (admission replies
    // before the handler finishes), and the wire stamp seeds the goal scope.
    let goal_id2 = "550e8400-e29b-41d4-a716-446655440078";
    let request2 = DummyAction::encode_send_goal_request(goal_id2, &DummyGoalMsg { value: "run" })
        .expect("encode send_goal request");
    let mut request2_slice = aorta_bytes_slice_t::default();
    let request2_view = view_from_bytes(&request2, &mut request2_slice);
    let (tx2, rx2) = mpsc::channel::<(aorta_result_t, Vec<u8>)>();
    let capture2 = Box::into_raw(Box::new(CtxCapture { tx: tx2 }));
    assert_eq!(
        unsafe {
            crate::ffi::c_api::action_client::aorta_action_client_send_goal_ctx(
                client,
                scope2,
                ptr::addr_of!(request2_view),
                Some(ctx_done_callback),
                capture2 as *mut c_void,
            )
        },
        AORTA_OK
    );
    let (code, payload) = rx2
        .recv_timeout(Duration::from_secs(5))
        .expect("send_goal_ctx delivers");
    assert_eq!(code, AORTA_OK);
    let send_goal = decode_send_goal_response(&payload).expect("decode send_goal response");
    assert!(send_goal.accepted);

    // Server-side wire-stamp proof: the goal scope was seeded from the
    // attachment — a real, finite budget ≤ the client scope's.
    let handler_seen = {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(remaining) = *seeded.lock().unwrap() {
                break remaining;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "handler never observed a seeded goal budget"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    assert!(
        handler_seen > Duration::ZERO && handler_seen <= LEG2_BUDGET,
        "goal scope must be seeded from the wire budget, got {handler_seen:?}"
    );

    // The budget must still be OPEN here — this is the entry condition the
    // old tests never established.
    let (mut has, mut remaining_ns): (u8, u64) = (0, 0);
    assert_eq!(
        unsafe {
            aorta_budget_remaining_ns(
                scope2,
                ptr::addr_of_mut!(has),
                ptr::addr_of_mut!(remaining_ns),
            )
        },
        AORTA_OK
    );
    assert_eq!(has, 1);
    assert!(
        remaining_ns > 500_000_000,
        "budget must be comfortably open at wait_result entry ({remaining_ns}ns left)"
    );

    let get_result_request =
        DummyAction::encode_get_result_request(goal_id2, &send_goal.goal_secret)
            .expect("encode get_result request");
    let goal_id2_c = CString::new(goal_id2).unwrap();
    let (tx3, rx3) = mpsc::channel::<(aorta_result_t, Vec<u8>)>();
    let capture3 = Box::into_raw(Box::new(CtxCapture { tx: tx3 }));
    let leg2_start = std::time::Instant::now();
    assert_eq!(
        unsafe {
            crate::ffi::c_api::action_client::aorta_action_client_wait_result_ctx(
                client,
                scope2,
                goal_id2_c.as_ptr(),
                get_result_request.as_ptr(),
                get_result_request.len(),
                10_000,
                Some(result_decode_status),
                Some(ctx_done_callback),
                capture3 as *mut c_void,
            )
        },
        AORTA_OK,
        "an OPEN budget must pass the entry gate"
    );

    // In-flight proof: while the budget is still open (and the terminal is 5s
    // away) NOTHING is delivered — the watcher is genuinely parked, not
    // refused at entry.
    assert!(
        rx3.recv_timeout(Duration::from_millis(300)).is_err(),
        "wait_result_ctx must still be in flight while the budget is open"
    );

    // Let the budget expire mid-flight, then observe it cooperatively
    // — the first checkpoint fires DeadlineExceeded.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let (mut has, mut ns): (u8, u64) = (0, u64::MAX);
        assert_eq!(
            unsafe {
                aorta_budget_remaining_ns(scope2, ptr::addr_of_mut!(has), ptr::addr_of_mut!(ns))
            },
            AORTA_OK
        );
        if has == 1 && ns == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "budget never reported expiry"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        aorta_scope_checkpoint(scope2),
        AORTA_ERR_CANCELLED,
        "checkpoint observes the expired budget into the token"
    );

    let (code, _) = rx3
        .recv_timeout(Duration::from_secs(5))
        .expect("mid-flight cancellation delivers");
    let leg2_elapsed = leg2_start.elapsed();
    assert_eq!(
        code, AORTA_ERR_CANCELLED,
        "budget expiry mid-flight resolves the parked wait as CANCELLED"
    );
    assert!(
        leg2_elapsed < Duration::from_secs(4),
        "cancellation must beat the 5s terminal; took {leg2_elapsed:?}"
    );
    // The winning reason is the deadline, not an explicit cancel.
    let mut flag: *const u8 = ptr::null();
    assert_eq!(
        unsafe { aorta_scope_cancelled_flag_ptr(scope2, &mut flag) },
        AORTA_OK
    );
    assert_eq!(unsafe { *flag }, AORTA_CANCEL_REASON_DEADLINE);

    unsafe {
        drop(Box::from_raw(capture1));
        drop(Box::from_raw(capture2));
        drop(Box::from_raw(capture3));
    }
    assert_eq!(aorta_scope_free(scope2), AORTA_OK);
    assert_eq!(aorta_action_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    runtime.block_on(async {
        server.stop_and_join(Duration::from_secs(1)).await;
        server_node.close().await.expect("close server node");
    });
}

// --- Action fault/timeout must carry the record ----

struct CtxRecordCapture {
    tx: mpsc::Sender<(aorta_result_t, bool, u8, u8, i32)>,
}

unsafe extern "C" fn ctx_record_callback(
    code: aorta_result_t,
    _payload: *const aorta_bytes_view_t,
    completion: *const crate::ffi::c_api::flow::aorta_flow_completion_t,
    user_data: *mut c_void,
) {
    if user_data.is_null() {
        return;
    }
    let capture = unsafe { &*(user_data as *const CtxRecordCapture) };
    let (has, kind, reason, rec_code) = if completion.is_null() {
        (false, 0, 0, 0)
    } else {
        let c = unsafe { &*completion };
        (true, c.stop_kind, c.cancel_reason, c.code)
    };
    let _ = capture.tx.send((code, has, kind, reason, rec_code));
}

#[test]
fn ffi_action_ctx_timeout_carries_a_deadline_record() {
    // Regression: a send_goal with no
    // server times out; the done callback used to receive a NULL record —
    // the deadline stop was invisible across the ABI. Contract: an
    // operation timeout is a CANCELLED record with DeadlineExceeded.
    let _action_guard = lock_action_test();
    let node_name = CString::new(unique_name("ffi_ctx_record_client")).unwrap();
    let group = CString::new("default").unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );
    let opts = aorta_action_client_options_t {
        control_timeout_ms: 300,
        info_timeout_ms: 300,
    };
    let mut client: aorta_action_client_t = 0;
    assert_eq!(
        unsafe {
            aorta_action_client_new_with_options(
                node,
                action_name.as_ptr(),
                std::ptr::addr_of!(opts),
                &mut client,
            )
        },
        AORTA_OK
    );
    let (tx, rx) = mpsc::channel();
    let capture = Box::leak(Box::new(CtxRecordCapture { tx }));
    let request = DummyAction::encode_send_goal_request(
        "550e8400-e29b-41d4-a716-446655440099",
        &DummyGoalMsg { value: "nobody" },
    )
    .expect("encode send_goal request");
    let mut slice = aorta_bytes_slice_t::default();
    let view = view_from_bytes(&request, &mut slice);
    assert_eq!(
        unsafe {
            crate::ffi::c_api::action_client::aorta_action_client_send_goal_ctx(
                client,
                0,
                ptr::addr_of!(view),
                Some(ctx_record_callback),
                capture as *const CtxRecordCapture as *mut c_void,
            )
        },
        AORTA_OK
    );
    let (code, has, kind, reason, rec_code) = rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("done callback fired");
    assert_eq!(code, AORTA_ERR_TIMEOUT, "legacy code stays TIMEOUT");
    assert!(has, "an operation timeout must carry a completion record");
    assert_eq!(
        kind,
        crate::ffi::c_api::flow::AORTA_STOP_KIND_CANCELLED,
        "a deadline is a cancellation, never a third vocabulary"
    );
    assert_eq!(reason, 2, "DeadlineExceeded byte");
    assert_eq!(rec_code, AORTA_ERR_CANCELLED, "normative record code");
    assert_eq!(
        crate::ffi::c_api::action_client::aorta_action_client_free(client),
        AORTA_OK
    );
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

// ---------------------------------------------------------------------------
// Raw action server dispatch: dedicated execute threads + cancel callback
// ---------------------------------------------------------------------------

fn raw_action_codec() -> aorta_action_server_codec_t {
    aorta_action_server_codec_t {
        decode_send_goal_request_goal_id: Some(
            ffi_decode_send_goal_request_goal_id as unsafe extern "C" fn(_, _) -> _,
        ),
        decode_cancel_goal_request: Some(
            ffi_decode_cancel_goal_request as unsafe extern "C" fn(_, _, _) -> _,
        ),
        decode_get_result_request: Some(
            ffi_decode_get_result_request as unsafe extern "C" fn(_, _, _) -> _,
        ),
        encode_send_goal_response: Some(
            ffi_encode_send_goal_response as unsafe extern "C" fn(_, _, _, _, _, _, _, _) -> _,
        ),
        encode_cancel_goal_response: Some(
            ffi_encode_cancel_goal_response as unsafe extern "C" fn(_, _, _, _, _, _) -> _,
        ),
        encode_public_status: Some(
            ffi_encode_public_status as unsafe extern "C" fn(_, _, _, _, _, _, _, _, _) -> _,
        ),
        encode_feedback: Some(
            ffi_encode_feedback as unsafe extern "C" fn(_, _, _, _, _, _, _) -> _,
        ),
        encode_get_result_response: Some(
            ffi_encode_get_result_response
                as unsafe extern "C" fn(_, _, _, _, _, _, _, _, _, _, _, _) -> _,
        ),
    }
}

fn raw_action_server_options(
    max_concurrency: usize,
    supports_cancel: bool,
) -> aorta_action_server_options_t {
    aorta_action_server_options_t {
        supports_cancel: u8::from(supports_cancel),
        max_concurrency,
        result_ttl_ms: 5000,
        dedup_ttl_ms: 10000,
    }
}

/// One raw (codec-driven) action server plus a connected raw client.
struct RawActionFixture {
    server_node: aorta_node_t,
    server: aorta_action_server_t,
    client_node: aorta_node_t,
    client: aorta_action_client_t,
}

impl RawActionFixture {
    /// `make_server` declares the server on the given node and returns
    /// `(rc, handle)`; the fixture then connects a client and waits until
    /// the server's info is visible.
    fn start(
        tag: &str,
        make_server: impl FnOnce(aorta_node_t, &CString) -> (aorta_result_t, aorta_action_server_t),
    ) -> Self {
        let group = CString::new(unique_name(&format!("{tag}_group"))).unwrap();
        let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();

        let server_name = CString::new(unique_name(&format!("{tag}_server"))).unwrap();
        let mut server_node: aorta_node_t = 0;
        let rc = unsafe { aorta_node_new(server_name.as_ptr(), group.as_ptr(), &mut server_node) };
        assert_eq!(rc, AORTA_OK);

        let (rc, server) = make_server(server_node, &action_name);
        assert_eq!(rc, AORTA_OK);
        assert_ne!(server, 0);

        let client_name = CString::new(unique_name(&format!("{tag}_client"))).unwrap();
        let mut client_node: aorta_node_t = 0;
        let rc = unsafe { aorta_node_new(client_name.as_ptr(), group.as_ptr(), &mut client_node) };
        assert_eq!(rc, AORTA_OK);
        let client_opts = aorta_action_client_options_t {
            control_timeout_ms: 3000,
            info_timeout_ms: 1000,
        };
        let mut client: aorta_action_client_t = 0;
        let rc = unsafe {
            aorta_action_client_new_with_options(
                client_node,
                action_name.as_ptr(),
                ptr::addr_of!(client_opts),
                &mut client,
            )
        };
        assert_eq!(rc, AORTA_OK);

        let mut info_bytes = aorta_owned_bytes_t::default();
        let mut rc = AORTA_ERR_NOT_FOUND;
        for _ in 0..50 {
            rc = unsafe { aorta_action_client_info(client, ptr::addr_of_mut!(info_bytes)) };
            if rc == AORTA_OK {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(rc, AORTA_OK, "action info never became visible");
        unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(info_bytes)) };

        Self {
            server_node,
            server,
            client_node,
            client,
        }
    }

    fn send_goal(&self, goal_id: &str) -> ActionSendGoalResponse {
        let request =
            DummyAction::encode_send_goal_request(goal_id, &DummyGoalMsg { value: "run" })
                .expect("encode send_goal request");
        let mut slice = aorta_bytes_slice_t::default();
        let view = view_from_bytes(&request, &mut slice);
        let mut out = aorta_owned_bytes_t::default();
        let rc = unsafe {
            aorta_action_client_send_goal(self.client, ptr::addr_of!(view), ptr::addr_of_mut!(out))
        };
        assert_eq!(rc, AORTA_OK);
        let response = decode_send_goal_response(&owned_bytes_to_vec(&out)).expect("decode");
        unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(out)) };
        response
    }

    fn cancel(&self, goal_id: &str, goal_secret: &str) -> ActionCancelGoalResponse {
        let request =
            DummyAction::encode_cancel_goal_request(goal_id, goal_secret).expect("encode cancel");
        let mut slice = aorta_bytes_slice_t::default();
        let view = view_from_bytes(&request, &mut slice);
        let mut out = aorta_owned_bytes_t::default();
        let rc = unsafe {
            aorta_action_client_cancel(self.client, ptr::addr_of!(view), ptr::addr_of_mut!(out))
        };
        assert_eq!(rc, AORTA_OK);
        let response = decode_cancel_goal_response(&owned_bytes_to_vec(&out)).expect("decode");
        unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(out)) };
        response
    }

    fn wait_terminal(&self, goal_id: &str, timeout: Duration) -> ActionPublicStatus {
        let goal_id_c = CString::new(goal_id).unwrap();
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let mut out = aorta_owned_bytes_t::default();
            let rc = unsafe {
                aorta_action_client_query_status(
                    self.client,
                    goal_id_c.as_ptr(),
                    ptr::addr_of_mut!(out),
                )
            };
            if rc == AORTA_OK {
                let status = decode_public_status(&owned_bytes_to_vec(&out)).expect("decode");
                unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(out)) };
                if status.status.is_terminal() {
                    return status;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "goal {goal_id} did not reach a terminal state in {timeout:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Frees the server first (callers that test release timing free it
    /// themselves and zero `server`), then the client side.
    fn shutdown(mut self) {
        self.free_server();
        assert_eq!(aorta_action_client_free(self.client), AORTA_OK);
        assert_eq!(aorta_node_free(self.client_node), AORTA_OK);
        assert_eq!(aorta_node_free(self.server_node), AORTA_OK);
    }

    fn free_server(&mut self) {
        if self.server != 0 {
            assert_eq!(aorta_action_server_free(self.server), AORTA_OK);
            self.server = 0;
        }
    }
}

fn succeed_and_free(ctx: crate::ffi::c_api::aorta_action_context_t) {
    let result = DummyAction::encode_result(&DummyResultMsg { value: "done" }).expect("result");
    let mut slice = aorta_bytes_slice_t::default();
    let view = view_from_bytes(&result, &mut slice);
    let _ = unsafe { aorta_action_context_succeed(ctx, ptr::addr_of!(view)) };
    let _ = aorta_action_context_free(ctx);
}

fn goal_id_for(index: usize) -> String {
    format!("7d3c1a52-9b1e-4c2f-8a6d-{index:012x}")
}

/// Thread name observed by the most recent raw execute dispatch.
fn execute_thread_names() -> &'static Mutex<Vec<Option<String>>> {
    static NAMES: OnceLock<Mutex<Vec<Option<String>>>> = OnceLock::new();
    NAMES.get_or_init(|| Mutex::new(Vec::new()))
}

unsafe extern "C" fn record_thread_and_succeed(
    _request: *const aorta_bytes_view_t,
    ctx: crate::ffi::c_api::aorta_action_context_t,
    _user_data: *mut c_void,
) {
    execute_thread_names()
        .lock()
        .unwrap()
        .push(std::thread::current().name().map(str::to_owned));
    succeed_and_free(ctx);
}

/// A raw execute handler may block until its goal ends, so it must run on
/// a thread of its own — never on the shared, CPU-sized offload pool.
#[test]
fn ffi_raw_action_execute_runs_on_a_dedicated_thread() {
    let _action_guard = lock_action_test();
    execute_thread_names().lock().unwrap().clear();
    let codec = raw_action_codec();
    let opts = raw_action_server_options(1, true);
    let fixture = RawActionFixture::start("ffi_raw_dispatch_thread", |node, name| {
        let mut server: aorta_action_server_t = 0;
        let rc = unsafe {
            aorta_action_server_new_with_options(
                node,
                name.as_ptr(),
                DummyAction::schema_bfbs().as_ptr(),
                DummyAction::schema_bfbs().len(),
                ptr::addr_of!(opts),
                ptr::addr_of!(codec),
                Some(record_thread_and_succeed as unsafe extern "C" fn(_, _, _)),
                ptr::null_mut(),
                &mut server,
            )
        };
        (rc, server)
    });

    let goal_id = goal_id_for(1);
    assert!(fixture.send_goal(&goal_id).accepted);
    let status = fixture.wait_terminal(&goal_id, Duration::from_secs(5));
    assert_eq!(status.status, ActionStatus::Succeeded);

    let names = execute_thread_names().lock().unwrap().clone();
    assert_eq!(names, vec![Some("aorta-action".to_string())]);
    fixture.shutdown();
}

/// Execute handlers that all block at the same time.
struct ConcurrencyProbe {
    entered: Mutex<usize>,
    all_entered: std::sync::Condvar,
    target: usize,
    saw_all: AtomicU64,
}

unsafe extern "C" fn block_until_all_entered(
    _request: *const aorta_bytes_view_t,
    ctx: crate::ffi::c_api::aorta_action_context_t,
    user_data: *mut c_void,
) {
    let probe = unsafe { &*(user_data as *const ConcurrencyProbe) };
    let mut entered = probe.entered.lock().unwrap();
    *entered += 1;
    probe.all_entered.notify_all();
    let (entered, _) = probe
        .all_entered
        .wait_timeout_while(entered, Duration::from_secs(5), |n| *n < probe.target)
        .unwrap();
    if *entered >= probe.target {
        probe.saw_all.fetch_add(1, Ordering::SeqCst);
    }
    drop(entered);
    succeed_and_free(ctx);
}

/// Regression for long-running goals starving new ones: with more
/// concurrently blocked goals than the offload pool has workers, every
/// execute handler must still be running at the same time.
#[test]
fn ffi_raw_action_blocked_goals_do_not_starve_new_goals() {
    let _action_guard = lock_action_test();
    let goals = crate::flow::offload::capacity() + 1;
    let probe = Box::new(ConcurrencyProbe {
        entered: Mutex::new(0),
        all_entered: std::sync::Condvar::new(),
        target: goals,
        saw_all: AtomicU64::new(0),
    });
    let probe_ptr = ptr::addr_of!(*probe) as *mut c_void;
    let codec = raw_action_codec();
    let opts = raw_action_server_options(goals, true);
    let fixture = RawActionFixture::start("ffi_raw_dispatch_starve", |node, name| {
        let mut server: aorta_action_server_t = 0;
        let rc = unsafe {
            aorta_action_server_new_with_options(
                node,
                name.as_ptr(),
                DummyAction::schema_bfbs().as_ptr(),
                DummyAction::schema_bfbs().len(),
                ptr::addr_of!(opts),
                ptr::addr_of!(codec),
                Some(block_until_all_entered as unsafe extern "C" fn(_, _, _)),
                probe_ptr,
                &mut server,
            )
        };
        (rc, server)
    });

    let ids: Vec<String> = (0..goals).map(goal_id_for).collect();
    for id in &ids {
        assert!(fixture.send_goal(id).accepted);
    }
    for id in &ids {
        let status = fixture.wait_terminal(id, Duration::from_secs(30));
        assert_eq!(status.status, ActionStatus::Succeeded);
    }
    fixture.shutdown();
    assert_eq!(
        probe.saw_all.load(Ordering::SeqCst),
        goals as u64,
        "not all {goals} execute handlers were running concurrently"
    );
}

/// Shared state for the cancel-callback and user-data-release tests.
#[derive(Default)]
struct CallbackProbe {
    cancelled_goals: Mutex<Vec<String>>,
    cancel_signal: std::sync::Condvar,
    executing: Mutex<bool>,
    executing_signal: std::sync::Condvar,
    proceed: Mutex<bool>,
    proceed_signal: std::sync::Condvar,
    released: AtomicU64,
}

impl CallbackProbe {
    fn into_user_data(probe: &Arc<Self>) -> *mut c_void {
        Arc::into_raw(Arc::clone(probe)) as *mut c_void
    }

    fn wait_executing(&self, timeout: Duration) -> bool {
        let guard = self.executing.lock().unwrap();
        let (guard, _) = self
            .executing_signal
            .wait_timeout_while(guard, timeout, |executing| !*executing)
            .unwrap();
        *guard
    }

    fn release_execute(&self) {
        *self.proceed.lock().unwrap() = true;
        self.proceed_signal.notify_all();
    }

    fn wait_cancel_callbacks(&self, count: usize, timeout: Duration) -> usize {
        let guard = self.cancelled_goals.lock().unwrap();
        let (guard, _) = self
            .cancel_signal
            .wait_timeout_while(guard, timeout, |goals| goals.len() < count)
            .unwrap();
        guard.len()
    }

    /// Waits (bounded) for the release, then checks it ran exactly once.
    fn assert_released_once(&self, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        while self.released.load(Ordering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "user data never released"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(self.released.load(Ordering::SeqCst), 1);
    }
}

unsafe extern "C" fn probe_release(user_data: *mut c_void) {
    let probe = unsafe { Arc::from_raw(user_data as *const CallbackProbe) };
    probe.released.fetch_add(1, Ordering::SeqCst);
}

unsafe extern "C" fn probe_cancel_requested(goal_id: aorta_string_view_t, user_data: *mut c_void) {
    let probe = unsafe { &*(user_data as *const CallbackProbe) };
    let goal_id = unsafe { string_view_to_owned(goal_id) };
    probe.cancelled_goals.lock().unwrap().push(goal_id);
    probe.cancel_signal.notify_all();
}

/// Execute that waits for the cancel callback and then for the test's
/// go-ahead, then ends the goal CANCELED.
unsafe extern "C" fn execute_until_cancel_callback(
    _request: *const aorta_bytes_view_t,
    ctx: crate::ffi::c_api::aorta_action_context_t,
    user_data: *mut c_void,
) {
    let probe = unsafe { &*(user_data as *const CallbackProbe) };
    {
        *probe.executing.lock().unwrap() = true;
        probe.executing_signal.notify_all();
    }
    probe.wait_cancel_callbacks(1, Duration::from_secs(5));
    let guard = probe.proceed.lock().unwrap();
    let (guard, _) = probe
        .proceed_signal
        .wait_timeout_while(guard, Duration::from_secs(10), |proceed| !*proceed)
        .unwrap();
    drop(guard);
    let message = CString::new("cancelled by callback").unwrap();
    let _ = aorta_action_context_cancel(ctx, ActionReplyCode::Canceled as i16, message.as_ptr());
    let _ = aorta_action_context_free(ctx);
}

/// Execute that blocks until the test lets it go, then succeeds.
unsafe extern "C" fn execute_until_released(
    _request: *const aorta_bytes_view_t,
    ctx: crate::ffi::c_api::aorta_action_context_t,
    user_data: *mut c_void,
) {
    let probe = unsafe { &*(user_data as *const CallbackProbe) };
    {
        *probe.executing.lock().unwrap() = true;
        probe.executing_signal.notify_all();
    }
    let guard = probe.proceed.lock().unwrap();
    let (guard, _) = probe
        .proceed_signal
        .wait_timeout_while(guard, Duration::from_secs(10), |proceed| !*proceed)
        .unwrap();
    drop(guard);
    succeed_and_free(ctx);
}

fn new_server_with_callbacks(
    node: aorta_node_t,
    name: &CString,
    opts: &aorta_action_server_options_t,
    codec: &aorta_action_server_codec_t,
    callbacks: &aorta_action_server_callbacks_t,
) -> (aorta_result_t, aorta_action_server_t) {
    let mut server: aorta_action_server_t = 0;
    let rc = unsafe {
        aorta_action_server_new_with_callbacks(
            node,
            name.as_ptr(),
            DummyAction::schema_bfbs().as_ptr(),
            DummyAction::schema_bfbs().len(),
            ptr::addr_of!(*opts),
            ptr::addr_of!(*codec),
            ptr::addr_of!(*callbacks),
            &mut server,
        )
    };
    (rc, server)
}

/// The cancel callback fires exactly once per goal, with its goal id,
/// as soon as the first cancel request is accepted.
#[test]
fn ffi_raw_action_cancel_callback_fires_once_with_goal_id() {
    let _action_guard = lock_action_test();
    let probe = Arc::new(CallbackProbe::default());
    let codec = raw_action_codec();
    let opts = raw_action_server_options(1, true);
    let callbacks = aorta_action_server_callbacks_t {
        execute: Some(execute_until_cancel_callback as unsafe extern "C" fn(_, _, _)),
        cancel_requested: Some(probe_cancel_requested as unsafe extern "C" fn(_, _)),
        user_data: CallbackProbe::into_user_data(&probe),
        user_data_release: Some(probe_release as unsafe extern "C" fn(_)),
    };
    let mut fixture = RawActionFixture::start("ffi_raw_cancel_cb", |node, name| {
        new_server_with_callbacks(node, name, &opts, &codec, &callbacks)
    });

    let goal_id = goal_id_for(7);
    let accepted = fixture.send_goal(&goal_id);
    assert!(accepted.accepted);
    assert!(probe.wait_executing(Duration::from_secs(5)));

    let first = fixture.cancel(&goal_id, &accepted.goal_secret);
    assert!(first.accepted);
    assert_eq!(probe.wait_cancel_callbacks(1, Duration::from_secs(5)), 1);
    // A repeated cancel while the goal is still CANCELING must not notify again.
    let repeated = fixture.cancel(&goal_id, &accepted.goal_secret);
    assert!(
        repeated.accepted,
        "a repeated cancel of a CANCELING goal is accepted"
    );
    std::thread::sleep(Duration::from_millis(200));
    probe.release_execute();
    let status = fixture.wait_terminal(&goal_id, Duration::from_secs(5));
    assert_eq!(status.status, ActionStatus::Canceled);
    let late = fixture.cancel(&goal_id, &accepted.goal_secret);
    assert!(!late.accepted);
    std::thread::sleep(Duration::from_millis(200));

    assert_eq!(*probe.cancelled_goals.lock().unwrap(), vec![goal_id]);
    assert_eq!(probe.released.load(Ordering::SeqCst), 0);
    fixture.free_server();
    probe.assert_released_once(Duration::from_secs(5));
    fixture.shutdown();
    assert_eq!(probe.cancelled_goals.lock().unwrap().len(), 1);
}

/// `user_data_release` runs exactly once, and only after the last
/// in-flight callback has returned — even when the server is freed while
/// an execute handler is still running.
#[test]
fn ffi_raw_action_user_data_outlives_in_flight_execute() {
    let _action_guard = lock_action_test();
    let probe = Arc::new(CallbackProbe::default());
    let codec = raw_action_codec();
    let opts = raw_action_server_options(1, true);
    let callbacks = aorta_action_server_callbacks_t {
        execute: Some(execute_until_released as unsafe extern "C" fn(_, _, _)),
        cancel_requested: None,
        user_data: CallbackProbe::into_user_data(&probe),
        user_data_release: Some(probe_release as unsafe extern "C" fn(_)),
    };
    let mut fixture = RawActionFixture::start("ffi_raw_release", |node, name| {
        new_server_with_callbacks(node, name, &opts, &codec, &callbacks)
    });

    assert!(fixture.send_goal(&goal_id_for(9)).accepted);
    assert!(probe.wait_executing(Duration::from_secs(5)));

    fixture.free_server();
    assert_eq!(
        probe.released.load(Ordering::SeqCst),
        0,
        "user data released while an execute handler was still running"
    );
    probe.release_execute();
    probe.assert_released_once(Duration::from_secs(5));
    fixture.shutdown();
}

/// Tracks how many execute handlers are running at once.
#[derive(Default)]
struct LingerProbe {
    running: AtomicU64,
    max_running: AtomicU64,
    first_done: AtomicBool,
    proceed: Mutex<bool>,
    proceed_signal: std::sync::Condvar,
    released: AtomicU64,
}

unsafe extern "C" fn linger_probe_release(user_data: *mut c_void) {
    let probe = unsafe { Arc::from_raw(user_data as *const LingerProbe) };
    probe.released.fetch_add(1, Ordering::SeqCst);
}

/// Succeeds its goal, then — for the first goal only — keeps running
/// until the test lets it go.
unsafe extern "C" fn execute_succeed_then_linger(
    _request: *const aorta_bytes_view_t,
    ctx: crate::ffi::c_api::aorta_action_context_t,
    user_data: *mut c_void,
) {
    let probe = unsafe { &*(user_data as *const LingerProbe) };
    let running = probe.running.fetch_add(1, Ordering::SeqCst) + 1;
    probe.max_running.fetch_max(running, Ordering::SeqCst);
    succeed_and_free(ctx);
    if !probe.first_done.swap(true, Ordering::SeqCst) {
        let guard = probe.proceed.lock().unwrap();
        let _ = probe
            .proceed_signal
            .wait_timeout_while(guard, Duration::from_secs(10), |proceed| !*proceed)
            .unwrap();
    }
    probe.running.fetch_sub(1, Ordering::SeqCst);
}

/// A handler still running after its goal is terminal holds its
/// execution slot: the next goal is accepted but its handler does not
/// start until that slot frees, so live execute threads never exceed
/// `max_concurrency`.
#[test]
fn ffi_raw_action_execute_threads_are_bounded_by_max_concurrency() {
    let _action_guard = lock_action_test();
    // Library-owned (released through the hook), so no handler can touch
    // it after the test drops its reference.
    let probe = Arc::new(LingerProbe::default());
    let codec = raw_action_codec();
    let opts = raw_action_server_options(1, true);
    let callbacks = aorta_action_server_callbacks_t {
        execute: Some(execute_succeed_then_linger as unsafe extern "C" fn(_, _, _)),
        cancel_requested: None,
        user_data: Arc::into_raw(Arc::clone(&probe)) as *mut c_void,
        user_data_release: Some(linger_probe_release as unsafe extern "C" fn(_)),
    };
    let mut fixture = RawActionFixture::start("ffi_raw_dispatch_linger", |node, name| {
        new_server_with_callbacks(node, name, &opts, &codec, &callbacks)
    });

    let first = goal_id_for(20);
    assert!(fixture.send_goal(&first).accepted);
    let status = fixture.wait_terminal(&first, Duration::from_secs(5));
    assert_eq!(status.status, ActionStatus::Succeeded);

    let second = goal_id_for(21);
    assert!(
        fixture.send_goal(&second).accepted,
        "a lingering handler must not make the server report busy"
    );
    std::thread::sleep(Duration::from_millis(300));

    *probe.proceed.lock().unwrap() = true;
    probe.proceed_signal.notify_all();
    let status = fixture.wait_terminal(&second, Duration::from_secs(5));
    assert_eq!(status.status, ActionStatus::Succeeded);
    // The release runs only after both handlers have returned, so every
    // update to `max_running` is visible from here on.
    fixture.free_server();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while probe.released.load(Ordering::SeqCst) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "user data never released"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    fixture.shutdown();
    assert_eq!(
        probe.max_running.load(Ordering::SeqCst),
        1,
        "two execute handlers ran at once with max_concurrency = 1"
    );
}

/// A constructor that fails before startup (argument validation) has
/// released the caller's user data, exactly once, by the time it returns.
#[test]
fn ffi_raw_action_constructor_failure_releases_user_data() {
    let _action_guard = lock_action_test();
    let group = CString::new(unique_name("ffi_raw_ctor_fail_group")).unwrap();
    let node_name = CString::new(unique_name("ffi_raw_ctor_fail")).unwrap();
    let action_name = CString::new(DummyAction::ACTION_NAME).unwrap();
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );
    let codec = raw_action_codec();

    // A cancel callback on a server that does not support cancel.
    let probe = Arc::new(CallbackProbe::default());
    let opts = raw_action_server_options(1, false);
    let callbacks = aorta_action_server_callbacks_t {
        execute: Some(execute_until_released as unsafe extern "C" fn(_, _, _)),
        cancel_requested: Some(probe_cancel_requested as unsafe extern "C" fn(_, _)),
        user_data: CallbackProbe::into_user_data(&probe),
        user_data_release: Some(probe_release as unsafe extern "C" fn(_)),
    };
    let (rc, server) = new_server_with_callbacks(node, &action_name, &opts, &codec, &callbacks);
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(server, 0);
    assert_eq!(probe.released.load(Ordering::SeqCst), 1);

    // A missing execute callback.
    let probe = Arc::new(CallbackProbe::default());
    let opts = raw_action_server_options(1, true);
    let callbacks = aorta_action_server_callbacks_t {
        execute: None,
        cancel_requested: None,
        user_data: CallbackProbe::into_user_data(&probe),
        user_data_release: Some(probe_release as unsafe extern "C" fn(_)),
    };
    let (rc, server) = new_server_with_callbacks(node, &action_name, &opts, &codec, &callbacks);
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(server, 0);
    assert_eq!(probe.released.load(Ordering::SeqCst), 1);

    assert_eq!(aorta_node_free(node), AORTA_OK);
}
