use std::collections::HashMap;

use flatbuffers_reflection::reflection;
use uuid::Uuid;

use crate::action_info_fb;
use crate::error::{AortaError, AortaResult};
use crate::key_expr::normalize_path;
use crate::schema::{SchemaHash, SchemaMeta, TypedSubscribeMessage};

pub const ACTION_INFO_ROOT_TYPE: &str = "aorta.action.common.ActionInfo";
pub const DEFAULT_ACTION_RESULT_TTL_MS: u64 = 60_000;
pub const DEFAULT_ACTION_DEDUP_TTL_MS: u64 = 300_000;

#[repr(i8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ActionStatus {
    #[default]
    Unknown = 0,
    Accepted = 1,
    Executing = 2,
    Canceling = 3,
    Succeeded = 4,
    Aborted = 5,
    Canceled = 6,
}

impl ActionStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Aborted | Self::Canceled)
    }
}

impl TryFrom<i8> for ActionStatus {
    type Error = AortaError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::Accepted),
            2 => Ok(Self::Executing),
            3 => Ok(Self::Canceling),
            4 => Ok(Self::Succeeded),
            5 => Ok(Self::Aborted),
            6 => Ok(Self::Canceled),
            other => Err(AortaError::invalid_argument(format!(
                "invalid action status discriminant {}",
                other
            ))),
        }
    }
}

#[repr(i16)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ActionReplyCode {
    #[default]
    None = 0,
    Ok = 1,
    DuplicateAccepted = 2,
    Succeeded = 100,
    Canceled = 101,
    Busy = 200,
    PayloadConflict = 201,
    Unauthorized = 202,
    NotFound = 203,
    AlreadyTerminal = 204,
    GoalExpired = 205,
    HandlerError = 300,
    HandlerPanic = 301,
    HandlerCompletedWithoutResult = 302,
    ContextDropped = 303,
    ServerCrash = 304,
    Conflict = 400,
}

impl TryFrom<i16> for ActionReplyCode {
    type Error = AortaError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Ok),
            2 => Ok(Self::DuplicateAccepted),
            100 => Ok(Self::Succeeded),
            101 => Ok(Self::Canceled),
            200 => Ok(Self::Busy),
            201 => Ok(Self::PayloadConflict),
            202 => Ok(Self::Unauthorized),
            203 => Ok(Self::NotFound),
            204 => Ok(Self::AlreadyTerminal),
            205 => Ok(Self::GoalExpired),
            300 => Ok(Self::HandlerError),
            301 => Ok(Self::HandlerPanic),
            302 => Ok(Self::HandlerCompletedWithoutResult),
            303 => Ok(Self::ContextDropped),
            304 => Ok(Self::ServerCrash),
            400 => Ok(Self::Conflict),
            other => Err(AortaError::invalid_argument(format!(
                "invalid action reply code discriminant {}",
                other
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ActionRootType {
    SendGoalRequest,
    SendGoalResponse,
    CancelGoalRequest,
    CancelGoalResponse,
    Feedback,
    PublicStatus,
    GetResultRequest,
    GetResultResponse,
}

impl ActionRootType {
    /// Every action surface. Used by
    /// [`resolve_action_root_types_from_bfbs`] to iterate the
    /// expected set, and by [`Self::match_by_suffix`] to find the
    /// surface a bfbs table belongs to.
    ///
    /// Order doesn't affect correctness — no surface suffix is a
    /// proper suffix of another (`SendGoalRequest` / `SendGoalResponse`
    /// are disjoint, `*FeedbackData` doesn't end in `Feedback`, etc.) —
    /// but listing them in protocol order keeps error messages
    /// predictable.
    pub const ALL: [ActionRootType; 8] = [
        Self::SendGoalRequest,
        Self::SendGoalResponse,
        Self::CancelGoalRequest,
        Self::CancelGoalResponse,
        Self::Feedback,
        Self::PublicStatus,
        Self::GetResultRequest,
        Self::GetResultResponse,
    ];

    pub fn suffix(self) -> &'static str {
        match self {
            Self::SendGoalRequest => "SendGoalRequest",
            Self::SendGoalResponse => "SendGoalResponse",
            Self::CancelGoalRequest => "CancelGoalRequest",
            Self::CancelGoalResponse => "CancelGoalResponse",
            Self::Feedback => "Feedback",
            Self::PublicStatus => "PublicStatus",
            Self::GetResultRequest => "GetResultRequest",
            Self::GetResultResponse => "GetResultResponse",
        }
    }

    /// Match a bare FlatBuffers table name (without its namespace
    /// prefix) against the eight surface suffixes. The bare name must
    /// be strictly longer than the suffix (so the suffix is identifying
    /// a prefix-stem, not the whole table). Suffixes are disjoint so
    /// at most one surface can match.
    pub fn match_by_suffix(bare_name: &str) -> Option<Self> {
        for surface in Self::ALL {
            let suffix = surface.suffix();
            if bare_name.ends_with(suffix) && bare_name.len() > suffix.len() {
                return Some(surface);
            }
        }
        None
    }
}

/// Fully-qualified FlatBuffers root type names for all eight action
/// surfaces (`SendGoalRequest`, `Feedback`, `GetResultResponse`, ...).
///
/// Resolved at runtime from the action's compiled `.bfbs` schema by
/// [`resolve_action_root_types_from_bfbs`] (or, in typed wrapper code
/// paths, looked up directly via the generated `SchemaMeta::root_type()`
/// associated functions). The previous name-derivation path
/// (`derive_action_root_type_names`) was removed because action
/// names are pure Zenoh routing strings now, free of FlatBuffers
/// identifier constraints (so Unicode, hyphens, and leading digits
/// are accepted on the wire).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionRootTypeNames {
    /// Map from each surface (8 variants of `ActionRootType`) to the
    /// fully-qualified FlatBuffers root type name found in the bfbs
    /// (or declared by the generated typed wrapper).
    by_surface: HashMap<ActionRootType, String>,
}

impl ActionRootTypeNames {
    /// Construct an [`ActionRootTypeNames`] from an explicit map. Used
    /// by the typed wrapper code path that knows the strings at
    /// codegen time. Returns an error if any of the eight surfaces is
    /// missing.
    pub fn from_map(by_surface: HashMap<ActionRootType, String>) -> AortaResult<Self> {
        for surface in ActionRootType::ALL {
            if !by_surface.contains_key(&surface) {
                return Err(AortaError::invalid_argument(format!(
                    "ActionRootTypeNames missing surface {:?}",
                    surface
                )));
            }
        }
        Ok(Self { by_surface })
    }

    pub fn for_surface(&self, root_type: ActionRootType) -> String {
        self.by_surface.get(&root_type).cloned().unwrap_or_else(|| {
            // Construction enforces all 8 surfaces are present, so
            // this branch is unreachable for any
            // ActionRootTypeNames returned from
            // resolve_action_root_types_from_bfbs / from_map.
            panic!("ActionRootTypeNames missing surface {:?}", root_type)
        })
    }
}

/// Resolve the fully-qualified FlatBuffers root type name for each
/// action surface by scanning the compiled `.bfbs`.
///
/// The compiled schema for an action always contains exactly one table
/// per surface (`*SendGoalRequest`, `*SendGoalResponse`,
/// `*CancelGoalRequest`, `*CancelGoalResponse`, `*Feedback`,
/// `*PublicStatus`, `*GetResultRequest`, `*GetResultResponse`) — this
/// is enforced by `message/tools/gen_action_wrapper.py` at codegen
/// time. We pattern-match on the type name suffix, exclude
/// `aorta.action.common.*` (shared types like `ActionReplyCode`), and
/// require an unambiguous one-to-one mapping.
///
/// Replaces the old name-derivation path (`derive_action_root_type_
/// names`) which projected `action_name` segments into the FlatBuffers
/// identifier set and forced action names to stay ASCII-only. With
/// this lookup the action name is purely a Zenoh routing string and
/// can use the full keyexpr-relaxed character set.
pub fn resolve_action_root_types_from_bfbs(bfbs: &[u8]) -> AortaResult<ActionRootTypeNames> {
    if bfbs.is_empty() {
        return Err(AortaError::invalid_argument(
            "action bfbs is empty; cannot resolve root type names",
        ));
    }
    let schema = reflection::root_as_schema(bfbs).map_err(|err| {
        AortaError::invalid_argument(format!("failed to parse action bfbs: {}", err))
    })?;

    let mut by_surface: HashMap<ActionRootType, String> = HashMap::new();
    for object in schema.objects().iter() {
        let qualified = object.name();
        // The shared `aorta.action.common.*` namespace declares
        // `ActionReplyCode`, `ActionStatus`, etc. — those are used by
        // every action and must be skipped here, otherwise the suffix
        // match would be ambiguous.
        if qualified.starts_with("aorta.action.common.") {
            continue;
        }
        // Compare against the bare table name (everything after the
        // last namespace separator) so a stray namespace component
        // ending in `Feedback` can't collide with the surface match.
        let bare = qualified
            .rsplit_once('.')
            .map(|(_, t)| t)
            .unwrap_or(qualified);
        let Some(surface) = ActionRootType::match_by_suffix(bare) else {
            continue;
        };
        if let Some(prev) = by_surface.insert(surface, qualified.to_string()) {
            return Err(AortaError::invalid_argument(format!(
                "action bfbs contains multiple tables matching surface \
                 {:?}: '{}' and '{}'",
                surface, prev, qualified
            )));
        }
    }

    for surface in ActionRootType::ALL {
        if !by_surface.contains_key(&surface) {
            return Err(AortaError::invalid_argument(format!(
                "action bfbs missing table for surface {:?} \
                 (expected exactly one table ending in '{}')",
                surface,
                surface.suffix()
            )));
        }
    }

    Ok(ActionRootTypeNames { by_surface })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionInfo {
    pub action_name: String,
    pub schema_hash: SchemaHash,
    pub supports_cancel: bool,
    pub max_concurrency: u32,
    pub result_ttl_ms: u64,
    pub dedup_ttl_ms: u64,
    pub server_node_name: String,
    pub server_epoch: String,
}

impl ActionInfo {
    pub fn validate(&self) -> AortaResult<()> {
        // Action names are pure Zenoh routing strings under the
        // keyexpr-relaxed rules. The previous FlatBuffers-
        // identifier check has been replaced by bfbs-based root-type
        // resolution (`resolve_action_root_types_from_bfbs`), which
        // happens at action declaration time and does not constrain
        // the wire name.
        let normalized = normalize_path(&self.action_name)?;
        if normalized != self.action_name {
            return Err(AortaError::invalid_argument(format!(
                "action name must already be normalized: expected '{}', got '{}'",
                normalized, self.action_name
            )));
        }
        if self.server_node_name.trim().is_empty() {
            return Err(AortaError::invalid_argument(
                "action info server_node_name is empty",
            ));
        }
        if self.max_concurrency == 0 {
            return Err(AortaError::invalid_argument(
                "action info max_concurrency must be greater than zero",
            ));
        }
        Uuid::parse_str(self.server_epoch.trim()).map_err(|err| {
            AortaError::invalid_argument(format!(
                "invalid action server_epoch '{}': {}",
                self.server_epoch, err
            ))
        })?;
        if self.dedup_ttl_ms < self.result_ttl_ms {
            return Err(AortaError::invalid_argument(format!(
                "action dedup_ttl_ms ({}) must be >= result_ttl_ms ({})",
                self.dedup_ttl_ms, self.result_ttl_ms
            )));
        }
        Ok(())
    }

    pub fn encode(&self) -> AortaResult<Vec<u8>> {
        self.validate()?;
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let action_name = builder.create_string(&self.action_name);
        let schema_hash_hex = builder.create_string(&self.schema_hash.to_hex());
        let server_node_name = builder.create_string(&self.server_node_name);
        let server_epoch = builder.create_string(&self.server_epoch);
        let root = action_info_fb::create_action_info(
            &mut builder,
            Some(action_name),
            Some(schema_hash_hex),
            self.supports_cancel,
            self.max_concurrency,
            self.result_ttl_ms,
            self.dedup_ttl_ms,
            Some(server_node_name),
            Some(server_epoch),
        );
        builder.finish(root, None);
        Ok(builder.finished_data().to_vec())
    }

    pub fn decode(bytes: &[u8]) -> AortaResult<Self> {
        let root = action_info_fb::root_as_action_info(bytes)
            .ok_or_else(|| AortaError::invalid_argument("invalid ActionInfo flatbuffer payload"))?;
        let action_name = root
            .action_name()
            .ok_or_else(|| AortaError::invalid_argument("ActionInfo.action_name is missing"))?;
        let schema_hash_hex = root
            .schema_hash_hex()
            .ok_or_else(|| AortaError::invalid_argument("ActionInfo.schema_hash_hex is missing"))?;
        let schema_hash = SchemaHash::from_hex_str(schema_hash_hex).ok_or_else(|| {
            AortaError::invalid_argument(format!(
                "invalid ActionInfo.schema_hash_hex '{}'",
                schema_hash_hex
            ))
        })?;
        let server_node_name = root.server_node_name().ok_or_else(|| {
            AortaError::invalid_argument("ActionInfo.server_node_name is missing")
        })?;
        let server_epoch = root
            .server_epoch()
            .ok_or_else(|| AortaError::invalid_argument("ActionInfo.server_epoch is missing"))?;

        let info = Self {
            action_name: action_name.to_string(),
            schema_hash,
            supports_cancel: root.supports_cancel(),
            max_concurrency: root.max_concurrency(),
            result_ttl_ms: root.result_ttl_ms(),
            dedup_ttl_ms: root.dedup_ttl_ms(),
            server_node_name: server_node_name.to_string(),
            server_epoch: server_epoch.to_string(),
        };
        info.validate()?;
        Ok(info)
    }
}

pub trait ActionSchema {
    type Goal: SchemaMeta;
    type Result: SchemaMeta;
    type FeedbackData: SchemaMeta;
    type SendGoalRequest: SchemaMeta;
    type SendGoalResponse: SchemaMeta;
    type CancelGoalRequest: SchemaMeta;
    type CancelGoalResponse: SchemaMeta;
    type Feedback: SchemaMeta;
    type PublicStatus: SchemaMeta;
    type GetResultRequest: SchemaMeta;
    type GetResultResponse: SchemaMeta;

    const ACTION_NAME: &'static str;

    fn schema_bfbs() -> &'static [u8] {
        Self::Goal::schema_bfbs()
    }

    fn schema_hash() -> SchemaHash {
        Self::Goal::schema_hash()
    }

    /// Fully-qualified FlatBuffers root type for a specific action
    /// surface, taken directly from the associated `SchemaMeta` types
    /// (the generated typed wrapper crate hardwires these strings at
    /// codegen time, so there's no runtime derivation).
    ///
    /// Was previously derived from `ACTION_NAME` via path-projection
    /// (`aorta.action.<segments>.<PascalCase>{Suffix}`); that path
    /// constrained action names to the FlatBuffers identifier set
    /// (ASCII letters/digits/`_`). That dependency was cut so
    /// action names are pure Zenoh routing strings; root-type lookups
    /// for runtime CLI/introspect paths now go through
    /// [`resolve_action_root_types_from_bfbs`] instead.
    fn root_type_for(surface: ActionRootType) -> &'static str {
        let resolved = match surface {
            ActionRootType::SendGoalRequest => Self::SendGoalRequest::root_type(),
            ActionRootType::SendGoalResponse => Self::SendGoalResponse::root_type(),
            ActionRootType::CancelGoalRequest => Self::CancelGoalRequest::root_type(),
            ActionRootType::CancelGoalResponse => Self::CancelGoalResponse::root_type(),
            ActionRootType::Feedback => Self::Feedback::root_type(),
            ActionRootType::PublicStatus => Self::PublicStatus::root_type(),
            ActionRootType::GetResultRequest => Self::GetResultRequest::root_type(),
            ActionRootType::GetResultResponse => Self::GetResultResponse::root_type(),
        };
        resolved.expect(
            "generated action wrapper must declare SchemaMeta::root_type() \
             for every surface — regenerate the typed wrapper crate from \
             the action .fbs",
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionSendGoalResponse {
    pub accepted: bool,
    pub goal_id: String,
    pub goal_secret: String,
    pub code: ActionReplyCode,
    pub status: ActionStatus,
    pub message: String,
    pub server_epoch: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionCancelGoalResponse {
    pub accepted: bool,
    pub goal_id: String,
    pub code: ActionReplyCode,
    pub status: ActionStatus,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionGetResultResponse<TFeedbackData, TResult> {
    pub goal_id: String,
    pub status: ActionStatus,
    pub accepted_stamp_ns: u64,
    pub started_stamp_ns: u64,
    pub finished_stamp_ns: u64,
    pub last_feedback_seq: u64,
    pub feedback_snapshot: TFeedbackData,
    pub result: Option<TResult>,
    pub code: ActionReplyCode,
    pub message: String,
    pub server_epoch: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionPublicStatus {
    pub goal_id: String,
    pub status: ActionStatus,
    pub code: ActionReplyCode,
    pub accepted_stamp_ns: u64,
    pub started_stamp_ns: u64,
    pub finished_stamp_ns: u64,
    pub last_feedback_seq: u64,
    pub server_epoch: String,
}

pub trait ActionWire: ActionSchema {
    type GoalMsg<'a>: Default
    where
        Self: 'a;
    type OwnedFeedbackDataMsg: Clone + Default + Send + Sync + 'static;
    type OwnedResultMsg: Clone + Default + Send + Sync + 'static;
    type ResultMsg<'a>: Default
    where
        Self: 'a;
    type FeedbackDataMsg<'a>: Default
    where
        Self: 'a;

    fn encode_send_goal_request(goal_id: &str, goal: &Self::GoalMsg<'_>) -> AortaResult<Vec<u8>>;

    fn decode_send_goal_request_goal_id(payload: &[u8]) -> AortaResult<String>;

    fn decode_goal_from_send_goal_request<'a>(
        payload: &'a [u8],
    ) -> AortaResult<<Self::Goal as TypedSubscribeMessage>::Root<'a>>
    where
        Self::Goal: TypedSubscribeMessage;

    fn encode_send_goal_response(response: &ActionSendGoalResponse) -> AortaResult<Vec<u8>>;

    fn decode_send_goal_response(payload: &[u8]) -> AortaResult<ActionSendGoalResponse>;

    fn encode_cancel_goal_request(goal_id: &str, goal_secret: &str) -> AortaResult<Vec<u8>>;

    fn decode_cancel_goal_request(payload: &[u8]) -> AortaResult<(String, String)>;

    fn encode_cancel_goal_response(response: &ActionCancelGoalResponse) -> AortaResult<Vec<u8>>;

    fn decode_cancel_goal_response(payload: &[u8]) -> AortaResult<ActionCancelGoalResponse>;

    fn encode_get_result_request(goal_id: &str, goal_secret: &str) -> AortaResult<Vec<u8>>;

    fn decode_get_result_request(payload: &[u8]) -> AortaResult<(String, String)>;

    fn clone_feedback_data_owned(
        data: &Self::FeedbackDataMsg<'_>,
    ) -> AortaResult<Self::OwnedFeedbackDataMsg>;

    fn clone_result_owned(result: &Self::ResultMsg<'_>) -> AortaResult<Self::OwnedResultMsg>;

    fn encode_get_result_response(
        response: &ActionGetResultResponse<Self::OwnedFeedbackDataMsg, Self::OwnedResultMsg>,
    ) -> AortaResult<Vec<u8>>;

    fn decode_get_result_response(
        payload: &[u8],
    ) -> AortaResult<ActionGetResultResponse<Self::OwnedFeedbackDataMsg, Self::OwnedResultMsg>>;

    fn encode_feedback(
        goal_id: &str,
        seq: u64,
        stamp_ns: u64,
        status: ActionStatus,
        data: &Self::FeedbackDataMsg<'_>,
        message: Option<&str>,
    ) -> AortaResult<Vec<u8>>;

    fn decode_feedback_goal_id(payload: &[u8]) -> AortaResult<String>;

    fn encode_feedback_data(data: &Self::FeedbackDataMsg<'_>) -> AortaResult<Vec<u8>>;

    fn encode_result(result: &Self::ResultMsg<'_>) -> AortaResult<Vec<u8>>;

    fn encode_public_status(status: &ActionPublicStatus) -> AortaResult<Vec<u8>>;

    fn decode_public_status(payload: &[u8]) -> AortaResult<ActionPublicStatus>;
}

pub fn goal_id_to_key(goal_id: &str) -> AortaResult<String> {
    let uuid = Uuid::parse_str(goal_id.trim()).map_err(|err| {
        AortaError::invalid_argument(format!("invalid action goal_id '{}': {}", goal_id, err))
    })?;
    Ok(uuid.simple().to_string())
}

pub fn goal_key_to_id(goal_key: &str) -> AortaResult<String> {
    let trimmed = goal_key.trim();
    if trimmed.len() != 32 {
        return Err(AortaError::invalid_argument(format!(
            "invalid action goal_key '{}': expected 32 hex characters",
            goal_key
        )));
    }
    let uuid = Uuid::parse_str(trimmed).map_err(|err| {
        AortaError::invalid_argument(format!("invalid action goal_key '{}': {}", goal_key, err))
    })?;
    Ok(uuid.hyphenated().to_string())
}

// `derive_action_root_type_names` / `pascal_case` were removed.
// The runtime CLI/introspect paths now call
// `resolve_action_root_types_from_bfbs` (above) instead of
// re-projecting `action_name` segments into FlatBuffers identifiers.
// The typed wrapper path uses `ActionSchema::root_type_for(surface)`,
// which dispatches via the generated associated `SchemaMeta` types.
