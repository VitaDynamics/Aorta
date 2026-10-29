use unicode_normalization::UnicodeNormalization;
use zenoh::key_expr::KeyExpr;

use crate::action::goal_id_to_key;
use crate::error::{AortaError, AortaResult};
use crate::runtime_env;
use crate::schema::SchemaHash;

const ROOT_PREFIX: &str = "aorta";
const SYS_SCHEMA_PREFIX: &str = "aorta/sys/schema";

/// Maximum UTF-8 byte length of a single key segment. Raised from the
/// original 64-byte cap so multi-byte Unicode segments (Chinese names,
/// emoji in dev/test keys) fit comfortably.
const MAX_SEGMENT_LEN: usize = 128;
/// Maximum UTF-8 byte length of the full suffix (everything after
/// `aorta/{group}/`). Raised in lockstep with `MAX_SEGMENT_LEN`.
const MAX_KEY_SUFFIX_LEN: usize = 512;
/// Group cap stays small — the group is part of every key and we want
/// it identifiable at a glance.
const MAX_GROUP_LEN: usize = 32;

pub const DEFAULT_GROUP: &str = "default";

#[derive(Debug, Clone, Copy)]
enum WildcardMode {
    /// Publish-side / resource-owner keys. Wildcards are rejected: you
    /// cannot publish to a glob.
    Reject,
    /// Subscribe-side / query keys. The whole-segment wildcards `*`
    /// (single chunk) and `**` (multi-chunk) are accepted verbatim;
    /// other Zenoh-reserved metacharacters (`$`, `#`, `?`) remain
    /// rejected. Chunk-wildcards `$*` inside a segment are deferred
    /// and currently rejected.
    Allow,
}

/// Whether a wildcard application-level namespace (`*` / `**`) is permitted for
/// a given key surface. Independent of the path [`WildcardMode`]: a service
/// client allows wildcards in the *service name* (`WildcardMode::Allow`) but must
/// still target a single robot, so its *namespace* is `NamespaceMode::Reject`.
/// See `spec/Aorta_Cloud_Cross_Namespace_Addressing_Proposal.md` §4.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NamespaceMode {
    /// Observe-side (topic / feedback / status subscribe): wildcard namespace OK.
    Allow,
    /// Request/reply, publish, owner: namespace must resolve to a single target.
    Reject,
}

/// Telemetry tier — the volume/priority class encoded into the key
/// (`sys/telemetry/{tier}/...`) so a single wildcard selects each plane: the
/// cloud uplink subscribes `vital/**`, the MCAP recorder captures both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryTier {
    /// Low-frequency, fleet-critical, cloud-uplinked. Bounded byte rate.
    Vital,
    /// High-frequency / high-volume, MCAP-recorded locally.
    Bulk,
}

impl TelemetryTier {
    pub fn as_str(self) -> &'static str {
        match self {
            TelemetryTier::Vital => "vital",
            TelemetryTier::Bulk => "bulk",
        }
    }
}

#[derive(Debug, Clone)]
pub struct KeyExprBuilder {
    group: String,
    /// Optional application-level namespace prefix (e.g. a robot SN) prepended
    /// ahead of `aorta/{group}/...`. `None` = pre-feature behavior. Distinct
    /// from the Zenoh session namespace; see
    /// `spec/Aorta_Cloud_Cross_Namespace_Addressing_Proposal.md`.
    namespace: Option<String>,
}

impl KeyExprBuilder {
    pub fn new(group: impl AsRef<str>) -> AortaResult<Self> {
        let group = normalize_group(group.as_ref())?;
        Ok(Self {
            group,
            namespace: None,
        })
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    /// Derive a builder bound to an application-level namespace prefix (e.g. a
    /// robot SN). The namespace is validated per-segment with subscribe-side
    /// rules (NFC, per-segment charset, whole-segment `*`/`**` allowed). Whether
    /// a wildcard namespace is actually accepted depends on the primitive
    /// (observe vs request/reply, see [`NamespaceMode`]). A segment equal to the
    /// key root `aorta` is rejected: it is the reverse-parse anchor used by
    /// [`KeyExprBuilder::split_full_key`].
    pub fn with_namespace(mut self, namespace: impl AsRef<str>) -> AortaResult<Self> {
        let ns = normalize_path_with_wildcards(namespace.as_ref())?;
        if ns.split('/').any(|seg| seg == ROOT_PREFIX) {
            return Err(AortaError::invalid_argument(
                "namespace segment cannot equal 'aorta' (reserved key root / reverse-parse anchor)",
            ));
        }
        self.namespace = Some(ns);
        Ok(self)
    }

    /// The application-level namespace prefix, if any.
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    // ---------- publish-side / owner keys (wildcards rejected) ----------

    /// Build a publisher topic key. The topic name is validated as a
    /// concrete path — wildcards are rejected because you cannot publish
    /// to a glob.
    pub fn topic_key_pub(&self, topic: &str) -> AortaResult<KeyExpr<'static>> {
        self.build_with_primitive("pub", topic, WildcardMode::Reject, NamespaceMode::Reject)
    }

    /// Build a service-server key. Wildcards rejected.
    pub fn service_key_server(&self, service: &str) -> AortaResult<KeyExpr<'static>> {
        self.build_with_primitive("srv", service, WildcardMode::Reject, NamespaceMode::Reject)
    }

    /// Build a service-server key that accepts callers from every application
    /// namespace, including namespace-free callers.
    pub fn service_key_server_all_namespaces(
        &self,
        service: &str,
    ) -> AortaResult<KeyExpr<'static>> {
        if self.namespace.is_some() {
            return Err(AortaError::invalid_argument(
                "an all-namespaces service cannot use an application-level namespace",
            ));
        }
        let base: String = self.service_key_server(service)?.into();
        key_expr_from_owned(format!("**/{}", base))
    }

    pub fn action_send_goal_key(&self, action: &str) -> AortaResult<KeyExpr<'static>> {
        self.action_key(
            "send_goal",
            action,
            WildcardMode::Reject,
            NamespaceMode::Reject,
        )
    }

    pub fn action_cancel_goal_key(&self, action: &str) -> AortaResult<KeyExpr<'static>> {
        self.action_key(
            "cancel_goal",
            action,
            WildcardMode::Reject,
            NamespaceMode::Reject,
        )
    }

    /// Action feedback publisher key (server side). Wildcards rejected.
    pub fn action_feedback_key_pub(&self, action: &str) -> AortaResult<KeyExpr<'static>> {
        self.action_key(
            "feedback",
            action,
            WildcardMode::Reject,
            NamespaceMode::Reject,
        )
    }

    /// Action get-result queryable key (server side). Wildcards rejected.
    pub fn action_get_result_key_pub(&self, action: &str) -> AortaResult<KeyExpr<'static>> {
        self.action_key(
            "get_result",
            action,
            WildcardMode::Reject,
            NamespaceMode::Reject,
        )
    }

    pub fn action_info_key(&self, action: &str) -> AortaResult<KeyExpr<'static>> {
        self.action_key("info", action, WildcardMode::Reject, NamespaceMode::Reject)
    }

    pub fn action_status_key(&self, action: &str, goal_id: &str) -> AortaResult<KeyExpr<'static>> {
        let goal_key = goal_id_to_key(goal_id)?;
        self.action_status_key_from_goal_key(action, &goal_key)
    }

    pub fn action_status_key_from_goal_key(
        &self,
        action: &str,
        goal_key: &str,
    ) -> AortaResult<KeyExpr<'static>> {
        let suffix = self.action_status_suffix(action, goal_key)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    /// Parameter publisher/owner key. Wildcards rejected (an owner
    /// declares a single parameter, not a glob).
    pub fn param_key_owner(&self, node: &str, param: &str) -> AortaResult<KeyExpr<'static>> {
        let node_norm = normalize_path_inner(node, WildcardMode::Reject)?;
        let param_norm = normalize_path_inner(param, WildcardMode::Reject)?;
        let suffix = format!("prm/{}/{}", node_norm, param_norm);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    pub fn schema_key(hash: &SchemaHash) -> AortaResult<KeyExpr<'static>> {
        let full = format!("{}/{}", SYS_SCHEMA_PREFIX, hash.to_hex());
        key_expr_from_owned(full)
    }

    pub fn schema_by_key(&self, key_suffix: &str) -> AortaResult<KeyExpr<'static>> {
        let suffix_norm = normalize_path_inner(key_suffix, WildcardMode::Reject)?;
        let suffix = format!("sys/schema_by_key/{}", suffix_norm);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    pub fn system_key(
        &self,
        subsystem: &str,
        node_name: &str,
        node_id: &str,
    ) -> AortaResult<KeyExpr<'static>> {
        let subsystem_norm = normalize_path_inner(subsystem, WildcardMode::Reject)?;
        let node_name_norm = normalize_path_inner(node_name, WildcardMode::Reject)?;
        let node_id_norm = validate_node_id(node_id)?;
        let suffix = format!("sys/{}/{}/{}", subsystem_norm, node_name_norm, node_id_norm);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    pub fn liveliness_key(&self, node_name: &str, node_id: &str) -> AortaResult<KeyExpr<'static>> {
        self.system_key("alive", node_name, node_id)
    }

    /// Owner-liveliness token key for one ContextStore instance
    /// (`aorta/{group}/sys/alive/ctx/{node_name}/{source_instance_id}`).
    /// One token per store, declared at the first successful
    /// `declare_owned_prefix`; Zenoh
    /// withdraws it on process death. The trailing
    /// `source_instance_id` (16 hex chars, per-ContextStore) lets
    /// consumers correlate prefixes to instances across restarts via
    /// the same id carried on every context event and query reply.
    pub fn ctx_owner_liveliness_key(
        &self,
        node_name: &str,
        source_instance_id: &str,
    ) -> AortaResult<KeyExpr<'static>> {
        // Both arguments must be SINGLE key segments: the reader-side
        // parse expects exactly two trailing segments after the fixed
        // `sys/alive/ctx/` prefix. `normalize_path_inner` tolerates
        // embedded '/' (it normalizes whole paths), so reject them
        // explicitly to keep the token key shape canonical.
        let node_name_norm = normalize_path_inner(node_name, WildcardMode::Reject)?;
        if node_name_norm.contains('/') {
            return Err(AortaError::invalid_argument(
                "ctx owner liveliness node_name must be a single segment",
            ));
        }
        let instance_norm = normalize_path_inner(source_instance_id, WildcardMode::Reject)?;
        if instance_norm.contains('/') {
            return Err(AortaError::invalid_argument(
                "ctx owner liveliness source_instance_id must be a single segment",
            ));
        }
        let suffix = format!("sys/alive/ctx/{}/{}", node_name_norm, instance_norm);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    /// Subscription pattern matching every ContextStore owner-liveliness
    /// token in this group (`aorta/{group}/sys/alive/ctx/**`). The
    /// trailing `**` matches exactly the `{node_name}/{source_instance_id}`
    /// segments of a token key.
    pub fn ctx_owner_liveliness_query(&self) -> AortaResult<KeyExpr<'static>> {
        let suffix = String::from("sys/alive/ctx/**");
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    /// Owner-claim liveliness key for one provider of `service_name`
    /// (`aorta/{group}/sys/svc_owner/{service}/{node_id}`). One token per
    /// (service, node); the trailing `node_id` lets the network-level
    /// single-provider check distinguish a foreign provider from itself across
    /// restarts.
    pub fn service_owner_key(
        &self,
        service_name: &str,
        node_id: &str,
    ) -> AortaResult<KeyExpr<'static>> {
        self.system_key("svc_owner", service_name, node_id)
    }

    /// Query pattern matching every provider's owner token for `service_name`.
    /// The trailing `*` matches exactly the single node-id segment, so it never
    /// bleeds into a sibling service that shares a key prefix.
    pub fn service_owner_query(&self, service_name: &str) -> AortaResult<KeyExpr<'static>> {
        let service_norm = normalize_path_inner(service_name, WildcardMode::Reject)?;
        let suffix = format!("sys/svc_owner/{}/*", service_norm);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    /// Alive-claim liveliness key for one provider of `action_name`
    /// (`aorta/{group}/sys/act_owner/{action}/{node_id}`). Declared by the
    /// action server for its lifetime; clients watch it so a server dying
    /// mid-goal surfaces as an error instead of an infinite `wait_result`.
    pub fn action_owner_key(
        &self,
        action_name: &str,
        node_id: &str,
    ) -> AortaResult<KeyExpr<'static>> {
        self.system_key("act_owner", action_name, node_id)
    }

    /// Query/subscription pattern matching every provider's alive token for
    /// `action_name`. The trailing `*` matches exactly the single node-id
    /// segment, mirroring [`Self::service_owner_query`].
    pub fn action_owner_query(&self, action_name: &str) -> AortaResult<KeyExpr<'static>> {
        let action_norm = normalize_path_inner(action_name, WildcardMode::Reject)?;
        let suffix = format!("sys/act_owner/{}/*", action_norm);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    pub fn context_key(&self, relative_key: &str) -> AortaResult<KeyExpr<'static>> {
        let normalized = normalize_path_inner(relative_key, WildcardMode::Reject)?;
        let suffix = format!("ctx/{}", normalized);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    // ---------- subscribe-side / query keys (wildcards allowed) ----------

    /// Build a subscriber topic-key pattern. Whole-segment wildcards
    /// `*` and `**` are accepted (Zenoh-native subscription semantics);
    /// reserved metacharacters `$`, `#`, `?` still rejected.
    pub fn topic_key_sub(&self, pattern: &str) -> AortaResult<KeyExpr<'static>> {
        self.build_with_primitive("pub", pattern, WildcardMode::Allow, NamespaceMode::Allow)
    }

    /// Build a service-client query key. Wildcards allowed in the service name
    /// (clients may query any service matching a glob), but the namespace must
    /// target a single robot — request/reply with a wildcard namespace would
    /// fan a (possibly non-idempotent) call out across the whole fleet.
    pub fn service_key_client(&self, pattern: &str) -> AortaResult<KeyExpr<'static>> {
        self.build_with_primitive("srv", pattern, WildcardMode::Allow, NamespaceMode::Reject)
    }

    pub fn action_feedback_key_sub(&self, pattern: &str) -> AortaResult<KeyExpr<'static>> {
        self.action_key(
            "feedback",
            pattern,
            WildcardMode::Allow,
            NamespaceMode::Allow,
        )
    }

    pub fn action_get_result_key_query(&self, pattern: &str) -> AortaResult<KeyExpr<'static>> {
        // get_result is a query that must resolve to a single robot's queryable.
        self.action_key(
            "get_result",
            pattern,
            WildcardMode::Allow,
            NamespaceMode::Reject,
        )
    }

    pub fn param_key_reader(
        &self,
        node_pattern: &str,
        param_pattern: &str,
    ) -> AortaResult<KeyExpr<'static>> {
        let node_norm = normalize_path_inner(node_pattern, WildcardMode::Allow)?;
        let param_norm = normalize_path_inner(param_pattern, WildcardMode::Allow)?;
        let suffix = format!("prm/{}/{}", node_norm, param_norm);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    pub fn context_prefix(&self, pattern: &str) -> AortaResult<KeyExpr<'static>> {
        let normalized = normalize_path_inner(pattern, WildcardMode::Allow)?;
        let suffix = format!("ctx/{}", normalized);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    /// Build a snoop/capture pattern over the telemetry stream
    /// (`aorta/{group}/sys/telemetry/<pattern>`). Wildcards allowed; the
    /// namespace targets the local group (telemetry is a local system stream).
    /// Used by `core::capture` so no layer above L1 hand-assembles `sys/` keys.
    pub fn telemetry_key_sub(&self, pattern: &str) -> AortaResult<KeyExpr<'static>> {
        let normalized = normalize_path_inner(pattern, WildcardMode::Allow)?;
        let suffix = format!("sys/telemetry/{}", normalized);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    /// Build a snoop/capture pattern over the RPC record-mirror stream
    /// (`aorta/{group}/sys/record/srv/<pattern>`). Wildcards allowed. The
    /// producer of this stream is a separate workstream; capture is ready to
    /// subscribe once it exists.
    pub fn rpc_record_key_sub(&self, pattern: &str) -> AortaResult<KeyExpr<'static>> {
        let normalized = normalize_path_inner(pattern, WildcardMode::Allow)?;
        let suffix = format!("sys/record/srv/{}", normalized);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, NamespaceMode::Reject)
    }

    pub fn action_status_prefix(&self, action: &str) -> AortaResult<String> {
        let action_norm = normalize_path_inner(action, WildcardMode::Reject)?;
        let suffix = format!("act/status/{}", action_norm);
        validate_key_suffix_len(&suffix)?;
        // status is an observe surface — wildcard namespace fan-out is allowed.
        self.build_grouped_string(&suffix, NamespaceMode::Allow)
    }

    // ---------- string-suffix helpers (publish-side) ----------

    pub fn key_suffix_for_topic(&self, topic: &str) -> AortaResult<String> {
        let topic_norm = normalize_path_inner(topic, WildcardMode::Reject)?;
        let suffix = format!("pub/{}", topic_norm);
        validate_key_suffix_len(&suffix)?;
        Ok(suffix)
    }

    pub fn key_suffix_for_service(&self, service: &str) -> AortaResult<String> {
        let service_norm = normalize_path_inner(service, WildcardMode::Reject)?;
        let suffix = format!("srv/{}", service_norm);
        validate_key_suffix_len(&suffix)?;
        Ok(suffix)
    }

    pub fn key_suffix_for_param(&self, node: &str, param: &str) -> AortaResult<String> {
        let node_norm = normalize_path_inner(node, WildcardMode::Reject)?;
        let param_norm = normalize_path_inner(param, WildcardMode::Reject)?;
        let suffix = format!("prm/{}/{}", node_norm, param_norm);
        validate_key_suffix_len(&suffix)?;
        Ok(suffix)
    }

    pub fn key_suffix_for_context(&self, relative_key: &str) -> AortaResult<String> {
        let normalized = normalize_path_inner(relative_key, WildcardMode::Reject)?;
        let suffix = format!("ctx/{}", normalized);
        validate_key_suffix_len(&suffix)?;
        Ok(suffix)
    }

    // ---------- telemetry keys ----------

    /// `aorta/{group}/sys/telemetry/vital/ctx/owner_conflict` — the conflict
    /// alarm. Event-driven; lives in the vital tier so the cloud sees it live.
    pub fn ctx_owner_conflict_telemetry_key(&self) -> AortaResult<String> {
        let suffix = "sys/telemetry/vital/ctx/owner_conflict".to_string();
        validate_key_suffix_len(&suffix)?;
        self.build_grouped_string(&suffix, NamespaceMode::Reject)
    }

    /// `aorta/{group}/sys/telemetry/vital/snapshot/{node}` — the merged 30 s
    /// vital record (node identity + resource inventory + runtime health).
    pub fn telemetry_vital_snapshot_key(&self, node_id: &str) -> AortaResult<String> {
        self.telemetry_key(TelemetryTier::Vital, "snapshot", node_id)
    }

    /// `aorta/{group}/sys/telemetry/bulk/runtime/{node}` — the 5 s full runtime
    /// metrics stream (executor / queue / throughput detail).
    pub fn telemetry_bulk_runtime_key(&self, node_id: &str) -> AortaResult<String> {
        self.telemetry_key(TelemetryTier::Bulk, "runtime", node_id)
    }

    /// Split a full wire key `[{namespace}/]aorta/{group}/{suffix}` into its
    /// optional application-level namespace and the `aorta/{group}/`-relative
    /// suffix. Returns `None` if the key does not carry this builder's group.
    ///
    /// Uses the fixed `aorta` root segment as the anchor, so it transparently
    /// handles a missing namespace, a single segment (`R1`), or a multi-segment
    /// namespace (`region/site/R1`). A namespace segment equal to `aorta` would
    /// be ambiguous and is rejected at [`KeyExprBuilder::with_namespace`].
    pub fn split_full_key(&self, full_key: &str) -> Option<(Option<String>, String)> {
        let segments: Vec<&str> = full_key.split('/').collect();
        let anchor = segments.iter().position(|seg| *seg == ROOT_PREFIX)?;
        if *segments.get(anchor + 1)? != self.group.as_str() {
            return None;
        }
        let suffix_segments = &segments[anchor + 2..];
        if suffix_segments.is_empty() {
            return None;
        }
        let suffix = suffix_segments.join("/");
        let namespace = if anchor == 0 {
            None
        } else {
            Some(segments[..anchor].join("/"))
        };
        Some((namespace, suffix))
    }

    /// Back-compat shim: the `aorta/{group}/`-relative suffix only. New callers
    /// that need the source namespace should use [`KeyExprBuilder::split_full_key`].
    pub fn key_suffix_from_full_key(&self, full_key: &str) -> Option<String> {
        self.split_full_key(full_key).map(|(_, suffix)| suffix)
    }

    // The pre-relaxation undifferentiated helpers (`topic_key`,
    // `service_key`, `action_feedback_key`, `action_get_result_key`,
    // `param_key`) have been removed outright. The only known external
    // consumers (C++ via the C-ABI, Rust via the aorta_sdk high-level
    // API) never called the builder methods directly — all uses go
    // through `Node::create_publisher` / `create_subscriber` / etc.,
    // which we migrated in this PR. Keeping `#[deprecated]` shims would
    // only protect imaginary consumers at the cost of API noise.

    // ---------- internal builders ----------

    fn build_with_primitive(
        &self,
        primitive: &str,
        path: &str,
        wildcard_mode: WildcardMode,
        ns_mode: NamespaceMode,
    ) -> AortaResult<KeyExpr<'static>> {
        let norm = normalize_path_inner(path, wildcard_mode)?;
        let suffix = format!("{}/{}", primitive, norm);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, ns_mode)
    }

    fn action_key(
        &self,
        surface: &str,
        action: &str,
        wildcard_mode: WildcardMode,
        ns_mode: NamespaceMode,
    ) -> AortaResult<KeyExpr<'static>> {
        let action_norm = normalize_path_inner(action, wildcard_mode)?;
        let suffix = format!("act/{}/{}", surface, action_norm);
        validate_key_suffix_len(&suffix)?;
        self.build_grouped(&suffix, ns_mode)
    }

    fn action_status_suffix(&self, action: &str, goal_key: &str) -> AortaResult<String> {
        let action_norm = normalize_path_inner(action, WildcardMode::Reject)?;
        let goal_key = normalize_goal_key(goal_key)?;
        let suffix = format!("act/status/{}/{}", action_norm, goal_key);
        validate_key_suffix_len(&suffix)?;
        Ok(suffix)
    }

    fn build_grouped(&self, suffix: &str, ns_mode: NamespaceMode) -> AortaResult<KeyExpr<'static>> {
        key_expr_from_owned(self.build_grouped_string(suffix, ns_mode)?)
    }

    fn build_grouped_string(&self, suffix: &str, ns_mode: NamespaceMode) -> AortaResult<String> {
        let base = format!("{}/{}/{}", ROOT_PREFIX, self.group, suffix);
        match &self.namespace {
            None => Ok(base),
            Some(ns) => {
                if ns_mode == NamespaceMode::Reject && ns_has_wildcard_segment(ns) {
                    return Err(AortaError::invalid_argument(
                        "wildcard namespace is not allowed for this operation \
                         (request/reply or publish targets a single namespace)",
                    ));
                }
                Ok(format!("{}/{}", ns, base))
            }
        }
    }

    fn telemetry_key(
        &self,
        tier: TelemetryTier,
        family: &str,
        node_id: &str,
    ) -> AortaResult<String> {
        let family_norm = normalize_path_inner(family, WildcardMode::Reject)?;
        let node_id_norm = validate_node_id(node_id)?;
        let suffix = format!(
            "sys/telemetry/{}/{}/{}",
            tier.as_str(),
            family_norm,
            node_id_norm
        );
        validate_key_suffix_len(&suffix)?;
        self.build_grouped_string(&suffix, NamespaceMode::Reject)
    }
}

/// A cross-namespace routing target for the consumer-side `*_for` APIs.
///
/// Carries an application-level namespace (e.g. a robot SN, or `*` for fleet
/// fan-out on observe surfaces). The `*_for` methods take this type
/// **explicitly** (not `impl Into<Target>`) so a namespace cannot be silently
/// confused with a topic/service/action name — those are `&str`, and an
/// implicit `impl Into<Target>` boundary would let a name string slip into the
/// namespace slot (and vice-versa). Construct it with [`Target::namespace`].
///
/// It is a forward-compatible wrapper: a `group` axis can be added later (group
/// wildcard). Intentionally does *not* expose a `.group()` setter
/// yet — group targeting is unimplemented, so offering a setter that is silently
/// ignored would be a footgun.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    namespace: String,
}

impl Target {
    /// Target a single namespace (or `*`/`**` on observe surfaces).
    pub fn namespace(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
        }
    }

    /// The namespace fragment to prepend (passed to
    /// [`KeyExprBuilder::with_namespace`]).
    pub fn as_str(&self) -> &str {
        &self.namespace
    }
}

impl From<&str> for Target {
    fn from(namespace: &str) -> Self {
        Self::namespace(namespace)
    }
}

impl From<String> for Target {
    fn from(namespace: String) -> Self {
        Self { namespace }
    }
}

impl From<&String> for Target {
    fn from(namespace: &String) -> Self {
        Self::namespace(namespace.clone())
    }
}

// =====================================================================
// validator (single source of truth)
// =====================================================================

/// Normalize a publish-side / owner key suffix into a canonical form.
/// Wildcards are rejected. See [`normalize_path_with_wildcards`] for the
/// subscribe-side variant.
pub fn normalize_path(path: &str) -> AortaResult<String> {
    normalize_path_inner(path, WildcardMode::Reject)
}

/// Subscribe-side / query variant of [`normalize_path`]. Accepts the
/// whole-segment Zenoh wildcards `*` and `**` verbatim; other rules
/// match the publish-side validator exactly.
pub fn normalize_path_with_wildcards(path: &str) -> AortaResult<String> {
    normalize_path_inner(path, WildcardMode::Allow)
}

// `normalize_action_path` and `validate_flatbuffers_identifier_segment`
// were removed. Action names no
// longer derive FlatBuffers root types via segment projection — the
// runtime now resolves root types from the compiled bfbs
// (`action::resolve_action_root_types_from_bfbs`), and the typed
// wrapper path uses the generated associated `SchemaMeta::root_type()`.
// Action names are pure Zenoh routing strings, validated by the same
// `normalize_path` everything else uses.

/// Internal implementation behind both [`normalize_path`] and
/// [`normalize_path_with_wildcards`].
///
/// Performs:
///   1. trim ASCII whitespace
///   2. Unicode NFC normalization (byte-equivalence canonicalization —
///      not a rename; `café` precomposed and `café` decomposed collapse
///      to the same byte sequence)
///   3. strip leading/trailing `/`
///   4. split on `/`; reject empty segments (no silent collapse)
///   5. reject `.` / `..`
///   6. validate each segment (`validate_segment`)
///   7. cap total UTF-8 byte length at `MAX_KEY_SUFFIX_LEN`
///
/// No case folding, no hyphen substitution, no other rewriting — the
/// user's spelling is preserved byte-for-byte after NFC.
fn normalize_path_inner(path: &str, wildcard_mode: WildcardMode) -> AortaResult<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(AortaError::invalid_argument("path is empty"));
    }

    // NFC at validator entry. After this point every byte length we
    // compute corresponds to what Zenoh will see on the wire.
    let nfc: String = trimmed.nfc().collect();

    // Strip leading/trailing `/`. Splitting `cam//front` then yields a
    // genuinely empty middle segment that we treat as a hard error —
    // the previous "silently collapse `//`" behavior hid typos.
    let stripped = nfc.trim_matches('/');
    if stripped.is_empty() {
        return Err(AortaError::invalid_argument("path resolves to empty"));
    }

    let mut segments = Vec::new();
    for segment in stripped.split('/') {
        validate_segment(segment, wildcard_mode)?;
        segments.push(segment);
    }

    let normalized = segments.join("/");
    if normalized.len() > MAX_KEY_SUFFIX_LEN {
        return Err(AortaError::invalid_argument(format!(
            "path exceeds max length {} (utf-8 bytes)",
            MAX_KEY_SUFFIX_LEN
        )));
    }

    Ok(normalized)
}

fn validate_segment(segment: &str, wildcard_mode: WildcardMode) -> AortaResult<()> {
    if segment.is_empty() {
        return Err(AortaError::invalid_argument(
            "empty path segment (use a non-empty segment; `//` and trailing `/` are rejected)",
        ));
    }
    if segment == "." || segment == ".." {
        return Err(AortaError::invalid_argument(
            "path segment cannot be '.' or '..'",
        ));
    }

    // Whole-segment wildcards. Accept verbatim on subscribe-side; reject
    // on publish-side. `$*` chunk-wildcard inside a segment is deferred
    // and falls through to the per-char rejection below.
    if segment == "*" || segment == "**" {
        return match wildcard_mode {
            WildcardMode::Allow => Ok(()),
            WildcardMode::Reject => Err(AortaError::invalid_argument(
                "wildcard segment used on a publish-side / owner key — \
                 use the subscribe-side or query-side builder method",
            )),
        };
    }

    if segment.len() > MAX_SEGMENT_LEN {
        return Err(AortaError::invalid_argument(format!(
            "path segment exceeds max length {} (utf-8 bytes)",
            MAX_SEGMENT_LEN
        )));
    }

    for ch in segment.chars() {
        if is_segment_char(ch) {
            continue;
        }
        // Provide a per-char hint for the most common footguns instead of
        // a generic "invalid character" — these are the bytes Zenoh
        // explicitly rejects or that we defer support for.
        let msg = match ch {
            '/' => "segment contains '/' (must be the separator, not inside a segment)",
            '*' => "segment contains '*' outside of a whole-segment wildcard",
            '$' => {
                "segment contains '$' (chunk-wildcard `$*` inside a segment is not yet supported)"
            }
            '#' => "segment contains '#' (reserved by Zenoh)",
            '?' => "segment contains '?' (reserved by Zenoh)",
            c if c.is_control() => "segment contains a control character",
            c if c.is_whitespace() => "segment contains a whitespace character",
            _ => "segment contains an unsupported character",
        };
        return Err(AortaError::invalid_argument(format!("{}: {:?}", msg, ch)));
    }

    Ok(())
}

/// True for any code point we accept inside a regular (non-wildcard)
/// segment.
///
/// The allowed set is the union of:
///   - `XID_Continue` (Unicode identifier-continue: letters of any
///     script, digits, marks, `_`).
///   - U+002D HYPHEN-MINUS (`-`).
///
/// XID_Continue already covers `_` (the only connector punctuation in
/// the class), ASCII letters, digits, and Unicode letters/digits. The
/// only extra ASCII character we permit is `-`, matching Zenoh's
/// native examples (`zenoh-rs-pub`) and common kebab-case in foreign
/// ecosystems.
fn is_segment_char(ch: char) -> bool {
    unicode_ident::is_xid_continue(ch) || ch == '-'
}

fn normalize_group(group: &str) -> AortaResult<String> {
    let normalized = normalize_path_inner(group, WildcardMode::Reject)?;
    if normalized.contains('/') {
        return Err(AortaError::invalid_argument(
            "group must be a single path segment",
        ));
    }
    if normalized.len() > MAX_GROUP_LEN {
        return Err(AortaError::invalid_argument(format!(
            "group exceeds max length {} (utf-8 bytes)",
            MAX_GROUP_LEN
        )));
    }
    Ok(normalized)
}

/// Validate a node id. Node ids historically bypassed `validate_segment`
/// (they were inserted into `system_key` / `telemetry_key` raw); we now
/// route them through the same segment validator so that mis-formed ids
/// fail early with a uniform error message. UUIDs (with or without
/// hyphens) pass trivially.
pub(crate) fn validate_node_id(node_id: &str) -> AortaResult<String> {
    let trimmed = node_id.trim();
    if trimmed.is_empty() {
        return Err(AortaError::invalid_argument("node id is empty"));
    }
    let nfc: String = trimmed.nfc().collect();
    validate_segment(&nfc, WildcardMode::Reject)?;
    Ok(nfc)
}

/// Extracts the `node_id` (last segment) from a service-owner liveliness key
/// produced by [`KeyExprBuilder::service_owner_key`]. Returns `None` for an
/// empty/degenerate key.
pub(crate) fn service_owner_key_node_id(key: &str) -> Option<&str> {
    key.rsplit('/').next().filter(|segment| !segment.is_empty())
}

pub fn group_from_env_or_default() -> String {
    match runtime_env::var("AORTA_GROUP") {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                DEFAULT_GROUP.to_string()
            } else {
                trimmed.to_string()
            }
        }
        None => DEFAULT_GROUP.to_string(),
    }
}

pub fn key_builder_from_group_env() -> KeyExprBuilder {
    let group = group_from_env_or_default();
    KeyExprBuilder::new(&group).unwrap_or_else(|err| {
        panic!(
            "invalid AORTA_GROUP '{}': {}. Please fix AORTA_GROUP to a valid group.",
            group, err
        )
    })
}

/// True if any whole segment of the namespace is a Zenoh wildcard (`*` / `**`).
/// Used to reject a wildcard namespace on request/reply and publish surfaces.
fn ns_has_wildcard_segment(ns: &str) -> bool {
    ns.split('/').any(|seg| seg == "*" || seg == "**")
}

fn key_expr_from_owned(input: String) -> AortaResult<KeyExpr<'static>> {
    KeyExpr::new(input)
        .map_err(|err| AortaError::invalid_argument(format!("invalid key expression: {}", err)))
}

fn normalize_goal_key(goal_key: &str) -> AortaResult<String> {
    let trimmed = goal_key.trim();
    if trimmed.len() != 32 {
        return Err(AortaError::invalid_argument(format!(
            "goal_key must contain exactly 32 hex characters, got {}",
            trimmed.len()
        )));
    }
    if !trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(AortaError::invalid_argument(
            "goal_key contains non-hex characters",
        ));
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn validate_key_suffix_len(suffix: &str) -> AortaResult<()> {
    if suffix.len() > MAX_KEY_SUFFIX_LEN {
        return Err(AortaError::invalid_argument(format!(
            "key suffix exceeds max length {} (utf-8 bytes)",
            MAX_KEY_SUFFIX_LEN
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../test/key_expr_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../test/key_expr_advanced_tests.rs"]
mod advanced_tests;
