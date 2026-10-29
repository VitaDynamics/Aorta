use std::collections::VecDeque;
use std::time::{Duration, Instant};

use aorta_introspect::client::{NodeDiscoverySource, TopicPreviewFrame};

use crate::data::{
    merge_field_template_into_request_json, merge_goal_field_template_into_goal_json,
    ActionCallResponseData, ActionCallSchemaData, ActionDetailData, ActionRow, CapabilityState,
    ContextData, DashboardData, FeedbackRow, FreshnessState, HealthLevel, LogRow, NodeDetailData,
    NodeRow, OverviewData, OverviewHealthSummary, SchemaDescribeData, ServiceCallResponseData,
    ServiceCallSchemaData, ServiceDetailData, ServiceRow, TopicDetailData, TopicRow,
    ZenohConnectivityData, ZenohConnectivityEventData, ZenohConnectivityEventKindData,
    ZenohInternalsData, ZenohRouterRow,
};
use crate::editor::TextEditor;
use crate::search::SearchIndex;

const TOPIC_PREVIEW_HISTORY_CAPACITY: usize = 32;
const EVENT_LOG_CAPACITY: usize = 64;
const ZENOH_CONNECTIVITY_EVENT_CAPACITY: usize = 32;
/// Per-resource latency sparkline ring capacity for the Telemetry screen. Bounds
/// the client-side history so it never grows without limit while the TUI runs.
pub(crate) const LATENCY_HISTORY_CAPACITY: usize = 64;
/// Bounded retention for the live `/log` stream viewer. Mirrors the
/// `event_log`/`zenoh_connectivity_events` bounded-deque pattern so the viewer
/// never grows without limit while the TUI runs.
pub(crate) const LOG_STREAM_CAPACITY: usize = 256;
/// Bounded retention for the per-goal live action feedback panel.
const ACTION_FEEDBACK_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverviewAction {
    Topics,
    Services,
    ServiceCall,
    Nodes,
}

impl OverviewAction {
    pub const ALL: [OverviewAction; 4] = [
        OverviewAction::Topics,
        OverviewAction::Services,
        OverviewAction::ServiceCall,
        OverviewAction::Nodes,
    ];

    pub fn title(self) -> &'static str {
        match self {
            OverviewAction::Topics => "Browse Topics",
            OverviewAction::Services => "Browse Services",
            OverviewAction::ServiceCall => "Open Service Call",
            OverviewAction::Nodes => "Browse Nodes",
        }
    }

    pub fn key(self) -> &'static str {
        match self {
            OverviewAction::Topics => "topics",
            OverviewAction::Services => "services",
            OverviewAction::ServiceCall => "service-call",
            OverviewAction::Nodes => "nodes",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Overview,
    Topics,
    Services,
    ServiceCall,
    Actions,
    ActionCall,
    Nodes,
    Context,
    Zenoh,
    Telemetry,
}

impl Screen {
    pub const ALL: [Screen; 10] = [
        Screen::Overview,
        Screen::Topics,
        Screen::Services,
        Screen::ServiceCall,
        Screen::Actions,
        Screen::ActionCall,
        Screen::Nodes,
        Screen::Context,
        Screen::Zenoh,
        Screen::Telemetry,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Screen::Overview => "Overview",
            Screen::Topics => "Topics",
            Screen::Services => "Services",
            Screen::ServiceCall => "Service Call",
            Screen::Actions => "Actions",
            Screen::ActionCall => "Action Call",
            Screen::Nodes => "Nodes",
            Screen::Context => "Context",
            Screen::Zenoh => "Zenoh",
            Screen::Telemetry => "Telemetry",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandActionScope {
    Global,
    Screen(Screen),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandActionMetadata {
    pub id: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    pub shortcut_hint: Option<&'static str>,
    pub scope: CommandActionScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandAction {
    OpenOverview,
    OpenTopics,
    OpenServices,
    OpenServiceCall,
    OpenActions,
    OpenActionCall,
    OpenNodes,
    OpenContext,
    OpenZenoh,
    OpenTelemetry,
    OpenDiagnosticsTimeline,
    OpenTopologyExplorer,
    OpenRecorderLaunchpad,
    MacroOpenTopicsSearch,
    MacroOpenServicesSearch,
    MacroOpenNodesSearch,
    MacroOpenServiceCallEditor,
    MacroOpenActionCallEditor,
    OpenNodeComparison,
    OpenBookmarks,
    RefreshSnapshot,
    ToggleAutoRefresh,
    JumpToTopicProviderNode,
    ToggleAdvancedMode,
    ToggleCurrentBookmark,
    ToggleNodeCompareAnchor,
    ToggleTopicRuntimeSection,
    ToggleServiceRuntimeSection,
    ToggleNodeRuntimeSection,
    ExportTopicPreviewPayload,
    CopyTopicPreviewContent,
    ExportServiceResponsePayload,
    OpenHelp,
}

impl CommandAction {
    pub const ALL: [CommandAction; 33] = [
        CommandAction::OpenOverview,
        CommandAction::OpenTopics,
        CommandAction::OpenServices,
        CommandAction::OpenServiceCall,
        CommandAction::OpenActions,
        CommandAction::OpenActionCall,
        CommandAction::OpenNodes,
        CommandAction::OpenContext,
        CommandAction::OpenZenoh,
        CommandAction::OpenTelemetry,
        CommandAction::OpenDiagnosticsTimeline,
        CommandAction::OpenTopologyExplorer,
        CommandAction::OpenRecorderLaunchpad,
        CommandAction::MacroOpenTopicsSearch,
        CommandAction::MacroOpenServicesSearch,
        CommandAction::MacroOpenNodesSearch,
        CommandAction::MacroOpenServiceCallEditor,
        CommandAction::MacroOpenActionCallEditor,
        CommandAction::OpenNodeComparison,
        CommandAction::OpenBookmarks,
        CommandAction::RefreshSnapshot,
        CommandAction::ToggleAutoRefresh,
        CommandAction::JumpToTopicProviderNode,
        CommandAction::ToggleAdvancedMode,
        CommandAction::ToggleCurrentBookmark,
        CommandAction::ToggleNodeCompareAnchor,
        CommandAction::ToggleTopicRuntimeSection,
        CommandAction::ToggleServiceRuntimeSection,
        CommandAction::ToggleNodeRuntimeSection,
        CommandAction::ExportTopicPreviewPayload,
        CommandAction::CopyTopicPreviewContent,
        CommandAction::ExportServiceResponsePayload,
        CommandAction::OpenHelp,
    ];

    pub fn metadata(self) -> CommandActionMetadata {
        match self {
            CommandAction::OpenOverview => CommandActionMetadata {
                id: "open-overview",
                title: "Open Overview",
                description: "Switch to the Overview screen.",
                shortcut_hint: None,
                scope: CommandActionScope::Global,
            },
            CommandAction::OpenTopics => CommandActionMetadata {
                id: "open-topics",
                title: "Open Topics",
                description: "Switch to the Topics screen.",
                shortcut_hint: None,
                scope: CommandActionScope::Global,
            },
            CommandAction::OpenServices => CommandActionMetadata {
                id: "open-services",
                title: "Open Services",
                description: "Switch to the Services screen.",
                shortcut_hint: None,
                scope: CommandActionScope::Global,
            },
            CommandAction::OpenServiceCall => CommandActionMetadata {
                id: "open-service-call",
                title: "Open Service Call",
                description: "Open the Service Call screen for the selected service.",
                shortcut_hint: Some("c"),
                scope: CommandActionScope::Screen(Screen::Services),
            },
            CommandAction::OpenActions => CommandActionMetadata {
                id: "open-actions",
                title: "Open Actions",
                description: "Switch to the Actions screen.",
                shortcut_hint: None,
                scope: CommandActionScope::Global,
            },
            CommandAction::OpenActionCall => CommandActionMetadata {
                id: "open-action-call",
                title: "Open Action Call",
                description: "Open the Action Call screen to send a goal to the selected action.",
                shortcut_hint: Some("c"),
                scope: CommandActionScope::Screen(Screen::Actions),
            },
            CommandAction::OpenNodes => CommandActionMetadata {
                id: "open-nodes",
                title: "Open Nodes",
                description: "Switch to the Nodes screen.",
                shortcut_hint: None,
                scope: CommandActionScope::Global,
            },
            CommandAction::OpenContext => CommandActionMetadata {
                id: "open-context",
                title: "Open Context",
                description: "Switch to the read-only ContextStore screen.",
                shortcut_hint: None,
                scope: CommandActionScope::Global,
            },
            CommandAction::OpenZenoh => CommandActionMetadata {
                id: "open-zenoh",
                title: "Open Zenoh Diagnostics",
                description: "Open the expert Zenoh diagnostics screen.",
                shortcut_hint: None,
                scope: CommandActionScope::Screen(Screen::Zenoh),
            },
            CommandAction::OpenTelemetry => CommandActionMetadata {
                id: "open-telemetry",
                title: "Open Telemetry",
                description:
                    "Open the Telemetry dashboard with client-observed latency sparklines and percentiles.",
                shortcut_hint: None,
                scope: CommandActionScope::Global,
            },
            CommandAction::OpenDiagnosticsTimeline => CommandActionMetadata {
                id: "open-diagnostics-timeline",
                title: "Open Diagnostics Timeline",
                description: "Open the event timeline with severity and source filters.",
                shortcut_hint: Some("e"),
                scope: CommandActionScope::Global,
            },
            CommandAction::OpenTopologyExplorer => CommandActionMetadata {
                id: "open-topology-explorer",
                title: "Open Topology Explorer",
                description:
                    "Open a lightweight topology summary for the current topic, service, or node.",
                shortcut_hint: Some("g"),
                scope: CommandActionScope::Global,
            },
            CommandAction::OpenRecorderLaunchpad => CommandActionMetadata {
                id: "open-recorder-launchpad",
                title: "Open Recorder Launchpad",
                description:
                    "Open the recorder launchpad for the current group or selection.",
                shortcut_hint: Some("o"),
                scope: CommandActionScope::Global,
            },
            CommandAction::MacroOpenTopicsSearch => CommandActionMetadata {
                id: "macro-open-topics-search",
                title: "Macro: Find Topic",
                description: "Open Topics and immediately enter search mode. Shortcut: T.",
                shortcut_hint: Some("T"),
                scope: CommandActionScope::Global,
            },
            CommandAction::MacroOpenServicesSearch => CommandActionMetadata {
                id: "macro-open-services-search",
                title: "Macro: Find Service",
                description: "Open Services and immediately enter search mode. Shortcut: S.",
                shortcut_hint: Some("S"),
                scope: CommandActionScope::Global,
            },
            CommandAction::MacroOpenNodesSearch => CommandActionMetadata {
                id: "macro-open-nodes-search",
                title: "Macro: Find Node",
                description: "Open Nodes and immediately enter search mode. Shortcut: N.",
                shortcut_hint: Some("N"),
                scope: CommandActionScope::Global,
            },
            CommandAction::MacroOpenServiceCallEditor => CommandActionMetadata {
                id: "macro-open-service-request-editor",
                title: "Macro: Service Request Editor",
                description:
                    "Open Service Call and focus the request editor for the current service selection. Shortcut: C.",
                shortcut_hint: Some("C"),
                scope: CommandActionScope::Global,
            },
            CommandAction::MacroOpenActionCallEditor => CommandActionMetadata {
                id: "macro-open-action-goal-editor",
                title: "Macro: Action Goal Editor",
                description:
                    "Open Action Call and focus the goal editor for the current action selection. Shortcut: A.",
                shortcut_hint: Some("A"),
                scope: CommandActionScope::Global,
            },
            CommandAction::OpenNodeComparison => CommandActionMetadata {
                id: "open-node-comparison",
                title: "Open Node Comparison",
                description:
                    "Open node comparison for the selected node against the current comparison anchor.",
                shortcut_hint: Some("c"),
                scope: CommandActionScope::Screen(Screen::Nodes),
            },
            CommandAction::OpenBookmarks => CommandActionMetadata {
                id: "open-bookmarks",
                title: "Open Bookmarks",
                description: "Open the bookmarks overlay for quick access to pinned resources.",
                shortcut_hint: Some("b"),
                scope: CommandActionScope::Global,
            },
            CommandAction::RefreshSnapshot => CommandActionMetadata {
                id: "refresh-snapshot",
                title: "Refresh Snapshot",
                description: "Refresh dashboard data for the current snapshot.",
                shortcut_hint: Some("r"),
                scope: CommandActionScope::Global,
            },
            CommandAction::ToggleAutoRefresh => CommandActionMetadata {
                id: "toggle-auto-refresh",
                title: "Toggle Auto Refresh",
                description:
                    "Enable or disable periodic background refresh of the dashboard and active view.",
                shortcut_hint: Some("f"),
                scope: CommandActionScope::Global,
            },
            CommandAction::JumpToTopicProviderNode => CommandActionMetadata {
                id: "jump-to-topic-provider-node",
                title: "Jump to Topic Provider Node",
                description:
                    "Open the Nodes screen with the selected topic's first provider node selected.",
                shortcut_hint: Some("n"),
                scope: CommandActionScope::Screen(Screen::Topics),
            },
            CommandAction::ToggleAdvancedMode => CommandActionMetadata {
                id: "toggle-advanced-mode",
                title: "Toggle Advanced Mode",
                description: "Enable or disable advanced diagnostics screens.",
                shortcut_hint: Some("z"),
                scope: CommandActionScope::Global,
            },
            CommandAction::ToggleCurrentBookmark => CommandActionMetadata {
                id: "toggle-current-bookmark",
                title: "Toggle Current Bookmark",
                description: "Pin or unpin the selected topic, service, or node.",
                shortcut_hint: Some("p"),
                scope: CommandActionScope::Global,
            },
            CommandAction::ToggleNodeCompareAnchor => CommandActionMetadata {
                id: "toggle-node-compare-anchor",
                title: "Toggle Node Compare Anchor",
                description:
                    "Set or clear the selected node as the comparison anchor for side-by-side summary.",
                shortcut_hint: Some("x"),
                scope: CommandActionScope::Screen(Screen::Nodes),
            },
            CommandAction::ToggleTopicRuntimeSection => CommandActionMetadata {
                id: "toggle-topic-runtime-section",
                title: "Toggle Topic Runtime Section",
                description: "Expand or collapse runtime details in the Topics detail panel.",
                shortcut_hint: Some("m"),
                scope: CommandActionScope::Screen(Screen::Topics),
            },
            CommandAction::ToggleServiceRuntimeSection => CommandActionMetadata {
                id: "toggle-service-runtime-section",
                title: "Toggle Service Runtime Section",
                description: "Expand or collapse runtime details in the Services detail panel.",
                shortcut_hint: Some("m"),
                scope: CommandActionScope::Screen(Screen::Services),
            },
            CommandAction::ToggleNodeRuntimeSection => CommandActionMetadata {
                id: "toggle-node-runtime-section",
                title: "Toggle Node Runtime Section",
                description: "Expand or collapse runtime details in the Nodes detail panel.",
                shortcut_hint: Some("m"),
                scope: CommandActionScope::Screen(Screen::Nodes),
            },
            CommandAction::ExportTopicPreviewPayload => CommandActionMetadata {
                id: "export-topic-preview-payload",
                title: "Export Topic Preview Payload",
                description: "Export the selected topic preview frame with fallback metadata.",
                shortcut_hint: Some("y"),
                scope: CommandActionScope::Screen(Screen::Topics),
            },
            CommandAction::CopyTopicPreviewContent => CommandActionMetadata {
                id: "copy-topic-preview-content",
                title: "Copy Topic Preview Content",
                description: "Copy the selected topic preview frame content to the system clipboard.",
                shortcut_hint: Some("Y"),
                scope: CommandActionScope::Screen(Screen::Topics),
            },
            CommandAction::ExportServiceResponsePayload => CommandActionMetadata {
                id: "export-service-response-payload",
                title: "Export Service Response Payload",
                description: "Export the latest service-call response with fallback metadata.",
                shortcut_hint: Some("y"),
                scope: CommandActionScope::Screen(Screen::ServiceCall),
            },
            CommandAction::OpenHelp => CommandActionMetadata {
                id: "open-help",
                title: "Open Help",
                description: "Show the shortcut and provenance help overlay.",
                shortcut_hint: Some("? / h"),
                scope: CommandActionScope::Global,
            },
        }
    }

    fn search_key(self) -> String {
        let metadata = self.metadata();
        format!(
            "{} {} {} {}",
            metadata.id,
            metadata.title,
            metadata.description,
            metadata.shortcut_hint.unwrap_or("")
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionMode {
    Normal,
    Timeline,
    LogStream,
    Topology,
    Recorder,
    NodeCompare,
    Bookmarks,
    Palette,
    Search,
    Edit,
    Help,
}

impl InteractionMode {
    pub fn label(self) -> &'static str {
        match self {
            InteractionMode::Normal => "NORMAL",
            InteractionMode::Timeline => "TIMELINE",
            InteractionMode::LogStream => "LOGS",
            InteractionMode::Topology => "TOPOLOGY",
            InteractionMode::Recorder => "RECORDER",
            InteractionMode::NodeCompare => "NODE-COMPARE",
            InteractionMode::Bookmarks => "BOOKMARKS",
            InteractionMode::Palette => "PALETTE",
            InteractionMode::Search => "SEARCH",
            InteractionMode::Edit => "EDIT",
            InteractionMode::Help => "HELP",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeSectionPreferences {
    pub topics_expanded: bool,
    pub services_expanded: bool,
    pub nodes_expanded: bool,
}

impl Default for RuntimeSectionPreferences {
    fn default() -> Self {
        Self {
            topics_expanded: true,
            services_expanded: true,
            nodes_expanded: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookmarkKind {
    Topic,
    Service,
    Node,
}

impl BookmarkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Topic => "topic",
            Self::Service => "service",
            Self::Node => "node",
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Self::Topic => "Topic",
            Self::Service => "Service",
            Self::Node => "Node",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceBookmark {
    pub group: String,
    pub kind: BookmarkKind,
    pub key: String,
    pub label: String,
}

impl ResourceBookmark {
    pub fn topic(group: impl Into<String>, name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            group: group.into(),
            kind: BookmarkKind::Topic,
            key: name.clone(),
            label: name,
        }
    }

    pub fn service(group: impl Into<String>, name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            group: group.into(),
            kind: BookmarkKind::Service,
            key: name.clone(),
            label: name,
        }
    }

    pub fn node(
        group: impl Into<String>,
        node_id: impl Into<String>,
        label: impl Into<String>,
    ) -> Self {
        Self {
            group: group.into(),
            kind: BookmarkKind::Node,
            key: node_id.into(),
            label: label.into(),
        }
    }

    pub fn title(&self) -> &'static str {
        self.kind.title()
    }

    pub fn identity(&self) -> (&'static str, &str) {
        (self.kind.as_str(), self.key.as_str())
    }

    pub fn belongs_to_group(&self, group: &str) -> bool {
        self.group == group
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookmarkToggleResult {
    pub bookmark: ResourceBookmark,
    pub pinned: bool,
}

pub trait SearchableRow {
    fn search_key(&self) -> String;
}

impl SearchableRow for TopicRow {
    fn search_key(&self) -> String {
        format!("{} {}", self.name, self.schema_availability.as_label())
    }
}

impl SearchableRow for ServiceRow {
    fn search_key(&self) -> String {
        format!("{} {}", self.name, self.schema_availability.as_label())
    }
}

impl SearchableRow for ActionRow {
    fn search_key(&self) -> String {
        format!(
            "{} {} {} {} {}",
            self.name,
            self.server_node_name,
            self.schema_hash,
            if self.supports_cancel {
                "cancel"
            } else {
                "no-cancel"
            },
            self.status_summary
        )
    }
}

impl SearchableRow for NodeRow {
    fn search_key(&self) -> String {
        format!("{} {} {}", self.name, self.node_id, self.group)
    }
}

impl SearchableRow for ZenohRouterRow {
    fn search_key(&self) -> String {
        format!(
            "{} {} {} {} {} {}",
            self.zid,
            self.whatami,
            self.version.as_deref().unwrap_or("unknown"),
            self.locators.join(" "),
            self.plugin_names.join(" "),
            if self.metrics_available {
                "metrics"
            } else {
                "no-metrics"
            }
        )
    }
}

pub struct SelectableList<T> {
    rows: Vec<T>,
    visible_indices: Vec<usize>,
    selected: usize,
    query: String,
    search: SearchIndex,
}

impl<T> Default for SelectableList<T> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            visible_indices: Vec::new(),
            selected: 0,
            query: String::new(),
            search: SearchIndex::default(),
        }
    }
}

impl<T> SelectableList<T> {
    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn total_row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn visible_row_count(&self) -> usize {
        self.visible_indices.len()
    }

    pub fn has_query(&self) -> bool {
        !self.query.trim().is_empty()
    }

    pub fn select_visible_index(&mut self, index: usize) -> bool {
        if index >= self.visible_indices.len() {
            return false;
        }
        self.selected = index;
        true
    }
}

impl<T: SearchableRow> SelectableList<T> {
    pub fn replace(&mut self, rows: Vec<T>) {
        self.rows = rows;
        self.search.replace(
            self.rows
                .iter()
                .enumerate()
                .map(|(index, row)| (index, row.search_key())),
        );
        self.apply_query();
    }

    pub fn set_query(&mut self, query: String) {
        self.query = query;
        self.apply_query();
    }

    pub fn select_next(&mut self) {
        if !self.visible_indices.is_empty() {
            self.selected = (self.selected + 1).min(self.visible_indices.len() - 1);
        }
    }

    pub fn select_previous(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    pub fn visible_rows(&self) -> Vec<&T> {
        self.visible_indices
            .iter()
            .filter_map(|index| self.rows.get(*index))
            .collect()
    }

    pub fn selected_row(&self) -> Option<&T> {
        let visible_index = *self.visible_indices.get(self.selected)?;
        self.rows.get(visible_index)
    }

    pub fn selected_index(&self) -> Option<usize> {
        if self.visible_indices.is_empty() {
            None
        } else {
            Some(self.selected)
        }
    }

    pub fn clear_query(&mut self) {
        self.query.clear();
        self.apply_query();
    }

    pub fn select_by<F>(&mut self, predicate: F) -> bool
    where
        F: Fn(&T) -> bool,
    {
        self.clear_query();
        self.select_by_preserving_query(predicate)
    }

    pub fn select_by_preserving_query<F>(&mut self, predicate: F) -> bool
    where
        F: Fn(&T) -> bool,
    {
        let Some(row_index) = self.rows.iter().position(predicate) else {
            return false;
        };
        let Some(visible_index) = self
            .visible_indices
            .iter()
            .position(|index| *index == row_index)
        else {
            return false;
        };
        self.selected = visible_index;
        true
    }

    fn apply_query(&mut self) {
        self.search.set_query(&self.query);
        self.visible_indices = self.search.matched_indices();
        if self.visible_indices.is_empty() {
            self.selected = 0;
        } else if self.selected >= self.visible_indices.len() {
            self.selected = self.visible_indices.len() - 1;
        }
    }
}

/// A single tracked resource's bounded client-observed latency ring, used by the
/// Telemetry screen to render a sparkline + approximate percentiles.
///
/// Each entry is a `last_call_latency_ns` last-value observed from a refresh of
/// the runtime breakdown. The ring is capped at [`LATENCY_HISTORY_CAPACITY`] and
/// de-duplicates on the paired `last_call_finished_ns` stamp so a single
/// unchanged last-value is not re-counted across refresh ticks. These are
/// CLIENT-OBSERVED latencies — approximate and not the robot's on-stage timing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceLatencyHistory {
    samples: VecDeque<u64>,
    last_finished_ns: u64,
}

impl ResourceLatencyHistory {
    fn observe(&mut self, latency_ns: u64, finished_ns: u64) {
        if finished_ns == 0 || finished_ns <= self.last_finished_ns {
            return;
        }
        self.last_finished_ns = finished_ns;
        if self.samples.len() >= LATENCY_HISTORY_CAPACITY {
            self.samples.pop_front();
        }
        self.samples.push_back(latency_ns);
    }

    /// The retained samples, oldest-first (sparkline render order).
    pub fn samples(&self) -> &VecDeque<u64> {
        &self.samples
    }
}

/// All per-resource latency rings tracked for the Telemetry screen, keyed by a
/// `kind:resource_name` label. Bounded by [`LATENCY_HISTORY_CAPACITY`] per ring;
/// keys accumulate for whatever resources the loaded node/service detail surfaces.
#[derive(Debug, Clone, Default)]
pub struct LatencyHistory {
    rings: std::collections::BTreeMap<String, ResourceLatencyHistory>,
}

impl LatencyHistory {
    fn observe(&mut self, label: &str, latency_ns: u64, finished_ns: u64) {
        self.rings
            .entry(label.to_string())
            .or_default()
            .observe(latency_ns, finished_ns);
    }

    /// Iterate tracked `(label, ring)` pairs in stable (sorted) order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &ResourceLatencyHistory)> {
        self.rings
            .iter()
            .map(|(label, ring)| (label.as_str(), ring))
    }

    /// Number of tracked resources.
    pub fn len(&self) -> usize {
        self.rings.len()
    }

    /// Whether any resource is tracked.
    pub fn is_empty(&self) -> bool {
        self.rings.is_empty()
    }
}

pub struct App {
    active_screen: Screen,
    advanced_mode: bool,
    current_group: String,
    bookmarks: Vec<ResourceBookmark>,
    overview: OverviewData,
    overview_selected_action: usize,
    topics: SelectableList<TopicRow>,
    services: SelectableList<ServiceRow>,
    actions: SelectableList<ActionRow>,
    nodes: SelectableList<NodeRow>,
    zenoh_routers: SelectableList<ZenohRouterRow>,
    topic_detail: DetailPane<TopicDetailData>,
    topic_preview: TopicPreviewState,
    topic_schema_describe: DetailPane<SchemaDescribeData>,
    service_detail: DetailPane<ServiceDetailData>,
    service_schema_describe: DetailPane<SchemaDescribeData>,
    action_detail: DetailPane<ActionDetailData>,
    node_detail: DetailPane<NodeDetailData>,
    context: DetailPane<ContextData>,
    zenoh_connectivity: DetailPane<ZenohConnectivityData>,
    zenoh_connectivity_events: VecDeque<ZenohConnectivityEventData>,
    zenoh_connectivity_event_cursor: u64,
    zenoh_internals: DetailPane<ZenohInternalsData>,
    service_call: ServiceCallState,
    action_call: ActionCallState,
    event_timeline: EventTimelineState,
    log_stream: LogStreamState,
    log_rows: VecDeque<LogRow>,
    topology_overlay: TopologyOverlayState,
    recorder_overlay: RecorderOverlayState,
    node_comparison: NodeComparisonState,
    bookmark_overlay: BookmarkOverlayState,
    command_palette: CommandPaletteState,
    runtime_sections: RuntimeSectionPreferences,
    status: String,
    event_log: VecDeque<EventLogEntry>,
    next_event_sequence: u64,
    searching: bool,
    help_visible: bool,
    should_quit: bool,
    detail_scroll: u16,
    auto_refresh_enabled: bool,
    latency_history: LatencyHistory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetailPane<T> {
    Empty,
    Loaded(T),
    Error(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSeverity {
    Info,
    Warn,
    Error,
}

impl EventSeverity {
    pub fn label(self) -> &'static str {
        match self {
            EventSeverity::Info => "INFO",
            EventSeverity::Warn => "WARN",
            EventSeverity::Error => "ERROR",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSource {
    System,
    Navigation,
    Data,
    TopicPreview,
    ServiceCall,
    ActionCall,
    Bookmarks,
    Zenoh,
}

impl EventSource {
    pub fn label(self) -> &'static str {
        match self {
            EventSource::System => "system",
            EventSource::Navigation => "navigation",
            EventSource::Data => "data",
            EventSource::TopicPreview => "preview",
            EventSource::ServiceCall => "service",
            EventSource::ActionCall => "action",
            EventSource::Bookmarks => "bookmark",
            EventSource::Zenoh => "zenoh",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventLogEntry {
    pub sequence: u64,
    pub observed_at: Instant,
    pub source: EventSource,
    pub severity: EventSeverity,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventTimelineSeverityFilter {
    All,
    WarnAndError,
    ErrorOnly,
    InfoOnly,
}

impl EventTimelineSeverityFilter {
    pub fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::WarnAndError => "warn+error",
            Self::ErrorOnly => "error-only",
            Self::InfoOnly => "info-only",
        }
    }

    fn includes(self, severity: EventSeverity) -> bool {
        match self {
            Self::All => true,
            Self::WarnAndError => matches!(severity, EventSeverity::Warn | EventSeverity::Error),
            Self::ErrorOnly => matches!(severity, EventSeverity::Error),
            Self::InfoOnly => matches!(severity, EventSeverity::Info),
        }
    }

    fn next(self) -> Self {
        match self {
            Self::All => Self::WarnAndError,
            Self::WarnAndError => Self::ErrorOnly,
            Self::ErrorOnly => Self::InfoOnly,
            Self::InfoOnly => Self::All,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventTimelineSourceFilter {
    All,
    System,
    Navigation,
    Data,
    TopicPreview,
    ServiceCall,
    Bookmarks,
    Zenoh,
}

impl EventTimelineSourceFilter {
    pub fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::System => "system",
            Self::Navigation => "navigation",
            Self::Data => "data",
            Self::TopicPreview => "preview",
            Self::ServiceCall => "service",
            Self::Bookmarks => "bookmark",
            Self::Zenoh => "zenoh",
        }
    }

    fn includes(self, source: EventSource) -> bool {
        match self {
            Self::All => true,
            Self::System => matches!(source, EventSource::System),
            Self::Navigation => matches!(source, EventSource::Navigation),
            Self::Data => matches!(source, EventSource::Data),
            Self::TopicPreview => matches!(source, EventSource::TopicPreview),
            Self::ServiceCall => matches!(source, EventSource::ServiceCall),
            Self::Bookmarks => matches!(source, EventSource::Bookmarks),
            Self::Zenoh => matches!(source, EventSource::Zenoh),
        }
    }

    fn next(self) -> Self {
        match self {
            Self::All => Self::System,
            Self::System => Self::Navigation,
            Self::Navigation => Self::Data,
            Self::Data => Self::TopicPreview,
            Self::TopicPreview => Self::ServiceCall,
            Self::ServiceCall => Self::Bookmarks,
            Self::Bookmarks => Self::Zenoh,
            Self::Zenoh => Self::All,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventTimelineState {
    visible: bool,
    selected: usize,
    severity_filter: EventTimelineSeverityFilter,
    source_filter: EventTimelineSourceFilter,
}

impl Default for EventTimelineState {
    fn default() -> Self {
        Self {
            visible: false,
            selected: 0,
            severity_filter: EventTimelineSeverityFilter::All,
            source_filter: EventTimelineSourceFilter::All,
        }
    }
}

impl EventTimelineState {
    fn open(&mut self, visible_count: usize) {
        self.visible = true;
        self.normalize(visible_count);
    }

    fn close(&mut self) {
        self.visible = false;
    }

    fn select_next(&mut self, visible_count: usize) {
        if visible_count > 0 {
            self.selected = (self.selected + 1).min(visible_count - 1);
        }
    }

    fn select_previous(&mut self, visible_count: usize) {
        if visible_count > 0 {
            self.selected = self.selected.saturating_sub(1);
        }
    }

    fn normalize(&mut self, visible_count: usize) {
        if visible_count == 0 {
            self.selected = 0;
        } else if self.selected >= visible_count {
            self.selected = visible_count - 1;
        }
    }

    fn selected_index(&self, visible_count: usize) -> Option<usize> {
        if visible_count == 0 {
            None
        } else {
            Some(self.selected.min(visible_count - 1))
        }
    }

    fn select_index(&mut self, index: usize, visible_count: usize) -> bool {
        if visible_count == 0 || index >= visible_count {
            return false;
        }
        self.selected = index;
        true
    }
}

/// Overlay state for the live `/log` stream viewer (Feature C4). Modeled on
/// [`EventTimelineState`]: a centered overlay listing the bounded `log_rows`
/// deque with a selected row whose detail is shown alongside. The buffer itself
/// lives on `App` (mirroring `event_log`); this only tracks the view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogStreamState {
    visible: bool,
    selected: usize,
}

impl LogStreamState {
    fn open(&mut self, visible_count: usize) {
        self.visible = true;
        self.normalize(visible_count);
    }

    fn close(&mut self) {
        self.visible = false;
    }

    fn select_next(&mut self, visible_count: usize) {
        if visible_count > 0 {
            self.selected = (self.selected + 1).min(visible_count - 1);
        }
    }

    fn select_previous(&mut self, visible_count: usize) {
        if visible_count > 0 {
            self.selected = self.selected.saturating_sub(1);
        }
    }

    fn normalize(&mut self, visible_count: usize) {
        if visible_count == 0 {
            self.selected = 0;
        } else if self.selected >= visible_count {
            self.selected = visible_count - 1;
        }
    }

    fn selected_index(&self, visible_count: usize) -> Option<usize> {
        if visible_count == 0 {
            None
        } else {
            Some(self.selected.min(visible_count - 1))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicPreviewViewMode {
    Decoded,
    Raw,
    Diff,
}

impl TopicPreviewViewMode {
    pub fn label(self) -> &'static str {
        match self {
            TopicPreviewViewMode::Decoded => "decoded",
            TopicPreviewViewMode::Raw => "raw",
            TopicPreviewViewMode::Diff => "diff",
        }
    }

    fn toggle(self) -> Self {
        match self {
            TopicPreviewViewMode::Decoded => TopicPreviewViewMode::Raw,
            TopicPreviewViewMode::Raw => TopicPreviewViewMode::Diff,
            TopicPreviewViewMode::Diff => TopicPreviewViewMode::Decoded,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicPreviewDiffReference {
    Previous,
    Next,
}

impl TopicPreviewDiffReference {
    pub fn label(self) -> &'static str {
        match self {
            TopicPreviewDiffReference::Previous => "previous",
            TopicPreviewDiffReference::Next => "next",
        }
    }
}

pub struct TopicPreviewDiffPair<'a> {
    pub selected_frame: &'a TopicPreviewFrame,
    pub selected_position: usize,
    pub reference_frame: &'a TopicPreviewFrame,
    pub reference_position: usize,
    pub reference_relation: TopicPreviewDiffReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeComparisonPair<'a> {
    pub anchor: &'a NodeRow,
    pub current: &'a NodeRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeComparisonAnchorToggle {
    pub active: bool,
    pub node_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecorderLaunchScope {
    Overview,
    Topic,
    Service,
    ServiceCall,
    Node,
}

impl RecorderLaunchScope {
    pub fn label(self) -> &'static str {
        match self {
            Self::Overview => "group",
            Self::Topic => "topic",
            Self::Service => "service",
            Self::ServiceCall => "service-call",
            Self::Node => "node",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecorderLaunchPlan {
    pub scope: RecorderLaunchScope,
    pub target_label: String,
    pub group: String,
    pub suggested_usage: String,
    pub output_path: String,
    pub primary_command: String,
    pub alternate_command: String,
    pub notes: Vec<String>,
    pub export_subject: String,
}

impl RecorderLaunchPlan {
    pub fn export_script(&self) -> String {
        let mut lines = vec![
            "#!/usr/bin/env bash".to_string(),
            "set -euo pipefail".to_string(),
            String::new(),
            "# Generated by aorta-tui recorder launchpad.".to_string(),
            format!("# Scope: {}", self.scope.label()),
            format!("# Target: {}", self.target_label),
            format!("# Group: {}", self.group),
            format!("# Suggested usage: {}", self.suggested_usage),
            format!("# Output: {}", self.output_path),
        ];
        if !self.notes.is_empty() {
            lines.push("# Notes:".to_string());
            lines.extend(self.notes.iter().map(|note| format!("# - {}", note)));
        }
        lines.extend([
            String::new(),
            self.primary_command.clone(),
            String::new(),
            "# Standalone binary alternative:".to_string(),
            self.alternate_command.clone(),
            String::new(),
        ]);
        lines.join("\n")
    }
}

#[derive(Debug, Clone)]
pub struct TopicPreviewEntry {
    pub frame: TopicPreviewFrame,
    pub observed_at: Instant,
}

pub struct TopicPreviewState {
    target_topic: Option<String>,
    latest_frame: Option<TopicPreviewFrame>,
    history: VecDeque<TopicPreviewEntry>,
    selected_history_index: Option<usize>,
    message_count: u64,
    last_message_at: Option<Instant>,
    intervals: VecDeque<f64>,
    /// Per-message payload byte counts, same sliding window as `intervals`.
    sizes: VecDeque<u64>,
    last_error: Option<String>,
    scroll: u16,
    view_mode: TopicPreviewViewMode,
}

impl Default for TopicPreviewState {
    fn default() -> Self {
        Self {
            target_topic: None,
            latest_frame: None,
            history: VecDeque::with_capacity(TOPIC_PREVIEW_HISTORY_CAPACITY),
            selected_history_index: None,
            message_count: 0,
            last_message_at: None,
            intervals: VecDeque::with_capacity(32),
            sizes: VecDeque::with_capacity(32),
            last_error: None,
            scroll: 0,
            view_mode: TopicPreviewViewMode::Decoded,
        }
    }
}

impl TopicPreviewState {
    pub fn target_topic(&self) -> Option<&str> {
        self.target_topic.as_deref()
    }

    pub fn latest_frame(&self) -> Option<&TopicPreviewFrame> {
        self.latest_frame.as_ref()
    }

    pub fn selected_frame(&self) -> Option<&TopicPreviewFrame> {
        let index = self.selected_history_index?;
        self.history.get(index).map(|entry| &entry.frame)
    }

    pub fn selected_frame_age(&self) -> Option<Duration> {
        let index = self.selected_history_index?;
        self.history
            .get(index)
            .map(|entry| entry.observed_at.elapsed())
    }

    pub fn history_position(&self) -> Option<(usize, usize)> {
        let total = self.history.len();
        let index = self.selected_history_index?;
        Some((index + 1, total))
    }

    pub fn diff_pair(&self) -> Option<TopicPreviewDiffPair<'_>> {
        let total = self.history.len();
        let selected_index = self.selected_history_index?;
        let (reference_index, reference_relation) = if selected_index > 0 {
            (selected_index - 1, TopicPreviewDiffReference::Previous)
        } else if selected_index + 1 < total {
            (selected_index + 1, TopicPreviewDiffReference::Next)
        } else {
            return None;
        };

        Some(TopicPreviewDiffPair {
            selected_frame: &self.history.get(selected_index)?.frame,
            selected_position: selected_index + 1,
            reference_frame: &self.history.get(reference_index)?.frame,
            reference_position: reference_index + 1,
            reference_relation,
        })
    }

    pub fn is_viewing_latest(&self) -> bool {
        matches!(self.history_position(), Some((position, total)) if position == total)
    }

    pub fn message_count(&self) -> u64 {
        self.message_count
    }

    pub fn last_message_age(&self) -> Option<Duration> {
        self.last_message_at.map(|instant| instant.elapsed())
    }

    pub fn live_rate_hz(&self) -> Option<f64> {
        if self.intervals.is_empty() {
            return None;
        }
        let mean = self.intervals.iter().copied().sum::<f64>() / self.intervals.len() as f64;
        if mean <= 0.0 {
            None
        } else {
            Some(1.0 / mean)
        }
    }

    /// Mean payload size (bytes) over the sliding window.
    pub fn mean_payload_bytes(&self) -> Option<f64> {
        if self.sizes.is_empty() {
            return None;
        }
        let sum: u64 = self.sizes.iter().copied().sum();
        Some(sum as f64 / self.sizes.len() as f64)
    }

    /// Live bandwidth estimate: `live_rate_hz * mean_payload_bytes`. `None` until
    /// both a rate (≥2 messages) and a size sample are available.
    pub fn live_bytes_per_s(&self) -> Option<f64> {
        Some(self.live_rate_hz()? * self.mean_payload_bytes()?)
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub fn scroll(&self) -> u16 {
        self.scroll
    }

    pub fn view_mode(&self) -> TopicPreviewViewMode {
        self.view_mode
    }
}

pub struct ServiceCallState {
    target_service: Option<String>,
    schema: DetailPane<ServiceCallSchemaData>,
    response: DetailPane<ServiceCallResponseData>,
    editor: TextEditor,
    editing: bool,
    selected_hint_index: usize,
    response_scroll: u16,
}

impl Default for ServiceCallState {
    fn default() -> Self {
        Self {
            target_service: None,
            schema: DetailPane::Empty,
            response: DetailPane::Empty,
            editor: TextEditor::default(),
            editing: false,
            selected_hint_index: 0,
            response_scroll: 0,
        }
    }
}

impl ServiceCallState {
    pub fn target_service(&self) -> Option<&str> {
        self.target_service.as_deref()
    }

    pub fn schema(&self) -> &DetailPane<ServiceCallSchemaData> {
        &self.schema
    }

    pub fn response(&self) -> &DetailPane<ServiceCallResponseData> {
        &self.response
    }

    pub fn editor(&self) -> &TextEditor {
        &self.editor
    }

    pub fn is_editing(&self) -> bool {
        self.editing
    }

    pub fn response_scroll(&self) -> u16 {
        self.response_scroll
    }
}

/// Interactive state for the Action Call screen — a JSON goal editor + send/
/// cancel surface, mirroring [`ServiceCallState`]. `last_goal` retains the
/// `(goal_id, goal_secret)` returned by the most recent accepted send so the
/// cancel key has the authorization token without re-prompting.
pub struct ActionCallState {
    target_action: Option<String>,
    schema: DetailPane<ActionCallSchemaData>,
    response: DetailPane<ActionCallResponseData>,
    editor: TextEditor,
    editing: bool,
    selected_hint_index: usize,
    response_scroll: u16,
    last_goal: Option<(String, String)>,
    /// Bounded live-feedback panel (Feature C2): decoded `Feedback` rows for the
    /// most recently sent goal, capped at [`ACTION_FEEDBACK_CAPACITY`].
    feedback: VecDeque<FeedbackRow>,
}

impl Default for ActionCallState {
    fn default() -> Self {
        Self {
            target_action: None,
            schema: DetailPane::Empty,
            response: DetailPane::Empty,
            editor: TextEditor::default(),
            editing: false,
            selected_hint_index: 0,
            response_scroll: 0,
            last_goal: None,
            feedback: VecDeque::with_capacity(ACTION_FEEDBACK_CAPACITY),
        }
    }
}

impl ActionCallState {
    pub fn target_action(&self) -> Option<&str> {
        self.target_action.as_deref()
    }

    pub fn schema(&self) -> &DetailPane<ActionCallSchemaData> {
        &self.schema
    }

    pub fn response(&self) -> &DetailPane<ActionCallResponseData> {
        &self.response
    }

    pub fn editor(&self) -> &TextEditor {
        &self.editor
    }

    pub fn is_editing(&self) -> bool {
        self.editing
    }

    pub fn response_scroll(&self) -> u16 {
        self.response_scroll
    }

    /// `(goal_id, goal_secret)` of the most recent accepted send, if any.
    pub fn last_goal(&self) -> Option<(&str, &str)> {
        self.last_goal
            .as_ref()
            .map(|(id, secret)| (id.as_str(), secret.as_str()))
    }

    /// Live feedback rows for the most recently sent goal (oldest first).
    pub fn feedback(&self) -> &VecDeque<FeedbackRow> {
        &self.feedback
    }
}

pub struct CommandPaletteState {
    visible: bool,
    actions: Vec<CommandAction>,
    visible_indices: Vec<usize>,
    selected: usize,
    query: String,
    search: SearchIndex,
}

impl Default for CommandPaletteState {
    fn default() -> Self {
        Self {
            visible: false,
            actions: Vec::new(),
            visible_indices: Vec::new(),
            selected: 0,
            query: String::new(),
            search: SearchIndex::default(),
        }
    }
}

#[derive(Default)]
struct TopologyOverlayState {
    visible: bool,
}

#[derive(Default)]
struct RecorderOverlayState {
    visible: bool,
}

#[derive(Default)]
struct NodeComparisonState {
    visible: bool,
    anchor_node_id: Option<String>,
}

#[derive(Default)]
struct BookmarkOverlayState {
    visible: bool,
    selected: usize,
}

impl BookmarkOverlayState {
    fn open(&mut self, bookmark_count: usize) {
        self.visible = true;
        if bookmark_count == 0 {
            self.selected = 0;
        } else if self.selected >= bookmark_count {
            self.selected = bookmark_count - 1;
        }
    }

    fn close(&mut self) {
        self.visible = false;
    }

    fn select_next(&mut self, bookmark_count: usize) {
        if bookmark_count > 0 {
            self.selected = (self.selected + 1).min(bookmark_count - 1);
        }
    }

    fn select_previous(&mut self, bookmark_count: usize) {
        if bookmark_count > 0 {
            self.selected = self.selected.saturating_sub(1);
        }
    }

    fn selected_index(&self, bookmark_count: usize) -> Option<usize> {
        if bookmark_count == 0 {
            None
        } else {
            Some(self.selected.min(bookmark_count - 1))
        }
    }

    fn normalize(&mut self, bookmark_count: usize) {
        if bookmark_count == 0 {
            self.selected = 0;
        } else if self.selected >= bookmark_count {
            self.selected = bookmark_count - 1;
        }
    }

    fn select_index(&mut self, index: usize, bookmark_count: usize) -> bool {
        if bookmark_count == 0 || index >= bookmark_count {
            return false;
        }
        self.selected = index;
        true
    }
}

impl CommandPaletteState {
    fn open(&mut self) {
        self.visible = true;
        self.query.clear();
        self.selected = 0;
        self.search.set_query("");
    }

    fn close(&mut self) {
        *self = Self::default();
    }

    fn replace_actions(&mut self, actions: Vec<CommandAction>) {
        self.actions = actions;
        self.search.replace(
            self.actions
                .iter()
                .enumerate()
                .map(|(index, action)| (index, action.search_key())),
        );
        self.apply_query();
    }

    fn set_query(&mut self, query: String) {
        self.query = query;
        self.apply_query();
    }

    fn select_next(&mut self) {
        if !self.visible_indices.is_empty() {
            self.selected = (self.selected + 1).min(self.visible_indices.len() - 1);
        }
    }

    fn select_previous(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    fn selected_action(&self) -> Option<CommandAction> {
        let visible_index = *self.visible_indices.get(self.selected)?;
        self.actions.get(visible_index).copied()
    }

    fn selected_index(&self) -> Option<usize> {
        if self.visible_indices.is_empty() {
            None
        } else {
            Some(self.selected)
        }
    }

    fn visible_actions(&self) -> Vec<CommandAction> {
        self.visible_indices
            .iter()
            .filter_map(|index| self.actions.get(*index).copied())
            .collect()
    }

    fn select_index(&mut self, index: usize) -> bool {
        if index >= self.visible_indices.len() {
            return false;
        }
        self.selected = index;
        true
    }

    fn apply_query(&mut self) {
        self.search.set_query(&self.query);
        self.visible_indices = self.search.matched_indices();
        if self.visible_indices.is_empty() {
            self.selected = 0;
        } else if self.selected >= self.visible_indices.len() {
            self.selected = self.visible_indices.len() - 1;
        }
    }
}

impl TopologyOverlayState {
    fn open(&mut self) {
        self.visible = true;
    }

    fn close(&mut self) {
        self.visible = false;
    }
}

impl RecorderOverlayState {
    fn open(&mut self) {
        self.visible = true;
    }

    fn close(&mut self) {
        self.visible = false;
    }
}

impl NodeComparisonState {
    fn set_anchor(&mut self, node_id: String) {
        self.anchor_node_id = Some(node_id);
    }

    fn clear_anchor(&mut self) {
        self.anchor_node_id = None;
        self.visible = false;
    }

    fn close(&mut self) {
        self.visible = false;
    }
}

impl Default for App {
    fn default() -> Self {
        Self {
            active_screen: Screen::Overview,
            advanced_mode: false,
            current_group: "default".to_string(),
            bookmarks: Vec::new(),
            overview: OverviewData {
                topic_count: 0,
                service_count: 0,
                node_count: 0,
                node_discovery_source: NodeDiscoverySource::AdminSnapshot,
                health: OverviewHealthSummary {
                    overall: HealthLevel::Unknown,
                    discovery_capability: CapabilityState::Partial,
                    runtime_capability: CapabilityState::Unavailable,
                    freshness: FreshnessState::Unavailable,
                    runtime_collection_period_ms: None,
                    runtime_topic_coverage: 0,
                    runtime_service_coverage: 0,
                    runtime_node_coverage: 0,
                    warning_count: 0,
                    error_count: 0,
                    unknown_count: 0,
                    schema_unknown_count: 0,
                },
                highlighted_topics: Vec::new(),
                highlighted_services: Vec::new(),
            },
            overview_selected_action: 0,
            topics: SelectableList::default(),
            services: SelectableList::default(),
            actions: SelectableList::default(),
            nodes: SelectableList::default(),
            zenoh_routers: SelectableList::default(),
            topic_detail: DetailPane::Empty,
            topic_preview: TopicPreviewState::default(),
            topic_schema_describe: DetailPane::Empty,
            service_detail: DetailPane::Empty,
            service_schema_describe: DetailPane::Empty,
            action_detail: DetailPane::Empty,
            node_detail: DetailPane::Empty,
            context: DetailPane::Empty,
            zenoh_connectivity: DetailPane::Empty,
            zenoh_connectivity_events: VecDeque::with_capacity(ZENOH_CONNECTIVITY_EVENT_CAPACITY),
            zenoh_connectivity_event_cursor: 0,
            zenoh_internals: DetailPane::Empty,
            service_call: ServiceCallState::default(),
            action_call: ActionCallState::default(),
            event_timeline: EventTimelineState::default(),
            log_stream: LogStreamState::default(),
            log_rows: VecDeque::with_capacity(LOG_STREAM_CAPACITY),
            topology_overlay: TopologyOverlayState::default(),
            recorder_overlay: RecorderOverlayState::default(),
            node_comparison: NodeComparisonState::default(),
            bookmark_overlay: BookmarkOverlayState::default(),
            command_palette: CommandPaletteState::default(),
            runtime_sections: RuntimeSectionPreferences::default(),
            status: "Ready. Press ? for help.".to_string(),
            event_log: VecDeque::from([EventLogEntry {
                sequence: 0,
                observed_at: Instant::now(),
                source: EventSource::System,
                severity: EventSeverity::Info,
                message: "Aorta TUI started.".to_string(),
            }]),
            next_event_sequence: 1,
            searching: false,
            help_visible: false,
            should_quit: false,
            detail_scroll: 0,
            auto_refresh_enabled: false,
            latency_history: LatencyHistory::default(),
        }
    }
}

impl App {
    pub fn replace_dashboard(&mut self, data: DashboardData) {
        let selected_topic = self.topics.selected_row().map(|row| row.name.clone());
        let selected_service = self.services.selected_row().map(|row| row.name.clone());
        let selected_node = self.nodes.selected_row().map(|row| row.node_id.clone());

        self.overview = data.overview;
        self.topics.replace(data.topics);
        self.services.replace(data.services);
        self.nodes.replace(data.nodes);
        if let Some(topic) = selected_topic {
            self.topics
                .select_by_preserving_query(|row| row.name == topic);
        }
        if let Some(service) = selected_service {
            self.services
                .select_by_preserving_query(|row| row.name == service);
        }
        if let Some(node_id) = selected_node {
            self.nodes
                .select_by_preserving_query(|row| row.node_id == node_id);
        }
        self.topic_detail = DetailPane::Empty;
        self.topic_preview = TopicPreviewState::default();
        self.topic_schema_describe = DetailPane::Empty;
        self.service_detail = DetailPane::Empty;
        self.service_schema_describe = DetailPane::Empty;
        self.action_detail = DetailPane::Empty;
        self.node_detail = DetailPane::Empty;
        self.context = DetailPane::Empty;
        self.zenoh_connectivity = DetailPane::Empty;
        self.zenoh_internals = DetailPane::Empty;
        self.zenoh_routers = SelectableList::default();
        self.service_call = ServiceCallState::default();
        self.action_call = ActionCallState::default();
        self.event_timeline
            .normalize(self.filtered_event_entries().len());
        self.topology_overlay.close();
        self.recorder_overlay.close();
        let anchor_exists = self
            .node_comparison
            .anchor_node_id
            .as_ref()
            .map(|anchor| self.nodes.rows().iter().any(|row| &row.node_id == anchor))
            .unwrap_or(false);
        if !anchor_exists {
            self.node_comparison.clear_anchor();
        } else if self.selected_node_comparison_pair().is_none() {
            self.node_comparison.close();
        }
        self.bookmark_overlay
            .normalize(self.current_group_bookmark_count());
        self.report_info_from(EventSource::Data, "Snapshot refreshed.".to_string());
    }

    pub fn replace_actions(&mut self, rows: Vec<ActionRow>) {
        let selected_name = self.actions.selected_row().map(|row| row.name.clone());
        self.actions.replace(rows);
        if let Some(name) = selected_name {
            self.actions
                .select_by_preserving_query(|row| row.name == name);
        }
        self.action_detail = DetailPane::Empty;
    }

    pub fn select_action_by_name(&mut self, name: &str) -> bool {
        self.actions
            .select_by_preserving_query(|row| row.name == name)
    }

    pub fn active_screen(&self) -> Screen {
        self.active_screen
    }

    pub fn visible_screens(&self) -> Vec<Screen> {
        let mut screens = vec![
            Screen::Overview,
            Screen::Topics,
            Screen::Services,
            Screen::ServiceCall,
            Screen::Actions,
            Screen::ActionCall,
            Screen::Nodes,
            Screen::Context,
        ];
        if self.advanced_mode {
            screens.push(Screen::Zenoh);
            screens.push(Screen::Telemetry);
        }
        screens
    }

    pub fn is_advanced_mode(&self) -> bool {
        self.advanced_mode
    }

    pub fn group(&self) -> &str {
        &self.current_group
    }

    pub fn set_group(&mut self, group: impl Into<String>) {
        self.current_group = group.into();
        self.bookmark_overlay
            .normalize(self.current_group_bookmark_count());
    }

    pub fn interaction_mode(&self) -> InteractionMode {
        if self.help_visible {
            InteractionMode::Help
        } else if self.event_timeline.visible {
            InteractionMode::Timeline
        } else if self.log_stream.visible {
            InteractionMode::LogStream
        } else if self.topology_overlay.visible {
            InteractionMode::Topology
        } else if self.recorder_overlay.visible {
            InteractionMode::Recorder
        } else if self.node_comparison.visible {
            InteractionMode::NodeCompare
        } else if self.bookmark_overlay.visible {
            InteractionMode::Bookmarks
        } else if self.command_palette.visible {
            InteractionMode::Palette
        } else if (self.active_screen == Screen::ServiceCall && self.service_call.is_editing())
            || (self.active_screen == Screen::ActionCall && self.action_call.is_editing())
        {
            InteractionMode::Edit
        } else if self.searching {
            InteractionMode::Search
        } else {
            InteractionMode::Normal
        }
    }

    pub fn available_command_actions(&self) -> Vec<CommandAction> {
        CommandAction::ALL
            .into_iter()
            .filter(|action| self.is_command_action_available(*action))
            .collect()
    }

    pub fn is_event_timeline_visible(&self) -> bool {
        self.event_timeline.visible
    }

    pub fn open_event_timeline(&mut self) -> bool {
        if self.interaction_mode() != InteractionMode::Normal {
            return false;
        }
        self.event_timeline
            .open(self.filtered_event_entries().len());
        true
    }

    pub fn close_event_timeline(&mut self) {
        self.event_timeline.close();
    }

    // --- Live `/log` stream viewer (Feature C4) ---------------------------

    pub fn is_log_stream_visible(&self) -> bool {
        self.log_stream.visible
    }

    /// Open the `/log` overlay. Only allowed from Normal mode (mirrors
    /// [`Self::open_event_timeline`]) so overlays stay mutually exclusive.
    pub fn open_log_stream(&mut self) -> bool {
        if self.interaction_mode() != InteractionMode::Normal {
            return false;
        }
        self.log_stream.open(self.log_rows.len());
        true
    }

    pub fn close_log_stream(&mut self) {
        self.log_stream.close();
    }

    /// Bounded `/log` rows (oldest first), as fed by the log-stream controller.
    pub fn log_rows(&self) -> &VecDeque<LogRow> {
        &self.log_rows
    }

    pub fn log_stream_selected_index(&self) -> Option<usize> {
        self.log_stream.selected_index(self.log_rows.len())
    }

    pub fn selected_log_row(&self) -> Option<&LogRow> {
        let index = self.log_stream_selected_index()?;
        self.log_rows.get(index)
    }

    pub fn log_stream_select_next(&mut self) {
        self.log_stream.select_next(self.log_rows.len());
    }

    pub fn log_stream_select_previous(&mut self) {
        self.log_stream.select_previous(self.log_rows.len());
    }

    /// Select a `/log` row by absolute index (mouse click); ignored when out of
    /// range. Returns whether the selection changed.
    pub fn select_log_stream_index(&mut self, index: usize) -> bool {
        let count = self.log_rows.len();
        if count == 0 || index >= count {
            return false;
        }
        self.log_stream.selected = index;
        true
    }

    /// Append a decoded `/log` row, evicting the oldest when the bounded buffer
    /// is full. Mirrors `record_event` / `push_zenoh_connectivity_event`.
    pub fn push_log_row(&mut self, row: LogRow) {
        if self.log_rows.len() >= LOG_STREAM_CAPACITY {
            self.log_rows.pop_front();
        }
        self.log_rows.push_back(row);
        // Keep the overlay selection in range as rows scroll past the cap.
        self.log_stream.normalize(self.log_rows.len());
    }

    // --- Live action feedback panel (Feature C2) --------------------------

    /// Append a decoded `Feedback` row for the in-flight goal, evicting the
    /// oldest when the per-goal panel is full.
    pub fn push_action_feedback_row(&mut self, row: FeedbackRow) {
        if self.action_call.feedback.len() >= ACTION_FEEDBACK_CAPACITY {
            self.action_call.feedback.pop_front();
        }
        self.action_call.feedback.push_back(row);
    }

    /// Drop all retained feedback rows (called when a new goal is sent or the
    /// Action Call screen is left, so the panel never shows stale feedback).
    pub fn clear_action_feedback(&mut self) {
        self.action_call.feedback.clear();
    }

    pub fn action_call_feedback(&self) -> &VecDeque<FeedbackRow> {
        self.action_call.feedback()
    }

    pub fn is_topology_overlay_visible(&self) -> bool {
        self.topology_overlay.visible
    }

    pub fn close_topology_overlay(&mut self) {
        self.topology_overlay.close();
    }

    pub fn is_recorder_overlay_visible(&self) -> bool {
        self.recorder_overlay.visible
    }

    pub fn close_recorder_overlay(&mut self) {
        self.recorder_overlay.close();
    }

    pub fn recorder_launch_plan(&self) -> Result<RecorderLaunchPlan, String> {
        match self.active_screen {
            Screen::Overview => Ok(build_overview_recorder_launch_plan(self.group())),
            Screen::Topics => {
                let selected = self
                    .topics
                    .selected_row()
                    .ok_or_else(|| "no topic is selected".to_string())?;
                Ok(build_topic_recorder_launch_plan(
                    self.group(),
                    &selected.name,
                ))
            }
            Screen::Services => {
                let selected = self
                    .services
                    .selected_row()
                    .ok_or_else(|| "no service is selected".to_string())?;
                Ok(build_service_recorder_launch_plan(
                    self.group(),
                    &selected.name,
                    RecorderLaunchScope::Service,
                ))
            }
            Screen::ServiceCall => {
                let service_name = self
                    .service_call_target()
                    .ok_or_else(|| "no service is selected for Service Call".to_string())?;
                Ok(build_service_recorder_launch_plan(
                    self.group(),
                    service_name,
                    RecorderLaunchScope::ServiceCall,
                ))
            }
            Screen::Nodes => {
                let selected = self
                    .nodes
                    .selected_row()
                    .ok_or_else(|| "no node is selected".to_string())?;
                Ok(build_node_recorder_launch_plan(
                    self.group(),
                    &selected.name,
                    &selected.node_id,
                ))
            }
            Screen::Actions => {
                Err("recorder launchpad is unavailable on the Actions screen".to_string())
            }
            Screen::ActionCall => {
                Err("recorder launchpad is unavailable on the Action Call screen".to_string())
            }
            Screen::Context => {
                Err("recorder launchpad is unavailable on the Context screen".to_string())
            }
            Screen::Zenoh => {
                Err("recorder launchpad is unavailable on the Zenoh screen".to_string())
            }
            Screen::Telemetry => {
                Err("recorder launchpad is unavailable on the Telemetry screen".to_string())
            }
        }
    }

    pub fn open_recorder_overlay(&mut self) -> Result<(), String> {
        if self.interaction_mode() != InteractionMode::Normal {
            return Err("recorder launchpad can only be opened in normal mode".to_string());
        }
        self.recorder_launch_plan()?;
        self.recorder_overlay.open();
        Ok(())
    }

    pub fn open_topology_overlay(&mut self) -> Result<(), String> {
        if self.interaction_mode() != InteractionMode::Normal {
            return Err("topology explorer can only be opened in normal mode".to_string());
        }
        match self.active_screen {
            Screen::Topics => {
                if self.topics.selected_row().is_none() {
                    return Err("no topic is selected".to_string());
                }
            }
            Screen::Services => {
                if self.services.selected_row().is_none() {
                    return Err("no service is selected".to_string());
                }
            }
            Screen::Nodes => {
                if self.nodes.selected_row().is_none() {
                    return Err("no node is selected".to_string());
                }
            }
            Screen::Overview
            | Screen::ServiceCall
            | Screen::Actions
            | Screen::ActionCall
            | Screen::Context
            | Screen::Zenoh
            | Screen::Telemetry => {
                return Err(
                    "topology explorer is available from Topics, Services, and Nodes".to_string(),
                );
            }
        }
        self.topology_overlay.open();
        Ok(())
    }

    pub fn is_node_comparison_visible(&self) -> bool {
        self.node_comparison.visible
    }

    pub fn close_node_comparison(&mut self) {
        self.node_comparison.close();
    }

    pub fn has_node_compare_anchor(&self) -> bool {
        self.node_comparison.anchor_node_id.is_some()
    }

    pub fn is_node_compare_anchor(&self, node_id: &str) -> bool {
        self.node_comparison.anchor_node_id.as_deref() == Some(node_id)
    }

    pub fn node_compare_anchor(&self) -> Option<&NodeRow> {
        let anchor_id = self.node_comparison.anchor_node_id.as_deref()?;
        self.nodes
            .rows()
            .iter()
            .find(|row| row.node_id == anchor_id)
    }

    pub fn selected_node_comparison_pair(&self) -> Option<NodeComparisonPair<'_>> {
        let anchor = self.node_compare_anchor()?;
        let current = self.nodes.selected_row()?;
        if anchor.node_id == current.node_id {
            return None;
        }
        Some(NodeComparisonPair { anchor, current })
    }

    pub fn toggle_node_compare_anchor(&mut self) -> Result<NodeComparisonAnchorToggle, String> {
        if self.active_screen != Screen::Nodes {
            return Err("node comparison anchor is only available on the Nodes screen".to_string());
        }
        let Some(selected) = self.nodes.selected_row() else {
            return Err("no node is selected".to_string());
        };
        if self.is_node_compare_anchor(&selected.node_id) {
            let node_name = selected.name.clone();
            self.node_comparison.clear_anchor();
            Ok(NodeComparisonAnchorToggle {
                active: false,
                node_name,
            })
        } else {
            let node_name = selected.name.clone();
            self.node_comparison.set_anchor(selected.node_id.clone());
            if self.selected_node_comparison_pair().is_none() {
                self.node_comparison.close();
            }
            Ok(NodeComparisonAnchorToggle {
                active: true,
                node_name,
            })
        }
    }

    pub fn open_node_comparison(&mut self) -> Result<(), String> {
        if self.interaction_mode() != InteractionMode::Normal {
            return Err("node comparison can only be opened in normal mode".to_string());
        }
        if self.active_screen != Screen::Nodes {
            return Err("node comparison is only available on the Nodes screen".to_string());
        }
        if self.node_compare_anchor().is_none() {
            return Err("set a comparison anchor first with `x` on a node".to_string());
        }
        if self.nodes.selected_row().is_none() {
            return Err("no node is selected".to_string());
        }
        if self.selected_node_comparison_pair().is_none() {
            return Err("select a different node to compare against the anchor".to_string());
        }
        self.node_comparison.visible = true;
        Ok(())
    }

    pub fn event_timeline_severity_filter(&self) -> EventTimelineSeverityFilter {
        self.event_timeline.severity_filter
    }

    pub fn event_timeline_source_filter(&self) -> EventTimelineSourceFilter {
        self.event_timeline.source_filter
    }

    pub fn event_timeline_selected_index(&self) -> Option<usize> {
        self.event_timeline
            .selected_index(self.filtered_event_entries().len())
    }

    pub fn filtered_event_entries(&self) -> Vec<&EventLogEntry> {
        let severity_filter = self.event_timeline.severity_filter;
        let source_filter = self.event_timeline.source_filter;
        self.event_log
            .iter()
            .rev()
            .filter(|entry| {
                severity_filter.includes(entry.severity) && source_filter.includes(entry.source)
            })
            .collect()
    }

    pub fn selected_filtered_event_entry(&self) -> Option<&EventLogEntry> {
        let index = self.event_timeline_selected_index()?;
        self.filtered_event_entries().into_iter().nth(index)
    }

    pub fn select_filtered_event_index(&mut self, index: usize) -> bool {
        self.event_timeline
            .select_index(index, self.filtered_event_entries().len())
    }

    pub fn select_next_filtered_event(&mut self) {
        self.event_timeline
            .select_next(self.filtered_event_entries().len());
    }

    pub fn select_previous_filtered_event(&mut self) {
        self.event_timeline
            .select_previous(self.filtered_event_entries().len());
    }

    pub fn cycle_event_timeline_severity_filter(&mut self) -> EventTimelineSeverityFilter {
        self.event_timeline.severity_filter = self.event_timeline.severity_filter.next();
        self.event_timeline
            .normalize(self.filtered_event_entries().len());
        self.event_timeline.severity_filter
    }

    pub fn cycle_event_timeline_source_filter(&mut self) -> EventTimelineSourceFilter {
        self.event_timeline.source_filter = self.event_timeline.source_filter.next();
        self.event_timeline
            .normalize(self.filtered_event_entries().len());
        self.event_timeline.source_filter
    }

    pub fn clear_event_timeline_filters(&mut self) {
        self.event_timeline.severity_filter = EventTimelineSeverityFilter::All;
        self.event_timeline.source_filter = EventTimelineSourceFilter::All;
        self.event_timeline
            .normalize(self.filtered_event_entries().len());
    }

    pub fn event_timeline_retention_limit(&self) -> usize {
        EVENT_LOG_CAPACITY
    }

    pub fn bookmarks(&self) -> &[ResourceBookmark] {
        &self.bookmarks
    }

    pub fn replace_bookmarks(&mut self, bookmarks: Vec<ResourceBookmark>) {
        self.bookmarks = normalize_bookmarks(bookmarks);
        self.bookmark_overlay
            .normalize(self.current_group_bookmark_count());
    }

    pub fn current_group_bookmarks(&self) -> Vec<&ResourceBookmark> {
        self.bookmarks
            .iter()
            .filter(|bookmark| bookmark.belongs_to_group(&self.current_group))
            .collect()
    }

    pub fn current_group_bookmark_count(&self) -> usize {
        self.bookmarks
            .iter()
            .filter(|bookmark| bookmark.belongs_to_group(&self.current_group))
            .count()
    }

    pub fn is_bookmark_overlay_visible(&self) -> bool {
        self.bookmark_overlay.visible
    }

    pub fn open_bookmark_overlay(&mut self) -> bool {
        if self.interaction_mode() != InteractionMode::Normal {
            return false;
        }
        self.bookmark_overlay
            .open(self.current_group_bookmark_count());
        true
    }

    pub fn close_bookmark_overlay(&mut self) {
        self.bookmark_overlay.close();
    }

    pub fn bookmark_overlay_selected_index(&self) -> Option<usize> {
        self.bookmark_overlay
            .selected_index(self.current_group_bookmark_count())
    }

    pub fn select_bookmark_overlay_index(&mut self, index: usize) -> bool {
        self.bookmark_overlay
            .select_index(index, self.current_group_bookmark_count())
    }

    pub fn selected_group_bookmark(&self) -> Option<&ResourceBookmark> {
        let index = self.bookmark_overlay_selected_index()?;
        self.current_group_bookmarks().into_iter().nth(index)
    }

    pub fn select_next_bookmark(&mut self) {
        self.bookmark_overlay
            .select_next(self.current_group_bookmark_count());
    }

    pub fn select_previous_bookmark(&mut self) {
        self.bookmark_overlay
            .select_previous(self.current_group_bookmark_count());
    }

    pub fn is_topic_bookmarked(&self, topic_name: &str) -> bool {
        self.bookmarks.iter().any(|bookmark| {
            bookmark.belongs_to_group(&self.current_group)
                && bookmark.kind == BookmarkKind::Topic
                && bookmark.key == topic_name
        })
    }

    pub fn is_service_bookmarked(&self, service_name: &str) -> bool {
        self.bookmarks.iter().any(|bookmark| {
            bookmark.belongs_to_group(&self.current_group)
                && bookmark.kind == BookmarkKind::Service
                && bookmark.key == service_name
        })
    }

    pub fn is_node_bookmarked(&self, node_id: &str) -> bool {
        self.bookmarks.iter().any(|bookmark| {
            bookmark.belongs_to_group(&self.current_group)
                && bookmark.kind == BookmarkKind::Node
                && bookmark.key == node_id
        })
    }

    pub fn is_bookmark_available(&self, bookmark: &ResourceBookmark) -> bool {
        if !bookmark.belongs_to_group(&self.current_group) {
            return false;
        }
        match bookmark.kind {
            BookmarkKind::Topic => self.topics.rows.iter().any(|row| row.name == bookmark.key),
            BookmarkKind::Service => self
                .services
                .rows
                .iter()
                .any(|row| row.name == bookmark.key),
            BookmarkKind::Node => self
                .nodes
                .rows
                .iter()
                .any(|row| row.node_id == bookmark.key),
        }
    }

    pub fn toggle_current_bookmark(&mut self) -> Result<BookmarkToggleResult, String> {
        let bookmark = self.current_selection_bookmark()?;
        let identity = bookmark_identity(&bookmark);
        if let Some(index) = self
            .bookmarks
            .iter()
            .position(|candidate| bookmark_identity(candidate) == identity)
        {
            let removed = self.bookmarks.remove(index);
            self.bookmark_overlay
                .normalize(self.current_group_bookmark_count());
            Ok(BookmarkToggleResult {
                bookmark: removed,
                pinned: false,
            })
        } else {
            self.bookmarks.push(bookmark.clone());
            self.bookmarks = normalize_bookmarks(std::mem::take(&mut self.bookmarks));
            self.bookmark_overlay
                .normalize(self.current_group_bookmark_count());
            Ok(BookmarkToggleResult {
                bookmark,
                pinned: true,
            })
        }
    }

    pub fn remove_selected_bookmark(&mut self) -> Result<ResourceBookmark, String> {
        let bookmark = self
            .selected_group_bookmark()
            .cloned()
            .ok_or_else(|| "no bookmark is selected".to_string())?;
        let identity = bookmark_identity(&bookmark);
        let Some(index) = self
            .bookmarks
            .iter()
            .position(|candidate| bookmark_identity(candidate) == identity)
        else {
            return Err("selected bookmark is no longer available".to_string());
        };
        let removed = self.bookmarks.remove(index);
        self.bookmark_overlay
            .normalize(self.current_group_bookmark_count());
        Ok(removed)
    }

    pub fn open_selected_bookmark(&mut self) -> Result<Screen, String> {
        let bookmark = self
            .selected_group_bookmark()
            .cloned()
            .ok_or_else(|| "no bookmark is selected".to_string())?;
        self.close_bookmark_overlay();
        self.open_bookmark(&bookmark)
    }

    pub fn is_command_palette_visible(&self) -> bool {
        self.command_palette.visible
    }

    pub fn command_palette_query(&self) -> &str {
        &self.command_palette.query
    }

    pub fn command_palette_actions(&self) -> Vec<CommandAction> {
        self.command_palette.visible_actions()
    }

    pub fn command_palette_selected_action(&self) -> Option<CommandAction> {
        self.command_palette.selected_action()
    }

    pub fn command_palette_selected_index(&self) -> Option<usize> {
        self.command_palette.selected_index()
    }

    pub fn select_command_palette_action_index(&mut self, index: usize) -> bool {
        self.command_palette.select_index(index)
    }

    pub fn runtime_section_preferences(&self) -> RuntimeSectionPreferences {
        self.runtime_sections
    }

    pub fn apply_runtime_section_preferences(&mut self, preferences: RuntimeSectionPreferences) {
        self.runtime_sections = preferences;
    }

    pub fn is_topic_runtime_section_expanded(&self) -> bool {
        self.runtime_sections.topics_expanded
    }

    pub fn is_service_runtime_section_expanded(&self) -> bool {
        self.runtime_sections.services_expanded
    }

    pub fn is_node_runtime_section_expanded(&self) -> bool {
        self.runtime_sections.nodes_expanded
    }

    pub fn toggle_topic_runtime_section(&mut self) -> bool {
        self.runtime_sections.topics_expanded = !self.runtime_sections.topics_expanded;
        self.runtime_sections.topics_expanded
    }

    pub fn toggle_service_runtime_section(&mut self) -> bool {
        self.runtime_sections.services_expanded = !self.runtime_sections.services_expanded;
        self.runtime_sections.services_expanded
    }

    pub fn toggle_node_runtime_section(&mut self) -> bool {
        self.runtime_sections.nodes_expanded = !self.runtime_sections.nodes_expanded;
        self.runtime_sections.nodes_expanded
    }

    pub fn overview(&self) -> &OverviewData {
        &self.overview
    }

    pub fn overview_selected_action(&self) -> OverviewAction {
        OverviewAction::ALL[self.overview_selected_action]
    }

    pub fn select_overview_action_index(&mut self, index: usize) -> bool {
        if index >= OverviewAction::ALL.len() {
            return false;
        }
        self.overview_selected_action = index;
        true
    }

    pub fn topics(&self) -> &SelectableList<TopicRow> {
        &self.topics
    }

    pub fn select_topic_visible_index(&mut self, index: usize) -> bool {
        let previous = self.topics.selected_index();
        let changed = self.topics.select_visible_index(index);
        if changed && self.topics.selected_index() != previous {
            self.clear_topic_schema_describe();
        }
        changed
    }

    pub fn services(&self) -> &SelectableList<ServiceRow> {
        &self.services
    }

    pub fn select_service_visible_index(&mut self, index: usize) -> bool {
        let previous = self.services.selected_index();
        let changed = self.services.select_visible_index(index);
        if changed && self.services.selected_index() != previous {
            self.clear_service_schema_describe();
        }
        changed
    }

    pub fn actions(&self) -> &SelectableList<ActionRow> {
        &self.actions
    }

    pub fn select_action_visible_index(&mut self, index: usize) -> bool {
        self.actions.select_visible_index(index)
    }

    pub fn nodes(&self) -> &SelectableList<NodeRow> {
        &self.nodes
    }

    pub fn select_node_visible_index(&mut self, index: usize) -> bool {
        self.nodes.select_visible_index(index)
    }

    pub fn zenoh_routers(&self) -> &SelectableList<ZenohRouterRow> {
        &self.zenoh_routers
    }

    pub fn select_zenoh_router_visible_index(&mut self, index: usize) -> bool {
        self.zenoh_routers.select_visible_index(index)
    }

    pub fn status(&self) -> &str {
        &self.status
    }

    pub fn is_help_visible(&self) -> bool {
        self.help_visible
    }

    pub fn event_log(&self) -> &VecDeque<EventLogEntry> {
        &self.event_log
    }

    pub fn zenoh_connectivity_events(&self) -> &VecDeque<ZenohConnectivityEventData> {
        &self.zenoh_connectivity_events
    }

    pub fn topic_detail(&self) -> &DetailPane<TopicDetailData> {
        &self.topic_detail
    }

    pub fn topic_preview(&self) -> &TopicPreviewState {
        &self.topic_preview
    }

    pub fn selected_topic_preview_frame(&self) -> Option<&TopicPreviewFrame> {
        self.topic_preview.selected_frame()
    }

    pub fn service_detail(&self) -> &DetailPane<ServiceDetailData> {
        &self.service_detail
    }

    pub fn topic_schema_describe(&self) -> &DetailPane<SchemaDescribeData> {
        &self.topic_schema_describe
    }

    pub fn service_schema_describe(&self) -> &DetailPane<SchemaDescribeData> {
        &self.service_schema_describe
    }

    pub fn is_topic_schema_describe_visible(&self) -> bool {
        !matches!(self.topic_schema_describe, DetailPane::Empty)
    }

    pub fn is_service_schema_describe_visible(&self) -> bool {
        !matches!(self.service_schema_describe, DetailPane::Empty)
    }

    pub fn action_detail(&self) -> &DetailPane<ActionDetailData> {
        &self.action_detail
    }

    pub fn node_detail(&self) -> &DetailPane<NodeDetailData> {
        &self.node_detail
    }

    pub fn context(&self) -> &DetailPane<ContextData> {
        &self.context
    }

    pub fn zenoh_internals(&self) -> &DetailPane<ZenohInternalsData> {
        &self.zenoh_internals
    }

    pub fn zenoh_connectivity(&self) -> &DetailPane<ZenohConnectivityData> {
        &self.zenoh_connectivity
    }

    pub fn service_call(&self) -> &ServiceCallState {
        &self.service_call
    }

    pub fn loaded_service_call_response(&self) -> Option<&ServiceCallResponseData> {
        match self.service_call.response() {
            DetailPane::Loaded(response) => Some(response),
            DetailPane::Empty | DetailPane::Error(_) => None,
        }
    }

    pub fn service_call_selected_hint_index(&self) -> Option<usize> {
        match &self.service_call.schema {
            DetailPane::Loaded(schema) if !schema.request_model.fields.is_empty() => {
                Some(self.service_call.selected_hint_index)
            }
            _ => None,
        }
    }

    pub fn service_call_selected_hint(&self) -> Option<&aorta_introspect::schema::SchemaField> {
        match &self.service_call.schema {
            DetailPane::Loaded(schema) => schema
                .request_model
                .fields
                .get(self.service_call.selected_hint_index),
            _ => None,
        }
    }

    pub fn select_service_call_hint_index(&mut self, index: usize) -> bool {
        let DetailPane::Loaded(schema) = &self.service_call.schema else {
            return false;
        };
        if index >= schema.request_model.fields.len() {
            return false;
        }
        self.service_call.selected_hint_index = index;
        true
    }

    pub fn set_status(&mut self, status: String) {
        self.report_info(status);
    }

    pub fn report_info(&mut self, message: String) {
        self.report_info_from(EventSource::System, message);
    }

    pub fn report_info_from(&mut self, source: EventSource, message: String) {
        self.push_event(source, EventSeverity::Info, message);
    }

    pub fn report_warn(&mut self, message: String) {
        self.report_warn_from(EventSource::System, message);
    }

    pub fn report_warn_from(&mut self, source: EventSource, message: String) {
        self.push_event(source, EventSeverity::Warn, message);
    }

    pub fn report_error(&mut self, message: String) {
        self.report_error_from(EventSource::System, message);
    }

    pub fn report_error_from(&mut self, source: EventSource, message: String) {
        self.push_event(source, EventSeverity::Error, message);
    }

    pub fn should_quit(&self) -> bool {
        self.should_quit
    }

    pub fn quit(&mut self) {
        self.should_quit = true;
    }

    pub fn set_topic_detail(&mut self, detail: DetailPane<TopicDetailData>) {
        self.topic_detail = detail;
    }

    pub fn set_topic_schema_describe(&mut self, detail: DetailPane<SchemaDescribeData>) {
        self.topic_schema_describe = detail;
    }

    pub fn set_service_schema_describe(&mut self, detail: DetailPane<SchemaDescribeData>) {
        self.service_schema_describe = detail;
    }

    pub fn clear_topic_schema_describe(&mut self) {
        self.topic_schema_describe = DetailPane::Empty;
    }

    pub fn clear_service_schema_describe(&mut self) {
        self.service_schema_describe = DetailPane::Empty;
    }

    fn clear_schema_describe_panes(&mut self) {
        self.topic_schema_describe = DetailPane::Empty;
        self.service_schema_describe = DetailPane::Empty;
    }

    pub fn begin_topic_preview(&mut self, topic_name: String) {
        if self.topic_preview.target_topic() == Some(topic_name.as_str()) {
            return;
        }
        self.topic_preview = TopicPreviewState {
            target_topic: Some(topic_name),
            ..TopicPreviewState::default()
        };
    }

    pub fn clear_topic_preview(&mut self) {
        self.topic_preview = TopicPreviewState::default();
    }

    pub fn set_topic_preview_error(&mut self, error: String) {
        self.topic_preview.last_error = Some(error);
    }

    pub fn apply_topic_preview_frame(&mut self, frame: TopicPreviewFrame) {
        let now = Instant::now();
        if self.topic_preview.target_topic() != Some(frame.topic.as_str()) {
            self.begin_topic_preview(frame.topic.as_str().to_string());
        }
        if let Some(last) = self.topic_preview.last_message_at {
            let interval_s = now.duration_since(last).as_secs_f64();
            if interval_s > 0.0 {
                if self.topic_preview.intervals.len() >= 32 {
                    self.topic_preview.intervals.pop_front();
                }
                self.topic_preview.intervals.push_back(interval_s);
            }
        }
        self.topic_preview.last_message_at = Some(now);
        // payload_hex is the full hex of the payload (introspect `to_hex(payload)`),
        // so byte count = hex_len / 2. Guard the invariant in debug builds.
        debug_assert_eq!(
            frame.payload_hex.len() % 2,
            0,
            "payload_hex must be full byte-pairs"
        );
        let payload_bytes = (frame.payload_hex.len() / 2) as u64;
        if self.topic_preview.sizes.len() >= 32 {
            self.topic_preview.sizes.pop_front();
        }
        self.topic_preview.sizes.push_back(payload_bytes);
        self.topic_preview.last_error = None;
        self.topic_preview.message_count = self.topic_preview.message_count.saturating_add(1);
        let was_following_latest = self.topic_preview.is_viewing_latest()
            || self.topic_preview.selected_history_index.is_none();
        if self.topic_preview.history.len() >= TOPIC_PREVIEW_HISTORY_CAPACITY {
            self.topic_preview.history.pop_front();
            if let Some(index) = self.topic_preview.selected_history_index.as_mut() {
                *index = index.saturating_sub(1);
            }
        }
        self.topic_preview.history.push_back(TopicPreviewEntry {
            frame: frame.clone(),
            observed_at: now,
        });
        if was_following_latest {
            self.topic_preview.selected_history_index =
                Some(self.topic_preview.history.len().saturating_sub(1));
        }
        self.topic_preview.latest_frame = Some(frame);
    }

    pub fn scroll_topic_preview_up(&mut self, lines: u16) {
        self.topic_preview.scroll = self.topic_preview.scroll.saturating_sub(lines);
    }

    pub fn scroll_topic_preview_down(&mut self, lines: u16) {
        self.topic_preview.scroll = self.topic_preview.scroll.saturating_add(lines);
    }

    pub fn detail_scroll(&self) -> u16 {
        self.detail_scroll
    }

    pub fn scroll_detail_up(&mut self, lines: u16) {
        self.detail_scroll = self.detail_scroll.saturating_sub(lines);
    }

    pub fn scroll_detail_down(&mut self, lines: u16) {
        self.detail_scroll = self.detail_scroll.saturating_add(lines);
    }

    pub fn reset_detail_scroll(&mut self) {
        self.detail_scroll = 0;
    }

    pub fn reset_topic_preview_scroll(&mut self) {
        self.topic_preview.scroll = 0;
    }

    pub fn toggle_topic_preview_view_mode(&mut self) {
        self.topic_preview.view_mode = self.topic_preview.view_mode.toggle();
        self.reset_topic_preview_scroll();
    }

    pub fn service_call_response_scroll(&self) -> u16 {
        self.service_call.response_scroll()
    }

    pub fn scroll_service_call_response_up(&mut self, lines: u16) {
        self.service_call.response_scroll = self.service_call.response_scroll.saturating_sub(lines);
    }

    pub fn scroll_service_call_response_down(&mut self, lines: u16) {
        self.service_call.response_scroll = self.service_call.response_scroll.saturating_add(lines);
    }

    pub fn reset_service_call_response_scroll(&mut self) {
        self.service_call.response_scroll = 0;
    }

    pub fn select_older_topic_preview_frame(&mut self) {
        let Some(index) = self.topic_preview.selected_history_index else {
            return;
        };
        if index > 0 {
            self.topic_preview.selected_history_index = Some(index - 1);
            self.reset_topic_preview_scroll();
        }
    }

    pub fn select_newer_topic_preview_frame(&mut self) {
        let Some(index) = self.topic_preview.selected_history_index else {
            return;
        };
        if index + 1 < self.topic_preview.history.len() {
            self.topic_preview.selected_history_index = Some(index + 1);
            self.reset_topic_preview_scroll();
        }
    }

    pub fn set_service_detail(&mut self, detail: DetailPane<ServiceDetailData>) {
        if let DetailPane::Loaded(data) = &detail {
            if let Some(runtime) = data.runtime_summary.as_ref() {
                // Services expose only the most-recent client call latency + its
                // finished stamp in the aggregate summary; feed it into the ring
                // keyed by the service name (dedup on the finished stamp).
                self.latency_history.observe(
                    &format!("service:{}", data.name),
                    runtime.client_last_call_latency_ns,
                    runtime.client_last_call_finished_ns,
                );
            }
        }
        self.service_detail = detail;
    }

    pub fn set_action_detail(&mut self, detail: DetailPane<ActionDetailData>) {
        self.action_detail = detail;
    }

    pub fn set_node_detail(&mut self, detail: DetailPane<NodeDetailData>) {
        if let DetailPane::Loaded(data) = &detail {
            if let Some(breakdown) = data.runtime_breakdown.as_ref() {
                // Each client row carries a per-resource last_call_latency_ns +
                // last_call_finished_ns; accumulate them client-side over refresh
                // ticks so the single last-value becomes an observed distribution.
                for client in &breakdown.clients {
                    self.latency_history.observe(
                        &format!("client:{}", client.resource_name),
                        client.last_call_latency_ns,
                        client.last_call_finished_ns,
                    );
                }
            }
        }
        self.node_detail = detail;
    }

    /// Read-only access to the per-resource client-observed latency rings that
    /// back the Telemetry screen's sparklines.
    pub fn latency_history(&self) -> &LatencyHistory {
        &self.latency_history
    }

    /// Directly record a client-observed latency observation for `label`
    /// (dedup on `finished_ns`). Used by tests to seed the Telemetry screen; the
    /// live path feeds this from `set_node_detail`/`set_service_detail`.
    pub fn observe_latency_sample(&mut self, label: &str, latency_ns: u64, finished_ns: u64) {
        self.latency_history.observe(label, latency_ns, finished_ns);
    }

    pub fn set_context(&mut self, detail: DetailPane<ContextData>) {
        self.context = detail;
    }

    pub fn set_zenoh_internals(&mut self, detail: DetailPane<ZenohInternalsData>) {
        let selected_router_zid = self.zenoh_routers.selected_row().map(|row| row.zid.clone());
        match &detail {
            DetailPane::Loaded(data) => self.zenoh_routers.replace(data.routers.clone()),
            DetailPane::Empty | DetailPane::Error(_) => self.zenoh_routers.replace(Vec::new()),
        }
        if let Some(zid) = selected_router_zid {
            self.zenoh_routers
                .select_by_preserving_query(|row| row.zid == zid);
        }
        self.zenoh_internals = detail;
    }

    pub fn set_zenoh_connectivity(&mut self, detail: DetailPane<ZenohConnectivityData>) {
        self.zenoh_connectivity = detail;
    }

    pub fn apply_zenoh_connectivity_events(&mut self, events: &[ZenohConnectivityEventData]) {
        for event in events {
            if event.sequence <= self.zenoh_connectivity_event_cursor {
                continue;
            }

            let severity = match event.kind {
                ZenohConnectivityEventKindData::TransportOpened
                | ZenohConnectivityEventKindData::LinkAdded => EventSeverity::Info,
                ZenohConnectivityEventKindData::TransportClosed
                | ZenohConnectivityEventKindData::LinkRemoved => EventSeverity::Warn,
            };
            if self.zenoh_connectivity_events.len() >= ZENOH_CONNECTIVITY_EVENT_CAPACITY {
                self.zenoh_connectivity_events.pop_front();
            }
            self.zenoh_connectivity_events.push_back(event.clone());
            self.push_event(EventSource::Zenoh, severity, event.message.clone());
            self.zenoh_connectivity_event_cursor = event.sequence;
        }
    }

    pub fn set_service_call_schema(&mut self, detail: DetailPane<ServiceCallSchemaData>) {
        if let DetailPane::Loaded(data) = &detail {
            if self.service_call.editor.is_empty() {
                self.service_call.editor.set_text(data.starter_json.clone());
            }
            if self.service_call.selected_hint_index >= data.request_model.fields.len() {
                self.service_call.selected_hint_index = 0;
            }
        }
        self.service_call.schema = detail;
    }

    pub fn set_service_call_response(&mut self, detail: DetailPane<ServiceCallResponseData>) {
        self.service_call.response = detail;
        self.reset_service_call_response_scroll();
    }

    pub fn next_screen(&mut self) {
        self.exit_search();
        self.exit_service_call_edit();
        self.exit_action_call_edit();
        self.detail_scroll = 0;
        self.active_screen = self.cycled_screen(1);
        self.close_topology_overlay();
        self.close_recorder_overlay();
        self.close_node_comparison();
        self.clear_schema_describe_panes();
        if self.active_screen == Screen::ServiceCall {
            self.prepare_service_call_from_selected_service();
        }
        if self.active_screen == Screen::ActionCall {
            self.prepare_action_call_from_selected_action();
        }
    }

    pub fn previous_screen(&mut self) {
        self.exit_search();
        self.exit_service_call_edit();
        self.exit_action_call_edit();
        self.detail_scroll = 0;
        self.active_screen = self.cycled_screen(-1);
        self.close_topology_overlay();
        self.close_recorder_overlay();
        self.close_node_comparison();
        self.clear_schema_describe_panes();
        if self.active_screen == Screen::ServiceCall {
            self.prepare_service_call_from_selected_service();
        }
        if self.active_screen == Screen::ActionCall {
            self.prepare_action_call_from_selected_action();
        }
    }

    pub fn toggle_advanced_mode(&mut self) -> bool {
        self.advanced_mode = !self.advanced_mode;
        if !self.advanced_mode && matches!(self.active_screen, Screen::Zenoh | Screen::Telemetry) {
            self.active_screen = Screen::Overview;
        }
        self.advanced_mode
    }

    pub fn is_auto_refresh_enabled(&self) -> bool {
        self.auto_refresh_enabled
    }

    pub fn toggle_auto_refresh(&mut self) -> bool {
        self.auto_refresh_enabled = !self.auto_refresh_enabled;
        self.auto_refresh_enabled
    }

    pub fn toggle_help(&mut self) -> bool {
        if !self.help_visible {
            self.close_event_timeline();
            self.close_topology_overlay();
            self.close_recorder_overlay();
            self.close_node_comparison();
            self.close_bookmark_overlay();
            self.close_command_palette();
        }
        self.help_visible = !self.help_visible;
        self.help_visible
    }

    pub fn hide_help(&mut self) {
        self.help_visible = false;
    }

    pub fn enter_search(&mut self) {
        self.hide_help();
        self.close_event_timeline();
        self.close_topology_overlay();
        self.close_recorder_overlay();
        self.close_node_comparison();
        self.close_bookmark_overlay();
        self.close_command_palette();
        self.searching = true;
    }

    pub fn exit_search(&mut self) {
        self.searching = false;
    }

    pub fn is_searching(&self) -> bool {
        self.searching
    }

    pub fn push_query_char(&mut self, ch: char) {
        if let Some(query) = self.active_query_mut() {
            query.push(ch);
            self.sync_active_query();
        }
    }

    pub fn pop_query_char(&mut self) {
        if let Some(query) = self.active_query_mut() {
            query.pop();
            self.sync_active_query();
        }
    }

    pub fn select_next(&mut self) {
        self.detail_scroll = 0;
        match self.active_screen {
            Screen::Overview => {
                self.overview_selected_action =
                    (self.overview_selected_action + 1).min(OverviewAction::ALL.len() - 1);
            }
            Screen::Topics => {
                let previous = self.topics.selected_index();
                self.topics.select_next();
                if self.topics.selected_index() != previous {
                    self.clear_topic_schema_describe();
                }
            }
            Screen::Services => {
                let previous = self.services.selected_index();
                self.services.select_next();
                if self.services.selected_index() != previous {
                    self.clear_service_schema_describe();
                }
            }
            Screen::ServiceCall => self.service_call_select_next_hint(),
            Screen::Actions => self.actions.select_next(),
            Screen::ActionCall => self.action_call_select_next_hint(),
            Screen::Nodes => self.nodes.select_next(),
            // Context is a read-only scrolling panel with no list selection.
            Screen::Context => {}
            Screen::Zenoh => self.zenoh_routers.select_next(),
            // Telemetry is a read-only scrolling dashboard with no list selection.
            Screen::Telemetry => {}
        }
    }

    pub fn select_previous(&mut self) {
        self.detail_scroll = 0;
        match self.active_screen {
            Screen::Overview => {
                self.overview_selected_action = self.overview_selected_action.saturating_sub(1);
            }
            Screen::Topics => {
                let previous = self.topics.selected_index();
                self.topics.select_previous();
                if self.topics.selected_index() != previous {
                    self.clear_topic_schema_describe();
                }
            }
            Screen::Services => {
                let previous = self.services.selected_index();
                self.services.select_previous();
                if self.services.selected_index() != previous {
                    self.clear_service_schema_describe();
                }
            }
            Screen::ServiceCall => self.service_call_select_previous_hint(),
            Screen::Actions => self.actions.select_previous(),
            Screen::ActionCall => self.action_call_select_previous_hint(),
            Screen::Nodes => self.nodes.select_previous(),
            // Context is a read-only scrolling panel with no list selection.
            Screen::Context => {}
            Screen::Zenoh => self.zenoh_routers.select_previous(),
            // Telemetry is a read-only scrolling dashboard with no list selection.
            Screen::Telemetry => {}
        }
    }

    pub fn activate_overview_action(&mut self) -> Result<Screen, String> {
        match self.overview_selected_action() {
            OverviewAction::Topics => {
                self.active_screen = Screen::Topics;
                Ok(Screen::Topics)
            }
            OverviewAction::Services => {
                self.active_screen = Screen::Services;
                Ok(Screen::Services)
            }
            OverviewAction::ServiceCall => {
                if self.services.selected_row().is_none() {
                    return Err("no service is available for Service Call".to_string());
                }
                self.prepare_service_call_from_selected_service();
                self.active_screen = Screen::ServiceCall;
                Ok(Screen::ServiceCall)
            }
            OverviewAction::Nodes => {
                self.active_screen = Screen::Nodes;
                Ok(Screen::Nodes)
            }
        }
    }

    pub fn activate_overview_highlighted_topic(&mut self, index: usize) -> Result<Screen, String> {
        let Some(topic_name) = self
            .overview
            .highlighted_topics
            .get(index)
            .map(|resource| resource.name.clone())
        else {
            return Err(format!("hot topic {} is unavailable", index + 1));
        };
        if !self.topics.select_by(|row| row.name == topic_name) {
            return Err(format!(
                "topic '{}' is not available in the topic list",
                topic_name
            ));
        }
        self.active_screen = Screen::Topics;
        self.clear_schema_describe_panes();
        Ok(Screen::Topics)
    }

    /// First known provider (publisher) node label for the loaded Topic detail,
    /// formatted as `name (id)`. `None` when no topic detail is loaded or it has
    /// no provider node.
    fn topic_detail_provider_label(&self) -> Option<String> {
        let DetailPane::Loaded(detail) = &self.topic_detail else {
            return None;
        };
        detail.publisher_node_names.first().cloned()
    }

    /// Jump from the loaded Topic detail pane to the Nodes screen, selecting the
    /// topic's first known provider (publisher) node. Returns the selected node
    /// label on success. No-op-with-hint when no provider is known or the node is
    /// not present in the current Nodes list.
    pub fn jump_to_topic_provider_node(&mut self) -> Result<String, String> {
        let DetailPane::Loaded(detail) = &self.topic_detail else {
            return Err("open a topic detail first to jump to its provider node".to_string());
        };
        let Some(provider_label) = detail.publisher_node_names.first().cloned() else {
            return Err(format!(
                "topic '{}' has no known provider node to jump to",
                detail.name
            ));
        };
        // Provider labels are formatted as "name (id)"; match on the embedded id
        // first (stable), then fall back to the display name.
        let provider_id = extract_node_id_from_label(&provider_label);
        let selected = match provider_id {
            Some(id) => self.nodes.select_by(|row| row.node_id == id),
            None => false,
        } || self
            .nodes
            .select_by(|row| node_label(row) == provider_label || row.name == provider_label);
        if !selected {
            return Err(format!(
                "provider node '{}' is not available in the Nodes list",
                provider_label
            ));
        }
        self.active_screen = Screen::Nodes;
        self.close_topology_overlay();
        self.clear_schema_describe_panes();
        Ok(provider_label)
    }

    pub fn activate_overview_highlighted_service(
        &mut self,
        index: usize,
    ) -> Result<Screen, String> {
        let Some(service_name) = self
            .overview
            .highlighted_services
            .get(index)
            .map(|resource| resource.name.clone())
        else {
            return Err(format!("hot service {} is unavailable", index + 1));
        };
        if !self.services.select_by(|row| row.name == service_name) {
            return Err(format!(
                "service '{}' is not available in the service list",
                service_name
            ));
        }
        self.active_screen = Screen::Services;
        self.clear_schema_describe_panes();
        Ok(Screen::Services)
    }

    pub fn open_service_call_from_selected_service(&mut self) -> bool {
        let Some(selected_service) = self.services.selected_row().map(|row| row.name.clone())
        else {
            return false;
        };
        self.begin_service_call(selected_service);
        self.active_screen = Screen::ServiceCall;
        true
    }

    pub fn service_call_target(&self) -> Option<&str> {
        self.service_call.target_service()
    }

    pub fn service_call_request_json(&self) -> &str {
        self.service_call.editor.text()
    }

    pub fn service_call_cursor_line_col(&self) -> (usize, usize) {
        self.service_call.editor.cursor_line_col()
    }

    pub fn is_service_call_editing(&self) -> bool {
        self.service_call.is_editing()
    }

    pub fn enter_service_call_edit(&mut self) {
        if self.active_screen == Screen::ServiceCall {
            self.close_event_timeline();
            self.close_topology_overlay();
            self.close_recorder_overlay();
            self.close_node_comparison();
            self.close_bookmark_overlay();
            self.close_command_palette();
            self.service_call.editing = true;
        }
    }

    pub fn open_command_palette(&mut self) -> bool {
        if self.interaction_mode() != InteractionMode::Normal {
            return false;
        }
        self.command_palette.open();
        self.sync_command_palette_actions();
        true
    }

    pub fn close_command_palette(&mut self) {
        self.command_palette.close();
    }

    pub fn push_command_palette_char(&mut self, ch: char) {
        if !self.command_palette.visible {
            return;
        }
        let mut query = self.command_palette.query.clone();
        query.push(ch);
        self.command_palette.set_query(query);
        self.sync_command_palette_actions();
    }

    pub fn pop_command_palette_char(&mut self) {
        if !self.command_palette.visible {
            return;
        }
        let mut query = self.command_palette.query.clone();
        query.pop();
        self.command_palette.set_query(query);
        self.sync_command_palette_actions();
    }

    pub fn select_next_command_palette_action(&mut self) {
        self.command_palette.select_next();
    }

    pub fn select_previous_command_palette_action(&mut self) {
        self.command_palette.select_previous();
    }

    pub fn open_screen(&mut self, screen: Screen) -> Result<(), String> {
        match screen {
            Screen::Zenoh if !self.advanced_mode => {
                return Err("advanced mode is required for Zenoh diagnostics".to_string());
            }
            Screen::Telemetry if !self.advanced_mode => {
                return Err("advanced mode is required for the Telemetry dashboard".to_string());
            }
            Screen::ServiceCall => {
                if self.services.selected_row().is_none() {
                    return Err("no service is available for Service Call".to_string());
                }
                self.prepare_service_call_from_selected_service();
            }
            Screen::ActionCall => {
                if self.actions.selected_row().is_none() {
                    return Err("no action is available for Action Call".to_string());
                }
                self.prepare_action_call_from_selected_action();
            }
            _ => {}
        }
        if screen != Screen::ServiceCall {
            self.exit_service_call_edit();
        }
        if screen != Screen::ActionCall {
            self.exit_action_call_edit();
        }
        let previous_screen = self.active_screen;
        self.active_screen = screen;
        if screen != Screen::Topics && screen != Screen::Services && screen != Screen::Nodes {
            self.close_topology_overlay();
        }
        self.close_recorder_overlay();
        if screen != Screen::Nodes {
            self.close_node_comparison();
        }
        if previous_screen != screen {
            self.clear_schema_describe_panes();
        }
        Ok(())
    }

    pub fn is_current_selection_bookmarked(&self) -> Option<bool> {
        self.current_selection_bookmark()
            .ok()
            .map(|bookmark| self.is_bookmark_available_exact(&bookmark))
    }

    pub fn exit_service_call_edit(&mut self) {
        self.service_call.editing = false;
    }

    pub fn service_call_insert_char(&mut self, ch: char) {
        self.service_call.editor.insert_char(ch);
    }

    pub fn service_call_insert_newline(&mut self) {
        self.service_call.editor.insert_newline();
    }

    pub fn service_call_insert_tab(&mut self) {
        self.service_call.editor.insert_str("  ");
    }

    pub fn service_call_backspace(&mut self) {
        self.service_call.editor.backspace();
    }

    pub fn service_call_delete(&mut self) {
        self.service_call.editor.delete();
    }

    pub fn service_call_move_left(&mut self) {
        self.service_call.editor.move_left();
    }

    pub fn service_call_move_right(&mut self) {
        self.service_call.editor.move_right();
    }

    pub fn service_call_move_up(&mut self) {
        self.service_call.editor.move_up();
    }

    pub fn service_call_move_down(&mut self) {
        self.service_call.editor.move_down();
    }

    pub fn service_call_move_home(&mut self) {
        self.service_call.editor.move_home();
    }

    pub fn service_call_move_end(&mut self) {
        self.service_call.editor.move_end();
    }

    pub fn service_call_select_next_hint(&mut self) {
        let DetailPane::Loaded(schema) = &self.service_call.schema else {
            return;
        };
        if !schema.request_model.fields.is_empty() {
            self.service_call.selected_hint_index =
                (self.service_call.selected_hint_index + 1) % schema.request_model.fields.len();
        }
    }

    pub fn service_call_select_previous_hint(&mut self) {
        let DetailPane::Loaded(schema) = &self.service_call.schema else {
            return;
        };
        if !schema.request_model.fields.is_empty() {
            self.service_call.selected_hint_index = if self.service_call.selected_hint_index == 0 {
                schema.request_model.fields.len() - 1
            } else {
                self.service_call.selected_hint_index - 1
            };
        }
    }

    pub fn service_call_apply_selected_hint_template(&mut self) -> Result<String, String> {
        let DetailPane::Loaded(schema) = &self.service_call.schema else {
            return Err("schema hints are not loaded".to_string());
        };
        let Some(field) = schema
            .request_model
            .fields
            .get(self.service_call.selected_hint_index)
        else {
            return Err("no schema field is selected".to_string());
        };
        let updated = merge_field_template_into_request_json(
            self.service_call.editor.text(),
            &schema.request_model,
            &field.path,
        )
        .map_err(|err| err.to_string())?;
        self.service_call.editor.set_text(updated);
        Ok(field.path.clone())
    }

    pub fn service_call_reset_request_to_starter(&mut self) -> Result<(), String> {
        let DetailPane::Loaded(schema) = &self.service_call.schema else {
            return Err("schema is not loaded".to_string());
        };
        self.service_call
            .editor
            .set_text(schema.starter_json.clone());
        Ok(())
    }

    pub fn active_selection_key(&self) -> Option<String> {
        match self.active_screen {
            Screen::Overview => Some(format!(
                "overview:{}",
                self.overview_selected_action().key()
            )),
            Screen::Topics => self
                .topics
                .selected_row()
                .map(|row| format!("topic:{}", row.name)),
            Screen::Services => self
                .services
                .selected_row()
                .map(|row| format!("service:{}", row.name)),
            Screen::ServiceCall => self
                .service_call
                .target_service()
                .map(|service| format!("service-call:{}", service)),
            Screen::Actions => self
                .actions
                .selected_row()
                .map(|row| format!("action:{}", row.name)),
            Screen::ActionCall => self
                .action_call
                .target_action()
                .map(|action| format!("action-call:{}", action)),
            Screen::Nodes => self
                .nodes
                .selected_row()
                .map(|row| format!("node:{}", row.node_id)),
            // The Context screen is a single scrolling panel (no per-row
            // selection); a constant key keeps refresh tied to screen switches
            // and explicit `r` refreshes.
            Screen::Context => Some("context".to_string()),
            Screen::Zenoh => self
                .zenoh_routers
                .selected_row()
                .map(|row| format!("zenoh:{}", row.zid)),
            // The Telemetry screen is a single scrolling dashboard (no per-row
            // selection); a constant key keeps refresh tied to screen switches.
            Screen::Telemetry => Some("telemetry".to_string()),
        }
    }

    fn active_query_mut(&mut self) -> Option<&mut String> {
        match self.active_screen {
            Screen::Overview => None,
            Screen::Topics => Some(&mut self.topics.query),
            Screen::Services => Some(&mut self.services.query),
            Screen::ServiceCall => None,
            Screen::Actions => Some(&mut self.actions.query),
            Screen::ActionCall => None,
            Screen::Nodes => Some(&mut self.nodes.query),
            // The Context screen has no searchable list.
            Screen::Context => None,
            Screen::Zenoh => Some(&mut self.zenoh_routers.query),
            // The Telemetry dashboard has no searchable list.
            Screen::Telemetry => None,
        }
    }

    fn sync_active_query(&mut self) {
        match self.active_screen {
            Screen::Overview => {}
            Screen::Topics => self.topics.set_query(self.topics.query.clone()),
            Screen::Services => self.services.set_query(self.services.query.clone()),
            Screen::ServiceCall => {}
            Screen::Actions => self.actions.set_query(self.actions.query.clone()),
            Screen::ActionCall => {}
            Screen::Nodes => self.nodes.set_query(self.nodes.query.clone()),
            Screen::Context => {}
            Screen::Zenoh => self
                .zenoh_routers
                .set_query(self.zenoh_routers.query.clone()),
            Screen::Telemetry => {}
        }
    }

    fn prepare_service_call_from_selected_service(&mut self) {
        if let Some(selected_service) = self.services.selected_row().map(|row| row.name.clone()) {
            if self.service_call.target_service() != Some(selected_service.as_str()) {
                self.begin_service_call(selected_service);
            }
        }
    }

    fn current_selection_bookmark(&self) -> Result<ResourceBookmark, String> {
        match self.active_screen {
            Screen::Topics => self
                .topics
                .selected_row()
                .map(|row| ResourceBookmark::topic(self.group(), row.name.clone()))
                .ok_or_else(|| "no topic is selected".to_string()),
            Screen::Services => self
                .services
                .selected_row()
                .map(|row| ResourceBookmark::service(self.group(), row.name.clone()))
                .ok_or_else(|| "no service is selected".to_string()),
            Screen::ServiceCall => self
                .service_call_target()
                .map(|service| ResourceBookmark::service(self.group(), service.to_string()))
                .ok_or_else(|| "no service is selected for Service Call".to_string()),
            Screen::Nodes => self
                .nodes
                .selected_row()
                .map(|row| {
                    ResourceBookmark::node(self.group(), row.node_id.clone(), row.name.clone())
                })
                .ok_or_else(|| "no node is selected".to_string()),
            Screen::Overview
            | Screen::Actions
            | Screen::ActionCall
            | Screen::Context
            | Screen::Zenoh
            | Screen::Telemetry => Err(
                "bookmarking is available for topics, services, service call, and nodes"
                    .to_string(),
            ),
        }
    }

    fn open_bookmark(&mut self, bookmark: &ResourceBookmark) -> Result<Screen, String> {
        if !bookmark.belongs_to_group(&self.current_group) {
            return Err(format!(
                "bookmark '{}' belongs to group '{}' instead of '{}'",
                bookmark.label, bookmark.group, self.current_group
            ));
        }
        match bookmark.kind {
            BookmarkKind::Topic => {
                if !self.topics.select_by(|row| row.name == bookmark.key) {
                    return Err(format!(
                        "bookmarked topic '{}' is not available in the current snapshot",
                        bookmark.label
                    ));
                }
                self.active_screen = Screen::Topics;
                self.clear_schema_describe_panes();
                Ok(Screen::Topics)
            }
            BookmarkKind::Service => {
                if !self.services.select_by(|row| row.name == bookmark.key) {
                    return Err(format!(
                        "bookmarked service '{}' is not available in the current snapshot",
                        bookmark.label
                    ));
                }
                self.active_screen = Screen::Services;
                self.clear_schema_describe_panes();
                Ok(Screen::Services)
            }
            BookmarkKind::Node => {
                if !self.nodes.select_by(|row| row.node_id == bookmark.key) {
                    return Err(format!(
                        "bookmarked node '{}' is not available in the current snapshot",
                        bookmark.label
                    ));
                }
                self.active_screen = Screen::Nodes;
                self.clear_schema_describe_panes();
                Ok(Screen::Nodes)
            }
        }
    }

    fn is_bookmark_available_exact(&self, bookmark: &ResourceBookmark) -> bool {
        self.bookmarks
            .iter()
            .any(|candidate| bookmark_identity(candidate) == bookmark_identity(bookmark))
    }

    fn begin_service_call(&mut self, service_name: String) {
        self.service_call.target_service = Some(service_name);
        self.service_call.schema = DetailPane::Empty;
        self.service_call.response = DetailPane::Empty;
        self.service_call.editor.clear();
        self.service_call.editing = false;
        self.service_call.selected_hint_index = 0;
        self.service_call.response_scroll = 0;
    }

    // ----- Action Call (send-goal / cancel) -----------------------------
    //
    // The Action Call screen mirrors Service Call end-to-end: a JSON goal
    // editor seeded from the action's SendGoalRequest schema, F5 to send, and a
    // response pane carrying goal_id + goal_secret. `x` cancels the last-sent
    // goal using the retained secret.

    pub fn action_call(&self) -> &ActionCallState {
        &self.action_call
    }

    pub fn loaded_action_call_response(&self) -> Option<&ActionCallResponseData> {
        match self.action_call.response() {
            DetailPane::Loaded(response) => Some(response),
            DetailPane::Empty | DetailPane::Error(_) => None,
        }
    }

    pub fn action_call_target(&self) -> Option<&str> {
        self.action_call.target_action()
    }

    pub fn action_call_request_json(&self) -> &str {
        self.action_call.editor.text()
    }

    pub fn action_call_cursor_line_col(&self) -> (usize, usize) {
        self.action_call.editor.cursor_line_col()
    }

    pub fn is_action_call_editing(&self) -> bool {
        self.action_call.is_editing()
    }

    /// `(goal_id, goal_secret)` of the most recent accepted send, if any.
    pub fn action_call_last_goal(&self) -> Option<(&str, &str)> {
        self.action_call.last_goal()
    }

    pub fn action_call_selected_hint_index(&self) -> Option<usize> {
        match &self.action_call.schema {
            DetailPane::Loaded(schema) if !schema.request_model.fields.is_empty() => {
                Some(self.action_call.selected_hint_index)
            }
            _ => None,
        }
    }

    pub fn action_call_selected_hint(&self) -> Option<&aorta_introspect::schema::SchemaField> {
        match &self.action_call.schema {
            DetailPane::Loaded(schema) => schema
                .request_model
                .fields
                .get(self.action_call.selected_hint_index),
            _ => None,
        }
    }

    pub fn select_action_call_hint_index(&mut self, index: usize) -> bool {
        let DetailPane::Loaded(schema) = &self.action_call.schema else {
            return false;
        };
        if index >= schema.request_model.fields.len() {
            return false;
        }
        self.action_call.selected_hint_index = index;
        true
    }

    pub fn action_call_response_scroll(&self) -> u16 {
        self.action_call.response_scroll()
    }

    pub fn scroll_action_call_response_up(&mut self, lines: u16) {
        self.action_call.response_scroll = self.action_call.response_scroll.saturating_sub(lines);
    }

    pub fn scroll_action_call_response_down(&mut self, lines: u16) {
        self.action_call.response_scroll = self.action_call.response_scroll.saturating_add(lines);
    }

    pub fn reset_action_call_response_scroll(&mut self) {
        self.action_call.response_scroll = 0;
    }

    pub fn set_action_call_schema(&mut self, detail: DetailPane<ActionCallSchemaData>) {
        if let DetailPane::Loaded(data) = &detail {
            if self.action_call.editor.is_empty() {
                self.action_call.editor.set_text(data.starter_json.clone());
            }
            if self.action_call.selected_hint_index >= data.request_model.fields.len() {
                self.action_call.selected_hint_index = 0;
            }
        }
        self.action_call.schema = detail;
    }

    pub fn set_action_call_response(&mut self, detail: DetailPane<ActionCallResponseData>) {
        if let DetailPane::Loaded(response) = &detail {
            if response.accepted {
                self.action_call.last_goal =
                    Some((response.goal_id.clone(), response.goal_secret.clone()));
            }
        }
        self.action_call.response = detail;
        self.reset_action_call_response_scroll();
    }

    pub fn open_action_call_from_selected_action(&mut self) -> bool {
        let Some(selected_action) = self.actions.selected_row().map(|row| row.name.clone()) else {
            return false;
        };
        self.begin_action_call(selected_action);
        self.active_screen = Screen::ActionCall;
        true
    }

    pub fn enter_action_call_edit(&mut self) {
        if self.active_screen == Screen::ActionCall {
            self.close_event_timeline();
            self.close_topology_overlay();
            self.close_recorder_overlay();
            self.close_node_comparison();
            self.close_bookmark_overlay();
            self.close_command_palette();
            self.action_call.editing = true;
        }
    }

    pub fn exit_action_call_edit(&mut self) {
        self.action_call.editing = false;
    }

    pub fn action_call_insert_char(&mut self, ch: char) {
        self.action_call.editor.insert_char(ch);
    }

    pub fn action_call_insert_newline(&mut self) {
        self.action_call.editor.insert_newline();
    }

    pub fn action_call_insert_tab(&mut self) {
        self.action_call.editor.insert_str("  ");
    }

    pub fn action_call_backspace(&mut self) {
        self.action_call.editor.backspace();
    }

    pub fn action_call_delete(&mut self) {
        self.action_call.editor.delete();
    }

    pub fn action_call_move_left(&mut self) {
        self.action_call.editor.move_left();
    }

    pub fn action_call_move_right(&mut self) {
        self.action_call.editor.move_right();
    }

    pub fn action_call_move_up(&mut self) {
        self.action_call.editor.move_up();
    }

    pub fn action_call_move_down(&mut self) {
        self.action_call.editor.move_down();
    }

    pub fn action_call_move_home(&mut self) {
        self.action_call.editor.move_home();
    }

    pub fn action_call_move_end(&mut self) {
        self.action_call.editor.move_end();
    }

    pub fn action_call_select_next_hint(&mut self) {
        let DetailPane::Loaded(schema) = &self.action_call.schema else {
            return;
        };
        if !schema.request_model.fields.is_empty() {
            self.action_call.selected_hint_index =
                (self.action_call.selected_hint_index + 1) % schema.request_model.fields.len();
        }
    }

    pub fn action_call_select_previous_hint(&mut self) {
        let DetailPane::Loaded(schema) = &self.action_call.schema else {
            return;
        };
        if !schema.request_model.fields.is_empty() {
            self.action_call.selected_hint_index = if self.action_call.selected_hint_index == 0 {
                schema.request_model.fields.len() - 1
            } else {
                self.action_call.selected_hint_index - 1
            };
        }
    }

    pub fn action_call_apply_selected_hint_template(&mut self) -> Result<String, String> {
        let DetailPane::Loaded(schema) = &self.action_call.schema else {
            return Err("schema hints are not loaded".to_string());
        };
        let Some(field) = schema
            .request_model
            .fields
            .get(self.action_call.selected_hint_index)
        else {
            return Err("no schema field is selected".to_string());
        };
        // The action editor holds only the goal payload; merge the hint relative
        // to the goal subtree (not the full SendGoalRequest) so it isn't wrapped
        // twice under `goal`.
        let updated = merge_goal_field_template_into_goal_json(
            self.action_call.editor.text(),
            &schema.request_model,
            &field.path,
        )
        .map_err(|err| err.to_string())?;
        self.action_call.editor.set_text(updated);
        Ok(field.path.clone())
    }

    pub fn action_call_reset_request_to_starter(&mut self) -> Result<(), String> {
        let DetailPane::Loaded(schema) = &self.action_call.schema else {
            return Err("schema is not loaded".to_string());
        };
        self.action_call
            .editor
            .set_text(schema.starter_json.clone());
        Ok(())
    }

    fn prepare_action_call_from_selected_action(&mut self) {
        if let Some(selected_action) = self.actions.selected_row().map(|row| row.name.clone()) {
            if self.action_call.target_action() != Some(selected_action.as_str()) {
                self.begin_action_call(selected_action);
            }
        }
    }

    fn begin_action_call(&mut self, action_name: String) {
        self.action_call.target_action = Some(action_name);
        self.action_call.schema = DetailPane::Empty;
        self.action_call.response = DetailPane::Empty;
        self.action_call.editor.clear();
        self.action_call.editing = false;
        self.action_call.selected_hint_index = 0;
        self.action_call.response_scroll = 0;
        self.action_call.last_goal = None;
    }

    fn push_event(&mut self, source: EventSource, severity: EventSeverity, message: String) {
        self.status = message.clone();
        if self.event_log.len() >= EVENT_LOG_CAPACITY {
            self.event_log.pop_front();
        }
        self.event_log.push_back(EventLogEntry {
            sequence: self.next_event_sequence,
            observed_at: Instant::now(),
            source,
            severity,
            message,
        });
        self.next_event_sequence = self.next_event_sequence.saturating_add(1);
        self.event_timeline
            .normalize(self.filtered_event_entries().len());
    }

    fn is_command_action_available(&self, action: CommandAction) -> bool {
        match self.interaction_mode() {
            InteractionMode::Help
            | InteractionMode::Timeline
            | InteractionMode::LogStream
            | InteractionMode::Topology
            | InteractionMode::Recorder
            | InteractionMode::NodeCompare
            | InteractionMode::Bookmarks
            | InteractionMode::Search
            | InteractionMode::Edit => {
                return false;
            }
            InteractionMode::Normal | InteractionMode::Palette => {}
        }

        match action {
            CommandAction::OpenOverview => self.active_screen != Screen::Overview,
            CommandAction::OpenTopics => self.active_screen != Screen::Topics,
            CommandAction::OpenServices => self.active_screen != Screen::Services,
            CommandAction::OpenServiceCall => {
                self.active_screen != Screen::ServiceCall && self.services.selected_row().is_some()
            }
            CommandAction::OpenActions => self.active_screen != Screen::Actions,
            CommandAction::OpenActionCall => {
                self.active_screen != Screen::ActionCall && self.actions.selected_row().is_some()
            }
            CommandAction::OpenNodes => self.active_screen != Screen::Nodes,
            CommandAction::OpenContext => self.active_screen != Screen::Context,
            CommandAction::OpenZenoh => self.advanced_mode && self.active_screen != Screen::Zenoh,
            CommandAction::OpenTelemetry => {
                self.advanced_mode && self.active_screen != Screen::Telemetry
            }
            CommandAction::OpenDiagnosticsTimeline => true,
            CommandAction::OpenTopologyExplorer => matches!(
                self.active_screen,
                Screen::Topics | Screen::Services | Screen::Nodes
            ),
            CommandAction::OpenRecorderLaunchpad => match self.active_screen {
                Screen::Overview => true,
                Screen::Topics => self.topics.selected_row().is_some(),
                Screen::Services => self.services.selected_row().is_some(),
                Screen::ServiceCall => self.service_call_target().is_some(),
                Screen::Actions => false,
                Screen::ActionCall => false,
                Screen::Nodes => self.nodes.selected_row().is_some(),
                Screen::Context => false,
                Screen::Zenoh => false,
                Screen::Telemetry => false,
            },
            CommandAction::MacroOpenTopicsSearch => true,
            CommandAction::MacroOpenServicesSearch => true,
            CommandAction::MacroOpenNodesSearch => true,
            CommandAction::MacroOpenServiceCallEditor => self.services.selected_row().is_some(),
            CommandAction::MacroOpenActionCallEditor => self.actions.selected_row().is_some(),
            CommandAction::OpenNodeComparison => {
                self.active_screen == Screen::Nodes
                    && self.selected_node_comparison_pair().is_some()
            }
            CommandAction::OpenBookmarks => true,
            CommandAction::RefreshSnapshot => true,
            CommandAction::ToggleAutoRefresh => true,
            CommandAction::JumpToTopicProviderNode => {
                self.active_screen == Screen::Topics && self.topic_detail_provider_label().is_some()
            }
            CommandAction::ToggleAdvancedMode => true,
            CommandAction::ToggleCurrentBookmark => {
                matches!(
                    self.active_screen,
                    Screen::Topics | Screen::Services | Screen::ServiceCall | Screen::Nodes
                ) && self.current_selection_bookmark().is_ok()
            }
            CommandAction::ToggleNodeCompareAnchor => {
                self.active_screen == Screen::Nodes && self.nodes.selected_row().is_some()
            }
            CommandAction::ToggleTopicRuntimeSection => self.active_screen == Screen::Topics,
            CommandAction::ToggleServiceRuntimeSection => self.active_screen == Screen::Services,
            CommandAction::ToggleNodeRuntimeSection => self.active_screen == Screen::Nodes,
            CommandAction::ExportTopicPreviewPayload => {
                self.active_screen == Screen::Topics
                    && self.selected_topic_preview_frame().is_some()
            }
            CommandAction::CopyTopicPreviewContent => {
                self.active_screen == Screen::Topics
                    && self.selected_topic_preview_frame().is_some()
            }
            CommandAction::ExportServiceResponsePayload => {
                self.active_screen == Screen::ServiceCall
                    && self.loaded_service_call_response().is_some()
            }
            CommandAction::OpenHelp => !self.help_visible,
        }
    }

    fn sync_command_palette_actions(&mut self) {
        if !self.command_palette.visible {
            return;
        }
        let query = self.command_palette.query.clone();
        self.command_palette
            .replace_actions(self.available_command_actions());
        self.command_palette.set_query(query);
    }

    fn cycled_screen(&self, direction: isize) -> Screen {
        let screens = self.visible_screens();
        let index = screens
            .iter()
            .position(|screen| *screen == self.active_screen)
            .unwrap_or(0);
        let len = screens.len() as isize;
        let next = (index as isize + direction).rem_euclid(len) as usize;
        screens[next]
    }
}

fn bookmark_identity(bookmark: &ResourceBookmark) -> (&str, &'static str, &str) {
    (&bookmark.group, bookmark.kind.as_str(), &bookmark.key)
}

/// Render a node row as the same `name (id)` label the topic detail uses for its
/// provider/subscriber node lists, so cross-entity navigation can match them.
fn node_label(row: &NodeRow) -> String {
    format!("{} ({})", row.name, row.node_id)
}

/// Extract the node id embedded in a `name (id)` provider label, if present.
fn extract_node_id_from_label(label: &str) -> Option<String> {
    let start = label.rfind('(')?;
    let end = label.rfind(')')?;
    if end <= start + 1 {
        return None;
    }
    Some(label[start + 1..end].to_string())
}

fn normalize_bookmarks(bookmarks: Vec<ResourceBookmark>) -> Vec<ResourceBookmark> {
    let mut normalized = Vec::new();
    for bookmark in bookmarks {
        if normalized
            .iter()
            .any(|candidate| bookmark_identity(candidate) == bookmark_identity(&bookmark))
        {
            continue;
        }
        normalized.push(bookmark);
    }
    normalized
}

fn build_overview_recorder_launch_plan(group: &str) -> RecorderLaunchPlan {
    let subject = sanitize_recorder_subject(group);
    let output_path = format!("recordings/{}.mcap", subject);
    build_recorder_launch_plan(
        RecorderLaunchScope::Overview,
        format!("Group {}", group),
        group,
        "broad group capture for general investigation",
        output_path,
        Vec::new(),
        vec![
            "This first cut captures the whole group.".to_string(),
            "Refine to a topic or service screen first when you need narrower capture.".to_string(),
        ],
        format!("group-{}", subject),
    )
}

fn build_topic_recorder_launch_plan(group: &str, topic_name: &str) -> RecorderLaunchPlan {
    let subject = sanitize_recorder_subject(topic_name);
    let output_path = format!("recordings/{}.mcap", subject);
    build_recorder_launch_plan(
        RecorderLaunchScope::Topic,
        format!("Topic {}", topic_name),
        group,
        "targeted topic capture with an exact include filter",
        output_path,
        vec!["--include".to_string(), shell_quote(topic_name)],
        vec![
            "Recorder include rules operate on topic suffix keyexpr patterns.".to_string(),
            "The selected topic maps directly to an exact `--include` filter.".to_string(),
        ],
        format!("topic-{}", subject),
    )
}

fn build_service_recorder_launch_plan(
    group: &str,
    service_name: &str,
    scope: RecorderLaunchScope,
) -> RecorderLaunchPlan {
    let subject = sanitize_recorder_subject(service_name);
    let output_path = format!("recordings/{}-rpc.mcap", subject);
    let target_label = match scope {
        RecorderLaunchScope::Service => format!("Service {}", service_name),
        RecorderLaunchScope::ServiceCall => format!("Service Call {}", service_name),
        RecorderLaunchScope::Overview | RecorderLaunchScope::Topic | RecorderLaunchScope::Node => {
            format!("Service {}", service_name)
        }
    };
    build_recorder_launch_plan(
        scope,
        target_label,
        group,
        "group-scoped RPC record stream capture",
        output_path,
        vec!["--rpc-record".to_string()],
        vec![
            "RPC record capture is enabled with `--rpc-record`.".to_string(),
            "This first cut is group-scoped and does not yet narrow to one service.".to_string(),
        ],
        format!("service-{}-rpc", subject),
    )
}

fn build_node_recorder_launch_plan(
    group: &str,
    node_name: &str,
    node_id: &str,
) -> RecorderLaunchPlan {
    let subject = sanitize_recorder_subject(node_name);
    let output_path = format!("recordings/{}.mcap", subject);
    build_recorder_launch_plan(
        RecorderLaunchScope::Node,
        format!("Node {}", node_name),
        group,
        "broad capture for a node-focused investigation session",
        output_path,
        Vec::new(),
        vec![
            format!("Selected node id: {}", node_id),
            "Node-specific recorder filtering is not available in this first cut.".to_string(),
            "Use Topics or Service Call to narrow capture before recording when possible."
                .to_string(),
        ],
        format!("node-{}", subject),
    )
}

fn build_recorder_launch_plan(
    scope: RecorderLaunchScope,
    target_label: String,
    group: &str,
    suggested_usage: &str,
    output_path: String,
    extra_args: Vec<String>,
    notes: Vec<String>,
    export_subject: String,
) -> RecorderLaunchPlan {
    RecorderLaunchPlan {
        scope,
        target_label,
        group: group.to_string(),
        suggested_usage: suggested_usage.to_string(),
        output_path: output_path.clone(),
        primary_command: build_recorder_command("aorta", group, &output_path, &extra_args),
        alternate_command: build_recorder_command("aorta-record", group, &output_path, &extra_args),
        notes,
        export_subject,
    }
}

fn build_recorder_command(
    program: &str,
    group: &str,
    output_path: &str,
    extra_args: &[String],
) -> String {
    let mut parts = Vec::new();
    parts.push(program.to_string());
    if program == "aorta" {
        parts.push("record".to_string());
    }
    parts.push("--group".to_string());
    parts.push(shell_quote(group));
    parts.push("-o".to_string());
    parts.push(shell_quote(output_path));
    parts.extend(extra_args.iter().cloned());
    parts.join(" ")
}

fn sanitize_recorder_subject(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' => ch.to_ascii_lowercase(),
            _ => '-',
        })
        .collect();
    let trimmed = sanitized.trim_matches('-');
    if trimmed.is_empty() {
        "capture".to_string()
    } else {
        trimmed.to_string()
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r#"'"'"'"#))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aorta_introspect::client::TopicPreviewFrame;

    use super::{
        sample_action_call_schema, sample_action_row, sample_dashboard, sample_service_call_schema,
        sample_topic_preview_frame, sample_zenoh_internals, App, CommandAction, DetailPane,
        EventSeverity, EventSource, EventTimelineSeverityFilter, EventTimelineSourceFilter,
        InteractionMode, OverviewAction, RecorderLaunchScope, ResourceBookmark,
        RuntimeSectionPreferences, Screen, TopicPreviewDiffReference, TopicPreviewViewMode,
        EVENT_LOG_CAPACITY,
    };
    use crate::data::{
        ActionCallResponseData, SchemaDescribeData, SchemaDescribeKind, SchemaDescribeSection,
        ServiceCallResponseData, ZenohConnectivityEventData, ZenohConnectivityEventKindData,
    };

    fn sample_topic_schema_describe() -> SchemaDescribeData {
        SchemaDescribeData {
            key_suffix: "pub/demo/chatter".to_string(),
            kind: SchemaDescribeKind::Topic,
            schema_hash: "abcdef1234567890".to_string(),
            bfbs_bytes: 256,
            sections: vec![SchemaDescribeSection {
                label: "Schema".to_string(),
                root_type: Some("demo.Chatter".to_string()),
                text: "table demo.Chatter { text:string; }".to_string(),
            }],
        }
    }

    #[test]
    fn set_topic_schema_describe_marks_pane_visible() {
        let mut app = App::default();
        assert!(!app.is_topic_schema_describe_visible());
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));
        assert!(app.is_topic_schema_describe_visible());
        app.clear_topic_schema_describe();
        assert!(!app.is_topic_schema_describe_visible());
    }

    #[test]
    fn schema_describe_clears_on_topic_selection_change() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));
        assert!(app.is_topic_schema_describe_visible());

        app.select_next();
        assert!(
            !app.is_topic_schema_describe_visible(),
            "schema describe pane should reset when topic selection changes"
        );
    }

    #[test]
    fn schema_describe_clears_on_screen_switch() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));
        app.next_screen();
        assert!(!app.is_topic_schema_describe_visible());
    }

    #[test]
    fn schema_describe_clears_when_open_screen_changes_screen() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));

        app.open_screen(Screen::Services).expect("open services");
        assert!(
            !app.is_topic_schema_describe_visible(),
            "open_screen should reset schema describe state when navigating away"
        );
    }

    #[test]
    fn schema_describe_clears_on_mouse_topic_selection_change() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));
        let target_index = 1;
        assert_ne!(app.topics().selected_index(), Some(target_index));

        assert!(app.select_topic_visible_index(target_index));
        assert!(
            !app.is_topic_schema_describe_visible(),
            "mouse-click selection change should reset the schema describe pane"
        );
    }

    #[test]
    fn schema_describe_preserved_when_mouse_clicks_already_selected_topic() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        let current = app.topics().selected_index().expect("selection");
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));

        assert!(app.select_topic_visible_index(current));
        assert!(
            app.is_topic_schema_describe_visible(),
            "re-clicking the same row must keep the schema describe pane open"
        );
    }

    #[test]
    fn schema_describe_clears_when_overview_hot_topic_jumps() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));
        app.open_screen(Screen::Overview).expect("open overview");
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));

        app.activate_overview_highlighted_topic(0)
            .expect("open hot topic");
        assert!(!app.is_topic_schema_describe_visible());
    }

    #[test]
    fn schema_describe_clears_when_opening_bookmarked_topic() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        let bookmark = app
            .toggle_current_bookmark()
            .expect("bookmark current topic");
        app.open_screen(Screen::Overview).expect("open overview");
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));

        app.open_bookmark(&bookmark.bookmark)
            .expect("open bookmarked topic");
        assert!(!app.is_topic_schema_describe_visible());
    }

    #[test]
    fn schema_describe_clears_when_select_next_changes_topic() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        let before = app.topics().selected_index();
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));

        app.select_next();
        assert_ne!(app.topics().selected_index(), before);
        assert!(
            !app.is_topic_schema_describe_visible(),
            "select_next that changes the topic must reset the schema describe pane"
        );
    }

    #[test]
    fn schema_describe_preserved_when_select_next_at_last_topic() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        while {
            let before = app.topics().selected_index();
            app.select_next();
            app.topics().selected_index() != before
        } {}
        let pinned = app.topics().selected_index();
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));

        app.select_next();
        assert_eq!(app.topics().selected_index(), pinned);
        assert!(
            app.is_topic_schema_describe_visible(),
            "select_next at the last row must not close the schema describe pane"
        );
    }

    #[test]
    fn schema_describe_preserved_when_select_previous_at_first_topic() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        assert_eq!(app.topics().selected_index(), Some(0));
        app.set_topic_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));

        app.select_previous();
        assert_eq!(app.topics().selected_index(), Some(0));
        assert!(
            app.is_topic_schema_describe_visible(),
            "select_previous at the first row must not close the schema describe pane"
        );
    }

    #[test]
    fn schema_describe_preserved_when_select_next_at_only_service() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Services).expect("open services");
        assert_eq!(app.services().selected_index(), Some(0));
        app.set_service_schema_describe(DetailPane::Loaded(sample_topic_schema_describe()));

        app.select_next();
        app.select_previous();
        assert_eq!(app.services().selected_index(), Some(0));
        assert!(
            app.is_service_schema_describe_visible(),
            "select_next/previous on a single-row services list must keep the pane open"
        );
    }

    #[test]
    fn next_screen_cycles_through_primary_screens() {
        let mut app = App::default();
        assert_eq!(app.active_screen(), Screen::Overview);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Topics);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Services);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::ServiceCall);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Actions);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::ActionCall);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Nodes);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Context);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Overview);
    }

    #[test]
    fn previous_screen_cycles_back_through_primary_screens() {
        let mut app = App::default();
        assert_eq!(app.active_screen(), Screen::Overview);
        app.previous_screen();
        assert_eq!(app.active_screen(), Screen::Context);
        app.previous_screen();
        assert_eq!(app.active_screen(), Screen::Nodes);
        app.previous_screen();
        assert_eq!(app.active_screen(), Screen::ActionCall);
        app.previous_screen();
        assert_eq!(app.active_screen(), Screen::Actions);
        app.previous_screen();
        assert_eq!(app.active_screen(), Screen::ServiceCall);
        app.previous_screen();
        assert_eq!(app.active_screen(), Screen::Services);
        app.previous_screen();
        assert_eq!(app.active_screen(), Screen::Topics);
        app.previous_screen();
        assert_eq!(app.active_screen(), Screen::Overview);
    }

    #[test]
    fn advanced_mode_adds_zenoh_screen_to_cycle() {
        let mut app = App::default();

        assert!(app.toggle_advanced_mode());
        assert!(app.is_advanced_mode());

        app.next_screen();
        app.next_screen();
        app.next_screen();
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Actions);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::ActionCall);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Nodes);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Context);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Zenoh);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Telemetry);
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Overview);
    }

    #[test]
    fn disabling_advanced_mode_while_on_zenoh_returns_to_overview() {
        let mut app = App::default();
        app.toggle_advanced_mode();
        app.next_screen();
        app.next_screen();
        app.next_screen();
        app.next_screen();
        app.next_screen();
        app.next_screen();
        app.next_screen();
        app.next_screen();
        assert_eq!(app.active_screen(), Screen::Zenoh);

        assert!(!app.toggle_advanced_mode());
        assert_eq!(app.active_screen(), Screen::Overview);
    }

    #[test]
    fn zenoh_search_matches_primary_locator_text() {
        let mut app = App::default();
        app.toggle_advanced_mode();
        app.set_zenoh_internals(DetailPane::Loaded(sample_zenoh_internals()));
        app.open_screen(Screen::Zenoh).expect("open zenoh");
        assert_eq!(app.active_screen(), Screen::Zenoh);

        app.enter_search();
        for ch in "127.0.0.1".chars() {
            app.push_query_char(ch);
        }

        let visible: Vec<_> = app
            .zenoh_routers()
            .visible_rows()
            .into_iter()
            .map(|row| row.zid.as_str())
            .collect();
        assert_eq!(visible, vec!["router-alpha"]);
    }

    #[test]
    fn overview_selection_cycles_between_actions() {
        let mut app = App::default();
        assert_eq!(app.overview_selected_action(), OverviewAction::Topics);

        app.select_next();
        assert_eq!(app.overview_selected_action(), OverviewAction::Services);
        app.select_next();
        assert_eq!(app.overview_selected_action(), OverviewAction::ServiceCall);
        app.select_previous();
        assert_eq!(app.overview_selected_action(), OverviewAction::Services);
    }

    #[test]
    fn topic_search_filters_visible_rows() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.enter_search();
        for ch in "cam".chars() {
            app.push_query_char(ch);
        }

        let visible: Vec<_> = app
            .topics()
            .visible_rows()
            .into_iter()
            .map(|row| row.name.as_str())
            .collect();
        assert_eq!(visible, vec!["camera/front"]);
    }

    #[test]
    fn active_selection_key_tracks_current_screen_selection() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        assert_eq!(
            app.active_selection_key().as_deref(),
            Some("overview:topics")
        );

        app.next_screen();
        assert_eq!(
            app.active_selection_key().as_deref(),
            Some("topic:imu/data")
        );

        app.next_screen();
        assert_eq!(
            app.active_selection_key().as_deref(),
            Some("service:nav/plan")
        );

        app.next_screen();
        assert_eq!(
            app.active_selection_key().as_deref(),
            Some("service-call:nav/plan")
        );

        // Actions: no action selected in the sample dashboard.
        app.next_screen();
        assert_eq!(app.active_selection_key().as_deref(), None);

        // Action Call: no action selected, so no target action.
        app.next_screen();
        assert_eq!(app.active_selection_key().as_deref(), None);

        app.next_screen();
        assert_eq!(
            app.active_selection_key().as_deref(),
            Some("node:a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6")
        );
    }

    #[test]
    fn overview_action_opens_service_call_when_service_exists() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.select_next();
        app.select_next();

        let opened = app.activate_overview_action().expect("open service call");
        assert_eq!(opened, Screen::ServiceCall);
        assert_eq!(app.active_screen(), Screen::ServiceCall);
        assert_eq!(app.service_call_target(), Some("nav/plan"));
    }

    #[test]
    fn overview_hot_topic_shortcut_selects_topic_and_enters_topics_screen() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        let opened = app
            .activate_overview_highlighted_topic(0)
            .expect("open hot topic");

        assert_eq!(opened, Screen::Topics);
        assert_eq!(app.active_screen(), Screen::Topics);
        assert_eq!(
            app.topics().selected_row().map(|row| row.name.as_str()),
            Some("camera/front")
        );
    }

    #[test]
    fn overview_hot_service_shortcut_selects_service_and_enters_services_screen() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        let opened = app
            .activate_overview_highlighted_service(0)
            .expect("open hot service");

        assert_eq!(opened, Screen::Services);
        assert_eq!(app.active_screen(), Screen::Services);
        assert_eq!(
            app.services().selected_row().map(|row| row.name.as_str()),
            Some("nav/plan")
        );
    }

    #[test]
    fn replacing_dashboard_clears_cached_detail_panes() {
        let mut app = App::default();
        app.set_topic_detail(DetailPane::Error("stale".to_string()));
        app.set_service_detail(DetailPane::Error("stale".to_string()));
        app.set_node_detail(DetailPane::Error("stale".to_string()));
        app.begin_topic_preview("imu/data".to_string());

        app.replace_dashboard(sample_dashboard());

        assert_eq!(app.topic_detail(), &DetailPane::Empty);
        assert!(app.topic_preview().target_topic().is_none());
        assert_eq!(app.service_detail(), &DetailPane::Empty);
        assert_eq!(app.node_detail(), &DetailPane::Empty);
        assert!(app.service_call().target_service().is_none());
    }

    #[test]
    fn report_error_updates_status_and_event_log() {
        let mut app = App::default();

        app.report_error("Service call failed.".to_string());

        assert_eq!(app.status(), "Service call failed.");
        let last = app.event_log().back().expect("event entry");
        assert_eq!(last.sequence, 1);
        assert_eq!(last.source, EventSource::System);
        assert_eq!(last.severity, EventSeverity::Error);
        assert_eq!(last.message, "Service call failed.");
    }

    #[test]
    fn command_action_metadata_ids_are_unique_and_non_empty() {
        let mut ids = std::collections::HashSet::new();

        for action in CommandAction::ALL {
            let metadata = action.metadata();
            assert!(!metadata.id.is_empty());
            assert!(!metadata.title.is_empty());
            assert!(!metadata.description.is_empty());
            assert!(ids.insert(metadata.id), "duplicate id: {}", metadata.id);
        }
    }

    #[test]
    fn available_command_actions_respect_screen_and_mode() {
        let mut app = App::default();
        let default_actions = app.available_command_actions();
        assert!(!default_actions.contains(&CommandAction::OpenOverview));
        assert!(default_actions.contains(&CommandAction::OpenTopics));
        assert!(default_actions.contains(&CommandAction::OpenDiagnosticsTimeline));
        assert!(default_actions.contains(&CommandAction::OpenRecorderLaunchpad));
        assert!(default_actions.contains(&CommandAction::MacroOpenTopicsSearch));
        assert!(default_actions.contains(&CommandAction::MacroOpenServicesSearch));
        assert!(default_actions.contains(&CommandAction::MacroOpenNodesSearch));
        assert!(!default_actions.contains(&CommandAction::MacroOpenServiceCallEditor));
        assert!(default_actions.contains(&CommandAction::OpenBookmarks));
        assert!(default_actions.contains(&CommandAction::RefreshSnapshot));
        assert!(default_actions.contains(&CommandAction::ToggleAdvancedMode));
        assert!(default_actions.contains(&CommandAction::OpenHelp));
        assert!(!default_actions.contains(&CommandAction::OpenTopologyExplorer));
        assert!(!default_actions.contains(&CommandAction::OpenServiceCall));
        assert!(!default_actions.contains(&CommandAction::ToggleCurrentBookmark));
        assert!(!default_actions.contains(&CommandAction::OpenZenoh));
        assert!(!default_actions.contains(&CommandAction::ExportTopicPreviewPayload));
        assert!(!default_actions.contains(&CommandAction::CopyTopicPreviewContent));

        app.replace_dashboard(sample_dashboard());
        let dashboard_actions = app.available_command_actions();
        assert!(dashboard_actions.contains(&CommandAction::OpenServiceCall));
        assert!(dashboard_actions.contains(&CommandAction::MacroOpenServiceCallEditor));

        app.toggle_advanced_mode();
        let advanced_actions = app.available_command_actions();
        assert!(advanced_actions.contains(&CommandAction::OpenZenoh));

        app.enter_search();
        assert!(app.available_command_actions().is_empty());
        app.exit_search();

        assert!(app.open_event_timeline());
        assert!(app.available_command_actions().is_empty());
        app.close_event_timeline();

        app.next_screen();
        let topic_actions = app.available_command_actions();
        assert!(topic_actions.contains(&CommandAction::OpenTopologyExplorer));
        assert!(topic_actions.contains(&CommandAction::OpenRecorderLaunchpad));
        assert!(topic_actions.contains(&CommandAction::ToggleCurrentBookmark));
        assert!(topic_actions.contains(&CommandAction::ToggleTopicRuntimeSection));
        assert!(!topic_actions.contains(&CommandAction::ExportTopicPreviewPayload));
        assert!(!topic_actions.contains(&CommandAction::CopyTopicPreviewContent));
        app.begin_topic_preview("imu/data".to_string());
        app.apply_topic_preview_frame(sample_topic_preview_frame("00", Some("{\"x\":1}")));
        assert!(app
            .available_command_actions()
            .contains(&CommandAction::ExportTopicPreviewPayload));
        assert!(app
            .available_command_actions()
            .contains(&CommandAction::CopyTopicPreviewContent));

        app.next_screen();
        let service_actions = app.available_command_actions();
        assert!(service_actions.contains(&CommandAction::OpenTopologyExplorer));
        assert!(service_actions.contains(&CommandAction::OpenRecorderLaunchpad));
        assert!(service_actions.contains(&CommandAction::ToggleCurrentBookmark));
        assert!(service_actions.contains(&CommandAction::ToggleServiceRuntimeSection));
        assert!(app.open_service_call_from_selected_service());
        let service_call_actions = app.available_command_actions();
        assert!(!service_call_actions.contains(&CommandAction::OpenServiceCall));
        assert!(service_call_actions.contains(&CommandAction::OpenRecorderLaunchpad));
        assert!(service_call_actions.contains(&CommandAction::ToggleCurrentBookmark));
        assert!(!service_call_actions.contains(&CommandAction::ExportServiceResponsePayload));

        app.enter_service_call_edit();
        assert!(app.available_command_actions().is_empty());
        app.exit_service_call_edit();

        app.next_screen();
        let action_screen_actions = app.available_command_actions();
        assert!(!action_screen_actions.contains(&CommandAction::OpenTopologyExplorer));
        assert!(!action_screen_actions.contains(&CommandAction::OpenRecorderLaunchpad));
        assert!(!action_screen_actions.contains(&CommandAction::ToggleCurrentBookmark));
        assert!(!action_screen_actions.contains(&CommandAction::ToggleNodeRuntimeSection));
        assert!(!action_screen_actions.contains(&CommandAction::ToggleNodeCompareAnchor));
        assert!(!action_screen_actions.contains(&CommandAction::OpenNodeComparison));

        // Action Call screen (no actions in the dashboard fixture).
        app.next_screen();
        let action_call_actions = app.available_command_actions();
        assert!(!action_call_actions.contains(&CommandAction::OpenTopologyExplorer));
        assert!(!action_call_actions.contains(&CommandAction::OpenRecorderLaunchpad));
        assert!(!action_call_actions.contains(&CommandAction::ToggleCurrentBookmark));

        app.next_screen();
        let node_actions = app.available_command_actions();
        assert!(node_actions.contains(&CommandAction::OpenTopologyExplorer));
        assert!(node_actions.contains(&CommandAction::OpenRecorderLaunchpad));
        assert!(node_actions.contains(&CommandAction::ToggleCurrentBookmark));
        assert!(node_actions.contains(&CommandAction::ToggleNodeRuntimeSection));
        assert!(node_actions.contains(&CommandAction::ToggleNodeCompareAnchor));
        assert!(!node_actions.contains(&CommandAction::OpenNodeComparison));

        app.toggle_node_compare_anchor().expect("set node anchor");
        app.select_next();
        let node_compare_actions = app.available_command_actions();
        assert!(node_compare_actions.contains(&CommandAction::OpenNodeComparison));
    }

    #[test]
    fn toggle_current_bookmark_adds_and_removes_topic_bookmark() {
        let mut app = App::default();
        app.set_group("robot_a");
        app.replace_dashboard(sample_dashboard());
        app.next_screen();

        let added = app.toggle_current_bookmark().expect("add bookmark");
        assert!(added.pinned);
        assert_eq!(
            added.bookmark,
            ResourceBookmark::topic("robot_a", "imu/data")
        );
        assert!(app.is_topic_bookmarked("imu/data"));

        let removed = app.toggle_current_bookmark().expect("remove bookmark");
        assert!(!removed.pinned);
        assert!(app.bookmarks().is_empty());
    }

    #[test]
    fn selected_bookmark_opens_target_screen_and_selection() {
        let mut app = App::default();
        app.set_group("robot_a");
        app.replace_dashboard(sample_dashboard());
        app.replace_bookmarks(vec![ResourceBookmark::node(
            "robot_a",
            "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
            "planner",
        )]);
        assert!(app.open_bookmark_overlay());

        let opened = app
            .open_selected_bookmark()
            .expect("open selected bookmark");
        assert_eq!(opened, Screen::Nodes);
        assert_eq!(app.active_screen(), Screen::Nodes);
        assert_eq!(
            app.nodes().selected_row().map(|row| row.node_id.as_str()),
            Some("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6")
        );
    }

    #[test]
    fn bookmark_overlay_is_blocked_in_search_and_edit_modes() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        app.enter_search();
        assert!(!app.open_bookmark_overlay());
        assert_eq!(app.interaction_mode(), InteractionMode::Search);

        app.exit_search();
        app.next_screen();
        app.next_screen();
        assert!(app.open_service_call_from_selected_service());
        app.enter_service_call_edit();
        assert_eq!(app.interaction_mode(), InteractionMode::Edit);
        assert!(!app.open_bookmark_overlay());
    }

    #[test]
    fn event_timeline_is_blocked_in_search_and_edit_modes() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        app.enter_search();
        assert!(!app.open_event_timeline());
        assert_eq!(app.interaction_mode(), InteractionMode::Search);

        app.exit_search();
        app.next_screen();
        app.next_screen();
        assert!(app.open_service_call_from_selected_service());
        app.enter_service_call_edit();
        assert_eq!(app.interaction_mode(), InteractionMode::Edit);
        assert!(!app.open_event_timeline());
    }

    #[test]
    fn topology_overlay_is_blocked_in_search_and_edit_modes() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();

        app.enter_search();
        assert_eq!(
            app.open_topology_overlay().expect_err("blocked in search"),
            "topology explorer can only be opened in normal mode"
        );
        assert_eq!(app.interaction_mode(), InteractionMode::Search);

        app.exit_search();
        app.next_screen();
        app.next_screen();
        assert!(app.open_service_call_from_selected_service());
        app.enter_service_call_edit();
        assert_eq!(app.interaction_mode(), InteractionMode::Edit);
        assert_eq!(
            app.open_topology_overlay().expect_err("blocked in edit"),
            "topology explorer can only be opened in normal mode"
        );
    }

    #[test]
    fn recorder_overlay_is_available_for_current_scope_and_blocked_in_search_and_edit_modes() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        let overview_plan = app.recorder_launch_plan().expect("overview recorder plan");
        assert_eq!(overview_plan.scope, RecorderLaunchScope::Overview);
        assert!(overview_plan.primary_command.contains("aorta record"));

        app.next_screen();
        let topic_plan = app.recorder_launch_plan().expect("topic recorder plan");
        assert_eq!(topic_plan.scope, RecorderLaunchScope::Topic);
        assert!(topic_plan.primary_command.contains("--include 'imu/data'"));

        app.enter_search();
        assert_eq!(
            app.open_recorder_overlay().expect_err("blocked in search"),
            "recorder launchpad can only be opened in normal mode"
        );
        assert_eq!(app.interaction_mode(), InteractionMode::Search);

        app.exit_search();
        app.next_screen();
        app.next_screen();
        assert!(app.open_service_call_from_selected_service());
        app.enter_service_call_edit();
        assert_eq!(app.interaction_mode(), InteractionMode::Edit);
        assert_eq!(
            app.open_recorder_overlay().expect_err("blocked in edit"),
            "recorder launchpad can only be opened in normal mode"
        );
    }

    #[test]
    fn recorder_launch_plan_matches_service_call_and_node_contexts() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        app.next_screen();
        app.next_screen();
        let service_plan = app.recorder_launch_plan().expect("service recorder plan");
        assert_eq!(service_plan.scope, RecorderLaunchScope::Service);
        assert!(service_plan.primary_command.contains("--rpc-record"));

        assert!(app.open_service_call_from_selected_service());
        app.exit_service_call_edit();
        let service_call_plan = app
            .recorder_launch_plan()
            .expect("service call recorder plan");
        assert_eq!(service_call_plan.scope, RecorderLaunchScope::ServiceCall);
        assert!(service_call_plan
            .target_label
            .contains("Service Call nav/plan"));

        app.open_screen(Screen::Nodes).expect("open nodes");
        let node_plan = app.recorder_launch_plan().expect("node recorder plan");
        assert_eq!(node_plan.scope, RecorderLaunchScope::Node);
        assert!(node_plan
            .notes
            .iter()
            .any(|note| note.contains("Node-specific recorder filtering")));
    }

    #[test]
    fn node_comparison_requires_anchor_and_second_selected_node() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Nodes).expect("open nodes");

        assert_eq!(
            app.open_node_comparison().expect_err("anchor required"),
            "set a comparison anchor first with `x` on a node"
        );

        let toggled = app
            .toggle_node_compare_anchor()
            .expect("set compare anchor");
        assert!(toggled.active);
        assert_eq!(toggled.node_name, "planner");
        assert!(app.has_node_compare_anchor());

        assert_eq!(
            app.open_node_comparison()
                .expect_err("must select another node"),
            "select a different node to compare against the anchor"
        );

        app.select_next();
        let pair = app
            .selected_node_comparison_pair()
            .expect("comparison pair available");
        assert_eq!(pair.anchor.name, "planner");
        assert_eq!(pair.current.name, "perception");

        app.open_node_comparison().expect("open node comparison");
        assert!(app.is_node_comparison_visible());
        assert_eq!(app.interaction_mode(), InteractionMode::NodeCompare);

        app.select_previous();
        let cleared = app
            .toggle_node_compare_anchor()
            .expect("clear compare anchor");
        assert!(!cleared.active);
        assert_eq!(cleared.node_name, "planner");
        assert!(!app.has_node_compare_anchor());
        assert!(!app.is_node_comparison_visible());
    }

    #[test]
    fn command_palette_opens_in_normal_mode_and_filters_actions() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        assert!(app.open_command_palette());
        assert_eq!(app.interaction_mode(), InteractionMode::Palette);
        assert!(app.is_command_palette_visible());
        assert!(app
            .command_palette_actions()
            .contains(&CommandAction::OpenTopics));

        for ch in "toggle advanced".chars() {
            app.push_command_palette_char(ch);
        }
        assert_eq!(app.command_palette_query(), "toggle advanced");
        assert_eq!(
            app.command_palette_actions(),
            vec![CommandAction::ToggleAdvancedMode]
        );
        assert_eq!(
            app.command_palette_selected_action(),
            Some(CommandAction::ToggleAdvancedMode)
        );

        app.pop_command_palette_char();
        assert_eq!(app.command_palette_query(), "toggle advance");

        app.close_command_palette();
        assert_eq!(app.interaction_mode(), InteractionMode::Normal);
        assert!(!app.is_command_palette_visible());
    }

    #[test]
    fn command_palette_is_blocked_in_search_and_edit_modes() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        app.enter_search();
        assert!(!app.open_command_palette());
        assert_eq!(app.interaction_mode(), InteractionMode::Search);

        app.exit_search();
        app.next_screen();
        app.next_screen();
        assert!(app.open_service_call_from_selected_service());
        app.enter_service_call_edit();
        assert_eq!(app.interaction_mode(), InteractionMode::Edit);
        assert!(!app.open_command_palette());
    }

    #[test]
    fn command_palette_selection_wraps_across_filtered_actions() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        assert!(app.open_command_palette());

        let first = app.command_palette_selected_action();
        app.select_next_command_palette_action();
        let second = app.command_palette_selected_action();
        assert_ne!(first, second);

        app.select_previous_command_palette_action();
        assert_eq!(app.command_palette_selected_action(), first);
    }

    #[test]
    fn runtime_section_preferences_toggle_independently() {
        let mut app = App::default();
        assert!(app.is_topic_runtime_section_expanded());
        assert!(app.is_service_runtime_section_expanded());
        assert!(app.is_node_runtime_section_expanded());

        assert!(!app.toggle_topic_runtime_section());
        assert!(app.is_service_runtime_section_expanded());
        assert!(app.is_node_runtime_section_expanded());

        assert!(!app.toggle_service_runtime_section());
        assert!(!app.toggle_node_runtime_section());
        assert_eq!(
            app.runtime_section_preferences(),
            RuntimeSectionPreferences {
                topics_expanded: false,
                services_expanded: false,
                nodes_expanded: false,
            }
        );
    }

    #[test]
    fn interaction_mode_prioritizes_help_over_search_and_edit() {
        let mut app = App::default();
        assert_eq!(app.interaction_mode(), InteractionMode::Normal);

        assert!(app.open_event_timeline());
        assert_eq!(app.interaction_mode(), InteractionMode::Timeline);

        app.close_event_timeline();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.open_topology_overlay().expect("open topology");
        assert_eq!(app.interaction_mode(), InteractionMode::Topology);

        app.close_topology_overlay();
        app.open_recorder_overlay().expect("open recorder");
        assert_eq!(app.interaction_mode(), InteractionMode::Recorder);

        app.enter_search();
        assert_eq!(app.interaction_mode(), InteractionMode::Search);

        app.exit_search();
        assert!(app.open_bookmark_overlay());
        assert_eq!(app.interaction_mode(), InteractionMode::Bookmarks);

        assert!(app.toggle_help());
        assert_eq!(app.interaction_mode(), InteractionMode::Help);

        app.hide_help();
        assert_eq!(app.interaction_mode(), InteractionMode::Normal);
        app.open_screen(Screen::Overview).expect("back to overview");

        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Nodes).expect("open nodes");
        app.toggle_node_compare_anchor().expect("anchor node");
        app.select_next();
        app.open_node_comparison().expect("open compare");
        assert_eq!(app.interaction_mode(), InteractionMode::NodeCompare);

        assert!(app.toggle_help());
        assert_eq!(app.interaction_mode(), InteractionMode::Help);

        app.hide_help();
        assert_eq!(app.interaction_mode(), InteractionMode::Normal);

        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.next_screen();
        assert!(app.open_service_call_from_selected_service());
        app.enter_service_call_edit();
        assert_eq!(app.interaction_mode(), InteractionMode::Edit);

        assert!(app.toggle_help());
        assert_eq!(app.interaction_mode(), InteractionMode::Help);
    }

    #[test]
    fn event_log_retention_keeps_bounded_recent_window() {
        let mut app = App::default();
        for index in 0..(EVENT_LOG_CAPACITY + 5) {
            app.report_info_from(EventSource::Data, format!("event {}", index));
        }

        assert_eq!(app.event_log().len(), EVENT_LOG_CAPACITY);
        let first = app.event_log().front().expect("first event");
        let last = app.event_log().back().expect("last event");
        assert_eq!(first.message, "event 5");
        assert_eq!(last.message, format!("event {}", EVENT_LOG_CAPACITY + 4));
    }

    #[test]
    fn zenoh_connectivity_events_append_once_and_preserve_latest_sequence() {
        let mut app = App::default();
        app.apply_zenoh_connectivity_events(&[
            ZenohConnectivityEventData {
                sequence: 1,
                timestamp_ns: 100,
                kind: ZenohConnectivityEventKindData::TransportOpened,
                message: "transport opened: router-alpha [router]".to_string(),
            },
            ZenohConnectivityEventData {
                sequence: 2,
                timestamp_ns: 200,
                kind: ZenohConnectivityEventKindData::LinkRemoved,
                message: "link removed: tcp/a -> tcp/b [router-alpha]".to_string(),
            },
        ]);

        app.apply_zenoh_connectivity_events(&[
            ZenohConnectivityEventData {
                sequence: 2,
                timestamp_ns: 200,
                kind: ZenohConnectivityEventKindData::LinkRemoved,
                message: "link removed: tcp/a -> tcp/b [router-alpha]".to_string(),
            },
            ZenohConnectivityEventData {
                sequence: 3,
                timestamp_ns: 300,
                kind: ZenohConnectivityEventKindData::LinkAdded,
                message: "link added: tcp/a -> tcp/b [router-alpha]".to_string(),
            },
        ]);

        let zenoh_entries: Vec<_> = app
            .event_log()
            .iter()
            .filter(|entry| entry.source == EventSource::Zenoh)
            .collect();
        assert_eq!(app.zenoh_connectivity_events().len(), 3);
        assert_eq!(zenoh_entries.len(), 3);
        assert_eq!(zenoh_entries[0].severity, EventSeverity::Info);
        assert_eq!(zenoh_entries[1].severity, EventSeverity::Warn);
        assert_eq!(zenoh_entries[2].severity, EventSeverity::Info);
        assert_eq!(
            zenoh_entries[2].message,
            "link added: tcp/a -> tcp/b [router-alpha]"
        );
        assert_eq!(
            app.zenoh_connectivity_events()
                .back()
                .map(|event| event.sequence),
            Some(3)
        );
    }

    #[test]
    fn event_timeline_filters_by_severity_and_source() {
        let mut app = App::default();
        app.report_info_from(EventSource::Navigation, "navigated".to_string());
        app.report_warn_from(EventSource::Data, "refresh degraded".to_string());
        app.report_error_from(EventSource::ServiceCall, "call failed".to_string());

        assert!(app.open_event_timeline());
        assert_eq!(
            app.event_timeline_severity_filter(),
            EventTimelineSeverityFilter::All
        );
        assert_eq!(app.filtered_event_entries().len(), 4);

        assert_eq!(
            app.cycle_event_timeline_severity_filter(),
            EventTimelineSeverityFilter::WarnAndError
        );
        let filtered = app.filtered_event_entries();
        assert_eq!(filtered.len(), 2);
        assert!(filtered
            .iter()
            .all(|entry| { matches!(entry.severity, EventSeverity::Warn | EventSeverity::Error) }));

        assert_eq!(
            app.cycle_event_timeline_source_filter(),
            EventTimelineSourceFilter::System
        );
        assert_eq!(app.filtered_event_entries().len(), 0);

        assert_eq!(
            app.cycle_event_timeline_source_filter(),
            EventTimelineSourceFilter::Navigation
        );
        assert_eq!(app.filtered_event_entries().len(), 0);

        assert_eq!(
            app.cycle_event_timeline_source_filter(),
            EventTimelineSourceFilter::Data
        );
        let filtered = app.filtered_event_entries();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].source, EventSource::Data);
        assert_eq!(filtered[0].message, "refresh degraded");

        app.clear_event_timeline_filters();
        assert_eq!(app.filtered_event_entries().len(), 4);
    }

    #[test]
    fn help_visibility_can_toggle_on_and_off() {
        let mut app = App::default();
        assert!(!app.is_help_visible());

        assert!(app.toggle_help());
        assert!(app.is_help_visible());

        app.hide_help();
        assert!(!app.is_help_visible());
    }

    #[test]
    fn app_group_can_be_overridden() {
        let mut app = App::default();
        assert_eq!(app.group(), "default");

        app.set_group("robot_a");
        assert_eq!(app.group(), "robot_a");
    }

    #[test]
    fn open_service_call_uses_selected_service_and_starts_in_normal_mode() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.next_screen();

        assert!(app.open_service_call_from_selected_service());
        assert_eq!(app.active_screen(), Screen::ServiceCall);
        assert_eq!(app.service_call_target(), Some("nav/plan"));
        assert!(!app.is_service_call_editing());

        app.enter_service_call_edit();
        assert!(app.is_service_call_editing());
    }

    #[test]
    fn setting_service_call_schema_populates_editor_once() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.next_screen();
        app.open_service_call_from_selected_service();

        app.set_service_call_schema(DetailPane::Loaded(sample_service_call_schema()));
        assert_eq!(
            app.service_call_request_json(),
            "{\n  \"message\": \"\",\n  \"options\": {\n    \"limit\": 10\n  },\n  \"mode\": \"AUTO\"\n}"
        );

        app.service_call_insert_char('x');
        app.set_service_call_schema(DetailPane::Loaded(sample_service_call_schema()));
        assert!(app.service_call_request_json().ends_with('x'));
    }

    #[test]
    fn setting_service_call_response_stores_latest_reply() {
        let mut app = App::default();
        app.set_service_call_response(DetailPane::Loaded(ServiceCallResponseData {
            response_json: Some("{\"ok\":true}".to_string()),
            response_utf8: None,
            response_hex: "7b7d".to_string(),
            decoded_with_schema: true,
            schema_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_string(),
            response_root_type: Some("demo.Response".to_string()),
        }));

        assert!(matches!(
            app.service_call().response(),
            DetailPane::Loaded(_)
        ));
    }

    /// Seed the actions list with a single action, select it, and open the
    /// Action Call screen — the test analog of opening Service Call from a
    /// selected service.
    fn app_on_action_call_screen() -> App {
        let mut app = App::default();
        app.replace_actions(vec![sample_action_row()]);
        app.open_screen(Screen::Actions).expect("open actions");
        assert!(app.open_action_call_from_selected_action());
        app
    }

    #[test]
    fn open_action_call_uses_selected_action_and_starts_in_normal_mode() {
        let app = app_on_action_call_screen();
        assert_eq!(app.active_screen(), Screen::ActionCall);
        assert_eq!(app.action_call_target(), Some("demo/execute_task"));
        assert!(!app.is_action_call_editing());
    }

    #[test]
    fn setting_action_call_schema_populates_goal_editor_once() {
        let mut app = app_on_action_call_screen();

        app.set_action_call_schema(DetailPane::Loaded(sample_action_call_schema()));
        // Seeded from the `goal` subtree (the goal payload), NOT the full
        // SendGoalRequest wrapper.
        assert_eq!(app.action_call_request_json(), "{\n  \"source\": \"\"\n}");

        // Re-applying the schema must not clobber in-flight edits.
        app.action_call_insert_char('x');
        app.set_action_call_schema(DetailPane::Loaded(sample_action_call_schema()));
        assert!(app.action_call_request_json().ends_with('x'));
    }

    #[test]
    fn action_call_editor_feeds_keys_and_builds_request_json() {
        let mut app = app_on_action_call_screen();
        app.set_action_call_schema(DetailPane::Loaded(sample_action_call_schema()));

        // Enter edit mode and replace the body with a concrete goal payload by
        // driving the same editor key methods the F5/edit-key path uses.
        app.enter_action_call_edit();
        assert_eq!(app.interaction_mode(), InteractionMode::Edit);
        assert!(app.is_action_call_editing());

        // Clear the starter body, then type a fresh JSON goal payload.
        for _ in 0..app.action_call_request_json().chars().count() {
            app.action_call_backspace();
        }
        assert_eq!(app.action_call_request_json(), "");
        for ch in "{\"source\":\"quick\"}".chars() {
            app.action_call_insert_char(ch);
        }
        assert_eq!(app.action_call_request_json(), "{\"source\":\"quick\"}");

        app.exit_action_call_edit();
        assert!(!app.is_action_call_editing());
    }

    #[test]
    fn action_call_hint_navigation_and_template_application_work() {
        let mut app = app_on_action_call_screen();
        app.set_action_call_schema(DetailPane::Loaded(sample_action_call_schema()));

        // Field list cycles across the three schema fields.
        assert_eq!(app.action_call_selected_hint_index(), Some(0));
        app.action_call_select_next_hint();
        app.action_call_select_next_hint();
        assert_eq!(
            app.action_call_selected_hint()
                .map(|field| field.path.as_str()),
            Some("goal.source")
        );

        // The editor holds only the goal payload, so applying the `goal.source`
        // template merges `source` at the payload root — it must NOT nest a second
        // `goal` wrapper (which submit-time wrapping would double into `goal.goal`).
        app.action_call.editor.set_text("{}".to_string());
        let path = app
            .action_call_apply_selected_hint_template()
            .expect("apply template");
        assert_eq!(path, "goal.source");
        let json = app.action_call_request_json();
        assert!(
            json.contains("\"source\""),
            "applies the source field: {json}"
        );
        assert!(
            !json.contains("\"goal\""),
            "must not nest a second goal wrapper: {json}"
        );
    }

    #[test]
    fn action_call_send_response_retains_goal_id_and_secret_for_cancel() {
        let mut app = app_on_action_call_screen();

        app.set_action_call_response(DetailPane::Loaded(ActionCallResponseData {
            accepted: true,
            goal_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            goal_secret: "secret-token".to_string(),
            status: "Accepted".to_string(),
            code: "Ok".to_string(),
            message: String::new(),
            server_epoch: "epoch-1".to_string(),
        }));

        assert!(app.loaded_action_call_response().is_some());
        // The cancel key relies on this retained pair.
        assert_eq!(
            app.action_call_last_goal(),
            Some(("550e8400-e29b-41d4-a716-446655440000", "secret-token"))
        );
        assert_eq!(app.action_call_response_scroll(), 0);
    }

    #[test]
    fn action_call_rejected_send_does_not_retain_goal_for_cancel() {
        let mut app = app_on_action_call_screen();

        app.set_action_call_response(DetailPane::Loaded(ActionCallResponseData {
            accepted: false,
            goal_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            goal_secret: String::new(),
            status: "Unknown".to_string(),
            code: "Busy".to_string(),
            message: "server busy".to_string(),
            server_epoch: "epoch-1".to_string(),
        }));

        // A rejected goal must not arm the cancel key.
        assert_eq!(app.action_call_last_goal(), None);
    }

    #[test]
    fn service_call_response_scroll_is_bounded_and_resets_on_new_response() {
        let mut app = App::default();
        app.scroll_service_call_response_down(12);
        app.scroll_service_call_response_up(5);
        assert_eq!(app.service_call().response_scroll(), 7);

        app.scroll_service_call_response_up(20);
        assert_eq!(app.service_call().response_scroll(), 0);

        app.scroll_service_call_response_down(9);
        app.set_service_call_response(DetailPane::Loaded(ServiceCallResponseData {
            response_json: Some("{\"ok\":true}".to_string()),
            response_utf8: None,
            response_hex: "7b7d".to_string(),
            decoded_with_schema: true,
            schema_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_string(),
            response_root_type: Some("demo.Response".to_string()),
        }));
        assert_eq!(app.service_call().response_scroll(), 0);
    }

    #[test]
    fn topic_preview_tracks_message_count_and_rate() {
        let mut app = App::default();
        app.begin_topic_preview("imu/data".to_string());
        app.apply_topic_preview_frame(TopicPreviewFrame {
            topic: aorta_introspect::ids::TopicSuffix::new("imu/data").expect("topic"),
            key_expr: "aorta/default/pub/imu/data".to_string(),
            payload_hex: "00".to_string(),
            decoded_json: Some("{\"x\":1}".to_string()),
            schema_hash: None,
            decode_status: aorta_introspect::client::TopicPreviewDecodeStatus::RawOnly,
        });
        std::thread::sleep(Duration::from_millis(5));
        app.apply_topic_preview_frame(TopicPreviewFrame {
            topic: aorta_introspect::ids::TopicSuffix::new("imu/data").expect("topic"),
            key_expr: "aorta/default/pub/imu/data".to_string(),
            // 3-byte payload (6 hex chars) -> exercises the hex_len/2 derivation.
            payload_hex: "010203".to_string(),
            decoded_json: Some("{\"x\":2}".to_string()),
            schema_hash: None,
            decode_status: aorta_introspect::client::TopicPreviewDecodeStatus::RawOnly,
        });

        assert_eq!(app.topic_preview().message_count(), 2);
        assert!(app.topic_preview().live_rate_hz().is_some());
        assert!(app.topic_preview().latest_frame().is_some());
        // A3 bytes column: sizes = [1 byte ("00"), 3 bytes ("010203")] -> mean 2.0,
        // and live bandwidth = rate * mean is available once a rate exists.
        assert_eq!(app.topic_preview().mean_payload_bytes(), Some(2.0));
        assert!(app.topic_preview().live_bytes_per_s().is_some());
    }

    #[test]
    fn topic_preview_scroll_is_bounded_at_zero() {
        let mut app = App::default();
        app.scroll_topic_preview_down(10);
        app.scroll_topic_preview_up(4);
        assert_eq!(app.topic_preview().scroll(), 6);

        app.scroll_topic_preview_up(20);
        assert_eq!(app.topic_preview().scroll(), 0);
    }

    #[test]
    fn topic_preview_view_mode_toggles_and_resets_scroll() {
        let mut app = App::default();
        app.scroll_topic_preview_down(10);
        assert_eq!(
            app.topic_preview().view_mode(),
            TopicPreviewViewMode::Decoded
        );

        app.toggle_topic_preview_view_mode();
        assert_eq!(app.topic_preview().view_mode(), TopicPreviewViewMode::Raw);
        assert_eq!(app.topic_preview().scroll(), 0);

        app.toggle_topic_preview_view_mode();
        assert_eq!(app.topic_preview().view_mode(), TopicPreviewViewMode::Diff);

        app.toggle_topic_preview_view_mode();
        assert_eq!(
            app.topic_preview().view_mode(),
            TopicPreviewViewMode::Decoded
        );
    }

    #[test]
    fn topic_preview_history_supports_navigation() {
        let mut app = App::default();
        app.begin_topic_preview("imu/data".to_string());
        app.apply_topic_preview_frame(sample_topic_preview_frame("00", Some("{\"x\":1}")));
        app.apply_topic_preview_frame(sample_topic_preview_frame("01", Some("{\"x\":2}")));
        app.apply_topic_preview_frame(sample_topic_preview_frame("02", Some("{\"x\":3}")));

        assert_eq!(app.topic_preview().history_position(), Some((3, 3)));
        assert_eq!(
            app.topic_preview()
                .selected_frame()
                .map(|frame| frame.payload_hex.as_str()),
            Some("02")
        );

        app.select_older_topic_preview_frame();
        assert_eq!(app.topic_preview().history_position(), Some((2, 3)));
        assert_eq!(
            app.topic_preview()
                .selected_frame()
                .map(|frame| frame.payload_hex.as_str()),
            Some("01")
        );

        app.select_older_topic_preview_frame();
        app.select_older_topic_preview_frame();
        assert_eq!(app.topic_preview().history_position(), Some((1, 3)));

        app.select_newer_topic_preview_frame();
        assert_eq!(app.topic_preview().history_position(), Some((2, 3)));
    }

    #[test]
    fn topic_preview_diff_pair_prefers_previous_and_falls_back_to_next() {
        let mut app = App::default();
        app.begin_topic_preview("imu/data".to_string());
        app.apply_topic_preview_frame(sample_topic_preview_frame("00", Some("{\"x\":1}")));
        app.apply_topic_preview_frame(sample_topic_preview_frame("01", Some("{\"x\":2}")));
        app.apply_topic_preview_frame(sample_topic_preview_frame("02", Some("{\"x\":3}")));

        let latest_pair = app.topic_preview().diff_pair().expect("latest diff pair");
        assert_eq!(latest_pair.selected_position, 3);
        assert_eq!(latest_pair.reference_position, 2);
        assert_eq!(
            latest_pair.reference_relation,
            TopicPreviewDiffReference::Previous
        );
        assert_eq!(latest_pair.selected_frame.payload_hex, "02");
        assert_eq!(latest_pair.reference_frame.payload_hex, "01");

        app.select_older_topic_preview_frame();
        app.select_older_topic_preview_frame();
        let oldest_pair = app.topic_preview().diff_pair().expect("oldest diff pair");
        assert_eq!(oldest_pair.selected_position, 1);
        assert_eq!(oldest_pair.reference_position, 2);
        assert_eq!(
            oldest_pair.reference_relation,
            TopicPreviewDiffReference::Next
        );
        assert_eq!(oldest_pair.selected_frame.payload_hex, "00");
        assert_eq!(oldest_pair.reference_frame.payload_hex, "01");
    }

    #[test]
    fn replacing_dashboard_preserves_list_selection_when_rows_still_exist() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.select_next();
        assert_eq!(
            app.topics().selected_row().map(|row| row.name.as_str()),
            Some("camera/front")
        );

        app.replace_dashboard(sample_dashboard());
        assert_eq!(
            app.topics().selected_row().map(|row| row.name.as_str()),
            Some("camera/front")
        );
    }

    #[test]
    fn zenoh_selection_is_preserved_when_router_still_exists() {
        let mut app = App::default();
        app.toggle_advanced_mode();
        app.set_zenoh_internals(DetailPane::Loaded(sample_zenoh_internals()));
        app.open_screen(Screen::Zenoh).expect("open zenoh");
        assert_eq!(app.active_screen(), Screen::Zenoh);
        app.select_next();
        assert_eq!(
            app.zenoh_routers()
                .selected_row()
                .map(|row| row.zid.as_str()),
            Some("router-beta")
        );

        app.set_zenoh_internals(DetailPane::Loaded(sample_zenoh_internals()));
        assert_eq!(
            app.zenoh_routers()
                .selected_row()
                .map(|row| row.zid.as_str()),
            Some("router-beta")
        );
    }

    #[test]
    fn service_call_hint_navigation_and_patch_application_work() {
        let mut app = App::default();
        app.set_service_call_schema(DetailPane::Loaded(sample_service_call_schema()));

        assert_eq!(
            app.service_call_selected_hint()
                .map(|field| field.path.as_str()),
            Some("message")
        );

        app.service_call_select_next_hint();
        app.service_call_select_next_hint();
        assert_eq!(
            app.service_call_selected_hint()
                .map(|field| field.path.as_str()),
            Some("options.limit")
        );

        app.service_call
            .editor
            .set_text("{\n  \"message\": \"hello\"\n}");
        let applied = app
            .service_call_apply_selected_hint_template()
            .expect("apply patch");
        assert_eq!(applied, "options.limit");
        assert_eq!(
            app.service_call_request_json(),
            "{\n  \"message\": \"hello\",\n  \"options\": {\n    \"limit\": 10\n  }\n}"
        );
    }

    #[test]
    fn service_call_request_can_reset_to_starter_json() {
        let mut app = App::default();
        app.set_service_call_schema(DetailPane::Loaded(sample_service_call_schema()));
        app.service_call.editor.set_text("{}");

        app.service_call_reset_request_to_starter()
            .expect("reset request");

        assert_eq!(
            app.service_call_request_json(),
            "{\n  \"message\": \"\",\n  \"options\": {\n    \"limit\": 10\n  },\n  \"mode\": \"AUTO\"\n}"
        );
    }
}

#[cfg(test)]
pub(crate) fn sample_dashboard() -> DashboardData {
    DashboardData {
        overview: OverviewData {
            topic_count: 3,
            service_count: 2,
            node_count: 2,
            node_discovery_source: NodeDiscoverySource::AdminSnapshot,
            health: OverviewHealthSummary {
                overall: HealthLevel::Warn,
                discovery_capability: CapabilityState::Partial,
                runtime_capability: CapabilityState::Partial,
                freshness: FreshnessState::Fresh,
                runtime_collection_period_ms: Some(1000),
                runtime_topic_coverage: 2,
                runtime_service_coverage: 1,
                runtime_node_coverage: 1,
                warning_count: 2,
                error_count: 0,
                unknown_count: 1,
                schema_unknown_count: 1,
            },
            highlighted_topics: vec![crate::data::HighlightedResource {
                name: "camera/front".to_string(),
                health: HealthLevel::Warn,
                reason: "runtime warnings=2".to_string(),
            }],
            highlighted_services: vec![crate::data::HighlightedResource {
                name: "nav/plan".to_string(),
                health: HealthLevel::Healthy,
                reason: "runtime ok (~1000 ms)".to_string(),
            }],
        },
        topics: vec![
            TopicRow {
                name: "imu/data".to_string(),
                provider_endpoint_count: 1,
                candidate_node_count: 1,
                publisher_node_count: Some(1),
                subscriber_node_count: None,
                schema_availability: crate::data::SchemaAvailability::Confirmed,
                schema_note: None,
                health: HealthLevel::Healthy,
                capability: CapabilityState::Full,
                freshness: FreshnessState::Fresh,
                runtime_collection_period_ms: Some(1000),
                status_summary: "runtime ok (~1000 ms)".to_string(),
            },
            TopicRow {
                name: "camera/front".to_string(),
                provider_endpoint_count: 1,
                candidate_node_count: 1,
                publisher_node_count: Some(1),
                subscriber_node_count: None,
                schema_availability: crate::data::SchemaAvailability::Confirmed,
                schema_note: None,
                health: HealthLevel::Warn,
                capability: CapabilityState::Full,
                freshness: FreshnessState::Fresh,
                runtime_collection_period_ms: Some(1000),
                status_summary: "runtime warnings=2".to_string(),
            },
            TopicRow {
                name: "navigation/plan".to_string(),
                provider_endpoint_count: 1,
                candidate_node_count: 1,
                publisher_node_count: Some(1),
                subscriber_node_count: None,
                schema_availability: crate::data::SchemaAvailability::Unknown,
                schema_note: None,
                health: HealthLevel::Warn,
                capability: CapabilityState::Partial,
                freshness: FreshnessState::Unavailable,
                runtime_collection_period_ms: None,
                status_summary: "schema not confirmed from list discovery".to_string(),
            },
        ],
        services: vec![ServiceRow {
            name: "nav/plan".to_string(),
            provider_endpoint_count: 1,
            candidate_node_count: 1,
            provider_node_count: Some(1),
            client_node_count: None,
            schema_availability: crate::data::SchemaAvailability::Confirmed,
            schema_note: None,
            health: HealthLevel::Healthy,
            capability: CapabilityState::Full,
            freshness: FreshnessState::Fresh,
            runtime_collection_period_ms: Some(1000),
            status_summary: "runtime ok (~1000 ms)".to_string(),
        }],
        nodes: vec![
            NodeRow {
                name: "planner".to_string(),
                node_id: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6".to_string(),
                group: "default".to_string(),
                endpoint_count: Some(1),
                topic_count: Some(1),
                service_count: Some(1),
                discovery_source: NodeDiscoverySource::AdminSnapshot,
                health: HealthLevel::Unknown,
                capability: CapabilityState::Partial,
                freshness: FreshnessState::Unavailable,
                runtime_collection_period_ms: None,
                status_summary: "runtime unavailable — admin snapshot only".to_string(),
            },
            NodeRow {
                name: "perception".to_string(),
                node_id: "b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6".to_string(),
                group: "default".to_string(),
                endpoint_count: Some(3),
                topic_count: Some(2),
                service_count: Some(0),
                discovery_source: NodeDiscoverySource::TelemetrySnapshot,
                health: HealthLevel::Healthy,
                capability: CapabilityState::Full,
                freshness: FreshnessState::Fresh,
                runtime_collection_period_ms: Some(1000),
                status_summary: "runtime healthy from telemetry snapshot".to_string(),
            },
        ],
    }
}

#[cfg(test)]
pub(crate) fn sample_topic_preview_frame(
    payload_hex: &str,
    decoded_json: Option<&str>,
) -> TopicPreviewFrame {
    TopicPreviewFrame {
        topic: aorta_introspect::ids::TopicSuffix::new("imu/data").expect("topic"),
        key_expr: "aorta/default/pub/imu/data".to_string(),
        payload_hex: payload_hex.to_string(),
        decoded_json: decoded_json.map(str::to_string),
        schema_hash: None,
        decode_status: aorta_introspect::client::TopicPreviewDecodeStatus::RawOnly,
    }
}

#[cfg(test)]
pub(crate) fn sample_service_call_schema() -> ServiceCallSchemaData {
    ServiceCallSchemaData {
        service_name: "nav/plan".to_string(),
        key_expr: "aorta/default/srv/nav/plan".to_string(),
        provider_endpoint_count: 1,
        candidate_node_count: 1,
        candidate_nodes_note: None,
        schema_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        request_root_type: Some("demo.Request".to_string()),
        response_root_type: Some("demo.Response".to_string()),
        request_model: aorta_introspect::schema::SchemaModel {
            root_type: "demo.Request".to_string(),
            fields: vec![
                aorta_introspect::schema::SchemaField {
                    path: "message".to_string(),
                    name: "message".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "string".to_string(),
                    type_kind: aorta_introspect::schema::SchemaTypeKind::String,
                    optional: false,
                    repeated: false,
                    default_json: Some(serde_json::json!("")),
                    enum_values: Vec::new(),
                    doc: Some("Planner prompt or instruction.".to_string()),
                },
                aorta_introspect::schema::SchemaField {
                    path: "options".to_string(),
                    name: "options".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "demo.Options".to_string(),
                    type_kind: aorta_introspect::schema::SchemaTypeKind::Table,
                    optional: false,
                    repeated: false,
                    default_json: None,
                    enum_values: Vec::new(),
                    doc: Some("Advanced planner options.".to_string()),
                },
                aorta_introspect::schema::SchemaField {
                    path: "options.limit".to_string(),
                    name: "limit".to_string(),
                    parent_path: Some("options".to_string()),
                    depth: 1,
                    type_name: "i32".to_string(),
                    type_kind: aorta_introspect::schema::SchemaTypeKind::Integer,
                    optional: false,
                    repeated: false,
                    default_json: Some(serde_json::json!(10)),
                    enum_values: Vec::new(),
                    doc: Some("Maximum number of waypoints.".to_string()),
                },
                aorta_introspect::schema::SchemaField {
                    path: "mode".to_string(),
                    name: "mode".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "demo.Mode".to_string(),
                    type_kind: aorta_introspect::schema::SchemaTypeKind::Enum,
                    optional: false,
                    repeated: false,
                    default_json: None,
                    enum_values: vec![
                        aorta_introspect::schema::SchemaEnumValue {
                            name: "AUTO".to_string(),
                            number: 0,
                        },
                        aorta_introspect::schema::SchemaEnumValue {
                            name: "MANUAL".to_string(),
                            number: 1,
                        },
                    ],
                    doc: Some("Planner mode.".to_string()),
                },
            ],
        },
        starter_json:
            "{\n  \"message\": \"\",\n  \"options\": {\n    \"limit\": 10\n  },\n  \"mode\": \"AUTO\"\n}"
                .to_string(),
    }
}

#[cfg(test)]
pub(crate) fn sample_action_row() -> ActionRow {
    ActionRow {
        name: "demo/execute_task".to_string(),
        server_node_name: "task_runner".to_string(),
        supports_cancel: true,
        max_concurrency: 1,
        result_ttl_ms: 60_000,
        dedup_ttl_ms: 30_000,
        schema_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        status_summary: "cancel=yes concurrency=1 result_ttl=60000ms".to_string(),
    }
}

#[cfg(test)]
pub(crate) fn sample_action_call_schema() -> ActionCallSchemaData {
    ActionCallSchemaData {
        action_name: "demo/execute_task".to_string(),
        key_expr: "aorta/default/act/send_goal/demo/execute_task".to_string(),
        server_node_name: "task_runner".to_string(),
        supports_cancel: true,
        schema_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        send_goal_request_root: "aorta.action.demo.ExecuteTaskSendGoalRequest".to_string(),
        request_model: aorta_introspect::schema::SchemaModel {
            root_type: "aorta.action.demo.ExecuteTaskSendGoalRequest".to_string(),
            fields: vec![
                aorta_introspect::schema::SchemaField {
                    path: "goal_id".to_string(),
                    name: "goal_id".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "string".to_string(),
                    type_kind: aorta_introspect::schema::SchemaTypeKind::String,
                    optional: false,
                    repeated: false,
                    default_json: Some(serde_json::json!("")),
                    enum_values: Vec::new(),
                    doc: Some("Caller-supplied goal id.".to_string()),
                },
                aorta_introspect::schema::SchemaField {
                    path: "goal".to_string(),
                    name: "goal".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "aorta.action.demo.ExecuteTaskGoal".to_string(),
                    type_kind: aorta_introspect::schema::SchemaTypeKind::Table,
                    optional: false,
                    repeated: false,
                    default_json: None,
                    enum_values: Vec::new(),
                    doc: Some("The goal payload.".to_string()),
                },
                aorta_introspect::schema::SchemaField {
                    path: "goal.source".to_string(),
                    name: "source".to_string(),
                    parent_path: Some("goal".to_string()),
                    depth: 1,
                    type_name: "string".to_string(),
                    type_kind: aorta_introspect::schema::SchemaTypeKind::String,
                    optional: false,
                    repeated: false,
                    default_json: Some(serde_json::json!("")),
                    enum_values: Vec::new(),
                    doc: Some("Where the task came from.".to_string()),
                },
            ],
        },
        // The editor body holds the *goal payload* (the goal_id/goal wrapper is
        // added at submit time); the starter mirrors the `goal` subtree.
        starter_json: "{\n  \"source\": \"\"\n}".to_string(),
    }
}

#[cfg(test)]
pub(crate) fn sample_zenoh_internals() -> ZenohInternalsData {
    ZenohInternalsData {
        router_count: 2,
        routers: vec![
            ZenohRouterRow {
                zid: "router-alpha".to_string(),
                whatami: "router".to_string(),
                version: Some("1.7.2".to_string()),
                session_count: Some(3),
                locator_count: 1,
                plugin_count: 1,
                metrics_available: true,
                metrics: None,
                locators: vec!["tcp/127.0.0.1:7447".to_string()],
                plugin_names: vec!["rest".to_string()],
                raw_payload_utf8: "{\"version\":\"1.7.2\"}".to_string(),
            },
            ZenohRouterRow {
                zid: "router-beta".to_string(),
                whatami: "router".to_string(),
                version: Some("1.7.2".to_string()),
                session_count: Some(1),
                locator_count: 1,
                plugin_count: 0,
                metrics_available: false,
                metrics: None,
                locators: vec!["tcp/10.0.0.2:7447".to_string()],
                plugin_names: Vec::new(),
                raw_payload_utf8: "{\"version\":\"1.7.2\"}".to_string(),
            },
        ],
    }
}
