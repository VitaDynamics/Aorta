use std::collections::VecDeque;
use std::time::Duration;

use aorta_introspect::client::{
    compute_latency_snapshot, NodeDiscoverySource, TopicPreviewDecodeStatus, TopicPreviewFrame,
};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, ListState, Paragraph, Row, Sparkline, Table,
    TableState, Tabs, Wrap,
};
use ratatui::Frame;

use crate::app::{
    App, CommandActionScope, DetailPane, EventSeverity, InteractionMode, OverviewAction,
    RecorderLaunchPlan, ResourceBookmark, Screen, SelectableList, TopicPreviewState,
    TopicPreviewViewMode,
};
use crate::data::{
    field_template_json_from_schema_model, ActionCallResponseData, ActionDetailData,
    CapabilityState, ContextData, ContextRow, ExactSchemaAvailability, FreshnessState, HealthLevel,
    HighlightedResource, NodeClientRuntimeEntryData, NodeDetailData, NodePublisherRuntimeEntryData,
    NodeRuntimeBreakdownData, NodeServiceRuntimeEntryData, NodeSubscriberRuntimeEntryData,
    SchemaDescribeData, ServiceCallResponseData, ServiceCallSchemaData, ServiceDetailData,
    TopicDetailData, ZenohConnectivityData, ZenohConnectivityEventData,
    ZenohConnectivityEventKindData, ZenohInternalsData, ZenohRouterRow,
};
use crate::mouse::{MouseTarget, MouseUiState, ScrollTarget};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LayoutMode {
    Compact,
    Standard,
    Wide,
}

impl LayoutMode {
    fn from_width(width: u16) -> Self {
        if width >= 140 {
            LayoutMode::Wide
        } else if width >= 100 {
            LayoutMode::Standard
        } else {
            LayoutMode::Compact
        }
    }

    fn label(self) -> &'static str {
        match self {
            LayoutMode::Compact => "compact",
            LayoutMode::Standard => "standard",
            LayoutMode::Wide => "wide",
        }
    }
}

pub fn render(frame: &mut Frame<'_>, app: &App) {
    let mut mouse = MouseUiState::default();
    render_with_mouse(frame, app, &mut mouse);
}

pub fn render_with_mouse(frame: &mut Frame<'_>, app: &App, mouse: &mut MouseUiState) {
    mouse.clear();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let register_base_mouse = matches!(
        app.interaction_mode(),
        InteractionMode::Normal | InteractionMode::Search
    );

    // Always register tab click regions so mouse tab-switching works in all modes.
    render_tabs(frame, layout[0], app, mouse, true);
    match app.active_screen() {
        Screen::Overview => render_overview(frame, layout[1], app, mouse, register_base_mouse),
        Screen::Topics => render_topics(frame, layout[1], app, mouse, register_base_mouse),
        Screen::Services => render_services(frame, layout[1], app, mouse, register_base_mouse),
        Screen::ServiceCall => {
            render_service_call(frame, layout[1], app, mouse, register_base_mouse)
        }
        Screen::Actions => render_actions(frame, layout[1], app, mouse, register_base_mouse),
        Screen::ActionCall => render_action_call(frame, layout[1], app, mouse, register_base_mouse),
        Screen::Nodes => render_nodes(frame, layout[1], app, mouse, register_base_mouse),
        Screen::Context => render_context(frame, layout[1], app),
        Screen::Zenoh => render_zenoh_internals(frame, layout[1], app, mouse, register_base_mouse),
        Screen::Telemetry => render_telemetry(frame, layout[1], app),
    }
    render_status(frame, layout[2], app);
    if app.is_help_visible() {
        render_help_overlay(frame, app);
    } else if app.is_event_timeline_visible() {
        render_event_timeline_overlay(frame, app, mouse);
    } else if app.is_log_stream_visible() {
        render_log_stream_overlay(frame, app, mouse);
    } else if app.is_topology_overlay_visible() {
        render_topology_overlay(frame, app);
    } else if app.is_recorder_overlay_visible() {
        render_recorder_overlay(frame, app);
    } else if app.is_node_comparison_visible() {
        render_node_comparison_overlay(frame, app);
    } else if app.is_bookmark_overlay_visible() {
        render_bookmarks_overlay(frame, app, mouse);
    } else if app.is_command_palette_visible() {
        render_command_palette_overlay(frame, app, mouse);
    }
}

fn render_tabs(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let screens = app.visible_screens();
    let titles: Vec<_> = screens
        .iter()
        .map(|screen| Line::from(screen.title()))
        .collect();
    let selected = screens
        .iter()
        .position(|screen| *screen == app.active_screen())
        .unwrap_or(0);
    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(app_shell_title(app)),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .select(selected);
    frame.render_widget(tabs, area);

    if register_mouse {
        register_tab_regions(mouse, area, &screens);
    }
}

fn render_overview(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let layout_mode = LayoutMode::from_width(area.width);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(6)])
        .split(area);
    let summary = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(33),
            Constraint::Percentage(34),
        ])
        .split(layout[0]);

    let overview = app.overview();
    frame.render_widget(
        stat_card("Topics", overview.topic_count.to_string()),
        summary[0],
    );
    frame.render_widget(
        stat_card("Services", overview.service_count.to_string()),
        summary[1],
    );
    frame.render_widget(
        stat_card("Nodes", overview.node_count.to_string()),
        summary[2],
    );

    let details = match layout_mode {
        LayoutMode::Compact => Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(14), Constraint::Min(8)])
            .split(layout[1]),
        LayoutMode::Standard | LayoutMode::Wide => Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(44), Constraint::Percentage(56)])
            .split(layout[1]),
    };

    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(9), Constraint::Min(6)])
        .split(details[0]);
    render_overview_actions(frame, left[0], app, mouse, register_mouse);
    render_overview_summary(frame, left[1], app);

    render_detail_panel(
        frame,
        details[1],
        overview_event_panel_title(app),
        overview_event_lines(app),
    );
}

fn render_overview_actions(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let items: Vec<ListItem<'_>> = OverviewAction::ALL
        .iter()
        .map(|action| ListItem::new(action.title()))
        .collect();
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Quick Actions (`Enter`/click to open)"),
        )
        .highlight_style(Style::default().bg(Color::Blue))
        .highlight_symbol(">> ");
    let mut state = ListState::default();
    state.select(Some(
        OverviewAction::ALL
            .iter()
            .position(|action| *action == app.overview_selected_action())
            .unwrap_or(0),
    ));
    frame.render_stateful_widget(list, area, &mut state);

    if register_mouse {
        register_list_rows(
            mouse,
            area,
            state.offset(),
            OverviewAction::ALL.len(),
            ScrollTarget::OverviewActions,
            MouseTarget::OverviewAction,
        );
    }
}

fn render_overview_summary(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let overview = app.overview();
    let runtime_freshness = format_freshness_with_period(
        overview.health.freshness,
        overview.health.runtime_collection_period_ms,
    );
    let mut lines = vec![
        Line::from(format!(
            "Overall health: {}",
            overview.health.overall.as_label()
        )),
        Line::from(format!(
            "Node discovery: {}",
            node_discovery_source_label(overview.node_discovery_source)
        )),
        Line::from(format!(
            "Discovery note: {}",
            node_discovery_source_note(overview.node_discovery_source)
        )),
        Line::from(format!(
            "Discovery capability: {}",
            overview.health.discovery_capability.as_label()
        )),
        Line::from(format!(
            "Runtime capability: {}",
            overview.health.runtime_capability.as_label()
        )),
        Line::from(format!("Runtime freshness: {}", runtime_freshness)),
        Line::from(format!(
            "Runtime coverage: topics {}/{} | services {}/{} | nodes {}/{}",
            overview.health.runtime_topic_coverage,
            overview.topic_count,
            overview.health.runtime_service_coverage,
            overview.service_count,
            overview.health.runtime_node_coverage,
            overview.node_count
        )),
        Line::from(format!(
            "Attention: err={} warn={} unknown={} schema_unknown={}",
            overview.health.error_count,
            overview.health.warning_count,
            overview.health.unknown_count,
            overview.health.schema_unknown_count
        )),
    ];
    lines.extend([
        Line::from("Tips:"),
        Line::from("  `Tab` switch screen"),
        Line::from("  `r` refresh snapshot"),
        Line::from("  `z` toggle advanced mode"),
        Line::from("  `1-5` open hot topic"),
        Line::from("  `F1-F5` open hot service"),
        Line::from(format!(
            "Advanced mode: {}",
            if app.is_advanced_mode() {
                "enabled"
            } else {
                "disabled"
            }
        )),
        Line::from("Hot topics:"),
    ]);
    lines.extend(
        overview
            .highlighted_topics
            .iter()
            .enumerate()
            .map(|(index, topic)| highlighted_resource_line(index + 1, topic, false)),
    );
    if overview.highlighted_topics.is_empty() {
        lines.push(Line::from("• none"));
    }
    lines.push(Line::from("Hot services:"));
    lines.extend(
        overview
            .highlighted_services
            .iter()
            .enumerate()
            .map(|(index, service)| highlighted_resource_line(index + 1, service, true)),
    );
    if overview.highlighted_services.is_empty() {
        lines.push(Line::from("• none"));
    }
    render_detail_panel(frame, area, &read_only_title("Overview"), lines);
}

fn render_topics(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let layout = split_master_detail(area);
    render_topic_table(
        frame,
        layout[0],
        "Topics",
        app,
        app.topics(),
        mouse,
        register_mouse,
    );
    let schema_describe_active = app.is_topic_schema_describe_visible();
    let right = match LayoutMode::from_width(area.width) {
        LayoutMode::Compact => {
            let constraints = if schema_describe_active {
                [Constraint::Min(16), Constraint::Length(6)]
            } else {
                [Constraint::Length(12), Constraint::Min(10)]
            };
            Layout::default()
                .direction(Direction::Vertical)
                .constraints(constraints)
                .split(layout[1])
        }
        LayoutMode::Standard | LayoutMode::Wide => {
            let constraints = if schema_describe_active {
                [Constraint::Min(16), Constraint::Length(6)]
            } else {
                [Constraint::Length(8), Constraint::Min(8)]
            };
            Layout::default()
                .direction(Direction::Vertical)
                .constraints(constraints)
                .split(layout[1])
        }
    };

    let detail_lines = if let Some(selected) = app.topics().selected_row() {
        topic_detail_lines(
            selected,
            app.topic_detail(),
            app.topic_schema_describe(),
            app.is_topic_runtime_section_expanded(),
            app.is_topic_bookmarked(&selected.name),
        )
    } else {
        selection_idle_lines("topic", "Topics", app.topics())
    };
    render_scrolling_detail_panel(
        frame,
        right[0],
        &read_only_scrollable_title("Topic Detail"),
        detail_lines,
        app.detail_scroll(),
    );
    if register_mouse {
        register_scrollable_panel(mouse, right[0], ScrollTarget::DetailPanel);
    }
    render_scrolling_detail_panel(
        frame,
        right[1],
        &read_only_scrollable_title("Topic Preview"),
        topic_preview_lines(app.topic_preview()),
        app.topic_preview().scroll(),
    );
    if register_mouse {
        register_scrollable_panel(mouse, right[1], ScrollTarget::TopicPreview);
    }
}

fn render_services(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let layout = split_master_detail(area);
    render_service_table(
        frame,
        layout[0],
        "Services",
        app,
        app.services(),
        mouse,
        register_mouse,
    );

    let detail_lines = if let Some(selected) = app.services().selected_row() {
        let mut lines = service_detail_lines(
            selected,
            app.service_detail(),
            app.service_schema_describe(),
            app.is_service_runtime_section_expanded(),
            app.is_service_bookmarked(&selected.name),
        );
        lines.push(Line::from("Press `c` to open Service Call."));
        lines
    } else {
        selection_idle_lines("service", "Services", app.services())
    };
    render_scrolling_detail_panel(
        frame,
        layout[1],
        &read_only_scrollable_title("Service Detail"),
        detail_lines,
        app.detail_scroll(),
    );
    if register_mouse {
        register_scrollable_panel(mouse, layout[1], ScrollTarget::DetailPanel);
    }
}

fn render_service_call(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let layout_mode = LayoutMode::from_width(area.width);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints(match layout_mode {
            LayoutMode::Compact => [
                Constraint::Length(6),
                Constraint::Min(16),
                Constraint::Length(9),
            ],
            LayoutMode::Standard | LayoutMode::Wide => [
                Constraint::Length(6),
                Constraint::Min(12),
                Constraint::Length(10),
            ],
        })
        .split(area);
    let center = match layout_mode {
        LayoutMode::Compact => Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(layout[1]),
        LayoutMode::Standard | LayoutMode::Wide => Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(layout[1]),
    };

    let summary_lines = service_call_summary_lines(app);
    render_detail_panel(
        frame,
        layout[0],
        &read_only_title("Service Call Summary"),
        summary_lines,
    );

    render_request_editor(frame, center[0], app);
    render_schema_hints(frame, center[1], app, mouse, register_mouse);
    render_service_call_response(frame, layout[2], app, mouse, register_mouse);
}

fn render_action_call(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let layout_mode = LayoutMode::from_width(area.width);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints(match layout_mode {
            LayoutMode::Compact => [
                Constraint::Length(6),
                Constraint::Min(16),
                Constraint::Length(9),
            ],
            LayoutMode::Standard | LayoutMode::Wide => [
                Constraint::Length(6),
                Constraint::Min(12),
                Constraint::Length(10),
            ],
        })
        .split(area);
    let center = match layout_mode {
        LayoutMode::Compact => Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(layout[1]),
        LayoutMode::Standard | LayoutMode::Wide => Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(layout[1]),
    };

    let summary_lines = action_call_summary_lines(app);
    render_detail_panel(
        frame,
        layout[0],
        &read_only_title("Action Call Summary"),
        summary_lines,
    );

    render_action_request_editor(frame, center[0], app);
    render_action_schema_hints(frame, center[1], app, mouse, register_mouse);

    // Bottom region: send/cancel result on the left, the live feedback panel
    // (Feature C2) on the right. Stacked vertically on Compact width.
    let bottom = match layout_mode {
        LayoutMode::Compact => Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(layout[2]),
        LayoutMode::Standard | LayoutMode::Wide => Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(layout[2]),
    };
    render_action_call_response(frame, bottom[0], app, mouse, register_mouse);
    render_action_call_feedback(frame, bottom[1], app);
}

/// Live action-feedback panel (Feature C2). Renders the bounded per-goal
/// feedback rows streamed in by the feedback controller; read-only, newest at
/// the bottom (most recent feedback closest to the result pane).
fn render_action_call_feedback(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let feedback = app.action_call_feedback();
    let lines: Vec<Line<'_>> = if feedback.is_empty() {
        match app.action_call_last_goal() {
            Some((goal_id, _)) => vec![
                Line::from(format!("Awaiting feedback for goal {}.", goal_id)),
                Line::from("Feedback entries stream in here as the server reports progress."),
            ],
            None => vec![
                Line::from("No goal in flight.".to_string()),
                Line::from("Send a goal with `F5` to stream its live feedback here."),
            ],
        }
    } else {
        feedback
            .iter()
            .map(|row| {
                Line::from(vec![
                    Span::styled(format!("#{:<4}", row.seq), Style::default().fg(Color::Gray)),
                    Span::styled(
                        format!("[{}] ", row.status),
                        Style::default().fg(Color::Cyan),
                    ),
                    Span::raw(if row.message.is_empty() {
                        truncate_with_ellipsis(&row.data_json, 64)
                    } else {
                        format!(
                            "{} {}",
                            row.message,
                            truncate_with_ellipsis(&row.data_json, 48)
                        )
                    }),
                ])
            })
            .collect()
    };
    let title = format!("Live Feedback ({})", feedback.len());
    render_detail_panel(frame, area, &read_only_title(&title), lines);
}

fn render_action_request_editor(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let title = action_call_request_editor_title(app);
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    let paragraph = Paragraph::new(app.action_call_request_json())
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);

    if app.active_screen() == Screen::ActionCall && app.is_action_call_editing() && inner.width > 0
    {
        let (line, col) = app.action_call_cursor_line_col();
        let x = inner.x + (col as u16).min(inner.width.saturating_sub(1));
        let y = inner.y + (line as u16).min(inner.height.saturating_sub(1));
        frame.set_cursor_position((x, y));
    }
}

fn render_action_schema_hints(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    match app.action_call().schema() {
        DetailPane::Loaded(schema) if !schema.request_model.fields.is_empty() => {
            let layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(9), Constraint::Length(8)])
                .split(area);

            render_detail_panel(
                frame,
                layout[0],
                "Goal Hints (selected field detail, read-only)",
                schema_hint_lines_for_model(
                    &schema.request_model,
                    app.action_call_selected_hint_index(),
                    None,
                ),
            );

            let items: Vec<ListItem<'_>> = schema
                .request_model
                .fields
                .iter()
                .map(|field| ListItem::new(format_schema_field_summary(field)))
                .collect();
            let list = List::new(items)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Field List (`click`/wheel select, `a` apply)"),
                )
                .highlight_style(Style::default().bg(Color::Blue))
                .highlight_symbol(">> ");
            let mut state = ListState::default();
            state.select(app.action_call_selected_hint_index());
            frame.render_stateful_widget(list, layout[1], &mut state);
            if register_mouse {
                register_list_rows(
                    mouse,
                    layout[1],
                    state.offset(),
                    schema.request_model.fields.len(),
                    ScrollTarget::ActionCallHintList,
                    MouseTarget::ActionCallHintRow,
                );
            }
        }
        DetailPane::Loaded(schema) => {
            render_detail_panel(
                frame,
                area,
                "Goal Hints (read-only)",
                schema_hint_lines_for_model(
                    &schema.request_model,
                    app.action_call_selected_hint_index(),
                    None,
                ),
            );
        }
        DetailPane::Error(message) => {
            render_detail_panel(
                frame,
                area,
                "Goal Hints (read-only)",
                vec![
                    Line::from("Hints unavailable.".to_string()),
                    Line::from(format!("Error: {}", message)),
                    Line::from(
                        "Open Action Call from the Actions screen again after schema discovery recovers."
                            .to_string(),
                    ),
                ],
            );
        }
        DetailPane::Empty => {
            render_detail_panel(
                frame,
                area,
                "Goal Hints (read-only)",
                vec![
                    Line::from("Hints unavailable until schema loads.".to_string()),
                    Line::from("Use `c` from the Actions screen.".to_string()),
                ],
            );
        }
    }
}

fn render_action_call_response(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let lines = match app.action_call().response() {
        DetailPane::Loaded(response) => action_response_lines(response),
        DetailPane::Error(message) => vec![
            Line::from("Last send/cancel failed.".to_string()),
            Line::from(format!("Error: {}", message)),
            Line::from(
                "Check goal JSON, server availability, and the action control timeout before retrying."
                    .to_string(),
            ),
        ],
        DetailPane::Empty => vec![
            Line::from("No goal sent yet.".to_string()),
            Line::from("Press `F5` to send the current goal.".to_string()),
            Line::from("Press `i` to edit the goal body, or `a` to merge a field template."),
        ],
    };
    render_scrolling_detail_panel(
        frame,
        area,
        &read_only_scrollable_title("Send/Cancel Result"),
        lines,
        app.action_call_response_scroll(),
    );
    if register_mouse {
        register_scrollable_panel(mouse, area, ScrollTarget::ActionCallResponse);
    }
}

fn action_call_summary_lines(app: &App) -> Vec<Line<'_>> {
    match app.action_call().schema() {
        DetailPane::Loaded(schema) => {
            let selected_hint = app
                .action_call_selected_hint()
                .map(|field| field.path.as_str())
                .unwrap_or("none");
            let last_goal = match app.action_call_last_goal() {
                Some((goal_id, _)) => format!("{} (`x` to cancel)", goal_id),
                None => "none".to_string(),
            };
            vec![
                Line::from(format!("Action: {}", schema.action_name)),
                Line::from(format!("Key: {}", schema.key_expr)),
                Line::from(format!(
                    "Server node: {} | supports cancel: {}",
                    schema.server_node_name,
                    if schema.supports_cancel { "yes" } else { "no" }
                )),
                Line::from(format!("Schema hash: {}", schema.schema_hash)),
                Line::from(format!("SendGoal root: {}", schema.send_goal_request_root)),
                Line::from(format!(
                    "Editor: {} | selected field: {} | last goal: {}",
                    if app.is_action_call_editing() {
                        "editing"
                    } else {
                        "view"
                    },
                    selected_hint,
                    last_goal
                )),
            ]
        }
        DetailPane::Error(message) => vec![
            Line::from(format!(
                "Action: {}",
                app.action_call_target().unwrap_or("(none)")
            )),
            Line::from("Action-call schema detail is currently unavailable.".to_string()),
            Line::from(format!("Error: {}", message)),
            Line::from("Use `c` from Actions again after schema discovery recovers.".to_string()),
        ],
        DetailPane::Empty => vec![
            Line::from(format!(
                "Action: {}",
                app.action_call_target().unwrap_or("(none)")
            )),
            Line::from("Action-call schema detail is not loaded yet.".to_string()),
            Line::from("Select an action in `Actions` and press `c`.".to_string()),
        ],
    }
}

fn action_call_request_editor_title(app: &App) -> String {
    let action = app.action_call_target().unwrap_or("(none)");
    if app.is_action_call_editing() {
        format!("Goal JSON | {action} | editing (`F5` send, `Esc` finish)")
    } else {
        format!(
            "Goal JSON | {action} | view (`i` edit, `a` apply, `R` reset, `F5` send, `x` cancel)"
        )
    }
}

fn action_response_lines(response: &ActionCallResponseData) -> Vec<Line<'_>> {
    let mut lines = vec![
        Line::from(format!(
            "Accepted: {}",
            if response.accepted { "yes" } else { "no" }
        )),
        Line::from(format!("Goal ID: {}", response.goal_id)),
        // The goal_secret is the authorization token the user needs to cancel
        // or fetch the result of this goal; surface it (the CLI prints it too).
        Line::from(format!("Goal Secret: {}", response.goal_secret)),
        Line::from(format!("Status: {}", response.status)),
        Line::from(format!("Reply code: {}", response.code)),
    ];
    if !response.server_epoch.is_empty() {
        lines.push(Line::from(format!(
            "Server epoch: {}",
            response.server_epoch
        )));
    }
    if !response.message.is_empty() {
        lines.push(Line::from(format!("Message: {}", response.message)));
    }
    if response.accepted {
        lines.push(Line::from(
            "Press `x` to cancel this goal (uses the goal_id + goal_secret above).".to_string(),
        ));
    }
    lines
}

fn render_actions(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let layout = split_master_detail(area);
    let rows: Vec<Row<'_>> = app
        .actions()
        .visible_rows()
        .into_iter()
        .map(|action| {
            Row::new(vec![
                Cell::from(action.name.clone()),
                Cell::from(if action.supports_cancel { "yes" } else { "no" }),
                Cell::from(action.max_concurrency.to_string()),
                Cell::from(action.server_node_name.clone()),
            ])
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Percentage(38),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Percentage(46),
        ],
    )
    .header(
        Row::new(vec!["Action", "Cancel", "Conc", "Server"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(resource_table_title("Actions", app.actions(), "name")),
    )
    .row_highlight_style(Style::default().bg(Color::Blue))
    .highlight_symbol(">> ");
    let mut state = TableState::default();
    state.select(app.actions().selected_index());
    frame.render_stateful_widget(table, layout[0], &mut state);

    if register_mouse {
        register_table_rows(
            mouse,
            layout[0],
            state.offset(),
            app.actions().visible_row_count(),
            ScrollTarget::ActionList,
            MouseTarget::ActionRow,
        );
    }

    let detail_lines = if let Some(selected) = app.actions().selected_row() {
        action_detail_lines(selected.name.as_str(), app.action_detail())
    } else {
        selection_idle_lines("action", "Actions", app.actions())
    };
    render_scrolling_detail_panel(
        frame,
        layout[1],
        &read_only_scrollable_title("Action Detail"),
        detail_lines,
        app.detail_scroll(),
    );
    if register_mouse {
        register_scrollable_panel(mouse, layout[1], ScrollTarget::DetailPanel);
    }
}

fn render_nodes(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let layout = split_master_detail(area);
    let rows: Vec<Row<'_>> = app
        .nodes()
        .visible_rows()
        .into_iter()
        .map(|node| {
            Row::new(vec![
                Cell::from(node_primary_label(app, node)),
                Cell::from(
                    node.topic_count
                        .map(|count| count.to_string())
                        .unwrap_or_else(|| "n/a".to_string()),
                ),
                Cell::from(
                    node.service_count
                        .map(|count| count.to_string())
                        .unwrap_or_else(|| "n/a".to_string()),
                ),
                health_cell(node.health, node.capability),
            ])
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Percentage(52),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec!["Node", "Topics", "Srv", "State"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(resource_table_title("Nodes", app.nodes(), "name")),
    )
    .row_highlight_style(Style::default().bg(Color::Blue))
    .highlight_symbol(">> ");
    let mut state = TableState::default();
    state.select(app.nodes().selected_index());
    frame.render_stateful_widget(table, layout[0], &mut state);

    if register_mouse {
        register_table_rows(
            mouse,
            layout[0],
            state.offset(),
            app.nodes().visible_row_count(),
            ScrollTarget::NodeList,
            MouseTarget::NodeRow,
        );
    }

    let detail_lines = if let Some(selected) = app.nodes().selected_row() {
        node_detail_lines(
            selected,
            app.node_detail(),
            app.is_node_runtime_section_expanded(),
            app.is_node_bookmarked(&selected.node_id),
        )
    } else {
        selection_idle_lines("node", "Nodes", app.nodes())
    };
    render_scrolling_detail_panel(
        frame,
        layout[1],
        &read_only_scrollable_title("Node Detail"),
        detail_lines,
        app.detail_scroll(),
    );
    if register_mouse {
        register_scrollable_panel(mouse, layout[1], ScrollTarget::DetailPanel);
    }
}

fn render_zenoh_internals(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let layout = split_master_detail(area);
    render_zenoh_router_table(frame, layout[0], app, mouse, register_mouse);

    let detail_lines = zenoh_detail_lines(
        app.zenoh_internals(),
        app.zenoh_connectivity(),
        app.zenoh_connectivity_events(),
        app.zenoh_routers().selected_row(),
    );
    render_scrolling_detail_panel(
        frame,
        layout[1],
        &read_only_scrollable_title("Zenoh Diagnostics"),
        detail_lines,
        app.detail_scroll(),
    );
    if register_mouse {
        register_scrollable_panel(mouse, layout[1], ScrollTarget::DetailPanel);
    }
}

fn render_context(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let lines = context_detail_lines(app.group(), app.context());
    render_scrolling_detail_panel(
        frame,
        area,
        &read_only_scrollable_title("Context Store"),
        lines,
        app.detail_scroll(),
    );
}

/// Render the Telemetry dashboard: a per-resource client-observed latency
/// sparkline plus an approximate p50/p95/p99 readout, computed CLIENT-SIDE from
/// the bounded history rings accumulated each refresh tick. These are
/// approximate (bucket-bounded) client-observed latencies — NOT the robot's
/// internal per-stage timing.
fn render_telemetry(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let outer = Block::default()
        .borders(Borders::ALL)
        .title("Latency Telemetry (approx, client-observed)");
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    let history = app.latency_history();
    if history.is_empty() {
        let hint = Paragraph::new(vec![
            Line::from(Span::styled(
                "No client-observed latency samples yet.",
                Style::default().fg(Color::DarkGray),
            )),
            Line::from(Span::styled(
                "Open a node or service detail; client-call latencies accumulate over refresh ticks.",
                Style::default().fg(Color::DarkGray),
            )),
        ])
        .wrap(Wrap { trim: true });
        frame.render_widget(hint, inner);
        return;
    }

    // Each tracked resource gets a 3-row band: label+percentiles, sparkline, gap.
    const BAND_ROWS: u16 = 3;
    let mut cursor_y = inner.y;
    let bottom = inner.y.saturating_add(inner.height);

    for (label, ring) in history.iter() {
        if cursor_y.saturating_add(BAND_ROWS) > bottom {
            break; // out of vertical space; remaining resources are clipped.
        }
        let samples: Vec<u64> = ring.samples().iter().copied().collect();
        let snapshot = compute_latency_snapshot(&samples);

        let header_line = match snapshot {
            Some(snapshot) => Line::from(vec![
                Span::styled(
                    label.to_string(),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled(
                    format!(
                        "p50 {} / p95 {} / p99 {} / max {} (approx, {} samples)",
                        format_latency_ns(snapshot.p50_ns),
                        format_latency_ns(snapshot.p95_ns),
                        format_latency_ns(snapshot.p99_ns),
                        format_latency_ns(snapshot.max_ns),
                        snapshot.count,
                    ),
                    Style::default().fg(Color::Gray),
                ),
            ]),
            None => Line::from(vec![
                Span::styled(
                    label.to_string(),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled("(no samples)", Style::default().fg(Color::DarkGray)),
            ]),
        };

        let header_area = Rect {
            x: inner.x,
            y: cursor_y,
            width: inner.width,
            height: 1,
        };
        frame.render_widget(Paragraph::new(header_line), header_area);

        let spark_area = Rect {
            x: inner.x,
            y: cursor_y.saturating_add(1),
            width: inner.width,
            height: 1,
        };
        let sparkline = Sparkline::default()
            .data(samples.as_slice())
            .style(Style::default().fg(Color::Green));
        frame.render_widget(sparkline, spark_area);

        cursor_y = cursor_y.saturating_add(BAND_ROWS);
    }
}

/// Truncate a string to `width` display chars, appending `…` when clipped, then
/// pad with spaces to exactly `width` so the textual table columns stay aligned.
fn context_column(text: &str, width: usize) -> String {
    let count = text.chars().count();
    if count <= width {
        let mut padded = text.to_string();
        padded.extend(std::iter::repeat_n(' ', width - count));
        padded
    } else if width == 0 {
        String::new()
    } else {
        let mut truncated: String = text.chars().take(width - 1).collect();
        truncated.push('…');
        truncated
    }
}

fn context_row_line(row: &ContextRow) -> Line<'static> {
    let instance = context_column(&context_short_instance(&row.source_instance_id), 10);
    let base = format!(
        "{} {} {} {} {} {}",
        context_column(&row.key, 44),
        context_column(&row.kind, 6),
        context_column(&row.value_text, 18),
        context_column(&row.source_node, 18),
        instance,
        context_column(row.freshness, 5),
    );
    if row.conflict {
        // Highlight competing-writer rows; the ⚠ marker mirrors the CLI.
        Line::from(vec![
            Span::raw(base),
            Span::styled(" ⚠", Style::default().fg(Color::Yellow)),
        ])
    } else {
        Line::from(base)
    }
}

/// Shorten an instance id to its first 8 chars (matching the CLI
/// `truncate_instance`).
fn context_short_instance(instance: &str) -> String {
    instance.chars().take(8).collect()
}

fn context_detail_lines(group: &str, detail: &DetailPane<ContextData>) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(format!("Scope: aorta/{}/ctx/**", group))];
    match detail {
        DetailPane::Empty => {
            lines.push(Line::from(
                "No context snapshot yet. Press `r` to query owners.".to_string(),
            ));
        }
        DetailPane::Error(message) => {
            lines.push(Line::styled(
                format!("Context query failed: {}", message),
                Style::default().fg(Color::Red),
            ));
        }
        DetailPane::Loaded(data) => {
            if data.rows.is_empty() {
                lines.push(Line::from(
                    "No context keys reachable under this group.".to_string(),
                ));
            } else {
                let conflict_keys = data
                    .rows
                    .iter()
                    .filter(|row| row.conflict)
                    .map(|row| row.key.as_str())
                    .collect::<std::collections::BTreeSet<_>>()
                    .len();
                lines.push(Line::from(format!(
                    "{} entr{} | {} conflicting key{} (⚠)",
                    data.rows.len(),
                    if data.rows.len() == 1 { "y" } else { "ies" },
                    conflict_keys,
                    if conflict_keys == 1 { "" } else { "s" },
                )));
                lines.push(Line::from(""));
                lines.push(Line::styled(
                    format!(
                        "{} {} {} {} {} {}",
                        context_column("key", 44),
                        context_column("kind", 6),
                        context_column("value", 18),
                        context_column("source_node", 18),
                        context_column("instance", 10),
                        context_column("fresh", 5),
                    ),
                    Style::default().add_modifier(Modifier::BOLD),
                ));
                for row in &data.rows {
                    lines.push(context_row_line(row));
                }
            }
        }
    }
    lines
}

fn render_status(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let layout_mode = LayoutMode::from_width(area.width);
    let status = status_line(app, layout_mode, area.width.saturating_sub(2) as usize);
    let widget = Paragraph::new(status).block(Block::default().borders(Borders::ALL));
    frame.render_widget(widget, area);
}

fn status_line(app: &App, layout_mode: LayoutMode, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }

    match layout_mode {
        LayoutMode::Compact => compact_status_line(app, max_width),
        LayoutMode::Standard | LayoutMode::Wide => {
            let mut status = format!(
                "{} | {}",
                app.interaction_mode().label(),
                layout_mode.label()
            );
            if app.is_advanced_mode() {
                status.push_str(" | advanced mode");
            }
            status.push_str(" | ");
            status.push_str(contextual_hint(app, layout_mode));
            status.push_str(" | ");
            status.push_str(app.status());
            status.push_str(if app.is_auto_refresh_enabled() {
                " | auto-refresh: on"
            } else {
                " | auto-refresh: off"
            });
            truncate_with_ellipsis(&status, max_width)
        }
    }
}

fn compact_status_line(app: &App, max_width: usize) -> String {
    let mode = app.interaction_mode().label();
    let status = compact_status_summary(app.status());
    let hint = contextual_hint(app, LayoutMode::Compact);
    let base = format!("{mode} | {status}");
    if base.chars().count() >= max_width {
        return truncate_with_ellipsis(&base, max_width);
    }

    let hint_budget = max_width.saturating_sub(base.chars().count() + 3);
    if hint_budget == 0 {
        return base;
    }

    format!("{base} | {}", truncate_with_ellipsis(hint, hint_budget))
}

fn compact_status_summary(status: &str) -> &str {
    if status.starts_with("Ready.") {
        "ok"
    } else {
        status
    }
}

fn truncate_with_ellipsis(text: &str, max_width: usize) -> String {
    let char_count = text.chars().count();
    if char_count <= max_width {
        return text.to_string();
    }

    match max_width {
        0 => String::new(),
        1 => "…".to_string(),
        _ => {
            let mut truncated: String = text.chars().take(max_width - 1).collect();
            truncated.push('…');
            truncated
        }
    }
}

fn app_shell_title(app: &App) -> String {
    let mut title = format!(
        "Aorta TUI | group={} | health={} | runtime={}",
        app.group(),
        app.overview().health.overall.as_label(),
        format_freshness_with_period(
            app.overview().health.freshness,
            app.overview().health.runtime_collection_period_ms
        )
    );
    if app.is_advanced_mode() {
        title.push_str(" | advanced");
    }
    if app.is_help_visible() {
        title.push_str(" | help");
    }
    title
}

fn contextual_hint(app: &App, layout_mode: LayoutMode) -> &'static str {
    match app.interaction_mode() {
        InteractionMode::Help => {
            if layout_mode == LayoutMode::Compact {
                "Esc/? close help"
            } else {
                "Esc/? close help"
            }
        }
        InteractionMode::Timeline => {
            if layout_mode == LayoutMode::Compact {
                "wheel/click • s/o • c • Esc"
            } else {
                "j/k or wheel select • click inspect • s severity • o source • c clear • Esc close"
            }
        }
        InteractionMode::LogStream => {
            if layout_mode == LayoutMode::Compact {
                "j/k • wheel • Esc/L close"
            } else {
                "j/k or wheel select • inspect detail • Esc/L close /log viewer"
            }
        }
        InteractionMode::Topology => {
            if layout_mode == LayoutMode::Compact {
                "Esc/g close"
            } else {
                "Esc/g close • use current resource selection as topology root"
            }
        }
        InteractionMode::Recorder => {
            if layout_mode == LayoutMode::Compact {
                "y export • Esc/o close"
            } else {
                "y export script • Esc/o close • use current selection as recorder scope"
            }
        }
        InteractionMode::NodeCompare => {
            if layout_mode == LayoutMode::Compact {
                "Esc/c close • x anchor"
            } else {
                "Esc/c close • x retarget anchor from Nodes list"
            }
        }
        InteractionMode::Bookmarks => {
            if layout_mode == LayoutMode::Compact {
                "wheel • click • d • Esc"
            } else {
                "j/k or wheel select • Enter/click open • d remove • Esc close"
            }
        }
        InteractionMode::Palette => {
            if layout_mode == LayoutMode::Compact {
                "type • wheel • click • Esc"
            } else {
                "type to filter • j/k or wheel select • Enter/click run • Esc close"
            }
        }
        InteractionMode::Search => {
            if layout_mode == LayoutMode::Compact {
                "type • wheel • Enter • Esc"
            } else {
                "type to filter • wheel move • Backspace • Enter keep • Esc close"
            }
        }
        InteractionMode::Edit => {
            if layout_mode == LayoutMode::Compact {
                "edit JSON • F5 send • Esc"
            } else {
                "edit JSON • a apply hint • F5 send • Esc finish edit"
            }
        }
        InteractionMode::Normal => match app.active_screen() {
            Screen::Overview => {
                if layout_mode == LayoutMode::Compact {
                    "Enter/click • wheel • T/S/N • C • e • o • : • ?"
                } else {
                    "Enter/click open • wheel move • T/S/N search macros • C service-call macro • e timeline • o recorder • : palette • ? help"
                }
            }
            Screen::Topics => {
                if layout_mode == LayoutMode::Compact {
                    "wheel/click • / • n • T/S/N • C • e • g • o • : • ?"
                } else {
                    "j/k or wheel select • click inspect • / search • n provider node • T/S/N macros • C call macro • e timeline • g topology • o recorder • : palette • ? help"
                }
            }
            Screen::Services => {
                if layout_mode == LayoutMode::Compact {
                    "wheel/click • / • T/S/N • C • e • g • o • : • ?"
                } else {
                    "j/k or wheel select • click inspect • / search • T/S/N macros • C call macro • e timeline • g topology • o recorder • : palette • ? help"
                }
            }
            Screen::ServiceCall => {
                if layout_mode == LayoutMode::Compact {
                    "hints/resp wheel • i • a • y • F5 • ?"
                } else {
                    "hint list wheel/click • response wheel/PgUp/PgDn • i edit • a apply • y export • R reset • F5 send • : palette • ? help"
                }
            }
            Screen::Actions => {
                if layout_mode == LayoutMode::Compact {
                    "wheel/click • / • c • e • : • ?"
                } else {
                    "j/k or wheel select • click inspect • / search • c send goal • e timeline • : palette • ? help"
                }
            }
            Screen::ActionCall => {
                if layout_mode == LayoutMode::Compact {
                    "hints/resp wheel • i • a • x • F5 • ?"
                } else {
                    "hint list wheel/click • response wheel/PgUp/PgDn • i edit • a apply • R reset • F5 send • x cancel • : palette • ? help"
                }
            }
            Screen::Context => {
                if layout_mode == LayoutMode::Compact {
                    "PgUp/PgDn • r • e • : • ?"
                } else {
                    "PgUp/PgDn scroll • r refresh • ⚠ marks conflicting writers • e timeline • : palette • ? help"
                }
            }
            Screen::Nodes => {
                if layout_mode == LayoutMode::Compact {
                    "wheel/click • / • T/S/N • C • e • g • o • : • ?"
                } else {
                    "j/k or wheel select • click inspect • / search • T/S/N macros • C call macro • e timeline • g topology • o recorder • : palette • ? help"
                }
            }
            Screen::Zenoh => {
                if layout_mode == LayoutMode::Compact {
                    "wheel/click • / • T/S/N • C • e • : • ?"
                } else {
                    "j/k or wheel select • click inspect • / search • T/S/N macros • C call macro • e timeline • : palette • advanced diagnostics • ? help"
                }
            }
            Screen::Telemetry => {
                if layout_mode == LayoutMode::Compact {
                    "approx client-observed latency • r • e • : • ?"
                } else {
                    "approx client-observed latency sparklines • r refresh • e timeline • : palette • advanced diagnostics • ? help"
                }
            }
        },
    }
}

fn render_command_palette_overlay(frame: &mut Frame<'_>, app: &App, mouse: &mut MouseUiState) {
    let area = centered_rect(frame.area(), 72, 60);
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Command Palette (`Enter`/click run, wheel select)");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(4),
            Constraint::Min(7),
        ])
        .split(inner);

    let query = if app.command_palette_query().is_empty() {
        ":".to_string()
    } else {
        format!(":{}", app.command_palette_query())
    };
    let query_widget =
        Paragraph::new(query).block(Block::default().borders(Borders::ALL).title("Filter"));
    frame.render_widget(query_widget, layout[0]);

    let detail_lines = match app.command_palette_selected_action() {
        Some(action) => {
            let metadata = action.metadata();
            let mut lines = vec![Line::from(format!("Selected: {}", metadata.title))];
            if let Some(shortcut) = metadata.shortcut_hint {
                lines.push(Line::from(format!("Shortcut: {}", shortcut)));
            }
            lines.push(Line::from(metadata.description));
            lines
        }
        None => vec![
            Line::from("No action matches the current query."),
            Line::from("Try a shorter keyword or press Backspace to clear the filter."),
        ],
    };
    let detail_widget = Paragraph::new(detail_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Details")),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(detail_widget, layout[1]);

    let actions = app.command_palette_actions();
    if actions.is_empty() {
        let empty = Paragraph::new(vec![
            Line::from("No actions match current filter."),
            Line::from("Available actions update automatically as screen state changes."),
        ])
        .block(Block::default().borders(Borders::ALL).title("Actions"))
        .wrap(Wrap { trim: false });
        frame.render_widget(empty, layout[2]);
        return;
    }

    let items: Vec<ListItem<'_>> = actions
        .iter()
        .map(|action| {
            let metadata = action.metadata();
            let scope = match metadata.scope {
                CommandActionScope::Global => "global",
                CommandActionScope::Screen(screen) => screen.title(),
            };
            let mut spans = vec![Span::raw(metadata.title)];
            if let Some(shortcut) = metadata.shortcut_hint {
                spans.push(Span::styled(
                    format!("  <{}>", shortcut),
                    Style::default().fg(Color::Yellow),
                ));
            }
            spans.push(Span::styled(
                format!("  [{}]", scope),
                Style::default().fg(Color::DarkGray),
            ));
            ListItem::new(Line::from(spans))
        })
        .collect();

    let mut state = ListState::default();
    state.select(app.command_palette_selected_index());
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("Actions ({}) | `click`/`Enter` run", actions.len())),
        )
        .highlight_style(Style::default().bg(Color::Blue))
        .highlight_symbol(">> ");
    frame.render_stateful_widget(list, layout[2], &mut state);
    register_list_rows(
        mouse,
        layout[2],
        state.offset(),
        actions.len(),
        ScrollTarget::CommandPaletteList,
        MouseTarget::CommandPaletteAction,
    );
}

fn render_event_timeline_overlay(frame: &mut Frame<'_>, app: &App, mouse: &mut MouseUiState) {
    let area = centered_rect(frame.area(), 88, 78);
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Diagnostics Timeline (`click` inspect, wheel select)");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Min(10)])
        .split(inner);
    let content = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(56), Constraint::Percentage(44)])
        .split(layout[1]);

    let header = Paragraph::new(vec![
        Line::from(format!(
            "Retention: last {} events | visible {} / {}",
            app.event_timeline_retention_limit(),
            app.filtered_event_entries().len(),
            app.event_log().len()
        )),
        Line::from(format!(
            "Filters: severity={} | source={} | mouse: click inspect • wheel select | keys: s severity • o source • c clear • Esc close",
            app.event_timeline_severity_filter().label(),
            app.event_timeline_source_filter().label()
        )),
    ])
    .block(Block::default().borders(Borders::ALL).title("Summary"))
    .wrap(Wrap { trim: false });
    frame.render_widget(header, layout[0]);

    let entries = app.filtered_event_entries();
    if entries.is_empty() {
        let empty = Paragraph::new(vec![
            Line::from("No events match the current filters."),
            Line::from("Press `c` to clear filters or wait for new events."),
        ])
        .block(Block::default().borders(Borders::ALL).title("Timeline"))
        .wrap(Wrap { trim: false });
        frame.render_widget(empty, content[0]);

        let detail = Paragraph::new(vec![
            Line::from("No event selected."),
            Line::from("Adjust severity/source filters to inspect retained diagnostics."),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Details")),
        )
        .wrap(Wrap { trim: false });
        frame.render_widget(detail, content[1]);
        return;
    }

    let items: Vec<ListItem<'_>> = entries
        .iter()
        .map(|entry| {
            ListItem::new(Line::from(format!(
                "#{:03} [{}] [{}] {} {}",
                entry.sequence,
                entry.severity.label(),
                entry.source.label(),
                format_duration(entry.observed_at.elapsed()),
                entry.message
            )))
        })
        .collect();
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("Timeline ({}) | `click` inspect", entries.len())),
        )
        .highlight_style(Style::default().bg(Color::Blue))
        .highlight_symbol(">> ");
    let mut state = ListState::default();
    state.select(app.event_timeline_selected_index());
    frame.render_stateful_widget(list, content[0], &mut state);
    register_list_rows(
        mouse,
        content[0],
        state.offset(),
        entries.len(),
        ScrollTarget::EventTimelineList,
        MouseTarget::EventTimelineRow,
    );

    let detail_lines = match app.selected_filtered_event_entry() {
        Some(entry) => vec![
            Line::from(format!("Event: #{:03}", entry.sequence)),
            Line::from(format!("Severity: {}", entry.severity.label())),
            Line::from(format!("Source: {}", entry.source.label())),
            Line::from(format!(
                "Age: {}",
                format_duration(entry.observed_at.elapsed())
            )),
            Line::from(""),
            Line::from("Message:"),
            Line::from(entry.message.clone()),
        ],
        None => vec![
            Line::from("No event selected."),
            Line::from("Use `j/k` to inspect retained events."),
        ],
    };
    let detail = Paragraph::new(detail_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Details")),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(detail, content[1]);
}

/// Color a log level label, matching the event-timeline severity colors.
///
/// Covers all three enums that can reach here: `aorta.log.AortaLogLevel` (the
/// preferred source), `foxglove.LogLevel` (`WARNING`/`FATAL`, used when a
/// producer such as the vlog bridge sets no `aorta_level`), and the legacy
/// `aorta.sys.LogLevel`.
fn log_level_color(level: &str) -> Color {
    match level.to_ascii_uppercase().as_str() {
        "FATAL" => Color::Magenta,
        "ERROR" => Color::Red,
        "WARN" | "WARNING" => Color::Yellow,
        "INFO" => Color::Green,
        "DEBUG" => Color::Cyan,
        "TRACE" => Color::Gray,
        _ => Color::White,
    }
}

/// Live `/log` stream viewer overlay (Feature C4). A centered overlay listing
/// the bounded `log_rows` deque with a selected-row detail pane — the literal
/// analog of [`render_event_timeline_overlay`], but fed by the live `/log`
/// subscription instead of the in-process event log.
fn render_log_stream_overlay(frame: &mut Frame<'_>, app: &App, mouse: &mut MouseUiState) {
    let area = centered_rect(frame.area(), 88, 78);
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Live /log Stream (`click` inspect, wheel select, Esc/L close)");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(10)])
        .split(inner);
    let content = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(layout[1]);

    let header = Paragraph::new(vec![Line::from(format!(
        "Subscribed to aorta/{}/pub/log | retained {} (cap {}) | keys: j/k select • Esc/L close",
        app.group(),
        app.log_rows().len(),
        crate::app::LOG_STREAM_CAPACITY,
    ))])
    .block(Block::default().borders(Borders::ALL).title("Summary"))
    .wrap(Wrap { trim: false });
    frame.render_widget(header, layout[0]);

    let rows = app.log_rows();
    if rows.is_empty() {
        let empty = Paragraph::new(vec![
            Line::from("No log records received yet."),
            Line::from("Records appear here as nodes publish to the /log topic."),
        ])
        .block(Block::default().borders(Borders::ALL).title("Logs"))
        .wrap(Wrap { trim: false });
        frame.render_widget(empty, content[0]);

        let detail = Paragraph::new(vec![Line::from("No log record selected.")])
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(read_only_title("Details")),
            )
            .wrap(Wrap { trim: false });
        frame.render_widget(detail, content[1]);
        return;
    }

    let items: Vec<ListItem<'_>> = rows
        .iter()
        .map(|row| {
            let level = format!("{:<5}", row.level);
            ListItem::new(Line::from(vec![
                Span::styled(
                    level,
                    Style::default()
                        .fg(log_level_color(&row.level))
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(" "),
                Span::styled(
                    format!("{:<16}", truncate_with_ellipsis(&row.node, 16)),
                    Style::default().fg(Color::Cyan),
                ),
                Span::raw(" "),
                Span::raw(truncate_with_ellipsis(&row.message, 80)),
            ]))
        })
        .collect();
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("Logs ({}) | `click` inspect", rows.len())),
        )
        .highlight_style(Style::default().bg(Color::Blue))
        .highlight_symbol(">> ");
    let mut state = ListState::default();
    state.select(app.log_stream_selected_index());
    frame.render_stateful_widget(list, content[0], &mut state);
    register_list_rows(
        mouse,
        content[0],
        state.offset(),
        rows.len(),
        ScrollTarget::LogStreamList,
        MouseTarget::LogStreamRow,
    );

    let detail_lines = match app.selected_log_row() {
        Some(row) => vec![
            Line::from(vec![
                Span::raw("Level: "),
                Span::styled(
                    row.level.clone(),
                    Style::default().fg(log_level_color(&row.level)),
                ),
            ]),
            Line::from(format!("Node: {}", row.node)),
            Line::from(format!("Target: {}", row.target)),
            Line::from(format!("Timestamp (ns): {}", row.timestamp_ns)),
            Line::from(""),
            Line::from("Message:"),
            Line::from(row.message.clone()),
        ],
        None => vec![Line::from("No log record selected.")],
    };
    let detail = Paragraph::new(detail_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Details")),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(detail, content[1]);
}

fn render_topology_overlay(frame: &mut Frame<'_>, app: &App) {
    let area = centered_rect(frame.area(), 88, 78);
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Topology Explorer");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(10)])
        .split(inner);

    let (summary_lines, topology_lines) = topology_overlay_sections(app);
    let summary = Paragraph::new(summary_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Summary")),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(summary, layout[0]);

    let topology = Paragraph::new(topology_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Topology")),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(topology, layout[1]);
}

fn render_recorder_overlay(frame: &mut Frame<'_>, app: &App) {
    let area = centered_rect(frame.area(), 88, 76);
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Recorder Launchpad");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Length(8),
            Constraint::Min(8),
        ])
        .split(inner);

    let Ok(plan) = app.recorder_launch_plan() else {
        let empty = Paragraph::new(vec![
            Line::from("Recorder launchpad is unavailable right now."),
            Line::from("Return to Overview, Topics, Services, Service Call, or Nodes."),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Summary")),
        )
        .wrap(Wrap { trim: false });
        frame.render_widget(empty, layout[0]);
        return;
    };

    let summary = Paragraph::new(recorder_summary_lines(&plan))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Summary")),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(summary, layout[0]);

    let commands = Paragraph::new(recorder_command_lines(&plan))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Commands")),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(commands, layout[1]);

    let notes = Paragraph::new(recorder_note_lines(&plan))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Notes")),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(notes, layout[2]);
}

fn render_node_comparison_overlay(frame: &mut Frame<'_>, app: &App) {
    let area = centered_rect(frame.area(), 88, 76);
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Node Comparison");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(10)])
        .split(inner);

    let Some(pair) = app.selected_node_comparison_pair() else {
        let empty = Paragraph::new(vec![
            Line::from("Node comparison is unavailable right now."),
            Line::from("Set an anchor with `x`, select another node, then press `c`."),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Summary")),
        )
        .wrap(Wrap { trim: false });
        frame.render_widget(empty, layout[0]);
        return;
    };

    let summary = Paragraph::new(vec![
        Line::from(format!(
            "Anchor: {} | Current: {}",
            pair.anchor.name, pair.current.name
        )),
        Line::from(format!(
            "Differences: {}",
            node_comparison_difference_summary(pair.anchor, pair.current)
        )),
        Line::from("Keys: Esc/c close • x retarget anchor from the Nodes list".to_string()),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(read_only_title("Summary")),
    )
    .wrap(Wrap { trim: false });
    frame.render_widget(summary, layout[0]);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(layout[1]);

    render_detail_panel(
        frame,
        columns[0],
        &format!("Anchor • {}", pair.anchor.name),
        node_comparison_lines(app, pair.anchor, true),
    );
    render_detail_panel(
        frame,
        columns[1],
        &format!("Current • {}", pair.current.name),
        node_comparison_lines(app, pair.current, false),
    );
}

fn render_bookmarks_overlay(frame: &mut Frame<'_>, app: &App, mouse: &mut MouseUiState) {
    let area = centered_rect(frame.area(), 76, 62);
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!("Bookmarks | group={}", app.group()));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(5),
            Constraint::Min(8),
        ])
        .split(inner);

    let header = Paragraph::new(vec![
        Line::from(format!(
            "Pinned resources in current group: {}",
            app.current_group_bookmark_count()
        )),
        Line::from("Enter/click open • wheel select • d/Delete remove • Esc/b close"),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(read_only_title("Summary")),
    )
    .wrap(Wrap { trim: false });
    frame.render_widget(header, layout[0]);

    let detail_lines = match app.selected_group_bookmark() {
        Some(bookmark) => vec![
            Line::from(format!("Kind: {}", bookmark.title())),
            Line::from(format!("Label: {}", bookmark.label)),
            Line::from(format!(
                "Availability: {}",
                if app.is_bookmark_available(bookmark) {
                    "available in current snapshot"
                } else {
                    "missing from current snapshot"
                }
            )),
            Line::from(format!(
                "Open target: {}",
                bookmark_target_screen_label(bookmark)
            )),
        ],
        None => vec![
            Line::from("No bookmark selected.".to_string()),
            Line::from("Use `p` on Topics, Services, Service Call, or Nodes to pin one."),
        ],
    };
    let detail = Paragraph::new(detail_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Details")),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(detail, layout[1]);

    let bookmarks = app.current_group_bookmarks();
    if bookmarks.is_empty() {
        let empty = Paragraph::new(vec![
            Line::from("No bookmarks saved for this group."),
            Line::from("Pin the current topic, service, or node with `p`."),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(read_only_title("Bookmarks")),
        )
        .wrap(Wrap { trim: false });
        frame.render_widget(empty, layout[2]);
        return;
    }

    let items: Vec<ListItem<'_>> = bookmarks
        .into_iter()
        .map(|bookmark| {
            let availability = if app.is_bookmark_available(bookmark) {
                "ready"
            } else {
                "missing"
            };
            ListItem::new(Line::from(vec![
                Span::raw(format!("★ {} ", bookmark.label)),
                Span::styled(
                    format!("[{} | {}]", bookmark.title(), availability),
                    Style::default().fg(Color::DarkGray),
                ),
            ]))
        })
        .collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(format!(
            "Bookmarks ({}) | `click`/`Enter` open",
            app.current_group_bookmark_count()
        )))
        .highlight_style(Style::default().bg(Color::Blue))
        .highlight_symbol(">> ");
    let mut state = ListState::default();
    state.select(app.bookmark_overlay_selected_index());
    frame.render_stateful_widget(list, layout[2], &mut state);
    register_list_rows(
        mouse,
        layout[2],
        state.offset(),
        app.current_group_bookmark_count(),
        ScrollTarget::BookmarkList,
        MouseTarget::BookmarkRow,
    );
}

fn render_help_overlay(frame: &mut Frame<'_>, app: &App) {
    let area = centered_rect(frame.area(), 84, 80);
    let lines = help_lines(app, LayoutMode::from_width(frame.area().width));
    frame.render_widget(Clear, area);
    let detail = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Help"))
        .wrap(Wrap { trim: false });
    frame.render_widget(detail, area);
}

fn help_lines(app: &App, layout_mode: LayoutMode) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::from(format!(
            "Current screen: {} | Group: {} | Layout: {}",
            app.active_screen().title(),
            app.group(),
            layout_mode.label()
        )),
        Line::from(format!(
            "Advanced mode (expert-only diagnostics): {}",
            if app.is_advanced_mode() {
                "enabled"
            } else {
                "disabled"
            }
        )),
        Line::from(format!(
            "Overview health: {} | Runtime freshness: {}",
            app.overview().health.overall.as_label(),
            format_freshness_with_period(
                app.overview().health.freshness,
                app.overview().health.runtime_collection_period_ms
            )
        )),
        Line::from(""),
        Line::from("Global"),
        Line::from("  q quit"),
        Line::from("  Tab / Shift-Tab switch screens"),
        Line::from("  e open diagnostics timeline"),
        Line::from("  L open live /log stream viewer"),
        Line::from("  g open topology explorer for the current selection"),
        Line::from("  o open recorder launchpad for the current scope"),
        Line::from("  T / S / N jump to Topics / Services / Nodes and enter search"),
        Line::from("  C open Service Call and focus the request editor"),
        Line::from("  b open bookmarks overlay"),
        Line::from("  : open command palette"),
        Line::from("  r refresh snapshot"),
        Line::from("  ? or h toggle help"),
        Line::from("  z toggle advanced mode"),
        Line::from(""),
        Line::from("Mouse"),
        Line::from("  click tabs to switch screens"),
        Line::from("  click list rows to inspect resources; wheel changes list selection"),
        Line::from("  click quick actions or bookmarks to open; click palette actions to run"),
        Line::from("  wheel over Topic Preview or Response to scroll long payloads/replies"),
        Line::from(""),
        Line::from("Modes"),
        Line::from("  NORMAL browse and inspect"),
        Line::from("  TIMELINE inspect retained events with severity/source filters"),
        Line::from("  LOGS browse live /log records streamed from the group"),
        Line::from("  TOPOLOGY inspect a textual ownership/topology summary"),
        Line::from("  RECORDER review recorder commands and export a shell snippet"),
        Line::from("  NODE-COMPARE inspect side-by-side node summary against an anchor"),
        Line::from("  BOOKMARKS open or remove pinned resources"),
        Line::from("  PALETTE filter and run actions"),
        Line::from("  SEARCH filter current list"),
        Line::from("  EDIT modify service-call JSON"),
        Line::from("  HELP shows this overlay"),
        Line::from(""),
        Line::from("Screen Shortcuts"),
    ];

    lines.extend(match app.active_screen() {
        Screen::Overview => vec![
            Line::from("  Enter open selected quick action"),
            Line::from("  1-5 open hot topic shortcut"),
            Line::from("  F1-F5 open hot service shortcut"),
            Line::from("  o open a broad group recorder launchpad"),
            Line::from("  T/S/N macro-jump to a searchable resource list"),
        ],
        Screen::Topics => vec![
            Line::from("  / start search for the topic list"),
            Line::from("  T/S/N macro-jump to a different searchable list"),
            Line::from("  C macro-open Service Call editor from current service selection"),
            Line::from("  g open topology explorer for the selected topic"),
            Line::from("  o open recorder launchpad for the selected topic"),
            Line::from("  p pin or unpin the selected topic"),
            Line::from("  y export the selected preview frame"),
            Line::from("  m collapse/expand runtime detail"),
            Line::from("  d show full schema definition for the selected topic"),
            Line::from("  v cycle decoded/raw/diff preview modes"),
            Line::from("  [ ] navigate recent preview history"),
            Line::from("  PgUp/PgDn scroll preview"),
        ],
        Screen::Services => vec![
            Line::from("  / start search for the service list"),
            Line::from("  T/S/N macro-jump to a different searchable list"),
            Line::from("  C macro-open Service Call editor for the current service selection"),
            Line::from("  g open topology explorer for the selected service"),
            Line::from("  o open recorder launchpad for RPC capture guidance"),
            Line::from("  p pin or unpin the selected service"),
            Line::from("  m collapse/expand runtime detail"),
            Line::from("  d show full request/response schema for the selected service"),
            Line::from("  c open Service Call for the selected service"),
        ],
        Screen::ServiceCall => vec![
            Line::from("  i enter request edit mode"),
            Line::from("  T/S/N macro-jump to a searchable list"),
            Line::from("  o open recorder launchpad for RPC record capture"),
            Line::from("  p pin or unpin the current service"),
            Line::from("  y export the latest response payload"),
            Line::from("  click or wheel on the field list to select a schema hint"),
            Line::from("  wheel or PgUp/PgDn on the response viewer to inspect long replies"),
            Line::from("  a apply selected schema field template"),
            Line::from("  R reset request body to starter JSON"),
            Line::from("  F5 send the current request"),
        ],
        Screen::Actions => vec![
            Line::from("  / start search for the action list"),
            Line::from("  c open Action Call to send a goal to the selected action"),
            Line::from("  inspect action metadata and retained goals"),
            Line::from("  use : to jump via command palette"),
        ],
        Screen::ActionCall => vec![
            Line::from("  i enter goal edit mode"),
            Line::from("  click or wheel on the field list to select a schema hint"),
            Line::from("  wheel or PgUp/PgDn on the result viewer to inspect long replies"),
            Line::from("  a apply selected schema field template"),
            Line::from("  R reset goal body to starter JSON"),
            Line::from("  F5 send the current goal (returns goal_id + goal_secret)"),
            Line::from("  x cancel the last accepted goal (uses goal_id + goal_secret)"),
        ],
        Screen::Nodes => vec![
            Line::from("  / start search for the node list"),
            Line::from("  T/S/N macro-jump to a different searchable list"),
            Line::from("  C macro-open Service Call editor from current service selection"),
            Line::from("  g open topology explorer for the selected node"),
            Line::from("  o open recorder launchpad for node-focused investigation"),
            Line::from("  p pin or unpin the selected node"),
            Line::from("  x set or clear the selected node as comparison anchor"),
            Line::from("  c open side-by-side summary against the comparison anchor"),
            Line::from("  m collapse/expand runtime detail"),
            Line::from("  inspect node identity, inventory, and runtime"),
        ],
        Screen::Context => vec![
            Line::from("  read-only ContextStore listing for the current group"),
            Line::from("  r refresh the snapshot of owner replies"),
            Line::from("  PgUp/PgDn scroll the entry table"),
            Line::from("  ⚠ marks keys written by more than one instance (conflict)"),
        ],
        Screen::Zenoh => vec![
            Line::from("  expert-only diagnostics for router/admin checks"),
            Line::from("  / search router zid / locator / plugin"),
            Line::from("  T/S/N macro-jump back to common resource lists"),
            Line::from("  C macro-open Service Call editor from current service selection"),
            Line::from(
                "  inspect router metrics capability, ACL visibility, and raw admin payload",
            ),
        ],
        Screen::Telemetry => vec![
            Line::from("  approximate, client-observed latency sparklines + p50/p95/p99"),
            Line::from(
                "  percentiles are bucket-bounded (factor-of-2) and accumulated client-side",
            ),
            Line::from("  NOT the robot's internal per-stage latency"),
            Line::from("  open node/service detail to feed the per-resource history rings"),
        ],
    });

    lines.extend([
        Line::from(""),
        Line::from("Provenance"),
        Line::from("  exact       telemetry exact ownership/runtime"),
        Line::from("  inferred    admin ownership fallback"),
        Line::from("  unavailable capability or data path missing"),
        Line::from(""),
        Line::from("Press Esc or ? to close help."),
    ]);

    lines
}

fn centered_rect(area: Rect, width_percent: u16, height_percent: u16) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - height_percent) / 2),
            Constraint::Percentage(height_percent),
            Constraint::Percentage((100 - height_percent) / 2),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_percent) / 2),
            Constraint::Percentage(width_percent),
            Constraint::Percentage((100 - width_percent) / 2),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn register_tab_regions(mouse: &mut MouseUiState, area: Rect, screens: &[Screen]) {
    if screens.is_empty() {
        return;
    }
    let inner = inner_block_area(area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }
    let mut x = inner.x;
    let y = inner.y;
    let left_padding = 1u16;
    let right_padding = 1u16;
    let divider_width = 1u16;

    for (index, screen) in screens.iter().copied().enumerate() {
        let title_width = screen.title().chars().count().min(u16::MAX as usize) as u16;
        let tab_width = left_padding
            .saturating_add(title_width)
            .saturating_add(right_padding);
        if x.saturating_add(tab_width) > inner.x.saturating_add(inner.width) {
            break;
        }
        mouse.register_click(Rect::new(x, y, tab_width, 1), MouseTarget::Tab(screen));
        x = x.saturating_add(tab_width);
        if index + 1 < screens.len() {
            x = x.saturating_add(divider_width);
        }
    }
}

fn register_list_rows<F>(
    mouse: &mut MouseUiState,
    area: Rect,
    offset: usize,
    row_count: usize,
    scroll_target: ScrollTarget,
    target_at: F,
) where
    F: Fn(usize) -> MouseTarget,
{
    register_row_regions(mouse, area, 0, offset, row_count, scroll_target, target_at);
}

fn register_table_rows<F>(
    mouse: &mut MouseUiState,
    area: Rect,
    offset: usize,
    row_count: usize,
    scroll_target: ScrollTarget,
    target_at: F,
) where
    F: Fn(usize) -> MouseTarget,
{
    register_row_regions(mouse, area, 1, offset, row_count, scroll_target, target_at);
}

fn register_row_regions<F>(
    mouse: &mut MouseUiState,
    area: Rect,
    top_offset: u16,
    offset: usize,
    row_count: usize,
    scroll_target: ScrollTarget,
    target_at: F,
) where
    F: Fn(usize) -> MouseTarget,
{
    let inner = inner_block_area(area);
    if inner.width == 0 || inner.height <= top_offset {
        return;
    }

    mouse.register_scroll(inner, scroll_target);

    let start_y = inner.y.saturating_add(top_offset);
    let max_visible_rows = usize::from(inner.height.saturating_sub(top_offset));
    let displayed_rows = row_count.saturating_sub(offset).min(max_visible_rows);
    for visible_offset in 0..displayed_rows {
        let index = offset + visible_offset;
        let row_area = Rect::new(
            inner.x,
            start_y.saturating_add(visible_offset as u16),
            inner.width,
            1,
        );
        mouse.register_click(row_area, target_at(index));
    }
}

fn register_scrollable_panel(mouse: &mut MouseUiState, area: Rect, scroll_target: ScrollTarget) {
    mouse.register_scroll(inner_block_area(area), scroll_target);
}

fn inner_block_area(area: Rect) -> Rect {
    Block::default().borders(Borders::ALL).inner(area)
}

fn render_request_editor(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let title = service_call_request_editor_title(app);
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    let paragraph = Paragraph::new(app.service_call_request_json())
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);

    if app.active_screen() == Screen::ServiceCall
        && app.is_service_call_editing()
        && inner.width > 0
    {
        let (line, col) = app.service_call_cursor_line_col();
        let x = inner.x + (col as u16).min(inner.width.saturating_sub(1));
        let y = inner.y + (line as u16).min(inner.height.saturating_sub(1));
        frame.set_cursor_position((x, y));
    }
}

fn render_schema_hints(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    match app.service_call().schema() {
        DetailPane::Loaded(schema) if !schema.request_model.fields.is_empty() => {
            let layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(9), Constraint::Length(8)])
                .split(area);

            render_detail_panel(
                frame,
                layout[0],
                "Request Hints (selected field detail, read-only)",
                schema_hint_lines(schema, app.service_call_selected_hint_index()),
            );

            let items: Vec<ListItem<'_>> = schema
                .request_model
                .fields
                .iter()
                .map(|field| ListItem::new(format_schema_field_summary(field)))
                .collect();
            let list = List::new(items)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Field List (`click`/wheel select, `a` apply)"),
                )
                .highlight_style(Style::default().bg(Color::Blue))
                .highlight_symbol(">> ");
            let mut state = ListState::default();
            state.select(app.service_call_selected_hint_index());
            frame.render_stateful_widget(list, layout[1], &mut state);
            if register_mouse {
                register_list_rows(
                    mouse,
                    layout[1],
                    state.offset(),
                    schema.request_model.fields.len(),
                    ScrollTarget::ServiceCallHintList,
                    MouseTarget::ServiceCallHintRow,
                );
            }
        }
        DetailPane::Loaded(schema) => {
            render_detail_panel(
                frame,
                area,
                "Request Hints (read-only)",
                schema_hint_lines(schema, app.service_call_selected_hint_index()),
            );
        }
        DetailPane::Error(message) => {
            render_detail_panel(
                frame,
                area,
                "Request Hints (read-only)",
                vec![
                    Line::from("Hints unavailable.".to_string()),
                    Line::from(format!("Error: {}", message)),
                    Line::from(
                        "Open Service Call from the Services screen again after schema discovery recovers."
                            .to_string(),
                    ),
                ],
            );
        }
        DetailPane::Empty => {
            render_detail_panel(
                frame,
                area,
                "Request Hints (read-only)",
                vec![
                    Line::from("Hints unavailable until schema loads.".to_string()),
                    Line::from("Use `c` from the Services screen.".to_string()),
                ],
            );
        }
    }
}

fn render_service_call_response(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let lines = match app.service_call().response() {
        DetailPane::Loaded(response) => response_lines(response),
        DetailPane::Error(message) => vec![
            Line::from("Last call failed.".to_string()),
            Line::from(format!("Error: {}", message)),
            Line::from(
                "Check request JSON, provider availability, and service timeout before retrying."
                    .to_string(),
            ),
        ],
        DetailPane::Empty => vec![
            Line::from("No reply received yet.".to_string()),
            Line::from("Press `F5` to send the current request.".to_string()),
            Line::from("Press `i` to edit the request body, or `a` to merge a field template."),
        ],
    };
    render_scrolling_detail_panel(
        frame,
        area,
        &read_only_scrollable_title("Response"),
        lines,
        app.service_call_response_scroll(),
    );
    if register_mouse {
        register_scrollable_panel(mouse, area, ScrollTarget::ServiceCallResponse);
    }
}

fn service_call_summary_lines(app: &App) -> Vec<Line<'_>> {
    match app.service_call().schema() {
        DetailPane::Loaded(schema) => {
            let selected_hint = app
                .service_call_selected_hint()
                .map(|field| field.path.as_str())
                .unwrap_or("none");
            vec![
                Line::from(format!("Service: {}", schema.service_name)),
                Line::from(bookmark_status_line(
                    app.is_current_selection_bookmarked().unwrap_or(false),
                )),
                Line::from(format!("Key: {}", schema.key_expr)),
                Line::from(format!(
                    "Providers: {} endpoint(s), {} node(s)",
                    schema.provider_endpoint_count, schema.candidate_node_count
                )),
                Line::from(format!("Schema hash: {}", schema.schema_hash)),
                Line::from(format!(
                    "Request root: {} | Response root: {}",
                    schema.request_root_type.as_deref().unwrap_or("n/a"),
                    schema.response_root_type.as_deref().unwrap_or("n/a")
                )),
                Line::from(format!(
                    "Editor: {} | selected field: {} | fields: {}",
                    if app.is_service_call_editing() {
                        "editing"
                    } else {
                        "view"
                    },
                    selected_hint,
                    schema.request_model.fields.len()
                )),
            ]
        }
        DetailPane::Error(message) => vec![
            Line::from(format!(
                "Service: {}",
                app.service_call_target().unwrap_or("(none)")
            )),
            Line::from(bookmark_status_line(
                app.is_current_selection_bookmarked().unwrap_or(false),
            )),
            Line::from("Service-call schema detail is currently unavailable.".to_string()),
            Line::from(format!("Error: {}", message)),
            Line::from("Use `c` from Services again after schema discovery recovers.".to_string()),
        ],
        DetailPane::Empty => vec![
            Line::from(format!(
                "Service: {}",
                app.service_call_target().unwrap_or("(none)")
            )),
            Line::from(bookmark_status_line(
                app.is_current_selection_bookmarked().unwrap_or(false),
            )),
            Line::from("Service-call schema detail is not loaded yet.".to_string()),
            Line::from("Select a service in `Services` and press `c`.".to_string()),
        ],
    }
}

fn service_call_request_editor_title(app: &App) -> String {
    let service = app.service_call_target().unwrap_or("(none)");
    if app.is_service_call_editing() {
        format!("Request JSON | {service} | editing (`F5` send, `Esc` finish)")
    } else {
        format!("Request JSON | {service} | view (`i` edit, `a` apply, `R` reset, `F5` send)")
    }
}

fn render_topic_table(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &str,
    app: &App,
    list: &SelectableList<crate::data::TopicRow>,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let rows: Vec<Row<'_>> = list
        .visible_rows()
        .into_iter()
        .map(|row| {
            Row::new(vec![
                Cell::from(bookmark_prefixed_label(
                    app.is_topic_bookmarked(&row.name),
                    &row.name,
                )),
                Cell::from(row.provider_endpoint_count.to_string()),
                Cell::from(format_optional_count(row.publisher_node_count)),
                Cell::from(format_optional_count(row.subscriber_node_count)),
                schema_availability_cell(row.schema_availability),
                health_cell(row.health, row.capability),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(50),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(7),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec!["Name", "EP", "Pub", "Sub", "Schema", "State"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(resource_table_title(title, list, "name")),
    )
    .row_highlight_style(Style::default().bg(Color::Blue))
    .highlight_symbol(">> ");
    let mut state = TableState::default();
    state.select(list.selected_index());
    frame.render_stateful_widget(table, area, &mut state);
    if register_mouse {
        register_table_rows(
            mouse,
            area,
            state.offset(),
            list.visible_row_count(),
            ScrollTarget::TopicList,
            MouseTarget::TopicRow,
        );
    }
}

fn render_service_table(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &str,
    app: &App,
    list: &SelectableList<crate::data::ServiceRow>,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let rows: Vec<Row<'_>> = list
        .visible_rows()
        .into_iter()
        .map(|row| {
            Row::new(vec![
                Cell::from(bookmark_prefixed_label(
                    app.is_service_bookmarked(&row.name),
                    &row.name,
                )),
                Cell::from(row.provider_endpoint_count.to_string()),
                Cell::from(format_optional_count(row.provider_node_count)),
                Cell::from(format_optional_count(row.client_node_count)),
                health_cell(row.health, row.capability),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(50),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec!["Name", "EP", "Prov", "Cli", "State"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(resource_table_title(title, list, "name")),
    )
    .row_highlight_style(Style::default().bg(Color::Blue))
    .highlight_symbol(">> ");
    let mut state = TableState::default();
    state.select(list.selected_index());
    frame.render_stateful_widget(table, area, &mut state);
    if register_mouse {
        register_table_rows(
            mouse,
            area,
            state.offset(),
            list.visible_row_count(),
            ScrollTarget::ServiceList,
            MouseTarget::ServiceRow,
        );
    }
}

fn render_zenoh_router_table(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    mouse: &mut MouseUiState,
    register_mouse: bool,
) {
    let rows: Vec<Row<'_>> = app
        .zenoh_routers()
        .visible_rows()
        .into_iter()
        .map(|router| {
            Row::new(vec![
                Cell::from(zenoh_router_primary_label(router)),
                Cell::from(
                    router
                        .session_count
                        .map(|count| count.to_string())
                        .unwrap_or_else(|| "n/a".to_string()),
                ),
                zenoh_metrics_cell(router.metrics_available),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(72),
            Constraint::Length(8),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec!["Router", "Sess", "Metrics"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(resource_table_title(
                "Zenoh Routers (expert-only)",
                app.zenoh_routers(),
                "zid/locator/plugin",
            )),
    )
    .row_highlight_style(Style::default().bg(Color::Blue))
    .highlight_symbol(">> ");
    let mut state = TableState::default();
    state.select(app.zenoh_routers().selected_index());
    frame.render_stateful_widget(table, area, &mut state);
    if register_mouse {
        register_table_rows(
            mouse,
            area,
            state.offset(),
            app.zenoh_routers().visible_row_count(),
            ScrollTarget::ZenohRouterList,
            MouseTarget::ZenohRouterRow,
        );
    }
}

fn zenoh_router_primary_label(router: &ZenohRouterRow) -> String {
    match router.locators.first() {
        Some(locator) => format!("{} [{}] @ {}", router.zid, router.whatami, locator),
        None => format!("{} [{}]", router.zid, router.whatami),
    }
}

fn zenoh_metrics_cell<'a>(metrics_available: bool) -> Cell<'a> {
    let (label, color) = if metrics_available {
        ("avail", Color::Green)
    } else {
        ("n/a", Color::Yellow)
    };
    Cell::from(Span::styled(
        label.to_string(),
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    ))
}

fn zenoh_metrics_capability_note(router: &ZenohRouterRow) -> String {
    if router.metrics_available {
        "Metrics note: router metrics are visible through admin space for this session.".to_string()
    } else {
        "Metrics note: metrics are not visible. Check router metrics exposure, admin-space ACL, and router reachability."
            .to_string()
    }
}

fn resource_table_title<T>(title: &str, list: &SelectableList<T>, search_target: &str) -> String {
    let filter = if list.has_query() {
        format!(" • filter={}", list.query())
    } else {
        String::new()
    };
    format!(
        "{} [{} visible / {} total{} • click inspect • wheel select • `/` search {}]",
        title,
        list.visible_row_count(),
        list.total_row_count(),
        filter,
        search_target
    )
}

fn read_only_title(base: &str) -> String {
    format!("{base} (read-only)")
}

fn read_only_scrollable_title(base: &str) -> String {
    format!("{base} (read-only, wheel scroll)")
}

fn bookmark_prefixed_label(bookmarked: bool, label: &str) -> String {
    if bookmarked {
        format!("★ {}", label)
    } else {
        label.to_string()
    }
}

fn node_primary_label(app: &App, node: &crate::data::NodeRow) -> String {
    let bookmarked = app.is_node_bookmarked(&node.node_id);
    let anchor = app.is_node_compare_anchor(&node.node_id);
    match (anchor, bookmarked) {
        (true, true) => format!("⇄ ★ {}", node.name),
        (true, false) => format!("⇄ {}", node.name),
        (false, true) => format!("★ {}", node.name),
        (false, false) => node.name.clone(),
    }
}

fn bookmark_status_line(bookmarked: bool) -> String {
    format!(
        "Pinned: {}",
        if bookmarked {
            "yes (`p` to unpin)"
        } else {
            "no (`p` to pin)"
        }
    )
}

fn optional_count_label(value: Option<usize>) -> String {
    value
        .map(|count| count.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

fn optional_period_label(value: Option<u32>) -> String {
    value
        .map(|period| format!("~{} ms", period))
        .unwrap_or_else(|| "unavailable".to_string())
}

fn node_comparison_lines(
    app: &App,
    node: &crate::data::NodeRow,
    is_anchor: bool,
) -> Vec<Line<'static>> {
    vec![
        Line::from(format!(
            "Role: {}",
            if is_anchor {
                "comparison anchor"
            } else {
                "current selection"
            }
        )),
        Line::from(bookmark_status_line(app.is_node_bookmarked(&node.node_id))),
        Line::from(format!(
            "Comparison anchor: {}",
            if app.is_node_compare_anchor(&node.node_id) {
                "yes"
            } else {
                "no"
            }
        )),
        Line::from(format!("Node: {}", node.name)),
        Line::from(format!("Group: {}", node.group)),
        Line::from(format!("Node ID: {}", node.node_id)),
        Line::from(format!(
            "Discovery: {}",
            node_discovery_source_label(node.discovery_source)
        )),
        Line::from(format!("Health: {}", node.health.as_label())),
        Line::from(format!("Capability: {}", node.capability.as_label())),
        Line::from(format!(
            "Freshness: {}",
            format_freshness_with_period(node.freshness, node.runtime_collection_period_ms)
        )),
        Line::from(format!(
            "Runtime period: {}",
            optional_period_label(node.runtime_collection_period_ms)
        )),
        Line::from(format!(
            "Endpoints / Topics / Services: {} / {} / {}",
            optional_count_label(node.endpoint_count),
            optional_count_label(node.topic_count),
            optional_count_label(node.service_count)
        )),
        Line::from("Status:".to_string()),
        Line::from(node.status_summary.clone()),
    ]
}

fn node_comparison_difference_summary(
    anchor: &crate::data::NodeRow,
    current: &crate::data::NodeRow,
) -> String {
    let mut differences = Vec::new();
    if anchor.group != current.group {
        differences.push("group");
    }
    if anchor.discovery_source != current.discovery_source {
        differences.push("discovery");
    }
    if anchor.health != current.health {
        differences.push("health");
    }
    if anchor.capability != current.capability {
        differences.push("capability");
    }
    if anchor.freshness != current.freshness {
        differences.push("freshness");
    }
    if anchor.runtime_collection_period_ms != current.runtime_collection_period_ms {
        differences.push("runtime period");
    }
    if anchor.endpoint_count != current.endpoint_count {
        differences.push("endpoint count");
    }
    if anchor.topic_count != current.topic_count {
        differences.push("topic count");
    }
    if anchor.service_count != current.service_count {
        differences.push("service count");
    }

    if differences.is_empty() {
        "same high-level health and resource-count summary".to_string()
    } else {
        differences.join(", ")
    }
}

fn topology_overlay_sections(app: &App) -> (Vec<Line<'static>>, Vec<Line<'static>>) {
    match app.active_screen() {
        Screen::Topics => topic_topology_sections(app.topics().selected_row(), app.topic_detail()),
        Screen::Services => {
            service_topology_sections(app.services().selected_row(), app.service_detail())
        }
        Screen::Nodes => node_topology_sections(app.nodes().selected_row(), app.node_detail()),
        Screen::Overview
        | Screen::ServiceCall
        | Screen::Actions
        | Screen::ActionCall
        | Screen::Context
        | Screen::Zenoh
        | Screen::Telemetry => topology_empty_sections(
            "Topology explorer is available from Topics, Services, and Nodes.",
        ),
    }
}

fn topology_empty_sections(message: impl Into<String>) -> (Vec<Line<'static>>, Vec<Line<'static>>) {
    let message = message.into();
    (
        vec![
            Line::from("Root: unavailable".to_string()),
            Line::from("Keys: Esc/g close".to_string()),
        ],
        vec![
            Line::from(message),
            Line::from(
                "Use the current screen selection as the topology root for this lightweight view."
                    .to_string(),
            ),
        ],
    )
}

fn topic_topology_sections(
    selected: Option<&crate::data::TopicRow>,
    detail: &DetailPane<TopicDetailData>,
) -> (Vec<Line<'static>>, Vec<Line<'static>>) {
    let Some(selected) = selected else {
        return topology_empty_sections("No topic is selected.");
    };

    match detail {
        DetailPane::Loaded(detail) => {
            let mut topology = vec![
                Line::from(format!("topic {}", detail.name)),
                Line::from(format!("  key: {}", detail.key_expr)),
                Line::from(format!(
                    "  schema: {}{}",
                    detail.schema_availability.as_label(),
                    detail
                        .schema_availability
                        .hash()
                        .map(|hash| format!(" ({})", hash))
                        .unwrap_or_default()
                )),
            ];
            if let Some(note) = detail.schema_note.as_deref() {
                topology.push(Line::from(format!("  schema note: {}", note)));
            }
            push_topology_list(
                &mut topology,
                "provider endpoints",
                detail.provider_endpoint_provenance,
                &detail.provider_endpoint_ids,
                None,
            );
            push_topology_list(
                &mut topology,
                "candidate nodes",
                detail.candidate_node_provenance,
                &detail.candidate_node_names,
                detail.candidate_nodes_note.as_deref(),
            );
            push_topology_list(
                &mut topology,
                "publisher nodes",
                detail.publisher_node_provenance,
                &detail.publisher_node_names,
                detail.publisher_nodes_note.as_deref(),
            );
            push_topology_list(
                &mut topology,
                "subscriber nodes",
                detail.subscriber_node_provenance,
                &detail.subscriber_node_names,
                detail.subscriber_nodes_note.as_deref(),
            );

            (
                vec![
                    Line::from(format!("Root: Topic {}", detail.name)),
                    Line::from(format!(
                        "Providers: endpoints {} | candidate nodes {}",
                        detail.provider_endpoint_count, detail.candidate_node_count
                    )),
                    Line::from(format!(
                        "Ownership: publishers={} | subscribers={}",
                        detail.publisher_node_provenance, detail.subscriber_node_provenance
                    )),
                    Line::from("Keys: Esc/g close".to_string()),
                ],
                topology,
            )
        }
        DetailPane::Error(message) => (
            vec![
                Line::from(format!("Root: Topic {}", selected.name)),
                Line::from("Keys: Esc/g close".to_string()),
            ],
            vec![
                Line::from("Topology summary is unavailable for this topic.".to_string()),
                Line::from(format!("Error: {}", message)),
                Line::from("Press `r` to refresh topic detail and try again.".to_string()),
            ],
        ),
        DetailPane::Empty => (
            vec![
                Line::from(format!("Root: Topic {}", selected.name)),
                Line::from("Keys: Esc/g close".to_string()),
            ],
            vec![
                Line::from("Topology summary is not loaded yet.".to_string()),
                Line::from(
                    "Stay on Topics or refresh the active view to populate ownership detail."
                        .to_string(),
                ),
            ],
        ),
    }
}

fn service_topology_sections(
    selected: Option<&crate::data::ServiceRow>,
    detail: &DetailPane<ServiceDetailData>,
) -> (Vec<Line<'static>>, Vec<Line<'static>>) {
    let Some(selected) = selected else {
        return topology_empty_sections("No service is selected.");
    };

    match detail {
        DetailPane::Loaded(detail) => {
            let mut topology = vec![
                Line::from(format!("service {}", detail.name)),
                Line::from(format!("  key: {}", detail.key_expr)),
                Line::from(format!(
                    "  schema: {}{}",
                    detail.schema_availability.as_label(),
                    detail
                        .schema_hash
                        .as_deref()
                        .map(|hash| format!(" ({})", hash))
                        .unwrap_or_default()
                )),
            ];
            if let Some(note) = detail.schema_note.as_deref() {
                topology.push(Line::from(format!("  schema note: {}", note)));
            }
            push_topology_list(
                &mut topology,
                "provider endpoints",
                detail.provider_endpoint_provenance,
                &detail.provider_endpoint_ids,
                None,
            );
            push_topology_list(
                &mut topology,
                "candidate nodes",
                detail.candidate_node_provenance,
                &detail.candidate_node_names,
                detail.candidate_nodes_note.as_deref(),
            );
            push_topology_list(
                &mut topology,
                "provider nodes",
                detail.provider_node_provenance,
                &detail.provider_node_names,
                detail.provider_nodes_note.as_deref(),
            );
            push_topology_list(
                &mut topology,
                "client nodes",
                detail.client_node_provenance,
                &detail.client_node_names,
                detail.client_nodes_note.as_deref(),
            );

            (
                vec![
                    Line::from(format!("Root: Service {}", detail.name)),
                    Line::from(format!(
                        "Providers: endpoints {} | candidate nodes {}",
                        detail.provider_endpoint_count, detail.candidate_node_count
                    )),
                    Line::from(format!(
                        "Ownership: providers={} | clients={}",
                        detail.provider_node_provenance, detail.client_node_provenance
                    )),
                    Line::from("Keys: Esc/g close".to_string()),
                ],
                topology,
            )
        }
        DetailPane::Error(message) => (
            vec![
                Line::from(format!("Root: Service {}", selected.name)),
                Line::from("Keys: Esc/g close".to_string()),
            ],
            vec![
                Line::from("Topology summary is unavailable for this service.".to_string()),
                Line::from(format!("Error: {}", message)),
                Line::from("Press `r` to refresh service detail and try again.".to_string()),
            ],
        ),
        DetailPane::Empty => (
            vec![
                Line::from(format!("Root: Service {}", selected.name)),
                Line::from("Keys: Esc/g close".to_string()),
            ],
            vec![
                Line::from("Topology summary is not loaded yet.".to_string()),
                Line::from(
                    "Stay on Services or refresh the active view to populate ownership detail."
                        .to_string(),
                ),
            ],
        ),
    }
}

fn node_topology_sections(
    selected: Option<&crate::data::NodeRow>,
    detail: &DetailPane<NodeDetailData>,
) -> (Vec<Line<'static>>, Vec<Line<'static>>) {
    let Some(selected) = selected else {
        return topology_empty_sections("No node is selected.");
    };

    match detail {
        DetailPane::Loaded(detail) => {
            let mut topology = vec![
                Line::from(format!("node {}", detail.name)),
                Line::from(format!("  id: {}", detail.node_id)),
                Line::from(format!("  group: {}", detail.group)),
            ];
            push_topology_list(
                &mut topology,
                "endpoints",
                detail.endpoint_provenance,
                &detail.endpoint_ids,
                detail.endpoint_note.as_deref(),
            );
            push_topology_list(
                &mut topology,
                "known topics",
                detail.topic_provenance,
                &detail.topic_names,
                detail.topic_note.as_deref(),
            );
            push_topology_list(
                &mut topology,
                "published topics",
                detail.published_topic_provenance,
                &detail.published_topic_names,
                detail.published_topic_note.as_deref(),
            );
            push_topology_list(
                &mut topology,
                "subscribed topics",
                detail.subscribed_topic_provenance,
                &detail.subscribed_topic_names,
                detail.subscribed_topic_note.as_deref(),
            );
            push_topology_list(
                &mut topology,
                "known services",
                detail.service_provenance,
                &detail.service_names,
                detail.service_note.as_deref(),
            );
            push_topology_list(
                &mut topology,
                "provided services",
                detail.provided_service_provenance,
                &detail.provided_service_names,
                detail.provided_service_note.as_deref(),
            );
            push_topology_list(
                &mut topology,
                "client services",
                detail.client_service_provenance,
                &detail.client_service_names,
                detail.client_service_note.as_deref(),
            );

            (
                vec![
                    Line::from(format!("Root: Node {}", detail.name)),
                    Line::from(format!(
                        "Resources: endpoints {} | topics {} | services {}",
                        optional_count_label(detail.endpoint_count),
                        optional_count_label(detail.topic_count),
                        optional_count_label(detail.service_count)
                    )),
                    Line::from(format!(
                        "Ownership: published={} | subscribed={} | provided={} | clients={}",
                        detail.published_topic_provenance,
                        detail.subscribed_topic_provenance,
                        detail.provided_service_provenance,
                        detail.client_service_provenance
                    )),
                    Line::from("Keys: Esc/g close".to_string()),
                ],
                topology,
            )
        }
        DetailPane::Error(message) => (
            vec![
                Line::from(format!("Root: Node {}", selected.name)),
                Line::from("Keys: Esc/g close".to_string()),
            ],
            vec![
                Line::from("Topology summary is unavailable for this node.".to_string()),
                Line::from(format!("Error: {}", message)),
                Line::from("Press `r` to refresh node detail and try again.".to_string()),
            ],
        ),
        DetailPane::Empty => (
            vec![
                Line::from(format!("Root: Node {}", selected.name)),
                Line::from("Keys: Esc/g close".to_string()),
            ],
            vec![
                Line::from("Topology summary is not loaded yet.".to_string()),
                Line::from(
                    "Stay on Nodes or refresh the active view to populate ownership detail."
                        .to_string(),
                ),
            ],
        ),
    }
}

fn push_topology_list(
    lines: &mut Vec<Line<'static>>,
    label: &str,
    provenance: &str,
    names: &[String],
    note: Option<&str>,
) {
    lines.push(Line::from(format!("  {} [{}]:", label, provenance)));
    if names.is_empty() {
        lines.push(Line::from("    • none".to_string()));
    } else {
        for name in names.iter().take(8) {
            lines.push(Line::from(format!("    • {}", name)));
        }
        if names.len() > 8 {
            lines.push(Line::from(format!(
                "    … {} more",
                names.len().saturating_sub(8)
            )));
        }
    }
    if let Some(note) = note {
        lines.push(Line::from(format!("    note: {}", note)));
    }
}

fn bookmark_target_screen_label(bookmark: &ResourceBookmark) -> &'static str {
    match bookmark.kind {
        crate::app::BookmarkKind::Topic => "Topics",
        crate::app::BookmarkKind::Service => "Services",
        crate::app::BookmarkKind::Node => "Nodes",
    }
}

fn recorder_summary_lines(plan: &RecorderLaunchPlan) -> Vec<Line<'static>> {
    vec![
        Line::from(format!(
            "Scope: {} | Target: {}",
            plan.scope.label(),
            plan.target_label
        )),
        Line::from(format!(
            "Group: {} | Output: {}",
            plan.group, plan.output_path
        )),
        Line::from(format!("Suggested usage: {}", plan.suggested_usage)),
        Line::from("Keys: y export script • Esc/o close".to_string()),
    ]
}

fn recorder_command_lines(plan: &RecorderLaunchPlan) -> Vec<Line<'static>> {
    vec![
        Line::from("Primary CLI wrapper:".to_string()),
        Line::from(plan.primary_command.clone()),
        Line::from(String::new()),
        Line::from("Standalone binary alternative:".to_string()),
        Line::from(plan.alternate_command.clone()),
    ]
}

fn recorder_note_lines(plan: &RecorderLaunchPlan) -> Vec<Line<'static>> {
    if plan.notes.is_empty() {
        return vec![Line::from(
            "No additional notes for this recorder scope.".to_string(),
        )];
    }

    plan.notes
        .iter()
        .map(|note| Line::from(format!("• {}", note)))
        .collect()
}

fn selection_idle_lines<T>(
    resource_singular: &str,
    resource_plural: &str,
    list: &SelectableList<T>,
) -> Vec<Line<'static>> {
    if list.total_row_count() == 0 {
        vec![
            Line::from(format!(
                "No {} discovered in the current snapshot.",
                resource_plural.to_lowercase()
            )),
            Line::from("Press `r` to refresh the snapshot.".to_string()),
        ]
    } else if list.visible_row_count() == 0 {
        vec![
            Line::from(format!(
                "No {} match the current filter.",
                resource_plural.to_lowercase()
            )),
            Line::from("Update the search query or press `Esc` to clear it.".to_string()),
        ]
    } else {
        vec![
            Line::from(format!(
                "Select a {} from the list to inspect its detailed view.",
                resource_singular
            )),
            Line::from(
                "Use `j/k` to move, `/` to filter, and `r` to refresh the snapshot.".to_string(),
            ),
        ]
    }
}

fn render_detail_panel(frame: &mut Frame<'_>, area: Rect, title: &str, lines: Vec<Line<'_>>) {
    let detail = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .wrap(Wrap { trim: false });
    frame.render_widget(detail, area);
}

fn render_scrolling_detail_panel(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &str,
    lines: Vec<Line<'_>>,
    vertical_scroll: u16,
) {
    let max_scroll = lines.len().saturating_sub(1) as u16;
    let clamped_scroll = vertical_scroll.min(max_scroll);
    let detail = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .wrap(Wrap { trim: false })
        .scroll((clamped_scroll, 0));
    frame.render_widget(detail, area);
}

fn split_master_detail(area: Rect) -> Vec<Rect> {
    match LayoutMode::from_width(area.width) {
        LayoutMode::Compact => Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(44), Constraint::Percentage(56)])
            .split(area)
            .to_vec(),
        LayoutMode::Standard | LayoutMode::Wide => Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(area)
            .to_vec(),
    }
}

fn topic_detail_lines<'a>(
    selected: &'a crate::data::TopicRow,
    detail: &'a DetailPane<TopicDetailData>,
    schema_describe: &'a DetailPane<SchemaDescribeData>,
    runtime_expanded: bool,
    pinned: bool,
) -> Vec<Line<'a>> {
    match detail {
        DetailPane::Loaded(detail) => {
            let mut lines = vec![
                Line::from(format!("Topic: {}", detail.name)),
                Line::from(bookmark_status_line(pinned)),
                Line::from(format!("Key: {}", detail.key_expr)),
                Line::from(format!(
                    "State: {} | capability={} | freshness={} | {}",
                    selected.health.as_label(),
                    selected.capability.as_label(),
                    format_freshness_with_period(
                        selected.freshness,
                        selected.runtime_collection_period_ms
                    ),
                    selected.status_summary
                )),
            ];
            push_section_heading(&mut lines, "Ownership");
            lines.push(Line::from(format!(
                "Provider endpoints: {}",
                detail.provider_endpoint_count
            )));
            lines.push(Line::from(format!(
                "Candidate nodes: {}",
                detail.candidate_node_count
            )));
            lines.push(Line::from(format!(
                "Publisher nodes: {}",
                format_optional_count(detail.publisher_node_count)
            )));
            lines.push(Line::from(format!(
                "Subscriber nodes: {}",
                format_optional_count(detail.subscriber_node_count)
            )));
            lines.push(Line::from(format!(
                "Ownership provenance: providers={} nodes={} pub={} sub={}",
                detail.provider_endpoint_provenance,
                detail.candidate_node_provenance,
                detail.publisher_node_provenance,
                detail.subscriber_node_provenance
            )));
            push_optional_line(
                &mut lines,
                detail.candidate_nodes_note.as_deref(),
                "Ownership note: none",
            );
            push_optional_line(
                &mut lines,
                detail.publisher_nodes_note.as_deref(),
                "Publisher note: none",
            );
            push_optional_line(
                &mut lines,
                detail.subscriber_nodes_note.as_deref(),
                "Subscriber note: none",
            );
            push_named_resource_lines(
                &mut lines,
                "Provider endpoints",
                &detail.provider_endpoint_ids,
            );
            push_named_resource_lines(&mut lines, "Nodes", &detail.candidate_node_names);
            push_named_resource_lines(&mut lines, "Publisher nodes", &detail.publisher_node_names);
            push_named_resource_lines(
                &mut lines,
                "Subscriber nodes",
                &detail.subscriber_node_names,
            );

            push_section_heading(&mut lines, "Schema");
            lines.push(Line::from(format!(
                "Schema status: {}",
                detail.schema_availability.as_label()
            )));
            lines.push(Line::from(format!(
                "Schema provenance: {}",
                detail.schema_provenance
            )));
            push_provenance_explanation(&mut lines, "Schema source", detail.schema_provenance);
            push_schema_lines(
                &mut lines,
                &detail.schema_availability,
                detail.schema_note.as_deref(),
            );
            push_schema_describe_lines(&mut lines, schema_describe);

            push_section_heading(
                &mut lines,
                if runtime_expanded {
                    "Runtime"
                } else {
                    "Runtime (collapsed)"
                },
            );
            lines.push(Line::from(format!(
                "Telemetry: {}",
                if detail.runtime_summary.is_some() {
                    "available"
                } else {
                    "unavailable"
                }
            )));
            lines.push(Line::from(format!(
                "Runtime provenance: {}",
                detail.runtime_provenance
            )));
            push_provenance_explanation(&mut lines, "Runtime source", detail.runtime_provenance);
            push_optional_line(
                &mut lines,
                detail.runtime_note.as_deref(),
                "Runtime note: none",
            );
            if let Some(runtime) = detail.runtime_summary.as_ref() {
                lines.push(Line::from(format!(
                    "Freshness: {}",
                    format_freshness_with_period(
                        selected.freshness,
                        Some(runtime.collection_period_ms)
                    )
                )));
                if runtime_expanded {
                    lines.push(Line::from(format!(
                        "Metrics rows: pub={} sub={}",
                        runtime.publisher_metric_count, runtime.subscriber_metric_count
                    )));
                    lines.push(Line::from(format!(
                        "Publisher summary: shm_hit={} fallback={} required_fail={} loan_inflight={} quota_exceeded={}",
                        runtime.publish_shm_hit_total,
                        runtime.publish_shm_fallback_total,
                        runtime.publish_shm_required_fail_total,
                        runtime.loan_inflight_current,
                        runtime.loan_quota_exceeded_total
                    )));
                    lines.push(Line::from(format!(
                        "Subscriber anomalies: dup_drop={} reorder={} reorder_drop={} exec_drop={} local_drop={} relay_enqueue={} relay_drop={}",
                        runtime.subscriber_duplicate_drop_count,
                        runtime.subscriber_out_of_order_count,
                        runtime.subscriber_out_of_order_drop_count,
                        runtime.subscriber_executor_drop_count,
                        runtime.subscriber_local_drop_count,
                        runtime.subscriber_overflow_relay_enqueue_count,
                        runtime.subscriber_overflow_relay_drop_count
                    )));
                    lines.push(Line::from(format!(
                        "Latest runtime ts: {}",
                        runtime.latest_timestamp_ns
                    )));
                } else {
                    lines.push(Line::from(format!(
                        "Metrics rows: pub={} sub={} (press `m` to expand)",
                        runtime.publisher_metric_count, runtime.subscriber_metric_count
                    )));
                }
            } else {
                lines.push(Line::from(
                    "Runtime telemetry is unavailable for this topic in the current discovery path."
                        .to_string(),
                ));
            }
            lines
        }
        DetailPane::Error(message) => vec![
            Line::from(format!("Topic: {}", selected.name)),
            Line::from(bookmark_status_line(pinned)),
            Line::from("Detailed view is currently unavailable.".to_string()),
            Line::from(format!("Error: {}", message)),
            Line::from("Press `r` to refresh or move away and back to retry.".to_string()),
        ],
        DetailPane::Empty => vec![
            Line::from(format!("Topic: {}", selected.name)),
            Line::from(bookmark_status_line(pinned)),
            Line::from("Detailed view is not loaded yet.".to_string()),
            Line::from("Stay on Topics or refresh the snapshot to load summary data.".to_string()),
        ],
    }
}

fn service_detail_lines<'a>(
    selected: &'a crate::data::ServiceRow,
    detail: &'a DetailPane<ServiceDetailData>,
    schema_describe: &'a DetailPane<SchemaDescribeData>,
    runtime_expanded: bool,
    pinned: bool,
) -> Vec<Line<'a>> {
    match detail {
        DetailPane::Loaded(detail) => {
            let mut lines = vec![
                Line::from(format!("Service: {}", detail.name)),
                Line::from(bookmark_status_line(pinned)),
                Line::from(format!("Key: {}", detail.key_expr)),
                Line::from(format!(
                    "State: {} | capability={} | freshness={} | {}",
                    selected.health.as_label(),
                    selected.capability.as_label(),
                    format_freshness_with_period(
                        selected.freshness,
                        selected.runtime_collection_period_ms
                    ),
                    selected.status_summary
                )),
            ];
            push_section_heading(&mut lines, "Ownership");
            lines.push(Line::from(format!(
                "Provider endpoints: {}",
                detail.provider_endpoint_count
            )));
            lines.push(Line::from(format!(
                "Candidate nodes: {}",
                detail.candidate_node_count
            )));
            lines.push(Line::from(format!(
                "Provider nodes: {}",
                format_optional_count(detail.provider_node_count)
            )));
            lines.push(Line::from(format!(
                "Client nodes: {}",
                format_optional_count(detail.client_node_count)
            )));
            lines.push(Line::from(format!(
                "Ownership provenance: providers={} nodes={} service={} client={}",
                detail.provider_endpoint_provenance,
                detail.candidate_node_provenance,
                detail.provider_node_provenance,
                detail.client_node_provenance
            )));
            push_optional_line(
                &mut lines,
                detail.candidate_nodes_note.as_deref(),
                "Ownership note: none",
            );
            push_optional_line(
                &mut lines,
                detail.provider_nodes_note.as_deref(),
                "Provider note: none",
            );
            push_optional_line(
                &mut lines,
                detail.client_nodes_note.as_deref(),
                "Client note: none",
            );
            push_named_resource_lines(
                &mut lines,
                "Provider endpoints",
                &detail.provider_endpoint_ids,
            );
            push_named_resource_lines(&mut lines, "Nodes", &detail.candidate_node_names);
            push_named_resource_lines(&mut lines, "Provider nodes", &detail.provider_node_names);
            push_named_resource_lines(&mut lines, "Client nodes", &detail.client_node_names);

            push_section_heading(&mut lines, "Schema");
            lines.push(Line::from(format!(
                "Schema status: {}",
                detail.schema_availability.as_label()
            )));
            lines.push(Line::from(format!(
                "Request root: {}",
                detail.request_root_type.as_deref().unwrap_or("n/a")
            )));
            lines.push(Line::from(format!(
                "Response root: {}",
                detail.response_root_type.as_deref().unwrap_or("n/a")
            )));
            lines.push(Line::from(format!(
                "Schema provenance: {}",
                detail.schema_provenance
            )));
            push_provenance_explanation(&mut lines, "Schema source", detail.schema_provenance);
            if let Some(schema_hash) = detail.schema_hash.as_deref() {
                lines.push(Line::from(format!("Schema hash: {}", schema_hash)));
            }
            push_schema_lines(
                &mut lines,
                &detail.schema_availability,
                detail.schema_note.as_deref(),
            );
            push_schema_describe_lines(&mut lines, schema_describe);

            push_section_heading(
                &mut lines,
                if runtime_expanded {
                    "Runtime"
                } else {
                    "Runtime (collapsed)"
                },
            );
            lines.push(Line::from(format!(
                "Telemetry: {}",
                if detail.runtime_summary.is_some() {
                    "available"
                } else {
                    "unavailable"
                }
            )));
            lines.push(Line::from(format!(
                "Runtime provenance: {}",
                detail.runtime_provenance
            )));
            push_provenance_explanation(&mut lines, "Runtime source", detail.runtime_provenance);
            push_optional_line(
                &mut lines,
                detail.runtime_note.as_deref(),
                "Runtime note: none",
            );
            if let Some(runtime) = detail.runtime_summary.as_ref() {
                lines.push(Line::from(format!(
                    "Freshness: {}",
                    format_freshness_with_period(
                        selected.freshness,
                        Some(runtime.collection_period_ms)
                    )
                )));
                if runtime_expanded {
                    lines.push(Line::from(format!(
                        "Metrics rows: srv={} cli={}",
                        runtime.service_metric_count, runtime.client_metric_count
                    )));
                    lines.push(Line::from(format!(
                        "Service summary: recv={} dispatched={} ok={} err={} send_fail={} dropped={} inflight={}",
                        runtime.service_request_received_count,
                        runtime.service_request_dispatched_count,
                        runtime.service_reply_ok_count,
                        runtime.service_reply_error_count,
                        runtime.service_reply_send_failure_count,
                        runtime.service_request_dropped_count,
                        runtime.service_in_flight_requests
                    )));
                    lines.push(Line::from(format!(
                        "Client summary: calls={} ok={} timeout={} conflict={} err={} retry={}",
                        runtime.client_call_count,
                        runtime.client_success_count,
                        runtime.client_timeout_count,
                        runtime.client_concurrent_conflict_count,
                        runtime.client_error_count,
                        runtime.client_retry_count
                    )));
                    // Latency readout: last client-call latency is a true duration;
                    // last request/reply are wall-clock stamps shown as an age
                    // relative to the telemetry snapshot timestamp.
                    lines.push(Line::from(format!(
                        "Latency: last_call={} | last_req={} ago | last_reply={} ago",
                        format_latency_ns(runtime.client_last_call_latency_ns),
                        format_stamp_age_ns(
                            runtime.latest_timestamp_ns,
                            runtime.service_last_request_ns
                        ),
                        format_stamp_age_ns(
                            runtime.latest_timestamp_ns,
                            runtime.service_last_reply_ns
                        ),
                    )));
                    lines.push(Line::from(format!(
                        "Latest runtime ts: {}",
                        runtime.latest_timestamp_ns
                    )));
                } else {
                    lines.push(Line::from(format!(
                        "Metrics rows: srv={} cli={} (press `m` to expand)",
                        runtime.service_metric_count, runtime.client_metric_count
                    )));
                }
            } else {
                lines.push(Line::from(
                    "Runtime telemetry is unavailable for this service in the current discovery path."
                        .to_string(),
                ));
            }
            lines
        }
        DetailPane::Error(message) => vec![
            Line::from(format!("Service: {}", selected.name)),
            Line::from(bookmark_status_line(pinned)),
            Line::from("Detailed view is currently unavailable.".to_string()),
            Line::from(format!("Error: {}", message)),
            Line::from("Press `r` to refresh or re-open Service Call after recovery.".to_string()),
        ],
        DetailPane::Empty => vec![
            Line::from(format!("Service: {}", selected.name)),
            Line::from(bookmark_status_line(pinned)),
            Line::from("Detailed view is not loaded yet.".to_string()),
            Line::from("Press `c` to open Service Call once the schema is loaded.".to_string()),
        ],
    }
}

fn action_detail_lines<'a>(
    selected_name: &'a str,
    detail: &'a DetailPane<ActionDetailData>,
) -> Vec<Line<'a>> {
    match detail {
        DetailPane::Loaded(detail) => {
            let mut lines = vec![
                Line::from(format!("Action: {}", detail.name)),
                Line::from(format!("Key: {}", detail.key_expr)),
                Line::from(format!("Server node: {}", detail.server_node_name)),
                Line::from(format!("Server epoch: {}", detail.server_epoch)),
                Line::from(format!("Schema hash: {}", detail.schema_hash)),
                Line::from(format!(
                    "Supports cancel: {}",
                    if detail.supports_cancel { "yes" } else { "no" }
                )),
                Line::from(format!("Concurrency: {}", detail.max_concurrency)),
                Line::from(format!(
                    "Result TTL: {} ms | Dedup TTL: {} ms",
                    detail.result_ttl_ms, detail.dedup_ttl_ms
                )),
                Line::from(""),
                Line::from("Goals:"),
            ];
            if detail.goals.is_empty() {
                lines.push(Line::from("  no retained goals"));
            } else {
                for goal in detail.goals.iter().take(10) {
                    lines.push(Line::from(format!(
                        "  {}  {}  {}  seq={}",
                        goal.goal_id, goal.status, goal.code, goal.last_feedback_seq
                    )));
                }
            }
            lines
        }
        DetailPane::Error(message) => vec![
            Line::from(format!("Action: {}", selected_name)),
            Line::from("Detailed view is currently unavailable."),
            Line::from(format!("Error: {}", message)),
        ],
        DetailPane::Empty => vec![
            Line::from(format!("Action: {}", selected_name)),
            Line::from("Detailed view is not loaded yet."),
        ],
    }
}

fn node_detail_lines<'a>(
    selected: &'a crate::data::NodeRow,
    detail: &'a DetailPane<NodeDetailData>,
    runtime_expanded: bool,
    pinned: bool,
) -> Vec<Line<'a>> {
    match detail {
        DetailPane::Loaded(detail) => {
            let mut lines = vec![
                Line::from(format!("Node: {}", detail.name)),
                Line::from(bookmark_status_line(pinned)),
                Line::from(format!("Node ID: {}", detail.node_id)),
                Line::from(format!("Group: {}", detail.group)),
                Line::from(format!(
                    "State: {} | capability={} | freshness={} | {}",
                    selected.health.as_label(),
                    selected.capability.as_label(),
                    format_freshness_with_period(
                        selected.freshness,
                        selected.runtime_collection_period_ms
                    ),
                    selected.status_summary
                )),
                Line::from(format!(
                    "Discovery source: {}",
                    node_discovery_source_label(detail.discovery_source)
                )),
                Line::from(format!(
                    "Discovery note: {}",
                    node_discovery_source_note(detail.discovery_source)
                )),
            ];
            push_section_heading(&mut lines, "Inventory");
            lines.push(Line::from(format!(
                "Endpoints: {} | Topics: {} | Services: {}",
                format_optional_count(detail.endpoint_count),
                format_optional_count(detail.topic_count),
                format_optional_count(detail.service_count)
            )));
            lines.push(Line::from(format!(
                "Published topics: {} | Subscribed topics: {}",
                format_optional_count(detail.published_topic_count),
                format_optional_count(detail.subscribed_topic_count)
            )));
            lines.push(Line::from(format!(
                "Provided services: {} | Client services: {}",
                format_optional_count(detail.provided_service_count),
                format_optional_count(detail.client_service_count)
            )));
            lines.push(Line::from(format!(
                "Inventory provenance: endpoints={} topics={} services={}",
                detail.endpoint_provenance, detail.topic_provenance, detail.service_provenance
            )));
            lines.push(Line::from(format!(
                "Role provenance: pub={} sub={} provide={} client={}",
                detail.published_topic_provenance,
                detail.subscribed_topic_provenance,
                detail.provided_service_provenance,
                detail.client_service_provenance,
            )));
            push_optional_line(
                &mut lines,
                detail.endpoint_note.as_deref(),
                "Endpoint note: none",
            );
            push_optional_line(&mut lines, detail.topic_note.as_deref(), "Topic note: none");
            push_optional_line(
                &mut lines,
                detail.service_note.as_deref(),
                "Service note: none",
            );
            push_optional_line(
                &mut lines,
                detail.published_topic_note.as_deref(),
                "Published topic note: none",
            );
            push_optional_line(
                &mut lines,
                detail.subscribed_topic_note.as_deref(),
                "Subscribed topic note: none",
            );
            push_optional_line(
                &mut lines,
                detail.provided_service_note.as_deref(),
                "Provided service note: none",
            );
            push_optional_line(
                &mut lines,
                detail.client_service_note.as_deref(),
                "Client service note: none",
            );
            push_named_resource_lines(&mut lines, "Endpoints", &detail.endpoint_ids);
            push_named_resource_lines(&mut lines, "All candidate topics", &detail.topic_names);
            push_named_resource_lines(&mut lines, "All candidate services", &detail.service_names);
            push_named_resource_lines(
                &mut lines,
                "Published topics",
                &detail.published_topic_names,
            );
            push_named_resource_lines(
                &mut lines,
                "Subscribed topics",
                &detail.subscribed_topic_names,
            );
            push_named_resource_lines(
                &mut lines,
                "Provided services",
                &detail.provided_service_names,
            );
            push_named_resource_lines(&mut lines, "Client services", &detail.client_service_names);

            push_section_heading(
                &mut lines,
                if runtime_expanded {
                    "Runtime"
                } else {
                    "Runtime (collapsed)"
                },
            );
            lines.push(Line::from(format!(
                "Telemetry: {}",
                if detail.runtime_summary.is_some() {
                    "available"
                } else {
                    "unavailable"
                }
            )));
            lines.push(Line::from(format!(
                "Runtime provenance: {}",
                detail.runtime_provenance
            )));
            push_provenance_explanation(&mut lines, "Runtime source", detail.runtime_provenance);
            push_optional_line(
                &mut lines,
                detail.runtime_note.as_deref(),
                "Runtime note: none",
            );
            if let Some(runtime) = detail.runtime_summary.as_ref() {
                lines.push(Line::from(format!(
                    "Freshness: {}",
                    format_freshness_with_period(
                        selected.freshness,
                        Some(runtime.collection_period_ms)
                    )
                )));
                if runtime_expanded {
                    lines.push(Line::from(format!(
                        "Metrics rows: pub={} sub={} srv={} cli={}",
                        runtime.publisher_metric_count,
                        runtime.subscriber_metric_count,
                        runtime.service_metric_count,
                        runtime.client_metric_count
                    )));
                    lines.push(Line::from(format!(
                        "Publisher summary: shm_hit={} fallback={} required_fail={} loan_inflight={} loan_quota_exceeded={}",
                        runtime.publish_shm_hit_total,
                        runtime.publish_shm_fallback_total,
                        runtime.publish_shm_required_fail_total,
                        runtime.loan_inflight_current,
                        runtime.loan_quota_exceeded_total
                    )));
                    lines.push(Line::from(format!(
                        "Subscriber anomalies: dup_drop={} reorder={} reorder_drop={} exec_drop={} local_drop={} relay_enqueue={} relay_drop={}",
                        runtime.subscriber_duplicate_drop_count,
                        runtime.subscriber_out_of_order_count,
                        runtime.subscriber_out_of_order_drop_count,
                        runtime.subscriber_executor_drop_count,
                        runtime.subscriber_local_drop_count,
                        runtime.subscriber_overflow_relay_enqueue_count,
                        runtime.subscriber_overflow_relay_drop_count
                    )));
                    lines.push(Line::from(format!(
                        "Service summary: recv={} ok={} err={} send_fail={} dropped={} inflight={}",
                        runtime.service_request_received_count,
                        runtime.service_reply_ok_count,
                        runtime.service_reply_error_count,
                        runtime.service_reply_send_failure_count,
                        runtime.service_request_dropped_count,
                        runtime.service_in_flight_requests
                    )));
                    lines.push(Line::from(format!(
                        "Client summary: calls={} ok={} timeout={} conflict={} err={} retry={}",
                        runtime.client_call_count,
                        runtime.client_success_count,
                        runtime.client_timeout_count,
                        runtime.client_concurrent_conflict_count,
                        runtime.client_error_count,
                        runtime.client_retry_count
                    )));
                    lines.push(Line::from(format!(
                        "Executor: queued_events={} queued_bytes={} peak_events={} peak_bytes={}",
                        format_optional_u64(runtime.executor_queued_events),
                        format_optional_u64(runtime.executor_queued_bytes),
                        format_optional_u64(runtime.executor_peak_queued_events),
                        format_optional_u64(runtime.executor_peak_queued_bytes),
                    )));
                    lines.push(Line::from(format!(
                        "Executor drops: full={} closed={} budget_events={} budget_bytes={}",
                        format_optional_u64(runtime.executor_dropped_queue_full),
                        format_optional_u64(runtime.executor_dropped_queue_closed),
                        format_optional_u64(runtime.executor_dropped_budget_events),
                        format_optional_u64(runtime.executor_dropped_budget_bytes),
                    )));
                    lines.push(Line::from(format!(
                        "Latest runtime ts: {}",
                        runtime.timestamp_ns
                    )));
                } else {
                    lines.push(Line::from(format!(
                        "Metrics rows: pub={} sub={} srv={} cli={} (press `m` to expand)",
                        runtime.publisher_metric_count,
                        runtime.subscriber_metric_count,
                        runtime.service_metric_count,
                        runtime.client_metric_count
                    )));
                }
            } else {
                lines.push(Line::from(
                    "Runtime telemetry is unavailable for this node in the current discovery path."
                        .to_string(),
                ));
            }
            if runtime_expanded {
                if let Some(breakdown) = detail.runtime_breakdown.as_ref() {
                    push_section_heading(&mut lines, "Runtime breakdown");
                    // Use the node's telemetry-snapshot timestamp as the "now"
                    // reference for rendering wall-clock stamps as ages.
                    let now_ns = detail
                        .runtime_summary
                        .as_ref()
                        .map(|summary| summary.timestamp_ns)
                        .unwrap_or(0);
                    push_node_runtime_breakdown_lines(&mut lines, breakdown, now_ns);
                }
            } else if detail.runtime_breakdown.is_some() {
                push_section_heading(&mut lines, "Runtime breakdown");
                lines.push(Line::from(
                    "Runtime breakdown hidden while runtime section is collapsed.".to_string(),
                ));
            }
            lines
        }
        DetailPane::Error(message) => vec![
            Line::from(format!("Node: {}", selected.name)),
            Line::from(bookmark_status_line(pinned)),
            Line::from("Detailed view is currently unavailable.".to_string()),
            Line::from(format!("Error: {}", message)),
            Line::from("Press `r` to refresh or reselect the node to retry.".to_string()),
        ],
        DetailPane::Empty => vec![
            Line::from(format!("Node: {}", selected.name)),
            Line::from(bookmark_status_line(pinned)),
            Line::from("Detailed view is not loaded yet.".to_string()),
            Line::from("Stay on Nodes or refresh the snapshot to load summary data.".to_string()),
        ],
    }
}

fn zenoh_detail_lines<'a>(
    detail: &'a DetailPane<ZenohInternalsData>,
    connectivity: &'a DetailPane<ZenohConnectivityData>,
    events: &'a VecDeque<ZenohConnectivityEventData>,
    selected: Option<&'a ZenohRouterRow>,
) -> Vec<Line<'a>> {
    let mut lines = zenoh_connectivity_lines(connectivity);
    let current_diagnosis_lines =
        zenoh_admin_connectivity_diagnosis_lines(detail, connectivity, events);
    if !lines.is_empty() && !current_diagnosis_lines.is_empty() {
        lines.push(Line::from(String::new()));
    }
    lines.extend(current_diagnosis_lines);
    let diagnosis_lines = zenoh_connectivity_diagnosis_lines(events);
    if !lines.is_empty() && !diagnosis_lines.is_empty() {
        lines.push(Line::from(String::new()));
    }
    lines.extend(diagnosis_lines);
    let router_lines = zenoh_router_detail_lines(detail, selected);
    if !lines.is_empty() && !router_lines.is_empty() {
        lines.push(Line::from(String::new()));
    }
    lines.extend(router_lines);
    lines
}

fn zenoh_connectivity_lines<'a>(detail: &'a DetailPane<ZenohConnectivityData>) -> Vec<Line<'a>> {
    match detail {
        DetailPane::Loaded(data) => {
            let mut lines = vec![
                Line::from(
                    "Session connectivity snapshot: use this section to confirm live peers, locators, and active links."
                        .to_string(),
                ),
                Line::from(format!("Session ZID: {}", data.session_zid)),
                Line::from(format!("Local locators: {}", data.local_locator_count)),
                Line::from(format!("Transports: {}", data.transport_count)),
                Line::from(format!("Links: {}", data.link_count)),
            ];
            push_named_resource_lines(&mut lines, "Local locator list", &data.local_locators);
            lines.push(Line::from("Transport list:".to_string()));
            if data.transports.is_empty() {
                lines.push(Line::from("  none".to_string()));
            } else {
                for transport in data.transports.iter().take(6) {
                    lines.push(Line::from(format!(
                        "  • {} [{}] qos={} shm={} multicast={}",
                        transport.remote_zid,
                        transport.remote_whatami,
                        if transport.supports_qos { "yes" } else { "no" },
                        if transport.supports_shm { "yes" } else { "no" },
                        if transport.is_multicast { "yes" } else { "no" },
                    )));
                }
                if data.transports.len() > 6 {
                    lines.push(Line::from(format!(
                        "  … {} more",
                        data.transports.len().saturating_sub(6)
                    )));
                }
            }
            lines.push(Line::from("Link list:".to_string()));
            if data.links.is_empty() {
                lines.push(Line::from("  none".to_string()));
            } else {
                for link in data.links.iter().take(6) {
                    lines.push(Line::from(format!(
                        "  • {} -> {} [{}] mtu={} rel={} ifaces={}",
                        link.src,
                        link.dst,
                        link.transport_zid,
                        link.mtu,
                        link.reliability.as_deref().unwrap_or("n/a"),
                        link.interface_count,
                    )));
                }
                if data.links.len() > 6 {
                    lines.push(Line::from(format!(
                        "  … {} more",
                        data.links.len().saturating_sub(6)
                    )));
                }
            }
            lines
        }
        DetailPane::Error(message) => vec![
            Line::from("Session connectivity snapshot is unavailable.".to_string()),
            Line::from(format!("Error: {}", message)),
            Line::from(
                "Check peer reachability, session locators, and whether the current Zenoh session is healthy."
                    .to_string(),
            ),
        ],
        DetailPane::Empty => Vec::new(),
    }
}

fn zenoh_admin_connectivity_diagnosis_lines<'a>(
    detail: &'a DetailPane<ZenohInternalsData>,
    connectivity: &'a DetailPane<ZenohConnectivityData>,
    events: &'a VecDeque<ZenohConnectivityEventData>,
) -> Vec<Line<'a>> {
    let admin_state = match detail {
        DetailPane::Loaded(data) => format!("available (routers={})", data.router_count),
        DetailPane::Error(_) => "unavailable".to_string(),
        DetailPane::Empty => "not loaded".to_string(),
    };
    let connectivity_state = match connectivity {
        DetailPane::Loaded(data) => {
            format!(
                "transports={} links={}",
                data.transport_count, data.link_count
            )
        }
        DetailPane::Error(_) => "unavailable".to_string(),
        DetailPane::Empty => "not loaded".to_string(),
    };

    let connectivity_present = matches!(
        connectivity,
        DetailPane::Loaded(data) if data.transport_count > 0 || data.link_count > 0
    );
    let recent_disconnect_seen = events.iter().any(|event| {
        matches!(
            event.kind,
            ZenohConnectivityEventKindData::TransportClosed
                | ZenohConnectivityEventKindData::LinkRemoved
        )
    });

    let assessment = match (detail, connectivity) {
        (DetailPane::Error(_), DetailPane::Loaded(data))
            if data.transport_count > 0 || data.link_count > 0 =>
        {
            "live transports/links are present but router admin diagnostics failed; likely admin-space ACL or visibility issue".to_string()
        }
        (DetailPane::Error(_), DetailPane::Loaded(data))
            if data.transport_count == 0 && data.link_count == 0 =>
        {
            "neither router admin diagnostics nor live transports are visible; likely a session reachability or locator issue".to_string()
        }
        (DetailPane::Loaded(data), DetailPane::Loaded(connectivity_data))
            if data.router_count == 0
                && (connectivity_data.transport_count > 0 || connectivity_data.link_count > 0) =>
        {
            "live transports/links are present but router admin inventory is empty; peer/session connectivity exists while router admin data is absent".to_string()
        }
        (DetailPane::Loaded(_), DetailPane::Loaded(data))
            if data.transport_count == 0 && data.link_count == 0 =>
        {
            "router admin space is visible but the current session has no active transports or links; check locators, mode, or routing path".to_string()
        }
        (_, DetailPane::Loaded(_)) if recent_disconnect_seen && connectivity_present => {
            "recent disconnect signals were observed, but current transports/links are live; likely recovered after flap".to_string()
        }
        (_, DetailPane::Error(_)) => {
            "current session connectivity snapshot is unavailable; refresh after checking session health".to_string()
        }
        (_, DetailPane::Empty) => {
            "waiting for a live connectivity snapshot before comparing it with router admin data".to_string()
        }
        (DetailPane::Empty, _) => {
            "waiting for router admin diagnostics before comparing them with current connectivity".to_string()
        }
        _ => "router admin and live connectivity signals are aligned in the current snapshot"
            .to_string(),
    };

    vec![
        Line::from("Current admin/connectivity diagnosis:".to_string()),
        Line::from(format!("Admin diagnostics: {}", admin_state)),
        Line::from(format!("Connectivity snapshot: {}", connectivity_state)),
        Line::from(format!("Assessment: {}", assessment)),
    ]
}

fn zenoh_connectivity_diagnosis_lines<'a>(
    events: &'a VecDeque<ZenohConnectivityEventData>,
) -> Vec<Line<'a>> {
    let disconnect_count = events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                ZenohConnectivityEventKindData::TransportClosed
                    | ZenohConnectivityEventKindData::LinkRemoved
            )
        })
        .count();
    let recovery_count = events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                ZenohConnectivityEventKindData::TransportOpened
                    | ZenohConnectivityEventKindData::LinkAdded
            )
        })
        .count();
    let last_disconnect = events.iter().rev().find(|event| {
        matches!(
            event.kind,
            ZenohConnectivityEventKindData::TransportClosed
                | ZenohConnectivityEventKindData::LinkRemoved
        )
    });
    let last_recovery = events.iter().rev().find(|event| {
        matches!(
            event.kind,
            ZenohConnectivityEventKindData::TransportOpened
                | ZenohConnectivityEventKindData::LinkAdded
        )
    });

    let assessment = match (last_disconnect, last_recovery) {
        (Some(disconnect), Some(recovery)) if recovery.sequence > disconnect.sequence => {
            "recovered after recent disconnect signal".to_string()
        }
        (Some(_), Some(_)) => {
            "latest retained signal is a disconnect; inspect current links and peers".to_string()
        }
        (Some(_), None) => {
            "disconnect signal observed without recovery in retained window".to_string()
        }
        (None, Some(_)) => {
            "recovery signals observed and no retained disconnects remain in window".to_string()
        }
        (None, None) => "no connectivity events retained yet".to_string(),
    };

    let mut lines = vec![
        Line::from("Recent connectivity diagnosis:".to_string()),
        Line::from(format!("Disconnect signals: {}", disconnect_count)),
        Line::from(format!("Recovery signals: {}", recovery_count)),
        Line::from(format!(
            "Flap suspicion: {}",
            if disconnect_count > 0 && recovery_count > 0 {
                "yes"
            } else {
                "no"
            }
        )),
        Line::from(format!("Assessment: {}", assessment)),
    ];
    if let Some(event) = last_disconnect {
        lines.push(Line::from(format!("Last disconnect: {}", event.message)));
    }
    if let Some(event) = last_recovery {
        lines.push(Line::from(format!("Last recovery: {}", event.message)));
    }
    lines
}

fn zenoh_router_detail_lines<'a>(
    detail: &'a DetailPane<ZenohInternalsData>,
    selected: Option<&'a ZenohRouterRow>,
) -> Vec<Line<'a>> {
    match detail {
        DetailPane::Loaded(data) => {
            let Some(router) = selected else {
                return vec![
                    Line::from("Advanced router diagnostics.".to_string()),
                    Line::from(format!("Routers discovered: {}", data.router_count)),
                    Line::from("No router selected. Use `j/k` to inspect a router.".to_string()),
                ];
            };
            let mut lines = vec![
                Line::from(
                    "Advanced screen: use this panel to separate router/admin issues from middleware issues."
                        .to_string(),
                ),
                Line::from(format!("Router ZID: {}", router.zid)),
                Line::from(format!("WhatAmI: {}", router.whatami)),
                Line::from(format!(
                    "Version: {}",
                    router.version.as_deref().unwrap_or("unknown")
                )),
                Line::from(format!(
                    "Sessions: {}",
                    router
                        .session_count
                        .map(|count| count.to_string())
                        .unwrap_or_else(|| "n/a".to_string())
                )),
                Line::from(format!(
                    "Metrics capability: {}",
                    if router.metrics_available {
                        "available"
                    } else {
                        "unavailable"
                    }
                )),
                Line::from(format!("Locators: {}", router.locator_count)),
                Line::from(format!("Plugins: {}", router.plugin_count)),
            ];
            if let Some(metrics) = router.metrics.as_ref() {
                lines.push(Line::from(
                    "Traffic counters (cumulative since router start):".to_string(),
                ));
                lines.push(Line::from(format!(
                    "  tx: {} ({} msgs)",
                    human_bytes(metrics.tx_bytes_total),
                    metrics.tx_msgs_total,
                )));
                lines.push(Line::from(format!(
                    "  rx: {} ({} msgs)",
                    human_bytes(metrics.rx_bytes_total),
                    metrics.rx_msgs_total,
                )));
                if let Some(transports) = metrics.transports_opened {
                    lines.push(Line::from(format!("  transports opened: {}", transports)));
                }
                if let Some(links) = metrics.links_opened {
                    lines.push(Line::from(format!("  links opened: {}", links)));
                }
            }
            lines.push(Line::from(zenoh_metrics_capability_note(router)));
            push_named_resource_lines(&mut lines, "Locator list", &router.locators);
            push_named_resource_lines(&mut lines, "Plugin list", &router.plugin_names);
            lines.push(Line::from("Raw admin payload:"));
            if router.raw_payload_utf8.trim().is_empty() {
                lines.push(Line::from(
                    "  <empty> (admin root replied without a readable UTF-8 payload)".to_string(),
                ));
            } else {
                lines.extend(
                    router
                        .raw_payload_utf8
                        .lines()
                        .map(|line| Line::from(line.to_string())),
                );
            }
            lines
        }
        DetailPane::Error(message) => vec![
            Line::from("Advanced Zenoh diagnostics are unavailable.".to_string()),
            Line::from(format!("Error: {}", message)),
            Line::from(
                "Check router admin-space ACL, router reachability, and whether admin roots are enabled."
                    .to_string(),
            ),
        ],
        DetailPane::Empty => vec![
            Line::from("Advanced Zenoh diagnostics are not requested yet.".to_string()),
            Line::from(
                "Press `z` to enable advanced mode and open the Zenoh tab when router diagnostics are needed."
                    .to_string(),
            ),
        ],
    }
}

fn schema_hint_lines(
    schema: &ServiceCallSchemaData,
    selected_index: Option<usize>,
) -> Vec<Line<'_>> {
    schema_hint_lines_for_model(
        &schema.request_model,
        selected_index,
        schema.candidate_nodes_note.as_deref(),
    )
}

/// Shared request/goal-field hint renderer used by both the Service Call and
/// Action Call editors. `discovery_note`, when present, is surfaced as an extra
/// line (Service Call passes its candidate-node note; Action Call passes None).
fn schema_hint_lines_for_model<'a>(
    model: &'a aorta_introspect::schema::SchemaModel,
    selected_index: Option<usize>,
    discovery_note: Option<&'a str>,
) -> Vec<Line<'a>> {
    let mut lines = vec![
        Line::from(format!("Root: {}", model.root_type)),
        Line::from(format!(
            "Fields: {} | Controls: `j/k` or wheel select, `click` inspect, `a` apply, `R` reset, `i` edit, `F5` send",
            model.fields.len()
        )),
        Line::from(
            "Use starter JSON plus merge patches instead of hand-authoring the full request."
                .to_string(),
        ),
    ];

    if let Some(note) = discovery_note {
        lines.push(Line::from(format!("Discovery note: {}", note)));
    }

    let Some(selected_index) = selected_index else {
        lines.push(Line::from("No schema fields available.".to_string()));
        return lines;
    };

    if let Some(field) = model.fields.get(selected_index) {
        lines.push(Line::from(format!(
            "Selected field: {} ({}/{})",
            field.path,
            selected_index + 1,
            model.fields.len()
        )));
        lines.push(Line::from(format!(
            "Type: {}",
            format_schema_field_label(field)
        )));
        if let Some(doc) = field.doc.as_deref() {
            lines.push(Line::from(format!("Doc: {}", doc)));
        }
        match field_template_json_from_schema_model(model, &field.path) {
            Ok(example_json) => {
                lines.push(Line::from("Example merge patch:"));
                lines.extend(
                    example_json
                        .lines()
                        .map(|line| Line::from(line.to_string())),
                );
            }
            Err(err) => lines.push(Line::from(format!("Example unavailable: {}", err))),
        }
    }

    lines
}

fn format_schema_field_summary(field: &aorta_introspect::schema::SchemaField) -> String {
    format!("{}: {}", field.path, format_schema_field_label(field))
}

fn format_schema_field_label(field: &aorta_introspect::schema::SchemaField) -> String {
    let mut label = field.type_name.clone();
    if field.optional {
        label.push_str(" [optional]");
    }
    if field.repeated {
        label.push_str(" [repeated]");
    }
    if !field.enum_values.is_empty() {
        let values = field
            .enum_values
            .iter()
            .map(|value| value.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        label.push_str(&format!(" = {{{}}}", values));
    }
    label
}

fn response_lines(response: &ServiceCallResponseData) -> Vec<Line<'_>> {
    let response_mode = if response.response_json.is_some() {
        "decoded json"
    } else if response.response_utf8.is_some() {
        "utf-8 fallback"
    } else {
        "hex fallback"
    };
    let mut lines = vec![
        Line::from("Call completed successfully.".to_string()),
        Line::from(format!("Reply mode: {}", response_mode)),
        Line::from("Export: press `y` to write this payload to a local JSON file.".to_string()),
        Line::from(format!(
            "Decoded with schema: {}",
            if response.decoded_with_schema {
                "yes"
            } else {
                "no"
            }
        )),
        Line::from(format!(
            "Response root: {}",
            response.response_root_type.as_deref().unwrap_or("n/a")
        )),
        Line::from(format!("Schema hash: {}", response.schema_hash)),
    ];

    if let Some(json) = response.response_json.as_deref() {
        lines.push(Line::from("Decoded JSON:".to_string()));
        lines.extend(json.lines().map(|line| Line::from(line.to_string())));
    } else if let Some(text) = response.response_utf8.as_deref() {
        lines.push(Line::from(
            "UTF-8 response body (schema decode unavailable):".to_string(),
        ));
        lines.extend(text.lines().map(|line| Line::from(line.to_string())));
    } else {
        lines.push(Line::from(
            "Raw response bytes (UTF-8 decode unavailable):".to_string(),
        ));
        lines.push(Line::from(format!("0x{}", response.response_hex)));
    }
    lines
}

fn topic_preview_lines(preview: &TopicPreviewState) -> Vec<Line<'_>> {
    let preview_status = topic_preview_status(preview);
    let mut lines = vec![
        Line::from(format!(
            "Target: {}",
            preview.target_topic().unwrap_or("(none)")
        )),
        Line::from(format!("Status: {}", preview_status)),
        Line::from(format!("Messages observed: {}", preview.message_count())),
        Line::from(format!(
            "Live rate: {}",
            preview
                .live_rate_hz()
                .map(|hz| format!("{:.2} Hz", hz))
                .unwrap_or_else(|| topic_preview_rate_hint(preview))
        )),
        Line::from(format!(
            "Live bandwidth: {}",
            preview
                .live_bytes_per_s()
                .map(|bps| format!(
                    "{}/s (mean {})",
                    human_bytes(bps as u64),
                    preview
                        .mean_payload_bytes()
                        .map(|mean| human_bytes(mean as u64))
                        .unwrap_or_else(|| "n/a".to_string())
                ))
                .unwrap_or_else(|| "n/a".to_string())
        )),
        Line::from(format!(
            "Latest sample age: {}",
            preview
                .last_message_age()
                .map(format_duration)
                .unwrap_or_else(|| "none".to_string())
        )),
        Line::from(format!("View: {} (v cycle)", preview.view_mode().label())),
        Line::from(format!(
            "History: {} ([ older ] newer)",
            format_topic_preview_history(preview)
        )),
        Line::from(format!(
            "Viewed frame age: {}",
            preview
                .selected_frame_age()
                .map(format_duration)
                .unwrap_or_else(|| "none".to_string())
        )),
        Line::from(format!(
            "Scroll: {} (PgUp/PgDn, Home reset)",
            preview.scroll()
        )),
        Line::from("Export: press `y` to write to file, `Y` to copy to clipboard.".to_string()),
    ];

    if let Some(error) = preview.last_error() {
        lines.push(Line::from(format!("Preview error: {}", error)));
    }

    let Some(frame) = preview.selected_frame() else {
        lines.push(Line::from(topic_preview_waiting_hint(preview)));
        return lines;
    };

    lines.push(Line::from(format!(
        "Decode: {}",
        topic_preview_decode_label(&frame.decode_status)
    )));
    if let Some(schema_hash) = frame.schema_hash.as_deref() {
        lines.push(Line::from(format!("Schema hash: {}", schema_hash)));
    }

    match preview.view_mode() {
        TopicPreviewViewMode::Decoded => {
            if let Some(decoded_json) = frame.decoded_json.as_deref() {
                lines.extend(
                    decoded_json
                        .lines()
                        .map(|line| Line::from(line.to_string())),
                );
            } else {
                lines.push(Line::from(topic_preview_decode_fallback_message(
                    &frame.decode_status,
                )));
                lines.push(Line::from(format!("0x{}", frame.payload_hex)));
            }
        }
        TopicPreviewViewMode::Raw => {
            lines.push(Line::from(format!("0x{}", frame.payload_hex)));
        }
        TopicPreviewViewMode::Diff => {
            lines.extend(topic_preview_diff_lines(preview));
        }
    }

    lines
}

const TOPIC_PREVIEW_DIFF_MAX_CHARS: usize = 16 * 1024;
const TOPIC_PREVIEW_DIFF_MAX_LINES: usize = 256;

fn topic_preview_diff_lines(preview: &TopicPreviewState) -> Vec<Line<'static>> {
    let Some(pair) = preview.diff_pair() else {
        return vec![
            Line::from("Diff unavailable until at least two retained frames exist.".to_string()),
            Line::from(
                "Capture another frame or use `[` / `]` to move within history.".to_string(),
            ),
        ];
    };

    let (reference_body, selected_body, mode_label) =
        topic_preview_diff_payload_bodies(pair.reference_frame, pair.selected_frame);
    let reference_chars = reference_body.chars().count();
    let selected_chars = selected_body.chars().count();

    let mut lines = vec![
        Line::from(format!(
            "Diff: frame {} vs {} frame {}",
            pair.selected_position,
            pair.reference_relation.label(),
            pair.reference_position
        )),
        Line::from(format!(
            "Mode: {} | selected chars={} | reference chars={}",
            mode_label, selected_chars, reference_chars
        )),
        Line::from("Legend: `-` removed from reference, `+` added in selected.".to_string()),
    ];

    if selected_chars > TOPIC_PREVIEW_DIFF_MAX_CHARS
        || reference_chars > TOPIC_PREVIEW_DIFF_MAX_CHARS
    {
        lines.push(Line::from(format!(
            "Diff unavailable: payload exceeds {} characters. Use raw/decoded mode to inspect the selected frame directly.",
            TOPIC_PREVIEW_DIFF_MAX_CHARS
        )));
        return lines;
    }

    lines.extend(
        bounded_line_diff(
            &reference_body,
            &selected_body,
            TOPIC_PREVIEW_DIFF_MAX_LINES,
        )
        .into_iter()
        .map(Line::from),
    );
    lines
}

fn topic_preview_diff_payload_bodies(
    reference: &TopicPreviewFrame,
    selected: &TopicPreviewFrame,
) -> (String, String, &'static str) {
    match (
        reference.decoded_json.as_deref(),
        selected.decoded_json.as_deref(),
    ) {
        (Some(reference_json), Some(selected_json)) => (
            normalize_json_for_diff(reference_json),
            normalize_json_for_diff(selected_json),
            "decoded-json",
        ),
        _ => (
            format!("0x{}", reference.payload_hex),
            format!("0x{}", selected.payload_hex),
            "raw-hex fallback",
        ),
    }
}

fn normalize_json_for_diff(payload: &str) -> String {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|value| serde_json::to_string_pretty(&value).ok())
        .unwrap_or_else(|| payload.to_string())
}

fn bounded_line_diff(reference: &str, selected: &str, max_lines: usize) -> Vec<String> {
    let reference_lines: Vec<&str> = if reference.is_empty() {
        vec![""]
    } else {
        reference.lines().collect()
    };
    let selected_lines: Vec<&str> = if selected.is_empty() {
        vec![""]
    } else {
        selected.lines().collect()
    };
    let reference_len = reference_lines.len();
    let selected_len = selected_lines.len();
    let mut lcs = vec![vec![0usize; selected_len + 1]; reference_len + 1];

    for reference_index in (0..reference_len).rev() {
        for selected_index in (0..selected_len).rev() {
            lcs[reference_index][selected_index] =
                if reference_lines[reference_index] == selected_lines[selected_index] {
                    lcs[reference_index + 1][selected_index + 1] + 1
                } else {
                    lcs[reference_index + 1][selected_index]
                        .max(lcs[reference_index][selected_index + 1])
                };
        }
    }

    let mut diff_lines = Vec::new();
    let mut reference_index = 0;
    let mut selected_index = 0;
    while reference_index < reference_len && selected_index < selected_len {
        if reference_lines[reference_index] == selected_lines[selected_index] {
            diff_lines.push(format!("  {}", reference_lines[reference_index]));
            reference_index += 1;
            selected_index += 1;
        } else if lcs[reference_index + 1][selected_index]
            >= lcs[reference_index][selected_index + 1]
        {
            diff_lines.push(format!("- {}", reference_lines[reference_index]));
            reference_index += 1;
        } else {
            diff_lines.push(format!("+ {}", selected_lines[selected_index]));
            selected_index += 1;
        }
    }

    while reference_index < reference_len {
        diff_lines.push(format!("- {}", reference_lines[reference_index]));
        reference_index += 1;
    }
    while selected_index < selected_len {
        diff_lines.push(format!("+ {}", selected_lines[selected_index]));
        selected_index += 1;
    }

    if diff_lines.len() > max_lines {
        diff_lines.truncate(max_lines);
        diff_lines.push(format!(
            "… diff truncated after {} lines to keep the terminal view bounded.",
            max_lines
        ));
    }

    diff_lines
}

fn format_topic_preview_history(preview: &TopicPreviewState) -> String {
    let Some((position, total)) = preview.history_position() else {
        return if preview.target_topic().is_none() {
            "idle".to_string()
        } else {
            "waiting for first sample".to_string()
        };
    };

    if preview.is_viewing_latest() {
        format!("latest ({}/{})", position, total)
    } else {
        format!("{}/{} selected", position, total)
    }
}

fn topic_preview_rate_hint(preview: &TopicPreviewState) -> String {
    if preview.message_count() == 0 {
        "waiting for first sample".to_string()
    } else {
        "collecting rate".to_string()
    }
}

fn topic_preview_decode_label(status: &TopicPreviewDecodeStatus) -> String {
    match status {
        TopicPreviewDecodeStatus::DecodedFromTopicSchema => "topic schema".to_string(),
        TopicPreviewDecodeStatus::DecodedFromAttachmentSchema => "attachment schema".to_string(),
        TopicPreviewDecodeStatus::RawOnly => "raw only".to_string(),
        TopicPreviewDecodeStatus::DecodeError(message) => format!("decode error ({})", message),
    }
}

fn topic_preview_status(preview: &TopicPreviewState) -> String {
    if preview.target_topic().is_none() {
        return "idle (select a topic to start lazy preview)".to_string();
    }
    if preview.last_error().is_some() && preview.selected_frame().is_none() {
        return "error (stream failed before first frame)".to_string();
    }
    if !preview.is_viewing_latest() && preview.selected_frame().is_some() {
        return "history (older frame selected)".to_string();
    }
    match preview.last_message_age() {
        Some(age) if age <= Duration::from_secs(1) => "live".to_string(),
        Some(age) if age <= Duration::from_secs(5) => {
            format!("quiet ({})", format_duration(age))
        }
        Some(age) => format!("stale ({})", format_duration(age)),
        None => "waiting for first sample".to_string(),
    }
}

fn topic_preview_waiting_hint(preview: &TopicPreviewState) -> String {
    if preview.target_topic().is_none() {
        "Select a topic to start lazy preview.".to_string()
    } else if let Some(error) = preview.last_error() {
        format!(
            "Preview stream is not healthy yet: {}. Stay on Topics or reselect the topic to retry.",
            error
        )
    } else {
        "Listening for samples on the selected topic. Stay on Topics to keep the preview active."
            .to_string()
    }
}

fn topic_preview_decode_fallback_message(status: &TopicPreviewDecodeStatus) -> String {
    match status {
        TopicPreviewDecodeStatus::RawOnly => {
            "Decoded JSON is unavailable for this frame; press `v` to focus on raw bytes."
                .to_string()
        }
        TopicPreviewDecodeStatus::DecodeError(message) => {
            format!("Decoded view failed for this frame ({message}); raw bytes are shown below.")
        }
        TopicPreviewDecodeStatus::DecodedFromTopicSchema
        | TopicPreviewDecodeStatus::DecodedFromAttachmentSchema => {
            "Decoded JSON is unavailable for this frame; raw bytes are shown below.".to_string()
        }
    }
}

fn format_duration(duration: Duration) -> String {
    if duration.as_secs() >= 1 {
        format!("{:.1}s ago", duration.as_secs_f64())
    } else {
        format!("{}ms ago", duration.as_millis())
    }
}

/// Format a nanosecond duration with an adaptive ns/us/ms/s/m unit (mirror of
/// the CLI `util::format_duration_ns`; the two crates don't share a lib).
fn format_duration_ns(duration_ns: u64) -> String {
    if duration_ns < 1_000 {
        return format!("{}ns", duration_ns);
    }
    if duration_ns < 1_000_000 {
        return format!("{:.3}us", duration_ns as f64 / 1_000.0);
    }
    if duration_ns < 1_000_000_000 {
        return format!("{:.3}ms", duration_ns as f64 / 1_000_000.0);
    }
    if duration_ns < 60_000_000_000 {
        return format!("{:.3}s", duration_ns as f64 / 1_000_000_000.0);
    }
    format!("{:.3}m", duration_ns as f64 / 60_000_000_000.0)
}

/// Format a latency duration, rendering `0` (no call yet) as `n/a`.
fn format_latency_ns(latency_ns: u64) -> String {
    if latency_ns == 0 {
        return "n/a".to_string();
    }
    format_duration_ns(latency_ns)
}

/// Format the age of a wall-clock `unix_time_ns` stamp relative to `now_ns`.
/// Zero stamp → `n/a`; a future stamp clamps to `0ns`.
fn format_stamp_age_ns(now_ns: u64, stamp_ns: u64) -> String {
    if stamp_ns == 0 {
        return "n/a".to_string();
    }
    format_duration_ns(now_ns.saturating_sub(stamp_ns))
}

/// Format a byte count with binary (1024-based) units for router-traffic readouts.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{} B", bytes);
    }
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{:.2} {}", value, UNITS[unit])
}

fn overview_event_panel_title(app: &App) -> &'static str {
    if app
        .event_log()
        .iter()
        .any(|entry| matches!(entry.severity, EventSeverity::Warn | EventSeverity::Error))
    {
        "Recent Alerts"
    } else {
        "Event Log"
    }
}

fn overview_event_lines(app: &App) -> Vec<Line<'_>> {
    let mut lines = Vec::new();
    let warning_entries: Vec<_> = app
        .event_log()
        .iter()
        .rev()
        .filter(|entry| matches!(entry.severity, EventSeverity::Warn | EventSeverity::Error))
        .take(8)
        .collect();
    let selected_entries: Vec<_> = if warning_entries.is_empty() {
        app.event_log().iter().rev().take(8).collect()
    } else {
        warning_entries
    };

    for entry in selected_entries {
        lines.push(Line::from(format!(
            "[{}|{}] {} {}",
            entry.severity.label(),
            entry.source.label(),
            format_duration(entry.observed_at.elapsed()),
            entry.message
        )));
    }

    if lines.is_empty() {
        lines.push(Line::from("No events yet."));
    } else if app
        .event_log()
        .iter()
        .all(|entry| matches!(entry.severity, EventSeverity::Info))
    {
        lines.insert(
            0,
            Line::from("No active alerts. Showing latest informational events."),
        );
    }

    lines
}

fn push_section_heading<'a>(lines: &mut Vec<Line<'a>>, title: &str) {
    if !lines.is_empty() {
        lines.push(Line::from(""));
    }
    lines.push(Line::from(format!("{title}:")));
}

fn push_schema_lines<'a>(
    lines: &mut Vec<Line<'a>>,
    availability: &ExactSchemaAvailability,
    note: Option<&'a str>,
) {
    if let Some(hash) = availability.hash() {
        lines.push(Line::from(format!("Schema hash: {}", hash)));
    }
    match note {
        Some(note) => lines.push(Line::from(note.to_string())),
        None if matches!(availability, ExactSchemaAvailability::Missing) => {
            lines.push(Line::from(
                "Schema note: schema_by_key returned no exact match".to_string(),
            ));
        }
        None => lines.push(Line::from("Schema note: none".to_string())),
    }
}

fn push_schema_describe_lines<'a>(
    lines: &mut Vec<Line<'a>>,
    schema_describe: &'a DetailPane<SchemaDescribeData>,
) {
    match schema_describe {
        DetailPane::Empty => {
            lines.push(Line::from(
                "Press `d` to load the full schema definition.".to_string(),
            ));
        }
        DetailPane::Loaded(data) => {
            push_section_heading(lines, "Schema Detail");
            lines.push(Line::from(format!("Schema kind: {}", data.kind.as_label())));
            lines.push(Line::from(format!("Schema key: {}", data.key_suffix)));
            lines.push(Line::from(format!(
                "Schema hash: {} ({} bytes)",
                data.schema_hash, data.bfbs_bytes
            )));
            lines.push(Line::from(
                "Press `d` again to close the schema viewer.".to_string(),
            ));
            for section in &data.sections {
                lines.push(Line::from(""));
                lines.push(Line::from(format!(
                    "{}{}:",
                    section.label,
                    section
                        .root_type
                        .as_deref()
                        .map(|root| format!(" (root: {})", root))
                        .unwrap_or_default(),
                )));
                for body_line in section.text.lines() {
                    lines.push(Line::from(body_line.to_string()));
                }
            }
        }
        DetailPane::Error(message) => {
            push_section_heading(lines, "Schema Detail");
            lines.push(Line::from(
                "Failed to fetch schema definition. Press `d` to close.".to_string(),
            ));
            lines.push(Line::from(format!("Error: {}", message)));
        }
    }
}

fn push_optional_line<'a>(lines: &mut Vec<Line<'a>>, value: Option<&'a str>, fallback: &'a str) {
    match value {
        Some(value) => lines.push(Line::from(value.to_string())),
        None => lines.push(Line::from(fallback.to_string())),
    }
}

fn push_provenance_explanation<'a>(lines: &mut Vec<Line<'a>>, label: &str, provenance: &'a str) {
    lines.push(Line::from(format!(
        "{}: {}",
        label,
        provenance_explanation(provenance)
    )));
}

fn push_named_resource_lines<'a>(lines: &mut Vec<Line<'a>>, label: &str, names: &'a [String]) {
    lines.push(Line::from(format!("{}:", label)));
    if names.is_empty() {
        lines.push(Line::from("  none".to_string()));
        return;
    }

    for name in names.iter().take(6) {
        lines.push(Line::from(format!("  • {}", name)));
    }
    if names.len() > 6 {
        lines.push(Line::from(format!(
            "  … {} more",
            names.len().saturating_sub(6)
        )));
    }
}

fn push_node_runtime_breakdown_lines<'a>(
    lines: &mut Vec<Line<'a>>,
    breakdown: &'a NodeRuntimeBreakdownData,
    now_ns: u64,
) {
    push_node_runtime_publisher_lines(lines, &breakdown.publishers);
    push_node_runtime_subscriber_lines(lines, &breakdown.subscribers);
    push_node_runtime_service_lines(lines, &breakdown.services, now_ns);
    push_node_runtime_client_lines(lines, &breakdown.clients, now_ns);
}

fn push_node_runtime_publisher_lines<'a>(
    lines: &mut Vec<Line<'a>>,
    entries: &'a [NodePublisherRuntimeEntryData],
) {
    lines.push(Line::from("Runtime publishers:"));
    if entries.is_empty() {
        lines.push(Line::from("  none".to_string()));
        return;
    }
    for entry in entries.iter().take(8) {
        lines.push(Line::from(format!(
            "  • {} [{}{}] shm_hit={} fallback={} required_fail={} loan_inflight={} quota_exceeded={}",
            entry.resource_name,
            entry.resource_instance_id,
            format_schema_suffix(entry.schema_hash.as_deref()),
            entry.publish_shm_hit_total,
            entry.publish_shm_fallback_total,
            entry.publish_shm_required_fail_total,
            entry.loan_inflight_current,
            entry.loan_quota_exceeded_total
        )));
    }
    if entries.len() > 8 {
        lines.push(Line::from(format!(
            "  … {} more",
            entries.len().saturating_sub(8)
        )));
    }
}

fn push_node_runtime_subscriber_lines<'a>(
    lines: &mut Vec<Line<'a>>,
    entries: &'a [NodeSubscriberRuntimeEntryData],
) {
    lines.push(Line::from("Runtime subscribers:"));
    if entries.is_empty() {
        lines.push(Line::from("  none".to_string()));
        return;
    }
    for entry in entries.iter().take(8) {
        lines.push(Line::from(format!(
            "  • {} [{}{}] dup={} reorder={} reorder_drop={} exec_drop={} local_drop={} relay_enqueue={} relay_drop={}",
            entry.resource_name,
            entry.resource_instance_id,
            format_schema_suffix(entry.schema_hash.as_deref()),
            entry.duplicate_drop_count,
            entry.out_of_order_count,
            entry.out_of_order_drop_count,
            entry.executor_drop_count,
            entry.local_drop_count,
            entry.overflow_relay_enqueue_count,
            entry.overflow_relay_drop_count
        )));
    }
    if entries.len() > 8 {
        lines.push(Line::from(format!(
            "  … {} more",
            entries.len().saturating_sub(8)
        )));
    }
}

fn push_node_runtime_service_lines<'a>(
    lines: &mut Vec<Line<'a>>,
    entries: &'a [NodeServiceRuntimeEntryData],
    now_ns: u64,
) {
    lines.push(Line::from("Runtime services:"));
    if entries.is_empty() {
        lines.push(Line::from("  none".to_string()));
        return;
    }
    for entry in entries.iter().take(8) {
        // last_req/last_reply are wall-clock stamps → render as an age relative
        // to the telemetry snapshot timestamp (`now_ns`).
        lines.push(Line::from(format!(
            "  • {} [{}{}] recv={} dispatched={} ok={} err={} send_fail={} dropped={} inflight={} last_req={} ago last_reply={} ago",
            entry.resource_name,
            entry.resource_instance_id,
            format_schema_suffix(entry.schema_hash.as_deref()),
            entry.request_received_count,
            entry.request_dispatched_count,
            entry.reply_ok_count,
            entry.reply_error_count,
            entry.reply_send_failure_count,
            entry.request_dropped_count,
            entry.in_flight_requests,
            format_stamp_age_ns(now_ns, entry.last_request_ns),
            format_stamp_age_ns(now_ns, entry.last_reply_ns),
        )));
    }
    if entries.len() > 8 {
        lines.push(Line::from(format!(
            "  … {} more",
            entries.len().saturating_sub(8)
        )));
    }
}

fn push_node_runtime_client_lines<'a>(
    lines: &mut Vec<Line<'a>>,
    entries: &'a [NodeClientRuntimeEntryData],
    now_ns: u64,
) {
    lines.push(Line::from("Runtime clients:"));
    if entries.is_empty() {
        lines.push(Line::from("  none".to_string()));
        return;
    }
    for entry in entries.iter().take(8) {
        // last_latency is a true elapsed duration; last_start/last_finish are
        // wall-clock stamps → render as an age relative to `now_ns`.
        lines.push(Line::from(format!(
            "  • {} [{}{}] calls={} ok={} timeout={} conflict={} err={} retry={} last_start={} ago last_finish={} ago last_latency={}",
            entry.resource_name,
            entry.resource_instance_id,
            format_schema_suffix(entry.schema_hash.as_deref()),
            entry.call_count,
            entry.success_count,
            entry.timeout_count,
            entry.concurrent_conflict_count,
            entry.error_count,
            entry.retry_count,
            format_stamp_age_ns(now_ns, entry.last_call_started_ns),
            format_stamp_age_ns(now_ns, entry.last_call_finished_ns),
            format_latency_ns(entry.last_call_latency_ns),
        )));
    }
    if entries.len() > 8 {
        lines.push(Line::from(format!(
            "  … {} more",
            entries.len().saturating_sub(8)
        )));
    }
}

fn format_optional_count(count: Option<usize>) -> String {
    count
        .map(|count| count.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

fn schema_availability_cell<'a>(availability: crate::data::SchemaAvailability) -> Cell<'a> {
    let (label, color) = match availability {
        crate::data::SchemaAvailability::Confirmed => ("ok", Color::Green),
        crate::data::SchemaAvailability::Unknown => ("?", Color::Yellow),
    };
    Cell::from(Span::styled(label.to_string(), Style::default().fg(color)))
}

fn health_cell<'a>(health: HealthLevel, capability: CapabilityState) -> Cell<'a> {
    let (label, color) = match capability {
        CapabilityState::Unavailable => ("N/A", Color::DarkGray),
        CapabilityState::Partial
            if matches!(health, HealthLevel::Healthy | HealthLevel::Unknown) =>
        {
            ("PART", Color::Yellow)
        }
        _ => (
            health.badge(),
            match health {
                HealthLevel::Healthy => Color::Green,
                HealthLevel::Warn => Color::Yellow,
                HealthLevel::Error => Color::Red,
                HealthLevel::Unknown => Color::DarkGray,
            },
        ),
    };
    Cell::from(Span::styled(
        label.to_string(),
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    ))
}

fn format_freshness_with_period(freshness: FreshnessState, period_ms: Option<u32>) -> String {
    match period_ms {
        Some(period_ms) => format!("{} (~{} ms)", freshness.as_label(), period_ms),
        None => freshness.as_label().to_string(),
    }
}

fn highlighted_resource_line<'a>(
    index: usize,
    resource: &'a HighlightedResource,
    is_function_key: bool,
) -> Line<'a> {
    let shortcut = if is_function_key {
        format!("[F{}]", index)
    } else {
        format!("[{}]", index)
    };
    Line::from(vec![
        Span::raw(format!("{shortcut} ")),
        Span::styled(
            format!("{} ", resource.health.badge()),
            Style::default()
                .fg(match resource.health {
                    HealthLevel::Healthy => Color::Green,
                    HealthLevel::Warn => Color::Yellow,
                    HealthLevel::Error => Color::Red,
                    HealthLevel::Unknown => Color::DarkGray,
                })
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("{} — {}", resource.name, resource.reason)),
    ])
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_schema_suffix(schema_hash: Option<&str>) -> String {
    schema_hash
        .map(|hash| {
            let short = hash.get(..8).unwrap_or(hash);
            format!(", schema={short}")
        })
        .unwrap_or_default()
}

fn node_discovery_source_label(source: NodeDiscoverySource) -> &'static str {
    match source {
        NodeDiscoverySource::TelemetrySnapshot => "telemetry exact inventory",
        NodeDiscoverySource::AdminSnapshot => "admin ownership fallback",
        NodeDiscoverySource::LivelinessFallback => "liveliness presence fallback",
    }
}

fn node_discovery_source_note(source: NodeDiscoverySource) -> &'static str {
    match source {
        NodeDiscoverySource::TelemetrySnapshot => {
            "topics and services come from node-reported telemetry inventory"
        }
        NodeDiscoverySource::AdminSnapshot => {
            "node presence is known, but ownership is reconstructed from admin endpoints"
        }
        NodeDiscoverySource::LivelinessFallback => {
            "only node presence is visible; topic and service ownership may be unavailable"
        }
    }
}

fn provenance_explanation(provenance: &str) -> &'static str {
    match provenance {
        "exact" => "telemetry exact ownership",
        "inferred" => "admin ownership fallback",
        "declared" => "admin-declared endpoint ownership",
        "resolved" => "schema_by_key exact match",
        "observed" => "observed from live samples",
        "unavailable" => "not available in the current discovery path",
        _ => "source detail unavailable",
    }
}

fn stat_card<'a>(title: &'a str, value: String) -> Paragraph<'a> {
    Paragraph::new(value)
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .block(Block::default().borders(Borders::ALL).title(title))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::{
        app_shell_title, contextual_hint, help_lines, node_detail_lines,
        node_discovery_source_label, node_discovery_source_note, overview_event_lines,
        overview_event_panel_title, provenance_explanation, push_schema_describe_lines, render,
        render_detail_panel, render_status, render_with_mouse, resource_table_title,
        response_lines, schema_hint_lines, selection_idle_lines, service_detail_lines,
        split_master_detail, status_line, topic_detail_lines,
        topic_preview_decode_fallback_message, topic_preview_lines, topic_preview_status,
        zenoh_detail_lines, zenoh_metrics_capability_note, zenoh_router_detail_lines,
        zenoh_router_primary_label, LayoutMode,
    };
    use crate::app::{
        sample_action_call_schema, sample_action_row, sample_dashboard, App, DetailPane,
        EventSource, ResourceBookmark, Screen, SelectableList,
    };
    use crate::data::{
        ActionCallResponseData, CapabilityState, ExactSchemaAvailability, FreshnessState,
        HealthLevel, NodeDetailData, NodePublisherRuntimeEntryData, NodeRow,
        NodeRuntimeBreakdownData, NodeRuntimeSummaryData, SchemaAvailability, SchemaDescribeData,
        SchemaDescribeKind, SchemaDescribeSection, ServiceCallResponseData, ServiceCallSchemaData,
        ServiceDetailData, ServiceRuntimeSummaryData, TopicDetailData, TopicRow,
        ZenohConnectivityData, ZenohConnectivityEventData, ZenohConnectivityEventKindData,
        ZenohInternalsData, ZenohLinkRow, ZenohRouterRow, ZenohTransportRow,
    };
    use crate::mouse::{MouseTarget, MouseUiState, ScrollTarget};
    use aorta_introspect::client::{
        NodeDiscoverySource, TopicPreviewDecodeStatus, TopicPreviewFrame, ZenohRouterMetrics,
    };
    use aorta_introspect::ids::TopicSuffix;
    use aorta_introspect::schema::{SchemaField, SchemaModel, SchemaTypeKind};
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;
    use ratatui::layout::{Constraint, Direction, Layout, Rect};
    use ratatui::text::Line;
    use ratatui::widgets::{Block, Borders};
    use ratatui::Terminal;

    #[test]
    fn telemetry_discovery_label_is_user_facing() {
        assert_eq!(
            node_discovery_source_label(NodeDiscoverySource::TelemetrySnapshot),
            "telemetry exact inventory"
        );
        assert_eq!(
            node_discovery_source_note(NodeDiscoverySource::TelemetrySnapshot),
            "topics and services come from node-reported telemetry inventory"
        );
    }

    #[test]
    fn admin_fallback_and_liveliness_labels_are_distinct() {
        assert_eq!(
            node_discovery_source_label(NodeDiscoverySource::AdminSnapshot),
            "admin ownership fallback"
        );
        assert_eq!(
            node_discovery_source_label(NodeDiscoverySource::LivelinessFallback),
            "liveliness presence fallback"
        );
    }

    #[test]
    fn provenance_explanations_match_exact_and_fallback_paths() {
        assert_eq!(provenance_explanation("exact"), "telemetry exact ownership");
        assert_eq!(
            provenance_explanation("inferred"),
            "admin ownership fallback"
        );
        assert_eq!(
            provenance_explanation("unavailable"),
            "not available in the current discovery path"
        );
    }

    #[test]
    fn layout_mode_thresholds_are_explicit() {
        assert_eq!(LayoutMode::from_width(80), LayoutMode::Compact);
        assert_eq!(LayoutMode::from_width(100), LayoutMode::Standard);
        assert_eq!(LayoutMode::from_width(140), LayoutMode::Wide);
    }

    #[test]
    fn contextual_hint_changes_with_interaction_mode() {
        let mut app = App::default();
        assert!(contextual_hint(&app, LayoutMode::Wide).contains("e timeline"));
        assert!(contextual_hint(&app, LayoutMode::Wide).contains(": palette"));
        assert!(contextual_hint(&app, LayoutMode::Wide).contains("? help"));
        assert_eq!(
            contextual_hint(&app, LayoutMode::Compact),
            "Enter/click • wheel • T/S/N • C • e • o • : • ?"
        );

        assert!(app.open_event_timeline());
        assert_eq!(
            contextual_hint(&app, LayoutMode::Wide),
            "j/k or wheel select • click inspect • s severity • o source • c clear • Esc close"
        );
        assert_eq!(
            contextual_hint(&app, LayoutMode::Compact),
            "wheel/click • s/o • c • Esc"
        );

        app.close_event_timeline();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.open_topology_overlay().expect("open topology");
        assert_eq!(
            contextual_hint(&app, LayoutMode::Wide),
            "Esc/g close • use current resource selection as topology root"
        );
        assert_eq!(contextual_hint(&app, LayoutMode::Compact), "Esc/g close");

        app.close_topology_overlay();
        app.open_recorder_overlay().expect("open recorder");
        assert_eq!(
            contextual_hint(&app, LayoutMode::Wide),
            "y export script • Esc/o close • use current selection as recorder scope"
        );
        assert_eq!(
            contextual_hint(&app, LayoutMode::Compact),
            "y export • Esc/o close"
        );

        app.close_recorder_overlay();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Nodes).expect("open nodes");
        app.toggle_node_compare_anchor().expect("anchor node");
        app.select_next();
        app.open_node_comparison().expect("open compare");
        assert_eq!(
            contextual_hint(&app, LayoutMode::Wide),
            "Esc/c close • x retarget anchor from Nodes list"
        );

        app.close_node_comparison();
        app.open_bookmark_overlay();
        assert_eq!(
            contextual_hint(&app, LayoutMode::Wide),
            "j/k or wheel select • Enter/click open • d remove • Esc close"
        );

        app.close_bookmark_overlay();
        app.open_command_palette();
        assert_eq!(
            contextual_hint(&app, LayoutMode::Wide),
            "type to filter • j/k or wheel select • Enter/click run • Esc close"
        );

        app.enter_search();
        assert_eq!(
            contextual_hint(&app, LayoutMode::Wide),
            "type to filter • wheel move • Backspace • Enter keep • Esc close"
        );

        app.exit_search();
        app.next_screen();
        app.next_screen();
        app.next_screen();
        assert!(app.open_service_call_from_selected_service());
        assert_eq!(
            contextual_hint(&app, LayoutMode::Wide),
            "hint list wheel/click • response wheel/PgUp/PgDn • i edit • a apply • y export • R reset • F5 send • : palette • ? help"
        );
        assert_eq!(
            contextual_hint(&app, LayoutMode::Compact),
            "hints/resp wheel • i • a • y • F5 • ?"
        );

        app.enter_service_call_edit();
        assert_eq!(
            contextual_hint(&app, LayoutMode::Wide),
            "edit JSON • a apply hint • F5 send • Esc finish edit"
        );
        assert_eq!(
            contextual_hint(&app, LayoutMode::Compact),
            "edit JSON • F5 send • Esc"
        );
    }

    #[test]
    fn app_shell_title_reflects_advanced_and_help_state() {
        let mut app = App::default();
        assert_eq!(
            app_shell_title(&app),
            "Aorta TUI | group=default | health=unknown | runtime=unavailable"
        );

        app.toggle_advanced_mode();
        assert_eq!(
            app_shell_title(&app),
            "Aorta TUI | group=default | health=unknown | runtime=unavailable | advanced"
        );

        app.toggle_help();
        assert_eq!(
            app_shell_title(&app),
            "Aorta TUI | group=default | health=unknown | runtime=unavailable | advanced | help"
        );
    }

    #[test]
    fn help_lines_include_group_screen_and_layout_context() {
        let mut app = App::default();
        app.set_group("robot_a");
        let lines = help_lines(&app, LayoutMode::Wide);
        let rendered: Vec<String> = lines.iter().map(|line| line.to_string()).collect();

        assert!(rendered[0].contains("Overview"));
        assert!(rendered[0].contains("robot_a"));
        assert!(rendered[0].contains("wide"));
        assert!(rendered[2].contains("Overview health"));
        assert!(rendered
            .iter()
            .any(|line| line.contains("e open diagnostics timeline")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("g open topology explorer")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("o open recorder launchpad")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("T / S / N jump to Topics / Services / Nodes")));
        assert!(rendered
            .iter()
            .any(|line| line.contains(": open command palette")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("b open bookmarks overlay")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("click tabs to switch screens")));
        assert!(rendered.iter().any(|line| line
            .contains("click list rows to inspect resources; wheel changes list selection")));
    }

    #[test]
    fn command_palette_overlay_renders_query_actions_and_description() {
        let mut app = App::default();
        assert!(app.open_command_palette());
        for ch in "topics".chars() {
            app.push_command_palette_char(ch);
        }

        let rendered = render_view(&app, 120, 40);
        assert!(rendered.contains("Command Palette"));
        assert!(rendered.contains("Filter"));
        assert!(rendered.contains(":topics"));
        assert!(rendered.contains("Actions"));
        assert!(rendered.contains("click"));
        assert!(rendered.contains("Open Topics"));
        assert!(rendered.contains("Switch to the Topics screen."));
    }

    #[test]
    fn command_palette_overlay_shows_empty_state_when_filter_has_no_match() {
        let mut app = App::default();
        assert!(app.open_command_palette());
        for ch in ['~', '~', '~'] {
            app.push_command_palette_char(ch);
        }

        let rendered = render_view(&app, 120, 40);
        assert!(rendered.contains("No action matches the current query."));
        assert!(rendered.contains("No actions match current filter."));
    }

    #[test]
    fn command_palette_overlay_can_surface_macro_actions() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        assert!(app.open_command_palette());
        for ch in "macro".chars() {
            app.push_command_palette_char(ch);
        }

        let rendered = render_view(&app, 120, 40);
        assert!(rendered.contains("Macro: Find Topic"));
        assert!(rendered.contains("<T>"));
    }

    #[test]
    fn telemetry_screen_renders_sparkline_and_percentile_readout() {
        let mut app = App::default();
        // Telemetry is an advanced-mode diagnostics tab.
        app.toggle_advanced_mode();
        app.open_screen(Screen::Telemetry).expect("open telemetry");

        // Seed a per-resource history ring with a low-skewed spread of distinct
        // client-observed latencies (dedup on the advancing finished stamp).
        let samples: [u64; 6] = [1_000, 1_200, 1_500, 50_000, 2_000, 1_800];
        for (idx, latency) in samples.iter().enumerate() {
            app.observe_latency_sample("client:nav/plan", *latency, (idx as u64) + 1);
        }

        // The accumulated ring is reflected in the read-only history accessor.
        let history = app.latency_history();
        assert_eq!(history.len(), 1);
        let (label, ring) = history.iter().next().expect("one tracked resource");
        assert_eq!(label, "client:nav/plan");
        assert_eq!(ring.samples().len(), 6, "all six distinct samples retained");

        let rendered = render_view(&app, 120, 40);
        // Banner labels the readout as approximate + client-observed.
        assert!(rendered.contains("Latency Telemetry (approx, client-observed)"));
        // The tracked resource label and a percentile readout line are present.
        assert!(rendered.contains("client:nav/plan"));
        assert!(rendered.contains("p50"));
        assert!(rendered.contains("p95"));
        assert!(rendered.contains("p99"));
        assert!(rendered.contains("approx"));
        assert!(rendered.contains("6 samples"));
        // The Sparkline widget renders at least one bar glyph from its block set.
        const BAR_GLYPHS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
        assert!(
            rendered.chars().any(|ch| BAR_GLYPHS.contains(&ch)),
            "expected a sparkline bar glyph in the rendered Telemetry screen"
        );
    }

    #[test]
    fn telemetry_screen_renders_empty_state_without_samples() {
        let mut app = App::default();
        app.toggle_advanced_mode();
        app.open_screen(Screen::Telemetry).expect("open telemetry");

        let rendered = render_view(&app, 120, 40);
        assert!(rendered.contains("Latency Telemetry (approx, client-observed)"));
        assert!(rendered.contains("No client-observed latency samples yet."));
    }

    #[test]
    fn render_with_mouse_registers_tab_and_overview_action_regions() {
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let app = App::default();
        let mut mouse = MouseUiState::default();

        terminal
            .draw(|frame| render_with_mouse(frame, &app, &mut mouse))
            .expect("draw");

        assert_eq!(
            mouse.click_target(14, 1),
            Some(MouseTarget::Tab(Screen::Topics))
        );
        // The Context tab is registered somewhere in the (now longer) tab strip;
        // scan for it rather than hard-coding a shifting pixel column.
        let context_registered =
            (0..120).any(|x| mouse.click_target(x, 1) == Some(MouseTarget::Tab(Screen::Context)));
        assert!(context_registered);
        // Far right of the tab strip is empty in default (non-advanced) mode.
        assert_eq!(mouse.click_target(119, 1), None);

        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(3),
            ])
            .split(Rect::new(0, 0, 120, 40));
        let content = layout[1];
        let overview = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(5), Constraint::Min(6)])
            .split(content);
        let details = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(44), Constraint::Percentage(56)])
            .split(overview[1]);
        let actions = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(9), Constraint::Min(6)])
            .split(details[0])[0];
        let inner = Block::default().borders(Borders::ALL).inner(actions);

        assert_eq!(
            mouse.click_target(inner.x + 1, inner.y),
            Some(MouseTarget::OverviewAction(0))
        );
        assert_eq!(
            mouse.scroll_target(inner.x + 1, inner.y),
            Some(ScrollTarget::OverviewActions)
        );
    }

    #[test]
    fn render_with_mouse_exposes_actions_tab_before_nodes_tab() {
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let app = App::default();
        let mut mouse = MouseUiState::default();

        terminal
            .draw(|frame| render_with_mouse(frame, &app, &mut mouse))
            .expect("draw");

        let mut ordered_tabs = Vec::new();
        for x in 0..120 {
            if let Some(MouseTarget::Tab(screen)) = mouse.click_target(x, 1) {
                if ordered_tabs.last() != Some(&screen) {
                    ordered_tabs.push(screen);
                }
            }
        }

        let actions_index = ordered_tabs
            .iter()
            .position(|screen| *screen == Screen::Actions)
            .expect("actions tab");
        let nodes_index = ordered_tabs
            .iter()
            .position(|screen| *screen == Screen::Nodes)
            .expect("nodes tab");
        assert!(actions_index < nodes_index);
    }

    #[test]
    fn render_with_mouse_registers_topic_table_and_preview_scroll_regions() {
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        let mut mouse = MouseUiState::default();

        terminal
            .draw(|frame| render_with_mouse(frame, &app, &mut mouse))
            .expect("draw");

        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(3),
            ])
            .split(Rect::new(0, 0, 120, 40));
        let content = layout[1];
        let master_detail = split_master_detail(content);
        let list_inner = Block::default()
            .borders(Borders::ALL)
            .inner(master_detail[0]);

        assert_eq!(
            mouse.click_target(list_inner.x + 1, list_inner.y + 1),
            Some(MouseTarget::TopicRow(0))
        );
        assert_eq!(
            mouse.scroll_target(list_inner.x + 1, list_inner.y + 1),
            Some(ScrollTarget::TopicList)
        );

        let right = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(8), Constraint::Min(8)])
            .split(master_detail[1]);
        let preview_inner = Block::default().borders(Borders::ALL).inner(right[1]);
        assert_eq!(
            mouse.scroll_target(preview_inner.x + 1, preview_inner.y + 1),
            Some(ScrollTarget::TopicPreview)
        );
    }

    #[test]
    fn diagnostics_timeline_overlay_renders_filters_and_selected_event() {
        let mut app = App::default();
        app.report_info_from(EventSource::Data, "snapshot refreshed".to_string());
        app.report_error_from(EventSource::ServiceCall, "call failed".to_string());
        assert!(app.open_event_timeline());

        let rendered = render_view(&app, 120, 40);
        assert!(rendered.contains("Diagnostics Timeline"));
        assert!(rendered.contains("click inspect"));
        assert!(rendered.contains("severity=all"));
        assert!(rendered.contains("source=all"));
        assert!(rendered.contains("[ERROR] [service]"));
        assert!(rendered.contains("call failed"));
    }

    #[test]
    fn render_with_mouse_registers_service_call_hint_regions() {
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.next_screen();
        assert!(app.open_service_call_from_selected_service());
        app.exit_service_call_edit();
        app.set_service_call_schema(DetailPane::Loaded(crate::app::sample_service_call_schema()));
        let mut mouse = MouseUiState::default();

        terminal
            .draw(|frame| render_with_mouse(frame, &app, &mut mouse))
            .expect("draw");

        let area = Rect::new(0, 0, 120, 40);
        let outer = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(3),
            ])
            .split(area);
        let service_call = outer[1];
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(6),
                Constraint::Min(12),
                Constraint::Length(10),
            ])
            .split(service_call);
        let center = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(layout[1]);
        let hints = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(9), Constraint::Length(8)])
            .split(center[1])[1];
        let hint_inner = Block::default().borders(Borders::ALL).inner(hints);

        assert_eq!(
            mouse.click_target(hint_inner.x + 1, hint_inner.y),
            Some(MouseTarget::ServiceCallHintRow(0))
        );
        assert_eq!(
            mouse.scroll_target(hint_inner.x + 1, hint_inner.y),
            Some(ScrollTarget::ServiceCallHintList)
        );
        let response_inner = Block::default().borders(Borders::ALL).inner(layout[2]);
        assert_eq!(
            mouse.scroll_target(response_inner.x + 1, response_inner.y),
            Some(ScrollTarget::ServiceCallResponse)
        );
    }

    #[test]
    fn recorder_overlay_renders_selection_aware_commands_and_notes() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.open_recorder_overlay().expect("open recorder");

        let rendered = render_view(&app, 140, 42);
        assert!(rendered.contains("Recorder Launchpad"));
        assert!(rendered.contains("Scope: topic"));
        assert!(rendered.contains("Topic imu/data"));
        assert!(rendered.contains("aorta record --group 'default'"));
        assert!(rendered.contains("--include 'imu/data'"));
        assert!(rendered.contains("Esc/o close"));
    }

    #[test]
    fn diagnostics_timeline_overlay_shows_empty_state_for_strict_filters() {
        let mut app = App::default();
        app.report_info_from(EventSource::Data, "snapshot refreshed".to_string());
        assert!(app.open_event_timeline());
        app.cycle_event_timeline_severity_filter();

        let rendered = render_view(&app, 120, 40);
        assert!(rendered.contains("No events match the current filters."));
        assert!(rendered.contains("Adjust severity/source filters"));
    }

    #[test]
    fn topology_overlay_renders_selected_topic_relationships() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.set_topic_detail(DetailPane::Loaded(TopicDetailData {
            name: "imu/data".to_string(),
            key_expr: "aorta/default/pub/imu/data".to_string(),
            provider_endpoint_count: 1,
            candidate_node_count: 1,
            publisher_node_count: Some(1),
            subscriber_node_count: Some(1),
            schema_availability: ExactSchemaAvailability::Confirmed {
                hash: "deadbeef".to_string(),
            },
            schema_provenance: "exact",
            provider_endpoint_provenance: "declared",
            candidate_node_provenance: "exact",
            publisher_node_provenance: "exact",
            subscriber_node_provenance: "inferred",
            provider_endpoint_ids: vec!["ep-camera".to_string()],
            candidate_node_names: vec!["planner".to_string()],
            publisher_node_names: vec!["planner".to_string()],
            subscriber_node_names: vec!["perception".to_string()],
            schema_note: None,
            candidate_nodes_note: None,
            publisher_nodes_note: None,
            subscriber_nodes_note: Some(
                "subscriber ownership inferred from admin snapshot".to_string(),
            ),
            runtime_summary: None,
            runtime_provenance: "unavailable",
            runtime_note: None,
        }));
        app.open_topology_overlay().expect("open topology");

        let rendered = render_view(&app, 140, 42);
        assert!(rendered.contains("Topology Explorer"));
        assert!(rendered.contains("Root: Topic imu/data"));
        assert!(rendered.contains("topic imu/data"));
        assert!(rendered.contains("provider endpoints"));
    }

    #[test]
    fn help_overlay_takes_priority_over_command_palette_overlay() {
        let mut app = App::default();
        assert!(app.open_command_palette());
        app.toggle_help();

        let rendered = render_view(&app, 120, 40);
        assert!(rendered.contains("Help"));
        assert!(!rendered.contains("Command Palette"));
    }

    #[test]
    fn bookmarks_overlay_renders_entries_and_missing_state() {
        let mut app = App::default();
        app.set_group("robot_a");
        app.replace_dashboard(sample_dashboard());
        app.replace_bookmarks(vec![
            ResourceBookmark::topic("robot_a", "camera/front"),
            ResourceBookmark::service("robot_a", "missing/service"),
        ]);
        assert!(app.open_bookmark_overlay());

        let rendered = render_view(&app, 120, 40);
        assert!(rendered.contains("Bookmarks | group=robot_a"));
        assert!(rendered.contains("★ camera/front"));
        assert!(rendered.contains("click"));
        assert!(rendered.contains("[Topic | ready]"));
    }

    #[test]
    fn compact_node_render_shows_collapsed_runtime_when_preference_is_disabled() {
        let selected = sample_dashboard().nodes[0].clone();
        let detail = DetailPane::Loaded(sample_runtime_node_detail(&selected));
        let lines = node_detail_lines(&selected, &detail, false, false);
        let rendered = render_detail_view("Node Detail", lines, 80, 80);

        assert!(rendered.contains("press `m` to expand"));
        assert!(!rendered.contains("Runtime publishers:"));
    }

    #[test]
    fn compact_node_render_shows_runtime_breakdown_when_expanded() {
        let selected = sample_dashboard().nodes[0].clone();
        let detail = DetailPane::Loaded(sample_runtime_node_detail(&selected));
        let lines = node_detail_lines(&selected, &detail, true, true);
        let rendered = render_detail_view("Node Detail", lines, 80, 80);

        assert!(rendered.contains("Runtime breakdown"));
        assert!(rendered.contains("Runtime publishers:"));
        assert!(rendered.contains("camera/front"));
        assert!(rendered.contains("Pinned: yes"));
    }

    #[test]
    fn topic_screen_keeps_runtime_note_without_cross_screen_visibility_diagnosis() {
        let selected = sample_dashboard().topics[0].clone();
        let detail = DetailPane::Loaded(TopicDetailData {
            name: "imu/data".to_string(),
            key_expr: "aorta/default/pub/imu/data".to_string(),
            provider_endpoint_count: 1,
            candidate_node_count: 1,
            publisher_node_count: Some(1),
            subscriber_node_count: None,
            schema_availability: ExactSchemaAvailability::Confirmed {
                hash: "deadbeef".to_string(),
            },
            schema_provenance: "exact",
            provider_endpoint_provenance: "declared",
            candidate_node_provenance: "inferred",
            publisher_node_provenance: "inferred",
            subscriber_node_provenance: "unavailable",
            provider_endpoint_ids: vec!["ep-imu".to_string()],
            candidate_node_names: vec!["planner".to_string()],
            publisher_node_names: vec!["planner".to_string()],
            subscriber_node_names: Vec::new(),
            schema_note: None,
            candidate_nodes_note: Some(
                "candidate nodes are inferred from admin endpoint ownership".to_string(),
            ),
            publisher_nodes_note: Some(
                "publisher nodes are inferred from admin endpoint ownership".to_string(),
            ),
            subscriber_nodes_note: Some(
                "subscriber nodes are unavailable outside telemetry snapshot mode".to_string(),
            ),
            runtime_summary: None,
            runtime_provenance: "unavailable",
            runtime_note: Some(
                "topic runtime metrics unavailable outside telemetry snapshot mode".to_string(),
            ),
        });
        let lines = topic_detail_lines(&selected, &detail, &DetailPane::Empty, true, false);
        let rendered = render_detail_view("Topic Detail", lines, 120, 80);
        assert!(
            rendered.contains("topic runtime metrics unavailable outside telemetry snapshot mode")
        );
        assert!(!rendered.contains("Visibility diagnosis"));
    }

    #[test]
    fn service_screen_keeps_runtime_summary_without_cross_screen_visibility_diagnosis() {
        let selected = sample_dashboard().services[0].clone();
        let detail = DetailPane::Loaded(ServiceDetailData {
            name: "nav/plan".to_string(),
            key_expr: "aorta/default/srv/nav/plan".to_string(),
            provider_endpoint_count: 1,
            candidate_node_count: 1,
            provider_node_count: Some(1),
            client_node_count: Some(1),
            schema_availability: ExactSchemaAvailability::Confirmed {
                hash: "deadbeef".to_string(),
            },
            schema_provenance: "exact",
            provider_endpoint_provenance: "declared",
            candidate_node_provenance: "exact",
            provider_node_provenance: "exact",
            client_node_provenance: "exact",
            schema_hash: Some("deadbeef".to_string()),
            request_root_type: Some("PlanRequest".to_string()),
            response_root_type: Some("PlanResponse".to_string()),
            provider_endpoint_ids: vec!["ep-plan".to_string()],
            candidate_node_names: vec!["planner".to_string()],
            provider_node_names: vec!["planner".to_string()],
            client_node_names: vec!["mission".to_string()],
            schema_note: None,
            candidate_nodes_note: None,
            provider_nodes_note: None,
            client_nodes_note: None,
            runtime_summary: Some(ServiceRuntimeSummaryData {
                latest_timestamp_ns: 123456789,
                collection_period_ms: 1000,
                service_metric_count: 1,
                client_metric_count: 1,
                service_request_received_count: 10,
                service_request_dispatched_count: 10,
                service_reply_ok_count: 9,
                service_reply_error_count: 1,
                service_reply_send_failure_count: 0,
                service_request_dropped_count: 0,
                service_in_flight_requests: 0,
                client_call_count: 8,
                client_success_count: 7,
                client_timeout_count: 1,
                client_concurrent_conflict_count: 0,
                client_error_count: 0,
                client_retry_count: 0,
                client_last_call_latency_ns: 2_500_000,
                client_last_call_finished_ns: 123_456_789,
                service_last_request_ns: 123_455_789,
                service_last_reply_ns: 123_456_789,
            }),
            runtime_provenance: "exact",
            runtime_note: None,
        });
        let rendered: Vec<String> =
            service_detail_lines(&selected, &detail, &DetailPane::Empty, true, false)
                .into_iter()
                .map(|line| line.to_string())
                .collect();
        assert!(!rendered
            .iter()
            .any(|line| line.contains("Visibility diagnosis")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Service summary: recv=10")));
        // A2: latency readout — last_call is a duration (2.5ms); last_req/last_reply
        // are wall-clock stamps shown as an age vs latest_timestamp_ns (123456789).
        assert!(
            rendered.iter().any(|line| line.contains(
                "Latency: last_call=2.500ms | last_req=1.000us ago | last_reply=0ns ago"
            )),
            "latency line missing/mismatched: {:?}",
            rendered
        );
    }

    #[test]
    fn resource_table_title_includes_visible_and_total_counts() {
        let mut list = SelectableList::default();
        list.replace(vec![
            TopicRow {
                name: "camera/front".to_string(),
                provider_endpoint_count: 1,
                candidate_node_count: 1,
                publisher_node_count: Some(1),
                subscriber_node_count: Some(1),
                schema_availability: SchemaAvailability::Confirmed,
                schema_note: None,
                health: HealthLevel::Healthy,
                capability: CapabilityState::Full,
                freshness: FreshnessState::Fresh,
                runtime_collection_period_ms: Some(1000),
                status_summary: "runtime ok (~1000 ms)".to_string(),
            },
            TopicRow {
                name: "imu/data".to_string(),
                provider_endpoint_count: 1,
                candidate_node_count: 1,
                publisher_node_count: Some(1),
                subscriber_node_count: Some(1),
                schema_availability: SchemaAvailability::Confirmed,
                schema_note: None,
                health: HealthLevel::Healthy,
                capability: CapabilityState::Full,
                freshness: FreshnessState::Fresh,
                runtime_collection_period_ms: Some(1000),
                status_summary: "runtime ok (~1000 ms)".to_string(),
            },
        ]);
        list.set_query("camera".to_string());

        assert_eq!(
            resource_table_title("Topics", &list, "name"),
            "Topics [1 visible / 2 total • filter=camera • click inspect • wheel select • `/` search name]"
        );
    }

    #[test]
    fn help_lines_use_consistent_mouse_terms_for_open_run_and_inspect() {
        let app = App::default();
        let rendered: Vec<String> = help_lines(&app, LayoutMode::Wide)
            .into_iter()
            .map(|line| line.to_string())
            .collect();

        assert!(rendered
            .iter()
            .any(|line| line.contains("click list rows to inspect resources")));
        assert!(rendered.iter().any(|line| line
            .contains("click quick actions or bookmarks to open; click palette actions to run")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Topic Preview or Response")));
    }

    #[test]
    fn selection_idle_lines_explain_empty_and_filtered_states() {
        let empty_list: SelectableList<TopicRow> = SelectableList::default();
        let empty_lines: Vec<String> = selection_idle_lines("topic", "Topics", &empty_list)
            .into_iter()
            .map(|line| line.to_string())
            .collect();
        assert!(empty_lines[0].contains("No topics discovered in the current snapshot"));

        let mut filtered_list = SelectableList::default();
        filtered_list.replace(vec![TopicRow {
            name: "camera/front".to_string(),
            provider_endpoint_count: 1,
            candidate_node_count: 1,
            publisher_node_count: Some(1),
            subscriber_node_count: Some(1),
            schema_availability: SchemaAvailability::Confirmed,
            schema_note: None,
            health: HealthLevel::Healthy,
            capability: CapabilityState::Full,
            freshness: FreshnessState::Fresh,
            runtime_collection_period_ms: Some(1000),
            status_summary: "runtime ok (~1000 ms)".to_string(),
        }]);
        filtered_list.set_query("imu".to_string());
        let filtered_lines: Vec<String> = selection_idle_lines("topic", "Topics", &filtered_list)
            .into_iter()
            .map(|line| line.to_string())
            .collect();
        assert!(filtered_lines[0].contains("No topics match the current filter"));
    }

    #[test]
    fn overview_event_panel_prioritizes_warnings_when_present() {
        let mut app = App::default();
        app.report_info("snapshot refreshed".to_string());
        app.report_warn("runtime telemetry degraded".to_string());

        assert_eq!(overview_event_panel_title(&app), "Recent Alerts");
        let lines: Vec<String> = overview_event_lines(&app)
            .into_iter()
            .map(|line| line.to_string())
            .collect();
        assert!(lines.iter().any(|line| line.contains("[WARN|system]")));
        assert!(lines
            .iter()
            .any(|line| line.contains("runtime telemetry degraded")));
        assert!(!lines[0].contains("No active alerts"));
    }

    #[test]
    fn overview_event_panel_falls_back_to_info_feed_when_no_warning_exists() {
        let mut app = App::default();
        app.report_info("snapshot refreshed".to_string());

        assert_eq!(overview_event_panel_title(&app), "Event Log");
        let lines: Vec<String> = overview_event_lines(&app)
            .into_iter()
            .map(|line| line.to_string())
            .collect();
        assert!(lines[0].contains("No active alerts"));
    }

    #[test]
    fn topic_preview_status_reports_waiting_and_history_states() {
        let mut app = App::default();
        app.begin_topic_preview("imu/data".to_string());
        assert_eq!(
            topic_preview_status(app.topic_preview()),
            "waiting for first sample"
        );

        for payload_hex in ["00", "01"] {
            app.apply_topic_preview_frame(TopicPreviewFrame {
                topic: TopicSuffix::new("imu/data").expect("topic"),
                key_expr: "aorta/default/pub/imu/data".to_string(),
                payload_hex: payload_hex.to_string(),
                decoded_json: Some(format!("{{\"hex\":\"{}\"}}", payload_hex)),
                schema_hash: None,
                decode_status: TopicPreviewDecodeStatus::RawOnly,
            });
        }
        app.select_older_topic_preview_frame();

        assert_eq!(
            topic_preview_status(app.topic_preview()),
            "history (older frame selected)"
        );
    }

    #[test]
    fn topic_preview_decode_fallback_message_is_actionable() {
        assert!(
            topic_preview_decode_fallback_message(&TopicPreviewDecodeStatus::RawOnly)
                .contains("press `v`")
        );
        assert!(
            topic_preview_decode_fallback_message(&TopicPreviewDecodeStatus::DecodeError(
                "bad payload".to_string()
            ))
            .contains("bad payload")
        );
    }

    #[test]
    fn topic_preview_diff_view_renders_adjacent_frame_changes() {
        let mut app = App::default();
        app.begin_topic_preview("imu/data".to_string());
        app.apply_topic_preview_frame(TopicPreviewFrame {
            topic: TopicSuffix::new("imu/data").expect("topic"),
            key_expr: "aorta/default/pub/imu/data".to_string(),
            payload_hex: "00".to_string(),
            decoded_json: Some("{\"x\":1,\"stable\":true}".to_string()),
            schema_hash: None,
            decode_status: TopicPreviewDecodeStatus::DecodedFromTopicSchema,
        });
        app.apply_topic_preview_frame(TopicPreviewFrame {
            topic: TopicSuffix::new("imu/data").expect("topic"),
            key_expr: "aorta/default/pub/imu/data".to_string(),
            payload_hex: "01".to_string(),
            decoded_json: Some("{\"x\":2,\"stable\":true}".to_string()),
            schema_hash: None,
            decode_status: TopicPreviewDecodeStatus::DecodedFromTopicSchema,
        });
        app.toggle_topic_preview_view_mode();
        app.toggle_topic_preview_view_mode();

        let rendered: Vec<String> = topic_preview_lines(app.topic_preview())
            .into_iter()
            .map(|line| line.to_string())
            .collect();
        assert!(rendered.iter().any(|line| line.contains("View: diff")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Diff: frame 2 vs previous frame 1")));
        assert!(rendered.iter().any(|line| line.contains("-   \"x\": 1")));
        assert!(rendered.iter().any(|line| line.contains("+   \"x\": 2")));
    }

    #[test]
    fn topic_preview_diff_view_guards_large_payloads() {
        let mut app = App::default();
        app.begin_topic_preview("imu/data".to_string());
        let large = "x".repeat(16 * 1024 + 8);
        app.apply_topic_preview_frame(TopicPreviewFrame {
            topic: TopicSuffix::new("imu/data").expect("topic"),
            key_expr: "aorta/default/pub/imu/data".to_string(),
            payload_hex: "00".to_string(),
            decoded_json: Some(format!("{{\"blob\":\"{}\"}}", large)),
            schema_hash: None,
            decode_status: TopicPreviewDecodeStatus::DecodedFromTopicSchema,
        });
        app.apply_topic_preview_frame(TopicPreviewFrame {
            topic: TopicSuffix::new("imu/data").expect("topic"),
            key_expr: "aorta/default/pub/imu/data".to_string(),
            payload_hex: "01".to_string(),
            decoded_json: Some("{\"blob\":\"small\"}".to_string()),
            schema_hash: None,
            decode_status: TopicPreviewDecodeStatus::DecodedFromTopicSchema,
        });
        app.toggle_topic_preview_view_mode();
        app.toggle_topic_preview_view_mode();

        let rendered: Vec<String> = topic_preview_lines(app.topic_preview())
            .into_iter()
            .map(|line| line.to_string())
            .collect();
        assert!(rendered
            .iter()
            .any(|line| line.contains("Diff unavailable: payload exceeds")));
    }

    #[test]
    fn node_comparison_overlay_renders_anchor_and_current_summary() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Nodes).expect("open nodes");
        app.toggle_node_compare_anchor().expect("anchor node");
        app.select_next();
        app.open_node_comparison().expect("open compare");

        let rendered = render_view(&app, 140, 42);
        assert!(rendered.contains("Node Comparison"));
        assert!(rendered.contains("Anchor: planner | Current: perception"));
        assert!(rendered.contains("Differences:"));
        assert!(rendered.contains("Anchor • planner"));
        assert!(rendered.contains("Current • perception"));
        assert!(rendered.contains("comparison anchor"));
        assert!(rendered.contains("current selection"));
    }

    #[test]
    fn help_lines_mark_zenoh_as_expert_only() {
        let mut app = App::default();
        app.toggle_advanced_mode();
        app.open_screen(Screen::Zenoh).expect("open zenoh");
        assert_eq!(app.active_screen(), Screen::Zenoh);
        let rendered: Vec<String> = help_lines(&app, LayoutMode::Wide)
            .into_iter()
            .map(|line| line.to_string())
            .collect();
        assert!(rendered
            .iter()
            .any(|line| line.contains("expert-only diagnostics for router/admin checks")));
    }

    #[test]
    fn help_lines_include_node_comparison_shortcuts() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Nodes).expect("open nodes");

        let rendered: Vec<String> = help_lines(&app, LayoutMode::Wide)
            .into_iter()
            .map(|line| line.to_string())
            .collect();
        assert!(rendered
            .iter()
            .any(|line| line.contains("x set or clear the selected node as comparison anchor")));
        assert!(
            rendered
                .iter()
                .any(|line| line
                    .contains("c open side-by-side summary against the comparison anchor"))
        );
    }

    #[test]
    fn footer_status_bar_renders_visible_text() {
        let app = App::default();
        let rendered = render_status_view(&app, 160, 3);
        assert!(rendered.contains("Ready."));
        assert!(rendered.chars().any(|ch| ch.is_ascii_alphabetic()));
    }

    #[test]
    fn status_line_surfaces_auto_refresh_state() {
        let mut app = App::default();
        let off = status_line(&app, LayoutMode::Wide, 240);
        assert!(
            off.contains("auto-refresh: off"),
            "expected off indicator, got: {off}"
        );

        app.toggle_auto_refresh();
        let on = status_line(&app, LayoutMode::Wide, 240);
        assert!(
            on.contains("auto-refresh: on"),
            "expected on indicator, got: {on}"
        );
    }

    #[test]
    fn compact_status_line_keeps_mode_hint_and_short_status() {
        let app = App::default();
        let rendered = status_line(&app, LayoutMode::Compact, 60);

        assert!(rendered.contains("NORMAL"));
        assert!(rendered.contains("ok"));
        assert!(rendered.contains("Enter/click"));
        assert!(!rendered.contains("Ready. Press ? for help."));
        assert!(rendered.chars().count() <= 60);
    }

    #[test]
    fn compact_status_line_prioritizes_non_ready_status_summary() {
        let mut app = App::default();
        app.report_error_from(
            EventSource::Data,
            "telemetry refresh degraded because response exceeded limit".to_string(),
        );

        let rendered = status_line(&app, LayoutMode::Compact, 36);

        assert!(rendered.contains("NORMAL"));
        assert!(rendered.contains("telemetry"));
        assert!(!rendered.contains("ok"));
        assert!(rendered.chars().count() <= 36);
    }

    #[test]
    fn topic_screen_titles_expose_read_only_and_scrollable_semantics() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");

        let rendered = render_view(&app, 140, 42);
        assert!(rendered.contains("Topic Detail (read-only, wheel scroll)"));
        assert!(rendered.contains("Topic Preview (read-only, wheel scroll)"));
    }

    #[test]
    fn service_call_titles_expose_actionable_and_read_only_semantics() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.next_screen();
        assert!(app.open_service_call_from_selected_service());
        app.exit_service_call_edit();
        app.set_service_call_schema(DetailPane::Loaded(crate::app::sample_service_call_schema()));

        let rendered = render_view(&app, 140, 42);
        assert!(rendered.contains("Service Call Summary (read-only)"));
        assert!(rendered.contains("Request Hints (selected field detail, read-only)"));
        assert!(rendered.contains("Field List (`click`/wheel select, `a` apply)"));
        assert!(rendered.contains("Response (read-only, wheel scroll)"));
    }

    #[test]
    fn action_call_screen_renders_editor_hints_and_response_panes() {
        let mut app = App::default();
        app.replace_actions(vec![sample_action_row()]);
        app.open_screen(Screen::Actions).expect("open actions");
        assert!(app.open_action_call_from_selected_action());
        app.set_action_call_schema(DetailPane::Loaded(sample_action_call_schema()));

        let rendered = render_view(&app, 140, 42);
        assert!(rendered.contains("Action Call Summary (read-only)"));
        assert!(rendered.contains("Goal Hints (selected field detail, read-only)"));
        assert!(rendered.contains("Field List (`click`/wheel select, `a` apply)"));
        assert!(rendered.contains("Send/Cancel Result (read-only, wheel scroll)"));
    }

    #[test]
    fn action_call_response_pane_surfaces_goal_id_and_secret() {
        let mut app = App::default();
        app.replace_actions(vec![sample_action_row()]);
        app.open_screen(Screen::Actions).expect("open actions");
        assert!(app.open_action_call_from_selected_action());
        app.set_action_call_schema(DetailPane::Loaded(sample_action_call_schema()));
        app.set_action_call_response(DetailPane::Loaded(ActionCallResponseData {
            accepted: true,
            goal_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            goal_secret: "secret-token".to_string(),
            status: "Accepted".to_string(),
            code: "Ok".to_string(),
            message: String::new(),
            server_epoch: "epoch-1".to_string(),
        }));

        let rendered = render_view(&app, 140, 42);
        // The user needs both the goal_id and goal_secret to cancel; both must
        // be visible in the result pane.
        assert!(rendered.contains("Goal ID: 550e8400-e29b-41d4-a716-446655440000"));
        assert!(rendered.contains("Goal Secret: secret-token"));
    }

    #[test]
    fn zenoh_detail_lines_explain_metrics_unavailable() {
        let router = ZenohRouterRow {
            zid: "router-beta".to_string(),
            whatami: "router".to_string(),
            version: Some("1.7.2".to_string()),
            session_count: None,
            locator_count: 1,
            plugin_count: 0,
            metrics_available: false,
            metrics: None,
            locators: vec!["tcp/10.0.0.2:7447".to_string()],
            plugin_names: Vec::new(),
            raw_payload_utf8: "{\"version\":\"1.7.2\"}".to_string(),
        };
        let detail = crate::app::DetailPane::Loaded(ZenohInternalsData {
            router_count: 1,
            routers: vec![router.clone()],
        });
        let events = VecDeque::new();
        let lines = zenoh_detail_lines(&detail, &DetailPane::Empty, &events, Some(&router));
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();

        assert!(rendered.iter().any(|line| line.contains("Advanced screen")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("metrics are not visible")));
        assert!(rendered.iter().any(|line| line.contains("admin-space ACL")));
    }

    #[test]
    fn zenoh_detail_lines_include_session_connectivity_snapshot() {
        let router = ZenohRouterRow {
            zid: "router-alpha".to_string(),
            whatami: "router".to_string(),
            version: Some("1.8.0".to_string()),
            session_count: Some(1),
            locator_count: 1,
            plugin_count: 1,
            metrics_available: true,
            metrics: None,
            locators: vec!["tcp/10.0.0.2:7447".to_string()],
            plugin_names: vec!["rest".to_string()],
            raw_payload_utf8: "{\"version\":\"1.8.0\"}".to_string(),
        };
        let detail = DetailPane::Loaded(ZenohInternalsData {
            router_count: 1,
            routers: vec![router.clone()],
        });
        let connectivity = DetailPane::Loaded(ZenohConnectivityData {
            session_zid: "session-alpha".to_string(),
            local_locator_count: 1,
            transport_count: 1,
            link_count: 1,
            local_locators: vec!["tcp/192.168.1.10:7447".to_string()],
            transports: vec![ZenohTransportRow {
                remote_zid: "router-alpha".to_string(),
                remote_whatami: "router".to_string(),
                supports_qos: true,
                is_multicast: false,
                supports_shm: true,
            }],
            links: vec![ZenohLinkRow {
                transport_zid: "router-alpha".to_string(),
                src: "tcp/192.168.1.10:7447".to_string(),
                dst: "tcp/10.0.0.2:7447".to_string(),
                group: Some("default".to_string()),
                mtu: 65_535,
                is_streamed: true,
                interface_count: 1,
                interfaces: vec!["en0".to_string()],
                auth_identifier: None,
                priorities: Some((1, 7)),
                reliability: Some("Reliable".to_string()),
            }],
        });

        let events = VecDeque::new();
        let lines = zenoh_detail_lines(&detail, &connectivity, &events, Some(&router));
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();

        assert!(rendered
            .iter()
            .any(|line| line.contains("Session connectivity snapshot")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Session ZID: session-alpha")));
        assert!(rendered.iter().any(|line| line.contains("Transports: 1")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("router-alpha [router] qos=yes shm=yes multicast=no")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("tcp/192.168.1.10:7447 -> tcp/10.0.0.2:7447")));
    }

    #[test]
    fn zenoh_detail_lines_include_reconnect_diagnosis() {
        let router = ZenohRouterRow {
            zid: "router-alpha".to_string(),
            whatami: "router".to_string(),
            version: Some("1.8.0".to_string()),
            session_count: Some(1),
            locator_count: 1,
            plugin_count: 1,
            metrics_available: true,
            metrics: None,
            locators: vec!["tcp/10.0.0.2:7447".to_string()],
            plugin_names: vec!["rest".to_string()],
            raw_payload_utf8: "{\"version\":\"1.8.0\"}".to_string(),
        };
        let detail = DetailPane::Loaded(ZenohInternalsData {
            router_count: 1,
            routers: vec![router.clone()],
        });
        let connectivity = DetailPane::Loaded(ZenohConnectivityData {
            session_zid: "session-alpha".to_string(),
            local_locator_count: 1,
            transport_count: 1,
            link_count: 1,
            local_locators: vec!["tcp/192.168.1.10:7447".to_string()],
            transports: vec![ZenohTransportRow {
                remote_zid: "router-alpha".to_string(),
                remote_whatami: "router".to_string(),
                supports_qos: true,
                is_multicast: false,
                supports_shm: true,
            }],
            links: vec![ZenohLinkRow {
                transport_zid: "router-alpha".to_string(),
                src: "tcp/192.168.1.10:7447".to_string(),
                dst: "tcp/10.0.0.2:7447".to_string(),
                group: Some("default".to_string()),
                mtu: 65_535,
                is_streamed: true,
                interface_count: 1,
                interfaces: vec!["en0".to_string()],
                auth_identifier: None,
                priorities: Some((1, 7)),
                reliability: Some("Reliable".to_string()),
            }],
        });
        let events = VecDeque::from([
            ZenohConnectivityEventData {
                sequence: 1,
                timestamp_ns: 100,
                kind: ZenohConnectivityEventKindData::TransportClosed,
                message: "transport closed: router-alpha [router] qos=yes shm=yes multicast=no"
                    .to_string(),
            },
            ZenohConnectivityEventData {
                sequence: 2,
                timestamp_ns: 200,
                kind: ZenohConnectivityEventKindData::TransportOpened,
                message: "transport opened: router-alpha [router] qos=yes shm=yes multicast=no"
                    .to_string(),
            },
        ]);

        let lines = zenoh_detail_lines(&detail, &connectivity, &events, Some(&router));
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();

        assert!(rendered
            .iter()
            .any(|line| line.contains("Recent connectivity diagnosis")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Disconnect signals: 1")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Recovery signals: 1")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Flap suspicion: yes")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Assessment: recovered after recent disconnect signal")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Last disconnect: transport closed")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Last recovery: transport opened")));
    }

    #[test]
    fn zenoh_detail_lines_flag_admin_visibility_mismatch_when_connectivity_is_live() {
        let detail = DetailPane::Error("admin root denied".to_string());
        let connectivity = DetailPane::Loaded(ZenohConnectivityData {
            session_zid: "session-alpha".to_string(),
            local_locator_count: 1,
            transport_count: 1,
            link_count: 1,
            local_locators: vec!["tcp/192.168.1.10:7447".to_string()],
            transports: vec![ZenohTransportRow {
                remote_zid: "router-alpha".to_string(),
                remote_whatami: "router".to_string(),
                supports_qos: true,
                is_multicast: false,
                supports_shm: true,
            }],
            links: vec![ZenohLinkRow {
                transport_zid: "router-alpha".to_string(),
                src: "tcp/192.168.1.10:7447".to_string(),
                dst: "tcp/10.0.0.2:7447".to_string(),
                group: Some("default".to_string()),
                mtu: 65_535,
                is_streamed: true,
                interface_count: 1,
                interfaces: vec!["en0".to_string()],
                auth_identifier: None,
                priorities: Some((1, 7)),
                reliability: Some("Reliable".to_string()),
            }],
        });

        let events = VecDeque::new();
        let lines = zenoh_detail_lines(&detail, &connectivity, &events, None);
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();

        assert!(rendered
            .iter()
            .any(|line| line.contains("Current admin/connectivity diagnosis")));
        assert!(rendered.iter().any(|line| {
            line.contains(
                "Assessment: live transports/links are present but router admin diagnostics failed",
            )
        }));
        assert!(rendered
            .iter()
            .any(|line| line.contains("admin-space ACL or visibility issue")));
    }

    #[test]
    fn zenoh_detail_lines_flag_missing_session_transport_when_admin_is_visible() {
        let router = ZenohRouterRow {
            zid: "router-alpha".to_string(),
            whatami: "router".to_string(),
            version: Some("1.8.0".to_string()),
            session_count: Some(1),
            locator_count: 1,
            plugin_count: 1,
            metrics_available: true,
            metrics: None,
            locators: vec!["tcp/10.0.0.2:7447".to_string()],
            plugin_names: vec!["rest".to_string()],
            raw_payload_utf8: "{\"version\":\"1.8.0\"}".to_string(),
        };
        let detail = DetailPane::Loaded(ZenohInternalsData {
            router_count: 1,
            routers: vec![router.clone()],
        });
        let connectivity = DetailPane::Loaded(ZenohConnectivityData {
            session_zid: "session-alpha".to_string(),
            local_locator_count: 1,
            transport_count: 0,
            link_count: 0,
            local_locators: vec!["tcp/192.168.1.10:7447".to_string()],
            transports: Vec::new(),
            links: Vec::new(),
        });

        let events = VecDeque::new();
        let lines = zenoh_detail_lines(&detail, &connectivity, &events, Some(&router));
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();

        assert!(rendered
            .iter()
            .any(|line| line.contains("Current admin/connectivity diagnosis")));
        assert!(rendered.iter().any(|line| {
            line.contains(
                "Assessment: router admin space is visible but the current session has no active transports or links"
            )
        }));
    }

    #[test]
    fn zenoh_detail_lines_flag_admin_inventory_gap_when_transports_are_live() {
        let detail = DetailPane::Loaded(ZenohInternalsData {
            router_count: 0,
            routers: Vec::new(),
        });
        let connectivity = DetailPane::Loaded(ZenohConnectivityData {
            session_zid: "session-alpha".to_string(),
            local_locator_count: 1,
            transport_count: 1,
            link_count: 1,
            local_locators: vec!["tcp/192.168.1.10:7447".to_string()],
            transports: vec![ZenohTransportRow {
                remote_zid: "router-alpha".to_string(),
                remote_whatami: "router".to_string(),
                supports_qos: true,
                is_multicast: false,
                supports_shm: true,
            }],
            links: vec![ZenohLinkRow {
                transport_zid: "router-alpha".to_string(),
                src: "tcp/192.168.1.10:7447".to_string(),
                dst: "tcp/10.0.0.2:7447".to_string(),
                group: Some("default".to_string()),
                mtu: 65_535,
                is_streamed: true,
                interface_count: 1,
                interfaces: vec!["en0".to_string()],
                auth_identifier: None,
                priorities: Some((1, 7)),
                reliability: Some("Reliable".to_string()),
            }],
        });

        let events = VecDeque::new();
        let lines = zenoh_detail_lines(&detail, &connectivity, &events, None);
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();

        assert!(rendered
            .iter()
            .any(|line| line.contains("Current admin/connectivity diagnosis")));
        assert!(rendered.iter().any(|line| line.contains(
            "Assessment: live transports/links are present but router admin inventory is empty"
        )));
    }

    #[test]
    fn zenoh_router_primary_label_includes_role_and_locator() {
        let router = ZenohRouterRow {
            zid: "router-alpha".to_string(),
            whatami: "router".to_string(),
            version: None,
            session_count: Some(3),
            locator_count: 1,
            plugin_count: 1,
            metrics_available: true,
            metrics: None,
            locators: vec!["tcp/127.0.0.1:7447".to_string()],
            plugin_names: vec!["rest".to_string()],
            raw_payload_utf8: String::new(),
        };

        assert_eq!(
            zenoh_router_primary_label(&router),
            "router-alpha [router] @ tcp/127.0.0.1:7447"
        );
        assert!(zenoh_metrics_capability_note(&router).contains("metrics are visible"));
    }

    #[test]
    fn zenoh_router_detail_lines_render_traffic_counters() {
        let router = ZenohRouterRow {
            zid: "router-alpha".to_string(),
            whatami: "router".to_string(),
            version: Some("1.7.2".to_string()),
            session_count: Some(3),
            locator_count: 1,
            plugin_count: 0,
            metrics_available: true,
            metrics: Some(ZenohRouterMetrics {
                tx_bytes_total: 2048,
                rx_bytes_total: 512,
                tx_msgs_total: 9,
                rx_msgs_total: 4,
                transports_opened: Some(2),
                links_opened: None,
            }),
            locators: vec!["tcp/127.0.0.1:7447".to_string()],
            plugin_names: Vec::new(),
            raw_payload_utf8: String::new(),
        };
        let detail = crate::app::DetailPane::Loaded(ZenohInternalsData {
            router_count: 1,
            routers: vec![router.clone()],
        });
        let rendered: Vec<String> = zenoh_router_detail_lines(&detail, Some(&router))
            .into_iter()
            .map(|line| line.to_string())
            .collect();

        assert!(rendered
            .iter()
            .any(|line| line.contains("Traffic counters")));
        assert!(
            rendered
                .iter()
                .any(|line| line.contains("tx: 2.00 KiB (9 msgs)")),
            "tx counter line missing: {:?}",
            rendered
        );
        assert!(rendered
            .iter()
            .any(|line| line.contains("rx: 512 B (4 msgs)")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("transports opened: 2")));
    }

    #[test]
    fn response_lines_describe_fallback_mode_explicitly() {
        let response = ServiceCallResponseData {
            response_json: None,
            response_utf8: Some("planner timeout".to_string()),
            response_hex: "706c616e6e6572".to_string(),
            decoded_with_schema: false,
            schema_hash: "01234567".to_string(),
            response_root_type: Some("demo.Response".to_string()),
        };
        let lines = response_lines(&response);
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();

        assert!(rendered
            .iter()
            .any(|line| line.contains("Reply mode: utf-8 fallback")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("schema decode unavailable")));
    }

    #[test]
    fn schema_hint_lines_explain_template_driven_flow() {
        let schema = ServiceCallSchemaData {
            service_name: "nav/plan".to_string(),
            key_expr: "aorta/default/srv/nav/plan".to_string(),
            provider_endpoint_count: 1,
            candidate_node_count: 1,
            candidate_nodes_note: Some("provider node inferred from telemetry".to_string()),
            schema_hash: "01234567".to_string(),
            request_root_type: Some("demo.Request".to_string()),
            response_root_type: Some("demo.Response".to_string()),
            request_model: SchemaModel {
                root_type: "demo.Request".to_string(),
                fields: vec![SchemaField {
                    path: "message".to_string(),
                    name: "message".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "string".to_string(),
                    type_kind: SchemaTypeKind::String,
                    repeated: false,
                    optional: false,
                    default_json: None,
                    enum_values: Vec::new(),
                    doc: Some("Planner request text.".to_string()),
                }],
            },
            starter_json: "{\n  \"message\": \"\"\n}".to_string(),
        };

        let lines = schema_hint_lines(&schema, Some(0));
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();

        assert!(rendered
            .iter()
            .any(|line| line.contains("merge patches instead of hand-authoring")));
        assert!(rendered.iter().any(|line| line.contains("click` inspect")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Selected field: message (1/1)")));
    }

    #[test]
    fn schema_describe_lines_prompt_to_open_when_empty() {
        let mut lines: Vec<Line<'_>> = Vec::new();
        let pane: DetailPane<SchemaDescribeData> = DetailPane::Empty;
        push_schema_describe_lines(&mut lines, &pane);
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();
        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].contains("Press `d` to load the full schema definition."));
    }

    #[test]
    fn schema_describe_lines_render_loaded_topic_with_hash_and_fields() {
        let pane = DetailPane::Loaded(SchemaDescribeData {
            key_suffix: "pub/demo/chatter".to_string(),
            kind: SchemaDescribeKind::Topic,
            schema_hash: "abcdef1234567890".to_string(),
            bfbs_bytes: 1234,
            sections: vec![SchemaDescribeSection {
                label: "Schema".to_string(),
                root_type: Some("demo.Chatter".to_string()),
                text: "table demo.Chatter {\n  text:string;\n}".to_string(),
            }],
        });
        let mut lines: Vec<Line<'_>> = Vec::new();
        push_schema_describe_lines(&mut lines, &pane);
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();
        assert!(rendered.iter().any(|line| line.contains("Schema Detail")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Schema kind: topic")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Schema key: pub/demo/chatter")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Schema hash: abcdef1234567890 (1234 bytes)")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Schema (root: demo.Chatter):")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("table demo.Chatter {")));
        assert!(rendered.iter().any(|line| line.contains("text:string;")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Press `d` again to close")));
    }

    #[test]
    fn schema_describe_lines_render_loaded_service_with_request_and_response() {
        let pane = DetailPane::Loaded(SchemaDescribeData {
            key_suffix: "srv/echo".to_string(),
            kind: SchemaDescribeKind::Service,
            schema_hash: "0123456789abcdef".to_string(),
            bfbs_bytes: 4096,
            sections: vec![
                SchemaDescribeSection {
                    label: "Request".to_string(),
                    root_type: Some("echo.Request".to_string()),
                    text: "table echo.Request { msg:string; }".to_string(),
                },
                SchemaDescribeSection {
                    label: "Response".to_string(),
                    root_type: Some("echo.Response".to_string()),
                    text: "table echo.Response { reply:string; }".to_string(),
                },
            ],
        });
        let mut lines: Vec<Line<'_>> = Vec::new();
        push_schema_describe_lines(&mut lines, &pane);
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();
        assert!(rendered
            .iter()
            .any(|line| line.contains("Schema kind: service")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Request (root: echo.Request):")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Response (root: echo.Response):")));
        assert!(rendered.iter().any(|line| line.contains("echo.Request")));
        assert!(rendered.iter().any(|line| line.contains("echo.Response")));
    }

    #[test]
    fn schema_describe_lines_render_error_with_message() {
        let pane: DetailPane<SchemaDescribeData> =
            DetailPane::Error("schema_by_key returned no match".to_string());
        let mut lines: Vec<Line<'_>> = Vec::new();
        push_schema_describe_lines(&mut lines, &pane);
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();
        assert!(rendered
            .iter()
            .any(|line| line.contains("Failed to fetch schema definition")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Error: schema_by_key returned no match")));
    }

    #[test]
    fn help_topics_screen_advertises_schema_describe_shortcut() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");

        let lines = help_lines(&app, LayoutMode::Wide);
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();
        assert!(rendered
            .iter()
            .any(|line| line.contains("d show full schema definition for the selected topic")));
    }

    #[test]
    fn help_services_screen_advertises_schema_describe_shortcut() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Services).expect("open services");

        let lines = help_lines(&app, LayoutMode::Wide);
        let rendered: Vec<String> = lines.into_iter().map(|line| line.to_string()).collect();
        assert!(rendered.iter().any(|line| {
            line.contains("d show full request/response schema for the selected service")
        }));
    }

    #[test]
    fn node_detail_lines_make_unavailable_runtime_explicit() {
        let selected = NodeRow {
            name: "planner".to_string(),
            node_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            group: "default".to_string(),
            endpoint_count: Some(1),
            topic_count: Some(2),
            service_count: Some(1),
            discovery_source: NodeDiscoverySource::AdminSnapshot,
            health: HealthLevel::Warn,
            capability: CapabilityState::Partial,
            freshness: FreshnessState::Unavailable,
            runtime_collection_period_ms: None,
            status_summary: "runtime unavailable — admin snapshot only".to_string(),
        };
        let detail = NodeDetailData {
            name: "planner".to_string(),
            node_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            group: "default".to_string(),
            endpoint_count: Some(1),
            topic_count: Some(2),
            service_count: Some(1),
            discovery_source: NodeDiscoverySource::AdminSnapshot,
            endpoint_ids: vec!["ep1".to_string()],
            topic_names: vec!["camera/front".to_string()],
            service_names: vec!["nav/plan".to_string()],
            published_topic_count: Some(1),
            subscribed_topic_count: None,
            provided_service_count: Some(1),
            client_service_count: None,
            published_topic_names: vec!["camera/front".to_string()],
            subscribed_topic_names: Vec::new(),
            provided_service_names: vec!["nav/plan".to_string()],
            client_service_names: Vec::new(),
            endpoint_provenance: "declared",
            topic_provenance: "inferred",
            service_provenance: "inferred",
            published_topic_provenance: "inferred",
            subscribed_topic_provenance: "unavailable",
            provided_service_provenance: "inferred",
            client_service_provenance: "unavailable",
            endpoint_note: None,
            topic_note: None,
            service_note: None,
            published_topic_note: None,
            subscribed_topic_note: Some(
                "subscriber ownership is unavailable in admin snapshot mode".to_string(),
            ),
            provided_service_note: None,
            client_service_note: Some(
                "client ownership is unavailable in admin snapshot mode".to_string(),
            ),
            runtime_summary: None,
            runtime_provenance: "unavailable",
            runtime_note: Some("runtime metrics unavailable in admin snapshot mode".to_string()),
            runtime_breakdown: None,
        };

        let rendered: Vec<String> = node_detail_lines(
            &selected,
            &crate::app::DetailPane::Loaded(detail),
            true,
            false,
        )
        .into_iter()
        .map(|line| line.to_string())
        .collect();

        assert!(rendered
            .iter()
            .any(|line| line.contains("State: warn | capability=partial")));
        assert!(!rendered
            .iter()
            .any(|line| line.contains("Visibility diagnosis")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("Runtime telemetry is unavailable for this node")));
    }

    #[test]
    fn overview_render_omits_visibility_diagnosis() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        let rendered = render_view(&app, 140, 42);

        assert!(!rendered.contains("Visibility diagnosis"));
        assert!(rendered.contains("Overall health:"));
    }

    fn sample_runtime_node_detail(selected: &NodeRow) -> NodeDetailData {
        NodeDetailData {
            name: selected.name.clone(),
            node_id: selected.node_id.clone(),
            group: selected.group.clone(),
            endpoint_count: selected.endpoint_count,
            topic_count: selected.topic_count,
            service_count: selected.service_count,
            discovery_source: selected.discovery_source,
            endpoint_ids: vec!["ep1".to_string()],
            topic_names: vec!["camera/front".to_string(), "imu/data".to_string()],
            service_names: vec!["nav/plan".to_string()],
            published_topic_count: Some(1),
            subscribed_topic_count: Some(1),
            provided_service_count: Some(1),
            client_service_count: Some(1),
            published_topic_names: vec!["camera/front".to_string()],
            subscribed_topic_names: vec!["imu/data".to_string()],
            provided_service_names: vec!["nav/plan".to_string()],
            client_service_names: vec!["planner/status".to_string()],
            endpoint_provenance: "exact",
            topic_provenance: "exact",
            service_provenance: "exact",
            published_topic_provenance: "exact",
            subscribed_topic_provenance: "exact",
            provided_service_provenance: "exact",
            client_service_provenance: "exact",
            endpoint_note: None,
            topic_note: None,
            service_note: None,
            published_topic_note: None,
            subscribed_topic_note: None,
            provided_service_note: None,
            client_service_note: None,
            runtime_summary: Some(NodeRuntimeSummaryData {
                timestamp_ns: 123456789,
                collection_period_ms: 1000,
                publisher_metric_count: 1,
                subscriber_metric_count: 1,
                service_metric_count: 1,
                client_metric_count: 1,
                publish_shm_hit_total: 7,
                publish_shm_fallback_total: 1,
                publish_shm_required_fail_total: 0,
                loan_inflight_current: 0,
                loan_quota_exceeded_total: 0,
                subscriber_duplicate_drop_count: 0,
                subscriber_out_of_order_count: 2,
                subscriber_out_of_order_drop_count: 1,
                subscriber_executor_drop_count: 0,
                subscriber_local_drop_count: 0,
                subscriber_overflow_relay_enqueue_count: 0,
                subscriber_overflow_relay_drop_count: 0,
                service_request_received_count: 10,
                service_reply_ok_count: 9,
                service_reply_error_count: 1,
                service_reply_send_failure_count: 0,
                service_request_dropped_count: 0,
                service_in_flight_requests: 0,
                client_call_count: 4,
                client_success_count: 4,
                client_timeout_count: 0,
                client_concurrent_conflict_count: 0,
                client_error_count: 0,
                client_retry_count: 0,
                executor_queued_events: Some(2),
                executor_queued_bytes: Some(128),
                executor_peak_queued_events: Some(3),
                executor_peak_queued_bytes: Some(256),
                executor_dropped_queue_full: Some(0),
                executor_dropped_queue_closed: Some(0),
                executor_dropped_budget_events: Some(0),
                executor_dropped_budget_bytes: Some(0),
                executor_spin_count: Some(0),
            }),
            runtime_provenance: "exact",
            runtime_note: None,
            runtime_breakdown: Some(NodeRuntimeBreakdownData {
                publishers: vec![NodePublisherRuntimeEntryData {
                    resource_name: "camera/front".to_string(),
                    resource_instance_id: "pub-1".to_string(),
                    schema_hash: Some("deadbeef".to_string()),
                    publish_shm_hit_total: 7,
                    publish_shm_fallback_total: 1,
                    publish_shm_required_fail_total: 0,
                    loan_inflight_current: 0,
                    loan_quota_exceeded_total: 0,
                }],
                subscribers: Vec::new(),
                services: Vec::new(),
                clients: Vec::new(),
            }),
        }
    }

    fn render_view(app: &App, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| render(frame, app))
            .expect("render app");
        buffer_view(terminal.backend().buffer())
    }

    fn render_detail_view(title: &str, lines: Vec<Line<'_>>, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| render_detail_panel(frame, frame.area(), title, lines))
            .expect("render detail panel");
        buffer_view(terminal.backend().buffer())
    }

    fn render_status_view(app: &App, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| render_status(frame, frame.area(), app))
            .expect("render status panel");
        buffer_view(terminal.backend().buffer())
    }

    fn buffer_view(buffer: &Buffer) -> String {
        let mut view = String::new();
        for row in buffer.content.chunks(buffer.area.width as usize) {
            for cell in row {
                view.push_str(cell.symbol());
            }
            view.push('\n');
        }
        view
    }
}
