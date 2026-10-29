pub mod app;
pub mod data;
pub mod editor;
pub mod mouse;
pub mod search;
pub mod state;
pub mod ui;

use std::io::{self, Stdout};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use aorta_introspect::client::{
    ActionFeedbackEvent, IntrospectClient, IntrospectOptions, LogEvent, TopicPreviewEvent,
};
use aorta_introspect::ids::{GroupName, TopicSuffix};
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
    MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tokio::sync::{mpsc, oneshot, watch};

use crate::app::{App, CommandAction, DetailPane, EventSource, Screen};
use crate::data::{
    fetch_action_call_schema_data, fetch_action_detail_data, fetch_actions_data,
    fetch_context_data, fetch_dashboard_data, fetch_node_detail_data,
    fetch_service_call_schema_data, fetch_service_detail_data, fetch_service_schema_describe,
    fetch_topic_detail_data, fetch_topic_schema_describe, fetch_zenoh_connectivity_data,
    fetch_zenoh_connectivity_event_data, fetch_zenoh_internals_data, invoke_action_cancel,
    invoke_action_send_goal, invoke_service_call, ActionCallResponseData,
};
use crate::mouse::{MouseTarget, MouseUiState, ScrollTarget};
use crate::state::{
    build_service_response_export, build_topic_preview_export, load_local_state, save_local_state,
    write_export_payload, write_text_export, LocalPaths, LocalState,
};

#[derive(Default)]
struct TopicPreviewController {
    topic_name: Option<String>,
    receiver: Option<watch::Receiver<Option<TopicPreviewEvent>>>,
    stop_tx: Option<oneshot::Sender<()>>,
}

/// Subscription controller for the live `/log` stream (Feature C4). Same shape
/// as [`TopicPreviewController`], but group-scoped: started once at startup and
/// never re-synced on selection.
#[derive(Default)]
struct LogStreamController {
    receiver: Option<watch::Receiver<Option<LogEvent>>>,
    stop_tx: Option<oneshot::Sender<()>>,
}

/// Subscription controller for the live action-feedback panel (Feature C2).
/// Keyed by `(action, goal_id)`: started when a goal is accepted on the Action
/// Call screen, cancelled and restarted when a new goal is sent.
#[derive(Default)]
struct ActionFeedbackController {
    action: Option<String>,
    goal_id: Option<String>,
    // Queued (not `watch`) so bursty feedback between UI ticks is never coalesced
    // — every sample must reach the 256-row per-goal history panel.
    receiver: Option<mpsc::UnboundedReceiver<ActionFeedbackEvent>>,
    stop_tx: Option<oneshot::Sender<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandRefresh {
    None,
    Reconcile,
    ActiveView,
    DashboardAndActiveView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BookmarkOverlayPostAction {
    None,
    PersistLocalState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KeyboardMacroBinding {
    key: char,
    action: CommandAction,
}

const KEYBOARD_MACRO_BINDINGS: [KeyboardMacroBinding; 5] = [
    KeyboardMacroBinding {
        key: 'T',
        action: CommandAction::MacroOpenTopicsSearch,
    },
    KeyboardMacroBinding {
        key: 'S',
        action: CommandAction::MacroOpenServicesSearch,
    },
    KeyboardMacroBinding {
        key: 'N',
        action: CommandAction::MacroOpenNodesSearch,
    },
    KeyboardMacroBinding {
        key: 'C',
        action: CommandAction::MacroOpenServiceCallEditor,
    },
    KeyboardMacroBinding {
        key: 'A',
        action: CommandAction::MacroOpenActionCallEditor,
    },
];

#[cfg(test)]
const RESERVED_NORMAL_MODE_SHORTCUTS: [char; 25] = [
    'q', '?', 'h', 'z', 'e', 'g', 'o', 'b', 'j', 'k', 'v', 'm', 'x', 'c', '[', ']', '/', 'r', 'p',
    'i', 'y', 'd', 'f', 'n', // `L` (uppercase) opens the live `/log` stream viewer overlay.
    'L',
];

/// Coarse default cadence for opt-in periodic auto-refresh. Kept >= ~1s because
/// every fetch is a blocking `block_on` on the single UI thread, so a tighter
/// interval would stall input/redraw.
const AUTO_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

/// Pure decision for whether a periodic auto-refresh is due. Factored out of the
/// event loop so it can be unit-tested without a terminal.
fn should_auto_refresh(enabled: bool, last: Instant, interval: Duration, now: Instant) -> bool {
    enabled && now.saturating_duration_since(last) >= interval
}

/// Resolve the effective group: explicit CLI value > `AORTA_GROUP` env >
/// `default`. Pure so it can be unit-tested without touching
/// the process environment.
fn resolve_group(cli_group: Option<&str>, env_group: Option<&str>) -> String {
    cli_group
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| env_group.map(str::trim).filter(|value| !value.is_empty()))
        .unwrap_or("default")
        .to_string()
}

pub fn run(group: Option<&str>, client_mode_override: bool) -> Result<()> {
    // AORTA_GROUP is authoritative for the group, fallback `default`. Every
    // sibling tool honors it; the TUI must not silently connect to `default`
    // on a grouped robot.
    let group = resolve_group(group, std::env::var("AORTA_GROUP").ok().as_deref());
    let group = group.as_str();
    let runtime = tokio::runtime::Runtime::new()?;
    let client = Arc::new(build_introspect_client(
        &runtime,
        group,
        client_mode_override,
    )?);
    let local_paths = LocalPaths::from_env();
    let mut app = App::default();
    app.set_group(group.to_string());
    match load_local_state(&local_paths) {
        Ok(state) => {
            app.apply_runtime_section_preferences(state.runtime_sections);
            app.replace_bookmarks(state.bookmarks);
        }
        Err(err) => app.report_warn_from(
            EventSource::System,
            format!("Failed to load local TUI state (using defaults): {}", err),
        ),
    }
    let mut topic_preview = TopicPreviewController::default();
    let mut log_stream = LogStreamController::default();
    let mut action_feedback = ActionFeedbackController::default();
    refresh_dashboard(&runtime, client.as_ref(), &mut app);
    refresh_active_view(&runtime, client.as_ref(), &mut app);
    sync_topic_preview_subscription(&runtime, Arc::clone(&client), &mut app, &mut topic_preview);
    // The `/log` stream is group-scoped, not selection-scoped: start it once at
    // startup and leave it running until exit (unlike the topic preview, which
    // re-syncs as the selected topic changes).
    start_log_stream_subscription(&runtime, Arc::clone(&client), &mut app, &mut log_stream);

    let mut terminal = init_terminal()?;
    let terminal_guard = TerminalGuard;
    let mut mouse_ui = MouseUiState::default();
    let mut last_auto_refresh = Instant::now();

    loop {
        drain_topic_preview_events(&mut app, &mut topic_preview);
        drain_log_events(&mut app, &mut log_stream);
        drain_action_feedback_events(&mut app, &mut action_feedback);
        // Feedback is tied to the Action Call screen: tear the subscription down
        // when the user navigates away so it never streams into a hidden panel.
        sync_action_feedback_lifecycle(&mut app, &mut action_feedback);
        terminal.draw(|frame| ui::render_with_mouse(frame, &app, &mut mouse_ui))?;

        if app.should_quit() {
            break;
        }

        if event::poll(Duration::from_millis(100))? {
            match event::read()? {
                Event::Key(key) => {
                    if key.kind != KeyEventKind::Press {
                        continue;
                    }

                    if app.is_help_visible() {
                        match key.code {
                            KeyCode::Char('q') => app.quit(),
                            KeyCode::Esc | KeyCode::Char('?') | KeyCode::Char('h') => {
                                app.hide_help();
                            }
                            _ => {}
                        }
                        continue;
                    }

                    let previous_screen = app.active_screen();
                    let previous_selection = app.active_selection_key();
                    let mut should_refresh_active_view = false;

                    if let Some(result) =
                        handle_event_timeline_key(&mut app, key.code, key.modifiers)
                    {
                        match result {
                            Ok(refresh) => {
                                should_refresh_active_view |= apply_command_refresh(
                                    &runtime,
                                    Arc::clone(&client),
                                    &mut app,
                                    &mut topic_preview,
                                    refresh,
                                );
                            }
                            Err(err) => app.report_warn_from(EventSource::Data, err),
                        }
                    } else if handle_log_stream_key(&mut app, key.code, key.modifiers).is_some() {
                        // The `/log` overlay consumed the key (select/close); no
                        // refresh is needed since the stream is push-driven.
                    } else if let Some(result) =
                        handle_topology_key(&mut app, key.code, key.modifiers)
                    {
                        match result {
                            Ok(refresh) => {
                                should_refresh_active_view |= apply_command_refresh(
                                    &runtime,
                                    Arc::clone(&client),
                                    &mut app,
                                    &mut topic_preview,
                                    refresh,
                                );
                            }
                            Err(err) => app.report_warn_from(EventSource::Navigation, err),
                        }
                    } else if let Some(result) =
                        handle_recorder_key(&mut app, &local_paths, key.code, key.modifiers)
                    {
                        match result {
                            Ok(refresh) => {
                                should_refresh_active_view |= apply_command_refresh(
                                    &runtime,
                                    Arc::clone(&client),
                                    &mut app,
                                    &mut topic_preview,
                                    refresh,
                                );
                            }
                            Err(err) => app.report_warn_from(EventSource::Navigation, err),
                        }
                    } else if let Some(result) =
                        handle_node_comparison_key(&mut app, key.code, key.modifiers)
                    {
                        match result {
                            Ok(refresh) => {
                                should_refresh_active_view |= apply_command_refresh(
                                    &runtime,
                                    Arc::clone(&client),
                                    &mut app,
                                    &mut topic_preview,
                                    refresh,
                                );
                            }
                            Err(err) => app.report_warn_from(EventSource::Navigation, err),
                        }
                    } else if let Some(result) =
                        handle_bookmark_overlay_key(&mut app, key.code, key.modifiers)
                    {
                        match result {
                            Ok((refresh, post_action)) => {
                                should_refresh_active_view |= apply_command_refresh(
                                    &runtime,
                                    Arc::clone(&client),
                                    &mut app,
                                    &mut topic_preview,
                                    refresh,
                                );
                                if matches!(
                                    post_action,
                                    BookmarkOverlayPostAction::PersistLocalState
                                ) {
                                    if let Err(err) = persist_local_state(&local_paths, &app) {
                                        app.report_error(err);
                                    }
                                }
                            }
                            Err(err) => app.report_warn_from(EventSource::Bookmarks, err),
                        }
                    } else if let Some(result) =
                        handle_command_palette_key(&mut app, key.code, key.modifiers)
                    {
                        match result {
                            Ok((refresh, action)) => {
                                should_refresh_active_view |= apply_command_refresh(
                                    &runtime,
                                    Arc::clone(&client),
                                    &mut app,
                                    &mut topic_preview,
                                    refresh,
                                );
                                if let Some(action) = action {
                                    apply_command_post_action(&local_paths, &mut app, action);
                                }
                            }
                            Err(err) => app.report_warn_from(EventSource::Navigation, err),
                        }
                    } else if handle_overlay_open_key(&mut app, key.code) {
                        // One of the overlay open-shortcuts (timeline `e`, /log
                        // `L`, bookmarks, palette, recorder `o`) handled the key.
                    } else {
                        if handle_service_call_edit_key(
                            &runtime,
                            client.as_ref(),
                            &mut app,
                            key.code,
                            key.modifiers,
                        ) {
                            continue;
                        }

                        if handle_action_call_edit_key(
                            &runtime,
                            Arc::clone(&client),
                            &mut app,
                            &mut action_feedback,
                            key.code,
                            key.modifiers,
                        ) {
                            continue;
                        }

                        match key.code {
                            KeyCode::Char('q') => app.quit(),
                            KeyCode::Char(ch @ '1'..='5')
                                if app.active_screen() == Screen::Overview
                                    && !app.is_searching() =>
                            {
                                let index = ch as usize - '1' as usize;
                                match app.activate_overview_highlighted_topic(index) {
                                    Ok(_) => {
                                        app.report_info_from(
                                            EventSource::Navigation,
                                            format!("Opened hot topic shortcut {}.", index + 1),
                                        );
                                        should_refresh_active_view = true;
                                    }
                                    Err(err) => app.report_warn_from(
                                        EventSource::Navigation,
                                        format!("Overview hot topic shortcut failed: {}", err),
                                    ),
                                }
                            }
                            KeyCode::F(number)
                                if app.active_screen() == Screen::Overview
                                    && !app.is_searching()
                                    && (1..=5).contains(&number) =>
                            {
                                let index = usize::from(number.saturating_sub(1));
                                match app.activate_overview_highlighted_service(index) {
                                    Ok(_) => {
                                        app.report_info_from(
                                            EventSource::Navigation,
                                            format!("Opened hot service shortcut F{}.", number),
                                        );
                                        should_refresh_active_view = true;
                                    }
                                    Err(err) => app.report_warn_from(
                                        EventSource::Navigation,
                                        format!("Overview hot service shortcut failed: {}", err),
                                    ),
                                }
                            }
                            KeyCode::Tab => {
                                app.next_screen();
                                should_refresh_active_view = true;
                            }
                            KeyCode::BackTab => {
                                app.previous_screen();
                                should_refresh_active_view = true;
                            }
                            KeyCode::Char('?') if !app.is_searching() => {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::OpenHelp,
                                );
                            }
                            KeyCode::Char('h')
                                if !app.is_searching()
                                    && app.active_screen() != Screen::ServiceCall
                                    && app.active_screen() != Screen::ActionCall =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::OpenHelp,
                                );
                            }
                            KeyCode::Char('z') if !app.is_searching() => {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::ToggleAdvancedMode,
                                );
                            }
                            KeyCode::Char('e') if !app.is_searching() => {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::OpenDiagnosticsTimeline,
                                );
                            }
                            KeyCode::Char('g') if !app.is_searching() => {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::OpenTopologyExplorer,
                                );
                            }
                            KeyCode::Char('o') if !app.is_searching() => {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::OpenRecorderLaunchpad,
                                );
                            }
                            KeyCode::Char(ch)
                                if !app.is_searching()
                                    && keyboard_macro_action(KeyCode::Char(ch)).is_some() =>
                            {
                                let action = keyboard_macro_action(KeyCode::Char(ch))
                                    .expect("macro action already checked");
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    action,
                                );
                            }
                            KeyCode::Char('b') if !app.is_searching() => {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::OpenBookmarks,
                                );
                            }
                            KeyCode::Down | KeyCode::Char('j') => {
                                app.select_next();
                                should_refresh_active_view = true;
                            }
                            KeyCode::Up | KeyCode::Char('k') => {
                                app.select_previous();
                                should_refresh_active_view = true;
                            }
                            KeyCode::PageDown if app.active_screen() == Screen::Topics => {
                                app.scroll_topic_preview_down(8);
                            }
                            KeyCode::PageDown
                                if app.active_screen() == Screen::ServiceCall
                                    && !app.is_service_call_editing() =>
                            {
                                app.scroll_service_call_response_down(8);
                            }
                            KeyCode::PageDown
                                if app.active_screen() == Screen::ActionCall
                                    && !app.is_action_call_editing() =>
                            {
                                app.scroll_action_call_response_down(8);
                            }
                            KeyCode::PageUp if app.active_screen() == Screen::Topics => {
                                app.scroll_topic_preview_up(8);
                            }
                            KeyCode::PageUp
                                if app.active_screen() == Screen::ServiceCall
                                    && !app.is_service_call_editing() =>
                            {
                                app.scroll_service_call_response_up(8);
                            }
                            KeyCode::PageUp
                                if app.active_screen() == Screen::ActionCall
                                    && !app.is_action_call_editing() =>
                            {
                                app.scroll_action_call_response_up(8);
                            }
                            KeyCode::Home if app.active_screen() == Screen::Topics => {
                                app.reset_topic_preview_scroll();
                            }
                            KeyCode::Home
                                if app.active_screen() == Screen::ServiceCall
                                    && !app.is_service_call_editing() =>
                            {
                                app.reset_service_call_response_scroll();
                            }
                            KeyCode::Home
                                if app.active_screen() == Screen::ActionCall
                                    && !app.is_action_call_editing() =>
                            {
                                app.reset_action_call_response_scroll();
                            }
                            KeyCode::Char('v')
                                if app.active_screen() == Screen::Topics && !app.is_searching() =>
                            {
                                app.toggle_topic_preview_view_mode();
                            }
                            KeyCode::Char('m')
                                if !app.is_searching() && app.active_screen() == Screen::Topics =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::ToggleTopicRuntimeSection,
                                );
                            }
                            KeyCode::Char('x')
                                if !app.is_searching() && app.active_screen() == Screen::Nodes =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::ToggleNodeCompareAnchor,
                                );
                            }
                            KeyCode::Char('c')
                                if !app.is_searching() && app.active_screen() == Screen::Nodes =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::OpenNodeComparison,
                                );
                            }
                            KeyCode::Char('[')
                                if app.active_screen() == Screen::Topics && !app.is_searching() =>
                            {
                                app.select_older_topic_preview_frame();
                            }
                            KeyCode::Char(']')
                                if app.active_screen() == Screen::Topics && !app.is_searching() =>
                            {
                                app.select_newer_topic_preview_frame();
                            }
                            KeyCode::Char('/') => {
                                if !matches!(
                                    app.active_screen(),
                                    Screen::Overview | Screen::ServiceCall | Screen::ActionCall
                                ) {
                                    app.enter_search();
                                }
                            }
                            KeyCode::Esc => {
                                app.exit_search();
                                app.exit_service_call_edit();
                                app.exit_action_call_edit();
                            }
                            KeyCode::Enter => {
                                if app.is_searching() {
                                    app.exit_search();
                                } else if app.active_screen() == Screen::Overview {
                                    match app.activate_overview_action() {
                                        Ok(_) => should_refresh_active_view = true,
                                        Err(err) => app.report_warn_from(
                                            EventSource::Navigation,
                                            format!("Overview action could not be opened: {}", err),
                                        ),
                                    }
                                }
                            }
                            KeyCode::Backspace => {
                                app.pop_query_char();
                                should_refresh_active_view = true;
                            }
                            KeyCode::Char('r') if !app.is_searching() => {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::RefreshSnapshot,
                                );
                            }
                            KeyCode::Char('f') if !app.is_searching() => {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::ToggleAutoRefresh,
                                );
                            }
                            KeyCode::Char('n')
                                if !app.is_searching() && app.active_screen() == Screen::Topics =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::JumpToTopicProviderNode,
                                );
                            }
                            KeyCode::Char('p')
                                if !app.is_searching() && !app.is_service_call_editing() =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::ToggleCurrentBookmark,
                                );
                            }
                            KeyCode::Char('c')
                                if !app.is_searching()
                                    && app.active_screen() == Screen::Services =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::OpenServiceCall,
                                );
                            }
                            KeyCode::Char('c')
                                if !app.is_searching()
                                    && app.active_screen() == Screen::Actions =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::OpenActionCall,
                                );
                            }
                            KeyCode::Char('m')
                                if !app.is_searching()
                                    && app.active_screen() == Screen::Services =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::ToggleServiceRuntimeSection,
                                );
                            }
                            KeyCode::Char('d')
                                if !app.is_searching()
                                    && matches!(
                                        app.active_screen(),
                                        Screen::Topics | Screen::Services
                                    ) =>
                            {
                                toggle_schema_describe(&runtime, client.as_ref(), &mut app);
                            }
                            KeyCode::Char('i') if app.active_screen() == Screen::ServiceCall => {
                                app.enter_service_call_edit();
                            }
                            KeyCode::Char('i') if app.active_screen() == Screen::ActionCall => {
                                app.enter_action_call_edit();
                            }
                            KeyCode::Char('y')
                                if !app.is_searching()
                                    && app.active_screen() == Screen::Topics
                                    && !app.is_service_call_editing() =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::ExportTopicPreviewPayload,
                                );
                            }
                            KeyCode::Char('Y')
                                if !app.is_searching()
                                    && app.active_screen() == Screen::Topics
                                    && !app.is_service_call_editing() =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::CopyTopicPreviewContent,
                                );
                            }
                            KeyCode::Char('a')
                                if !app.is_searching()
                                    && app.active_screen() == Screen::ServiceCall
                                    && !app.is_service_call_editing() =>
                            {
                                match app.service_call_apply_selected_hint_template() {
                                    Ok(path) => {
                                        app.report_info_from(
                                            EventSource::ServiceCall,
                                            format!("Applied schema template for '{}'.", path),
                                        );
                                    }
                                    Err(err) => {
                                        app.report_error_from(
                                            EventSource::ServiceCall,
                                            format!("Failed to apply schema template: {}", err),
                                        );
                                    }
                                }
                            }
                            KeyCode::Char('R')
                                if !app.is_searching()
                                    && app.active_screen() == Screen::ServiceCall
                                    && !app.is_service_call_editing() =>
                            {
                                match app.service_call_reset_request_to_starter() {
                                    Ok(()) => app.report_info_from(
                                        EventSource::ServiceCall,
                                        "Reset request body to starter JSON.".to_string(),
                                    ),
                                    Err(err) => {
                                        app.report_error_from(
                                            EventSource::ServiceCall,
                                            format!("Failed to reset request body: {}", err),
                                        );
                                    }
                                }
                            }
                            KeyCode::F(5) if app.active_screen() == Screen::ServiceCall => {
                                submit_service_call(&runtime, client.as_ref(), &mut app);
                            }
                            KeyCode::Char('a')
                                if !app.is_searching()
                                    && app.active_screen() == Screen::ActionCall
                                    && !app.is_action_call_editing() =>
                            {
                                match app.action_call_apply_selected_hint_template() {
                                    Ok(path) => {
                                        app.report_info_from(
                                            EventSource::ActionCall,
                                            format!("Applied schema template for '{}'.", path),
                                        );
                                    }
                                    Err(err) => {
                                        app.report_error_from(
                                            EventSource::ActionCall,
                                            format!("Failed to apply schema template: {}", err),
                                        );
                                    }
                                }
                            }
                            KeyCode::Char('R')
                                if !app.is_searching()
                                    && app.active_screen() == Screen::ActionCall
                                    && !app.is_action_call_editing() =>
                            {
                                match app.action_call_reset_request_to_starter() {
                                    Ok(()) => app.report_info_from(
                                        EventSource::ActionCall,
                                        "Reset goal body to starter JSON.".to_string(),
                                    ),
                                    Err(err) => {
                                        app.report_error_from(
                                            EventSource::ActionCall,
                                            format!("Failed to reset goal body: {}", err),
                                        );
                                    }
                                }
                            }
                            KeyCode::Char('x')
                                if !app.is_searching()
                                    && app.active_screen() == Screen::ActionCall
                                    && !app.is_action_call_editing() =>
                            {
                                cancel_last_action_goal(&runtime, client.as_ref(), &mut app);
                            }
                            KeyCode::F(5) if app.active_screen() == Screen::ActionCall => {
                                submit_action_goal(
                                    &runtime,
                                    Arc::clone(&client),
                                    &mut app,
                                    &mut action_feedback,
                                );
                            }
                            KeyCode::Char('y')
                                if !app.is_searching()
                                    && app.active_screen() == Screen::ServiceCall
                                    && !app.is_service_call_editing() =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::ExportServiceResponsePayload,
                                );
                            }
                            KeyCode::Char('m')
                                if !app.is_searching() && app.active_screen() == Screen::Nodes =>
                            {
                                should_refresh_active_view |= dispatch_command_action(
                                    &runtime,
                                    Arc::clone(&client),
                                    &local_paths,
                                    &mut app,
                                    &mut topic_preview,
                                    CommandAction::ToggleNodeRuntimeSection,
                                );
                            }
                            KeyCode::Char(ch) if app.is_searching() => {
                                app.push_query_char(ch);
                                should_refresh_active_view = true;
                            }
                            _ => {}
                        }
                    }

                    if should_refresh_active_view
                        && (app.active_screen() != previous_screen
                            || app.active_selection_key() != previous_selection)
                    {
                        refresh_active_view(&runtime, client.as_ref(), &mut app);
                        sync_topic_preview_subscription(
                            &runtime,
                            Arc::clone(&client),
                            &mut app,
                            &mut topic_preview,
                        );
                    }
                }
                Event::Mouse(mouse) => {
                    let previous_screen = app.active_screen();
                    let previous_selection = app.active_selection_key();
                    let should_refresh_active_view = handle_mouse_event(
                        &runtime,
                        Arc::clone(&client),
                        &local_paths,
                        &mut app,
                        &mut topic_preview,
                        &mouse_ui,
                        mouse,
                    );
                    if should_refresh_active_view
                        && (app.active_screen() != previous_screen
                            || app.active_selection_key() != previous_selection)
                    {
                        refresh_active_view(&runtime, client.as_ref(), &mut app);
                        sync_topic_preview_subscription(
                            &runtime,
                            Arc::clone(&client),
                            &mut app,
                            &mut topic_preview,
                        );
                    }
                }
                _ => {}
            }
        } else if should_auto_refresh(
            app.is_auto_refresh_enabled(),
            last_auto_refresh,
            AUTO_REFRESH_INTERVAL,
            Instant::now(),
        ) {
            refresh_dashboard(&runtime, client.as_ref(), &mut app);
            refresh_active_view(&runtime, client.as_ref(), &mut app);
            sync_topic_preview_subscription(
                &runtime,
                Arc::clone(&client),
                &mut app,
                &mut topic_preview,
            );
            last_auto_refresh = Instant::now();
        }
    }

    cancel_topic_preview_subscription(&mut topic_preview);
    cancel_log_stream_subscription(&mut log_stream);
    cancel_action_feedback_subscription(&mut action_feedback);
    drop(terminal);
    drop(terminal_guard);
    if let Err(err) = runtime.block_on(client.shutdown()) {
        eprintln!("WARN: failed to shutdown TUI cleanly: {}", err);
    }
    Ok(())
}

fn handle_command_palette_open_key(app: &mut App, code: KeyCode) -> bool {
    matches!(code, KeyCode::Char(':')) && app.open_command_palette()
}

fn handle_event_timeline_open_key(app: &mut App, code: KeyCode) -> bool {
    matches!(code, KeyCode::Char('e')) && app.open_event_timeline()
}

/// Try every overlay open-shortcut in priority order, returning whether one of
/// them opened an overlay. Folding the chain into a single short-circuiting
/// expression keeps the run-loop dispatch free of repeated empty `else if {}`
/// arms (which `clippy::if_same_then_else` flags).
fn handle_overlay_open_key(app: &mut App, code: KeyCode) -> bool {
    handle_event_timeline_open_key(app, code)
        || handle_log_stream_open_key(app, code)
        || handle_bookmark_overlay_open_key(app, code)
        || handle_command_palette_open_key(app, code)
        || handle_recorder_open_key(app, code)
}

/// `L` (uppercase) opens the live `/log` stream viewer overlay (Feature C4).
/// Uppercase to avoid colliding with the lowercase normal-mode shortcuts.
fn handle_log_stream_open_key(app: &mut App, code: KeyCode) -> bool {
    matches!(code, KeyCode::Char('L')) && app.open_log_stream()
}

/// Consume a key while the `/log` overlay is visible. Returns `Some(())` when
/// the overlay handled the key (so the normal-mode handler is skipped),
/// mirroring the event-timeline overlay's key handling.
fn handle_log_stream_key(app: &mut App, code: KeyCode, modifiers: KeyModifiers) -> Option<()> {
    if !app.is_log_stream_visible() {
        return None;
    }
    match code {
        KeyCode::Char('q') => app.quit(),
        KeyCode::Esc | KeyCode::Char('L') => app.close_log_stream(),
        KeyCode::Down | KeyCode::Char('j') if modifiers.is_empty() => {
            app.log_stream_select_next();
        }
        KeyCode::Up | KeyCode::Char('k') if modifiers.is_empty() => {
            app.log_stream_select_previous();
        }
        _ => {}
    }
    Some(())
}

fn handle_recorder_open_key(app: &mut App, code: KeyCode) -> bool {
    matches!(code, KeyCode::Char('o')) && app.open_recorder_overlay().is_ok()
}

fn keyboard_macro_action(code: KeyCode) -> Option<CommandAction> {
    let KeyCode::Char(ch) = code else {
        return None;
    };
    KEYBOARD_MACRO_BINDINGS
        .iter()
        .find(|binding| binding.key == ch)
        .map(|binding| binding.action)
}

#[cfg(test)]
fn handle_topology_open_key(app: &mut App, code: KeyCode) -> bool {
    matches!(code, KeyCode::Char('g')) && app.open_topology_overlay().is_ok()
}

#[cfg(test)]
fn handle_node_comparison_open_key(app: &mut App, code: KeyCode) -> bool {
    matches!(code, KeyCode::Char('c')) && app.open_node_comparison().is_ok()
}

fn handle_event_timeline_key(
    app: &mut App,
    code: KeyCode,
    modifiers: KeyModifiers,
) -> Option<Result<CommandRefresh, String>> {
    if !app.is_event_timeline_visible() {
        return None;
    }

    let refresh = match code {
        KeyCode::Char('q') => {
            app.quit();
            Ok(CommandRefresh::None)
        }
        KeyCode::Esc | KeyCode::Char('e') => {
            app.close_event_timeline();
            Ok(CommandRefresh::None)
        }
        KeyCode::Down | KeyCode::Char('j') if modifiers.is_empty() => {
            app.select_next_filtered_event();
            Ok(CommandRefresh::None)
        }
        KeyCode::Up | KeyCode::Char('k') if modifiers.is_empty() => {
            app.select_previous_filtered_event();
            Ok(CommandRefresh::None)
        }
        KeyCode::Char('s') if modifiers.is_empty() => {
            let filter = app.cycle_event_timeline_severity_filter();
            app.report_info_from(
                EventSource::Data,
                format!("Timeline severity filter set to '{}'.", filter.label()),
            );
            Ok(CommandRefresh::None)
        }
        KeyCode::Char('o') if modifiers.is_empty() => {
            let filter = app.cycle_event_timeline_source_filter();
            app.report_info_from(
                EventSource::Data,
                format!("Timeline source filter set to '{}'.", filter.label()),
            );
            Ok(CommandRefresh::None)
        }
        KeyCode::Char('c') if modifiers.is_empty() => {
            app.clear_event_timeline_filters();
            app.report_info_from(EventSource::Data, "Timeline filters cleared.".to_string());
            Ok(CommandRefresh::None)
        }
        _ => Ok(CommandRefresh::None),
    };

    Some(refresh)
}

fn handle_topology_key(
    app: &mut App,
    code: KeyCode,
    _modifiers: KeyModifiers,
) -> Option<Result<CommandRefresh, String>> {
    if !app.is_topology_overlay_visible() {
        return None;
    }

    let refresh = match code {
        KeyCode::Char('q') => {
            app.quit();
            Ok(CommandRefresh::None)
        }
        KeyCode::Esc | KeyCode::Char('g') => {
            app.close_topology_overlay();
            Ok(CommandRefresh::None)
        }
        _ => Ok(CommandRefresh::None),
    };

    Some(refresh)
}

fn handle_recorder_key(
    app: &mut App,
    local_paths: &LocalPaths,
    code: KeyCode,
    _modifiers: KeyModifiers,
) -> Option<Result<CommandRefresh, String>> {
    if !app.is_recorder_overlay_visible() {
        return None;
    }

    let refresh = match code {
        KeyCode::Char('q') => {
            app.quit();
            Ok(CommandRefresh::None)
        }
        KeyCode::Esc | KeyCode::Char('o') => {
            app.close_recorder_overlay();
            Ok(CommandRefresh::None)
        }
        KeyCode::Char('y') => export_recorder_launch_script(local_paths, app).map(|message| {
            app.report_info_from(EventSource::Navigation, message);
            CommandRefresh::None
        }),
        _ => Ok(CommandRefresh::None),
    };

    Some(refresh)
}

fn handle_node_comparison_key(
    app: &mut App,
    code: KeyCode,
    _modifiers: KeyModifiers,
) -> Option<Result<CommandRefresh, String>> {
    if !app.is_node_comparison_visible() {
        return None;
    }

    let refresh = match code {
        KeyCode::Char('q') => {
            app.quit();
            Ok(CommandRefresh::None)
        }
        KeyCode::Esc | KeyCode::Char('c') => {
            app.close_node_comparison();
            Ok(CommandRefresh::None)
        }
        _ => Ok(CommandRefresh::None),
    };

    Some(refresh)
}

fn handle_bookmark_overlay_open_key(app: &mut App, code: KeyCode) -> bool {
    matches!(code, KeyCode::Char('b')) && app.open_bookmark_overlay()
}

fn handle_bookmark_overlay_key(
    app: &mut App,
    code: KeyCode,
    modifiers: KeyModifiers,
) -> Option<Result<(CommandRefresh, BookmarkOverlayPostAction), String>> {
    if !app.is_bookmark_overlay_visible() {
        return None;
    }

    let outcome = match code {
        KeyCode::Char('q') => {
            app.quit();
            Ok((CommandRefresh::None, BookmarkOverlayPostAction::None))
        }
        KeyCode::Esc | KeyCode::Char('b') => {
            app.close_bookmark_overlay();
            Ok((CommandRefresh::None, BookmarkOverlayPostAction::None))
        }
        KeyCode::Down | KeyCode::Char('j') if modifiers.is_empty() => {
            app.select_next_bookmark();
            Ok((CommandRefresh::None, BookmarkOverlayPostAction::None))
        }
        KeyCode::Up | KeyCode::Char('k') if modifiers.is_empty() => {
            app.select_previous_bookmark();
            Ok((CommandRefresh::None, BookmarkOverlayPostAction::None))
        }
        KeyCode::Enter => {
            let bookmark = match app.selected_group_bookmark().cloned() {
                Some(bookmark) => bookmark,
                None => return Some(Err("no bookmark is selected".to_string())),
            };
            match app.open_selected_bookmark() {
                Ok(_) => {
                    app.report_info(format!(
                        "Opened bookmarked {} '{}'.",
                        bookmark.title(),
                        bookmark.label
                    ));
                    Ok((CommandRefresh::Reconcile, BookmarkOverlayPostAction::None))
                }
                Err(err) => Err(err),
            }
        }
        KeyCode::Delete | KeyCode::Backspace | KeyCode::Char('d') if modifiers.is_empty() => {
            match app.remove_selected_bookmark() {
                Ok(removed) => {
                    app.report_info(format!(
                        "Removed bookmark for {} '{}'.",
                        removed.title(),
                        removed.label
                    ));
                    Ok((
                        CommandRefresh::None,
                        BookmarkOverlayPostAction::PersistLocalState,
                    ))
                }
                Err(err) => Err(err),
            }
        }
        _ => Ok((CommandRefresh::None, BookmarkOverlayPostAction::None)),
    };

    Some(outcome)
}

fn handle_command_palette_key(
    app: &mut App,
    code: KeyCode,
    modifiers: KeyModifiers,
) -> Option<Result<(CommandRefresh, Option<CommandAction>), String>> {
    if !app.is_command_palette_visible() {
        return None;
    }

    let refresh = match code {
        KeyCode::Esc => {
            app.close_command_palette();
            (CommandRefresh::None, None)
        }
        KeyCode::Backspace => {
            app.pop_command_palette_char();
            (CommandRefresh::None, None)
        }
        KeyCode::Down | KeyCode::Char('j') if modifiers.is_empty() => {
            app.select_next_command_palette_action();
            (CommandRefresh::None, None)
        }
        KeyCode::Up | KeyCode::Char('k') if modifiers.is_empty() => {
            app.select_previous_command_palette_action();
            (CommandRefresh::None, None)
        }
        KeyCode::Enter => {
            let Some(action) = app.command_palette_selected_action() else {
                app.report_warn("No command matches current filter.".to_string());
                return Some(Ok((CommandRefresh::None, None)));
            };
            return Some(
                execute_command_action(app, action).map(|refresh| (refresh, Some(action))),
            );
        }
        KeyCode::Char(ch) if modifiers.is_empty() || modifiers == KeyModifiers::SHIFT => {
            app.push_command_palette_char(ch);
            (CommandRefresh::None, None)
        }
        _ => (CommandRefresh::None, None),
    };

    Some(Ok(refresh))
}

fn handle_mouse_event(
    runtime: &tokio::runtime::Runtime,
    client: Arc<IntrospectClient>,
    local_paths: &LocalPaths,
    app: &mut App,
    topic_preview: &mut TopicPreviewController,
    mouse_ui: &MouseUiState,
    mouse: MouseEvent,
) -> bool {
    match mouse.kind {
        MouseEventKind::Down(MouseButton::Left) => handle_mouse_click_target(
            runtime,
            client,
            local_paths,
            app,
            topic_preview,
            mouse_ui.click_target(mouse.column, mouse.row),
        ),
        MouseEventKind::ScrollDown => {
            handle_mouse_scroll_target(app, mouse_ui.scroll_target(mouse.column, mouse.row), true)
        }
        MouseEventKind::ScrollUp => {
            handle_mouse_scroll_target(app, mouse_ui.scroll_target(mouse.column, mouse.row), false)
        }
        _ => false,
    }
}

fn handle_mouse_click_target(
    runtime: &tokio::runtime::Runtime,
    client: Arc<IntrospectClient>,
    local_paths: &LocalPaths,
    app: &mut App,
    topic_preview: &mut TopicPreviewController,
    target: Option<MouseTarget>,
) -> bool {
    let Some(target) = target else {
        return false;
    };

    match target {
        MouseTarget::Tab(screen) => match app.open_screen(screen) {
            Ok(()) => true,
            Err(err) => {
                app.report_warn_from(EventSource::Navigation, err);
                false
            }
        },
        MouseTarget::OverviewAction(index) => {
            if !app.select_overview_action_index(index) {
                return false;
            }
            match app.activate_overview_action() {
                Ok(_) => true,
                Err(err) => {
                    app.report_warn_from(EventSource::Navigation, err);
                    false
                }
            }
        }
        MouseTarget::TopicRow(index) => app.select_topic_visible_index(index),
        MouseTarget::ServiceRow(index) => app.select_service_visible_index(index),
        MouseTarget::ActionRow(index) => app.select_action_visible_index(index),
        MouseTarget::NodeRow(index) => app.select_node_visible_index(index),
        MouseTarget::ZenohRouterRow(index) => app.select_zenoh_router_visible_index(index),
        MouseTarget::ServiceCallHintRow(index) => app.select_service_call_hint_index(index),
        MouseTarget::ActionCallHintRow(index) => app.select_action_call_hint_index(index),
        MouseTarget::EventTimelineRow(index) => app.select_filtered_event_index(index),
        MouseTarget::LogStreamRow(index) => app.select_log_stream_index(index),
        MouseTarget::BookmarkRow(index) => {
            if !app.select_bookmark_overlay_index(index) {
                return false;
            }
            let bookmark = match app.selected_group_bookmark().cloned() {
                Some(bookmark) => bookmark,
                None => return false,
            };
            match app.open_selected_bookmark() {
                Ok(_) => {
                    app.report_info(format!(
                        "Opened bookmarked {} '{}'.",
                        bookmark.title(),
                        bookmark.label
                    ));
                    apply_command_refresh(
                        runtime,
                        client,
                        app,
                        topic_preview,
                        CommandRefresh::Reconcile,
                    )
                }
                Err(err) => {
                    app.report_warn_from(EventSource::Bookmarks, err);
                    false
                }
            }
        }
        MouseTarget::CommandPaletteAction(index) => {
            if !app.select_command_palette_action_index(index) {
                return false;
            }
            let Some(action) = app.command_palette_selected_action() else {
                app.report_warn("No command matches current filter.".to_string());
                return false;
            };
            match execute_command_action(app, action) {
                Ok(refresh) => {
                    let should_refresh_active_view =
                        apply_command_refresh(runtime, client, app, topic_preview, refresh);
                    apply_command_post_action(local_paths, app, action);
                    should_refresh_active_view
                }
                Err(err) => {
                    app.report_warn_from(EventSource::Navigation, err);
                    false
                }
            }
        }
    }
}

fn handle_mouse_scroll_target(app: &mut App, target: Option<ScrollTarget>, downward: bool) -> bool {
    let Some(target) = target else {
        return false;
    };

    match target {
        ScrollTarget::OverviewActions
        | ScrollTarget::TopicList
        | ScrollTarget::ServiceList
        | ScrollTarget::ActionList
        | ScrollTarget::NodeList
        | ScrollTarget::ZenohRouterList => {
            if downward {
                app.select_next();
            } else {
                app.select_previous();
            }
            true
        }
        ScrollTarget::TopicPreview => {
            if downward {
                app.scroll_topic_preview_down(4);
            } else {
                app.scroll_topic_preview_up(4);
            }
            false
        }
        ScrollTarget::ServiceCallHintList => {
            if downward {
                app.service_call_select_next_hint();
            } else {
                app.service_call_select_previous_hint();
            }
            true
        }
        ScrollTarget::ServiceCallResponse => {
            if downward {
                app.scroll_service_call_response_down(4);
            } else {
                app.scroll_service_call_response_up(4);
            }
            false
        }
        ScrollTarget::ActionCallHintList => {
            if downward {
                app.action_call_select_next_hint();
            } else {
                app.action_call_select_previous_hint();
            }
            true
        }
        ScrollTarget::ActionCallResponse => {
            if downward {
                app.scroll_action_call_response_down(4);
            } else {
                app.scroll_action_call_response_up(4);
            }
            false
        }
        ScrollTarget::BookmarkList => {
            if downward {
                app.select_next_bookmark();
            } else {
                app.select_previous_bookmark();
            }
            false
        }
        ScrollTarget::CommandPaletteList => {
            if downward {
                app.select_next_command_palette_action();
            } else {
                app.select_previous_command_palette_action();
            }
            false
        }
        ScrollTarget::EventTimelineList => {
            if downward {
                app.select_next_filtered_event();
            } else {
                app.select_previous_filtered_event();
            }
            false
        }
        ScrollTarget::LogStreamList => {
            if downward {
                app.log_stream_select_next();
            } else {
                app.log_stream_select_previous();
            }
            false
        }
        ScrollTarget::DetailPanel => {
            if downward {
                app.scroll_detail_down(4);
            } else {
                app.scroll_detail_up(4);
            }
            false
        }
    }
}

fn dispatch_command_action(
    runtime: &tokio::runtime::Runtime,
    client: Arc<IntrospectClient>,
    local_paths: &LocalPaths,
    app: &mut App,
    topic_preview: &mut TopicPreviewController,
    action: CommandAction,
) -> bool {
    match execute_command_action(app, action) {
        Ok(refresh) => {
            let should_refresh_active_view =
                apply_command_refresh(runtime, client, app, topic_preview, refresh);
            apply_command_post_action(local_paths, app, action);
            should_refresh_active_view
        }
        Err(err) => {
            app.report_warn_from(EventSource::Navigation, err);
            false
        }
    }
}

fn handle_service_call_edit_key(
    runtime: &tokio::runtime::Runtime,
    client: &IntrospectClient,
    app: &mut App,
    code: KeyCode,
    modifiers: KeyModifiers,
) -> bool {
    if app.active_screen() != Screen::ServiceCall || !app.is_service_call_editing() {
        return false;
    }

    match code {
        KeyCode::Esc => app.exit_service_call_edit(),
        KeyCode::F(5) => submit_service_call(runtime, client, app),
        KeyCode::Left => app.service_call_move_left(),
        KeyCode::Right => app.service_call_move_right(),
        KeyCode::Up => app.service_call_move_up(),
        KeyCode::Down => app.service_call_move_down(),
        KeyCode::Home => app.service_call_move_home(),
        KeyCode::End => app.service_call_move_end(),
        KeyCode::Backspace => app.service_call_backspace(),
        KeyCode::Delete => app.service_call_delete(),
        KeyCode::Enter => app.service_call_insert_newline(),
        KeyCode::Tab => app.service_call_insert_tab(),
        KeyCode::Char(ch) if modifiers.is_empty() || modifiers == KeyModifiers::SHIFT => {
            app.service_call_insert_char(ch);
        }
        _ => return false,
    }

    true
}

fn handle_action_call_edit_key(
    runtime: &tokio::runtime::Runtime,
    client: Arc<IntrospectClient>,
    app: &mut App,
    feedback: &mut ActionFeedbackController,
    code: KeyCode,
    modifiers: KeyModifiers,
) -> bool {
    if app.active_screen() != Screen::ActionCall || !app.is_action_call_editing() {
        return false;
    }

    match code {
        KeyCode::Esc => app.exit_action_call_edit(),
        KeyCode::F(5) => submit_action_goal(runtime, client, app, feedback),
        KeyCode::Left => app.action_call_move_left(),
        KeyCode::Right => app.action_call_move_right(),
        KeyCode::Up => app.action_call_move_up(),
        KeyCode::Down => app.action_call_move_down(),
        KeyCode::Home => app.action_call_move_home(),
        KeyCode::End => app.action_call_move_end(),
        KeyCode::Backspace => app.action_call_backspace(),
        KeyCode::Delete => app.action_call_delete(),
        KeyCode::Enter => app.action_call_insert_newline(),
        KeyCode::Tab => app.action_call_insert_tab(),
        KeyCode::Char(ch) if modifiers.is_empty() || modifiers == KeyModifiers::SHIFT => {
            app.action_call_insert_char(ch);
        }
        _ => return false,
    }

    true
}

fn execute_command_action(app: &mut App, action: CommandAction) -> Result<CommandRefresh, String> {
    app.close_command_palette();

    match action {
        CommandAction::OpenOverview => {
            app.open_screen(Screen::Overview)?;
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::OpenTopics => {
            app.open_screen(Screen::Topics)?;
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::OpenServices => {
            app.open_screen(Screen::Services)?;
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::OpenServiceCall => {
            app.open_screen(Screen::ServiceCall)?;
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::OpenActions => {
            app.open_screen(Screen::Actions)?;
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::OpenActionCall => {
            app.open_screen(Screen::ActionCall)?;
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::OpenNodes => {
            app.open_screen(Screen::Nodes)?;
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::OpenContext => {
            app.open_screen(Screen::Context)?;
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::OpenZenoh => {
            app.open_screen(Screen::Zenoh)?;
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::OpenTelemetry => {
            app.open_screen(Screen::Telemetry)?;
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::OpenDiagnosticsTimeline => {
            app.open_event_timeline();
            app.report_info_from(
                EventSource::Data,
                "Opened diagnostics timeline.".to_string(),
            );
            Ok(CommandRefresh::None)
        }
        CommandAction::OpenTopologyExplorer => {
            app.open_topology_overlay()?;
            app.report_info_from(
                EventSource::Navigation,
                "Opened topology explorer.".to_string(),
            );
            Ok(CommandRefresh::None)
        }
        CommandAction::OpenRecorderLaunchpad => {
            app.open_recorder_overlay()?;
            app.report_info_from(
                EventSource::Navigation,
                "Opened recorder launchpad.".to_string(),
            );
            Ok(CommandRefresh::None)
        }
        CommandAction::MacroOpenTopicsSearch => {
            app.open_screen(Screen::Topics)?;
            app.enter_search();
            app.report_info_from(
                EventSource::Navigation,
                "Macro opened Topics and entered search mode.".to_string(),
            );
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::MacroOpenServicesSearch => {
            app.open_screen(Screen::Services)?;
            app.enter_search();
            app.report_info_from(
                EventSource::Navigation,
                "Macro opened Services and entered search mode.".to_string(),
            );
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::MacroOpenNodesSearch => {
            app.open_screen(Screen::Nodes)?;
            app.enter_search();
            app.report_info_from(
                EventSource::Navigation,
                "Macro opened Nodes and entered search mode.".to_string(),
            );
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::MacroOpenServiceCallEditor => {
            app.open_screen(Screen::ServiceCall)?;
            app.enter_service_call_edit();
            app.report_info_from(
                EventSource::Navigation,
                "Macro opened Service Call and focused the request editor.".to_string(),
            );
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::MacroOpenActionCallEditor => {
            app.open_screen(Screen::ActionCall)?;
            app.enter_action_call_edit();
            app.report_info_from(
                EventSource::Navigation,
                "Macro opened Action Call and focused the goal editor.".to_string(),
            );
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::OpenNodeComparison => {
            app.open_node_comparison()?;
            app.report_info_from(
                EventSource::Navigation,
                "Opened node comparison summary.".to_string(),
            );
            Ok(CommandRefresh::None)
        }
        CommandAction::OpenBookmarks => {
            app.open_bookmark_overlay();
            app.report_info_from(
                EventSource::Bookmarks,
                "Opened bookmarks overlay.".to_string(),
            );
            Ok(CommandRefresh::None)
        }
        CommandAction::RefreshSnapshot => {
            if app.active_screen() == Screen::ServiceCall {
                Ok(CommandRefresh::ActiveView)
            } else {
                Ok(CommandRefresh::DashboardAndActiveView)
            }
        }
        CommandAction::ToggleAutoRefresh => {
            let enabled = app.toggle_auto_refresh();
            app.report_info_from(
                EventSource::Data,
                format!(
                    "Auto-refresh {}.",
                    if enabled { "enabled" } else { "disabled" }
                ),
            );
            Ok(CommandRefresh::None)
        }
        CommandAction::JumpToTopicProviderNode => {
            let provider = app.jump_to_topic_provider_node()?;
            app.report_info_from(
                EventSource::Navigation,
                format!("Jumped to provider node '{}'.", provider),
            );
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::ToggleAdvancedMode => {
            let enabled = app.toggle_advanced_mode();
            if enabled {
                app.report_info_from(
                    EventSource::Navigation,
                    "Advanced mode enabled.".to_string(),
                );
            } else {
                app.report_info_from(
                    EventSource::Navigation,
                    "Advanced mode disabled.".to_string(),
                );
            }
            Ok(CommandRefresh::Reconcile)
        }
        CommandAction::ToggleCurrentBookmark => {
            let toggled = app.toggle_current_bookmark()?;
            if toggled.pinned {
                app.report_info_from(
                    EventSource::Bookmarks,
                    format!(
                        "Pinned {} '{}'.",
                        toggled.bookmark.title(),
                        toggled.bookmark.label
                    ),
                );
            } else {
                app.report_info_from(
                    EventSource::Bookmarks,
                    format!(
                        "Removed bookmark for {} '{}'.",
                        toggled.bookmark.title(),
                        toggled.bookmark.label
                    ),
                );
            }
            Ok(CommandRefresh::None)
        }
        CommandAction::ToggleNodeCompareAnchor => {
            let toggled = app.toggle_node_compare_anchor()?;
            if toggled.active {
                app.report_info_from(
                    EventSource::Navigation,
                    format!("Set node comparison anchor to '{}'.", toggled.node_name),
                );
            } else {
                app.report_info_from(
                    EventSource::Navigation,
                    format!("Cleared node comparison anchor '{}'.", toggled.node_name),
                );
            }
            Ok(CommandRefresh::None)
        }
        CommandAction::ToggleTopicRuntimeSection => {
            let expanded = app.toggle_topic_runtime_section();
            app.report_info_from(
                EventSource::Data,
                format!(
                    "Topic runtime section {}.",
                    if expanded { "expanded" } else { "collapsed" }
                ),
            );
            Ok(CommandRefresh::None)
        }
        CommandAction::ToggleServiceRuntimeSection => {
            let expanded = app.toggle_service_runtime_section();
            app.report_info_from(
                EventSource::Data,
                format!(
                    "Service runtime section {}.",
                    if expanded { "expanded" } else { "collapsed" }
                ),
            );
            Ok(CommandRefresh::None)
        }
        CommandAction::ToggleNodeRuntimeSection => {
            let expanded = app.toggle_node_runtime_section();
            app.report_info_from(
                EventSource::Data,
                format!(
                    "Node runtime section {}.",
                    if expanded { "expanded" } else { "collapsed" }
                ),
            );
            Ok(CommandRefresh::None)
        }
        CommandAction::ExportTopicPreviewPayload => {
            if app.selected_topic_preview_frame().is_none() {
                return Err("no topic preview frame is available for export".to_string());
            }
            Ok(CommandRefresh::None)
        }
        CommandAction::CopyTopicPreviewContent => {
            if app.selected_topic_preview_frame().is_none() {
                return Err("no topic preview frame is available for copying".to_string());
            }
            Ok(CommandRefresh::None)
        }
        CommandAction::ExportServiceResponsePayload => {
            if app.loaded_service_call_response().is_none() {
                return Err("no service response is available for export".to_string());
            }
            Ok(CommandRefresh::None)
        }
        CommandAction::OpenHelp => {
            app.toggle_help();
            Ok(CommandRefresh::None)
        }
    }
}

fn apply_command_refresh(
    runtime: &tokio::runtime::Runtime,
    client: Arc<IntrospectClient>,
    app: &mut App,
    topic_preview: &mut TopicPreviewController,
    refresh: CommandRefresh,
) -> bool {
    match refresh {
        CommandRefresh::None => false,
        CommandRefresh::Reconcile => true,
        CommandRefresh::ActiveView => {
            refresh_active_view(runtime, client.as_ref(), app);
            sync_topic_preview_subscription(runtime, client, app, topic_preview);
            false
        }
        CommandRefresh::DashboardAndActiveView => {
            refresh_dashboard(runtime, client.as_ref(), app);
            refresh_active_view(runtime, client.as_ref(), app);
            sync_topic_preview_subscription(runtime, client, app, topic_preview);
            false
        }
    }
}

fn apply_command_post_action(local_paths: &LocalPaths, app: &mut App, action: CommandAction) {
    let result = match action {
        CommandAction::ToggleCurrentBookmark
        | CommandAction::ToggleTopicRuntimeSection
        | CommandAction::ToggleServiceRuntimeSection
        | CommandAction::ToggleNodeRuntimeSection => persist_local_state(local_paths, app),
        CommandAction::ExportTopicPreviewPayload => export_selected_topic_preview(local_paths, app)
            .map(|message| app.report_info_from(EventSource::TopicPreview, message)),
        CommandAction::CopyTopicPreviewContent => copy_topic_preview_to_clipboard(app)
            .map(|message| app.report_info_from(EventSource::TopicPreview, message)),
        CommandAction::ExportServiceResponsePayload => {
            export_latest_service_response(local_paths, app)
                .map(|message| app.report_info_from(EventSource::ServiceCall, message))
        }
        CommandAction::OpenOverview
        | CommandAction::OpenTopics
        | CommandAction::OpenServices
        | CommandAction::OpenServiceCall
        | CommandAction::OpenActions
        | CommandAction::OpenActionCall
        | CommandAction::OpenNodes
        | CommandAction::OpenContext
        | CommandAction::OpenZenoh
        | CommandAction::OpenTelemetry
        | CommandAction::OpenDiagnosticsTimeline
        | CommandAction::OpenTopologyExplorer
        | CommandAction::OpenRecorderLaunchpad
        | CommandAction::MacroOpenTopicsSearch
        | CommandAction::MacroOpenServicesSearch
        | CommandAction::MacroOpenNodesSearch
        | CommandAction::MacroOpenServiceCallEditor
        | CommandAction::MacroOpenActionCallEditor
        | CommandAction::OpenNodeComparison
        | CommandAction::OpenBookmarks
        | CommandAction::RefreshSnapshot
        | CommandAction::ToggleAutoRefresh
        | CommandAction::JumpToTopicProviderNode
        | CommandAction::ToggleAdvancedMode
        | CommandAction::ToggleNodeCompareAnchor
        | CommandAction::OpenHelp => Ok(()),
    };

    if let Err(err) = result {
        app.report_error(err);
    }
}

fn persist_local_state(local_paths: &LocalPaths, app: &App) -> Result<(), String> {
    save_local_state(
        local_paths,
        LocalState {
            runtime_sections: app.runtime_section_preferences(),
            bookmarks: app.bookmarks().to_vec(),
        },
    )
}

fn export_selected_topic_preview(local_paths: &LocalPaths, app: &App) -> Result<String, String> {
    let frame = app
        .selected_topic_preview_frame()
        .ok_or_else(|| "no topic preview frame is available for export".to_string())?;
    let payload = build_topic_preview_export(app.group(), app.topic_preview())?;
    let path = write_export_payload(local_paths, "topic-preview", frame.topic.as_str(), &payload)?;
    Ok(format!(
        "Exported topic preview payload to {}.",
        path.display()
    ))
}

fn copy_topic_preview_to_clipboard(app: &App) -> Result<String, String> {
    let frame = app
        .selected_topic_preview_frame()
        .ok_or_else(|| "no topic preview frame is available for copying".to_string())?;
    let content = match frame.decoded_json.as_deref() {
        Some(decoded) => decoded.to_string(),
        None => format!("0x{}", frame.payload_hex),
    };
    let mut clipboard = arboard::Clipboard::new()
        .map_err(|err| format!("failed to access system clipboard: {}", err))?;
    clipboard
        .set_text(content)
        .map_err(|err| format!("failed to copy to clipboard: {}", err))?;
    Ok("Copied topic preview content to clipboard.".to_string())
}

fn export_latest_service_response(local_paths: &LocalPaths, app: &App) -> Result<String, String> {
    let response = app
        .loaded_service_call_response()
        .ok_or_else(|| "no service response is available for export".to_string())?;
    let service_name = app
        .service_call_target()
        .ok_or_else(|| "no service is selected for response export".to_string())?;
    let payload = build_service_response_export(app.group(), service_name, response)?;
    let path = write_export_payload(local_paths, "service-response", service_name, &payload)?;
    Ok(format!(
        "Exported service response payload to {}.",
        path.display()
    ))
}

fn export_recorder_launch_script(local_paths: &LocalPaths, app: &App) -> Result<String, String> {
    let plan = app.recorder_launch_plan()?;
    let path = write_text_export(
        local_paths,
        "recorder-launchpad",
        &plan.export_subject,
        "sh",
        &plan.export_script(),
    )?;
    Ok(format!(
        "Exported recorder launch script to {}.",
        path.display()
    ))
}

fn build_introspect_client(
    runtime: &tokio::runtime::Runtime,
    group: &str,
    client_mode_override: bool,
) -> Result<IntrospectClient> {
    let session = runtime.block_on(async {
        let mut config = aorta_core::Session::config_from_env_or_local_default()
            .await
            .map_err(|err| anyhow::anyhow!("Failed to build Zenoh config: {}", err))?;
        if client_mode_override {
            aorta_core::Session::apply_mode_override(&mut config, "client")
                .map_err(|err| anyhow::anyhow!("Failed to apply client mode override: {}", err))?;
        }
        zenoh::open(config)
            .await
            .map_err(|err| anyhow::anyhow!("Failed to open Zenoh session: {}", err))
    })?;
    let group = GroupName::new(group)?;
    Ok(IntrospectClient::from_session(
        Arc::new(session),
        IntrospectOptions::new(group),
    ))
}

fn refresh_dashboard(runtime: &tokio::runtime::Runtime, client: &IntrospectClient, app: &mut App) {
    match runtime.block_on(fetch_dashboard_data(client)) {
        Ok(data) => app.replace_dashboard(data),
        Err(err) => app.report_error_from(EventSource::Data, format!("Refresh failed: {}", err)),
    }
}

fn refresh_active_view(
    runtime: &tokio::runtime::Runtime,
    client: &IntrospectClient,
    app: &mut App,
) {
    match app.active_screen() {
        Screen::Overview => {}
        Screen::Topics => {
            let Some(selected) = app.topics().selected_row().cloned() else {
                app.set_topic_detail(DetailPane::Empty);
                return;
            };
            match runtime.block_on(fetch_topic_detail_data(client, &selected)) {
                Ok(detail) => app.set_topic_detail(DetailPane::Loaded(detail)),
                Err(err) => {
                    app.report_error_from(
                        EventSource::Data,
                        format!("Topic detail refresh failed: {}", err),
                    );
                    app.set_topic_detail(DetailPane::Error(err.to_string()));
                }
            }
        }
        Screen::Services => {
            let Some(selected) = app.services().selected_row().cloned() else {
                app.set_service_detail(DetailPane::Empty);
                return;
            };
            match runtime.block_on(fetch_service_detail_data(client, &selected)) {
                Ok(detail) => app.set_service_detail(DetailPane::Loaded(detail)),
                Err(err) => {
                    app.report_error_from(
                        EventSource::Data,
                        format!("Service detail refresh failed: {}", err),
                    );
                    app.set_service_detail(DetailPane::Error(err.to_string()));
                }
            }
        }
        Screen::ServiceCall => {
            let Some(service_name) = app.service_call_target().map(str::to_string) else {
                app.set_service_call_schema(DetailPane::Empty);
                app.report_warn_from(
                    EventSource::ServiceCall,
                    "Select a service and press `c` to start a call.".to_string(),
                );
                return;
            };
            match runtime.block_on(fetch_service_call_schema_data(client, &service_name)) {
                Ok(detail) => app.set_service_call_schema(DetailPane::Loaded(detail)),
                Err(err) => {
                    app.report_error_from(
                        EventSource::ServiceCall,
                        format!("Service call schema refresh failed: {}", err),
                    );
                    app.set_service_call_schema(DetailPane::Error(err.to_string()));
                }
            }
        }
        Screen::Actions => match runtime.block_on(fetch_actions_data(client)) {
            Ok(rows) => {
                let selected_name = app.actions().selected_row().map(|row| row.name.clone());
                app.replace_actions(rows);
                if let Some(name) = selected_name {
                    app.select_action_by_name(&name);
                }
                let Some(selected) = app.actions().selected_row().cloned() else {
                    app.set_action_detail(DetailPane::Empty);
                    return;
                };
                match runtime.block_on(fetch_action_detail_data(client, &selected)) {
                    Ok(detail) => app.set_action_detail(DetailPane::Loaded(detail)),
                    Err(err) => {
                        app.report_error_from(
                            EventSource::Data,
                            format!("Action detail refresh failed: {}", err),
                        );
                        app.set_action_detail(DetailPane::Error(err.to_string()));
                    }
                }
            }
            Err(err) => {
                app.report_error_from(
                    EventSource::Data,
                    format!("Actions refresh failed: {}", err),
                );
                app.set_action_detail(DetailPane::Error(err.to_string()));
            }
        },
        Screen::ActionCall => {
            let Some(action_name) = app.action_call_target().map(str::to_string) else {
                app.set_action_call_schema(DetailPane::Empty);
                app.report_warn_from(
                    EventSource::ActionCall,
                    "Select an action and press `c` to start a goal.".to_string(),
                );
                return;
            };
            match runtime.block_on(fetch_action_call_schema_data(client, &action_name)) {
                Ok(detail) => app.set_action_call_schema(DetailPane::Loaded(detail)),
                Err(err) => {
                    app.report_error_from(
                        EventSource::ActionCall,
                        format!("Action call schema refresh failed: {}", err),
                    );
                    app.set_action_call_schema(DetailPane::Error(err.to_string()));
                }
            }
        }
        Screen::Nodes => {
            let Some(selected) = app.nodes().selected_row().cloned() else {
                app.set_node_detail(DetailPane::Empty);
                return;
            };
            match runtime.block_on(fetch_node_detail_data(client, &selected)) {
                Ok(detail) => app.set_node_detail(DetailPane::Loaded(detail)),
                Err(err) => {
                    app.report_error_from(
                        EventSource::Data,
                        format!("Node detail refresh failed: {}", err),
                    );
                    app.set_node_detail(DetailPane::Error(err.to_string()));
                }
            }
        }
        Screen::Context => match runtime.block_on(fetch_context_data(client)) {
            Ok(data) => app.set_context(DetailPane::Loaded(data)),
            Err(err) => {
                app.report_error_from(
                    EventSource::Data,
                    format!("Context refresh failed: {}", err),
                );
                app.set_context(DetailPane::Error(err.to_string()));
            }
        },
        Screen::Zenoh => {
            match runtime.block_on(fetch_zenoh_connectivity_event_data(client, 32)) {
                Ok(events) => app.apply_zenoh_connectivity_events(&events),
                Err(err) => app.report_warn_from(
                    EventSource::Zenoh,
                    format!("Zenoh connectivity events refresh failed: {}", err),
                ),
            }

            match runtime.block_on(fetch_zenoh_connectivity_data(client)) {
                Ok(detail) => app.set_zenoh_connectivity(DetailPane::Loaded(detail)),
                Err(err) => {
                    app.report_error_from(
                        EventSource::Zenoh,
                        format!("Zenoh connectivity refresh failed: {}", err),
                    );
                    app.set_zenoh_connectivity(DetailPane::Error(err.to_string()));
                }
            }

            match runtime.block_on(fetch_zenoh_internals_data(client)) {
                Ok(detail) => app.set_zenoh_internals(DetailPane::Loaded(detail)),
                Err(err) => {
                    app.report_error_from(
                        EventSource::Zenoh,
                        format!("Zenoh internals refresh failed: {}", err),
                    );
                    app.set_zenoh_internals(DetailPane::Error(err.to_string()));
                }
            }
        }
        // The Telemetry dashboard renders from the client-observed latency rings
        // that are accumulated when node/service detail is refreshed; it issues no
        // dedicated fetch of its own.
        Screen::Telemetry => {}
    }
}

fn sync_topic_preview_subscription(
    runtime: &tokio::runtime::Runtime,
    client: Arc<IntrospectClient>,
    app: &mut App,
    controller: &mut TopicPreviewController,
) {
    let target_topic = if app.active_screen() == Screen::Topics {
        app.topics().selected_row().map(|row| row.name.clone())
    } else {
        None
    };

    if topic_preview_subscription_matches_target(controller, &target_topic) {
        return;
    }

    cancel_topic_preview_subscription(controller);

    let Some(topic_name) = target_topic else {
        app.clear_topic_preview();
        return;
    };

    let topic = match TopicSuffix::new(&topic_name) {
        Ok(topic) => topic,
        Err(err) => {
            app.clear_topic_preview();
            app.report_error_from(
                EventSource::TopicPreview,
                format!("Failed to start topic preview: {}", err),
            );
            return;
        }
    };

    let (sender, receiver) = watch::channel(None);
    let sender_for_error = sender.clone();
    let (stop_tx, stop_rx) = oneshot::channel();

    controller.topic_name = Some(topic_name.clone());
    controller.receiver = Some(receiver);
    controller.stop_tx = Some(stop_tx);
    app.begin_topic_preview(topic_name);

    runtime.spawn(async move {
        if let Err(err) = client.watch_topic_preview(topic, sender, stop_rx).await {
            let _ = sender_for_error.send(Some(TopicPreviewEvent::Error(err.to_string())));
        }
    });
}

fn topic_preview_subscription_matches_target(
    controller: &mut TopicPreviewController,
    target_topic: &Option<String>,
) -> bool {
    if controller.topic_name != *target_topic {
        return false;
    }
    controller
        .receiver
        .as_mut()
        .map(|receiver| receiver.has_changed().is_ok())
        .unwrap_or(false)
}

fn cancel_topic_preview_subscription(controller: &mut TopicPreviewController) {
    if let Some(stop_tx) = controller.stop_tx.take() {
        let _ = stop_tx.send(());
    }
    controller.topic_name = None;
    controller.receiver = None;
}

fn drain_topic_preview_events(app: &mut App, controller: &mut TopicPreviewController) {
    let Some(receiver) = controller.receiver.as_mut() else {
        return;
    };

    let Ok(true) = receiver.has_changed() else {
        return;
    };
    let Some(event) = receiver.borrow_and_update().clone() else {
        return;
    };

    match event {
        TopicPreviewEvent::Sample(frame) => app.apply_topic_preview_frame(frame),
        TopicPreviewEvent::Error(error) => {
            app.set_topic_preview_error(error.clone());
            app.report_error_from(
                EventSource::TopicPreview,
                format!("Topic preview error: {}", error),
            );
        }
    }
}

// --- Live `/log` stream controller (Feature C4) ---------------------------

/// Start the group-scoped `/log` subscription. Called once at startup; the
/// stream runs until [`cancel_log_stream_subscription`] on exit. Mirrors
/// [`sync_topic_preview_subscription`] minus the selection-tracking.
fn start_log_stream_subscription(
    runtime: &tokio::runtime::Runtime,
    client: Arc<IntrospectClient>,
    app: &mut App,
    controller: &mut LogStreamController,
) {
    // Idempotent: never start a second subscriber if one is already live.
    if controller.receiver.is_some() {
        return;
    }

    let (sender, receiver) = watch::channel(None);
    let sender_for_error = sender.clone();
    let (stop_tx, stop_rx) = oneshot::channel();

    controller.receiver = Some(receiver);
    controller.stop_tx = Some(stop_tx);

    runtime.spawn(async move {
        if let Err(err) = client.watch_log_stream(sender, stop_rx).await {
            let _ = sender_for_error.send(Some(LogEvent::Error(err.to_string())));
        }
    });

    app.report_info_from(
        EventSource::System,
        "Started live /log stream (press `L` to view).".to_string(),
    );
}

fn cancel_log_stream_subscription(controller: &mut LogStreamController) {
    if let Some(stop_tx) = controller.stop_tx.take() {
        let _ = stop_tx.send(());
    }
    controller.receiver = None;
}

fn drain_log_events(app: &mut App, controller: &mut LogStreamController) {
    // NOTE: the `watch` channel holds only the LATEST value, so this applies at
    // most ONE record per loop tick (intra-tick bursts coalesce). Acceptable for a
    // TUI log viewer at normal rates; a burst-lossless variant would switch the
    // producer to an mpsc channel and drain all pending here. Do not add a loop
    // expecting to pull multiple events — `watch` cannot deliver them.
    let Some(receiver) = controller.receiver.as_mut() else {
        return;
    };

    let Ok(true) = receiver.has_changed() else {
        return;
    };
    let Some(event) = receiver.borrow_and_update().clone() else {
        return;
    };

    match event {
        LogEvent::Record(frame) => app.push_log_row(frame.into()),
        LogEvent::Error(error) => {
            app.report_error_from(EventSource::System, format!("/log stream error: {}", error));
        }
    }
}

// --- Live action feedback controller (Feature C2) -------------------------

/// Start (or restart) the per-goal feedback subscription. Cancels any in-flight
/// feedback subscription first, clears the panel, then subscribes to the new
/// goal's feedback. Mirrors [`start_log_stream_subscription`].
fn start_action_feedback_subscription(
    runtime: &tokio::runtime::Runtime,
    client: Arc<IntrospectClient>,
    app: &mut App,
    controller: &mut ActionFeedbackController,
    action: String,
    goal_id: String,
) {
    cancel_action_feedback_subscription(controller);
    app.clear_action_feedback();

    let (sender, receiver) = mpsc::unbounded_channel();
    let sender_for_error = sender.clone();
    let (stop_tx, stop_rx) = oneshot::channel();

    controller.action = Some(action.clone());
    controller.goal_id = Some(goal_id.clone());
    controller.receiver = Some(receiver);
    controller.stop_tx = Some(stop_tx);

    runtime.spawn(async move {
        if let Err(err) = client
            .watch_action_feedback(action, goal_id, sender, stop_rx)
            .await
        {
            let _ = sender_for_error.send(ActionFeedbackEvent::Error(err.to_string()));
        }
    });
}

fn cancel_action_feedback_subscription(controller: &mut ActionFeedbackController) {
    if let Some(stop_tx) = controller.stop_tx.take() {
        let _ = stop_tx.send(());
    }
    controller.action = None;
    controller.goal_id = None;
    controller.receiver = None;
}

/// Bind the feedback subscription to the Action Call screen lifecycle: when the
/// user leaves the screen, cancel the stream and clear the panel so feedback
/// never accumulates for a goal the user can no longer see. Started on goal
/// send (see [`submit_action_goal`]), never auto-started here.
fn sync_action_feedback_lifecycle(app: &mut App, controller: &mut ActionFeedbackController) {
    if controller.receiver.is_some() && app.active_screen() != Screen::ActionCall {
        cancel_action_feedback_subscription(controller);
        app.clear_action_feedback();
    }
}

fn drain_action_feedback_events(app: &mut App, controller: &mut ActionFeedbackController) {
    let Some(receiver) = controller.receiver.as_mut() else {
        return;
    };

    // Drain ALL queued events this tick — feedback can burst between UI loop
    // iterations and every sample must land in the per-goal history panel.
    while let Ok(event) = receiver.try_recv() {
        match event {
            ActionFeedbackEvent::Feedback(frame) => app.push_action_feedback_row(frame.into()),
            ActionFeedbackEvent::Error(error) => {
                app.report_error_from(
                    EventSource::ActionCall,
                    format!("Action feedback error: {}", error),
                );
            }
        }
    }
}

fn toggle_schema_describe(
    runtime: &tokio::runtime::Runtime,
    client: &IntrospectClient,
    app: &mut App,
) {
    match app.active_screen() {
        Screen::Topics => {
            if app.is_topic_schema_describe_visible() {
                app.clear_topic_schema_describe();
                app.report_info_from(EventSource::Data, "Closed topic schema viewer.".to_string());
                return;
            }
            let Some(selected) = app.topics().selected_row().cloned() else {
                app.report_warn_from(
                    EventSource::Data,
                    "Select a topic before opening the schema viewer.".to_string(),
                );
                return;
            };
            match runtime.block_on(fetch_topic_schema_describe(client, &selected)) {
                Ok(data) => {
                    app.set_topic_schema_describe(crate::app::DetailPane::Loaded(data));
                    app.report_info_from(
                        EventSource::Data,
                        format!("Loaded schema for topic '{}'.", selected.name),
                    );
                }
                Err(err) => {
                    let message = err.to_string();
                    app.report_error_from(
                        EventSource::Data,
                        format!(
                            "Failed to describe schema for topic '{}': {}",
                            selected.name, message
                        ),
                    );
                    app.set_topic_schema_describe(crate::app::DetailPane::Error(message));
                }
            }
        }
        Screen::Services => {
            if app.is_service_schema_describe_visible() {
                app.clear_service_schema_describe();
                app.report_info_from(
                    EventSource::Data,
                    "Closed service schema viewer.".to_string(),
                );
                return;
            }
            let Some(selected) = app.services().selected_row().cloned() else {
                app.report_warn_from(
                    EventSource::Data,
                    "Select a service before opening the schema viewer.".to_string(),
                );
                return;
            };
            match runtime.block_on(fetch_service_schema_describe(client, &selected)) {
                Ok(data) => {
                    app.set_service_schema_describe(crate::app::DetailPane::Loaded(data));
                    app.report_info_from(
                        EventSource::Data,
                        format!("Loaded schema for service '{}'.", selected.name),
                    );
                }
                Err(err) => {
                    let message = err.to_string();
                    app.report_error_from(
                        EventSource::Data,
                        format!(
                            "Failed to describe schema for service '{}': {}",
                            selected.name, message
                        ),
                    );
                    app.set_service_schema_describe(crate::app::DetailPane::Error(message));
                }
            }
        }
        _ => {}
    }
}

fn submit_service_call(
    runtime: &tokio::runtime::Runtime,
    client: &IntrospectClient,
    app: &mut App,
) {
    let Some(service_name) = app.service_call_target().map(str::to_string) else {
        app.report_warn_from(
            EventSource::ServiceCall,
            "No service selected for service call.".to_string(),
        );
        return;
    };

    match runtime.block_on(invoke_service_call(
        client,
        &service_name,
        app.service_call_request_json(),
    )) {
        Ok(response) => {
            app.set_service_call_response(DetailPane::Loaded(response));
            app.report_info_from(
                EventSource::ServiceCall,
                "Service call completed.".to_string(),
            );
        }
        Err(err) => {
            app.set_service_call_response(DetailPane::Error(err.to_string()));
            app.report_error_from(
                EventSource::ServiceCall,
                format!("Service call failed: {}", err),
            );
        }
    }
}

fn submit_action_goal(
    runtime: &tokio::runtime::Runtime,
    client: Arc<IntrospectClient>,
    app: &mut App,
    feedback: &mut ActionFeedbackController,
) {
    let Some(action_name) = app.action_call_target().map(str::to_string) else {
        app.report_warn_from(
            EventSource::ActionCall,
            "No action selected for goal send.".to_string(),
        );
        return;
    };

    match runtime.block_on(invoke_action_send_goal(
        client.as_ref(),
        &action_name,
        app.action_call_request_json(),
    )) {
        Ok(response) => {
            let accepted = response.accepted;
            let goal_id = response.goal_id.clone();
            app.set_action_call_response(DetailPane::Loaded(response));
            if accepted {
                app.report_info_from(
                    EventSource::ActionCall,
                    format!("Goal '{}' accepted.", goal_id),
                );
                // Start streaming this goal's live feedback into the panel. Any
                // previous goal's feedback subscription is cancelled and the
                // panel cleared by `start_action_feedback_subscription`.
                start_action_feedback_subscription(
                    runtime,
                    client,
                    app,
                    feedback,
                    action_name,
                    goal_id,
                );
            } else {
                // Rejected goals get no feedback stream; drop any stale one.
                cancel_action_feedback_subscription(feedback);
                app.clear_action_feedback();
                app.report_warn_from(
                    EventSource::ActionCall,
                    format!("Goal '{}' was rejected by the server.", goal_id),
                );
            }
        }
        Err(err) => {
            app.set_action_call_response(DetailPane::Error(err.to_string()));
            app.report_error_from(
                EventSource::ActionCall,
                format!("Send goal failed: {}", err),
            );
        }
    }
}

fn cancel_last_action_goal(
    runtime: &tokio::runtime::Runtime,
    client: &IntrospectClient,
    app: &mut App,
) {
    let Some(action_name) = app.action_call_target().map(str::to_string) else {
        app.report_warn_from(
            EventSource::ActionCall,
            "No action selected for cancel.".to_string(),
        );
        return;
    };
    let Some((goal_id, goal_secret)) = app
        .action_call_last_goal()
        .map(|(id, secret)| (id.to_string(), secret.to_string()))
    else {
        app.report_warn_from(
            EventSource::ActionCall,
            "No goal has been sent yet — send a goal before cancelling.".to_string(),
        );
        return;
    };

    match runtime.block_on(invoke_action_cancel(
        client,
        &action_name,
        &goal_id,
        &goal_secret,
    )) {
        Ok(result) => {
            let accepted = result.accepted;
            let goal_id = result.goal_id.clone();
            // Surface the cancel result in the same response pane (the goal_id
            // is carried through; goal_secret stays put for follow-up).
            app.set_action_call_response(DetailPane::Loaded(ActionCallResponseData {
                accepted: result.accepted,
                goal_id: result.goal_id,
                goal_secret,
                status: result.status,
                code: result.code,
                message: result.message,
                server_epoch: String::new(),
            }));
            if accepted {
                app.report_info_from(
                    EventSource::ActionCall,
                    format!("Cancel requested for goal '{}'.", goal_id),
                );
            } else {
                app.report_warn_from(
                    EventSource::ActionCall,
                    format!("Cancel for goal '{}' was rejected.", goal_id),
                );
            }
        }
        Err(err) => {
            app.set_action_call_response(DetailPane::Error(err.to_string()));
            app.report_error_from(
                EventSource::ActionCall,
                format!("Cancel goal failed: {}", err),
            );
        }
    }
}

fn init_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    execute!(io::stdout(), EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(io::stdout());
    let terminal = Terminal::new(backend)?;
    Ok(terminal)
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(stdout, DisableMouseCapture, LeaveAlternateScreen);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use aorta_introspect::client::{
        ActionFeedbackEvent, ActionFeedbackFrame, LogEvent, LogRecordFrame, TopicPreviewEvent,
    };
    use crossterm::event::{KeyCode, KeyModifiers};
    use tokio::sync::{mpsc, watch};

    use super::{
        apply_command_post_action, drain_action_feedback_events, drain_log_events,
        drain_topic_preview_events, execute_command_action, handle_bookmark_overlay_key,
        handle_bookmark_overlay_open_key, handle_command_palette_key,
        handle_command_palette_open_key, handle_event_timeline_key, handle_event_timeline_open_key,
        handle_log_stream_key, handle_log_stream_open_key, handle_mouse_scroll_target,
        handle_node_comparison_key, handle_node_comparison_open_key, handle_recorder_key,
        handle_recorder_open_key, handle_topology_key, handle_topology_open_key,
        keyboard_macro_action, should_auto_refresh, sync_action_feedback_lifecycle,
        topic_preview_subscription_matches_target, ActionFeedbackController,
        BookmarkOverlayPostAction, CommandRefresh, LogStreamController, TopicPreviewController,
        AUTO_REFRESH_INTERVAL, KEYBOARD_MACRO_BINDINGS, RESERVED_NORMAL_MODE_SHORTCUTS,
    };
    use crate::app::{
        sample_dashboard, App, CommandAction, DetailPane, EventSource, ResourceBookmark, Screen,
    };
    use crate::data::{ExactSchemaAvailability, TopicDetailData};
    use crate::mouse::ScrollTarget;
    use crate::state::{load_local_state, LocalPaths};
    use std::time::{Duration, Instant};

    fn topic_detail_with_provider(publisher_node_names: Vec<String>) -> TopicDetailData {
        TopicDetailData {
            name: "imu/data".to_string(),
            key_expr: "aorta/default/pub/imu/data".to_string(),
            provider_endpoint_count: 1,
            candidate_node_count: 1,
            publisher_node_count: Some(publisher_node_names.len()),
            subscriber_node_count: Some(0),
            schema_availability: ExactSchemaAvailability::Confirmed {
                hash: "deadbeef".to_string(),
            },
            schema_provenance: "exact",
            provider_endpoint_provenance: "declared",
            candidate_node_provenance: "exact",
            publisher_node_provenance: "exact",
            subscriber_node_provenance: "inferred",
            provider_endpoint_ids: vec!["ep-camera".to_string()],
            candidate_node_names: publisher_node_names.clone(),
            publisher_node_names,
            subscriber_node_names: Vec::new(),
            schema_note: None,
            candidate_nodes_note: None,
            publisher_nodes_note: None,
            subscriber_nodes_note: None,
            runtime_summary: None,
            runtime_provenance: "unavailable",
            runtime_note: None,
        }
    }

    #[test]
    fn should_auto_refresh_respects_enabled_flag_and_interval() {
        let now = Instant::now();
        let interval = Duration::from_secs(2);

        // Disabled: never fires, even when the interval has clearly elapsed.
        let long_ago = now - Duration::from_secs(10);
        assert!(!should_auto_refresh(false, long_ago, interval, now));

        // Enabled but interval not yet elapsed: does not fire.
        let recent = now - Duration::from_millis(500);
        assert!(!should_auto_refresh(true, recent, interval, now));

        // Enabled and interval elapsed: fires.
        assert!(should_auto_refresh(true, long_ago, interval, now));

        // Boundary: exactly at the interval is due.
        let exactly = now - interval;
        assert!(should_auto_refresh(true, exactly, interval, now));
    }

    #[test]
    fn resolve_group_prefers_cli_then_env_then_default() {
        // The TUI must honor AORTA_GROUP
        // instead of silently connecting to `default` on a grouped robot.
        assert_eq!(super::resolve_group(Some("cli_g"), Some("env_g")), "cli_g");
        assert_eq!(super::resolve_group(None, Some("env_g")), "env_g");
        assert_eq!(super::resolve_group(None, Some("  env_g  ")), "env_g");
        assert_eq!(super::resolve_group(None, Some("   ")), "default");
        assert_eq!(super::resolve_group(None, None), "default");
        assert_eq!(super::resolve_group(Some(""), Some("env_g")), "env_g");
    }

    #[test]
    fn toggle_auto_refresh_command_flips_flag_and_reports_status() {
        let mut app = App::default();
        assert!(!app.is_auto_refresh_enabled());

        let refresh = execute_command_action(&mut app, CommandAction::ToggleAutoRefresh)
            .expect("toggle auto-refresh on");
        assert_eq!(refresh, CommandRefresh::None);
        assert!(app.is_auto_refresh_enabled());
        assert_eq!(app.status(), "Auto-refresh enabled.");

        let refresh = execute_command_action(&mut app, CommandAction::ToggleAutoRefresh)
            .expect("toggle auto-refresh off");
        assert_eq!(refresh, CommandRefresh::None);
        assert!(!app.is_auto_refresh_enabled());
        assert_eq!(app.status(), "Auto-refresh disabled.");

        // AUTO_REFRESH_INTERVAL is coarse enough to avoid stalling the blocking loop.
        assert!(AUTO_REFRESH_INTERVAL >= Duration::from_secs(1));
    }

    #[test]
    fn jump_to_topic_provider_node_selects_node_and_switches_screen() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        // Provider label uses the `name (id)` format produced by the topic detail.
        app.set_topic_detail(DetailPane::Loaded(topic_detail_with_provider(vec![
            "planner (a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6)".to_string(),
        ])));

        let refresh = execute_command_action(&mut app, CommandAction::JumpToTopicProviderNode)
            .expect("jump to provider node");

        assert_eq!(refresh, CommandRefresh::Reconcile);
        assert_eq!(app.active_screen(), Screen::Nodes);
        assert_eq!(
            app.nodes().selected_row().map(|row| row.name.as_str()),
            Some("planner")
        );
        assert!(app.status().contains("Jumped to provider node"));
    }

    #[test]
    fn jump_to_topic_provider_node_hints_when_no_provider_is_known() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.open_screen(Screen::Topics).expect("open topics");
        app.set_topic_detail(DetailPane::Loaded(topic_detail_with_provider(Vec::new())));

        let err = execute_command_action(&mut app, CommandAction::JumpToTopicProviderNode)
            .expect_err("no provider should be a hinted no-op");
        assert!(err.contains("no known provider node"));
        // Stays on the Topics screen.
        assert_eq!(app.active_screen(), Screen::Topics);
    }

    #[test]
    fn execute_command_action_toggles_advanced_mode_and_reports_status() {
        let mut app = App::default();

        let refresh = execute_command_action(&mut app, CommandAction::ToggleAdvancedMode)
            .expect("toggle advanced");

        assert_eq!(refresh, CommandRefresh::Reconcile);
        assert!(app.is_advanced_mode());
        assert_eq!(app.status(), "Advanced mode enabled.");
    }

    #[test]
    fn execute_command_action_requires_selected_service_for_service_call() {
        let mut app = App::default();

        let err = execute_command_action(&mut app, CommandAction::OpenServiceCall)
            .expect_err("service call should fail without selection");

        assert!(err.contains("no service is available"));
    }

    #[test]
    fn execute_command_action_opens_service_call_when_service_exists() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        let refresh = execute_command_action(&mut app, CommandAction::OpenServiceCall)
            .expect("open service call");

        assert_eq!(refresh, CommandRefresh::Reconcile);
        assert_eq!(app.active_screen(), Screen::ServiceCall);
        assert_eq!(app.service_call_target(), Some("nav/plan"));
    }

    #[test]
    fn execute_command_action_opens_context_screen() {
        let mut app = App::default();
        assert_ne!(app.active_screen(), Screen::Context);

        let refresh = execute_command_action(&mut app, CommandAction::OpenContext)
            .expect("open context screen");

        assert_eq!(refresh, CommandRefresh::Reconcile);
        assert_eq!(app.active_screen(), Screen::Context);
    }

    #[test]
    fn context_screen_is_a_normal_visible_screen() {
        let app = App::default();
        // Context is always visible (not gated behind advanced mode), unlike Zenoh.
        assert!(app.visible_screens().contains(&Screen::Context));
        assert!(!app.visible_screens().contains(&Screen::Zenoh));
    }

    #[test]
    fn execute_command_action_opens_topology_explorer_when_supported() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();

        let refresh = execute_command_action(&mut app, CommandAction::OpenTopologyExplorer)
            .expect("open topology explorer");

        assert_eq!(refresh, CommandRefresh::None);
        assert!(app.is_topology_overlay_visible());
        assert_eq!(app.status(), "Opened topology explorer.");
    }

    #[test]
    fn execute_command_action_opens_recorder_launchpad_when_supported() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();

        let refresh = execute_command_action(&mut app, CommandAction::OpenRecorderLaunchpad)
            .expect("open recorder launchpad");

        assert_eq!(refresh, CommandRefresh::None);
        assert!(app.is_recorder_overlay_visible());
        assert_eq!(app.status(), "Opened recorder launchpad.");
    }

    #[test]
    fn execute_command_action_runs_topic_search_macro() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        let refresh = execute_command_action(&mut app, CommandAction::MacroOpenTopicsSearch)
            .expect("run topic search macro");

        assert_eq!(refresh, CommandRefresh::Reconcile);
        assert_eq!(app.active_screen(), Screen::Topics);
        assert_eq!(app.interaction_mode(), crate::app::InteractionMode::Search);
        assert_eq!(app.status(), "Macro opened Topics and entered search mode.");
    }

    #[test]
    fn execute_command_action_runs_service_call_editor_macro() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        let refresh = execute_command_action(&mut app, CommandAction::MacroOpenServiceCallEditor)
            .expect("run service call editor macro");

        assert_eq!(refresh, CommandRefresh::Reconcile);
        assert_eq!(app.active_screen(), Screen::ServiceCall);
        assert!(app.is_service_call_editing());
        assert_eq!(
            app.status(),
            "Macro opened Service Call and focused the request editor."
        );
    }

    #[test]
    fn execute_command_action_runs_action_call_editor_macro() {
        let mut app = App::default();
        app.replace_actions(vec![crate::app::sample_action_row()]);
        app.open_screen(Screen::Actions).expect("open actions");

        let refresh = execute_command_action(&mut app, CommandAction::MacroOpenActionCallEditor)
            .expect("run action call editor macro");

        assert_eq!(refresh, CommandRefresh::Reconcile);
        assert_eq!(app.active_screen(), Screen::ActionCall);
        assert!(app.is_action_call_editing());
        assert_eq!(app.action_call_target(), Some("demo/execute_task"));
        assert_eq!(
            app.status(),
            "Macro opened Action Call and focused the goal editor."
        );
    }

    #[test]
    fn execute_command_action_opens_node_comparison_when_ready() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        prepare_node_comparison(&mut app);

        let refresh = execute_command_action(&mut app, CommandAction::OpenNodeComparison)
            .expect("open node comparison");

        assert_eq!(refresh, CommandRefresh::None);
        assert!(app.is_node_comparison_visible());
        assert_eq!(app.status(), "Opened node comparison summary.");
    }

    #[test]
    fn execute_command_action_refresh_snapshot_requests_expected_refresh_kind() {
        let mut app = App::default();
        assert_eq!(
            execute_command_action(&mut app, CommandAction::RefreshSnapshot)
                .expect("refresh snapshot"),
            CommandRefresh::DashboardAndActiveView
        );

        app.replace_dashboard(sample_dashboard());
        app.open_service_call_from_selected_service();
        app.exit_service_call_edit();

        assert_eq!(
            execute_command_action(&mut app, CommandAction::RefreshSnapshot)
                .expect("refresh active view"),
            CommandRefresh::ActiveView
        );
    }

    #[test]
    fn command_palette_open_key_respects_normal_mode() {
        let mut app = App::default();
        assert!(handle_command_palette_open_key(
            &mut app,
            KeyCode::Char(':')
        ));
        assert!(app.is_command_palette_visible());

        app.close_command_palette();
        app.enter_search();
        assert!(!handle_command_palette_open_key(
            &mut app,
            KeyCode::Char(':')
        ));
        assert!(!app.is_command_palette_visible());
    }

    #[test]
    fn bookmark_overlay_open_key_respects_normal_mode() {
        let mut app = App::default();
        assert!(handle_bookmark_overlay_open_key(
            &mut app,
            KeyCode::Char('b')
        ));
        assert!(app.is_bookmark_overlay_visible());

        app.close_bookmark_overlay();
        app.enter_search();
        assert!(!handle_bookmark_overlay_open_key(
            &mut app,
            KeyCode::Char('b')
        ));
        assert!(!app.is_bookmark_overlay_visible());
    }

    #[test]
    fn event_timeline_open_key_respects_normal_mode() {
        let mut app = App::default();
        assert!(handle_event_timeline_open_key(&mut app, KeyCode::Char('e')));
        assert!(app.is_event_timeline_visible());

        app.close_event_timeline();
        app.enter_search();
        assert!(!handle_event_timeline_open_key(
            &mut app,
            KeyCode::Char('e')
        ));
        assert!(!app.is_event_timeline_visible());
    }

    #[test]
    fn recorder_open_key_respects_normal_mode() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        assert!(handle_recorder_open_key(&mut app, KeyCode::Char('o')));
        assert!(app.is_recorder_overlay_visible());

        app.close_recorder_overlay();
        app.enter_search();
        assert!(!handle_recorder_open_key(&mut app, KeyCode::Char('o')));
        assert!(!app.is_recorder_overlay_visible());
    }

    #[test]
    fn keyboard_macro_shortcuts_are_unique_and_non_conflicting() {
        let mut seen = std::collections::HashSet::new();
        for binding in KEYBOARD_MACRO_BINDINGS {
            assert!(
                seen.insert(binding.key),
                "duplicate macro key: {}",
                binding.key
            );
            assert!(
                !RESERVED_NORMAL_MODE_SHORTCUTS.contains(&binding.key),
                "macro key conflicts with reserved direct shortcut: {}",
                binding.key
            );
            assert_eq!(
                keyboard_macro_action(KeyCode::Char(binding.key)),
                Some(binding.action)
            );
        }
    }

    #[test]
    fn macro_shortcut_key_opens_topics_search_flow() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        let action = keyboard_macro_action(KeyCode::Char('T')).expect("macro action");
        let refresh = execute_command_action(&mut app, action).expect("execute macro");

        assert_eq!(refresh, CommandRefresh::Reconcile);
        assert_eq!(app.active_screen(), Screen::Topics);
        assert_eq!(app.command_palette_query(), "");
        assert!(app.is_searching());
    }

    #[test]
    fn topology_open_key_respects_normal_mode() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();

        assert!(handle_topology_open_key(&mut app, KeyCode::Char('g')));
        assert!(app.is_topology_overlay_visible());

        app.close_topology_overlay();
        app.enter_search();
        assert!(!handle_topology_open_key(&mut app, KeyCode::Char('g')));
        assert!(!app.is_topology_overlay_visible());
    }

    #[test]
    fn node_comparison_open_key_respects_normal_mode() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        prepare_node_comparison(&mut app);

        assert!(handle_node_comparison_open_key(
            &mut app,
            KeyCode::Char('c')
        ));
        assert!(app.is_node_comparison_visible());

        app.close_node_comparison();
        app.enter_search();
        assert!(!handle_node_comparison_open_key(
            &mut app,
            KeyCode::Char('c')
        ));
        assert!(!app.is_node_comparison_visible());
    }

    #[test]
    fn handle_command_palette_key_filters_selects_and_executes_actions() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        assert!(app.open_command_palette());

        let initial = app.command_palette_selected_action();
        assert_eq!(
            handle_command_palette_key(&mut app, KeyCode::Char('j'), KeyModifiers::NONE),
            Some(Ok((CommandRefresh::None, None)))
        );
        assert_ne!(app.command_palette_selected_action(), initial);
        assert_eq!(
            handle_command_palette_key(&mut app, KeyCode::Char('k'), KeyModifiers::NONE),
            Some(Ok((CommandRefresh::None, None)))
        );
        assert_eq!(app.command_palette_selected_action(), initial);

        for ch in "calls".chars() {
            assert_eq!(
                handle_command_palette_key(&mut app, KeyCode::Char(ch), KeyModifiers::NONE),
                Some(Ok((CommandRefresh::None, None)))
            );
        }
        assert_eq!(
            handle_command_palette_key(&mut app, KeyCode::Backspace, KeyModifiers::NONE),
            Some(Ok((CommandRefresh::None, None)))
        );
        assert_eq!(app.command_palette_query(), "call");
        assert_eq!(
            app.command_palette_selected_action(),
            Some(CommandAction::OpenServiceCall)
        );

        assert_eq!(
            handle_command_palette_key(&mut app, KeyCode::Enter, KeyModifiers::NONE),
            Some(Ok((
                CommandRefresh::Reconcile,
                Some(CommandAction::OpenServiceCall)
            )))
        );
        assert_eq!(app.active_screen(), Screen::ServiceCall);
        assert_eq!(app.service_call_target(), Some("nav/plan"));
        assert!(!app.is_command_palette_visible());
    }

    #[test]
    fn handle_command_palette_key_keeps_palette_open_for_empty_results_and_can_close() {
        let mut app = App::default();
        assert!(app.open_command_palette());

        for ch in ['~', '~', '~'] {
            let _ = handle_command_palette_key(&mut app, KeyCode::Char(ch), KeyModifiers::NONE);
        }
        assert!(app.command_palette_actions().is_empty());

        assert_eq!(
            handle_command_palette_key(&mut app, KeyCode::Enter, KeyModifiers::NONE),
            Some(Ok((CommandRefresh::None, None)))
        );
        assert_eq!(app.status(), "No command matches current filter.");
        assert!(app.is_command_palette_visible());

        assert_eq!(
            handle_command_palette_key(&mut app, KeyCode::Esc, KeyModifiers::NONE),
            Some(Ok((CommandRefresh::None, None)))
        );
        assert!(!app.is_command_palette_visible());
    }

    #[test]
    fn handle_bookmark_overlay_key_opens_bookmarked_topic_and_closes_overlay() {
        let mut app = App::default();
        app.set_group("robot_a");
        app.replace_dashboard(sample_dashboard());
        app.replace_bookmarks(vec![ResourceBookmark::topic("robot_a", "camera/front")]);
        assert!(app.open_bookmark_overlay());

        assert_eq!(
            handle_bookmark_overlay_key(&mut app, KeyCode::Enter, KeyModifiers::NONE),
            Some(Ok((
                CommandRefresh::Reconcile,
                BookmarkOverlayPostAction::None
            )))
        );
        assert_eq!(app.active_screen(), Screen::Topics);
        assert_eq!(
            app.topics().selected_row().map(|row| row.name.as_str()),
            Some("camera/front")
        );
        assert!(!app.is_bookmark_overlay_visible());
    }

    #[test]
    fn handle_bookmark_overlay_key_removes_stale_bookmark() {
        let mut app = App::default();
        app.set_group("robot_a");
        app.replace_dashboard(sample_dashboard());
        app.replace_bookmarks(vec![ResourceBookmark::topic("robot_a", "missing/topic")]);
        assert!(app.open_bookmark_overlay());

        assert_eq!(
            handle_bookmark_overlay_key(&mut app, KeyCode::Char('d'), KeyModifiers::NONE),
            Some(Ok((
                CommandRefresh::None,
                BookmarkOverlayPostAction::PersistLocalState
            )))
        );
        assert!(app.current_group_bookmarks().is_empty());
        assert_eq!(app.status(), "Removed bookmark for Topic 'missing/topic'.");
    }

    #[test]
    fn handle_event_timeline_key_cycles_filters_and_closes_overlay() {
        let mut app = App::default();
        app.report_warn_from(EventSource::Data, "refresh degraded".to_string());
        assert!(app.open_event_timeline());

        assert_eq!(
            handle_event_timeline_key(&mut app, KeyCode::Char('s'), KeyModifiers::NONE),
            Some(Ok(CommandRefresh::None))
        );
        assert_eq!(
            app.event_timeline_severity_filter(),
            crate::app::EventTimelineSeverityFilter::WarnAndError
        );

        assert_eq!(
            handle_event_timeline_key(&mut app, KeyCode::Char('o'), KeyModifiers::NONE),
            Some(Ok(CommandRefresh::None))
        );
        assert_eq!(
            app.event_timeline_source_filter(),
            crate::app::EventTimelineSourceFilter::System
        );

        assert_eq!(
            handle_event_timeline_key(&mut app, KeyCode::Char('c'), KeyModifiers::NONE),
            Some(Ok(CommandRefresh::None))
        );
        assert_eq!(
            app.event_timeline_severity_filter(),
            crate::app::EventTimelineSeverityFilter::All
        );

        assert_eq!(
            handle_event_timeline_key(&mut app, KeyCode::Esc, KeyModifiers::NONE),
            Some(Ok(CommandRefresh::None))
        );
        assert!(!app.is_event_timeline_visible());
    }

    #[test]
    fn handle_mouse_scroll_target_scrolls_service_call_response() {
        let mut app = App::default();
        handle_mouse_scroll_target(&mut app, Some(ScrollTarget::ServiceCallResponse), true);
        assert_eq!(app.service_call_response_scroll(), 4);

        handle_mouse_scroll_target(&mut app, Some(ScrollTarget::ServiceCallResponse), false);
        assert_eq!(app.service_call_response_scroll(), 0);
    }

    #[test]
    fn handle_topology_key_closes_overlay() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.open_topology_overlay().expect("open topology");

        assert_eq!(
            handle_topology_key(&mut app, KeyCode::Esc, KeyModifiers::NONE),
            Some(Ok(CommandRefresh::None))
        );
        assert!(!app.is_topology_overlay_visible());
    }

    #[test]
    fn handle_recorder_key_exports_script_and_closes_overlay() {
        let root = unique_test_root("recorder-launchpad");
        let paths = LocalPaths::from_root(root.clone());
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        app.next_screen();
        app.open_recorder_overlay().expect("open recorder");

        assert_eq!(
            handle_recorder_key(&mut app, &paths, KeyCode::Char('y'), KeyModifiers::NONE),
            Some(Ok(CommandRefresh::None))
        );
        assert!(app.status().contains("Exported recorder launch script to"));

        let raw = read_only_exported_file(&paths);
        assert!(raw.contains("aorta record --group 'default'"));
        assert!(raw.contains("--include 'imu/data'"));

        assert_eq!(
            handle_recorder_key(&mut app, &paths, KeyCode::Esc, KeyModifiers::NONE),
            Some(Ok(CommandRefresh::None))
        );
        assert!(!app.is_recorder_overlay_visible());
        cleanup_test_root(root);
    }

    #[test]
    fn handle_node_comparison_key_closes_overlay() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        prepare_node_comparison(&mut app);
        app.open_node_comparison().expect("open compare");

        assert_eq!(
            handle_node_comparison_key(&mut app, KeyCode::Esc, KeyModifiers::NONE),
            Some(Ok(CommandRefresh::None))
        );
        assert!(!app.is_node_comparison_visible());
    }

    #[test]
    fn apply_command_post_action_persists_runtime_preferences() {
        let root = unique_test_root("runtime-prefs");
        let paths = LocalPaths::from_root(root.clone());
        let mut app = App::default();

        execute_command_action(&mut app, CommandAction::ToggleNodeRuntimeSection)
            .expect("toggle node runtime");
        apply_command_post_action(&paths, &mut app, CommandAction::ToggleNodeRuntimeSection);

        let saved = load_local_state(&paths).expect("load local state");
        assert!(!saved.runtime_sections.nodes_expanded);
        cleanup_test_root(root);
    }

    #[test]
    fn apply_command_post_action_persists_bookmarks() {
        let root = unique_test_root("bookmarks");
        let paths = LocalPaths::from_root(root.clone());
        let mut app = App::default();
        app.set_group("robot_a");
        app.replace_dashboard(sample_dashboard());
        app.next_screen();

        execute_command_action(&mut app, CommandAction::ToggleCurrentBookmark)
            .expect("toggle topic bookmark");
        apply_command_post_action(&paths, &mut app, CommandAction::ToggleCurrentBookmark);

        let saved = load_local_state(&paths).expect("load local state");
        assert_eq!(
            saved.bookmarks,
            vec![ResourceBookmark::topic("robot_a", "imu/data")]
        );
        cleanup_test_root(root);
    }

    #[test]
    fn apply_command_post_action_exports_topic_preview_payload_to_disk() {
        let root = unique_test_root("topic-export");
        let paths = LocalPaths::from_root(root.clone());
        let mut app = App::default();
        app.set_group("robot_a");
        app.begin_topic_preview("imu/data".to_string());
        app.apply_topic_preview_frame(crate::app::sample_topic_preview_frame(
            "00ff",
            Some("{\"imu\":1}"),
        ));

        execute_command_action(&mut app, CommandAction::ExportTopicPreviewPayload)
            .expect("validate export action");
        apply_command_post_action(&paths, &mut app, CommandAction::ExportTopicPreviewPayload);

        let raw = read_only_exported_file(&paths);
        assert!(raw.contains("\"kind\": \"topic_preview_payload\""));
        assert!(raw.contains("\"group\": \"robot_a\""));
        cleanup_test_root(root);
    }

    #[test]
    fn drain_topic_preview_events_applies_latest_frame_only() {
        let mut app = App::default();
        app.begin_topic_preview("imu/data".to_string());
        let (sender, receiver) = watch::channel(None);
        let mut controller = TopicPreviewController {
            topic_name: Some("imu/data".to_string()),
            receiver: Some(receiver),
            stop_tx: None,
        };

        let _ = sender.send(Some(TopicPreviewEvent::Sample(
            crate::app::sample_topic_preview_frame("00", Some("{\"seq\":0}")),
        )));
        let _ = sender.send(Some(TopicPreviewEvent::Sample(
            crate::app::sample_topic_preview_frame("01", Some("{\"seq\":1}")),
        )));

        drain_topic_preview_events(&mut app, &mut controller);

        assert_eq!(app.topic_preview().message_count(), 1);
        assert_eq!(
            app.topic_preview()
                .latest_frame()
                .map(|frame| frame.payload_hex.as_str()),
            Some("01")
        );
    }

    fn sample_log_frame(message: &str) -> LogRecordFrame {
        LogRecordFrame {
            timestamp_ns: 42,
            level: "INFO".to_string(),
            target: "aorta_core::node".to_string(),
            message: message.to_string(),
            node: "node-1".to_string(),
            decoded_json: None,
        }
    }

    fn sample_feedback_frame(goal_id: &str, seq: u64) -> ActionFeedbackFrame {
        ActionFeedbackFrame {
            goal_id: goal_id.to_string(),
            seq,
            stamp_ns: 1_000 + seq,
            status: "Executing".to_string(),
            message: "progress".to_string(),
            data_json: format!("{{\"seq\":{}}}", seq),
        }
    }

    #[test]
    fn drain_log_events_pushes_latest_record_into_bounded_buffer() {
        // Mirrors `drain_topic_preview_events_applies_latest_frame_only`: the
        // watch channel collapses to the latest value, so a single drain pushes
        // exactly one (the most recent) record into the bounded log buffer.
        let mut app = App::default();
        let (sender, receiver) = watch::channel(None);
        let mut controller = LogStreamController {
            receiver: Some(receiver),
            stop_tx: None,
        };

        let _ = sender.send(Some(LogEvent::Record(sample_log_frame("first"))));
        let _ = sender.send(Some(LogEvent::Record(sample_log_frame("second"))));

        drain_log_events(&mut app, &mut controller);

        assert_eq!(app.log_rows().len(), 1);
        assert_eq!(
            app.log_rows().back().map(|row| row.message.as_str()),
            Some("second")
        );
    }

    #[test]
    fn drain_log_events_accumulates_across_loop_iterations() {
        let mut app = App::default();
        let (sender, receiver) = watch::channel(None);
        let mut controller = LogStreamController {
            receiver: Some(receiver),
            stop_tx: None,
        };

        let _ = sender.send(Some(LogEvent::Record(sample_log_frame("a"))));
        drain_log_events(&mut app, &mut controller);
        let _ = sender.send(Some(LogEvent::Record(sample_log_frame("b"))));
        drain_log_events(&mut app, &mut controller);

        assert_eq!(app.log_rows().len(), 2);
        assert_eq!(
            app.log_rows().front().map(|row| row.message.as_str()),
            Some("a")
        );
        assert_eq!(
            app.log_rows().back().map(|row| row.message.as_str()),
            Some("b")
        );
    }

    #[test]
    fn drain_action_feedback_events_keeps_every_burst_sample() {
        let mut app = App::default();
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut controller = ActionFeedbackController {
            action: Some("nav/move".to_string()),
            goal_id: Some("goal-1".to_string()),
            receiver: Some(receiver),
            stop_tx: None,
        };

        // Two feedback samples arrive between UI ticks; both must reach the panel
        // (the old `watch` channel coalesced them and dropped seq 1).
        let _ = sender.send(ActionFeedbackEvent::Feedback(sample_feedback_frame(
            "goal-1", 1,
        )));
        let _ = sender.send(ActionFeedbackEvent::Feedback(sample_feedback_frame(
            "goal-1", 2,
        )));

        drain_action_feedback_events(&mut app, &mut controller);

        assert_eq!(
            app.action_call_feedback().len(),
            2,
            "no burst sample dropped"
        );
        assert_eq!(
            app.action_call_feedback().front().map(|row| row.seq),
            Some(1)
        );
        assert_eq!(
            app.action_call_feedback().back().map(|row| row.seq),
            Some(2)
        );
    }

    #[test]
    fn open_log_stream_overlay_toggles_visibility_and_closes() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());

        assert!(handle_log_stream_open_key(&mut app, KeyCode::Char('L')));
        assert!(app.is_log_stream_visible());

        // A lowercase `l` must NOT open it (uppercase-only shortcut).
        app.close_log_stream();
        assert!(!handle_log_stream_open_key(&mut app, KeyCode::Char('l')));
        assert!(!app.is_log_stream_visible());
    }

    #[test]
    fn log_stream_overlay_key_navigates_and_closes() {
        let mut app = App::default();
        app.push_log_row(sample_log_frame("a").into());
        app.push_log_row(sample_log_frame("b").into());
        assert!(app.open_log_stream());

        // j selects the next row; the overlay consumes the key.
        assert!(handle_log_stream_key(&mut app, KeyCode::Char('j'), KeyModifiers::NONE).is_some());
        assert_eq!(app.log_stream_selected_index(), Some(1));

        // Esc closes the overlay.
        assert!(handle_log_stream_key(&mut app, KeyCode::Esc, KeyModifiers::NONE).is_some());
        assert!(!app.is_log_stream_visible());

        // When not visible the handler declines (returns None).
        assert!(handle_log_stream_key(&mut app, KeyCode::Char('j'), KeyModifiers::NONE).is_none());
    }

    #[test]
    fn action_feedback_lifecycle_cancels_when_leaving_action_call_screen() {
        let mut app = App::default();
        app.replace_dashboard(sample_dashboard());
        // Pretend a feedback subscription is live with a buffered row.
        let (_sender, receiver) = mpsc::unbounded_channel::<ActionFeedbackEvent>();
        let mut controller = ActionFeedbackController {
            action: Some("nav/move".to_string()),
            goal_id: Some("goal-1".to_string()),
            receiver: Some(receiver),
            stop_tx: None,
        };
        app.push_action_feedback_row(sample_feedback_frame("goal-1", 1).into());

        // On a non-ActionCall screen (default is Overview) the lifecycle sync
        // tears the subscription down and clears the panel.
        assert_ne!(app.active_screen(), Screen::ActionCall);
        sync_action_feedback_lifecycle(&mut app, &mut controller);

        assert!(controller.receiver.is_none());
        assert!(app.action_call_feedback().is_empty());
    }

    #[test]
    fn topic_preview_subscription_restarts_when_watch_channel_is_closed() {
        let (sender, receiver) = watch::channel(None);
        let mut controller = TopicPreviewController {
            topic_name: Some("imu/data".to_string()),
            receiver: Some(receiver),
            stop_tx: None,
        };
        drop(sender);

        assert!(!topic_preview_subscription_matches_target(
            &mut controller,
            &Some("imu/data".to_string())
        ));
    }

    #[test]
    fn topic_preview_subscription_reuses_live_channel_for_same_topic() {
        let (sender, receiver) = watch::channel(None);
        let mut controller = TopicPreviewController {
            topic_name: Some("imu/data".to_string()),
            receiver: Some(receiver),
            stop_tx: None,
        };

        assert!(topic_preview_subscription_matches_target(
            &mut controller,
            &Some("imu/data".to_string())
        ));

        drop(sender);
    }

    fn prepare_node_comparison(app: &mut App) {
        app.open_screen(Screen::Nodes).expect("open nodes");
        app.toggle_node_compare_anchor().expect("anchor node");
        app.select_next();
    }

    fn read_only_exported_file(paths: &LocalPaths) -> String {
        let exported = fs::read_dir(paths.exports_dir())
            .expect("read export dir")
            .map(|entry| entry.expect("dir entry").path())
            .collect::<Vec<PathBuf>>();
        assert_eq!(exported.len(), 1);
        fs::read_to_string(&exported[0]).expect("read export file")
    }

    fn unique_test_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "aorta-tui-lib-{}-{}",
            label,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ))
    }

    fn cleanup_test_root(root: PathBuf) {
        let _ = fs::remove_dir_all(root);
    }
}
