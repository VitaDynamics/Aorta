//! Offline trace waterfall viewer (D1). A file-driven ratatui/crossterm TUI:
//! it discovers persisted `.atrc` trace files via the SAME `AORTA_TRACE_DIR`
//! resolution the writer uses, decodes them through the SAME reader + decode +
//! pairing path as `aorta-trace2perfetto`, and renders a per-(process,thread)
//! lane waterfall of spans + stages with durations.
//!
//! NO Zenoh, NO live session, NO trace enable/control: this is a viewer over
//! what is already on disk (the deferred-operator-surface boundary).

pub mod layout;
pub mod load;
pub mod state;
pub mod ui;

use std::io::{self, Stdout};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{
    self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tachyonfx::{fx, Effect, EffectTimer, Interpolation, Motion};

use crate::layout::{Layout, RegionButton};
use crate::state::WaterfallState;

/// One vertical page of lane rows for scroll keys (PgUp/PgDn approximation).
const PAGE_ROWS: usize = 10;

/// Outcome of handling a key: keep running, quit, or (from the empty/splash
/// screen) open a freshly-typed trace directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Continue,
    Quit,
    /// Re-run discovery against this path (typed on the splash screen).
    LoadPath(PathBuf),
    /// Tab-complete the splash path input against the filesystem.
    CompletePath,
}

/// Expand a leading `~` to `$HOME` for convenience in the path input.
fn expand_tilde(p: &str) -> PathBuf {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    } else if p == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home);
        }
    }
    PathBuf::from(p)
}

/// Apply a key to the viewer state. Pure (no terminal I/O) so it is fully
/// unit-testable. `view_rows` is the number of lane rows currently visible
/// (drives page scrolling); pass a small constant in tests.
pub fn handle_key(state: &mut WaterfallState, key: KeyEvent, view_rows: usize) -> Action {
    // Ctrl-C always quits.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return Action::Quit;
    }
    // Track the visible row budget so vertical auto-scroll can scroll down.
    state.view_rows = view_rows;
    // Empty/splash screen: the only interaction is editing the trace-dir path and
    // pressing Enter to open it (Esc quits). Swallow everything else as text.
    if state.is_empty() {
        match key.code {
            KeyCode::Enter => {
                state.path_completions.clear();
                let p = state.path_input.trim().to_string();
                if !p.is_empty() {
                    state.load_error = None;
                    return Action::LoadPath(expand_tilde(&p));
                }
            }
            // Tab → complete the path (the FS lookup runs in the event loop).
            KeyCode::Tab => return Action::CompletePath,
            // Esc clears a typed path first; only quits when the box is empty.
            KeyCode::Esc => {
                if state.path_input.is_empty() {
                    return Action::Quit;
                }
                state.path_input.clear();
                state.load_error = None;
                state.path_completions.clear();
            }
            KeyCode::Backspace => {
                state.path_input.pop();
                state.path_completions.clear();
            }
            KeyCode::Char(c) => {
                state.path_input.push(c);
                state.path_completions.clear();
            }
            _ => {}
        }
        return Action::Continue;
    }
    // Help overlay is modal: any of ?, Esc, q closes it; everything else no-ops.
    if state.show_help {
        if matches!(
            key.code,
            KeyCode::Char('?') | KeyCode::Esc | KeyCode::Char('q')
        ) {
            state.show_help = false;
        }
        return Action::Continue;
    }
    // Find picker is modal: text filters the name list; ↑/↓ pick a name (with
    // live preview); Enter confirms it; Esc cancels.
    if state.search_input.is_some() {
        match key.code {
            KeyCode::Enter => state.search_commit(),
            KeyCode::Esc => state.search_clear(),
            KeyCode::Up => state.search_name_move(false),
            KeyCode::Down => state.search_name_move(true),
            KeyCode::Backspace => state.search_input_backspace(),
            KeyCode::Char(c) => state.search_input_push(c),
            _ => {}
        }
        return Action::Continue;
    }
    // Region-filter input mode (same shape as search input).
    if state.region_filter_input.is_some() {
        match key.code {
            KeyCode::Enter => state.region_filter_commit(),
            KeyCode::Esc => state.region_filter_clear(),
            KeyCode::Backspace => state.region_filter_input_backspace(),
            KeyCode::Char(c) => state.region_filter_input_push(c),
            _ => {}
        }
        return Action::Continue;
    }
    match key.code {
        KeyCode::Char('q') => return Action::Quit,
        // Esc unwinds one layer at a time: region filter → region/search → quit.
        KeyCode::Esc => {
            if state.region_filter.is_some() {
                state.region_filter_clear();
            } else if state.region.is_some() || state.search_query.is_some() {
                state.clear_region();
                state.search_clear();
            } else {
                return Action::Quit;
            }
        }
        // toggle the keybinding/mouse help overlay
        KeyCode::Char('?') => state.toggle_help(),
        // search
        KeyCode::Char('/') => state.search_begin(),
        KeyCode::Char('n') => state.search_cycle(true),
        KeyCode::Char('N') => state.search_cycle(false),
        // toggle the time axis between relative ns and absolute wall-clock
        KeyCode::Char('t') => state.toggle_clock(),
        // jump to the caller (parent) / first callee (child) of the selection
        KeyCode::Char('p') => state.jump_to_caller(),
        KeyCode::Char('P') => state.jump_to_first_callee(),
        // z: zoom to the marqueed region if one is set, else onto the selected
        // span — "zoom to my current focus". Z always zooms the selected span.
        KeyCode::Char('z') => {
            if state.region.is_some() {
                state.zoom_to_region();
            } else {
                zoom_selected_span(state);
            }
        }
        KeyCode::Char('Z') => zoom_selected_span(state),
        // region panel: F filters its rows, , / . scroll its list
        KeyCode::Char('F') => state.region_filter_begin(),
        KeyCode::Char(',') => state.region_scroll_lines(false),
        KeyCode::Char('.') => state.region_scroll_lines(true),
        // group lanes by thread / scope / name
        KeyCode::Char('1') => state.set_group_mode(crate::state::GroupMode::Thread),
        KeyCode::Char('2') => state.set_group_mode(crate::state::GroupMode::Scope),
        KeyCode::Char('3') => state.set_group_mode(crate::state::GroupMode::Name),
        // collapse/expand the selected span's lane group (or click the gutter)
        KeyCode::Char('c') => state.toggle_collapse_selected(),
        // selection / vertical
        KeyCode::Char('j') | KeyCode::Down => state.select_step(true),
        KeyCode::Char('k') | KeyCode::Up => state.select_step(false),
        KeyCode::Char('g') => state.select_end(false),
        KeyCode::Char('G') => state.select_end(true),
        KeyCode::PageDown => {
            for _ in 0..PAGE_ROWS {
                state.scroll_lanes(true, view_rows);
            }
        }
        KeyCode::PageUp => {
            for _ in 0..PAGE_ROWS {
                state.scroll_lanes(false, view_rows);
            }
        }
        // horizontal zoom / pan (Perfetto WASD + the original bindings)
        KeyCode::Char('+') | KeyCode::Char('=') | KeyCode::Char('w') => state.zoom(0.5),
        KeyCode::Char('-') | KeyCode::Char('_') | KeyCode::Char('s') => state.zoom(2.0),
        KeyCode::Char('[') | KeyCode::Left | KeyCode::Char('h') | KeyCode::Char('a') => {
            state.pan(-0.25)
        }
        KeyCode::Char(']') | KeyCode::Right | KeyCode::Char('l') | KeyCode::Char('d') => {
            state.pan(0.25)
        }
        KeyCode::Char('f') => state.fit(),
        _ => {}
    }
    Action::Continue
}

/// Drag bookkeeping carried across mouse events. A left-drag draws a marquee
/// region (Perfetto's select-a-range), a plain left-click selects a span, and a
/// right-drag pans the timeline (grab-and-drag).
#[derive(Debug, Default)]
pub struct MouseDrag {
    /// Epoch-rel ns where a left-press began, for the marquee region.
    marquee_anchor: Option<u64>,
    /// Bar hit on left-press; selected on release if the press never dragged.
    press_hit: Option<(usize, usize)>,
    /// Caller/callee row hit in the detail pane on left-press; jumped on release.
    detail_jump: Option<usize>,
    /// Whether the left button moved since pressing (drag vs click).
    moved: bool,
    /// Whether the active left drag started on the minimap (scrub the viewport).
    minimap: bool,
    /// Whether the press toggled a gutter collapse header (consume the release).
    gutter: bool,
    /// Last column during a right-button pan drag.
    pan_col: Option<u16>,
}

/// Apply a mouse event to the viewer state against the last-rendered [`Layout`].
/// Pure (no terminal I/O) so gestures are unit-testable with synthetic events.
pub fn handle_mouse(
    state: &mut WaterfallState,
    layout: &Layout,
    ev: MouseEvent,
    drag: &mut MouseDrag,
) -> Action {
    // Help overlay is modal for the mouse too: swallow every event so a click,
    // drag, or wheel can't fall through to the (now hidden) waterfall geometry
    // captured in `layout` and silently mutate state under the overlay.
    if state.show_help {
        return Action::Continue;
    }
    let ctrl = ev.modifiers.contains(KeyModifiers::CONTROL);
    let shift = ev.modifiers.contains(KeyModifiers::SHIFT);
    match ev.kind {
        // Wheel: over the region panel ⇒ scroll its list; ctrl ⇒ zoom at cursor,
        // shift ⇒ horizontal pan, else vertical lane scroll.
        MouseEventKind::ScrollDown => {
            if layout.in_detail(ev.column) && state.region.is_some() {
                state.region_scroll_lines(true);
            } else if ctrl {
                if let Some(anchor) = layout.col_to_ns(ev.column) {
                    state.zoom_at(2.0, anchor);
                }
            } else if shift {
                state.pan(0.15);
            } else {
                state.scroll_lanes(true, 0);
            }
        }
        MouseEventKind::ScrollUp => {
            if layout.in_detail(ev.column) && state.region.is_some() {
                state.region_scroll_lines(false);
            } else if ctrl {
                if let Some(anchor) = layout.col_to_ns(ev.column) {
                    state.zoom_at(0.5, anchor);
                }
            } else if shift {
                state.pan(-0.15);
            } else {
                state.scroll_lanes(false, 0);
            }
        }
        MouseEventKind::ScrollLeft => state.pan(-0.15),
        MouseEventKind::ScrollRight => state.pan(0.15),
        // Left press: minimap → scrub; gutter header → collapse/expand; else arm
        // a marquee and remember the bar / detail-pane row hit.
        MouseEventKind::Down(MouseButton::Left) => {
            drag.minimap = false;
            drag.gutter = false;
            if let Some(ts) = layout.minimap_hit(ev.column, ev.row) {
                drag.minimap = true;
                state.focus_on_time(ts);
            } else if layout.minimap_readout_hit(ev.column, ev.row) {
                // Click the wall-clock readout row to toggle the clock (≡ `t`).
                state.toggle_clock();
            } else if let Some(btn) = layout.region_button_hit(ev.column, ev.row) {
                match btn {
                    RegionButton::Zoom => state.zoom_to_region(),
                    RegionButton::Filter => state.region_filter_begin(),
                }
            } else if layout.group_chip_hit(ev.column, ev.row) {
                // Click the legend chip to cycle grouping (≡ `1`/`2`/`3`).
                state.cycle_group_mode();
            } else if let Some(label) = layout.gutter_hit(ev.column, ev.row) {
                let label = label.to_string();
                state.toggle_collapse(&label);
                drag.gutter = true;
            } else {
                // Only arm a marquee for presses INSIDE the track (in_track has
                // the upper column bound `col_to_ns` lacks) — a press in the
                // detail pane must not start a region.
                drag.marquee_anchor = if layout.in_track(ev.column, ev.row) {
                    layout.col_to_ns(ev.column)
                } else {
                    None
                };
                drag.press_hit = layout.hit_span(ev.column, ev.row);
                drag.detail_jump = layout.detail_hit(ev.column, ev.row);
            }
            drag.moved = false;
        }
        // Left drag: scrub the minimap, else extend the marquee region.
        MouseEventKind::Drag(MouseButton::Left) => {
            if drag.minimap {
                if let Some(ts) = layout.minimap_col_to_ns(ev.column) {
                    state.focus_on_time(ts);
                }
            } else if let Some(anchor) = drag.marquee_anchor {
                // A region needs real width. Time is column-quantized, so a drag
                // that never leaves the press column collapses to a 0ns interval —
                // treat that as not-yet-a-region (Perfetto: a click is not a range).
                // Only arm `moved`/the region once the width is nonzero; collapse
                // it again if the drag returns to the start column.
                let cur = layout.col_to_ns(ev.column).unwrap_or(anchor);
                if cur != anchor {
                    drag.moved = true;
                    state.set_region(anchor, cur);
                } else if drag.moved {
                    state.clear_region();
                }
            }
        }
        // Left release (no drag): jump along a detail-pane caller/callee link,
        // else select the hit bar, else clear the region on an empty-track click.
        MouseEventKind::Up(MouseButton::Left) => {
            if !drag.moved && !drag.minimap && !drag.gutter {
                if let Some(idx) = drag.detail_jump {
                    state.jump_to_span_idx(idx);
                } else if let Some(hit) = drag.press_hit {
                    state.selected = Some(hit);
                    state.clear_region();
                } else if drag.marquee_anchor.is_some() {
                    state.clear_region();
                }
            }
            drag.marquee_anchor = None;
            drag.press_hit = None;
            drag.detail_jump = None;
            drag.minimap = false;
            drag.gutter = false;
            drag.moved = false;
        }
        // Right drag: grab-pan so the held point follows the cursor.
        MouseEventKind::Down(MouseButton::Right) => {
            drag.pan_col = layout.in_track(ev.column, ev.row).then_some(ev.column);
        }
        MouseEventKind::Drag(MouseButton::Right) => {
            if let (Some(prev), true) = (drag.pan_col, layout.track_w > 0) {
                if ev.column != prev {
                    let dcols = prev as f64 - ev.column as f64; // drag right ⇒ view left
                    state.pan(dcols / layout.track_w as f64);
                }
                drag.pan_col = Some(ev.column);
            }
        }
        MouseEventKind::Up(MouseButton::Right) => drag.pan_col = None,
        // Bare motion: track the bar under the cursor for the hover readout.
        MouseEventKind::Moved => state.hovered = layout.hit_span(ev.column, ev.row),
        _ => {}
    }
    Action::Continue
}

/// Build the viewer state for a set of input paths (default = the writer dir).
pub fn build_state(inputs: &[PathBuf]) -> Result<WaterfallState> {
    let loaded = load::load(inputs)?;
    let hint = load::empty_hint(&loaded.inputs);
    // Pre-fill the splash path box with the scanned directory so the empty state
    // shows where it looked and the user can edit it in place.
    let path_input = loaded
        .inputs
        .first()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    Ok(WaterfallState::new(loaded.trace)
        .with_empty_hint(hint)
        .with_path_input(path_input))
}

/// Enter raw mode + alternate screen, run the viewer, and ALWAYS restore the
/// terminal on exit (incl. panic, via the guard's Drop).
pub fn run(inputs: &[PathBuf]) -> Result<()> {
    let mut state = build_state(inputs)?;

    // Restore the terminal BEFORE a panic prints, so a crash leaves a readable
    // message on a clean screen instead of a corrupted alt-screen / silent freeze.
    install_panic_hook();
    let _guard = TerminalGuard::install()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;
    let mut layout = Layout::default();
    let mut drag = MouseDrag::default();

    // One-shot boot animation for the splash (skipped if we open straight into a
    // trace). Driven by a wall clock so it advances independent of input.
    let mut intro: Option<Effect> = state.is_empty().then(splash_intro);
    let mut last = Instant::now();

    loop {
        let now = Instant::now();
        let dt = now.duration_since(last);
        last = now;

        // Capture the painted geometry so the next mouse event hit-tests against
        // exactly what is on screen.
        terminal.draw(|frame| {
            let area = frame.area();
            layout = ui::render(frame, &state);
            // Apply the boot effect over the rendered splash (splash only).
            if state.is_empty() {
                if let Some(eff) = intro.as_mut() {
                    if !eff.done() {
                        eff.process(dt.into(), frame.buffer_mut(), area);
                    }
                }
            }
        })?;
        // The visible lane-row count for page scrolling (body height minus the
        // axis row, status line, and borders ≈ area - 4).
        let view_rows = terminal
            .size()
            .map(|s| (s.height as usize).saturating_sub(4))
            .unwrap_or(PAGE_ROWS);
        // Keep the row budget current for mouse-driven jumps (detail click, etc.).
        state.view_rows = view_rows;
        // Keep the region-panel page size current so its scroll clamps to a page.
        state.region_rows = layout.region_page;
        // Keep the track width current so zoom-to-span can size the window.
        state.track_w = layout.track_w;

        // While the intro plays, tick ~60fps so it animates without input; once it
        // finishes (or we're past the splash) block on events to stay idle.
        let animating = state.is_empty() && intro.as_ref().is_some_and(|e| !e.done());
        if animating && !event::poll(Duration::from_millis(16))? {
            continue; // no input this tick — loop to advance the animation
        }
        // Handle the first event, then DRAIN any others already queued before the
        // next redraw, so a burst of wheel/mouse events collapses into one frame.
        // Without this, scrolling to the region-panel bottom leaves a backlog of
        // no-op wheel events that each force a full redraw, so reversing direction
        // visibly lags while the queue drains.
        let mut quit = false;
        loop {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => {
                    match handle_key(&mut state, key, view_rows) {
                        Action::Quit => {
                            quit = true;
                            break;
                        }
                        // A reload swaps the whole state → relayout before more input.
                        Action::LoadPath(path) => {
                            state = open_path(state, &path);
                            break;
                        }
                        // Tab completion does a filesystem lookup (kept out of the
                        // pure key handler) and updates the input + candidate list.
                        Action::CompletePath => {
                            let (completed, candidates) = complete_trace_path(&state.path_input);
                            if let Some(c) = completed {
                                state.path_input = c;
                            }
                            state.path_completions = candidates;
                            state.load_error = None;
                        }
                        Action::Continue => {}
                    }
                }
                Event::Mouse(me) => {
                    if handle_mouse(&mut state, &layout, me, &mut drag) == Action::Quit {
                        quit = true;
                        break;
                    }
                }
                // Bracketed paste — append into the splash path box (pasting a path).
                Event::Paste(s) if state.is_empty() => state.path_input.push_str(s.trim()),
                _ => {}
            }
            // Stop draining once the input queue is empty; redraw with final state.
            if !event::poll(Duration::from_millis(0))? {
                break;
            }
        }
        if quit {
            break;
        }
    }
    Ok(())
}

/// The splash boot animation (tachyonfx): the composition coalesces into place
/// while a right-to-left sweep reveals it — echoing the vbot loading mark, whose
/// symbol resolves before the wordmark. Plays once, then settles.
fn splash_intro() -> Effect {
    fx::parallel(&[
        fx::coalesce(EffectTimer::from_ms(820, Interpolation::SineOut)),
        fx::sweep_in(
            Motion::RightToLeft,
            16,
            8,
            ratatui::style::Color::Rgb(0x12, 0x10, 0x0f),
            EffectTimer::from_ms(1100, Interpolation::SineOut),
        ),
    ])
}

/// Re-run discovery against `path`. On success swap in the loaded state; on an
/// empty result or error, keep the splash screen with an error under the input.
fn open_path(prev: WaterfallState, path: &std::path::Path) -> WaterfallState {
    match build_state(std::slice::from_ref(&path.to_path_buf())) {
        Ok(s) if !s.is_empty() => s,
        Ok(mut s) => {
            s.load_error = Some(format!("no .atrc trace files found in {}", path.display()));
            s
        }
        Err(e) => {
            let mut s = prev;
            s.load_error = Some(format!("{e}"));
            s
        }
    }
}

/// Zoom onto the currently-selected span: size the window to the span's rendered
/// label width (capped at filling the track) and centre it. No-op without a
/// selection. The label width comes from the renderer so "fits the text" matches
/// exactly what the bar draws.
fn zoom_selected_span(state: &mut WaterfallState) {
    let cols = state.selected.and_then(|(li, pos)| {
        let span = state.span_at((li, pos))?;
        let depth = state.lanes.get(li)?.spans.get(pos)?.depth;
        Some(ui::bar_label_width(&span.name, span.duration_ns(), depth, span.is_async) as u16)
    });
    if let Some(cols) = cols {
        state.zoom_to_selected_span(cols);
    }
}

/// Tab-complete the splash path input against the filesystem: list the directory
/// of the current input and complete its trailing component to the single
/// matching sub-directory, else the longest common prefix of all matches.
/// Directory-only (a trace location is a directory), follows symlinks, hides
/// dotfiles unless explicitly typed, and a unique match gets a trailing `/`.
/// Returns `(new_input, candidates)` — `candidates` lists the matches to show
/// when more than one survived. Kept out of `handle_key` so that stays I/O-free.
fn complete_trace_path(input: &str) -> (Option<String>, Vec<String>) {
    let expanded = expand_tilde(input);
    let s = expanded.to_string_lossy().into_owned();
    let Some(slash) = s.rfind('/') else {
        return (None, Vec::new()); // need an anchoring '/' (an absolute or ~/ path)
    };
    let dir = &s[..=slash];
    let prefix = &s[slash + 1..];
    let Ok(read) = std::fs::read_dir(dir) else {
        return (None, Vec::new());
    };
    let mut names: Vec<String> = read
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir()) // follows symlinks; trace locations are dirs
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| prefix.starts_with('.') || !n.starts_with('.'))
        .filter(|n| n.starts_with(prefix))
        .collect();
    names.sort();
    match names.as_slice() {
        [] => (None, Vec::new()),
        [only] => (Some(format!("{dir}{only}/")), Vec::new()),
        _ => {
            let lcp = longest_common_prefix(&names);
            let completed = (lcp.len() > prefix.len()).then(|| format!("{dir}{lcp}"));
            (completed, names)
        }
    }
}

/// Longest common prefix of a sorted, non-empty list (char-boundary safe).
fn longest_common_prefix(sorted: &[String]) -> String {
    let (Some(first), Some(last)) = (sorted.first(), sorted.last()) else {
        return String::new();
    };
    let n = first
        .chars()
        .zip(last.chars())
        .take_while(|(a, b)| a == b)
        .count();
    first.chars().take(n).collect()
}

/// Install a panic hook that restores the terminal (raw mode off, leave the
/// alt-screen, disable mouse/paste) before the default hook prints the panic —
/// otherwise a panic mid-render leaves the message swallowed by the alt-screen.
fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(
            io::stdout(),
            DisableBracketedPaste,
            DisableMouseCapture,
            LeaveAlternateScreen
        );
        default(info);
    }));
}

/// Restores the terminal on drop, so a panic mid-render still leaves a usable
/// shell (mirrors `aorta-tui`'s guard).
struct TerminalGuard;

impl TerminalGuard {
    fn install() -> Result<TerminalGuard> {
        enable_raw_mode()?;
        execute!(
            io::stdout(),
            EnterAlternateScreen,
            EnableMouseCapture,
            EnableBracketedPaste
        )?;
        Ok(TerminalGuard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut stdout: Stdout = io::stdout();
        let _ = execute!(
            stdout,
            DisableBracketedPaste,
            DisableMouseCapture,
            LeaveAlternateScreen
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aorta_core::trace::chunk::{build_chunk, ChunkEventType, ChunkProvenance, RingRecord};
    use aorta_trace2perfetto::decode_trace;
    use crossterm::event::KeyModifiers;
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;
    use ratatui::Terminal;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn prov() -> ChunkProvenance {
        ChunkProvenance {
            node_id: "robotA".into(),
            run_id: "run".into(),
            pid: 42,
            process_name: "demo".into(),
            host_clock_domain_id: 1,
            seq: 0,
            dropped: 0,
        }
    }

    fn rec(
        scope: u16,
        event: u16,
        thread: u16,
        span_id: u64,
        parent: u64,
        mono: u64,
        ty: ChunkEventType,
        async_flag: bool,
    ) -> RingRecord {
        RingRecord {
            trace_id: [9u8; 16],
            span_id,
            parent_span_id: parent,
            mono_ns: mono,
            scope_id: scope,
            event_id: event,
            thread_id: thread,
            event_type: ty as u8,
            flags: if async_flag {
                aorta_core::trace::chunk::FLAG_ASYNC
            } else {
                0
            },
            ..RingRecord::default()
        }
    }

    fn fixed_state() -> WaterfallState {
        // A deterministic mini-trace: a root span with a nested child, plus an
        // async wait, all on thread0, and one span on thread1.
        let names = [
            "topic.publish",
            "topic.callback",
            "sched.queue_wait",
            "service.reply",
        ];
        let mut async_b = rec(1, 2, 0, 9, 0, 1_500, ChunkEventType::SliceBegin, true);
        async_b.flags = aorta_core::trace::chunk::FLAG_ASYNC;
        let recs = vec![
            rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin, false),
            rec(0, 1, 0, 2, 1, 2_000, ChunkEventType::SliceBegin, false),
            rec(0, 1, 0, 2, 1, 3_000, ChunkEventType::SliceEnd, false),
            async_b,
            rec(1, 2, 0, 9, 0, 2_500, ChunkEventType::SliceEnd, true),
            rec(0, 0, 0, 1, 0, 5_000, ChunkEventType::SliceEnd, false),
            rec(2, 3, 1, 3, 0, 4_000, ChunkEventType::SliceBegin, false),
            rec(2, 3, 1, 3, 0, 6_000, ChunkEventType::SliceEnd, false),
        ];
        let chunk = build_chunk(
            &prov(),
            1_000,
            1_700_000_000_000_000_000,
            &recs,
            |s| ["aorta.topic", "aorta.sched", "aorta.service"][s as usize].to_string(),
            |e| names[e as usize].to_string(),
            |t| (100 + t as u32, format!("worker-{t}")),
        );
        let decoded = decode_trace(&[chunk]).unwrap();
        WaterfallState::new(decoded).with_empty_hint("no trace files found in: /tmp/atrc")
    }

    /// Like [`fixed_state`] but with counter samples + instant events, so the
    /// always-on bottom tracks (sparklines + the `◆` instants row) are exercised
    /// — the data shape the small fixture lacks.
    fn fixed_state_rich() -> WaterfallState {
        let names = [
            "topic.publish",
            "topic.callback",
            "sched.queue_wait",
            "service.reply",
            "grp_waiters",
            "prio_inv",
        ];
        let mut recs = vec![
            rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin, false),
            rec(0, 1, 0, 2, 1, 2_000, ChunkEventType::SliceBegin, false),
            rec(0, 1, 0, 2, 1, 3_000, ChunkEventType::SliceEnd, false),
            rec(0, 0, 0, 1, 0, 5_000, ChunkEventType::SliceEnd, false),
            rec(2, 3, 1, 3, 0, 4_000, ChunkEventType::SliceBegin, false),
            rec(2, 3, 1, 3, 0, 6_000, ChunkEventType::SliceEnd, false),
        ];
        // Two counter series (event 4 on threads 0 and 1) → two sparkline rows.
        for (t, v, thr) in [
            (1_200u64, 1u64, 0u16),
            (2_200, 3, 0),
            (3_200, 2, 0),
            (1_500, 2, 1),
            (4_500, 4, 1),
        ] {
            let mut r = rec(1, 4, thr, 0, 0, t, ChunkEventType::Counter, false);
            r.value = v;
            recs.push(r);
        }
        // Instant events (event 5) → the `◆` instants row.
        for t in [2_500u64, 3_500, 4_800] {
            recs.push(rec(1, 5, 0, 0, 0, t, ChunkEventType::Instant, false));
        }
        let chunk = build_chunk(
            &prov(),
            1_000,
            1_700_000_000_000_000_000,
            &recs,
            |s| ["aorta.topic", "aorta.sched", "aorta.service"][s as usize].to_string(),
            |e| names[e as usize].to_string(),
            |t| (100 + t as u32, format!("worker-{t}")),
        );
        let decoded = decode_trace(&[chunk]).unwrap();
        WaterfallState::new(decoded).with_empty_hint("rich")
    }

    fn buffer_text(buffer: &Buffer) -> String {
        let mut out = String::new();
        for row in buffer.content.chunks(buffer.area.width as usize) {
            for cell in row {
                out.push_str(cell.symbol());
            }
            out.push('\n');
        }
        out
    }

    fn render_to_text(state: &WaterfallState, w: u16, h: u16) -> String {
        let backend = TestBackend::new(w, h);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| {
                ui::render(frame, state);
            })
            .expect("render");
        buffer_text(terminal.backend().buffer())
    }

    /// Render `state` to a test backend and return the captured [`Layout`] so
    /// mouse tests can hit-test against the real painted geometry.
    fn render_layout(state: &WaterfallState, w: u16, h: u16) -> Layout {
        let backend = TestBackend::new(w, h);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        let mut layout = Layout::default();
        terminal
            .draw(|frame| layout = ui::render(frame, state))
            .expect("render");
        layout
    }

    fn mouse(kind: MouseEventKind, col: u16, row: u16, mods: KeyModifiers) -> MouseEvent {
        MouseEvent {
            kind,
            column: col,
            row,
            modifiers: mods,
        }
    }

    #[test]
    fn waterfall_snapshot_renders_lanes_axis_and_detail() {
        let state = fixed_state();
        let view = render_to_text(&state, 100, 18);
        // Process header label present.
        assert!(
            view.contains("demo @ robotA"),
            "process label in gutter:\n{view}"
        );
        // A worker thread lane present.
        assert!(view.contains("worker-0"), "thread lane label:\n{view}");
        // The async lane is segregated and labeled.
        assert!(
            view.contains("[async]"),
            "async lane labeled distinctly:\n{view}"
        );
        // Span names render on the bars / detail pane.
        assert!(view.contains("topic.publish"), "a span bar name:\n{view}");
        // The detail pane is open (default selection) and shows its fields.
        assert!(view.contains("span detail"), "detail pane open:\n{view}");
        assert!(view.contains("duration"), "detail shows duration:\n{view}");
        // The help line lists the keybindings.
        assert!(view.contains("zoom"), "status/help line:\n{view}");
    }

    #[test]
    fn empty_state_splash_names_the_directory_and_offers_input() {
        let empty = WaterfallState::new(Default::default())
            .with_empty_hint("no trace files found in: /tmp/atrc");
        let view = render_to_text(&empty, 100, 32);
        assert!(view.contains("/tmp/atrc"), "names the scanned dir:\n{view}");
        assert!(
            view.contains("AORTA_TRACE_DIR"),
            "names the env var:\n{view}"
        );
        assert!(
            view.contains("open trace directory"),
            "shows the path input box:\n{view}"
        );
        assert!(
            view.contains('█') || view.contains('▀'),
            "renders the vbot logo (half-block art):\n{view}"
        );
        assert!(view.contains("A O R T A"), "shows the app title:\n{view}");
    }

    #[test]
    fn group_switch_with_region_and_scroll_never_panics() {
        use crate::state::GroupMode;
        for build in [fixed_state as fn() -> WaterfallState, fixed_state_rich] {
            for h in 5..42u16 {
                for w in [38u16, 60, 80, 120] {
                    let mut s = build();
                    s.set_region(s.total_start_ns, s.total_end_ns);
                    s.region_rows = 3;
                    for _ in 0..30 {
                        s.region_scroll_lines(true);
                    }
                    let _ = render_to_text(&s, w, h);
                    for m in [GroupMode::Scope, GroupMode::Name, GroupMode::Thread] {
                        s.set_group_mode(m);
                        for _ in 0..30 {
                            s.region_scroll_lines(true);
                        }
                        for _ in 0..50 {
                            s.scroll_lanes(true, h as usize);
                        }
                        let _ = render_to_text(&s, w, h);
                    }
                }
            }
        }
    }

    #[test]
    fn fuzz_random_ops_never_panic() {
        use crate::state::GroupMode;
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut rng = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut s = fixed_state_rich();
        let ext = s.total_end_ns - s.total_start_ns;
        for _ in 0..6000 {
            match rng() % 14 {
                0 => {
                    let a = s.total_start_ns + rng() % ext.max(1);
                    let b = s.total_start_ns + rng() % ext.max(1);
                    s.set_region(a, b);
                }
                1 => s.clear_region(),
                2 => s.region_scroll_lines(rng() % 2 == 0),
                3 => s.scroll_lanes(rng() % 2 == 0, (rng() % 40) as usize),
                4 => s.set_group_mode(
                    [GroupMode::Thread, GroupMode::Scope, GroupMode::Name][(rng() % 3) as usize],
                ),
                5 => s.select_step(rng() % 2 == 0),
                6 => s.select_end(rng() % 2 == 0),
                7 => s.toggle_collapse_selected(),
                8 => s.zoom(if rng() % 2 == 0 { 0.5 } else { 2.0 }),
                9 => s.jump_to_caller(),
                10 => s.jump_to_first_callee(),
                11 => s.region_rows = (rng() % 8) as usize,
                12 => s.toggle_help(),
                _ => {
                    let w = 16 + (rng() % 120) as u16;
                    let h = 3 + (rng() % 48) as u16;
                    let _ = render_to_text(&s, w, h);
                }
            }
        }
    }

    #[test]
    fn splash_intro_effect_advances_and_completes() {
        use ratatui::buffer::Buffer;
        use ratatui::layout::Rect;
        let mut eff = splash_intro();
        let area = Rect::new(0, 0, 60, 16);
        let mut buf = Buffer::empty(area);
        assert!(!eff.done(), "effect starts not-done");
        // Advance in 16ms ticks; it must take several frames (not instant) and finish.
        let mut steps = 0;
        while !eff.done() && steps < 300 {
            eff.process(Duration::from_millis(16).into(), &mut buf, area);
            steps += 1;
        }
        assert!(steps > 5, "intro animates over many frames, not instantly");
        assert!(
            eff.done(),
            "intro completes within ~1.5s of ticks (got {steps})"
        );
    }

    #[test]
    fn empty_screen_edits_path_then_enter_loads_it() {
        let mut state = WaterfallState::new(Default::default());
        assert!(state.is_empty());
        for c in "/tmp/xyz".chars() {
            assert_eq!(
                handle_key(&mut state, key(KeyCode::Char(c)), 10),
                Action::Continue
            );
        }
        assert_eq!(state.path_input, "/tmp/xyz");
        handle_key(&mut state, key(KeyCode::Backspace), 10);
        assert_eq!(state.path_input, "/tmp/xy", "Backspace edits the path");
        for c in "z2".chars() {
            handle_key(&mut state, key(KeyCode::Char(c)), 10);
        }
        assert_eq!(
            handle_key(&mut state, key(KeyCode::Enter), 10),
            Action::LoadPath(std::path::PathBuf::from("/tmp/xyz2")),
            "Enter opens the typed path"
        );
        // Esc clears the still-typed path first (Continue), then quits when empty.
        assert_eq!(
            handle_key(&mut state, key(KeyCode::Esc), 10),
            Action::Continue
        );
        assert!(state.path_input.is_empty(), "first Esc clears the path");
        assert_eq!(handle_key(&mut state, key(KeyCode::Esc), 10), Action::Quit);
    }

    #[test]
    fn empty_screen_tab_requests_completion() {
        let mut state = WaterfallState::new(Default::default());
        assert_eq!(
            handle_key(&mut state, key(KeyCode::Tab), 10),
            Action::CompletePath
        );
    }

    #[test]
    fn empty_screen_renders_completion_candidates_and_tab_hint() {
        let mut state = WaterfallState::new(Default::default());
        state.path_completions = vec!["alpha".to_string(), "beta".to_string()];
        let view = render_to_text(&state, 100, 26);
        assert!(view.contains("alpha/"), "lists candidate alpha/:\n{view}");
        assert!(view.contains("beta/"), "lists candidate beta/");
        assert!(view.contains("Tab complete"), "footer advertises Tab");
    }

    #[test]
    fn complete_trace_path_unique_lcp_and_none() {
        use std::fs;
        let base = std::env::temp_dir().join(format!("aorta-tui-complete-{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        for d in ["alpha", "alpine", "beta"] {
            fs::create_dir_all(base.join(d)).unwrap();
        }
        let p = base.to_string_lossy().into_owned();
        // Unique prefix completes to the dir with a trailing slash, no candidates.
        let (c, cand) = complete_trace_path(&format!("{p}/be"));
        assert_eq!(c.as_deref(), Some(format!("{p}/beta/").as_str()));
        assert!(cand.is_empty());
        // Ambiguous prefix completes to the longest common prefix + lists matches.
        let (c, cand) = complete_trace_path(&format!("{p}/al"));
        assert_eq!(c.as_deref(), Some(format!("{p}/alp").as_str()));
        assert_eq!(cand, vec!["alpha".to_string(), "alpine".to_string()]);
        // No match → no change, no candidates.
        let (c, cand) = complete_trace_path(&format!("{p}/zzz"));
        assert!(c.is_none() && cand.is_empty());
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn expand_tilde_resolves_home() {
        assert_eq!(
            expand_tilde("/abs/path"),
            std::path::PathBuf::from("/abs/path")
        );
        if let Some(home) = std::env::var_os("HOME") {
            assert_eq!(
                expand_tilde("~/traces"),
                std::path::PathBuf::from(&home).join("traces")
            );
        }
    }

    #[test]
    fn open_path_on_a_missing_dir_keeps_the_splash_with_an_error() {
        let empty = WaterfallState::new(Default::default());
        let result = open_path(empty, std::path::Path::new("/no/such/trace/dir/xyzzy"));
        assert!(result.is_empty(), "a missing dir stays on the splash");
        assert!(
            result
                .load_error
                .as_deref()
                .is_some_and(|e| e.contains("no .atrc")),
            "shows a not-found error: {:?}",
            result.load_error
        );
    }

    #[test]
    fn empty_screen_renders_a_load_error() {
        let mut state = WaterfallState::new(Default::default());
        state.load_error = Some("no .atrc trace files found in /tmp/nope".to_string());
        let view = render_to_text(&state, 100, 32);
        assert!(
            view.contains("/tmp/nope") && view.contains('⚠'),
            "renders the load error:\n{view}"
        );
    }

    #[test]
    fn keys_zoom_pan_select_and_quit() {
        let mut state = fixed_state();
        let full = state.view_span_ns();
        assert_eq!(
            handle_key(&mut state, key(KeyCode::Char('+')), 10),
            Action::Continue
        );
        assert!(state.view_span_ns() < full, "+ zooms in");
        handle_key(&mut state, key(KeyCode::Char('f')), 10);
        assert_eq!(state.view_span_ns(), full, "f fits");

        // selection traversal
        handle_key(&mut state, key(KeyCode::Char('g')), 10);
        let first = state.selected_span().unwrap().span_id;
        handle_key(&mut state, key(KeyCode::Char('G')), 10);
        let last = state.selected_span().unwrap().span_id;
        assert_ne!(first, last, "g/G move selection");

        // quit keys
        assert_eq!(
            handle_key(&mut state, key(KeyCode::Char('q')), 10),
            Action::Quit
        );
        assert_eq!(handle_key(&mut state, key(KeyCode::Esc), 10), Action::Quit);
        let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        assert_eq!(handle_key(&mut state, ctrl_c, 10), Action::Quit);
    }

    #[test]
    fn wasd_keys_zoom_and_pan() {
        let mut state = fixed_state();
        let full = state.view_span_ns();
        handle_key(&mut state, key(KeyCode::Char('w')), 10);
        assert!(state.view_span_ns() < full, "w zooms in");
        handle_key(&mut state, key(KeyCode::Char('s')), 10);
        assert_eq!(state.view_span_ns(), full, "s zooms back out to full");
        // Zoom in so there is room on both sides, then a/d move the window.
        state.zoom(0.25);
        let s0 = state.view_start_ns;
        handle_key(&mut state, key(KeyCode::Char('d')), 10);
        let s1 = state.view_start_ns;
        assert!(s1 > s0, "d pans right");
        handle_key(&mut state, key(KeyCode::Char('a')), 10);
        assert!(state.view_start_ns < s1, "a pans left");
    }

    #[test]
    fn wheel_scrolls_vertically_and_ctrl_wheel_zooms_at_cursor() {
        let mut state = fixed_state();
        let layout = render_layout(&state, 100, 18);
        let mut drag = MouseDrag::default();
        let full = state.view_span_ns();

        // Ctrl+wheel-up over a column zooms in (window shrinks).
        let col = layout.track_x + 5;
        handle_mouse(
            &mut state,
            &layout,
            mouse(
                MouseEventKind::ScrollUp,
                col,
                layout.track_y0,
                KeyModifiers::CONTROL,
            ),
            &mut drag,
        );
        assert!(
            state.view_span_ns() < full,
            "ctrl+wheel-up zooms in at cursor"
        );

        // Plain wheel-down scrolls lanes (v_scroll moves) when there is room.
        // (Single-process fixture has ≥2 lanes, so scrolling down is a no-op-safe
        // clamp; assert it never panics and stays within bounds.)
        let before = state.v_scroll;
        handle_mouse(
            &mut state,
            &layout,
            mouse(
                MouseEventKind::ScrollDown,
                col,
                layout.track_y0,
                KeyModifiers::empty(),
            ),
            &mut drag,
        );
        assert!(
            state.v_scroll >= before,
            "wheel-down scrolls lanes down (clamped)"
        );
    }

    #[test]
    fn left_click_selects_the_bar_under_the_cursor() {
        let mut state = fixed_state();
        let layout = render_layout(&state, 100, 18);
        let mut drag = MouseDrag::default();
        // Pick a real painted bar; a click is press THEN release with no move.
        let bar = *layout.bars.first().expect("at least one bar painted");
        let empty = KeyModifiers::empty();
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Down(MouseButton::Left), bar.x, bar.y, empty),
            &mut drag,
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Up(MouseButton::Left), bar.x, bar.y, empty),
            &mut drag,
        );
        assert_eq!(
            state.selected,
            Some((bar.lane_idx, bar.pos)),
            "clicking a bar selects its lane slot"
        );
    }

    #[test]
    fn left_drag_sets_a_marquee_region_for_aggregation() {
        let mut state = fixed_state();
        let layout = render_layout(&state, 100, 18);
        let mut drag = MouseDrag::default();
        let empty = KeyModifiers::empty();
        let row = layout.track_y1 - 1;
        let x0 = layout.track_x + 10;
        let x1 = layout.track_x + layout.track_w - 10;
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Down(MouseButton::Left), x0, row, empty),
            &mut drag,
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Drag(MouseButton::Left), x1, row, empty),
            &mut drag,
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Up(MouseButton::Left), x1, row, empty),
            &mut drag,
        );
        let agg = state
            .region_aggregate()
            .expect("dragging set a region with an aggregate");
        assert!(agg.count >= 1, "the region rolls up some spans");
        assert!(agg.end_ns > agg.start_ns, "region has width");
    }

    #[test]
    fn same_column_drag_never_creates_a_zero_width_region() {
        let mut state = fixed_state();
        let layout = render_layout(&state, 100, 18);
        let mut drag = MouseDrag::default();
        let empty = KeyModifiers::empty();
        // Press on a painted bar, "drag" without leaving its column, release. Time
        // is column-quantized, so this would map to a 0ns interval — it must NOT
        // become a region; instead the press is treated as a click that selects.
        let bar = *layout.bars.first().expect("at least one bar painted");
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Down(MouseButton::Left), bar.x, bar.y, empty),
            &mut drag,
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Drag(MouseButton::Left), bar.x, bar.y, empty),
            &mut drag,
        );
        assert!(
            state.region.is_none(),
            "a same-column drag must not arm a 0ns-width region"
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Up(MouseButton::Left), bar.x, bar.y, empty),
            &mut drag,
        );
        assert!(
            state.region.is_none(),
            "releasing a same-column drag leaves no region"
        );
        assert_eq!(
            state.selected,
            Some((bar.lane_idx, bar.pos)),
            "the zero-width drag falls back to a click and selects the bar"
        );
    }

    #[test]
    fn right_drag_pans_the_viewport() {
        let mut state = fixed_state();
        state.zoom(0.25); // zoom in so there is room to pan both ways
        let layout = render_layout(&state, 100, 18);
        let mut drag = MouseDrag::default();
        let empty = KeyModifiers::empty();
        let row = layout.track_y1 - 1;
        let mid = layout.track_x + layout.track_w / 2;
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Down(MouseButton::Right), mid, row, empty),
            &mut drag,
        );
        let start_before = state.view_start_ns;
        handle_mouse(
            &mut state,
            &layout,
            mouse(
                MouseEventKind::Drag(MouseButton::Right),
                layout.track_x,
                row,
                empty,
            ),
            &mut drag,
        );
        assert!(
            state.view_start_ns >= start_before,
            "right-dragging left pans the window toward later time"
        );
    }

    #[test]
    fn search_keys_enter_input_match_and_clear() {
        let mut state = fixed_state();
        handle_key(&mut state, key(KeyCode::Char('/')), 10);
        assert!(state.search_input.is_some(), "/ enters search input");
        for c in "topic".chars() {
            handle_key(&mut state, key(KeyCode::Char(c)), 10);
        }
        handle_key(&mut state, key(KeyCode::Enter), 10);
        assert!(state.search_input.is_none(), "Enter commits");
        assert!(
            !state.search_matches.is_empty(),
            "'topic' matches the demo span names"
        );
        // Esc clears a live search (does not quit); Esc when idle quits.
        assert_eq!(
            handle_key(&mut state, key(KeyCode::Esc), 10),
            Action::Continue
        );
        assert!(state.search_query.is_none());
        assert_eq!(handle_key(&mut state, key(KeyCode::Esc), 10), Action::Quit);
    }

    #[test]
    fn search_picker_lists_distinct_names_filtered_by_name() {
        let mut state = fixed_state();
        handle_key(&mut state, key(KeyCode::Char('/')), 10);
        // The name list re-filters as you type — before any Enter.
        for c in "topic".chars() {
            handle_key(&mut state, key(KeyCode::Char(c)), 10);
        }
        assert!(state.search_input.is_some(), "picker still open");
        assert!(state.search_query.is_none(), "not committed yet");
        // Two DISTINCT names match 'topic' (not the many instances), sorted.
        assert_eq!(
            state.search_names.len(),
            2,
            "topic.callback + topic.publish"
        );
        assert_eq!(
            state.search_name_row(0).map(|r| r.0),
            Some("topic.callback")
        );
        assert_eq!(state.search_name_row(1).map(|r| r.0), Some("topic.publish"));
        // The top candidate's instances preview (its name is highlighted).
        assert_eq!(
            state.selected_span().map(|s| s.name.as_str()),
            Some("topic.callback"),
        );
    }

    #[test]
    fn opening_the_picker_lists_names_without_moving_or_highlighting() {
        let mut state = fixed_state();
        let before_view = state.view_start_ns;
        let before_sel = state.selected;
        handle_key(&mut state, key(KeyCode::Char('/')), 10);
        assert!(state.search_input.is_some(), "picker open");
        assert!(!state.search_names.is_empty(), "lists every span name");
        assert!(
            state.search_matches.is_empty(),
            "empty query highlights nothing yet"
        );
        assert_eq!(
            state.view_start_ns, before_view,
            "'/' must not pan the view"
        );
        assert_eq!(
            state.selected, before_sel,
            "'/' must not move the selection"
        );
    }

    #[test]
    fn search_picker_arrows_pick_a_name_then_enter_highlights_it() {
        let mut state = fixed_state();
        handle_key(&mut state, key(KeyCode::Char('/')), 10);
        for c in "topic".chars() {
            handle_key(&mut state, key(KeyCode::Char(c)), 10);
        }
        // Down moves over the NAME list (callback → publish), with live preview.
        handle_key(&mut state, key(KeyCode::Down), 10);
        assert_eq!(state.search_name_idx, 1);
        assert_eq!(
            state.selected_span().map(|s| s.name.as_str()),
            Some("topic.publish"),
            "arrow previews the highlighted name"
        );
        // Enter confirms the NAME (not the typed 'topic') as the active highlight.
        handle_key(&mut state, key(KeyCode::Enter), 10);
        assert!(state.search_input.is_none(), "Enter closes the picker");
        assert_eq!(state.search_query.as_deref(), Some("topic.publish"));
        assert_eq!(
            state.selected_span().map(|s| s.name.as_str()),
            Some("topic.publish"),
        );
    }

    #[test]
    fn search_picker_overlay_renders_query_and_name_rows() {
        let mut state = fixed_state();
        handle_key(&mut state, key(KeyCode::Char('/')), 10);
        for c in "topic".chars() {
            handle_key(&mut state, key(KeyCode::Char(c)), 10);
        }
        let text = render_to_text(&state, 120, 28);
        assert!(text.contains("/topic"), "query line shows in the overlay");
        assert!(text.contains("2 names"), "distinct-name count shows");
        assert!(text.contains("topic.publish"), "name rows are listed");
        assert!(text.contains("topic.callback"), "name rows are listed");
        assert!(
            text.contains("Enter highlight"),
            "the picker keybinding hint shows"
        );
    }

    #[test]
    fn committed_search_highlights_only_the_picked_name() {
        use ratatui::style::Color;
        let mut state = fixed_state();
        // Type 'topic' (matches both topic names), then commit the top candidate.
        handle_key(&mut state, key(KeyCode::Char('/')), 10);
        for c in "topic".chars() {
            handle_key(&mut state, key(KeyCode::Char(c)), 10);
        }
        handle_key(&mut state, key(KeyCode::Enter), 10);
        assert_eq!(
            state.search_query.as_deref(),
            Some("topic.callback"),
            "the committed highlight is the picked NAME, not 'topic'"
        );

        let backend = TestBackend::new(120, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        let mut layout = Layout::default();
        terminal
            .draw(|f| layout = ui::render(f, &state))
            .expect("render");
        let buf = terminal.backend().buffer();

        // The picked-name span is the brand-orange cursor.
        let sel = state
            .selected
            .expect("the picked name's instance is selected");
        let cursor = layout
            .bars
            .iter()
            .find(|b| (b.lane_idx, b.pos) == sel)
            .expect("the cursor span is painted");
        assert_eq!(
            buf.content[buf.index_of(cursor.x, cursor.y)].bg,
            ui::CURSOR_BG,
            "the picked name's current instance is the orange cursor"
        );

        // topic.publish ALSO fuzzy-matches 'topic' but is a DIFFERENT name, so it
        // is NOT a hit — it keeps its NORMAL scope color (a search never recolors
        // non-matches; the bug was painting them bright/selected).
        let publish = layout
            .bars
            .iter()
            .find(|b| {
                state
                    .span_at((b.lane_idx, b.pos))
                    .is_some_and(|s| s.name == "topic.publish")
            })
            .expect("topic.publish is painted");
        assert!(
            !state.is_search_match((publish.lane_idx, publish.pos)),
            "a different fuzzy-matching name is not a hit"
        );
        assert_eq!(
            buf.content[buf.index_of(publish.x, publish.y)].bg,
            Color::LightBlue,
            "a non-match keeps its scope color, unchanged by the search"
        );
    }

    /// Two non-overlapping spans that share one name on one thread — so a search
    /// has a match that ISN'T the cursor (to exercise the muted match tint).
    fn two_same_name_state() -> WaterfallState {
        let recs = vec![
            rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin, false),
            rec(0, 0, 0, 1, 0, 2_000, ChunkEventType::SliceEnd, false),
            rec(0, 0, 0, 2, 0, 3_000, ChunkEventType::SliceBegin, false),
            rec(0, 0, 0, 2, 0, 4_000, ChunkEventType::SliceEnd, false),
        ];
        let chunk = build_chunk(
            &prov(),
            1_000,
            1_700_000_000_000_000_000,
            &recs,
            |_| "aorta.topic".to_string(),
            |_| "topic.callback".to_string(),
            |t| (100 + t as u32, format!("worker-{t}")),
        );
        WaterfallState::new(decode_trace(&[chunk]).unwrap())
    }

    #[test]
    fn a_non_cursor_search_hit_pops_in_white_the_cursor_in_orange() {
        use ratatui::style::Color;
        let mut state = two_same_name_state();
        state.search_begin();
        for c in "topic".chars() {
            state.search_input_push(c);
        }
        state.search_commit();
        assert_eq!(
            state.search_matches.len(),
            2,
            "two instances of the one name"
        );

        let backend = TestBackend::new(120, 16);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        let mut layout = Layout::default();
        terminal
            .draw(|f| layout = ui::render(f, &state))
            .expect("render");
        let buf = terminal.backend().buffer();

        let sel = state.selected.expect("cursor set after commit");
        // The cursor (current hit) pops in brand orange.
        let cursor = layout
            .bars
            .iter()
            .find(|b| (b.lane_idx, b.pos) == sel)
            .expect("cursor bar painted");
        assert_eq!(
            buf.content[buf.index_of(cursor.x, cursor.y)].bg,
            ui::CURSOR_BG,
            "the current hit is the orange cursor"
        );
        // The OTHER hit pops in striking white (not the scope color, not dimmed).
        let other = layout
            .bars
            .iter()
            .find(|b| (b.lane_idx, b.pos) != sel && state.is_search_match((b.lane_idx, b.pos)))
            .expect("a non-cursor match bar is painted");
        assert_eq!(
            buf.content[buf.index_of(other.x, other.y)].bg,
            ui::MATCH_BG,
            "a non-cursor hit pops in the striking white"
        );
        assert_ne!(
            ui::MATCH_BG,
            Color::LightBlue,
            "hits are not the scope color"
        );
    }

    #[test]
    fn minimap_click_recenters_the_viewport() {
        let mut state = fixed_state();
        state.zoom(0.25); // narrow viewport so the recenter is observable
        let layout = render_layout(&state, 120, 20);
        assert!(layout.minimap_w > 0, "minimap rendered");
        let mut drag = MouseDrag::default();
        let before = state.view_start_ns;
        let col = layout.minimap_x0 + layout.minimap_w - 2; // near the right end
        handle_mouse(
            &mut state,
            &layout,
            mouse(
                MouseEventKind::Down(MouseButton::Left),
                col,
                layout.minimap_y,
                KeyModifiers::empty(),
            ),
            &mut drag,
        );
        assert!(
            state.view_start_ns > before,
            "clicking the right of the minimap moves the viewport later"
        );
    }

    #[test]
    fn clicking_or_dragging_the_detail_pane_never_starts_a_region() {
        let mut state = fixed_state();
        let layout = render_layout(&state, 120, 24);
        assert!(layout.detail_x0 > 0, "detail pane is shown");
        let empty = KeyModifiers::empty();
        let mut drag = MouseDrag::default();
        // A press + small drag + release fully inside the detail pane (right of
        // the track) must NOT arm a marquee / set a region (the yellow guide bug).
        let col = layout.detail_x0 + 3;
        let row = layout.track_y0 + 2;
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Down(MouseButton::Left), col, row, empty),
            &mut drag,
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Drag(MouseButton::Left), col + 1, row, empty),
            &mut drag,
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Up(MouseButton::Left), col + 1, row, empty),
            &mut drag,
        );
        assert!(
            state.region.is_none(),
            "detail-pane interaction must not create a region"
        );
    }

    #[test]
    fn collapsed_header_survives_scroll_into_the_group_middle() {
        let mut state = fixed_state();
        let label = state.lanes[0].process_label.clone();
        state.toggle_collapse(&label);
        state.v_scroll = 1; // start on a non-first lane of the collapsed group
        let layout = render_layout(&state, 120, 20);
        assert!(
            layout.gutter_hits.iter().any(|(_, l)| *l == label),
            "the collapsed summary header is still drawn + clickable when v_scroll \
             lands inside the group"
        );
    }

    #[test]
    fn gutter_click_collapses_and_expands_a_group() {
        let mut state = fixed_state();
        let empty = KeyModifiers::empty();
        let mut drag = MouseDrag::default();
        let layout = render_layout(&state, 120, 22);
        let (row, label) = layout
            .gutter_hits
            .first()
            .cloned()
            .expect("a group header is registered in the gutter");
        // col 1 is in the gutter (left of the track).
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Down(MouseButton::Left), 1, row, empty),
            &mut drag,
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Up(MouseButton::Left), 1, row, empty),
            &mut drag,
        );
        assert!(
            state.is_label_collapsed(&label),
            "gutter click collapses the group"
        );
        // The collapsed header is still clickable → expands again.
        let layout2 = render_layout(&state, 120, 22);
        let (row2, _) = layout2
            .gutter_hits
            .first()
            .cloned()
            .expect("collapsed header still clickable");
        handle_mouse(
            &mut state,
            &layout2,
            mouse(MouseEventKind::Down(MouseButton::Left), 1, row2, empty),
            &mut drag,
        );
        assert!(!state.is_label_collapsed(&label), "clicking again expands");
    }

    #[test]
    fn p_key_and_detail_click_follow_caller_callee_links() {
        let mut state = fixed_state();
        // Select a child span (topic.callback span2 has caller topic.publish span1).
        let child = state
            .trace
            .spans
            .iter()
            .position(|s| s.span_id == 2)
            .expect("child span");
        state.jump_to_span_idx(child);
        let caller = state.caller_of_selected().expect("child has a caller");
        handle_key(&mut state, key(KeyCode::Char('p')), 10);
        assert_eq!(
            state.selected_span_idx(),
            Some(caller),
            "p jumps to the caller"
        );

        // Now render and click a registered caller/callee row → jump to it.
        let layout = render_layout(&state, 120, 24);
        let (row, target) = *layout
            .detail_hits
            .first()
            .expect("caller/callees registered as clickable rows");
        let col = layout.detail_x0 + 2;
        let empty = KeyModifiers::empty();
        let mut drag = MouseDrag::default();
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Down(MouseButton::Left), col, row, empty),
            &mut drag,
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Up(MouseButton::Left), col, row, empty),
            &mut drag,
        );
        assert_eq!(
            state.selected_span_idx(),
            Some(target),
            "clicking a detail caller/callee row jumps to that span"
        );
    }

    #[test]
    fn shift_p_jumps_to_the_first_callee() {
        let mut state = fixed_state();
        let parent = state
            .trace
            .spans
            .iter()
            .position(|s| s.span_id == 1)
            .expect("parent span");
        state.jump_to_span_idx(parent);
        let first_callee = state
            .callees_of_selected()
            .first()
            .copied()
            .expect("parent has a callee");
        handle_key(&mut state, key(KeyCode::Char('P')), 10);
        assert_eq!(
            state.selected_span_idx(),
            Some(first_callee),
            "P jumps to the first callee (keyboard parity for the detail click)"
        );
    }

    #[test]
    fn z_key_zooms_to_the_selected_region() {
        let mut state = fixed_state();
        let full = state.view_span_ns();
        let ext = state.total_end_ns - state.total_start_ns;
        // A middle-half region, so zooming to it is observably narrower than full.
        let a = state.total_start_ns + ext / 4;
        let b = state.total_start_ns + 3 * ext / 4;
        state.set_region(a, b);
        handle_key(&mut state, key(KeyCode::Char('z')), 10);
        assert!(
            state.view_span_ns() < full,
            "z zooms the viewport into the region (span shrinks)"
        );
        assert!(
            state.view_start_ns <= a && state.view_end_ns >= b,
            "the whole region stays inside the zoomed viewport"
        );
    }

    #[test]
    fn zoom_to_selected_span_fits_label_then_caps_at_window() {
        let mut state = fixed_state();
        state.track_w = 80;
        let idx = state
            .trace
            .spans
            .iter()
            .position(|s| s.span_id == 2) // a short nested span (1µs of the ~5µs trace)
            .expect("span 2");
        state.jump_to_span_idx(idx);
        let dur = state.selected_span().unwrap().duration_ns();
        let start = state.rel_start(state.selected_span().unwrap());
        // A 20-col label of an 80-col track → the span takes 20/80, window = 4× dur.
        state.zoom_to_selected_span(20);
        assert_eq!(
            state.view_span_ns(),
            dur * 4,
            "span occupies the label width"
        );
        assert!(
            state.view_start_ns <= start && state.view_end_ns >= start + dur,
            "the span is fully inside the viewport"
        );
        // A label wider than the whole track → caps at filling the window.
        state.zoom_to_selected_span(200);
        assert_eq!(
            state.view_span_ns(),
            dur,
            "caps at the span filling the window"
        );
    }

    #[test]
    fn z_zooms_the_selected_span_when_no_region() {
        let mut state = fixed_state();
        state.track_w = 80;
        state.clear_region();
        let idx = state
            .trace
            .spans
            .iter()
            .position(|s| s.span_id == 2)
            .expect("span 2");
        state.jump_to_span_idx(idx);
        let full = state.view_span_ns();
        handle_key(&mut state, key(KeyCode::Char('z')), 10);
        assert!(
            state.view_span_ns() < full,
            "z with no region zooms onto the selected span"
        );
    }

    #[test]
    fn region_filter_input_filters_the_panel_rows() {
        let mut state = fixed_state();
        state.set_region(state.total_start_ns, state.total_end_ns);
        handle_key(&mut state, key(KeyCode::Char('F')), 10);
        assert!(
            state.region_filter_input.is_some(),
            "F opens the region filter input (region active)"
        );
        for c in "service".chars() {
            handle_key(&mut state, key(KeyCode::Char(c)), 10);
        }
        handle_key(&mut state, key(KeyCode::Enter), 10);
        assert_eq!(state.region_filter.as_deref(), Some("service"));
        let agg = state.region_aggregate().expect("region still active");
        assert!(
            !agg.by_name.is_empty(),
            "service.reply matches the committed filter"
        );
        assert!(
            agg.by_name
                .iter()
                .all(|s| crate::state::fuzzy_subsequence(&s.name, "service")),
            "every surviving row fuzzy-matches the filter"
        );
        // Esc peels the filter first, keeping the region itself.
        handle_key(&mut state, key(KeyCode::Esc), 10);
        assert!(state.region_filter.is_none(), "Esc clears the filter");
        assert!(state.region.is_some(), "...but keeps the region");
    }

    #[test]
    fn region_scroll_keys_advance_and_clamp() {
        let mut state = fixed_state();
        state.set_region(state.total_start_ns, state.total_end_ns);
        let n = state.region_aggregate().unwrap().by_name.len();
        assert!(n >= 2, "the fixture region has multiple names to scroll");
        assert_eq!(state.region_scroll, 0);
        handle_key(&mut state, key(KeyCode::Char('.')), 10);
        assert_eq!(state.region_scroll, 1, ". scrolls the region list down");
        for _ in 0..10 {
            handle_key(&mut state, key(KeyCode::Char('.')), 10);
        }
        assert_eq!(
            state.region_scroll,
            n - 1,
            "scroll clamps at the last entry"
        );
        handle_key(&mut state, key(KeyCode::Char(',')), 10);
        assert_eq!(state.region_scroll, n - 2, ", scrolls back up");
    }

    #[test]
    fn region_panel_buttons_zoom_and_open_filter() {
        let mut state = fixed_state();
        let full = state.view_span_ns();
        let ext = state.total_end_ns - state.total_start_ns;
        state.set_region(
            state.total_start_ns + ext / 4,
            state.total_start_ns + 3 * ext / 4,
        );
        let layout = render_layout(&state, 120, 28);
        let empty = KeyModifiers::empty();
        let mut drag = MouseDrag::default();
        let zoom_row = layout
            .region_buttons
            .iter()
            .find(|(_, b)| *b == crate::layout::RegionButton::Zoom)
            .map(|(r, _)| *r)
            .expect("zoom button registered");
        let filter_row = layout
            .region_buttons
            .iter()
            .find(|(_, b)| *b == crate::layout::RegionButton::Filter)
            .map(|(r, _)| *r)
            .expect("filter button registered");
        let col = layout.detail_x0 + 2;
        handle_mouse(
            &mut state,
            &layout,
            mouse(
                MouseEventKind::Down(MouseButton::Left),
                col,
                zoom_row,
                empty,
            ),
            &mut drag,
        );
        assert!(
            state.view_span_ns() < full,
            "clicking ⤢ zoom zooms into the region"
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(
                MouseEventKind::Down(MouseButton::Left),
                col,
                filter_row,
                empty,
            ),
            &mut drag,
        );
        assert!(
            state.region_filter_input.is_some(),
            "clicking ⌕ filter opens the region filter input"
        );
    }

    #[test]
    fn region_panel_wheel_scrolls_the_list() {
        let mut state = fixed_state();
        state.set_region(state.total_start_ns, state.total_end_ns);
        let layout = render_layout(&state, 120, 24);
        let empty = KeyModifiers::empty();
        let mut drag = MouseDrag::default();
        let col = layout.detail_x0 + 2; // over the region panel
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::ScrollDown, col, layout.track_y0 + 6, empty),
            &mut drag,
        );
        assert_eq!(
            state.region_scroll, 1,
            "wheel over the region panel scrolls its list, not the lanes"
        );
    }

    #[test]
    fn help_overlay_toggles_and_swallows_keys() {
        let mut state = fixed_state();
        let sel = state.selected;
        handle_key(&mut state, key(KeyCode::Char('?')), 10);
        assert!(state.show_help, "? opens the help overlay");
        // Other keys are swallowed: no quit, no selection change, stays open.
        assert_eq!(
            handle_key(&mut state, key(KeyCode::Char('j')), 10),
            Action::Continue
        );
        assert!(state.show_help, "help stays open under unrelated keys");
        assert_eq!(state.selected, sel, "keys don't act while help is open");
        // q does NOT quit while help is open — it closes the overlay.
        assert_eq!(
            handle_key(&mut state, key(KeyCode::Char('q')), 10),
            Action::Continue
        );
        assert!(!state.show_help, "q closes help (does not quit)");
    }

    #[test]
    fn legend_chip_click_cycles_grouping() {
        let mut state = fixed_state();
        assert_eq!(state.group_mode, crate::state::GroupMode::Thread);
        let layout = render_layout(&state, 120, 22);
        let (row, x0, _x1) = layout.group_chip.expect("group chip registered");
        let empty = KeyModifiers::empty();
        let mut drag = MouseDrag::default();
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Down(MouseButton::Left), x0, row, empty),
            &mut drag,
        );
        assert_eq!(
            state.group_mode,
            crate::state::GroupMode::Scope,
            "clicking the legend chip cycles thread → scope"
        );
    }

    #[test]
    fn minimap_readout_click_toggles_clock() {
        let mut state = fixed_state();
        assert!(!state.clock_abs);
        let layout = render_layout(&state, 120, 22);
        let empty = KeyModifiers::empty();
        let mut drag = MouseDrag::default();
        let col = layout.minimap_x0; // on the strip, left of the detail pane
        handle_mouse(
            &mut state,
            &layout,
            mouse(
                MouseEventKind::Down(MouseButton::Left),
                col,
                layout.minimap_readout_y,
                empty,
            ),
            &mut drag,
        );
        assert!(
            state.clock_abs,
            "clicking the wall-clock readout row toggles the clock (≡ t)"
        );
    }

    #[test]
    fn help_overlay_is_modal_for_the_mouse() {
        let mut state = fixed_state();
        let layout = render_layout(&state, 100, 18);
        state.show_help = true;
        let empty = KeyModifiers::empty();
        let mut drag = MouseDrag::default();
        // A drag that WOULD create a region (proven elsewhere) must be swallowed.
        let row = layout.track_y1 - 1;
        let x0 = layout.track_x + 10;
        let x1 = layout.track_x + layout.track_w - 10;
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Down(MouseButton::Left), x0, row, empty),
            &mut drag,
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Drag(MouseButton::Left), x1, row, empty),
            &mut drag,
        );
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::Up(MouseButton::Left), x1, row, empty),
            &mut drag,
        );
        assert!(
            state.region.is_none(),
            "a drag under the help overlay must not create a region"
        );
        // Wheel is swallowed too (no lane scroll / zoom side effect).
        let before = state.v_scroll;
        handle_mouse(
            &mut state,
            &layout,
            mouse(MouseEventKind::ScrollDown, x0, row, empty),
            &mut drag,
        );
        assert_eq!(
            state.v_scroll, before,
            "the wheel is swallowed while help is open"
        );
    }

    #[test]
    fn new_region_resets_scroll_and_stale_filter() {
        let mut state = fixed_state();
        state.set_region(state.total_start_ns, state.total_end_ns);
        // Simulate having scrolled + filtered the first region's panel.
        state.region_filter = Some("topic".into());
        state.region_scroll = 3;
        // Dragging/Setting a NEW region must start clean.
        let ext = state.total_end_ns - state.total_start_ns;
        state.set_region(
            state.total_start_ns + ext / 4,
            state.total_start_ns + ext / 2,
        );
        assert_eq!(state.region_scroll, 0, "scroll resets for a new region");
        assert!(
            state.region_filter.is_none(),
            "the stale filter is dropped for a new region"
        );
    }

    #[test]
    fn region_scroll_clamps_to_a_full_last_page() {
        let mut state = fixed_state();
        state.set_region(state.total_start_ns, state.total_end_ns);
        let n = state.region_aggregate().unwrap().by_name.len();
        assert!(n >= 3, "need several names to page through");
        state.region_rows = 2; // panel shows two entries at a time
        for _ in 0..50 {
            state.region_scroll_lines(true);
        }
        assert_eq!(
            state.region_scroll,
            n - 2,
            "scroll stops so the last page stays full (len - page, not len - 1)"
        );
    }

    #[test]
    fn region_buttons_are_not_registered_off_panel_in_a_short_pane() {
        let mut state = fixed_state();
        state.set_region(state.total_start_ns, state.total_end_ns);
        // Tall pane: both buttons fit and register.
        let tall = render_layout(&state, 120, 28);
        assert_eq!(tall.region_buttons.len(), 2, "both buttons fit when tall");
        // Short pane: the lower button would overflow the panel onto the
        // border/legend/status — it must NOT be registered (no phantom hit).
        let short = render_layout(&state, 120, 11);
        assert!(
            short.region_buttons.len() < 2,
            "the overflowing button is dropped in a short pane"
        );
        assert!(
            short
                .region_buttons
                .iter()
                .all(|(_, b)| *b == crate::layout::RegionButton::Zoom),
            "only the in-panel Zoom button survives"
        );
    }
}
