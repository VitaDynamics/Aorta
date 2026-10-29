//! Waterfall rendering: a left gutter of process/thread lane labels, a top time
//! axis with µs/ms tick labels, per-lane horizontal span bars positioned by the
//! viewport scale (children indented under parents, colored by scope, with a
//! duration label where the bar is wide enough), and a span-detail pane for the
//! current selection. Pure layout over [`WaterfallState`] + a ratatui `Frame`;
//! every helper that does arithmetic is a free fn so it is unit-testable.

use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::layout::{BarRect, Layout as ViewLayout, RegionButton};
use crate::state::{CallerState, GroupMode, WaterfallState};

/// Width of the left label gutter (process/thread names).
const GUTTER_W: u16 = 26;

// ── Vbot/VitaDynamics brand palette (design-system tokens), tuned for a dark
// terminal: the hero orange #fa6837 → peach #ffc58f gradient, warm-gray text. ──
/// Core brand orange (PANTONE 1645 C) — reserved for the hero accent.
const BRAND: Color = Color::Rgb(0xfa, 0x68, 0x37);
/// Gradient end (peach) for the brand orange→peach wordmark.
const PEACH: Color = Color::Rgb(0xff, 0xc5, 0x8f);
/// Primary text on dark (light warm gray).
const INK: Color = Color::Rgb(0xd6, 0xd5, 0xd0);
/// Secondary / muted warm gray.
const MUTED: Color = Color::Rgb(0x8f, 0x88, 0x82);
/// Brand off-white (#f8f7f6) — the ONLY tone the vbot logo is shown in (the brand
/// rule forbids recoloring the logo with the orange/accents).
const CREAM: Color = Color::Rgb(0xf8, 0xf7, 0xf6);

// The official vbot logo, rasterized to terminal QUADRANT blocks (2×2 sub-cell
// resolution) from the official vbot logo vector artwork — crisper than
// half-blocks. Shown only in the brand off-white.
/// vbot combination mark (wordmark · flower · robot face), 76×8.
const LOGO_WIDE: [&str; 8] = [
    "                                                 ▄███▄        ▗▄▄▄█████▄▄▄  ",
    "             ███                    ▐██▌        ▐█████▌      ▐█████████████▖",
    "▗▄▄▄     ▄▄▄ ███  ▗▄        ▗▄▖    ▗▟██▙▄▖  ▟██████▀██████▙  ██████████████▌",
    " ▜██▙   ▟██▛ ███▟█████▄  ▗▟██████▄ ██████▌ ▐██████▛ ▜██████▌ ▜██▘ ▝██▛  ▜██▘",
    "  ▜██▙ ▟██▛  ███▛▀▘▀▜██▙▗███▀▘▀▜██▙ ▜██▌    ▀███▙▄▄ ▄▄▟███▀  ▟██▖  ██▌  ▐██▖",
    "   ▜█████▛   ███▌   ▐███▐██▙   ▗███ ▐██▌     ▗█████▄█████▖   ██████████████▌",
    "    ▜███▘    ████▙▄▟███▘ ▜███▄▟███▘ ▐███▄▖   ▐███████████▌   ▜█████████████▘",
    "     ▀█▘     ███▀▜███▀▘   ▝▀███▛▀    ▀███▌    ▀▜██▀ ▀██▛▀     ▝▀████████▛▀▘ ",
];
/// vbot symbol (flower · robot face) for narrow terminals, 26×7.
const LOGO_SYM: [&str; 7] = [
    "    ▗▟█▙▖      ▄▄█████▙▄▖ ",
    " ▗▄▄█████▄▄▖  ▟██████████▌",
    "▐█████▀█████▌ ███▀▜██▛▜███",
    "▜███▛▘ ▝▜███▛ ▐█▌  ██  ▐█▘",
    " ▝████ ████▘  ███▄▟██▄▄███",
    " ▐█████████▌  ▜██████████▌",
    "  ▀██▛▀▜██▀    ▀▜██████▛▀ ",
];

/// Render the whole viewer into `area`, returning the [`ViewLayout`] geometry
/// (track viewport + drawn bar rects) so the event loop can hit-test the mouse
/// against exactly what was painted.
pub fn render(frame: &mut Frame, state: &WaterfallState) -> ViewLayout {
    let area = frame.area();
    // Bottom status/help line; the rest is the waterfall (split with a detail
    // pane on the right when a span is selected).
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(3),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(area);
    let body = rows[0];
    let legend = rows[1];
    let status = rows[2];

    if state.is_empty() {
        // The splash owns the whole frame — no legend/status chrome.
        render_empty(frame, area, state);
        if state.show_help {
            render_help(frame, area);
        }
        return ViewLayout::default();
    }

    // Detail pane takes a fixed slice on the right for a selection OR a region.
    let (left, detail_area) = if state.selected.is_some() || state.region.is_some() {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(30), Constraint::Length(38)])
            .split(body);
        (cols[0], Some(cols[1]))
    } else {
        (body, None)
    };
    // Carve a two-row minimap strip (wall-clock readout + density) off the top
    // of the waterfall column; the density row aligns with the waterfall track.
    let lrows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(3)])
        .split(left);
    let minimap_area = lrows[0];
    let wf_area = lrows[1];

    let mut view_layout = ViewLayout::default();
    render_minimap(frame, minimap_area, state, &mut view_layout);
    render_waterfall(frame, wf_area, state, &mut view_layout);
    if let Some(da) = detail_area {
        render_detail(frame, da, state, &mut view_layout);
    }
    render_legend(frame, legend, state, &mut view_layout);
    render_status_line(frame, status, state);
    // Overlays paint last so they sit above everything.
    if state.search_input.is_some() {
        render_search_picker(frame, area, state);
    }
    if state.show_help {
        render_help(frame, area);
    }
    view_layout
}

/// The minimap strip (two rows): a wall-clock readout on top, then a span-
/// density sparkline over the FULL trace extent whose columns ALIGN with the
/// waterfall track below (same gutter + track geometry). Click/drag the density
/// row to jump the viewport.
fn render_minimap(frame: &mut Frame, area: Rect, state: &WaterfallState, vl: &mut ViewLayout) {
    // Mirror the waterfall block's inner geometry so the density lines up: the
    // waterfall has a 1-col border, then a GUTTER_W gutter, then the track.
    if area.height < 2 || area.width <= GUTTER_W + 2 {
        return;
    }
    let gutter_x = area.x + 1;
    let track_x = gutter_x + GUTTER_W;
    let track_w = area.width - 2 - GUTTER_W;
    let extent = state
        .total_end_ns
        .saturating_sub(state.total_start_ns)
        .max(1);
    let off = state.wall_offset_secs;
    let epoch = state.trace.epoch_ns;
    let region_active = state.region.is_some();

    // ── Row 0: live wall-clock readout (region's, else the viewport's) ──
    let (a, b, color) = match state.region {
        Some((rs, re)) => (rs, re, Color::Yellow),
        None => (state.view_start_ns, state.view_end_ns, Color::Gray),
    };
    frame.render_widget(
        Paragraph::new(Span::styled(
            format!(
                " ◷ {} – {}    ({} – {})",
                fmt_wall_clock(epoch + a, off),
                fmt_wall_clock(epoch + b, off),
                humanize_ns(a),
                humanize_ns(b),
            ),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        )),
        Rect {
            x: gutter_x,
            y: area.y,
            width: area.width.saturating_sub(1),
            height: 1,
        },
    );

    // ── Row 1: gutter label + density aligned with the waterfall track ──
    let yr = area.y + 1;
    let (label, lcolor) = if region_active {
        ("region", Color::Yellow)
    } else {
        ("overview", Color::DarkGray)
    };
    frame.render_widget(
        Paragraph::new(Span::styled(
            format!("  {}", truncate(label, GUTTER_W as usize - 3)),
            Style::default().fg(lcolor),
        )),
        Rect {
            x: gutter_x,
            y: yr,
            width: GUTTER_W,
            height: 1,
        },
    );
    let col_of = |t: u64| -> usize {
        ((t.saturating_sub(state.total_start_ns)) as u128 * track_w as u128 / extent as u128)
            .min(track_w as u128 - 1) as usize
    };
    let mut counts = vec![0u32; track_w as usize];
    for s in &state.trace.spans {
        let c0 = col_of(state.rel_start(s));
        let c1 = col_of(state.rel_end(s));
        for c in counts.iter_mut().take(c1 + 1).skip(c0) {
            *c += 1;
        }
    }
    let maxc = counts.iter().copied().max().unwrap_or(1).max(1) as u128;
    const BLOCKS: [char; 9] = [' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let dens: String = counts
        .iter()
        .map(|&c| BLOCKS[((c as u128 * 8 / maxc).min(8)) as usize])
        .collect();
    frame.render_widget(
        Paragraph::new(Span::styled(
            dens.clone(),
            Style::default().fg(Color::DarkGray),
        )),
        Rect {
            x: track_x,
            y: yr,
            width: track_w,
            height: 1,
        },
    );
    // Highlight the current viewport window (bright reversed) over the density.
    let vc0 = col_of(state.view_start_ns) as u16;
    let vc1 = col_of(state.view_end_ns) as u16;
    let vw = vc1.saturating_sub(vc0).max(1);
    let seg: String = dens.chars().skip(vc0 as usize).take(vw as usize).collect();
    frame.render_widget(
        Paragraph::new(Span::styled(
            seg,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::REVERSED),
        )),
        Rect {
            x: track_x + vc0,
            y: yr,
            width: vw,
            height: 1,
        },
    );
    vl.minimap_y = yr;
    vl.minimap_x0 = track_x;
    vl.minimap_w = track_w;
    vl.minimap_total_start_ns = state.total_start_ns;
    vl.minimap_extent_ns = extent;
    vl.minimap_readout_y = area.y; // row 0 = wall-clock readout (click → toggle clock)
}

/// The legend row: a colored swatch per scope present, plus the hover/selection
/// readout — so a span's name + duration is always reachable even when its bar
/// is sub-pixel at the current zoom.
fn render_legend(frame: &mut Frame, area: Rect, state: &WaterfallState, vl: &mut ViewLayout) {
    // Leading clickable chip: cycles the lane grouping (mouse parity for 1/2/3).
    let chip = format!("[⊞ {} ▸]", state.group_mode.label());
    let chip_w = chip.chars().count() as u16;
    vl.group_chip = Some((area.y, area.x, area.x + chip_w));
    let mut spans: Vec<Span> = vec![
        Span::styled(
            chip,
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
    ];
    for scope in &state.scopes_present {
        let leaf = scope.rsplit('.').next().unwrap_or(scope);
        spans.push(Span::styled("●", Style::default().fg(scope_color(scope))));
        spans.push(Span::styled(
            format!(" {leaf}  "),
            Style::default().fg(Color::Gray),
        ));
    }
    // Search state takes the readout slot while active; else the hover/selection.
    if let Some(input) = &state.search_input {
        // Show the highlighted candidate name + how many instances it has.
        let tail = match state.search_name_row(state.search_name_idx) {
            Some((name, count)) => format!("→ {name} ×{count}"),
            None => "(no names)".to_string(),
        };
        spans.push(Span::styled(
            format!("│ /{input}▏  {tail}"),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
    } else if let Some(q) = &state.search_query {
        let pos = if state.search_matches.is_empty() {
            0
        } else {
            state.search_idx + 1
        };
        spans.push(Span::styled(
            format!(
                "│ find '{}' {}/{}  (n/N)",
                q,
                pos,
                state.search_matches.len()
            ),
            Style::default().fg(Color::Yellow),
        ));
    } else if let Some(span) = state.readout_span() {
        spans.push(Span::styled("│ ▸ ", Style::default().fg(Color::DarkGray)));
        spans.push(Span::styled(
            format!(
                "{} · {} · {} · ◷{}",
                span.name,
                span.scope,
                humanize_ns(span.duration_ns()),
                fmt_wall_clock(span.start_ns, state.wall_offset_secs)
            ),
            Style::default()
                .fg(scope_color(&span.scope))
                .add_modifier(Modifier::BOLD),
        ));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

/// Empty-state splash: the AORTA wordmark (brand orange→peach gradient) over a
/// pulse motif, with an editable trace-directory input box (type + Enter to
/// open). Styled to the Vbot/VitaDynamics design system.
fn render_empty(frame: &mut Frame, area: Rect, state: &WaterfallState) {
    // A faint screen frame so the splash reads as its own surface.
    let outer = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(MUTED))
        .title(Span::styled(
            " aorta ",
            Style::default().fg(BRAND).add_modifier(Modifier::BOLD),
        ));
    let inner = outer.inner(area);
    frame.render_widget(outer, area);
    if inner.width < 24 || inner.height < 6 {
        return; // too small for the splash; the frame alone is enough
    }

    // The vbot logo (off-white, per the brand rule). All art lines are padded to
    // equal width, so center-alignment keeps the block aligned. Pick the widest
    // mark that fits; fall back to the bare wordmark on a narrow pane.
    let logo: &[&str] = if inner.width >= 80 && inner.height >= 16 {
        &LOGO_WIDE
    } else if inner.width >= 28 && inner.height >= 14 {
        &LOGO_SYM
    } else {
        &[]
    };
    let mut lines: Vec<Line> = logo
        .iter()
        .map(|l| Line::from(Span::styled(*l, Style::default().fg(CREAM))))
        .collect();
    if logo.is_empty() {
        lines.push(Line::from(Span::styled(
            "vbot",
            Style::default().fg(CREAM).add_modifier(Modifier::BOLD),
        )));
    }
    lines.push(Line::from(""));
    // App title (orange) + descriptor — AORTA is the product, vbot is the maker.
    lines.push(Line::from(vec![
        Span::styled(
            "A O R T A",
            Style::default().fg(BRAND).add_modifier(Modifier::BOLD),
        ),
        Span::styled("   ·   offline trace waterfall", Style::default().fg(MUTED)),
    ]));
    lines.push(Line::from(""));
    if !state.empty_hint.is_empty() {
        lines.push(Line::from(Span::styled(
            state.empty_hint.clone(),
            Style::default().fg(MUTED),
        )));
        lines.push(Line::from(""));
    }

    // Vertical center: art lines + input box (3) + error row + footer row.
    let art_h = lines.len() as u16;
    let total = art_h + 3 + 2;
    let top = inner.y + inner.height.saturating_sub(total) / 2;
    for (i, line) in lines.into_iter().enumerate() {
        let y = top + i as u16;
        if y >= inner.y + inner.height {
            break;
        }
        frame.render_widget(
            Paragraph::new(line).alignment(Alignment::Center),
            Rect {
                x: inner.x,
                y,
                width: inner.width,
                height: 1,
            },
        );
    }

    // The editable path box — bordered, brand-accented, centered.
    let box_w = inner.width.saturating_sub(8).clamp(20, 64);
    let box_x = inner.x + inner.width.saturating_sub(box_w) / 2;
    let box_y = top + art_h;
    let ibox = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BRAND))
        .title(Span::styled(
            " open trace directory ",
            Style::default().fg(PEACH).add_modifier(Modifier::BOLD),
        ));
    let box_rect = Rect {
        x: box_x,
        y: box_y,
        width: box_w,
        height: 3,
    };
    let box_inner = ibox.inner(box_rect);
    frame.render_widget(ibox, box_rect);
    // Show the END of a long path (where the cursor is), not the head.
    let avail = box_inner.width.saturating_sub(2) as usize;
    let shown = if state.path_input.chars().count() > avail {
        let tail: String = state
            .path_input
            .chars()
            .rev()
            .take(avail.saturating_sub(1))
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("…{tail}")
    } else {
        state.path_input.clone()
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::raw(" "),
            Span::styled(shown, Style::default().fg(INK)),
            Span::styled("▏", Style::default().fg(BRAND).add_modifier(Modifier::BOLD)),
        ])),
        box_inner,
    );

    // Row below the box: the last-open error, else Tab-completion candidates.
    let err_y = box_y + 3;
    if let Some(err) = &state.load_error {
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                format!("⚠ {err}"),
                Style::default().fg(BRAND).add_modifier(Modifier::BOLD),
            )))
            .alignment(Alignment::Center),
            Rect {
                x: inner.x,
                y: err_y,
                width: inner.width,
                height: 1,
            },
        );
    } else if !state.path_completions.is_empty() {
        const CAP: usize = 8;
        let mut shown: String = state
            .path_completions
            .iter()
            .take(CAP)
            .map(|n| format!("{n}/"))
            .collect::<Vec<_>>()
            .join("  ");
        if state.path_completions.len() > CAP {
            shown.push_str(&format!("  +{} more", state.path_completions.len() - CAP));
        }
        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled("↹ ", Style::default().fg(BRAND)),
                Span::styled(shown, Style::default().fg(MUTED)),
            ]))
            .alignment(Alignment::Center),
            Rect {
                x: inner.x,
                y: err_y,
                width: inner.width,
                height: 1,
            },
        );
    }
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            "Tab complete · Enter open · Esc clear/quit · or set AORTA_TRACE_DIR",
            Style::default().fg(MUTED),
        )))
        .alignment(Alignment::Center),
        Rect {
            x: inner.x,
            y: err_y + 1,
            width: inner.width,
            height: 1,
        },
    );
}

/// The waterfall body: time axis + lane gutter + span bars. Fills `vl` with the
/// track viewport + every drawn bar rect for mouse hit-testing.
fn render_waterfall(frame: &mut Frame, area: Rect, state: &WaterfallState, vl: &mut ViewLayout) {
    let abs_start = state.trace.epoch_ns + state.total_start_ns;
    let block = Block::default().borders(Borders::ALL).title(format!(
        " waterfall  [{}]  start {}  view {} ",
        humanize_ns(state.total_end_ns.saturating_sub(state.total_start_ns)),
        fmt_wall_datetime(abs_start, state.wall_offset_secs),
        format_window(state.view_start_ns, state.view_end_ns),
    ));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    // Top line = time axis; the rest = lane rows.
    let axis_y = inner.y;
    let track_x = inner.x + GUTTER_W;
    let track_w = inner.width.saturating_sub(GUTTER_W);
    if track_w == 0 {
        return;
    }
    render_axis(frame, track_x, axis_y, track_w, state);

    let first_lane_y = axis_y + 1;
    let last_y = inner.y + inner.height; // exclusive
                                         // Publish the track viewport so the loop can map columns ↔ time.
    vl.track_x = track_x;
    vl.track_y0 = first_lane_y;
    vl.track_y1 = last_y;
    vl.track_w = track_w;
    vl.view_start_ns = state.view_start_ns;
    vl.view_span_ns = state.view_span_ns();

    // Reserve the bottom rows for the always-visible event + counter tracks
    // (one row per counter series + one instants row); span lanes get the rest.
    let n_counters = state.counter_tracks.len() as u16;
    let has_instants = !state.instants.is_empty();
    let bottom_rows = n_counters + u16::from(has_instants);
    let span_bottom = last_y.saturating_sub(bottom_rows).max(first_lane_y);

    let mut y = first_lane_y;
    // Label of the collapsed group whose summary row we've already drawn, so we
    // draw it exactly once even when v_scroll starts on a non-first lane of it.
    let mut drawn_collapsed: Option<String> = None;
    for (lane_idx, lane) in state.lanes.iter().enumerate().skip(state.v_scroll) {
        if y >= span_bottom {
            break;
        }
        // Collapsed group: one ▶ summary row on the first lane of the group we
        // reach (any lane, not only first_in_process), then skip its other lanes.
        if state.is_label_collapsed(&lane.process_label) {
            if drawn_collapsed.as_deref() != Some(lane.process_label.as_str()) {
                render_collapsed_header(frame, inner.x, track_x, track_w, lane, state, y, vl);
                drawn_collapsed = Some(lane.process_label.clone());
                y = y.saturating_add(1);
            }
            continue;
        }
        drawn_collapsed = None;
        if lane.first_in_process {
            if state.group_mode == GroupMode::Thread {
                // A standalone, full-width process-name header row above the
                // process's thread lanes: the full name shows and never shares a
                // row with span bars.
                render_process_header(
                    frame,
                    inner.x,
                    GUTTER_W + track_w,
                    &lane.process_label,
                    y,
                    vl,
                );
                y = y.saturating_add(1);
                if y >= span_bottom {
                    break;
                }
            } else {
                // Scope/name mode: each group is a single lane labelled inline.
                vl.gutter_hits.push((y, lane.process_label.clone()));
            }
        }
        let h = lane.height_rows();
        render_lane(
            frame,
            inner.x,
            track_x,
            track_w,
            y,
            span_bottom,
            state,
            lane,
            lane_idx,
            vl,
        );
        y = y.saturating_add(h);
    }

    // Bottom tracks, fixed (independent of span-lane scroll) so the priority-
    // inversion story stays on screen: instants row, then counter sparklines.
    let mut by = span_bottom;
    if has_instants && by < last_y {
        render_instants_row(frame, inner.x, track_x, track_w, by, state);
        by += 1;
    }
    for track in &state.counter_tracks {
        if by >= last_y {
            break;
        }
        render_counter_track(frame, inner.x, track_x, track_w, by, track, state);
        by += 1;
    }

    // Marquee region: bright vertical guides at the two edges, spanning the
    // track height, so the selected window is obvious over the bars.
    if let Some((rs, re)) = state.region {
        let h = last_y.saturating_sub(first_lane_y);
        if h > 0 {
            let column = vec!["┃"; h as usize].join("\n");
            let vs = state.view_start_ns;
            let vspan = state.view_span_ns();
            for edge in [rs, re] {
                if let Some(col) = project_point(edge, vs, vspan, track_w) {
                    frame.render_widget(
                        Paragraph::new(column.clone()).style(
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        ),
                        Rect {
                            x: track_x + col,
                            y: first_lane_y,
                            width: 1,
                            height: h,
                        },
                    );
                }
            }
        }
    }
}

/// The instants row: a `◆` marker (colored by scope) at each instant event's
/// time; the gutter labels it. Multiple events in one column collapse to one.
fn render_instants_row(
    frame: &mut Frame,
    gutter_x: u16,
    track_x: u16,
    track_w: u16,
    y: u16,
    state: &WaterfallState,
) {
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            "  ⌁ events".to_string(),
            Style::default().fg(Color::DarkGray),
        ))),
        Rect {
            x: gutter_x,
            y,
            width: GUTTER_W,
            height: 1,
        },
    );
    let view_start = state.view_start_ns;
    let view_span = state.view_span_ns();
    for ev in &state.instants {
        let Some(col) = project_point(ev.ts_ns, view_start, view_span, track_w) else {
            continue;
        };
        frame.render_widget(
            Paragraph::new(Span::styled(
                "◆",
                Style::default().fg(scope_color(&ev.scope)),
            )),
            Rect {
                x: track_x + col,
                y,
                width: 1,
                height: 1,
            },
        );
    }
}

/// One counter series as a stepped sparkline (block chars scaled to the series
/// peak), colored by scope. The gutter shows the leaf name.
fn render_counter_track(
    frame: &mut Frame,
    gutter_x: u16,
    track_x: u16,
    track_w: u16,
    y: u16,
    track: &crate::state::CounterTrack,
    state: &WaterfallState,
) {
    // Keep the instance suffix (…:control / …:1) — only drop a path prefix — so
    // the two waiter groups stay distinguishable in the gutter.
    let leaf = track.name.rsplit('/').next().unwrap_or(&track.name);
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            format!("  ∿ {}", truncate(leaf, GUTTER_W as usize - 4)),
            Style::default().fg(Color::DarkGray),
        ))),
        Rect {
            x: gutter_x,
            y,
            width: GUTTER_W,
            height: 1,
        },
    );
    const BLOCKS: [char; 9] = [' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let view_start = state.view_start_ns;
    let view_span = state.view_span_ns();
    let max = track.max_value.max(1);
    let mut s = String::with_capacity(track_w as usize);
    for cx in 0..track_w {
        let ts = view_start + (cx as u128 * view_span as u128 / track_w as u128) as u64;
        let level = (track.value_at(ts) as u128 * 8 / max as u128).min(8) as usize;
        s.push(BLOCKS[level]);
    }
    frame.render_widget(
        Paragraph::new(Span::styled(
            s,
            Style::default().fg(scope_color(&track.scope)),
        )),
        Rect {
            x: track_x,
            y,
            width: track_w,
            height: 1,
        },
    );
}

/// A standalone process header row (thread mode): `▼ <full process name>`
/// spanning the whole row (gutter + track), no bars — so the complete name shows
/// and the thread lanes render cleanly beneath it. Registers the gutter as a
/// collapse toggle.
fn render_process_header(
    frame: &mut Frame,
    x: u16,
    width: u16,
    label: &str,
    y: u16,
    vl: &mut ViewLayout,
) {
    frame.render_widget(
        Paragraph::new(Span::styled(
            format!("▼ {}", truncate(label, width.saturating_sub(2) as usize)),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Rect {
            x,
            y,
            width,
            height: 1,
        },
    );
    vl.gutter_hits.push((y, label.to_string()));
}

/// One summary row for a collapsed group: a `▶` + label in the gutter and a
/// `(N lanes · M spans, collapsed)` note in the track. Registers the gutter row
/// as a click target so the group can be expanded again.
#[allow(clippy::too_many_arguments)]
fn render_collapsed_header(
    frame: &mut Frame,
    gutter_x: u16,
    track_x: u16,
    track_w: u16,
    lane: &crate::state::Lane,
    state: &WaterfallState,
    y: u16,
    vl: &mut ViewLayout,
) {
    frame.render_widget(
        Paragraph::new(Span::styled(
            format!("▶ {}", truncate(&lane.process_label, GUTTER_W as usize - 3)),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Rect {
            x: gutter_x,
            y,
            width: GUTTER_W,
            height: 1,
        },
    );
    let (lanes, spans) = state.group_summary(&lane.process_label);
    frame.render_widget(
        Paragraph::new(Span::styled(
            format!("  ({lanes} lanes · {spans} spans, collapsed)"),
            Style::default().fg(Color::DarkGray),
        )),
        Rect {
            x: track_x,
            y,
            width: track_w,
            height: 1,
        },
    );
    vl.gutter_hits.push((y, lane.process_label.clone()));
}

/// One lane: a gutter label on the lane's first row, plus span bars placed on
/// `y + depth` so a nested child renders on its own sub-row beneath its parent
/// (a true waterfall). `last_y` is the exclusive bottom of the body.
#[allow(clippy::too_many_arguments)]
fn render_lane(
    frame: &mut Frame,
    gutter_x: u16,
    track_x: u16,
    track_w: u16,
    y: u16,
    last_y: u16,
    state: &WaterfallState,
    lane: &crate::state::Lane,
    lane_idx: usize,
    vl: &mut ViewLayout,
) {
    // ── gutter label (on the lane's first row) ──
    // In thread mode the process name lives on its own header row, so every lane
    // shows its indented thread label. In scope/name mode the (single-lane) group
    // is labelled inline in bold.
    let gutter_line = if lane.first_in_process && state.group_mode != GroupMode::Thread {
        Line::from(vec![Span::styled(
            format!("▼ {}", truncate(&lane.process_label, GUTTER_W as usize - 3)),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )])
    } else {
        let color = if lane.is_async {
            Color::Magenta
        } else {
            Color::Gray
        };
        Line::from(vec![Span::styled(
            format!("  {}", truncate(&lane.thread_label, GUTTER_W as usize - 3)),
            Style::default().fg(color),
        )])
    };
    frame.render_widget(
        Paragraph::new(gutter_line),
        Rect {
            x: gutter_x,
            y,
            width: GUTTER_W,
            height: 1,
        },
    );

    // ── span bars (one sub-row per nesting depth) ──
    let view_start = state.view_start_ns;
    let view_span = state.view_span_ns();
    let sel_pos = match state.selected {
        Some((li, pos)) if li == lane_idx => Some(pos),
        _ => None,
    };
    let hov_pos = match state.hovered {
        Some((li, pos)) if li == lane_idx => Some(pos),
        _ => None,
    };
    // Highlight after committing, and while the picker has a live candidate.
    // (An empty-query picker has no matches yet → leave the waterfall untouched.)
    let search_active = state.search_query.is_some()
        || (state.search_input.is_some() && !state.search_matches.is_empty());
    for (pos, ls) in lane.spans.iter().enumerate() {
        let Some(span) = state.trace.spans.get(ls.span_idx) else {
            continue;
        };
        let row_y = y.saturating_add(ls.depth);
        if row_y >= last_y {
            continue; // sub-row scrolled past the bottom
        }
        let s = state.rel_start(span);
        let e = state.rel_end(span);
        let Some((col, w)) = project_bar(s, e, view_start, view_span, track_w) else {
            continue; // fully outside the viewport
        };
        let selected = sel_pos == Some(pos);
        // Only the search HITS are recolored (striking) — non-matches keep their
        // normal scope color, so the hits pop out instead of dimming everything.
        let is_match = search_active && state.is_search_match((lane_idx, pos));
        // Caller/callee of the selection: make it glow (the on-screen "flow" cue).
        let related = !selected && !is_match && state.is_related_to_selection(ls.span_idx);
        let color = scope_color(&span.scope);
        let bar_style = bar_bg_style(color, selected, related, hov_pos == Some(pos), is_match);
        let dur = span.duration_ns();
        let bar_text = bar_label(&span.name, dur, ls.depth, w, span.is_async);
        let bar_x = track_x + col;
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(bar_text, bar_style))),
            Rect {
                x: bar_x,
                y: row_y,
                width: w,
                height: 1,
            },
        );
        // Record the painted rect so a click maps back to this lane slot.
        vl.bars.push(BarRect {
            x: bar_x,
            y: row_y,
            w,
            lane_idx,
            pos,
        });
    }
}

/// The time axis: a tick-marked line with µs/ms value labels written in-line at
/// each tick (epoch-relative). The first tick anchors at the left; later labels
/// start one column right of their tick so the `┬` glyph stays visible.
fn render_axis(frame: &mut Frame, x: u16, y: u16, w: u16, state: &WaterfallState) {
    let ticks = axis_ticks(state.view_start_ns, state.view_end_ns, w);
    let mut chars: Vec<char> = vec!['─'; w as usize];
    for (col, _) in &ticks {
        if (*col as usize) < chars.len() {
            chars[*col as usize] = '┬';
        }
    }
    // Write each tick's value label into the line, skipping a position where it
    // would clobber a later tick glyph.
    for (i, (col, ts)) in ticks.iter().enumerate() {
        // Absolute local wall-clock (`t` toggle) or relative ns. The tick `ts`
        // is epoch-relative; add the epoch for the wall time.
        let label = if state.clock_abs {
            fmt_wall_hms(state.trace.epoch_ns + *ts, state.wall_offset_secs)
        } else {
            humanize_ns(*ts)
        };
        // First tick: label starts AT the tick; others one cell right of it.
        let start = if i == 0 {
            *col as usize
        } else {
            *col as usize + 1
        };
        let next_tick = ticks.get(i + 1).map(|(c, _)| *c as usize);
        for (k, ch) in label.chars().enumerate() {
            let pos = start + k;
            if pos >= chars.len() {
                break;
            }
            // Don't overwrite the next tick's `┬`.
            if Some(pos) == next_tick {
                break;
            }
            chars[pos] = ch;
        }
    }
    let axis: String = chars.into_iter().collect();
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            axis,
            Style::default().fg(Color::DarkGray),
        ))),
        Rect {
            x,
            y,
            width: w,
            height: 1,
        },
    );
}

/// The region-aggregation panel shown while a marquee region is active: window
/// bounds + slice/total roll-up, then per-name `count · total · max` sorted by
/// total time (Perfetto's "selected region" stats).
fn render_region_detail(
    frame: &mut Frame,
    area: Rect,
    state: &WaterfallState,
    agg: &crate::state::RegionAgg,
    vl: &mut ViewLayout,
) {
    let block = Block::default().borders(Borders::ALL).title(" region ");
    let inner = block.inner(area);
    let off = state.wall_offset_secs;
    let epoch = state.trace.epoch_ns;
    let dim = Style::default().fg(Color::DarkGray);
    let btn = Style::default()
        .fg(Color::Black)
        .bg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let kv =
        |k: &str, v: String| Line::from(vec![Span::styled(format!("{k:<8}"), dim), Span::raw(v)]);
    let mut lines = vec![
        Line::from(Span::styled(
            format!(
                "{} – {}",
                humanize_ns(agg.start_ns),
                humanize_ns(agg.end_ns)
            ),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            format!(
                "◷ {} – {}",
                fmt_wall_clock(epoch + agg.start_ns, off),
                fmt_wall_clock(epoch + agg.end_ns, off)
            ),
            dim,
        )),
        kv(
            "width",
            humanize_ns(agg.end_ns.saturating_sub(agg.start_ns)),
        ),
        kv("slices", agg.count.to_string()),
        kv("total", humanize_ns(agg.total_ns)),
        Line::from(""),
    ];
    // ── clickable action buttons (mouse parity for z / F) ── Only register a
    // button row that actually lands inside the panel (mirrors render_detail's
    // `< inner.height` guard) so a short pane can't leave a phantom hit-target on
    // the border / legend / status line below.
    let mut push_button = |lines: &mut Vec<Line>, label: &str, kind: RegionButton| {
        let li = lines.len() as u16;
        if li < inner.height {
            vl.region_buttons.push((inner.y + li, kind));
        }
        lines.push(Line::from(Span::styled(label.to_string(), btn)));
    };
    push_button(&mut lines, " ⤢ zoom to region (z) ", RegionButton::Zoom);
    push_button(&mut lines, " ⌕ filter rows (F) ", RegionButton::Filter);
    // Filter status: live input takes precedence over a committed filter.
    if let Some(input) = &state.region_filter_input {
        lines.push(Line::from(Span::styled(
            format!("⌕ {input}▏"),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )));
    } else if let Some(f) = &state.region_filter {
        lines.push(Line::from(vec![
            Span::styled("filter  ", dim),
            Span::styled(
                format!("'{f}'  (Esc clears)"),
                Style::default().fg(Color::Yellow),
            ),
        ]));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "by name · count · total · max",
        dim,
    )));

    // Scroll + window the by-name list to what fits (2 lines per entry, reserving
    // one row for the scroll indicator).
    let total = agg.by_name.len();
    let avail = (inner.height as usize).saturating_sub(lines.len() + 1);
    let fit = (avail / 2).max(1);
    // Expose the page size so the scroll handlers can clamp to a full last page
    // (not past it). Clamp `start` to the last full page too, defensively.
    vl.region_page = fit;
    let start = state.region_scroll.min(total.saturating_sub(fit));
    for st in agg.by_name.iter().skip(start).take(fit) {
        lines.push(Line::from(vec![
            Span::styled(format!("{:>3}× ", st.count), dim),
            Span::styled(truncate(&st.name, 24), Style::default().fg(Color::Gray)),
        ]));
        lines.push(Line::from(Span::styled(
            format!(
                "      {} / {}",
                humanize_ns(st.total_ns),
                humanize_ns(st.max_ns)
            ),
            dim,
        )));
    }
    let shown_end = (start + fit).min(total);
    if total > fit {
        lines.push(Line::from(Span::styled(
            format!("  [{}–{} / {}]  ,/. wheel", start + 1, shown_end, total),
            dim,
        )));
    }
    // No wrap: keep line index == screen row so button rows hit-test exactly.
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

fn render_detail(frame: &mut Frame, area: Rect, state: &WaterfallState, vl: &mut ViewLayout) {
    // The detail pane is on the right; record its left edge for click routing.
    vl.detail_x0 = area.x;
    // A marquee region takes precedence: show the aggregation panel.
    if let Some(agg) = state.region_aggregate() {
        render_region_detail(frame, area, state, &agg, vl);
        return;
    }
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" span detail ");
    let inner = block.inner(area);
    let Some(span) = state.selected_span() else {
        frame.render_widget(block, area);
        return;
    };
    let trace_hex: String = span.trace_id.iter().map(|b| format!("{b:02x}")).collect();
    let kv = |k: &str, v: String| {
        Line::from(vec![
            Span::styled(format!("{k:<11}"), Style::default().fg(Color::DarkGray)),
            Span::raw(v),
        ])
    };
    let dim = Style::default().fg(Color::DarkGray);
    let mut lines: Vec<Line> = Vec::new();
    // (line_index, span_idx) for clickable caller/callee rows.
    let mut hits: Vec<(usize, usize)> = Vec::new();

    lines.push(Line::from(Span::styled(
        span.name.clone(),
        Style::default()
            .fg(scope_color(&span.scope))
            .add_modifier(Modifier::BOLD),
    )));
    lines.push(Line::from(""));

    // ── flow navigation: caller (▲) then callees (▼) ──
    match state.caller_state() {
        Some(CallerState::Resolved(cidx)) => {
            lines.push(Line::from(Span::styled("▲ caller  (p · click)", dim)));
            let cs = &state.trace.spans[cidx];
            let lane = state
                .span_to_slot
                .get(cidx)
                .copied()
                .flatten()
                .and_then(|(li, _)| state.lanes.get(li))
                .map(|l| l.thread_label.clone())
                .unwrap_or_default();
            hits.push((lines.len(), cidx));
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(
                    truncate(&cs.name, 22),
                    Style::default().fg(scope_color(&cs.scope)),
                ),
                Span::styled(format!("  @{}", truncate(&lane, 9)), dim),
            ]));
        }
        // Parent referenced but absent — surface it, don't pretend it's a root.
        Some(CallerState::Unresolved(pid)) => {
            lines.push(Line::from(Span::styled(
                format!("▲ caller  (unresolved id {pid})"),
                Style::default().fg(Color::Yellow),
            )));
        }
        _ => lines.push(Line::from(Span::styled("▲ caller  (root)", dim))),
    }
    let callees = state.callees_of_selected();
    lines.push(Line::from(Span::styled(
        format!("▼ callees ({})  (P · click)", callees.len()),
        dim,
    )));
    const CALLEE_CAP: usize = 6;
    for &kidx in callees.iter().take(CALLEE_CAP) {
        let ks = &state.trace.spans[kidx];
        hits.push((lines.len(), kidx));
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled(
                truncate(&ks.name, 20),
                Style::default().fg(scope_color(&ks.scope)),
            ),
            Span::styled(format!("  {}", humanize_ns(ks.duration_ns())), dim),
        ]));
    }
    if callees.len() > CALLEE_CAP {
        lines.push(Line::from(Span::styled(
            format!("  (+{} more)", callees.len() - CALLEE_CAP),
            dim,
        )));
    }
    lines.push(Line::from(""));

    // ── fields ──
    lines.push(kv("scope", span.scope.clone()));
    lines.push(kv("duration", humanize_ns(span.duration_ns())));
    lines.push(kv("start", humanize_ns(state.rel_start(span))));
    lines.push(kv("end", humanize_ns(state.rel_end(span))));
    lines.push(kv(
        "wall",
        fmt_wall_datetime(span.start_ns, state.wall_offset_secs),
    ));
    lines.push(kv("thread", span.thread_label.clone()));
    lines.push(kv(
        "kind",
        if span.is_async {
            "async (wait)".to_string()
        } else {
            "sync span".to_string()
        },
    ));
    lines.push(kv("span id", span.span_id.to_string()));
    lines.push(kv("trace id", trace_hex));

    // ── same-name duration distribution (histogram + clickable percentiles) ──
    const HIST_BUCKETS: usize = 24;
    if let Some(stats) = state.name_stats(&span.name, HIST_BUCKETS) {
        if stats.count > 1 {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("▭ same-name durations ({})", stats.count),
                dim,
            )));
            // A bottom-anchored vertical bar chart, HIST_H rows tall.
            const HIST_H: u128 = 4;
            let maxc = stats.buckets.iter().copied().max().unwrap_or(1).max(1) as u128;
            for r in 0..HIST_H {
                let threshold = HIST_H - r; // top row needs a full-height bar
                let bar: String = stats
                    .buckets
                    .iter()
                    .map(|&c| {
                        let level = (c as u128 * HIST_H).div_ceil(maxc);
                        if level >= threshold {
                            '█'
                        } else {
                            ' '
                        }
                    })
                    .collect();
                lines.push(Line::from(Span::styled(
                    format!(" {bar}"),
                    Style::default().fg(scope_color(&span.scope)),
                )));
            }
            lines.push(Line::from(Span::styled(
                format!(
                    " {}–{}  (click a row to jump)",
                    humanize_ns(stats.min_ns),
                    humanize_ns(stats.max_ns)
                ),
                dim,
            )));
            // Each percentile is its own clickable row → jump to that span.
            for m in &stats.marks {
                hits.push((lines.len(), m.span_idx));
                lines.push(Line::from(vec![
                    Span::styled(
                        format!(" ▸ {:<4} ", m.label),
                        Style::default().fg(Color::Cyan),
                    ),
                    Span::styled(humanize_ns(m.value_ns), Style::default().fg(Color::Gray)),
                ]));
            }
        }
    }

    // Register clickable rows that actually land inside the pane.
    for (li, idx) in hits {
        if (li as u16) < inner.height {
            vl.detail_hits.push((inner.y + li as u16, idx));
        }
    }
    // No wrap: keep line index == screen row so detail clicks map exactly.
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

/// Bottom help/status line.
fn render_status_line(frame: &mut Frame, area: Rect, state: &WaterfallState) {
    let n_spans = state.trace.spans.len();
    let n_lanes = state.lanes.len();
    let text = format!(
        " {n_spans} spans · {n_lanes} lanes (by {}) · {}   / find · z/Z zoom region/span · 1/2/3 group · c collapse · f fit · ? help · q",
        state.group_mode.label(),
        format_window(state.view_start_ns, state.view_end_ns),
    );
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            text,
            Style::default().fg(Color::DarkGray),
        ))),
        area,
    );
}

/// Centered, opaque keybinding/mouse cheat-sheet overlay (`?`).
fn render_help(frame: &mut Frame, area: Rect) {
    let w = 64.min(area.width);
    let h = 34.min(area.height);
    let rect = Rect {
        x: area.x + area.width.saturating_sub(w) / 2,
        y: area.y + area.height.saturating_sub(h) / 2,
        width: w,
        height: h,
    };
    let head = |s: &str| {
        Line::from(Span::styled(
            s.to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
    };
    let row = |k: &str, d: &str| {
        Line::from(vec![
            Span::styled(format!(" {k:<14}"), Style::default().fg(Color::Yellow)),
            Span::styled(d.to_string(), Style::default().fg(Color::Gray)),
        ])
    };
    let lines = vec![
        head(" Keyboard"),
        row("j / k  ↑ ↓", "select prev / next span"),
        row("g / G", "first / last span"),
        row("PgUp / PgDn", "page lanes up / down"),
        row("+ -  (w s)", "zoom in / out"),
        row("[ ]  a d h l", "pan left / right"),
        row("f", "fit — zoom to full extent"),
        row("z", "zoom to the region, else the selected span"),
        row("Z", "zoom to fit the selected span"),
        row(
            "/",
            "find by name — picker lists names; ↑↓ pick, Enter highlight",
        ),
        row(
            "n / N",
            "next / previous instance of the name (chronological)",
        ),
        row("p / P", "jump to caller / first callee"),
        row("1 / 2 / 3", "group by thread / scope / name"),
        row("c", "collapse / expand the group"),
        row("t", "toggle relative-ns / wall-clock"),
        row("F", "filter region rows (fuzzy)"),
        row(", / .", "scroll region list up / down"),
        row("?", "toggle this help"),
        row("Esc", "clear region/search, else quit"),
        row("q", "quit"),
        Line::from(""),
        head(" Mouse"),
        row("left-click", "select span · jump · panel buttons"),
        row("left-drag", "select a time region"),
        row("right-drag", "pan the timeline"),
        row("wheel", "scroll lanes · region list over panel"),
        row("ctrl+wheel", "zoom at cursor"),
        row("shift+wheel", "pan the timeline"),
        row("minimap", "click/drag navigate · row 0 = clock"),
        row("legend chip", "click [⊞ …] to cycle grouping"),
        row("gutter header", "click to collapse / expand a group"),
    ];
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" help · ? or Esc to close ");
    frame.render_widget(Clear, rect);
    frame.render_widget(Paragraph::new(lines).block(block), rect);
}

/// Centered find picker (`/`): a live query line over a scrollable list of
/// distinct span **names** (each with its instance count). The query filters by
/// name only; the highlighted name's instances preview in the waterfall. Enter
/// confirms that name as the highlight; Esc cancels.
fn render_search_picker(frame: &mut Frame, area: Rect, state: &WaterfallState) {
    let input = state.search_input.as_deref().unwrap_or("");
    let total = state.search_names.len();
    let sel = state.search_name_idx;

    let w = 76.min(area.width).max(24);
    let h = 26.min(area.height).max(6);
    let rect = Rect {
        x: area.x + area.width.saturating_sub(w) / 2,
        y: area.y + area.height.saturating_sub(h) / 2,
        width: w,
        height: h,
    };
    let inner_w = w.saturating_sub(2) as usize;
    // content = header + blank + N rows (borders eat 2).
    let visible = (h.saturating_sub(4)) as usize;

    // Scroll so the cursor stays in view, roughly centered.
    let first = if total <= visible {
        0
    } else {
        sel.saturating_sub(visible / 2).min(total - visible)
    };

    let pos = if total == 0 { 0 } else { sel + 1 };
    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                format!("/{input}▏"),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("   {pos}/{total} names"),
                Style::default().fg(Color::Gray),
            ),
        ]),
        Line::from(""),
    ];

    if total == 0 {
        let msg = if input.trim().is_empty() {
            "  (no spans)"
        } else {
            "  (no names match)"
        };
        lines.push(Line::from(Span::styled(
            msg,
            Style::default().fg(Color::DarkGray),
        )));
    }

    // `name … ×count`, count right-padded into a fixed gutter.
    let count_w = 8usize;
    let name_w = inner_w.saturating_sub(2 + 1 + count_w).max(8);

    for i in first..(first + visible).min(total) {
        let Some((name, count)) = state.search_name_row(i) else {
            continue;
        };
        let name_s = pad(&truncate(name, name_w), name_w);
        let count_s = format!("×{count}");
        if i == sel {
            let full = format!("▶ {name_s} {count_s}");
            lines.push(Line::from(Span::styled(
                pad(&full, inner_w),
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )));
        } else {
            lines.push(Line::from(vec![
                Span::styled("  ", Style::default().fg(Color::DarkGray)),
                Span::styled(name_s, Style::default().fg(Color::White)),
                Span::raw(" "),
                Span::styled(count_s, Style::default().fg(Color::DarkGray)),
            ]));
        }
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" find · ↑↓ name · Enter highlight · Esc cancel ");
    frame.render_widget(Clear, rect);
    frame.render_widget(Paragraph::new(lines).block(block), rect);
}

// ── pure layout helpers (unit-tested) ───────────────────────────────────────

/// Map an epoch-relative `[start,end]` span to `(col_offset, width)` columns
/// within a track of `track_w` cells showing `[view_start, view_start+view_span]`.
/// Returns `None` when the span is fully outside the viewport. A visible span is
/// always at least 1 column wide (so a sub-pixel span is still a marker).
pub fn project_bar(
    start: u64,
    end: u64,
    view_start: u64,
    view_span: u64,
    track_w: u16,
) -> Option<(u16, u16)> {
    if track_w == 0 || view_span == 0 {
        return None;
    }
    let view_end = view_start.saturating_add(view_span);
    if end < view_start || start > view_end {
        return None;
    }
    let w = track_w as u64;
    let clamp_start = start.max(view_start);
    let clamp_end = end.min(view_end).max(clamp_start);
    let col = ((clamp_start - view_start) as u128 * w as u128 / view_span as u128) as u64;
    let end_col = ((clamp_end - view_start) as u128 * w as u128 / view_span as u128) as u64;
    let col = col.min(w.saturating_sub(1));
    let width = (end_col.saturating_sub(col)).max(1).min(w - col);
    Some((col as u16, width as u16))
}

/// Column for an epoch-relative point `ts` within a track showing
/// `[view_start, view_start+view_span]`, or `None` if outside the window. The
/// point analogue of [`project_bar`] (instant markers, counter columns).
pub fn project_point(ts: u64, view_start: u64, view_span: u64, track_w: u16) -> Option<u16> {
    if track_w == 0 || view_span == 0 {
        return None;
    }
    if ts < view_start || ts > view_start.saturating_add(view_span) {
        return None;
    }
    let col = (ts - view_start) as u128 * track_w as u128 / view_span as u128;
    Some((col as u64).min(track_w as u64 - 1) as u16)
}

/// Choose up to ~`track_w/16` evenly-spaced tick `(col, epoch_rel_ns)` pairs.
pub fn axis_ticks(view_start: u64, view_end: u64, track_w: u16) -> Vec<(u16, u64)> {
    if track_w < 2 || view_end <= view_start {
        return Vec::new();
    }
    let n = (track_w / 16).clamp(1, 8) as u64;
    let span = view_end - view_start;
    let mut out = Vec::new();
    for i in 0..=n {
        let col = (i * track_w as u64 / n).min(track_w as u64 - 1) as u16;
        let ts = view_start + i * span / n;
        out.push((col, ts));
    }
    out
}

/// The text drawn inside a span bar: a depth marker, the name, and a duration
/// suffix when the bar is wide enough to show it.
pub fn bar_label(name: &str, dur_ns: u64, depth: u16, w: u16, is_async: bool) -> String {
    let w = w as usize;
    if w == 0 {
        return String::new();
    }
    let marker = if is_async {
        "~".to_string()
    } else {
        "·".repeat(depth.min(6) as usize)
    };
    let dur = humanize_ns(dur_ns);
    // Prefer "marker name (dur)"; fall back to shorter forms as width shrinks.
    let full = format!("{marker}{name} ({dur})");
    if full.chars().count() <= w {
        return pad(&full, w);
    }
    let mid = format!("{marker}{name}");
    if mid.chars().count() <= w {
        return pad(&mid, w);
    }
    pad(&truncate(&format!("{marker}{name}"), w), w)
}

/// The natural (un-truncated) column width of a span bar's full label
/// `marker name (dur)` — what `zoom-to-span` needs the bar to be wide enough for.
pub fn bar_label_width(name: &str, dur_ns: u64, depth: u16, is_async: bool) -> usize {
    let marker = if is_async { 1 } else { depth.min(6) as usize };
    // "{marker}{name} ({dur})" → marker + name + " (" + dur + ")".
    marker + name.chars().count() + humanize_ns(dur_ns).chars().count() + 3
}

/// Human-readable ns: ns / µs / ms / s with 3 significant decimals.
pub fn humanize_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.3}us", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.3}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.3}s", ns as f64 / 1_000_000_000.0)
    }
}

fn format_window(start: u64, end: u64) -> String {
    format!("{}–{}", humanize_ns(start), humanize_ns(end))
}

/// Broken-out local wall-clock parts for an absolute realtime ns + UTC offset:
/// `(year, month, day, hour, min, sec, milli)`.
fn wall_parts(unix_ns: u64, offset_secs: i64) -> (i64, u32, u32, u64, u64, u64, u64) {
    let local = (unix_ns as i128 + offset_secs as i128 * 1_000_000_000).max(0) as u128;
    let ms = ((local / 1_000_000) % 1000) as u64;
    let secs = (local / 1_000_000_000) as i64;
    let (y, mo, d) = civil_from_days(secs.div_euclid(86_400));
    let tod = secs.rem_euclid(86_400) as u64;
    (y, mo, d, tod / 3600, (tod % 3600) / 60, tod % 60, ms)
}

/// `HH:MM:SS` (axis ticks — compact).
pub fn fmt_wall_hms(unix_ns: u64, offset_secs: i64) -> String {
    let (_, _, _, h, mi, s, _) = wall_parts(unix_ns, offset_secs);
    format!("{h:02}:{mi:02}:{s:02}")
}

/// `HH:MM:SS.mmm` (readout / region bounds).
pub fn fmt_wall_clock(unix_ns: u64, offset_secs: i64) -> String {
    let (_, _, _, h, mi, s, ms) = wall_parts(unix_ns, offset_secs);
    format!("{h:02}:{mi:02}:{s:02}.{ms:03}")
}

/// `YYYY-MM-DD HH:MM:SS.mmm` (title anchor / span detail).
pub fn fmt_wall_datetime(unix_ns: u64, offset_secs: i64) -> String {
    let (y, mo, d, h, mi, s, ms) = wall_parts(unix_ns, offset_secs);
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{ms:03}")
}

/// Howard Hinnant's civil-from-days: days since 1970-01-01 → `(year, month,
/// day)`. Pure integer math (no date library).
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Striking fills for search hits — they pop against the *unchanged* scope-
/// colored background (non-matches are never recolored by a search). `MATCH_BG`
/// (white) marks every hit; `CURSOR_BG` (brand orange) marks the `n`/`N` current
/// hit / the selected span. Explicit RGB so they paint opaque under terminal
/// transparency.
pub(crate) const MATCH_BG: Color = Color::White;
pub(crate) const CURSOR_BG: Color = Color::Rgb(0xfa, 0x68, 0x37);

/// The visual hierarchy for a single span bar (bars are background-filled, so
/// the background color *is* the bar). Precedence, strongest first:
///
/// * **selected** → the brand-orange cursor (the `n`/`N` current hit / clicked
///   span). No scope uses it, so it's unmistakable.
/// * **search match** (a hit that isn't the cursor) → solid **white** — a very
///   prominent contrast against the normal colors.
/// * **related** → caller/callee of the selection: scope color + white underline.
/// * otherwise → the **normal scope color** — a search does NOT change the color
///   of non-matches; `hovered` adds bold+underline.
fn bar_bg_style(
    color: Color,
    selected: bool,
    related: bool,
    hovered: bool,
    search_match: bool,
) -> Style {
    if selected {
        return Style::default()
            .bg(CURSOR_BG)
            .fg(Color::Black)
            .add_modifier(Modifier::BOLD);
    }
    if search_match {
        return Style::default()
            .bg(MATCH_BG)
            .fg(Color::Black)
            .add_modifier(Modifier::BOLD);
    }
    if related {
        return Style::default()
            .bg(color)
            .fg(Color::White)
            .add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
    }
    let base = Style::default().bg(color).fg(Color::Black);
    if hovered {
        base.add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
    } else {
        base
    }
}

/// Color for a recording scope: a fixed, legible mapping for the known aorta
/// scopes (so `topic` is always blue, `sched` always amber, …), with a stable
/// hash fallback for anything unrecognized. Matches on the scope leaf, so
/// `aorta.topic` and `topic` agree.
fn scope_color(scope: &str) -> Color {
    match scope.rsplit('.').next().unwrap_or(scope) {
        "topic" => Color::LightBlue,
        "sched" => Color::LightYellow,
        "service" => Color::LightGreen,
        "action" => Color::LightMagenta,
        "stage" => Color::LightCyan,
        "timer" => Color::LightRed,
        _ => hash_scope_color(scope),
    }
}

/// Stable hash → one of the dimmer base colors, so an unknown scope keeps a
/// consistent color that still reads as distinct from the bright known set.
fn hash_scope_color(scope: &str) -> Color {
    let mut h: u32 = 2166136261;
    for b in scope.bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(16777619);
    }
    const PALETTE: [Color; 6] = [
        Color::Blue,
        Color::Green,
        Color::Yellow,
        Color::Magenta,
        Color::Cyan,
        Color::Red,
    ];
    PALETTE[(h as usize) % PALETTE.len()]
}

fn truncate(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if s.chars().count() <= max {
        return s.to_string();
    }
    if max == 1 {
        return "…".to_string();
    }
    let keep: String = s.chars().take(max - 1).collect();
    format!("{keep}…")
}

fn pad(s: &str, w: usize) -> String {
    let len = s.chars().count();
    if len >= w {
        s.chars().take(w).collect()
    } else {
        let mut out = s.to_string();
        out.extend(std::iter::repeat_n(' ', w - len));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn humanize_ns_scales_units() {
        assert_eq!(humanize_ns(500), "500ns");
        assert_eq!(humanize_ns(1_500), "1.500us");
        assert_eq!(humanize_ns(2_000_000), "2.000ms");
        assert_eq!(humanize_ns(3_000_000_000), "3.000s");
    }

    #[test]
    fn project_bar_maps_full_window_to_full_track() {
        // A span covering exactly the whole viewport spans the whole track.
        let (col, w) = project_bar(0, 1000, 0, 1000, 100).unwrap();
        assert_eq!(col, 0);
        assert_eq!(w, 100);
    }

    #[test]
    fn project_bar_positions_and_widths_within_window() {
        // Span [500,750] of a [0,1000] window over 100 cols → col 50, width 25.
        let (col, w) = project_bar(500, 750, 0, 1000, 100).unwrap();
        assert_eq!(col, 50);
        assert_eq!(w, 25);
    }

    #[test]
    fn project_bar_outside_window_is_none() {
        assert!(project_bar(2000, 3000, 0, 1000, 100).is_none());
    }

    #[test]
    fn project_point_maps_into_track_and_culls_outside() {
        assert_eq!(project_point(0, 0, 1000, 100), Some(0));
        assert_eq!(project_point(500, 0, 1000, 100), Some(50));
        assert_eq!(project_point(1000, 0, 1000, 100), Some(99)); // clamps to last col
        assert_eq!(project_point(2000, 0, 1000, 100), None); // past the window
    }

    #[test]
    fn wall_clock_formats_a_known_timestamp_and_offset() {
        // 1_700_000_000 s since the Unix epoch = 2023-11-14 22:13:20 UTC.
        let ns = 1_700_000_000u64 * 1_000_000_000;
        assert_eq!(fmt_wall_datetime(ns, 0), "2023-11-14 22:13:20.000");
        assert_eq!(fmt_wall_hms(ns, 0), "22:13:20");
        // +8h (CST) rolls into the next day.
        assert_eq!(fmt_wall_datetime(ns, 8 * 3600), "2023-11-15 06:13:20.000");
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn scope_color_is_semantic_and_stable() {
        // Leaf-matched: the fully-qualified and bare forms agree.
        assert_eq!(scope_color("aorta.topic"), scope_color("topic"));
        assert_eq!(scope_color("aorta.sched"), Color::LightYellow);
        // Distinct known scopes get distinct colors.
        assert_ne!(scope_color("aorta.topic"), scope_color("aorta.sched"));
        // Unknown scopes are stable (same input → same color).
        assert_eq!(scope_color("custom.thing"), scope_color("custom.thing"));
    }

    #[test]
    fn project_bar_subpixel_span_is_at_least_one_col() {
        // A 1ns span in a 1ms window is sub-pixel but must still be a marker.
        let (_, w) = project_bar(500_000, 500_001, 0, 1_000_000, 80).unwrap();
        assert_eq!(w, 1);
    }

    #[test]
    fn bar_styles_form_a_clear_search_hierarchy() {
        // topic scope is LightBlue.
        let topic = scope_color("aorta.topic");
        // A NON-match keeps its normal scope color — a search never recolors it,
        // whether the search is active or not.
        let non_match = bar_bg_style(topic, false, false, false, false);
        assert_eq!(
            non_match.bg,
            Some(topic),
            "non-matches keep their scope color"
        );
        // A hit (not the cursor) pops in solid white.
        let m = bar_bg_style(topic, false, false, false, true);
        assert_eq!(m.bg, Some(MATCH_BG));
        assert_ne!(m.bg, Some(topic), "a hit is recolored to a striking white");
        assert!(!m.add_modifier.contains(Modifier::REVERSED));
        // The cursor (current hit / selection) is the brand-orange accent.
        let sel = bar_bg_style(topic, true, false, false, true);
        assert_eq!(sel.bg, Some(CURSOR_BG));
        assert_ne!(sel.bg, m.bg, "cursor is distinct from other hits");
        // Selection wins over match/related/hover (top of the hierarchy).
        assert_eq!(
            bar_bg_style(topic, true, true, true, true).bg,
            Some(CURSOR_BG)
        );
    }

    #[test]
    fn bar_label_shows_duration_when_wide_else_truncates() {
        let wide = bar_label("topic.publish", 1500, 0, 40, false);
        assert!(wide.contains("topic.publish"));
        assert!(wide.contains("1.500us"), "duration shown when wide: {wide}");
        // Narrow: at least part of the name, padded to width.
        let narrow = bar_label("topic.publish", 1500, 0, 6, false);
        assert_eq!(narrow.chars().count(), 6);
    }

    #[test]
    fn axis_ticks_span_the_window() {
        let ticks = axis_ticks(0, 1000, 80);
        assert!(!ticks.is_empty());
        assert_eq!(ticks.first().unwrap().1, 0);
        assert_eq!(ticks.last().unwrap().1, 1000);
    }
}
