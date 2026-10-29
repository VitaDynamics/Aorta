//! Viewer state for the offline trace waterfall: lanes (one per
//! `(process, thread)`), a time viewport (zoom/pan), vertical scroll, and a
//! selection — all derived from the shared
//! [`aorta_trace2perfetto::DecodedTrace`]. No I/O, no rendering, no terminal
//! here: pure state transitions so they are exhaustively unit-testable.

use aorta_core::trace::chunk::ChunkEventType;
use aorta_trace2perfetto::{DecodedTrace, TraceSpan};

/// One placed span on a lane: the shared [`TraceSpan`] plus its nesting depth
/// (how deep under same-lane parents it sits) so the waterfall can indent
/// children under parents.
#[derive(Debug, Clone)]
pub struct LaneSpan {
    /// Index into [`DecodedTrace::spans`] — the source of truth for details.
    pub span_idx: usize,
    /// Nesting depth from `parent_span_id` chains within this lane (0 = root).
    pub depth: u16,
}

/// A lane groups all spans recorded on one `(pid, tid)` (or the synthetic async
/// section of a process). One row in the left gutter; one horizontal band of
/// span bars on the right.
#[derive(Debug, Clone)]
pub struct Lane {
    pub pid: i64,
    pub tid: i64,
    pub process_label: String,
    pub thread_label: String,
    /// Async lanes hold `FLAG_ASYNC` waits that overlap by nature; they are
    /// rendered as a separate section, never as a per-thread call stack. The
    /// thread label is suffixed `[async]`.
    pub is_async: bool,
    /// `true` for the first lane of a process group (drives the process header
    /// line in the gutter).
    pub first_in_process: bool,
    pub spans: Vec<LaneSpan>,
}

impl Lane {
    /// Number of vertical sub-rows this lane needs: one per nesting depth, so a
    /// child renders on its own row beneath its parent (a true waterfall). At
    /// least 1 even when the lane is empty.
    pub fn height_rows(&self) -> u16 {
        self.spans
            .iter()
            .map(|s| s.depth)
            .max()
            .map(|d| d + 1)
            .unwrap_or(1)
    }
}

/// A counter time-series (e.g. `sched.group_waiters`) projected from point
/// events: samples sorted by time plus the peak, for a stepped sparkline.
#[derive(Debug, Clone)]
pub struct CounterTrack {
    pub scope: String,
    pub name: String,
    pub process_label: String,
    /// `(epoch_relative_ts_ns, value)` sorted by time.
    pub samples: Vec<(u64, u64)>,
    pub max_value: u64,
}

impl CounterTrack {
    /// The counter value in effect at `ts` — the most recent sample at/before
    /// `ts` (0 before the first). A step function, like a Perfetto counter track.
    pub fn value_at(&self, ts: u64) -> u64 {
        match self.samples.binary_search_by_key(&ts, |s| s.0) {
            Ok(i) => self.samples[i].1,
            Err(0) => 0,
            Err(i) => self.samples[i - 1].1,
        }
    }
}

/// A point-in-time event (e.g. `sched.priority_inversion`) on the timeline.
#[derive(Debug, Clone)]
pub struct InstantEvent {
    pub scope: String,
    pub name: String,
    /// Epoch-relative timestamp.
    pub ts_ns: u64,
    pub process_label: String,
}

/// Per-span-name roll-up within a marquee-selected region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionStat {
    pub name: String,
    pub count: usize,
    pub total_ns: u64,
    pub max_ns: u64,
}

/// Aggregation over a marquee-selected time region (Perfetto's "select a range,
/// see the stats"). `by_name` is sorted by total duration descending.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionAgg {
    pub start_ns: u64,
    pub end_ns: u64,
    pub count: usize,
    pub total_ns: u64,
    pub by_name: Vec<RegionStat>,
}

/// One percentile marker over a same-name duration distribution, carrying the
/// span instance at that rank so the viewer can jump to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PctMark {
    pub label: &'static str,
    pub value_ns: u64,
    pub span_idx: usize,
}

/// Duration distribution of all spans sharing a name: a linear histogram plus
/// p50/p90/p99/max, each pointing at the span at that rank.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameStats {
    pub name: String,
    pub count: usize,
    pub min_ns: u64,
    pub max_ns: u64,
    /// Linear-binned counts over `[min_ns, max_ns]`.
    pub buckets: Vec<u32>,
    /// p50 / p90 / p99 / max, in that order.
    pub marks: Vec<PctMark>,
}

/// How spans are bucketed into lanes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupMode {
    /// One lane per `(process, thread)` — a true call-stack waterfall (default).
    Thread,
    /// One lane per recording scope (`aorta.topic`, `aorta.sched`, …).
    Scope,
    /// One lane per span name.
    Name,
}

impl GroupMode {
    pub fn label(self) -> &'static str {
        match self {
            GroupMode::Thread => "thread",
            GroupMode::Scope => "scope",
            GroupMode::Name => "name",
        }
    }
}

/// The caller relationship of the selected span, for the detail pane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallerState {
    /// True root: `parent_span_id == 0`.
    Root,
    /// Parent resolved to this span index (clickable / `p`-jumpable).
    Resolved(usize),
    /// `parent_span_id` is set but the parent span is absent (cross-process,
    /// dropped chunk, ring overflow). Surfaced instead of a silent phantom root.
    Unresolved(u64),
}

/// The full viewer model.
pub struct WaterfallState {
    pub trace: DecodedTrace,
    pub lanes: Vec<Lane>,
    /// Inclusive-exclusive time window currently shown, in epoch-relative ns.
    pub view_start_ns: u64,
    pub view_end_ns: u64,
    /// Full extent of the trace in epoch-relative ns (`view_*` is clamped here).
    pub total_start_ns: u64,
    pub total_end_ns: u64,
    /// First visible lane row (vertical scroll).
    pub v_scroll: usize,
    /// Currently selected span as `(lane_idx, span_position_in_lane)`.
    pub selected: Option<(usize, usize)>,
    /// Lane slot under the mouse cursor (hover), for the readout + bar emphasis.
    pub hovered: Option<(usize, usize)>,
    /// Distinct scopes present across spans + points, sorted — drives the legend.
    pub scopes_present: Vec<String>,
    /// Counter series (one stepped sparkline track each), built from point events.
    pub counter_tracks: Vec<CounterTrack>,
    /// Instant events (markers on a dedicated events row), sorted by time.
    pub instants: Vec<InstantEvent>,
    /// A marquee-selected time region `(start_ns, end_ns)` for aggregation.
    pub region: Option<(u64, u64)>,
    /// Scroll offset (entry index) into the region panel's by-name list.
    pub region_scroll: usize,
    /// How many by-name entries the region panel shows at once (one page), kept
    /// current from the last-rendered layout so scrolling clamps to a full page.
    pub region_rows: usize,
    /// Committed fuzzy filter narrowing the region panel's by-name rows.
    pub region_filter: Option<String>,
    /// Live input buffer while typing a region filter (`F`); `None` when idle.
    pub region_filter_input: Option<String>,
    /// Whether the keybinding/mouse help overlay is shown (`?`).
    pub show_help: bool,
    /// Committed search: the **exact span name** the user picked. Only spans of
    /// this name are highlighted (the typed query merely filters the name list).
    pub search_query: Option<String>,
    /// Live fuzzy query while the find picker is open (`/`); `None` when closed.
    /// Matches the span **name only** — never thread/scope/other metadata.
    pub search_input: Option<String>,
    /// Distinct span names matching the live query (the picker list) + their
    /// instance counts, and the cursor into them.
    pub search_names: Vec<(String, usize)>,
    pub search_name_idx: usize,
    /// Instances (`(lane,pos)`, chronological) of the currently-picked name —
    /// the highlighted set + what `n`/`N` cycles. `search_idx` is the cursor.
    pub search_matches: Vec<(usize, usize)>,
    pub search_idx: usize,
    /// When true the time axis shows absolute local wall-clock; else relative ns.
    pub clock_abs: bool,
    /// Local offset (seconds east of UTC) at the trace epoch, for wall-clock
    /// formatting. Computed once via `localtime_r`.
    pub wall_offset_secs: i64,
    /// `span_idx → (lane_idx, pos)`, so a span can be re-selected by index when
    /// jumping along a caller/callee link.
    pub span_to_slot: Vec<Option<(usize, usize)>>,
    /// `span_idx → parent span_idx`, resolved via `(trace_id, parent_span_id)`
    /// (`None` for roots or dangling parents).
    pub parent_of: Vec<Option<usize>>,
    /// `span_idx → direct child span_idxs` (the inverse of `parent_of`).
    pub children: Vec<Vec<usize>>,
    /// How spans are currently bucketed into lanes (`1`/`2`/`3` toggles).
    pub group_mode: GroupMode,
    /// Group labels (process / scope / name) collapsed to one summary row.
    pub collapsed: std::collections::HashSet<String>,
    /// Visible lane-row budget (set from the terminal each frame) so vertical
    /// auto-scroll can keep a downward selection/jump on screen.
    pub view_rows: usize,
    /// Track width in columns (set from the rendered layout each frame), so
    /// zoom-to-span can size the window to fit the selection's label.
    pub track_w: u16,
    /// Human hint shown in the empty state (names the directory scanned).
    pub empty_hint: String,
    /// Editable directory path on the empty/splash screen (type + Enter to open).
    pub path_input: String,
    /// Error from the last failed open attempt, shown under the path input.
    pub load_error: Option<String>,
    /// Candidate sub-directory names from the last Tab completion (shown under
    /// the input when more than one matched).
    pub path_completions: Vec<String>,
}

/// Minimum window the viewport will zoom into (1µs) so a single very short span
/// is still inspectable without the scale collapsing to zero.
const MIN_VIEW_NS: u64 = 1_000;

impl WaterfallState {
    /// Build the lane model from a decoded trace. Lanes are ordered by
    /// `(process, async-last, thread)` so a process's real-thread lanes come
    /// before its async section, and processes stay grouped.
    pub fn new(trace: DecodedTrace) -> WaterfallState {
        let lanes = build_lanes(&trace, GroupMode::Thread);
        // Epoch-relative extent: spans/points are absolute realtime; subtract
        // the epoch so the timeline starts near zero (matching the converter).
        let epoch = trace.epoch_ns;
        let mut min = u64::MAX;
        let mut max = 0u64;
        for s in &trace.spans {
            min = min.min(s.start_ns.saturating_sub(epoch));
            max = max.max(s.end_ns.saturating_sub(epoch));
        }
        for p in &trace.points {
            let t = p.ts_ns.saturating_sub(epoch);
            min = min.min(t);
            max = max.max(t);
        }
        let (total_start, total_end) = if min == u64::MAX {
            (0, 0)
        } else {
            // Guarantee a non-zero span so the scale is always finite.
            (min, max.max(min + MIN_VIEW_NS))
        };
        let selected = lanes
            .iter()
            .enumerate()
            .find(|(_, l)| !l.spans.is_empty())
            .map(|(i, _)| (i, 0usize));
        // Distinct scopes (spans + points) for the legend, computed once.
        let mut scopes_present: Vec<String> = trace
            .spans
            .iter()
            .map(|s| s.scope.clone())
            .chain(trace.points.iter().map(|p| p.scope.clone()))
            .collect();
        scopes_present.sort();
        scopes_present.dedup();
        // Project point events into instant markers + per-(scope,name) counter
        // series — the data the priority-inversion story lives in.
        use std::collections::BTreeMap;
        let mut instants: Vec<InstantEvent> = Vec::new();
        // Key by process too, so same-named counters from different processes
        // (e.g. scheduler counters across nodes) stay separate tracks.
        let mut counters: BTreeMap<(String, String, String), CounterTrack> = BTreeMap::new();
        for p in &trace.points {
            let ts = p.ts_ns.saturating_sub(epoch);
            match p.kind {
                ChunkEventType::Instant => instants.push(InstantEvent {
                    scope: p.scope.clone(),
                    name: p.name.clone(),
                    ts_ns: ts,
                    process_label: p.process_label.clone(),
                }),
                ChunkEventType::Counter => {
                    let track = counters
                        .entry((p.process_label.clone(), p.scope.clone(), p.name.clone()))
                        .or_insert_with(|| CounterTrack {
                            scope: p.scope.clone(),
                            name: p.name.clone(),
                            process_label: p.process_label.clone(),
                            samples: Vec::new(),
                            max_value: 0,
                        });
                    track.samples.push((ts, p.value));
                    track.max_value = track.max_value.max(p.value);
                }
                _ => {}
            }
        }
        instants.sort_by_key(|e| e.ts_ns);
        let mut counter_tracks: Vec<CounterTrack> = counters.into_values().collect();
        for track in &mut counter_tracks {
            track.samples.sort_by_key(|s| s.0);
        }
        // Caller/callee indices for flow navigation: reverse map to lane slots,
        // and parent/child links resolved by (trace_id, span_id).
        use std::collections::HashMap;
        let n = trace.spans.len();
        let mut span_to_slot = vec![None; n];
        for (li, lane) in lanes.iter().enumerate() {
            for (pos, ls) in lane.spans.iter().enumerate() {
                if let Some(slot) = span_to_slot.get_mut(ls.span_idx) {
                    *slot = Some((li, pos));
                }
            }
        }
        let mut by_id: HashMap<([u8; 16], u64), usize> = HashMap::with_capacity(n);
        for (i, s) in trace.spans.iter().enumerate() {
            by_id.insert((s.trace_id, s.span_id), i);
        }
        let mut parent_of = vec![None; n];
        let mut children = vec![Vec::new(); n];
        for (i, s) in trace.spans.iter().enumerate() {
            if s.parent_span_id != 0 {
                if let Some(&p) = by_id.get(&(s.trace_id, s.parent_span_id)) {
                    parent_of[i] = Some(p);
                    children[p].push(i);
                }
            }
        }
        WaterfallState {
            trace,
            lanes,
            view_start_ns: total_start,
            view_end_ns: total_end,
            total_start_ns: total_start,
            total_end_ns: total_end,
            v_scroll: 0,
            selected,
            hovered: None,
            scopes_present,
            counter_tracks,
            instants,
            region: None,
            region_scroll: 0,
            region_rows: 0,
            region_filter: None,
            region_filter_input: None,
            show_help: false,
            search_query: None,
            search_input: None,
            search_names: Vec::new(),
            search_name_idx: 0,
            search_matches: Vec::new(),
            search_idx: 0,
            clock_abs: false,
            wall_offset_secs: local_wall_offset_secs(epoch),
            span_to_slot,
            parent_of,
            children,
            group_mode: GroupMode::Thread,
            collapsed: std::collections::HashSet::new(),
            view_rows: 0,
            track_w: 0,
            empty_hint: String::new(),
            path_input: String::new(),
            load_error: None,
            path_completions: Vec::new(),
        }
    }

    /// Set the message shown when there are no spans (names the scanned dir).
    pub fn with_empty_hint(mut self, hint: impl Into<String>) -> WaterfallState {
        self.empty_hint = hint.into();
        self
    }

    /// Pre-fill the empty-screen path input (the directory that was scanned), so
    /// the user can edit it and re-open without retyping the whole path.
    pub fn with_path_input(mut self, path: impl Into<String>) -> WaterfallState {
        self.path_input = path.into();
        self
    }

    pub fn is_empty(&self) -> bool {
        self.lanes.iter().all(|l| l.spans.is_empty())
    }

    /// Resolve a lane slot `(lane_idx, pos)` to its source span.
    pub fn span_at(&self, sel: (usize, usize)) -> Option<&TraceSpan> {
        let (lane_i, pos) = sel;
        let ls = self.lanes.get(lane_i)?.spans.get(pos)?;
        self.trace.spans.get(ls.span_idx)
    }

    /// The currently-selected span (resolved through the lane index).
    pub fn selected_span(&self) -> Option<&TraceSpan> {
        self.selected.and_then(|sel| self.span_at(sel))
    }

    /// The span the status readout describes: the hovered one if the mouse is
    /// over a bar, else the selection.
    pub fn readout_span(&self) -> Option<&TraceSpan> {
        self.hovered
            .or(self.selected)
            .and_then(|sel| self.span_at(sel))
    }

    // ── marquee region + aggregation ─────────────────────────────────────────

    /// Set the marquee region to the sorted `(a, b)` interval. A region is always
    /// a fresh view, so reset the panel's scroll + any stale filter (mirrors the
    /// invariant `clear_region` / `region_filter_commit` uphold) — otherwise a new
    /// marquee opens silently pre-filtered/scrolled from the previous one.
    pub fn set_region(&mut self, a: u64, b: u64) {
        self.region = Some((a.min(b), a.max(b)));
        self.region_scroll = 0;
        self.region_filter = None;
        self.region_filter_input = None;
    }

    /// Clear any marquee region (and its panel filter/scroll state).
    pub fn clear_region(&mut self) {
        self.region = None;
        self.region_scroll = 0;
        self.region_filter = None;
        self.region_filter_input = None;
    }

    /// Zoom the viewport to the marquee region's time bounds, with a little
    /// padding so the edges stay visible. No-op when no region is set.
    pub fn zoom_to_region(&mut self) {
        let Some((a, b)) = self.region else { return };
        let (start, end) = (a.min(b), a.max(b));
        let span = end.saturating_sub(start).max(MIN_VIEW_NS);
        let pad = span / 20; // ~5% breathing room on each side
        self.set_view(start.saturating_sub(pad), end.saturating_add(pad));
    }

    /// Zoom the viewport onto the selected span so it occupies the smaller of
    /// `label_cols` (just wide enough to show its text) or the full track width
    /// (filling the window) — whichever is fewer columns — and centre it. The
    /// caller supplies the rendered label width. No-op without a selection.
    pub fn zoom_to_selected_span(&mut self, label_cols: u16) {
        let Some(span) = self.selected_span() else {
            return;
        };
        let dur = span.duration_ns().max(MIN_VIEW_NS);
        let start = self.rel_start(span);
        let tw = self.track_w.max(1) as u64;
        // Target columns for the span: enough for the label, capped at the window.
        let target = (label_cols.max(1) as u64).min(tw);
        let view_span = dur.saturating_mul(tw) / target.max(1);
        let view_span = view_span.max(MIN_VIEW_NS);
        let center = start.saturating_add(dur / 2);
        let new_start = center.saturating_sub(view_span / 2);
        self.set_view(new_start, new_start + view_span);
    }

    /// Scroll the region panel's by-name list by one entry, clamped so the last
    /// page stays full (`len - page`, not `len - 1`). `region_rows` (one page) is
    /// kept current from the rendered layout. No-op without a region.
    pub fn region_scroll_lines(&mut self, down: bool) {
        let len = self
            .region_aggregate()
            .map(|a| a.by_name.len())
            .unwrap_or(0);
        let page = self.region_rows.max(1);
        let max = len.saturating_sub(page);
        if down {
            self.region_scroll = (self.region_scroll + 1).min(max);
        } else {
            self.region_scroll = self.region_scroll.saturating_sub(1);
        }
    }

    /// Begin typing a region filter (only meaningful with a region active).
    pub fn region_filter_begin(&mut self) {
        if self.region.is_some() {
            self.region_filter_input = Some(String::new());
        }
    }
    pub fn region_filter_input_push(&mut self, c: char) {
        if let Some(q) = &mut self.region_filter_input {
            q.push(c);
        }
    }
    pub fn region_filter_input_backspace(&mut self) {
        if let Some(q) = &mut self.region_filter_input {
            q.pop();
        }
    }
    /// Commit the typed region filter (empty clears it); resets the scroll.
    pub fn region_filter_commit(&mut self) {
        let Some(q) = self.region_filter_input.take() else {
            return;
        };
        let q = q.trim().to_string();
        self.region_filter = if q.is_empty() { None } else { Some(q) };
        self.region_scroll = 0;
    }
    /// Drop the committed region filter and any live input; resets the scroll.
    pub fn region_filter_clear(&mut self) {
        self.region_filter = None;
        self.region_filter_input = None;
        self.region_scroll = 0;
    }

    /// Toggle the keybinding/mouse help overlay.
    pub fn toggle_help(&mut self) {
        self.show_help = !self.show_help;
    }

    /// Roll up every span intersecting the current region by name (count, total
    /// and max duration), sorted by total duration. `None` when no region set.
    pub fn region_aggregate(&self) -> Option<RegionAgg> {
        let (start, end) = self.region?;
        use std::collections::BTreeMap;
        let mut by: BTreeMap<&str, (usize, u64, u64)> = BTreeMap::new();
        let mut count = 0usize;
        let mut total = 0u64;
        for s in &self.trace.spans {
            // Keep spans whose extent intersects [start, end].
            if self.rel_end(s) < start || self.rel_start(s) > end {
                continue;
            }
            // When a panel filter is active, keep only fuzzy-matching names.
            if let Some(f) = &self.region_filter {
                if !fuzzy_subsequence(&s.name, f) {
                    continue;
                }
            }
            let dur = s.duration_ns();
            count += 1;
            total = total.saturating_add(dur);
            let e = by.entry(s.name.as_str()).or_insert((0, 0, 0));
            e.0 += 1;
            e.1 = e.1.saturating_add(dur);
            e.2 = e.2.max(dur);
        }
        let mut by_name: Vec<RegionStat> = by
            .into_iter()
            .map(|(name, (count, total_ns, max_ns))| RegionStat {
                name: name.to_string(),
                count,
                total_ns,
                max_ns,
            })
            .collect();
        by_name.sort_by(|a, b| {
            b.total_ns
                .cmp(&a.total_ns)
                .then(b.count.cmp(&a.count))
                .then(a.name.cmp(&b.name))
        });
        Some(RegionAgg {
            start_ns: start,
            end_ns: end,
            count,
            total_ns: total,
            by_name,
        })
    }

    // ── search ───────────────────────────────────────────────────────────────

    /// Open the find picker (`/`): list every span name, preview the first.
    pub fn search_begin(&mut self) {
        self.search_input = Some(String::new());
        self.search_name_idx = 0;
        self.recompute_search_names();
    }

    /// Edit the live query — the picker's **name** list re-filters as you type and
    /// the top candidate's instances preview in the waterfall.
    pub fn search_input_push(&mut self, c: char) {
        if let Some(q) = &mut self.search_input {
            q.push(c);
        }
        self.search_name_idx = 0;
        self.recompute_search_names();
        self.jump_to_current_match(); // preview (no-op when nothing highlighted)
    }
    pub fn search_input_backspace(&mut self) {
        if let Some(q) = &mut self.search_input {
            q.pop();
        }
        self.search_name_idx = 0;
        self.recompute_search_names();
        self.jump_to_current_match();
    }

    /// Rebuild the distinct-**name** candidate list from the live query, matching
    /// the span **name only** (fuzzy subsequence). Empty query → every name, but
    /// nothing is highlighted yet (so opening `/` leaves the waterfall untouched);
    /// once you type, the top candidate's instances highlight.
    fn recompute_search_names(&mut self) {
        use std::collections::BTreeMap;
        let q = self
            .search_input
            .as_deref()
            .or(self.search_query.as_deref())
            .unwrap_or("")
            .trim()
            .to_string();
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for lane in &self.lanes {
            for ls in &lane.spans {
                if let Some(s) = self.trace.spans.get(ls.span_idx) {
                    if q.is_empty() || fuzzy_subsequence(&s.name, &q) {
                        *counts.entry(s.name.as_str()).or_default() += 1;
                    }
                }
            }
        }
        let names: Vec<(String, usize)> = counts
            .into_iter()
            .map(|(n, c)| (n.to_string(), c))
            .collect();
        self.search_names = names;
        if self.search_name_idx >= self.search_names.len() {
            self.search_name_idx = 0;
        }
        if q.is_empty() {
            // Browsing the full list — don't highlight/dim until the user narrows.
            self.search_matches.clear();
            self.search_idx = 0;
        } else {
            self.select_candidate_name();
        }
    }

    /// Point `search_matches` at the currently-highlighted candidate name's
    /// instances (chronological). Pure — the caller previews via
    /// [`Self::jump_to_current_match`] when a viewport move is wanted.
    fn select_candidate_name(&mut self) {
        let name = self
            .search_names
            .get(self.search_name_idx)
            .map(|(n, _)| n.clone());
        self.search_matches = match name {
            Some(n) => self.instances_of_name(&n),
            None => Vec::new(),
        };
        self.search_idx = 0;
    }

    /// Every `(lane, pos)` whose span has exactly `name`, sorted by start time.
    fn instances_of_name(&self, name: &str) -> Vec<(usize, usize)> {
        let mut keyed: Vec<(u64, (usize, usize))> = Vec::new();
        for (li, lane) in self.lanes.iter().enumerate() {
            for (pos, ls) in lane.spans.iter().enumerate() {
                if let Some(s) = self.trace.spans.get(ls.span_idx) {
                    if s.name == name {
                        keyed.push((s.start_ns, (li, pos)));
                    }
                }
            }
        }
        keyed.sort_by_key(|k| k.0);
        keyed.into_iter().map(|k| k.1).collect()
    }

    /// A picker row: `(name, instance_count)`.
    pub fn search_name_row(&self, i: usize) -> Option<(&str, usize)> {
        self.search_names.get(i).map(|(n, c)| (n.as_str(), *c))
    }

    /// Move the picker cursor over the **name** list (↑/↓), previewing the new
    /// candidate's instances.
    pub fn search_name_move(&mut self, down: bool) {
        let n = self.search_names.len();
        if n == 0 {
            return;
        }
        self.search_name_idx = if down {
            (self.search_name_idx + 1) % n
        } else {
            (self.search_name_idx + n - 1) % n
        };
        self.select_candidate_name();
        self.jump_to_current_match();
    }

    /// Confirm the highlighted **name**: only spans of that exact name stay
    /// highlighted (never the loose typed query), and `n`/`N` cycle its instances.
    pub fn search_commit(&mut self) {
        if self.search_input.take().is_none() {
            return; // not in the picker
        }
        if self.search_names.is_empty() {
            self.search_query = None;
            self.search_matches.clear();
            self.search_idx = 0;
            return;
        }
        // Populate the picked name's instances (the empty-query browse path left
        // them cleared) and lock it in as the active highlight.
        self.select_candidate_name();
        self.search_query = self
            .search_names
            .get(self.search_name_idx)
            .map(|(n, _)| n.clone());
        self.jump_to_current_match();
    }

    /// Move to the next/previous instance of the active name, recentering on it.
    pub fn search_cycle(&mut self, forward: bool) {
        let n = self.search_matches.len();
        if n == 0 {
            return;
        }
        self.search_idx = if forward {
            (self.search_idx + 1) % n
        } else {
            (self.search_idx + n - 1) % n
        };
        self.jump_to_current_match();
    }

    fn jump_to_current_match(&mut self) {
        if let Some(&sel) = self.search_matches.get(self.search_idx) {
            self.selected = Some(sel);
            self.ensure_selected_visible();
            if let Some(span) = self.span_at(sel) {
                self.center_on(self.rel_start(span));
            }
        }
    }

    /// Drop the active query + input + candidates + matches.
    pub fn search_clear(&mut self) {
        self.search_query = None;
        self.search_input = None;
        self.search_names.clear();
        self.search_name_idx = 0;
        self.search_matches.clear();
        self.search_idx = 0;
    }

    /// Whether `sel` is one of the current search matches (drives highlighting).
    pub fn is_search_match(&self, sel: (usize, usize)) -> bool {
        self.search_matches.contains(&sel)
    }

    /// Duration distribution over every span named `name` (histogram +
    /// p50/p90/p99/max, each pointing at the span at that rank). `None` if none.
    pub fn name_stats(&self, name: &str, n_buckets: usize) -> Option<NameStats> {
        let mut items: Vec<(usize, u64)> = self
            .trace
            .spans
            .iter()
            .enumerate()
            .filter(|(_, s)| s.name == name)
            .map(|(i, s)| (i, s.duration_ns()))
            .collect();
        if items.is_empty() {
            return None;
        }
        items.sort_by_key(|&(_, d)| d);
        let count = items.len();
        let min_ns = items[0].1;
        let max_ns = items[count - 1].1;
        let nb = n_buckets.max(1);
        let mut buckets = vec![0u32; nb];
        let extent = (max_ns - min_ns).max(1) as u128;
        for &(_, d) in &items {
            // +1 in the divisor keeps the max value inside the last bucket.
            let b = ((d - min_ns) as u128 * nb as u128 / (extent + 1)).min(nb as u128 - 1) as usize;
            buckets[b] += 1;
        }
        // rank for percentile p: ceil(p*count)-1, clamped into [0, count-1].
        let at =
            |p: f64| -> usize { (((p * count as f64).ceil() as usize).max(1) - 1).min(count - 1) };
        let mark = |label: &'static str, r: usize| PctMark {
            label,
            value_ns: items[r].1,
            span_idx: items[r].0,
        };
        let marks = vec![
            mark("p50", at(0.50)),
            mark("p90", at(0.90)),
            mark("p99", at(0.99)),
            mark("max", count - 1),
        ];
        Some(NameStats {
            name: name.to_string(),
            count,
            min_ns,
            max_ns,
            buckets,
            marks,
        })
    }

    /// Toggle the time axis between relative ns and absolute local wall-clock.
    pub fn toggle_clock(&mut self) {
        self.clock_abs = !self.clock_abs;
    }

    /// Switch the lane grouping (`1`/`2`/`3`), rebuilding lanes if it changed.
    pub fn set_group_mode(&mut self, mode: GroupMode) {
        if self.group_mode != mode {
            self.group_mode = mode;
            self.regroup();
        }
    }

    /// Cycle the lane grouping thread → scope → name → thread (the legend chip).
    pub fn cycle_group_mode(&mut self) {
        let next = match self.group_mode {
            GroupMode::Thread => GroupMode::Scope,
            GroupMode::Scope => GroupMode::Name,
            GroupMode::Name => GroupMode::Thread,
        };
        self.set_group_mode(next);
    }

    // ── collapse / expand lane groups ────────────────────────────────────────

    /// Whether a lane group (by its label) is collapsed to a summary row.
    pub fn is_label_collapsed(&self, label: &str) -> bool {
        self.collapsed.contains(label)
    }

    /// Collapse ⇄ expand the group with this label.
    pub fn toggle_collapse(&mut self, label: &str) {
        if !self.collapsed.remove(label) {
            self.collapsed.insert(label.to_string());
        }
    }

    /// Collapse/expand the group the current selection belongs to (`c`).
    pub fn toggle_collapse_selected(&mut self) {
        if let Some(label) = self
            .selected
            .and_then(|(li, _)| self.lanes.get(li))
            .map(|l| l.process_label.clone())
        {
            self.toggle_collapse(&label);
        }
    }

    /// `(lane_count, span_count)` of the group sharing `label` — the collapsed
    /// summary line.
    pub fn group_summary(&self, label: &str) -> (usize, usize) {
        let lanes = self
            .lanes
            .iter()
            .filter(|l| l.process_label == label)
            .collect::<Vec<_>>();
        (lanes.len(), lanes.iter().map(|l| l.spans.len()).sum())
    }

    /// Re-bucket spans into lanes for the current `group_mode`, rebuild the
    /// span→slot map, and reset selection/scroll (the slots changed).
    fn regroup(&mut self) {
        // Preserve which span is selected across the relayout (resolve before
        // the lane indices change).
        let keep = self.selected_span_idx();
        self.lanes = build_lanes(&self.trace, self.group_mode);
        let mut span_to_slot = vec![None; self.trace.spans.len()];
        for (li, lane) in self.lanes.iter().enumerate() {
            for (pos, ls) in lane.spans.iter().enumerate() {
                if let Some(slot) = span_to_slot.get_mut(ls.span_idx) {
                    *slot = Some((li, pos));
                }
            }
        }
        self.span_to_slot = span_to_slot;
        // Re-resolve the kept span to its new slot, else fall back to the first.
        self.selected = keep
            .and_then(|idx| self.span_to_slot.get(idx).copied().flatten())
            .or_else(|| {
                self.lanes
                    .iter()
                    .enumerate()
                    .find(|(_, l)| !l.spans.is_empty())
                    .map(|(i, _)| (i, 0))
            });
        self.hovered = None;
        self.v_scroll = 0;
        // Search matches are `(lane,pos)` and no longer valid after relayout.
        self.search_clear();
    }

    // ── flow navigation (caller / callees) ───────────────────────────────────

    /// The `DecodedTrace::spans` index of the current selection, if any.
    pub fn selected_span_idx(&self) -> Option<usize> {
        let (li, pos) = self.selected?;
        self.lanes.get(li)?.spans.get(pos).map(|ls| ls.span_idx)
    }

    /// The caller (parent span index) of the current selection.
    pub fn caller_of_selected(&self) -> Option<usize> {
        self.parent_of
            .get(self.selected_span_idx()?)
            .copied()
            .flatten()
    }

    /// Classify the selection's caller: true root, resolved parent, or a set-but-
    /// missing parent (so a dropped link is shown, never silently a root).
    pub fn caller_state(&self) -> Option<CallerState> {
        let idx = self.selected_span_idx()?;
        let span = self.trace.spans.get(idx)?;
        if span.parent_span_id == 0 {
            return Some(CallerState::Root);
        }
        Some(match self.parent_of.get(idx).copied().flatten() {
            Some(p) => CallerState::Resolved(p),
            None => CallerState::Unresolved(span.parent_span_id),
        })
    }

    /// The direct callees (child span indices) of the current selection.
    pub fn callees_of_selected(&self) -> &[usize] {
        match self.selected_span_idx() {
            Some(idx) => self.children.get(idx).map(|v| v.as_slice()).unwrap_or(&[]),
            None => &[],
        }
    }

    /// Select the span at `span_idx` and frame the viewport on it (follow a
    /// caller/callee link). No-op if the index has no lane slot.
    pub fn jump_to_span_idx(&mut self, span_idx: usize) {
        if let Some(Some(slot)) = self.span_to_slot.get(span_idx).copied() {
            self.selected = Some(slot);
            self.ensure_selected_visible();
            if let Some(span) = self.span_at(slot) {
                self.center_on(self.rel_start(span));
            }
        }
    }

    /// Jump to the caller of the current selection, if any.
    pub fn jump_to_caller(&mut self) {
        if let Some(p) = self.caller_of_selected() {
            self.jump_to_span_idx(p);
        }
    }

    /// Jump to the first callee (child) of the current selection — the keyboard
    /// mirror of clicking a callee row in the detail pane.
    pub fn jump_to_first_callee(&mut self) {
        if let Some(k) = self.callees_of_selected().first().copied() {
            self.jump_to_span_idx(k);
        }
    }

    /// Whether `span_idx` is the caller or a direct callee of the selection
    /// (drives the on-screen related-bar highlight).
    pub fn is_related_to_selection(&self, span_idx: usize) -> bool {
        let Some(sel) = self.selected_span_idx() else {
            return false;
        };
        if self.parent_of.get(sel).copied().flatten() == Some(span_idx) {
            return true;
        }
        self.children
            .get(sel)
            .is_some_and(|c| c.contains(&span_idx))
    }

    /// Epoch-relative start of a span (what the timeline renders against).
    pub fn rel_start(&self, span: &TraceSpan) -> u64 {
        span.start_ns.saturating_sub(self.trace.epoch_ns)
    }
    /// Epoch-relative end of a span.
    pub fn rel_end(&self, span: &TraceSpan) -> u64 {
        span.end_ns.saturating_sub(self.trace.epoch_ns)
    }

    pub fn view_span_ns(&self) -> u64 {
        self.view_end_ns.saturating_sub(self.view_start_ns).max(1)
    }

    // ── vertical scroll + selection ──────────────────────────────────────────

    /// Move selection to the next/previous span in lane order (down/up). Wraps
    /// across lanes; skips empty lanes. `delta` is +1 (down) or -1 (up).
    pub fn select_step(&mut self, down: bool) {
        let flat = self.flat_order();
        if flat.is_empty() {
            return;
        }
        let cur = self
            .selected
            .and_then(|sel| flat.iter().position(|&x| x == sel))
            .unwrap_or(0);
        let next = if down {
            (cur + 1).min(flat.len() - 1)
        } else {
            cur.saturating_sub(1)
        };
        self.selected = Some(flat[next]);
        self.ensure_selected_visible();
    }

    /// Jump selection to the first / last span (g / G).
    pub fn select_end(&mut self, last: bool) {
        let flat = self.flat_order();
        if flat.is_empty() {
            return;
        }
        self.selected = Some(if last { flat[flat.len() - 1] } else { flat[0] });
        self.ensure_selected_visible();
        // Also frame the viewport to the selected span's edge.
        if let Some(span) = self.selected_span() {
            if last {
                self.center_on(self.rel_end(span));
            } else {
                self.center_on(self.rel_start(span));
            }
        }
    }

    /// Lanes-first, then span-position order, of every non-empty slot — the
    /// linear selection order for up/down.
    fn flat_order(&self) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        for (li, lane) in self.lanes.iter().enumerate() {
            for pos in 0..lane.spans.len() {
                out.push((li, pos));
            }
        }
        out
    }

    pub fn scroll_lanes(&mut self, down: bool, view_rows: usize) {
        let max_scroll = self.lanes.len().saturating_sub(1);
        if down {
            self.v_scroll = (self.v_scroll + 1).min(max_scroll);
        } else {
            self.v_scroll = self.v_scroll.saturating_sub(1);
        }
        let _ = view_rows;
    }

    fn ensure_selected_visible(&mut self) {
        if let Some((lane_i, _)) = self.selected {
            // Scroll up if above the fold, or down if at/below the visible
            // bottom (bringing the lane to the top guarantees it is drawn).
            let window = self.view_rows.max(1);
            if lane_i < self.v_scroll || lane_i >= self.v_scroll + window {
                self.v_scroll = lane_i;
            }
        }
    }

    // ── horizontal zoom + pan ────────────────────────────────────────────────

    /// Zoom in (`factor < 1`) or out (`factor > 1`) about the viewport center,
    /// clamped to `[MIN_VIEW_NS, total]`.
    pub fn zoom(&mut self, factor: f64) {
        let center = self.view_start_ns + self.view_span_ns() / 2;
        self.zoom_at(factor, center);
    }

    /// Zoom keeping `anchor_ns` pinned at its current on-screen position — the
    /// cursor-anchored zoom a mouse wheel uses (Perfetto `ctrl+wheel`). Clamped
    /// to `[MIN_VIEW_NS, total]`.
    pub fn zoom_at(&mut self, factor: f64, anchor_ns: u64) {
        let span = self.view_span_ns();
        let new_span = ((span as f64 * factor) as u64)
            .clamp(MIN_VIEW_NS, self.total_extent_ns().max(MIN_VIEW_NS));
        // Where the anchor sits in the current window (0..=1); keep it there.
        let anchor = anchor_ns.clamp(self.view_start_ns, self.view_end_ns);
        let frac = (anchor - self.view_start_ns) as f64 / span as f64;
        let new_start = anchor.saturating_sub((new_span as f64 * frac) as u64);
        self.set_view(new_start, new_start + new_span);
    }

    /// Pan by a fraction of the visible window (`frac` may be negative).
    pub fn pan(&mut self, frac: f64) {
        let delta = (self.view_span_ns() as f64 * frac) as i64;
        let span = self.view_span_ns();
        let start = if delta >= 0 {
            self.view_start_ns.saturating_add(delta as u64)
        } else {
            self.view_start_ns.saturating_sub((-delta) as u64)
        };
        self.set_view(start, start + span);
    }

    /// Reset the viewport to the full trace extent (zoom-to-fit).
    pub fn fit(&mut self) {
        self.view_start_ns = self.total_start_ns;
        self.view_end_ns = self.total_end_ns;
    }

    /// Recenter the viewport on `ts_ns` (keeping its width) — the minimap click.
    pub fn focus_on_time(&mut self, ts_ns: u64) {
        self.center_on(ts_ns);
    }

    fn center_on(&mut self, ts: u64) {
        let span = self.view_span_ns();
        let start = ts.saturating_sub(span / 2);
        self.set_view(start, start + span);
    }

    fn total_extent_ns(&self) -> u64 {
        self.total_end_ns.saturating_sub(self.total_start_ns)
    }

    /// Clamp a requested window into the trace extent, keeping its width.
    fn set_view(&mut self, mut start: u64, mut end: u64) {
        let span = end.saturating_sub(start).max(MIN_VIEW_NS);
        if start < self.total_start_ns {
            start = self.total_start_ns;
            end = start + span;
        }
        if end > self.total_end_ns {
            end = self.total_end_ns;
            start = end.saturating_sub(span).max(self.total_start_ns);
        }
        self.view_start_ns = start;
        self.view_end_ns = end.max(start + MIN_VIEW_NS);
    }
}

/// Dispatch lane construction by the active grouping.
fn build_lanes(trace: &DecodedTrace, mode: GroupMode) -> Vec<Lane> {
    match mode {
        GroupMode::Thread => build_lanes_by_thread(trace),
        GroupMode::Scope => build_lanes_by_key(trace, |s| s.scope.clone()),
        GroupMode::Name => build_lanes_by_key(trace, |s| s.name.clone()),
    }
}

/// Group spans by a string key (scope or name), one lane per key, with rows
/// assigned by greedy interval packing (overlapping spans get separate rows —
/// the spans in a non-thread lane are NOT a call stack). Lanes sorted by key.
fn build_lanes_by_key(trace: &DecodedTrace, key_fn: impl Fn(&TraceSpan) -> String) -> Vec<Lane> {
    use std::collections::BTreeMap;
    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (idx, s) in trace.spans.iter().enumerate() {
        groups.entry(key_fn(s)).or_default().push(idx);
    }
    let mut lanes = Vec::new();
    for (key, mut idxs) in groups {
        idxs.sort_by_key(|&i| trace.spans[i].start_ns);
        // First-fit packing: place each span on the first row whose last span
        // has already ended; this reproduces nesting depth for a real stack.
        let mut row_end: Vec<u64> = Vec::new();
        let mut spans = Vec::with_capacity(idxs.len());
        for &i in &idxs {
            let s = &trace.spans[i];
            let row = row_end
                .iter()
                .position(|&end| end <= s.start_ns)
                .unwrap_or(row_end.len());
            if row == row_end.len() {
                row_end.push(s.end_ns);
            } else {
                row_end[row] = s.end_ns;
            }
            spans.push(LaneSpan {
                span_idx: i,
                depth: row.min(u16::MAX as usize) as u16,
            });
        }
        lanes.push(Lane {
            pid: 0,
            tid: 0,
            process_label: key.clone(),
            thread_label: key,
            is_async: false,
            first_in_process: true, // each key is its own gutter header
            spans,
        });
    }
    lanes
}

/// Group spans into `(pid, tid)` lanes; async spans go to a per-process
/// `[async]` lane so they are never laid on a thread call-stack row.
fn build_lanes_by_thread(trace: &DecodedTrace) -> Vec<Lane> {
    use std::collections::BTreeMap;
    // Key: (pid, is_async, tid). async-last within a process via the bool.
    let mut groups: BTreeMap<(i64, bool, i64), Lane> = BTreeMap::new();
    for (idx, s) in trace.spans.iter().enumerate() {
        let key = (s.pid, s.is_async, s.tid);
        let lane = groups.entry(key).or_insert_with(|| Lane {
            pid: s.pid,
            tid: s.tid,
            process_label: s.process_label.clone(),
            thread_label: if s.is_async {
                format!("{} [async]", s.thread_label)
            } else {
                s.thread_label.clone()
            },
            is_async: s.is_async,
            first_in_process: false,
            spans: Vec::new(),
        });
        lane.spans.push(LaneSpan {
            span_idx: idx,
            depth: 0,
        });
    }
    let mut lanes: Vec<Lane> = groups.into_values().collect();

    // Per-lane: sort spans by start (longer first at ties) and compute nesting
    // depth from parent_span_id chains within the same lane.
    for lane in &mut lanes {
        let spans = &trace.spans;
        lane.spans.sort_by(|a, b| {
            let sa = &spans[a.span_idx];
            let sb = &spans[b.span_idx];
            sa.start_ns
                .cmp(&sb.start_ns)
                .then(sb.end_ns.cmp(&sa.end_ns))
                .then(sa.span_id.cmp(&sb.span_id))
        });
        // span_id -> end, for a simple depth-by-containment walk (the spans on
        // one OS-thread lane form a proper stack in the sync case).
        let mut stack: Vec<u64> = Vec::new(); // ends of currently-open ancestors
        for ls in &mut lane.spans {
            let s = &spans[ls.span_idx];
            while let Some(&open_end) = stack.last() {
                if s.start_ns >= open_end {
                    stack.pop();
                } else {
                    break;
                }
            }
            ls.depth = stack.len().min(u16::MAX as usize) as u16;
            stack.push(s.end_ns);
        }
    }

    // Mark the first lane of each process (gutter header).
    let mut last_pid: Option<i64> = None;
    for lane in &mut lanes {
        if last_pid != Some(lane.pid) {
            lane.first_in_process = true;
            last_pid = Some(lane.pid);
        }
    }
    lanes
}

/// Case-insensitive subsequence ("fuzzy") match: every char of `needle` appears
/// in `haystack` in order, not necessarily contiguous (so `tcb` matches
/// `topic.callback`). An empty needle matches everything.
pub fn fuzzy_subsequence(haystack: &str, needle: &str) -> bool {
    let mut hay = haystack.chars().map(|c| c.to_ascii_lowercase());
    'next: for nc in needle.chars().map(|c| c.to_ascii_lowercase()) {
        for hc in hay.by_ref() {
            if hc == nc {
                continue 'next;
            }
        }
        return false;
    }
    true
}

/// Local offset (seconds east of UTC) in effect at `reference_unix_ns`, via one
/// `localtime_r` call. Returns 0 (UTC) if the reference is unusable.
fn local_wall_offset_secs(reference_unix_ns: u64) -> i64 {
    let secs = (reference_unix_ns / 1_000_000_000) as i64;
    // SAFETY: `localtime_r` fills a zeroed `tm` we own; we only read `tm_gmtoff`.
    unsafe {
        let t = secs as libc::time_t;
        let mut tm: libc::tm = std::mem::zeroed();
        if libc::localtime_r(&t, &mut tm).is_null() {
            return 0;
        }
        tm.tm_gmtoff as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aorta_core::trace::chunk::{build_chunk, ChunkEventType, ChunkProvenance, RingRecord};
    use aorta_trace2perfetto::decode_trace;

    fn prov(node: &str, pid: u32) -> ChunkProvenance {
        ChunkProvenance {
            node_id: node.to_string(),
            run_id: "run".to_string(),
            pid,
            process_name: "proc".to_string(),
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
            trace_id: [3u8; 16],
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

    fn build(recs: &[RingRecord]) -> Vec<u8> {
        build_chunk(
            &prov("nodeA", 5),
            1_000,
            0,
            recs,
            |s| format!("scope{s}"),
            |e| format!("event{e}"),
            |t| (100 + t as u32, format!("thread{t}")),
        )
    }

    fn sample_state() -> WaterfallState {
        // Root [1000,5000] thread0; nested child [2000,3000] thread0; an async
        // wait [1500,2500] thread0; a span on thread1 [4000,6000].
        let mut async_b = rec(2, 3, 0, 9, 0, 1_500, ChunkEventType::SliceBegin, true);
        async_b.flags = aorta_core::trace::chunk::FLAG_ASYNC;
        let recs = vec![
            rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin, false),
            rec(1, 1, 0, 2, 1, 2_000, ChunkEventType::SliceBegin, false),
            rec(1, 1, 0, 2, 1, 3_000, ChunkEventType::SliceEnd, false),
            async_b,
            rec(2, 3, 0, 9, 0, 2_500, ChunkEventType::SliceEnd, true),
            rec(0, 0, 0, 1, 0, 5_000, ChunkEventType::SliceEnd, false),
            rec(0, 4, 1, 3, 0, 4_000, ChunkEventType::SliceBegin, false),
            rec(0, 4, 1, 3, 0, 6_000, ChunkEventType::SliceEnd, false),
        ];
        let decoded = decode_trace(&[build(&recs)]).unwrap();
        WaterfallState::new(decoded)
    }

    #[test]
    fn lanes_group_by_thread_and_segregate_async() {
        let st = sample_state();
        // thread0 sync, thread0 async, thread1 sync = 3 lanes.
        assert_eq!(st.lanes.len(), 3, "{:?}", st.lanes);
        assert_eq!(st.lanes.iter().filter(|l| l.is_async).count(), 1);
        let async_lane = st.lanes.iter().find(|l| l.is_async).unwrap();
        assert!(
            async_lane.thread_label.contains("[async]"),
            "async lane labeled distinctly: {}",
            async_lane.thread_label
        );
        assert_eq!(
            async_lane.spans.len(),
            1,
            "the async wait is on the async lane"
        );
    }

    #[test]
    fn nesting_depth_indents_child_under_parent() {
        let st = sample_state();
        let sync0 = st
            .lanes
            .iter()
            .find(|l| !l.is_async && l.tid == 100)
            .expect("thread0 sync lane");
        // root + child on thread0. The child must be depth 1 under the root.
        let depths: Vec<u16> = sync0.spans.iter().map(|s| s.depth).collect();
        assert!(depths.contains(&0) && depths.contains(&1), "{depths:?}");
    }

    #[test]
    fn zoom_in_shrinks_then_fit_restores() {
        let mut st = sample_state();
        let full = st.view_span_ns();
        st.zoom(0.5);
        assert!(st.view_span_ns() < full, "zoom in shrinks the window");
        st.fit();
        assert_eq!(st.view_span_ns(), full, "fit restores full extent");
    }

    #[test]
    fn zoom_at_keeps_the_anchor_pinned() {
        let mut st = sample_state();
        // Anchor at 25% into the window; after zooming in it should stay at the
        // same fraction (i.e. the time under the cursor doesn't run away).
        let before_span = st.view_span_ns();
        let anchor = st.view_start_ns + before_span / 4;
        st.zoom_at(0.5, anchor);
        let after_span = st.view_span_ns();
        assert!(after_span < before_span, "zoom in shrinks");
        let frac = (anchor.saturating_sub(st.view_start_ns)) as f64 / after_span as f64;
        assert!(
            (frac - 0.25).abs() < 0.05,
            "anchor stays ~25% across the window, got {frac}"
        );
    }

    #[test]
    fn counter_and_instant_points_become_tracks() {
        let recs = vec![
            RingRecord {
                mono_ns: 1_000,
                scope_id: 1,
                event_id: 0,
                thread_id: 0,
                event_type: ChunkEventType::Counter as u8,
                value: 2,
                ..RingRecord::default()
            },
            RingRecord {
                mono_ns: 2_000,
                scope_id: 1,
                event_id: 0,
                thread_id: 0,
                event_type: ChunkEventType::Counter as u8,
                value: 5,
                ..RingRecord::default()
            },
            RingRecord {
                mono_ns: 1_500,
                scope_id: 1,
                event_id: 1,
                thread_id: 0,
                event_type: ChunkEventType::Instant as u8,
                value: 0,
                ..RingRecord::default()
            },
        ];
        let st = WaterfallState::new(decode_trace(&[build(&recs)]).unwrap());
        assert_eq!(
            st.counter_tracks.len(),
            1,
            "one (scope,name) counter series"
        );
        let track = &st.counter_tracks[0];
        assert_eq!(track.samples.len(), 2);
        assert_eq!(track.max_value, 5);
        let (first_ts, _) = track.samples[0];
        let (last_ts, _) = track.samples[1];
        assert_eq!(track.value_at(first_ts), 2, "step holds the sampled value");
        assert_eq!(
            track.value_at(last_ts),
            5,
            "carried forward to the last sample"
        );
        assert_eq!(st.instants.len(), 1, "one instant event");
    }

    #[test]
    fn region_aggregate_rolls_up_intersecting_spans() {
        let mut st = sample_state();
        assert!(st.region_aggregate().is_none(), "no region → no aggregate");
        st.set_region(st.total_start_ns, st.total_end_ns);
        let agg = st.region_aggregate().expect("region set");
        assert_eq!(
            agg.count,
            st.trace.spans.len(),
            "full-extent region counts every span"
        );
        assert!(!agg.by_name.is_empty());
        for w in agg.by_name.windows(2) {
            assert!(w[0].total_ns >= w[1].total_ns, "sorted by total desc");
        }
    }

    #[test]
    fn search_finds_and_cycles_matches() {
        let mut st = sample_state();
        st.search_begin();
        for c in "event".chars() {
            st.search_input_push(c);
        }
        st.search_commit();
        assert!(
            !st.search_matches.is_empty(),
            "'event' matches the demo span names"
        );
        let first = st.selected;
        st.search_cycle(true);
        if st.search_matches.len() > 1 {
            assert_ne!(st.selected, first, "n cycles to the next match");
        }
        st.search_clear();
        assert!(st.search_query.is_none() && st.search_matches.is_empty());
    }

    #[test]
    fn search_highlights_only_the_committed_name_not_the_typed_query() {
        let mut st = sample_state();
        st.search_begin();
        for c in "event".chars() {
            st.search_input_push(c);
        }
        // 'event' fuzzy-matches several distinct names — the picker lists them.
        assert!(st.search_names.len() >= 2, "multiple names match 'event'");
        let picked = st
            .search_name_row(st.search_name_idx)
            .unwrap()
            .0
            .to_string();
        st.search_commit();
        assert_eq!(st.search_query.as_deref(), Some(picked.as_str()));

        // Every highlighted (lane,pos) is the committed NAME — not just any fuzzy
        // 'event' match.
        for &(li, pos) in &st.search_matches {
            assert_eq!(st.span_at((li, pos)).unwrap().name, picked);
        }
        // The highlight is strictly narrower than the typed-query fuzzy set: other
        // 'event' names exist but are not highlighted.
        let fuzzy_total = st
            .lanes
            .iter()
            .flat_map(|l| &l.spans)
            .filter(|ls| {
                st.trace
                    .spans
                    .get(ls.span_idx)
                    .is_some_and(|s| fuzzy_subsequence(&s.name, "event"))
            })
            .count();
        assert!(
            st.search_matches.len() < fuzzy_total,
            "highlight ({}) must be narrower than the fuzzy query ({fuzzy_total})",
            st.search_matches.len()
        );
    }

    #[test]
    fn caller_callee_indices_link_and_jump_across_lanes() {
        // root span1 (thread0); child span2 (thread0, nested); child span3
        // (thread1 — CROSS-lane), both parented to span1.
        let recs = vec![
            rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin, false),
            rec(0, 1, 0, 2, 1, 2_000, ChunkEventType::SliceBegin, false),
            rec(0, 1, 0, 2, 1, 3_000, ChunkEventType::SliceEnd, false),
            rec(0, 2, 1, 3, 1, 2_500, ChunkEventType::SliceBegin, false),
            rec(0, 2, 1, 3, 1, 4_000, ChunkEventType::SliceEnd, false),
            rec(0, 0, 0, 1, 0, 5_000, ChunkEventType::SliceEnd, false),
        ];
        let mut st = WaterfallState::new(decode_trace(&[build(&recs)]).unwrap());
        let root = st.trace.spans.iter().position(|s| s.span_id == 1).unwrap();
        assert_eq!(st.children[root].len(), 2, "root has two callees");
        for &c in &st.children[root] {
            assert_eq!(st.parent_of[c], Some(root), "callee points back to caller");
        }
        // Select root → jump to a callee → selection follows, and its caller is root.
        st.selected = st.span_to_slot[root];
        assert_eq!(st.selected_span_idx(), Some(root));
        let callee = st.callees_of_selected()[0];
        assert!(
            st.is_related_to_selection(callee),
            "callee is related to root"
        );
        st.jump_to_span_idx(callee);
        assert_eq!(st.selected_span_idx(), Some(callee));
        assert_eq!(
            st.caller_of_selected(),
            Some(root),
            "p would return to root"
        );
    }

    #[test]
    fn caller_state_distinguishes_root_resolved_and_unresolved() {
        // root span1; child span2 (parent present); orphan span7 (parent 99 absent).
        let recs = vec![
            rec(0, 0, 0, 1, 0, 1_000, ChunkEventType::SliceBegin, false),
            rec(0, 1, 0, 2, 1, 1_500, ChunkEventType::SliceBegin, false),
            rec(0, 1, 0, 2, 1, 1_800, ChunkEventType::SliceEnd, false),
            rec(0, 0, 0, 1, 0, 3_000, ChunkEventType::SliceEnd, false),
            rec(0, 2, 0, 7, 99, 2_000, ChunkEventType::SliceBegin, false),
            rec(0, 2, 0, 7, 99, 2_200, ChunkEventType::SliceEnd, false),
        ];
        let mut st = WaterfallState::new(decode_trace(&[build(&recs)]).unwrap());
        let idx = |id: u64| st.trace.spans.iter().position(|s| s.span_id == id).unwrap();
        let (root, child, orphan) = (idx(1), idx(2), idx(7));
        st.selected = st.span_to_slot[root];
        assert_eq!(st.caller_state(), Some(CallerState::Root));
        st.selected = st.span_to_slot[child];
        assert_eq!(st.caller_state(), Some(CallerState::Resolved(root)));
        st.selected = st.span_to_slot[orphan];
        assert_eq!(
            st.caller_state(),
            Some(CallerState::Unresolved(99)),
            "a set-but-missing parent is surfaced, not shown as root"
        );
    }

    #[test]
    fn group_by_scope_rebuckets_lanes_and_keeps_every_span_addressable() {
        let mut st = sample_state();
        let thread_lanes = st.lanes.len();
        st.set_group_mode(GroupMode::Scope);
        assert_eq!(st.group_mode, GroupMode::Scope);
        let labels: Vec<&str> = st.lanes.iter().map(|l| l.process_label.as_str()).collect();
        assert!(
            labels.contains(&"scope0") && labels.contains(&"scope1"),
            "scope lanes present: {labels:?}"
        );
        assert!(st.selected.is_some(), "a selection survives regrouping");
        for i in 0..st.trace.spans.len() {
            assert!(
                st.span_to_slot[i].is_some(),
                "span {i} still has a lane slot"
            );
        }
        st.set_group_mode(GroupMode::Thread);
        assert_eq!(st.lanes.len(), thread_lanes, "thread layout restored");
    }

    #[test]
    fn collapse_toggles_a_group_and_summarizes_it() {
        let mut st = sample_state();
        let label = st.lanes[0].process_label.clone();
        assert!(!st.is_label_collapsed(&label));
        st.toggle_collapse(&label);
        assert!(st.is_label_collapsed(&label));
        let (lanes, spans) = st.group_summary(&label);
        assert!(lanes >= 1 && spans >= 1, "summary counts the group");
        st.toggle_collapse(&label);
        assert!(!st.is_label_collapsed(&label), "toggles back open");
    }

    #[test]
    fn name_stats_computes_histogram_percentiles_and_jump_targets() {
        // 10 spans all named "event0" with durations 100,200,…,1000 (sequential).
        let mut recs = Vec::new();
        for i in 1..=10u64 {
            let start = 1_000 + i * 5_000;
            recs.push(rec(0, 0, 0, i, 0, start, ChunkEventType::SliceBegin, false));
            recs.push(rec(
                0,
                0,
                0,
                i,
                0,
                start + i * 100,
                ChunkEventType::SliceEnd,
                false,
            ));
        }
        let st = WaterfallState::new(decode_trace(&[build(&recs)]).unwrap());
        let s = st.name_stats("event0", 8).expect("same-name spans exist");
        assert_eq!(s.count, 10);
        assert_eq!(s.min_ns, 100);
        assert_eq!(s.max_ns, 1000);
        assert_eq!(s.buckets.iter().sum::<u32>(), 10, "every span is binned");
        let mark = |l: &str| s.marks.iter().find(|m| m.label == l).unwrap();
        assert_eq!(mark("p50").value_ns, 500);
        assert_eq!(mark("p90").value_ns, 900);
        assert_eq!(mark("p99").value_ns, 1000);
        assert_eq!(mark("max").value_ns, 1000);
        // The mark points at the actual span instance at that rank.
        assert_eq!(st.trace.spans[mark("max").span_idx].duration_ns(), 1000);
        assert_eq!(st.trace.spans[mark("p50").span_idx].duration_ns(), 500);
    }

    #[test]
    fn selecting_a_lane_below_the_fold_scrolls_down() {
        let mut st = sample_state();
        st.view_rows = 1; // a one-lane window
        st.v_scroll = 0;
        st.select_end(true); // G → last span, on the last lane
        let (lane_i, _) = st.selected.unwrap();
        assert!(
            st.v_scroll <= lane_i && lane_i < st.v_scroll + st.view_rows.max(1),
            "the selection is within the visible window after auto-scrolling down (v_scroll={}, lane={lane_i})",
            st.v_scroll
        );
    }

    #[test]
    fn fuzzy_subsequence_matches_in_order_case_insensitively() {
        assert!(fuzzy_subsequence("topic.callback", "tcb")); // subsequence
        assert!(fuzzy_subsequence("topic.callback", "callback")); // substring still ok
        assert!(fuzzy_subsequence("Topic.Callback", "tc")); // case-insensitive
        assert!(!fuzzy_subsequence("topic.callback", "xyz")); // missing chars
        assert!(!fuzzy_subsequence("abc", "cba")); // wrong order
        assert!(fuzzy_subsequence("anything", "")); // empty matches
    }

    #[test]
    fn pan_clamps_within_extent() {
        let mut st = sample_state();
        st.zoom(0.25);
        st.pan(-100.0); // pan way left, must clamp at total_start
        assert!(st.view_start_ns >= st.total_start_ns);
        st.pan(100.0); // pan way right, must clamp at total_end
        assert!(st.view_end_ns <= st.total_end_ns);
    }

    #[test]
    fn select_step_and_ends_traverse_all_spans() {
        let mut st = sample_state();
        st.select_end(false);
        let first = st.selected_span().unwrap().clone();
        st.select_end(true);
        let last = st.selected_span().unwrap().clone();
        assert_ne!(
            (first.pid, first.tid, first.span_id),
            (last.pid, last.tid, last.span_id),
            "g and G land on different spans"
        );
        // Stepping down from the first eventually reaches the last.
        st.select_end(false);
        for _ in 0..10 {
            st.select_step(true);
        }
        assert_eq!(
            st.selected_span().unwrap().span_id,
            last.span_id,
            "stepping down saturates at the last span"
        );
    }
}
