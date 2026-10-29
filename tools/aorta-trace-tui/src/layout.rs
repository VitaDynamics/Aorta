//! Render geometry captured each frame, used for mouse hit-testing. Built by
//! [`crate::ui::render`] (the single source of truth for placement) and consumed
//! by mouse handling. Pure: no terminal, no I/O — fully unit-testable.

/// A clickable action button in the region panel (mouse parity for `z`/`F`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionButton {
    /// Zoom the viewport to the region bounds (key `z`).
    Zoom,
    /// Begin a region-panel fuzzy filter (key `F`).
    Filter,
}

/// One drawn span bar's screen rect plus the lane slot it came from, so a click
/// maps straight back to a selection without re-deriving the projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BarRect {
    pub x: u16,
    pub y: u16,
    pub w: u16,
    pub lane_idx: usize,
    pub pos: usize,
}

/// Geometry of the last-rendered waterfall: the track viewport rectangle, the
/// time window it shows, and every drawn bar. Default = empty (no track), so
/// mouse handling no-ops on the empty state.
#[derive(Debug, Clone, Default)]
pub struct Layout {
    /// Left edge of the track area (first column right of the gutter).
    pub track_x: u16,
    /// First lane row (inclusive) — directly under the time axis.
    pub track_y0: u16,
    /// Bottom of the track area (exclusive).
    pub track_y1: u16,
    /// Track width in columns (0 ⇒ nothing rendered / empty state).
    pub track_w: u16,
    pub view_start_ns: u64,
    pub view_span_ns: u64,
    pub bars: Vec<BarRect>,
    /// Left edge of the detail pane (0 ⇒ no detail pane shown).
    pub detail_x0: u16,
    /// Clickable caller/callee rows in the detail pane: `(screen_row, span_idx)`.
    pub detail_hits: Vec<(u16, usize)>,
    /// Minimap strip geometry, for click-to-navigate over the full extent.
    pub minimap_y: u16,
    pub minimap_x0: u16,
    pub minimap_w: u16,
    pub minimap_total_start_ns: u64,
    pub minimap_extent_ns: u64,
    /// Clickable group-header rows in the gutter: `(screen_row, group_label)` —
    /// click to collapse/expand.
    pub gutter_hits: Vec<(u16, String)>,
    /// Clickable action buttons in the region panel: `(screen_row, button)`.
    pub region_buttons: Vec<(u16, RegionButton)>,
    /// Number of by-name entries the region panel can show at once (one page),
    /// so the scroll handlers clamp to a full last page rather than past it.
    pub region_page: usize,
    /// The minimap's wall-clock readout row (row 0 of the strip) — click to
    /// toggle the clock (mouse parity for `t`). 0 ⇒ not rendered.
    pub minimap_readout_y: u16,
    /// The legend's group-mode chip rect `(row, x0, x1_exclusive)` — click to
    /// cycle thread→scope→name (mouse parity for `1`/`2`/`3`). `None` ⇒ absent.
    pub group_chip: Option<(u16, u16, u16)>,
}

impl Layout {
    /// True when `(col,row)` falls inside the timeline track area (not the
    /// gutter, axis, or detail pane).
    pub fn in_track(&self, col: u16, row: u16) -> bool {
        self.track_w > 0
            && col >= self.track_x
            && col < self.track_x.saturating_add(self.track_w)
            && row >= self.track_y0
            && row < self.track_y1
    }

    /// Epoch-relative ns under a screen column (clamped into the track). `None`
    /// outside the track or when there is no viewport. Inverse of
    /// [`crate::ui::project_bar`].
    pub fn col_to_ns(&self, col: u16) -> Option<u64> {
        if self.track_w == 0 || self.view_span_ns == 0 || col < self.track_x {
            return None;
        }
        let rel = (col - self.track_x).min(self.track_w - 1) as u128;
        let ns =
            self.view_start_ns as u128 + rel * self.view_span_ns as u128 / self.track_w as u128;
        Some(ns as u64)
    }

    /// The `(lane_idx, pos)` of the bar under a screen cell, if any. Bars on one
    /// row never overlap horizontally (a proper call stack), so the first match
    /// is unambiguous.
    pub fn hit_span(&self, col: u16, row: u16) -> Option<(usize, usize)> {
        self.bars
            .iter()
            .find(|b| row == b.y && col >= b.x && col < b.x.saturating_add(b.w))
            .map(|b| (b.lane_idx, b.pos))
    }

    /// The `span_idx` of the clickable caller/callee row under a cell in the
    /// detail pane, if any.
    pub fn detail_hit(&self, col: u16, row: u16) -> Option<usize> {
        if self.detail_x0 == 0 || col < self.detail_x0 {
            return None;
        }
        self.detail_hits
            .iter()
            .find(|(r, _)| *r == row)
            .map(|(_, idx)| *idx)
    }

    /// The epoch-relative time under a cell in the minimap strip, if the cell is
    /// on the strip's row (the initial click test).
    pub fn minimap_hit(&self, col: u16, row: u16) -> Option<u64> {
        if row != self.minimap_y
            || self.minimap_w == 0
            || col < self.minimap_x0
            // Upper bound: the detail pane's top row shares `minimap_y` and starts
            // exactly where the strip ends — don't hijack clicks there.
            || col >= self.minimap_x0.saturating_add(self.minimap_w)
        {
            return None;
        }
        self.minimap_col_to_ns(col)
    }

    /// The epoch-relative time at a minimap column, clamped to the strip and
    /// row-agnostic (a drag may wander off the strip row).
    pub fn minimap_col_to_ns(&self, col: u16) -> Option<u64> {
        if self.minimap_w == 0 {
            return None;
        }
        let col = col.clamp(self.minimap_x0, self.minimap_x0 + self.minimap_w - 1);
        let rel = (col - self.minimap_x0) as u128;
        Some(
            self.minimap_total_start_ns
                + (rel * self.minimap_extent_ns as u128 / self.minimap_w as u128) as u64,
        )
    }

    /// The group label of the gutter header row under a cell (left of the track),
    /// if any — clicking it collapses/expands that group.
    pub fn gutter_hit(&self, col: u16, row: u16) -> Option<&str> {
        if col >= self.track_x {
            return None;
        }
        self.gutter_hits
            .iter()
            .find(|(r, _)| *r == row)
            .map(|(_, l)| l.as_str())
    }

    /// True when `(col,row)` is inside the right-hand detail/region pane.
    pub fn in_detail(&self, col: u16) -> bool {
        self.detail_x0 != 0 && col >= self.detail_x0
    }

    /// The region-panel action button under a cell, if any (detail pane only).
    pub fn region_button_hit(&self, col: u16, row: u16) -> Option<RegionButton> {
        if !self.in_detail(col) {
            return None;
        }
        self.region_buttons
            .iter()
            .find(|(r, _)| *r == row)
            .map(|(_, b)| *b)
    }

    /// True when `(col,row)` is on the minimap wall-clock readout row (the strip's
    /// top row, left of the detail pane) — a click toggles the clock. Guarded on
    /// `minimap_w` (the strip's rendered flag) since the readout row may be row 0.
    pub fn minimap_readout_hit(&self, col: u16, row: u16) -> bool {
        self.minimap_w != 0 && row == self.minimap_readout_y && !self.in_detail(col)
    }

    /// True when `(col,row)` is on the legend's group-mode chip — a click cycles
    /// the grouping.
    pub fn group_chip_hit(&self, col: u16, row: u16) -> bool {
        match self.group_chip {
            Some((r, x0, x1)) => row == r && col >= x0 && col < x1,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout() -> Layout {
        Layout {
            track_x: 10,
            track_y0: 2,
            track_y1: 12,
            track_w: 100,
            view_start_ns: 0,
            view_span_ns: 1_000,
            bars: vec![
                BarRect {
                    x: 10,
                    y: 3,
                    w: 20,
                    lane_idx: 0,
                    pos: 0,
                },
                BarRect {
                    x: 40,
                    y: 4,
                    w: 5,
                    lane_idx: 0,
                    pos: 1,
                },
            ],
            detail_x0: 120,
            detail_hits: vec![(5, 7), (6, 9)],
            minimap_y: 1,
            minimap_x0: 10,
            minimap_w: 100,
            minimap_total_start_ns: 0,
            minimap_extent_ns: 1_000,
            gutter_hits: vec![(3, "procA".to_string())],
            region_buttons: vec![(2, RegionButton::Zoom), (3, RegionButton::Filter)],
            region_page: 4,
            minimap_readout_y: 0,
            group_chip: Some((22, 1, 12)),
        }
    }

    #[test]
    fn col_to_ns_inverts_projection() {
        let l = layout();
        // Left edge of the track maps to view_start.
        assert_eq!(l.col_to_ns(10), Some(0));
        // Halfway across (col 60 = track_x+50) → ~half the window.
        assert_eq!(l.col_to_ns(60), Some(500));
        // Gutter / out of track → None.
        assert_eq!(l.col_to_ns(5), None);
    }

    #[test]
    fn hit_span_finds_the_bar_under_the_cell() {
        let l = layout();
        assert_eq!(l.hit_span(15, 3), Some((0, 0)));
        assert_eq!(l.hit_span(42, 4), Some((0, 1)));
        // Right of the bar, same row → miss.
        assert_eq!(l.hit_span(46, 4), None);
        // Empty cell → miss.
        assert_eq!(l.hit_span(80, 7), None);
    }

    #[test]
    fn detail_hit_maps_row_to_span_in_the_pane() {
        let l = layout();
        assert_eq!(l.detail_hit(130, 5), Some(7)); // caller row
        assert_eq!(l.detail_hit(130, 6), Some(9)); // callee row
        assert_eq!(l.detail_hit(130, 8), None); // non-clickable row
        assert_eq!(l.detail_hit(50, 5), None); // left of the pane (in the track)
        assert_eq!(Layout::default().detail_hit(0, 0), None); // no pane
    }

    #[test]
    fn gutter_hit_maps_header_rows_left_of_the_track() {
        let l = layout();
        assert_eq!(l.gutter_hit(2, 3), Some("procA")); // gutter (col < track_x=10)
        assert_eq!(l.gutter_hit(2, 4), None); // non-header row
        assert_eq!(l.gutter_hit(15, 3), None); // inside the track, not the gutter
    }

    #[test]
    fn minimap_hit_maps_column_to_full_extent_time() {
        let l = layout();
        assert_eq!(l.minimap_hit(10, 1), Some(0)); // left edge → extent start
        assert_eq!(l.minimap_hit(60, 1), Some(500)); // halfway → mid extent
        assert_eq!(l.minimap_hit(109, 1), Some(990)); // last strip column
        assert_eq!(l.minimap_hit(60, 2), None); // wrong row
        assert_eq!(l.minimap_hit(5, 1), None); // left of the strip
        assert_eq!(l.minimap_hit(110, 1), None); // right of the strip (detail pane)
    }

    #[test]
    fn in_track_bounds() {
        let l = layout();
        assert!(l.in_track(10, 2));
        assert!(l.in_track(109, 11));
        assert!(!l.in_track(9, 5)); // gutter
        assert!(!l.in_track(50, 12)); // below
        assert!(!Layout::default().in_track(0, 0)); // empty
    }

    #[test]
    fn in_detail_is_the_right_hand_pane() {
        let l = layout();
        assert!(l.in_detail(120)); // detail_x0
        assert!(l.in_detail(140));
        assert!(!l.in_detail(119)); // left of the pane
        assert!(!Layout::default().in_detail(0)); // no pane
    }

    #[test]
    fn region_button_hit_maps_pane_rows_to_buttons() {
        let l = layout();
        assert_eq!(l.region_button_hit(125, 2), Some(RegionButton::Zoom));
        assert_eq!(l.region_button_hit(125, 3), Some(RegionButton::Filter));
        assert_eq!(l.region_button_hit(125, 9), None); // non-button row
        assert_eq!(l.region_button_hit(50, 2), None); // left of the pane (track)
    }

    #[test]
    fn minimap_readout_hit_is_row0_left_of_the_pane() {
        let l = layout(); // minimap_readout_y = 0, minimap_w = 100
        assert!(l.minimap_readout_hit(3, 0)); // gutter side of the readout row
        assert!(l.minimap_readout_hit(60, 0)); // over the strip
        assert!(!l.minimap_readout_hit(125, 0)); // in the detail pane
        assert!(!l.minimap_readout_hit(60, 1)); // density row, not the readout
        assert!(!Layout::default().minimap_readout_hit(0, 0)); // strip not rendered
    }

    #[test]
    fn group_chip_hit_covers_only_the_chip_rect() {
        let l = layout(); // chip at (row 22, x0 1, x1 12)
        assert!(l.group_chip_hit(1, 22));
        assert!(l.group_chip_hit(11, 22));
        assert!(!l.group_chip_hit(12, 22)); // x1 is exclusive
        assert!(!l.group_chip_hit(5, 21)); // wrong row
        assert!(!Layout::default().group_chip_hit(0, 0)); // no chip
    }
}
