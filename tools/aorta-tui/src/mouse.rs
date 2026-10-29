use ratatui::layout::Rect;
use ratatui_interact::traits::ClickRegionRegistry;

use crate::app::Screen;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MouseTarget {
    Tab(Screen),
    OverviewAction(usize),
    TopicRow(usize),
    ServiceRow(usize),
    ActionRow(usize),
    NodeRow(usize),
    ZenohRouterRow(usize),
    ServiceCallHintRow(usize),
    ActionCallHintRow(usize),
    BookmarkRow(usize),
    CommandPaletteAction(usize),
    EventTimelineRow(usize),
    LogStreamRow(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrollTarget {
    OverviewActions,
    TopicList,
    TopicPreview,
    ServiceList,
    ActionList,
    NodeList,
    ZenohRouterList,
    ServiceCallHintList,
    ServiceCallResponse,
    ActionCallHintList,
    ActionCallResponse,
    BookmarkList,
    CommandPaletteList,
    EventTimelineList,
    LogStreamList,
    DetailPanel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScrollRegion {
    area: Rect,
    target: ScrollTarget,
}

pub struct MouseUiState {
    click_regions: ClickRegionRegistry<MouseTarget>,
    scroll_regions: Vec<ScrollRegion>,
}

impl Default for MouseUiState {
    fn default() -> Self {
        Self {
            click_regions: ClickRegionRegistry::new(),
            scroll_regions: Vec::new(),
        }
    }
}

impl MouseUiState {
    pub fn clear(&mut self) {
        self.click_regions.clear();
        self.scroll_regions.clear();
    }

    pub fn register_click(&mut self, area: Rect, target: MouseTarget) {
        if area.width == 0 || area.height == 0 {
            return;
        }
        self.click_regions.register(area, target);
    }

    pub fn register_scroll(&mut self, area: Rect, target: ScrollTarget) {
        if area.width == 0 || area.height == 0 {
            return;
        }
        self.scroll_regions.push(ScrollRegion { area, target });
    }

    pub fn click_target(&self, column: u16, row: u16) -> Option<MouseTarget> {
        self.click_regions
            .regions()
            .iter()
            .rev()
            .find(|region| rect_contains(region.area, column, row))
            .map(|region| region.data)
    }

    pub fn scroll_target(&self, column: u16, row: u16) -> Option<ScrollTarget> {
        self.scroll_regions
            .iter()
            .rev()
            .find(|region| rect_contains(region.area, column, row))
            .map(|region| region.target)
    }
}

fn rect_contains(area: Rect, column: u16, row: u16) -> bool {
    column >= area.x
        && column < area.x.saturating_add(area.width)
        && row >= area.y
        && row < area.y.saturating_add(area.height)
}

#[cfg(test)]
mod tests {
    use super::{MouseTarget, MouseUiState, ScrollTarget};
    use crate::app::Screen;
    use ratatui::layout::Rect;

    #[test]
    fn click_registry_returns_registered_target() {
        let mut mouse = MouseUiState::default();
        mouse.register_click(Rect::new(10, 4, 12, 2), MouseTarget::Tab(Screen::Topics));

        assert_eq!(
            mouse.click_target(12, 4),
            Some(MouseTarget::Tab(Screen::Topics))
        );
        assert_eq!(mouse.click_target(9, 4), None);
    }

    #[test]
    fn latest_click_region_wins_on_overlap() {
        let mut mouse = MouseUiState::default();
        mouse.register_click(Rect::new(0, 0, 10, 2), MouseTarget::Tab(Screen::Overview));
        mouse.register_click(Rect::new(3, 0, 4, 2), MouseTarget::Tab(Screen::Topics));

        assert_eq!(
            mouse.click_target(4, 0),
            Some(MouseTarget::Tab(Screen::Topics))
        );
        assert_eq!(
            mouse.click_target(1, 0),
            Some(MouseTarget::Tab(Screen::Overview))
        );
    }

    #[test]
    fn latest_scroll_region_wins_on_overlap() {
        let mut mouse = MouseUiState::default();
        mouse.register_scroll(Rect::new(0, 0, 20, 10), ScrollTarget::TopicList);
        mouse.register_scroll(Rect::new(5, 2, 6, 4), ScrollTarget::TopicPreview);

        assert_eq!(mouse.scroll_target(6, 3), Some(ScrollTarget::TopicPreview));
        assert_eq!(mouse.scroll_target(2, 3), Some(ScrollTarget::TopicList));
        assert_eq!(mouse.scroll_target(25, 3), None);
    }
}
