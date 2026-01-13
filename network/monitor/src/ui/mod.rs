//! UI rendering for the Tapedrive Network Monitor.
//!
//! This module contains the layout and rendering logic for the TUI.
//! The main entry point is the `draw` function which renders the appropriate
//! view based on the current application state.

pub mod dashboard;
pub mod epoch;
pub mod nodes;
pub mod popup;
pub mod tracks;
pub mod widgets;

pub use dashboard::Dashboard;
pub use epoch::render_epoch_history;
pub use nodes::render_node_list;
pub use popup::{render_help_popup, NodeDetailPopup};
pub use tracks::render_track_search;
pub use widgets::{EpochProgress, EventLog, NodeGrid, SpoolBar};

use ratatui::{buffer::Buffer, layout::Rect, Frame};

use crate::app::{App, View};
use crate::theme::{theme, Theme};

/// Main draw function - renders the current view to the terminal.
///
/// This is called on each frame to update the display.
pub fn draw(frame: &mut Frame, app: &App) {
    let theme = theme();
    let area = frame.area();

    // Render based on current view
    match &app.current_view {
        View::Dashboard => {
            render_dashboard(area, frame.buffer_mut(), app, theme);
        }
        View::NodeDetail(idx) => {
            // Render dashboard first, then overlay popup
            render_dashboard(area, frame.buffer_mut(), app, theme);
            if let Some(node) = app.nodes.get(*idx) {
                NodeDetailPopup::new(node, theme).render(area, frame.buffer_mut());
            }
        }
        View::Help => {
            // Render dashboard first, then overlay help
            render_dashboard(area, frame.buffer_mut(), app, theme);
            render_help_popup(area, frame.buffer_mut(), theme);
        }
        View::NodeList => {
            // Full-screen node list view
            render_node_list(area, frame.buffer_mut(), app, theme);
        }
        View::EpochHistory => {
            // Full-screen epoch history view
            render_epoch_history(area, frame.buffer_mut(), app, theme);
        }
        View::Search(query) => {
            // Full-screen search view
            render_track_search(area, frame.buffer_mut(), app, theme, query);
        }
    }
}

/// Render the main dashboard view.
fn render_dashboard(area: Rect, buf: &mut Buffer, app: &App, theme: &Theme) {
    Dashboard::new(app, theme).render(area, buf);
}

use ratatui::widgets::Widget;
