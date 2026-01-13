//! Keyboard input handling for the Tapedrive Network Monitor.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// Actions that can be triggered by keyboard input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Quit the application.
    Quit,
    /// Force refresh data.
    Refresh,
    /// Navigate up (previous item).
    Up,
    /// Navigate down (next item).
    Down,
    /// Navigate left.
    Left,
    /// Navigate right.
    Right,
    /// Select current item / confirm.
    Select,
    /// Go back / close popup.
    Back,
    /// Switch to dashboard view.
    Dashboard,
    /// Switch to node list view.
    NodeList,
    /// Switch to epoch history view.
    EpochHistory,
    /// Open track search.
    TrackSearch,
    /// Open search.
    Search,
    /// Show help.
    Help,
    /// Page up.
    PageUp,
    /// Page down.
    PageDown,
    /// Go to first item.
    Home,
    /// Go to last item.
    End,
    /// Toggle auto-scroll in event log.
    ToggleAutoScroll,
    /// No action (key not mapped).
    None,
}

/// Handle a key event and return the corresponding action.
pub fn handle_key_event(key: KeyEvent) -> Action {
    // Check for Ctrl+C first (universal quit)
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return Action::Quit;
    }

    match key.code {
        // Quit
        KeyCode::Char('q') | KeyCode::Char('Q') => Action::Quit,

        // Refresh
        KeyCode::Char('r') | KeyCode::Char('R') => Action::Refresh,

        // Navigation
        KeyCode::Up | KeyCode::Char('k') => Action::Up,
        KeyCode::Down | KeyCode::Char('j') => Action::Down,
        KeyCode::Left | KeyCode::Char('h') => Action::Left,
        KeyCode::Right | KeyCode::Char('l') => Action::Right,

        // Selection
        KeyCode::Enter => Action::Select,
        KeyCode::Esc => Action::Back,

        // Views
        KeyCode::Char('d') | KeyCode::Char('D') => Action::Dashboard,
        KeyCode::Char('n') | KeyCode::Char('N') => Action::NodeList,
        KeyCode::Char('e') | KeyCode::Char('E') => Action::EpochHistory,
        KeyCode::Char('t') | KeyCode::Char('T') => Action::TrackSearch,

        // Search and help
        KeyCode::Char('/') => Action::Search,
        KeyCode::Char('?') => Action::Help,

        // Pagination
        KeyCode::PageUp => Action::PageUp,
        KeyCode::PageDown => Action::PageDown,
        KeyCode::Home => Action::Home,
        KeyCode::End => Action::End,

        // Toggle auto-scroll
        KeyCode::Char('a') | KeyCode::Char('A') => Action::ToggleAutoScroll,

        // Tab for cycling through panels (treat as right navigation)
        KeyCode::Tab => Action::Right,
        KeyCode::BackTab => Action::Left,

        _ => Action::None,
    }
}

/// Input mode for different view states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InputMode {
    /// Normal navigation mode.
    #[default]
    Normal,
    /// Search input mode (typing search query).
    Search,
}

impl InputMode {
    /// Check if we're in search input mode.
    pub fn is_search(&self) -> bool {
        matches!(self, InputMode::Search)
    }
}

/// Handle keyboard input and update application state.
/// This is the main input handler called from the event loop.
pub fn handle_input(app: &mut crate::app::App, key: KeyEvent) {
    use crate::app::{View, NodeSortOrder, NodeFilter};

    // Handle view-specific keys first
    if app.current_view == View::NodeList {
        match key.code {
            // Sort keys
            KeyCode::Char('s') | KeyCode::Char('S') => {
                app.node_sort = NodeSortOrder::Stake;
                return;
            }
            KeyCode::Char('n') | KeyCode::Char('N') => {
                app.node_sort = NodeSortOrder::Name;
                return;
            }
            KeyCode::Char('l') | KeyCode::Char('L') => {
                app.node_sort = NodeSortOrder::Latency;
                return;
            }
            KeyCode::Char('c') | KeyCode::Char('C') => {
                app.node_sort = NodeSortOrder::Commission;
                return;
            }
            KeyCode::Char('p') | KeyCode::Char('P') => {
                app.node_sort = NodeSortOrder::Spools;
                return;
            }
            // Filter keys
            KeyCode::Char('o') | KeyCode::Char('O') => {
                app.node_filter = NodeFilter::Online;
                return;
            }
            KeyCode::Char('a') | KeyCode::Char('A') => {
                app.node_filter = NodeFilter::All;
                return;
            }
            KeyCode::Char('f') | KeyCode::Char('F') => {
                app.node_filter = NodeFilter::Offline;
                return;
            }
            _ => {}
        }
    }

    let action = handle_key_event(key);

    match action {
        Action::Quit => {
            // Handled in main loop
        }
        Action::Refresh => {
            // Force refresh - handled in main loop
        }
        Action::Up => {
            app.select_prev();
        }
        Action::Down => {
            app.select_next();
        }
        Action::Left | Action::Right => {
            // Panel navigation - not yet implemented
        }
        Action::Select => {
            // Open node detail if a node is selected
            if let Some(idx) = app.selected_node {
                app.current_view = View::NodeDetail(idx);
            }
        }
        Action::Back => {
            match &app.current_view {
                View::Dashboard => {}
                _ => app.current_view = View::Dashboard,
            }
            app.clear_selection();
        }
        Action::Dashboard => {
            app.current_view = View::Dashboard;
        }
        Action::NodeList => {
            app.current_view = View::NodeList;
        }
        Action::EpochHistory => {
            app.current_view = View::EpochHistory;
        }
        Action::TrackSearch => {
            app.current_view = View::Search(String::new());
        }
        Action::Search => {
            app.current_view = View::Search(String::new());
        }
        Action::Help => {
            app.current_view = View::Help;
        }
        Action::PageUp => {
            app.scroll_offset = app.scroll_offset.saturating_sub(10);
        }
        Action::PageDown => {
            app.scroll_offset = app.scroll_offset.saturating_add(10);
        }
        Action::Home => {
            app.scroll_offset = 0;
        }
        Action::End => {
            app.scroll_offset = app.nodes.len().saturating_sub(1);
        }
        Action::ToggleAutoScroll => {
            app.event_auto_scroll = !app.event_auto_scroll;
        }
        Action::None => {}
    }
}

/// Handle key event in search mode.
/// Returns the action and optionally a character to add to the search query.
pub fn handle_search_key_event(key: KeyEvent) -> (Action, Option<char>) {
    match key.code {
        // Exit search
        KeyCode::Esc => (Action::Back, None),
        KeyCode::Enter => (Action::Select, None),

        // Character input
        KeyCode::Char(c) => (Action::None, Some(c)),

        // Backspace handled by the app
        KeyCode::Backspace => (Action::None, None),

        // Navigation still works
        KeyCode::Up => (Action::Up, None),
        KeyCode::Down => (Action::Down, None),

        _ => (Action::None, None),
    }
}
