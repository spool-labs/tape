//! Tapedrive Network Monitor - Real-time TUI dashboard library.
//!
//! This crate provides the core components for the TUI network monitor:
//!
//! - [`app`] - Application state and data structures
//! - [`ui`] - UI widgets and rendering
//! - [`theme`] - Color theme definitions
//! - [`input`] - Keyboard input handling
//! - [`data`] - Data fetching and caching
//!
//! # Example Usage
//!
//! ```ignore
//! use tape_monitor::{app::App, ui, theme};
//! use ratatui::Frame;
//!
//! let mut app = App::new();
//! app.load_demo_data();
//!
//! // In your render loop:
//! terminal.draw(|f| ui::draw(f, &app))?;
//! ```

pub mod app;
pub mod data;
pub mod input;
pub mod theme;
pub mod ui;

pub use app::App;
pub use theme::theme;
