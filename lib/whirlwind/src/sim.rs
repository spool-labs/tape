//! Whirlwind latency simulation and plotting.
//!
//! Places a spool group at real cities, times one challenge round per the
//! mechanism using real ping latencies and the machine's measured crypto costs,
//! and renders the stacked latency histogram.

pub mod epoch;
pub mod export;
pub mod latency;
pub mod node;
pub mod plot;
pub mod project;
pub mod round;
pub mod schedule;
pub mod scoreboard;
pub mod timeline;
pub mod track;
