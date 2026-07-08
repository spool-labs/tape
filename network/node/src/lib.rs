/// Identifies this build in logs and stats: the Cargo version plus a short
/// git sha stamped at compile time.
pub const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    "+",
    env!("TAPE_BUILD_SHA"),
    env!("TAPE_BUILD_SUFFIX"),
);

pub mod chain;
pub mod config;
pub mod context;
pub mod core;
pub mod features;
#[cfg(feature = "metrics")]
pub mod observe;
pub mod runtime;
pub mod supervisor;

#[cfg(test)]
pub mod harness;
