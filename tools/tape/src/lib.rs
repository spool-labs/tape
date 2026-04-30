pub mod cassette;
pub mod commands;
pub mod context;
pub mod error;
pub mod metrics;

// Shared plumbing comes from tape-cli-common. Re-export the modules and
// types the commands/main use so paths stay short and internal modules
// don't need to know about the common crate directly.
pub use tape_cli_common::{cluster, config, output};
pub use tape_cli_common::{
    CliConfig, CliOutput, GlobalArgs, KeyValue, OkMessage, OutputFormat, emit,
};

pub use context::Context;
pub use error::{Error, Result};
