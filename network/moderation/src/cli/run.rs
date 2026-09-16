//! Dispatching a parsed command

use std::process::ExitCode;

use tape_blacklist::entry::Entry;
use tracing::error;
use tracing_subscriber::fmt;
use tracing_subscriber::EnvFilter;

use crate::cli::args::{AnyTarget, Cli, Command, TapeTarget, TrackTarget};
use crate::cli::error::CliError;
use crate::config::settings::Config;
use crate::operator::commands::{create_rack, queue_decision, restore, status};
use crate::service::serve::serve;
use crate::types::target::Target;

const DEFAULT_LOG_LEVEL: &str = "info";

pub async fn run(cli: Cli) -> ExitCode {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_LEVEL));
    fmt().with_env_filter(filter).init();

    match execute(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(failure) => {
            error!(error = %failure, "command failed");
            ExitCode::FAILURE
        }
    }
}

async fn execute(cli: Cli) -> Result<(), CliError> {
    let path = cli.config.ok_or(CliError::MissingConfig)?;
    let config = Config::load(&path)?;
    match cli.command {
        Command::Serve => serve(config).await?,
        Command::Remove { target: TrackTarget::Track { address } } => {
            queue_decision(&config, Entry::remove(address)).await?
        }
        Command::Unlist { target: TapeTarget::Tape { address } } => {
            queue_decision(&config, Entry::unlist(address)).await?
        }
        Command::Restore { target } => restore(&config, restore_target(target)).await?,
        Command::CreateRack => create_rack(&config).await?,
        Command::Status => status(&config).await?,
    }
    Ok(())
}

fn restore_target(target: AnyTarget) -> Target {
    match target {
        AnyTarget::Track { address } => Target::track(address),
        AnyTarget::Tape { address } => Target::tape(address),
    }
}
