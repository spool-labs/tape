//! Tapedrive's blacklist producer

pub mod api;
pub mod chain;
pub mod cli;
pub mod config;
pub mod funder;
pub mod observe;
pub mod operator;
pub mod publisher;
pub mod records;
pub mod service;
pub mod types;
pub mod worker;

use std::process::ExitCode;

use clap::Parser;

use crate::cli::args::Cli;
use crate::cli::run::run;

#[tokio::main]
async fn main() -> ExitCode {
    run(Cli::parse()).await
}
