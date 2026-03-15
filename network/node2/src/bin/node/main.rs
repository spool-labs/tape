use std::process::ExitCode;

use node::core::config::AppConfig;
use node::core::runtime::{build_runtime, init_tracing, run_application};

fn main() -> ExitCode {
    if let Err(error) = init_tracing() {
        eprintln!("tracing initialization failed: {error}");
        return ExitCode::FAILURE;
    }

    let config = match AppConfig::production() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("configuration failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    let runtime = match build_runtime(&config.runtime) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("runtime build failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run_application(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("application failed: {error}");
            ExitCode::FAILURE
        }
    }
}
