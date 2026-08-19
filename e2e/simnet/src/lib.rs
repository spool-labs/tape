//! Simnet harness scaffold for multi-node Tapedrive e2e tests.
//!
//! This crate intentionally starts as a lightweight skeleton:
//! - network builder + fixture APIs
//! - LiteSVM chain helper utilities
//! - a reel volume per node fixture
//! - runtime lifecycle controls

use std::future::Future;
use std::path::Path;

use anyhow::{Context, Result};
use tempfile::TempDir;

pub mod chain;
pub mod config;
pub mod fixtures;
pub mod gateway;
pub mod log;
pub mod node;
pub mod scenario;
pub mod simnet;
pub mod tls;

pub use chain::ChainFixture;
pub use config::{NodeRuntimeMode, SeededAccount, SimnetConfig};
pub use gateway::TestGateway;
pub use node::TestNode;
pub use scenario::SimnetScenario;
pub use simnet::{SimnetBuilder, SimnetHarness};

pub const SIMNET_TEST_STACK_SIZE: usize = 32 * 1024 * 1024;

/// Directory under the workspace target that holds this run's node volumes
const VOLUME_SUBDIR: &str = "simnet";

/// A fresh reel volume for one node or gateway, deleted when its owner drops
///
/// Under the workspace target rather than the system temp dir: a run that dies
/// mid-test leaves its volume behind, and there it is swept by `rm -rf target`
/// or `cargo clean` instead of sitting somewhere nobody thinks to look.
pub(crate) fn node_volume() -> Result<TempDir> {
    let workspace = ChainFixture::workspace_root_from_manifest(Path::new(env!(
        "CARGO_MANIFEST_DIR"
    )))?;
    let parent = workspace.join("target").join(VOLUME_SUBDIR);
    std::fs::create_dir_all(&parent)
        .with_context(|| format!("create {}", parent.display()))?;
    tempfile::tempdir_in(&parent)
        .with_context(|| format!("node volume under {}", parent.display()))
}

pub fn run_simnet_test<T, F>(test: T)
where
    T: FnOnce() -> F + Send + 'static,
    F: Future<Output = ()> + 'static,
{
    let thread = std::thread::Builder::new()
        .name("simnet-test".into())
        .stack_size(SIMNET_TEST_STACK_SIZE)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .thread_stack_size(SIMNET_TEST_STACK_SIZE)
                .enable_all()
                .build()
                .expect("build simnet test runtime")
                .block_on(test())
        })
        .expect("spawn simnet test thread");

    thread.join().expect("simnet test thread joins");
}
