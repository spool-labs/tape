//! Simnet harness scaffold for multi-node Tapedrive e2e tests.
//!
//! This crate intentionally starts as a lightweight skeleton:
//! - network builder + fixture APIs
//! - LiteSVM chain helper utilities
//! - in-memory node fixtures
//! - runtime lifecycle controls

use std::future::Future;

pub mod chain;
pub mod config;
pub mod fixtures;
pub mod log;
pub mod node;
pub mod scenario;
pub mod simnet;
pub mod tls;

pub use chain::ChainFixture;
pub use config::{NodeRuntimeMode, SeededAccount, SimnetConfig};
pub use node::TestNode;
pub use scenario::SimnetScenario;
pub use simnet::{SimnetBuilder, SimnetHarness};

pub const SIMNET_TEST_STACK_SIZE: usize = 32 * 1024 * 1024;

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
