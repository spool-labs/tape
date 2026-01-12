//! Shared test utilities for RpcClient integration tests
//!
//! This module contains common setup code for tests that require a TestValidator.

use rpc_client::RpcClient;
use rpc_test::TestRpc;
use solana_sdk::bpf_loader_upgradeable;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_test_validator::{TestValidatorGenesis, UpgradeableProgramInfo};
use std::path::PathBuf;

/// Metaplex Token Metadata program ID
pub const MPL_TOKEN_METADATA_ID: Pubkey = pubkey!("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");

// =============================================================================
// Path Helpers
// =============================================================================

/// Get the workspace root directory
pub fn workspace_root() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Get the path to a deployed program .so file
pub fn program_path(name: &str) -> PathBuf {
    workspace_root()
        .join("target/deploy")
        .join(format!("{}.so", name))
}

/// Get the path to external program ELF files (from test/elfs/)
pub fn external_program_path(name: &str) -> PathBuf {
    workspace_root()
        .join("test/elfs")
        .join(format!("{}.so", name))
}

// =============================================================================
// Program Info Builders
// =============================================================================

/// Create an UpgradeableProgramInfo for loading a program from target/deploy/
pub fn program_info(name: &str, program_id: Pubkey) -> UpgradeableProgramInfo {
    UpgradeableProgramInfo {
        program_id,
        loader: bpf_loader_upgradeable::id(),
        upgrade_authority: Pubkey::default(),
        program_path: program_path(name),
    }
}

/// Create an UpgradeableProgramInfo for external programs (from test/elfs/)
pub fn external_program_info(name: &str, program_id: Pubkey) -> UpgradeableProgramInfo {
    UpgradeableProgramInfo {
        program_id,
        loader: bpf_loader_upgradeable::id(),
        upgrade_authority: Pubkey::default(),
        program_path: external_program_path(name),
    }
}

// =============================================================================
// Validator Setup
// =============================================================================

/// Helper to create a basic test validator (no custom programs).
/// Use this for testing the RPC layer itself.
#[allow(dead_code)]
pub async fn setup_basic_validator() -> (solana_test_validator::TestValidator, Keypair) {
    TestValidatorGenesis::default().start_async().await
}

/// Helper to create a test validator with all Tape programs loaded.
pub async fn setup_validator() -> (solana_test_validator::TestValidator, Keypair) {
    TestValidatorGenesis::default()
        .add_upgradeable_programs_with_path(&[
            // Our programs (from target/deploy/)
            program_info("tapedrive", tape_api::program::tapedrive::ID),
            program_info("token", tape_api::program::token::ID),
            program_info("exchange", tape_api::program::exchange::ID),
            program_info("staking", tape_api::program::staking::ID),
            // External programs (from test/elfs/)
            external_program_info("mpl_token_metadata", MPL_TOKEN_METADATA_ID),
        ])
        .start_async()
        .await
}

/// Helper to create RpcClient with TestRpc
pub fn create_client(validator: &solana_test_validator::TestValidator) -> RpcClient<TestRpc> {
    let rpc = TestRpc::new(validator);
    RpcClient::from_rpc(rpc)
}

// =============================================================================
// Validator Guard
// =============================================================================

/// Guard that ensures TestValidator is cleaned up without blocking.
/// TestValidator::drop() can hang waiting for threads to terminate,
/// so we spawn the drop in a detached background thread.
pub struct ValidatorGuard(Option<solana_test_validator::TestValidator>);

impl ValidatorGuard {
    pub fn new(validator: solana_test_validator::TestValidator) -> Self {
        Self(Some(validator))
    }

    pub fn validator(&self) -> &solana_test_validator::TestValidator {
        self.0.as_ref().unwrap()
    }
}

impl Drop for ValidatorGuard {
    fn drop(&mut self) {
        if let Some(v) = self.0.take() {
            // Spawn cleanup in a detached thread so we don't block.
            std::thread::spawn(move || drop(v));
        }
    }
}

// =============================================================================
// System Initialization
// =============================================================================

/// Helper to fully initialize the system (mint + system + expand + epoch/archive).
/// This handles the account size expansion needed for the System account.
pub async fn initialize_system(client: &RpcClient<TestRpc>, payer: &Keypair) {
    use tape_api::instruction::{
        build_create_system_ix, build_expand_system_ix, build_initialize_ix,
        build_initialize_mint_ix,
    };

    // Step 1: Initialize the TAPE token mint
    let mint_ix = build_initialize_mint_ix(payer.pubkey(), payer.pubkey());
    client
        .send_instructions(payer, vec![mint_ix])
        .await
        .expect("Failed to initialize mint");

    // Step 2: Create the System singleton (starts at ~10KB)
    let create_system_ix = build_create_system_ix(payer.pubkey(), payer.pubkey());
    client
        .send_instructions(payer, vec![create_system_ix])
        .await
        .expect("Failed to create system");

    // Step 3: Expand System account to full size
    // System is ~70KB, MAX_PERMITTED_DATA_INCREASE is 10KB per tx
    // Need multiple expand calls until the account reaches full size
    for _ in 0..10 {
        let expand_ix = build_expand_system_ix(payer.pubkey(), payer.pubkey());
        match client.send_instructions(payer, vec![expand_ix]).await {
            Ok(_) => {}
            Err(e) => {
                // AccountAlreadyInitialized means we've reached full size
                let err_str = format!("{:?}", e);
                if err_str.contains("AccountAlreadyInitialized")
                    || err_str.contains("already initialized")
                    || err_str.contains("uninitialized account")
                {
                    break;
                } else {
                    panic!("Expand failed unexpectedly: {:?}", e);
                }
            }
        }
    }

    // Step 4: Initialize Epoch and Archive
    let init_ix = build_initialize_ix(payer.pubkey(), payer.pubkey());
    client
        .send_instructions(payer, vec![init_ix])
        .await
        .expect("Failed to initialize epoch/archive");
}
