//! Integration tests for TapeClient using TestValidator
//!
//! These tests start a local Solana test validator with the Tape programs loaded,
//! then exercise the TapeClient API against it.
//!
//! Run with: `cargo test -p rpc-test --test integration -- --ignored`
//!
//! Note: These tests are marked #[ignore] by default because they take ~2-5 seconds
//! each to start the test validator. Run them explicitly when testing integration.
//!
//! **Resource Requirements:** Test validators require significant RAM (8GB+ recommended).
//! On low-memory systems (<4GB), the validator may hang during startup while
//! "Waiting for fees to stabilize".

use bytemuck::Zeroable;
use rpc_test::TestRpc;
use solana_sdk::bpf_loader_upgradeable;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_test_validator::{TestValidatorGenesis, UpgradeableProgramInfo};
use tape_client::TapeClient;
use std::path::PathBuf;

// Note: Programs are loaded from target/deploy/ using manifest dir to find workspace root

/// Get the path to a deployed program .so file
fn program_path(name: &str) -> PathBuf {
    // CARGO_MANIFEST_DIR is client/rpc-test, go up to workspace root
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .parent() // client/
        .unwrap()
        .parent() // workspace root
        .unwrap()
        .join("target/deploy")
        .join(format!("{}.so", name))
}

/// Create an UpgradeableProgramInfo for loading a program
fn program_info(name: &str, program_id: Pubkey) -> UpgradeableProgramInfo {
    UpgradeableProgramInfo {
        program_id,
        loader: bpf_loader_upgradeable::id(),
        upgrade_authority: Pubkey::default(),
        program_path: program_path(name),
    }
}

/// Helper to create a test validator with all Tape programs loaded
async fn setup_validator() -> (solana_test_validator::TestValidator, Keypair) {
    TestValidatorGenesis::default()
        .add_upgradeable_programs_with_path(&[
            program_info("tapedrive", tape_api::program::tapedrive::ID),
            program_info("token", tape_api::program::token::ID),
            program_info("exchange", tape_api::program::exchange::ID),
            program_info("staking", tape_api::program::staking::ID),
        ])
        .start_async()
        .await
}

/// Helper to create TapeClient with TestRpc
fn create_client(validator: &solana_test_validator::TestValidator) -> TapeClient<TestRpc> {
    let rpc = TestRpc::new(validator);
    TapeClient::from_rpc(rpc)
}

// =============================================================================
// Basic RPC Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_get_slot() {
    let (validator, _payer) = setup_validator().await;
    let client = create_client(&validator);

    let slot = client.get_slot().await.unwrap();
    // slot is u64, so always >= 0; just verify we got a value
    assert!(slot < u64::MAX, "Should get a valid slot number");
}

#[tokio::test]
#[ignore]
async fn test_get_block() {
    let (validator, _payer) = setup_validator().await;
    let client = create_client(&validator);

    // Get current slot and fetch its block
    let _slot = client.get_slot().await.unwrap();

    // Note: The most recent slot might not be confirmed yet, so we try slot 0
    // which should always exist in a test validator
    let block = client.get_block(0).await;
    assert!(block.is_ok(), "Should fetch genesis block");
}

// =============================================================================
// System Initialization Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_initialize_system() {
    use tape_api::instruction::{build_create_system_ix, build_initialize_ix, build_initialize_mint_ix};

    let (validator, payer) = setup_validator().await;
    let client = create_client(&validator);

    // Step 1: Initialize the TAPE token mint
    let mint_ix = build_initialize_mint_ix(payer.pubkey());
    let result = client.send_instructions(&payer, vec![mint_ix]).await;
    assert!(result.is_ok(), "Failed to initialize mint: {:?}", result.err());

    // Step 2: Create the System singleton
    let create_system_ix = build_create_system_ix(payer.pubkey());
    let result = client.send_instructions(&payer, vec![create_system_ix]).await;
    assert!(result.is_ok(), "Failed to create system: {:?}", result.err());

    // Step 3: Initialize Epoch and Archive
    let init_ix = build_initialize_ix(payer.pubkey());
    let result = client.send_instructions(&payer, vec![init_ix]).await;
    assert!(result.is_ok(), "Failed to initialize: {:?}", result.err());

    // Step 4: Verify we can fetch the singleton accounts
    let system = client.get_system().await;
    assert!(system.is_ok(), "Failed to fetch System: {:?}", system.err());

    let epoch = client.get_epoch().await;
    assert!(epoch.is_ok(), "Failed to fetch Epoch: {:?}", epoch.err());

    let archive = client.get_archive().await;
    assert!(archive.is_ok(), "Failed to fetch Archive: {:?}", archive.err());
}

// =============================================================================
// Node Registration Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_register_node() {
    use tape_api::instruction::{
        build_create_system_ix, build_initialize_ix, build_initialize_mint_ix,
        build_register_node_ix,
    };
    use tape_api::utils::to_name;
    use tape_core::prelude::*;

    let (validator, payer) = setup_validator().await;
    let client = create_client(&validator);

    // Initialize the system first
    let mint_ix = build_initialize_mint_ix(payer.pubkey());
    let create_system_ix = build_create_system_ix(payer.pubkey());
    let init_ix = build_initialize_ix(payer.pubkey());

    client.send_instructions(&payer, vec![mint_ix]).await.unwrap();
    client.send_instructions(&payer, vec![create_system_ix]).await.unwrap();
    client.send_instructions(&payer, vec![init_ix]).await.unwrap();

    // Create a node keypair
    let node_authority = Keypair::new();

    // Fund the node authority (transfer some SOL from payer)
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &node_authority.pubkey(),
        1_000_000_000, // 1 SOL
    );
    client.send_instructions(&payer, vec![transfer_ix]).await.unwrap();

    // Register a node
    let name = to_name("test-node");
    let commission_rate = BasisPoints(500); // 5%
    let network_address = NetworkAddress::zeroed();
    let network_tls = solana_sdk::pubkey::Pubkey::new_unique();
    // Note: In production you'd use real BLS keys. For testing, zeroed values work
    // as long as the program doesn't validate the PoP (which it might skip in dev mode).
    let bls_pubkey: BlsPubkey = Zeroable::zeroed();
    let bls_pop: BlsSignature = Zeroable::zeroed();

    let register_ix = build_register_node_ix(
        node_authority.pubkey(),
        name,
        commission_rate,
        network_address,
        network_tls,
        bls_pubkey,
        bls_pop,
    );

    let result = client.send_instructions(&node_authority, vec![register_ix]).await;
    assert!(result.is_ok(), "Failed to register node: {:?}", result.err());

    // Verify we can fetch the node
    let node = client.get_node(&node_authority.pubkey()).await;
    assert!(node.is_ok(), "Failed to fetch Node: {:?}", node.err());

    let node = node.unwrap();
    assert_eq!(node.metadata.name, name, "Node name should match");
}

// =============================================================================
// Account Discovery Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_get_all_nodes() {
    use tape_api::instruction::{
        build_create_system_ix, build_initialize_ix, build_initialize_mint_ix,
        build_register_node_ix,
    };
    use tape_api::utils::to_name;
    use tape_core::prelude::*;

    let (validator, payer) = setup_validator().await;
    let client = create_client(&validator);

    // Initialize the system
    let mint_ix = build_initialize_mint_ix(payer.pubkey());
    let create_system_ix = build_create_system_ix(payer.pubkey());
    let init_ix = build_initialize_ix(payer.pubkey());

    client.send_instructions(&payer, vec![mint_ix]).await.unwrap();
    client.send_instructions(&payer, vec![create_system_ix]).await.unwrap();
    client.send_instructions(&payer, vec![init_ix]).await.unwrap();

    // Register multiple nodes
    for i in 0..3 {
        let node_authority = Keypair::new();

        // Fund the node
        let transfer_ix = solana_sdk::system_instruction::transfer(
            &payer.pubkey(),
            &node_authority.pubkey(),
            1_000_000_000,
        );
        client.send_instructions(&payer, vec![transfer_ix]).await.unwrap();

        // Register
        let name = to_name(&format!("node-{}", i));
        let register_ix = build_register_node_ix(
            node_authority.pubkey(),
            name,
            BasisPoints(500),
            NetworkAddress::zeroed(),
            solana_sdk::pubkey::Pubkey::new_unique(),
            Zeroable::zeroed(),
            Zeroable::zeroed(),
        );
        client.send_instructions(&node_authority, vec![register_ix]).await.unwrap();
    }

    // Fetch all nodes
    let nodes = client.get_all_nodes().await;
    assert!(nodes.is_ok(), "Failed to fetch all nodes: {:?}", nodes.err());

    let nodes = nodes.unwrap();
    assert_eq!(nodes.len(), 3, "Should have 3 registered nodes");
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_fetch_nonexistent_account() {
    let (validator, _payer) = setup_validator().await;
    let client = create_client(&validator);

    // Try to fetch System account before initialization
    let result = client.get_system().await;
    assert!(result.is_err(), "Should fail to fetch uninitialized System");
}

#[tokio::test]
#[ignore]
async fn test_transaction_insufficient_funds() {
    let (validator, _payer) = setup_validator().await;
    let client = create_client(&validator);

    // Create a new keypair with no funds
    let broke_user = Keypair::new();

    // Try to send a transaction
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &broke_user.pubkey(),
        &solana_sdk::pubkey::Pubkey::new_unique(),
        1_000_000,
    );

    let result = client.send_instructions(&broke_user, vec![transfer_ix]).await;
    assert!(result.is_err(), "Should fail due to insufficient funds");
}

// =============================================================================
// Concurrent Operations Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_concurrent_reads() {
    use tape_api::instruction::{build_create_system_ix, build_initialize_ix, build_initialize_mint_ix};

    let (validator, payer) = setup_validator().await;
    let client = create_client(&validator);

    // Initialize system
    let mint_ix = build_initialize_mint_ix(payer.pubkey());
    let create_system_ix = build_create_system_ix(payer.pubkey());
    let init_ix = build_initialize_ix(payer.pubkey());

    client.send_instructions(&payer, vec![mint_ix]).await.unwrap();
    client.send_instructions(&payer, vec![create_system_ix]).await.unwrap();
    client.send_instructions(&payer, vec![init_ix]).await.unwrap();

    // Perform concurrent reads
    let (system, epoch, archive, slot) = tokio::join!(
        client.get_system(),
        client.get_epoch(),
        client.get_archive(),
        client.get_slot(),
    );

    assert!(system.is_ok(), "Concurrent System fetch failed");
    assert!(epoch.is_ok(), "Concurrent Epoch fetch failed");
    assert!(archive.is_ok(), "Concurrent Archive fetch failed");
    assert!(slot.is_ok(), "Concurrent slot fetch failed");
}
