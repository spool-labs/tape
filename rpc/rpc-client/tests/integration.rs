//! Integration tests for RpcClient using TestValidator
//!
//! These tests start a local Solana test validator with the Tape programs loaded,
//! then exercise the RpcClient API against it.
//!
//! ## Test Categories
//!
//! - **Basic RPC tests** (`test_get_slot`): Test the RPC layer
//!   without loading custom programs. These validate that `TestRpc` works correctly.
//!
//! - **Full integration tests** (`test_initialize_system`, etc.): Test business logic
//!   with all Tape programs loaded.
//!
//! ## Running Tests
//!
//! Basic RPC tests (no external programs needed):
//! ```bash
//! cargo test -p rpc-client --test integration -- --ignored test_get
//! cargo test -p rpc-client --test integration -- --ignored test_fetch
//! cargo test -p rpc-client --test integration -- --ignored test_transaction
//! ```
//!
//! Full integration tests (run individually to avoid memory issues):
//! ```bash
//! cargo test -p rpc-client --test integration test_initialize_system -- --ignored
//! cargo test -p rpc-client --test integration test_register_node -- --ignored
//! cargo test -p rpc-client --test integration test_get_all_nodes -- --ignored
//! cargo test -p rpc-client --test integration test_concurrent_reads -- --ignored
//! ```
//!
//! Run all tests:
//! ```bash
//! cargo test -p rpc-client --test integration -- --ignored --test-threads=1
//! ```
//!
//! **Resource Requirements:** Test validators require significant RAM (8GB+ recommended).

mod common;

use common::{
    create_client, initialize_system, setup_basic_validator, setup_validator, ValidatorGuard,
};
use solana_sdk::signature::{Keypair, Signer};

// =============================================================================
// Basic RPC Tests (no custom programs needed)
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_get_slot() {
    let (validator, _payer) = setup_basic_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    let slot = client.get_slot().await.unwrap();
    // slot is u64, so always >= 0; just verify we got a value
    assert!(slot < u64::MAX, "Should get a valid slot number");
}

// =============================================================================
// System Initialization Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_initialize_system() {
    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Initialize the full system (mint + system + expand + epoch/archive)
    initialize_system(&client, &payer).await;

    // Verify we can fetch the singleton accounts
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
    use tape_api::instruction::build_register_node_ix;
    use tape_api::utils::to_name;
    use tape_core::prelude::*;

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Initialize the system first
    initialize_system(&client, &payer).await;

    // Create a node keypair
    let node_authority = Keypair::new();

    // Fund the node authority (transfer some SOL from payer)
    let transfer_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &node_authority.pubkey(),
        1_000_000_000, // 1 SOL
    );
    client.send_instructions(&payer, vec![transfer_ix]).await.unwrap();

    // Register a node with valid BLS keypair
    let name = to_name("test-node");
    let commission_rate = BasisPoints(500); // 5%
    let network_address = NetworkAddress::from_bytes([0u8; 24]);
    let network_tls = solana_sdk::pubkey::Pubkey::new_unique();

    // Generate valid BLS keypair with proof of possession
    let bls_secret = BlsPrivateKey::from_random();
    let bls_pubkey = bls_secret.public_key().expect("derive BLS pubkey");
    let bls_pop = bls_secret.proof_of_possession().expect("generate PoP");

    let register_ix = build_register_node_ix(
        node_authority.pubkey(),
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
    use tape_api::instruction::build_register_node_ix;
    use tape_api::utils::to_name;
    use tape_core::prelude::*;

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Initialize the system
    initialize_system(&client, &payer).await;

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

        // Generate valid BLS keypair with proof of possession
        let bls_secret = BlsPrivateKey::from_random();
        let bls_pubkey = bls_secret.public_key().expect("derive BLS pubkey");
        let bls_pop = bls_secret.proof_of_possession().expect("generate PoP");

        // Register
        let name = to_name(&format!("node-{}", i));
        let register_ix = build_register_node_ix(
            node_authority.pubkey(),
            node_authority.pubkey(),
            name,
            BasisPoints(500),
            NetworkAddress::from_bytes([0u8; 24]),
            solana_sdk::pubkey::Pubkey::new_unique(),
            bls_pubkey,
            bls_pop,
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
// Error Handling Tests (no custom programs needed)
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_fetch_nonexistent_account() {
    let (validator, _payer) = setup_basic_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Try to fetch System account before initialization
    let result = client.get_system().await;
    assert!(result.is_err(), "Should fail to fetch uninitialized System");
}

#[tokio::test]
#[ignore]
async fn test_transaction_insufficient_funds() {
    let (validator, _payer) = setup_basic_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

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
    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    // Initialize system
    initialize_system(&client, &payer).await;

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
