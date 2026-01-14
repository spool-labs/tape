//! Integration tests for RpcClient using TestValidator
//!
//! These tests start a local Solana test validator with the Tape programs loaded,
//! then exercise the RpcClient API against it.
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test -p rpc-client --test integration -- --ignored --test-threads=1
//! ```

mod common;

use common::{
    create_client, initialize_system, register_node, setup_basic_validator, setup_validator,
    ValidatorGuard,
};
use solana_sdk::signature::{Keypair, Signer};

#[tokio::test]
#[ignore]
async fn test_get_slot() {
    let (validator, _payer) = setup_basic_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    let slot = client.get_slot().await.unwrap();
    assert!(slot < u64::MAX, "Should get a valid slot number");
}

#[tokio::test]
#[ignore]
async fn test_initialize_system() {
    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;

    let system = client.get_system().await;
    assert!(system.is_ok(), "Failed to fetch System: {:?}", system.err());

    let epoch = client.get_epoch().await;
    assert!(epoch.is_ok(), "Failed to fetch Epoch: {:?}", epoch.err());

    let archive = client.get_archive().await;
    assert!(
        archive.is_ok(),
        "Failed to fetch Archive: {:?}",
        archive.err()
    );
}

#[tokio::test]
#[ignore]
async fn test_register_node() {
    use tape_api::utils::to_name;

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;

    let (node_keypair, _) = register_node(&client, &payer, "test-node").await;

    let node = client.get_node(&node_keypair.pubkey()).await;
    assert!(node.is_ok(), "Failed to fetch Node: {:?}", node.err());

    let node = node.unwrap();
    assert_eq!(node.metadata.name, to_name("test-node"), "Node name should match");
}

#[tokio::test]
#[ignore]
async fn test_get_all_nodes() {
    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;

    for i in 0..3 {
        register_node(&client, &payer, &format!("node-{}", i)).await;
    }

    let nodes = client.get_all_nodes().await;
    assert!(
        nodes.is_ok(),
        "Failed to fetch all nodes: {:?}",
        nodes.err()
    );

    let nodes = nodes.unwrap();
    assert_eq!(nodes.len(), 3, "Should have 3 registered nodes");
}

#[tokio::test]
#[ignore]
async fn test_fetch_nonexistent_account() {
    let (validator, _payer) = setup_basic_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    let result = client.get_system().await;
    assert!(result.is_err(), "Should fail to fetch uninitialized System");
}

#[tokio::test]
#[ignore]
async fn test_transaction_insufficient_funds() {
    let (validator, _payer) = setup_basic_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    let broke_user = Keypair::new();

    let transfer_ix = solana_sdk::system_instruction::transfer(
        &broke_user.pubkey(),
        &solana_sdk::pubkey::Pubkey::new_unique(),
        1_000_000,
    );

    let result = client
        .send_instructions(&broke_user, vec![transfer_ix])
        .await;
    assert!(result.is_err(), "Should fail due to insufficient funds");
}

#[tokio::test]
#[ignore]
async fn test_concurrent_reads() {
    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;

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
