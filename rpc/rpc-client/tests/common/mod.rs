//! Shared test utilities for RpcClient integration tests
//!
//! This module contains common setup code for tests that require a TestValidator.

#![allow(dead_code)]

use rpc_client::RpcClient;
use rpc_test::TestRpc;
use solana_sdk::bpf_loader_upgradeable;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_test_validator::{TestValidatorGenesis, UpgradeableProgramInfo};
use std::path::PathBuf;

use tape_api::fsm::{NodeAction, NodeStateMachine};
use tape_api::instruction::{
    build_advance_epoch_ix, build_advance_pool_ix, build_epoch_sync_ix, build_join_network_ix,
    build_register_node_ix, build_stake_with_pool_ix,
};
use tape_api::program::tapedrive::node_pda;
use tape_api::utils::to_name;
use tape_core::prelude::*;
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::NodeId;

/// Metaplex Token Metadata program ID
pub const MPL_TOKEN_METADATA_ID: Pubkey = pubkey!("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");

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
            std::thread::spawn(move || drop(v));
        }
    }
}

/// Wait for real time to pass. Use this for epoch-based timing in tests.
#[allow(dead_code)]
pub async fn wait_for_epoch_duration(seconds: u64) {
    println!("Waiting {} seconds for epoch duration...", seconds);
    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
    println!("Wait complete");
}

/// Helper to fully initialize the system (mint + system + expand + epoch/archive).
/// This handles the account size expansion needed for the System account.
pub async fn initialize_system(client: &RpcClient<TestRpc>, payer: &Keypair) {
    use tape_api::instruction::{
        build_create_system_ix, build_expand_system_ix, build_initialize_ix,
        build_initialize_mint_ix,
    };

    let mint_ix = build_initialize_mint_ix(payer.pubkey(), payer.pubkey());
    client
        .send_instructions(payer, vec![mint_ix])
        .await
        .expect("Failed to initialize mint");

    let create_system_ix = build_create_system_ix(payer.pubkey(), payer.pubkey());
    client
        .send_instructions(payer, vec![create_system_ix])
        .await
        .expect("Failed to create system");

    // Expand System account to full size (~70KB, 10KB per tx)
    for _ in 0..10 {
        let expand_ix = build_expand_system_ix(payer.pubkey(), payer.pubkey());
        match client.send_instructions(payer, vec![expand_ix]).await {
            Ok(_) => {}
            Err(e) => {
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

    let init_ix = build_initialize_ix(payer.pubkey(), payer.pubkey());
    client
        .send_instructions(payer, vec![init_ix])
        .await
        .expect("Failed to initialize epoch/archive");
}

/// Register a node and return its keypair and node address
pub async fn register_node(
    client: &RpcClient<TestRpc>,
    payer: &Keypair,
    name: &str,
) -> (Keypair, Pubkey) {
    let node_keypair = Keypair::new();
    let (node_address, _) = node_pda(node_keypair.pubkey());

    let transfer_ix = solana_sdk::system_instruction::transfer(
        &payer.pubkey(),
        &node_keypair.pubkey(),
        1_000_000_000,
    );
    client
        .send_instructions(payer, vec![transfer_ix])
        .await
        .expect("Failed to fund node");

    let bls_secret = BlsPrivateKey::from_random();
    let bls_pubkey = bls_secret.public_key().expect("derive BLS pubkey");
    let bls_pop = bls_secret.proof_of_possession().expect("generate PoP");

    let register_ix = build_register_node_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        to_name(name),
        BasisPoints(500),
        NetworkAddress::from_bytes([0u8; 24]),
        Pubkey::new_unique(),
        bls_pubkey,
        bls_pop,
    );

    client
        .send_instructions(&node_keypair, vec![register_ix])
        .await
        .expect("Failed to register node");

    (node_keypair, node_address)
}

/// Transfer TAPE tokens from payer to recipient
pub async fn transfer_tape(
    client: &RpcClient<TestRpc>,
    payer: &Keypair,
    recipient: &Pubkey,
    amount: u64,
) {
    use tape_api::program::token::mint_pda;
    use tape_api::utils::ata;

    let (mint_address, _) = mint_pda();
    let source_ata = ata(&payer.pubkey());
    let dest_ata = ata(recipient);

    let create_ata_ix =
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &payer.pubkey(),
            recipient,
            &mint_address,
            &spl_token::id(),
        );

    let transfer_ix = spl_token::instruction::transfer(
        &spl_token::id(),
        &source_ata,
        &dest_ata,
        &payer.pubkey(),
        &[],
        amount,
    )
    .expect("Failed to create transfer instruction");

    client
        .send_instructions(payer, vec![create_ata_ix, transfer_ix])
        .await
        .expect("Failed to transfer TAPE tokens");
}

/// Stake TAPE tokens to a node's pool
pub async fn stake_to_node(
    client: &RpcClient<TestRpc>,
    staker: &Keypair,
    node_address: Pubkey,
    amount: Coin<TAPE>,
) {
    let stake_ix = build_stake_with_pool_ix(staker.pubkey(), staker.pubkey(), node_address, amount);
    client
        .send_instructions(staker, vec![stake_ix])
        .await
        .expect("Failed to stake to node");
}

/// Join a node to the committee
pub async fn join_committee(
    client: &RpcClient<TestRpc>,
    node_keypair: &Keypair,
    node_address: Pubkey,
) -> Result<(), String> {
    let join_ix = build_join_network_ix(node_keypair.pubkey(), node_keypair.pubkey(), node_address);
    client
        .send_instructions(node_keypair, vec![join_ix])
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Advance the epoch
pub async fn advance_epoch(client: &RpcClient<TestRpc>, payer: &Keypair) -> Result<(), String> {
    let advance_ix = build_advance_epoch_ix(payer.pubkey(), payer.pubkey());
    client
        .send_instructions(payer, vec![advance_ix])
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Call AdvancePool for a node
pub async fn advance_pool(
    client: &RpcClient<TestRpc>,
    node_keypair: &Keypair,
    node_address: Pubkey,
) -> Result<(), String> {
    let advance_ix =
        build_advance_pool_ix(node_keypair.pubkey(), node_keypair.pubkey(), node_address);
    client
        .send_instructions(node_keypair, vec![advance_ix])
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Call SyncEpoch for a node in the current committee.
/// Fetches state internally - use build_sync_ix_for_node for batch operations.
pub async fn sync_epoch(
    client: &RpcClient<TestRpc>,
    node_keypair: &Keypair,
    node_address: Pubkey,
) -> Result<(), String> {
    let system = Box::new(client.get_system().await.map_err(|e| e.to_string())?);
    let epoch = Box::new(client.get_epoch().await.map_err(|e| e.to_string())?);
    let node = Box::new(
        client
            .get_node(&node_keypair.pubkey())
            .await
            .map_err(|e| e.to_string())?,
    );

    let member_index = system
        .committee
        .index_of(&node.id)
        .ok_or("Node not in committee")?;
    let spools = system.spools.spools_for_member(member_index);

    let sync_ix = build_epoch_sync_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
        epoch.id,
        &spools,
    );
    client
        .send_instructions(node_keypair, vec![sync_ix])
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Build a sync instruction from pre-fetched state (avoids stack overflow in loops)
pub fn build_sync_ix_for_node(
    system: &tape_api::prelude::System,
    epoch_id: EpochNumber,
    node_id: NodeId,
    node_keypair: &Keypair,
    node_address: Pubkey,
) -> Result<solana_sdk::instruction::Instruction, String> {
    let member_index = system
        .committee
        .index_of(&node_id)
        .ok_or("Node not in committee")?;
    let spools = system.spools.spools_for_member(member_index);

    Ok(build_epoch_sync_ix(
        node_keypair.pubkey(),
        node_keypair.pubkey(),
        node_address,
        epoch_id,
        &spools,
    ))
}

/// Get the FSM action for a node
pub async fn get_fsm_action(client: &RpcClient<TestRpc>, node_keypair: &Keypair) -> NodeAction {
    let system = Box::new(client.get_system().await.expect("get system"));
    let epoch = Box::new(client.get_epoch().await.expect("get epoch"));
    let node = Box::new(
        client
            .get_node(&node_keypair.pubkey())
            .await
            .expect("get node"),
    );
    NodeStateMachine::determine_action(&system, &epoch, &node, 0)
}

/// Debug print FSM state for a node
pub async fn debug_fsm(client: &RpcClient<TestRpc>, node_keypair: &Keypair, label: &str) {
    let system = Box::new(client.get_system().await.expect("get system"));
    let epoch = Box::new(client.get_epoch().await.expect("get epoch"));
    let node = Box::new(
        client
            .get_node(&node_keypair.pubkey())
            .await
            .expect("get node"),
    );
    let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);

    let phase = if epoch.state.is_syncing() {
        "Syncing"
    } else if epoch.state.is_settling() {
        "Settling"
    } else if epoch.state.is_active() {
        "Active"
    } else {
        "Unknown"
    };

    println!(
        "[FSM {}] Epoch {} ({}) | Node {} | Action: {:?}",
        label,
        epoch.id.as_u64(),
        phase,
        node.id.as_u64(),
        action
    );
}

/// Print detailed epoch and node state using FSM
pub async fn debug_state(client: &RpcClient<TestRpc>, node_keypair: &Keypair, label: &str) {
    let system = Box::new(client.get_system().await.expect("Failed to get system"));
    let epoch = Box::new(client.get_epoch().await.expect("Failed to get epoch"));
    let node = Box::new(
        client
            .get_node(&node_keypair.pubkey())
            .await
            .expect("Failed to get node"),
    );
    let action = NodeStateMachine::determine_action(&system, &epoch, &node, 0);

    let phase = if epoch.state.is_syncing() {
        "Syncing"
    } else if epoch.state.is_settling() {
        "Settling"
    } else if epoch.state.is_active() {
        "Active"
    } else {
        "Unknown"
    };

    println!("\n[{}]", label);
    println!(
        "  Epoch: {} | Phase: {} | Weight: {}",
        epoch.id.as_u64(),
        phase,
        epoch.state.weight
    );
    println!(
        "  Committees: prev={} curr={} next={}",
        system.committee_prev.size(),
        system.committee.size(),
        system.committee_next.size()
    );
    println!(
        "  Node {}: stake={} sync_epoch={} advance_epoch={}",
        node.id.as_u64(),
        node.pool.stake.as_u64(),
        node.latest_sync_epoch.as_u64(),
        node.latest_advance_epoch.as_u64()
    );
    println!(
        "  Node in: prev={} curr={} next={}",
        system.committee_prev.contains(&node.id),
        system.committee.contains(&node.id),
        system.committee_next.contains(&node.id)
    );
    println!("  FSM Action: {:?}", action);
    println!();
}

/// Assert the FSM expects a specific action
pub async fn assert_fsm_action(
    client: &RpcClient<TestRpc>,
    node_keypair: &Keypair,
    expected: NodeAction,
    context: &str,
) {
    let actual = get_fsm_action(client, node_keypair).await;

    if actual != expected {
        println!("\n!!! FSM MISMATCH at '{}' !!!", context);
        debug_state(client, node_keypair, "State at mismatch").await;
        panic!(
            "FSM expected {:?}, got {:?} at '{}'",
            expected, actual, context
        );
    }
}
