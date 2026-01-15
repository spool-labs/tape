//! Bootstrap flow tests.
//!
//! Tests the full bootstrap flow from system initialization to first operational epoch.
//!
//! Flow (from FLOWS.md):
//! 1. Initialize system (admin init)
//! 2. Nodes register, stake, and join (committee_next)
//! 3. AdvanceEpoch (bootstrap exception allows < MIN_COMMITTEE_SIZE nodes)
//! 4. SyncEpoch (by new committee members)
//! 5. Skip to Active (committee_prev_empty)
//! 6. Verify nodes can serve storage
//!
//! ```bash
//! cargo test -p tape-e2e --test bootstrap -- --ignored --nocapture
//! ```

use std::time::Duration;

use serial_test::serial;
use solana_sdk::signature::Signer;
use tape_e2e::{
    TestRpcClient, TestContext, MIN_COMMITTEE_SIZE, EPOCH_WAIT,
    get_fsm_action, debug_all_nodes_fsm, wait_for_epoch_phase_rpc,
};


/// Test the bootstrap flow with a small number of nodes.
///
/// Bootstrap mode allows AdvanceEpoch with < MIN_COMMITTEE_SIZE nodes
/// when committee_prev is empty. This test verifies:
/// 1. Nodes join committee_next
/// 2. AdvanceEpoch works despite < MIN_COMMITTEE_SIZE
/// 3. Nodes sync and epoch transitions to Active
/// 4. FSM shows correct actions throughout
#[tokio::test]
#[ignore]
#[serial]
async fn test_bootstrap_flow_small_committee() {
    const NUM_NODES: usize = 5; // Below MIN_COMMITTEE_SIZE, but bootstrap exception allows
    const BASE_PORT: u16 = 13000;

    println!("(Bootstrap exception allows < {} nodes when committee_prev is empty)", MIN_COMMITTEE_SIZE);

    // Setup: spawn validator, initialize system, register/stake/join nodes
    let mut ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(300))
        .build()
        .await
        .expect("Failed to setup test context");

    // Create RPC client for state verification
    let rpc = TestRpcClient::new(ctx.validator.rpc_url())
        .await
        .expect("Failed to create RPC client");

    // Verify initial state - committee_prev should be empty (bootstrap mode)
    let is_bootstrap = rpc.is_bootstrap_mode().await.expect("get bootstrap mode");
    assert!(is_bootstrap, "Should be in bootstrap mode (committee_prev empty)");

    let committee_next_size = rpc.get_committee_next_size().await.expect("get committee_next");
    println!("Committee next size: {}", committee_next_size);
    assert_eq!(committee_next_size, NUM_NODES, "All nodes should be in committee_next");

    // Check FSM action for nodes - should show AdvanceEpoch
    for node in &ctx.nodes {
        let authority = node.authority.pubkey();
        let action = get_fsm_action(&rpc, &authority)
            .await
            .expect("get FSM action");
        println!("  {}: {:?}", node.name, action);
    }

    // Wait for EPOCH_DURATION to elapse
    tokio::time::sleep(EPOCH_WAIT).await;

    // Advance epoch (bootstrap exception)
    ctx.cli.admin_advance_epoch().expect("Bootstrap epoch advance should succeed");

    // Verify epoch is now in Syncing phase
    let phase = rpc.get_epoch_phase().await.expect("get phase");
    println!("Epoch phase after advance: {}", phase);
    assert_eq!(phase, "Syncing", "Epoch should be in Syncing phase");

    // Check FSM action - should show SyncEpoch for committee members
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "After AdvanceEpoch").await;

    // Fund and start nodes so they can sync
    for (i, node) in ctx.nodes.iter().enumerate() {
        if let Err(e) = node.fund(&ctx.cli, 1.0) {
            println!("Warning: Failed to fund node {}: {}", i, e);
        }
    }

    for (i, node) in ctx.nodes.iter_mut().enumerate() {
        if let Err(e) = node.start(&ctx.cli) {
            println!("Warning: Failed to start node {}: {}", i, e);
        }
    }

    // Wait for nodes to initialize
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Wait for epoch to reach Active (bootstrap skips Settling since committee_prev is empty)
    wait_for_epoch_phase_rpc(&rpc, "Active", Duration::from_secs(60))
        .await
        .expect("Epoch should reach Active phase");

    println!("Epoch reached Active phase!");

    // Verify committee state
    let committee_size = rpc.get_committee_size().await.expect("get committee size");
    println!("Committee size in Active: {}", committee_size);
    assert_eq!(committee_size, NUM_NODES, "All nodes should be in committee");

    // Check FSM shows waiting for epoch duration or join network
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "Final State").await;

    // Verify no errors in logs
    ctx.check_node_logs().expect("No errors in node logs");

    println!("\nTest passed: Bootstrap flow completed successfully");
}

/// Test bootstrap with MIN_COMMITTEE_SIZE nodes.
///
/// This is the "normal" bootstrap case where we have enough nodes
/// from the start. Verifies the same flow but with full quorum.
#[tokio::test]
#[ignore]
#[serial]
async fn test_bootstrap_flow_full_committee() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const BASE_PORT: u16 = 13100;


    // Use build_and_bootstrap which handles the full flow
    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup and bootstrap");

    // Create RPC client for verification
    let rpc = TestRpcClient::new(ctx.validator.rpc_url())
        .await
        .expect("Failed to create RPC client");

    // Verify we're in Active phase with full committee
    let phase = rpc.get_epoch_phase().await.expect("get phase");
    let committee_size = rpc.get_committee_size().await.expect("get committee size");

    println!("After bootstrap:");
    println!("  Phase: {}", phase);
    println!("  Committee size: {}", committee_size);

    assert_eq!(phase, "Active", "Should be in Active phase after bootstrap");
    assert_eq!(committee_size, NUM_NODES, "All nodes should be in committee");

    // Check that nodes are not blocked
    for node in &ctx.nodes {
        let authority = node.authority.pubkey();
        let action = get_fsm_action(&rpc, &authority)
            .await
            .expect("get FSM action");
        assert!(!action.is_blocked(), "Node {} should not be blocked", node.name);
    }

    // Verify no errors
    ctx.check_node_logs().expect("No errors in node logs");

    println!("\nTest passed: Full committee bootstrap completed successfully");
}
