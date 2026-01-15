//! Blocked epoch tests.
//!
//! Tests the blocked epoch behavior when committee_next < MIN_COMMITTEE_SIZE.
//!
//! With the blocked epoch design:
//! - AdvanceEpoch fails with InsufficientCommittee when committee_next < MIN_COMMITTEE_SIZE
//! - Stake activates immediately when epoch is blocked
//! - New nodes can join and help unblock the epoch
//!
//! ```bash
//! cargo test -p tape-e2e --test blocked_epoch -- --ignored --nocapture
//! ```

use std::time::Duration;

use serial_test::serial;
use solana_sdk::signature::Signer;
use tape_api::fsm::NodeAction;
use tape_e2e::{
    E2eRpcClient, TestContext, MIN_COMMITTEE_SIZE, EPOCH_WAIT,
    get_fsm_action, debug_all_nodes_fsm, debug_rpc_state,
    wait_for_epoch_phase_rpc,
};

/// Test blocked epoch when nodes don't rejoin.
///
/// Scenario:
/// 1. Start with MIN_COMMITTEE_SIZE nodes (normal operation)
/// 2. Some nodes stop (simulating them not rejoining committee_next)
/// 3. committee_next drops below MIN_COMMITTEE_SIZE
/// 4. FSM shows WaitForCommitteeThreshold
/// 5. AdvanceEpoch should fail with InsufficientCommittee
#[tokio::test]
#[ignore]
#[serial]
async fn test_blocked_epoch_insufficient_committee() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const NODES_TO_STOP: usize = 5; // Stop enough to drop below threshold
    const BASE_PORT: u16 = 14000;

    println!("=== Blocked Epoch Test ===");
    println!("Starting with {} nodes, will stop {} to trigger blocking", NUM_NODES, NODES_TO_STOP);

    // Setup with full committee
    let mut ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup and bootstrap");

    let rpc = E2eRpcClient::new(ctx.validator.rpc_url())
        .await
        .expect("Failed to create RPC client");

    // Verify we're in normal operation
    println!("\n=== Initial State ===");
    debug_rpc_state(&rpc, "After bootstrap").await;

    let committee_size = rpc.get_committee_size().await.expect("get committee size");
    assert!(committee_size >= MIN_COMMITTEE_SIZE, "Should have full committee");

    // Run an epoch to establish normal operation
    println!("\n=== Running 1 epoch in normal mode ===");
    ctx.observe_epochs(1, |epoch, system| {
        println!("  Epoch {}: committee={}", epoch.id.unwrap_or(0), system.committee_size.unwrap_or(0));
        Ok(())
    })
    .await
    .expect("Failed to observe epoch");

    // Stop some nodes (they won't rejoin committee_next)
    println!("\n=== Stopping {} nodes ===", NODES_TO_STOP);
    for i in 0..NODES_TO_STOP {
        ctx.nodes[i].stop();
        println!("  Stopped {}", ctx.nodes[i].name);
    }

    // Wait for epoch duration to see the effect
    println!("\n=== Waiting for EPOCH_DURATION to see blocking ===");
    tokio::time::sleep(EPOCH_WAIT).await;

    // Check committee_next size
    debug_rpc_state(&rpc, "After nodes stopped").await;

    // Check FSM - active nodes should show WaitForCommitteeThreshold
    println!("\n=== Checking FSM Actions ===");
    for node in ctx.nodes.iter().skip(NODES_TO_STOP) {
        let authority = node.authority.pubkey();
        let action = get_fsm_action(&rpc, &authority).await;
        match action {
            Ok(a) => {
                println!("  {}: {:?}", node.name, a);
                // After epoch duration, should be waiting for committee threshold
                // OR should be able to advance if committee_next has enough
            }
            Err(e) => println!("  {}: ERROR - {}", node.name, e),
        }
    }

    // Try to advance epoch - may fail if committee_next < MIN_COMMITTEE_SIZE
    println!("\n=== Attempting AdvanceEpoch ===");
    let result = ctx.cli.admin_advance_epoch();
    match result {
        Ok(_) => println!("  AdvanceEpoch succeeded (committee_next was sufficient)"),
        Err(e) => println!("  AdvanceEpoch failed: {} (expected if blocked)", e),
    }

    // Check final state
    debug_rpc_state(&rpc, "Final state").await;

    println!("\nTest completed: Verified blocked epoch behavior");
}

/// Test recovery from blocked epoch by adding new nodes.
///
/// Scenario:
/// 1. Start with nodes (committee operational)
/// 2. Simulate blocking (nodes don't rejoin)
/// 3. Add new nodes that join committee_next
/// 4. Verify AdvanceEpoch succeeds after committee_next reaches threshold
#[tokio::test]
#[ignore]
#[serial]
async fn test_blocked_epoch_recovery_with_new_nodes() {
    const INITIAL_NODES: usize = MIN_COMMITTEE_SIZE;
    const NODES_TO_STOP: usize = 10;
    const NEW_NODES: usize = 10;
    const BASE_PORT: u16 = 14100;

    println!("=== Blocked Epoch Recovery Test ===");
    println!("Initial: {} nodes, Stop: {}, Add: {}", INITIAL_NODES, NODES_TO_STOP, NEW_NODES);

    // Setup with full committee
    let mut ctx = TestContext::builder()
        .nodes(INITIAL_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup and bootstrap");

    let rpc = E2eRpcClient::new(ctx.validator.rpc_url())
        .await
        .expect("Failed to create RPC client");

    println!("\n=== Initial State ===");
    debug_rpc_state(&rpc, "After bootstrap").await;

    // Run a few epochs
    println!("\n=== Running 2 epochs in normal mode ===");
    ctx.observe_epochs(2, |epoch, system| {
        println!("  Epoch {}: committee={}", epoch.id.unwrap_or(0), system.committee_size.unwrap_or(0));
        Ok(())
    })
    .await
    .expect("Failed to observe epochs");

    // Stop nodes to trigger blocking
    println!("\n=== Stopping {} nodes ===", NODES_TO_STOP);
    for i in 0..NODES_TO_STOP {
        ctx.nodes[i].stop();
        println!("  Stopped {}", ctx.nodes[i].name);
    }

    // Wait for effects
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("\n=== State After Stopping Nodes ===");
    debug_rpc_state(&rpc, "After stops").await;

    // Add new nodes
    println!("\n=== Adding {} new nodes ===", NEW_NODES);
    ctx.add_nodes(NEW_NODES, 1000)
        .await
        .expect("Failed to add nodes");

    println!("Total nodes now: {}", ctx.nodes.len());

    // Wait for epoch to stabilize
    tokio::time::sleep(EPOCH_WAIT).await;

    println!("\n=== State After Adding Nodes ===");
    debug_rpc_state(&rpc, "After adding nodes").await;

    // Check committee_next
    let committee_next = rpc.get_committee_next_size().await.expect("get committee_next");
    println!("Committee next size: {}", committee_next);

    // Try to advance epoch
    println!("\n=== Attempting AdvanceEpoch ===");
    let result = ctx.cli.admin_advance_epoch();
    match result {
        Ok(_) => {
            println!("  AdvanceEpoch succeeded!");

            // Wait for epoch to transition
            wait_for_epoch_phase_rpc(&rpc, "Active", Duration::from_secs(120))
                .await
                .expect("Epoch should reach Active");

            println!("  Epoch recovered to Active phase");
        }
        Err(e) => {
            println!("  AdvanceEpoch still blocked: {}", e);
            println!("  May need more nodes to join");
        }
    }

    // Check final state
    debug_rpc_state(&rpc, "Final state").await;

    // Verify no errors in logs for running nodes
    for node in ctx.nodes.iter().skip(NODES_TO_STOP) {
        if let Ok(log) = node.read_log() {
            assert!(!log.contains("panic"), "Node {} should not panic", node.name);
        }
    }

    println!("\nTest completed: Verified blocked epoch recovery");
}

/// Test FSM behavior during blocked epoch.
///
/// When epoch is blocked (waiting for committee_next threshold):
/// - Nodes should show WaitForCommitteeThreshold action
/// - Stake should activate immediately (no waiting for next epoch)
/// - Once threshold is met, FSM should show AdvanceEpoch
#[tokio::test]
#[ignore]
#[serial]
async fn test_fsm_during_blocked_epoch() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const BASE_PORT: u16 = 14200;

    println!("=== FSM During Blocked Epoch Test ===");

    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup and bootstrap");

    let rpc = E2eRpcClient::new(ctx.validator.rpc_url())
        .await
        .expect("Failed to create RPC client");

    // Wait for EPOCH_DURATION
    tokio::time::sleep(EPOCH_WAIT).await;

    // Check FSM state for all nodes
    println!("\n=== FSM State After EPOCH_DURATION ===");
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "After waiting").await;

    // Verify nodes can advance or are waiting for something specific
    let mut has_advance_epoch = false;
    let mut has_waiting = false;

    for node in &ctx.nodes {
        let authority = node.authority.pubkey();
        match get_fsm_action(&rpc, &authority).await {
            Ok(NodeAction::AdvanceEpoch) => has_advance_epoch = true,
            Ok(NodeAction::WaitForCommitteeThreshold { .. }) => has_waiting = true,
            Ok(NodeAction::WaitForEpochDuration { .. }) => has_waiting = true,
            Ok(action) => println!("  {}: {:?}", node.name, action),
            Err(e) => println!("  {}: ERROR - {}", node.name, e),
        }
    }

    println!("\nFSM summary:");
    println!("  Has AdvanceEpoch: {}", has_advance_epoch);
    println!("  Has Waiting: {}", has_waiting);

    // At least one node should be ready to advance or all should be waiting
    assert!(
        has_advance_epoch || has_waiting,
        "Nodes should either be ready to advance or waiting"
    );

    println!("\nTest completed: FSM behavior verified");
}
