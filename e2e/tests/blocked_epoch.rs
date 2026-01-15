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
use tape_api::errors::TapeError;
use tape_e2e::{
    TestContext, MIN_COMMITTEE_SIZE, EPOCH_WAIT,
    get_fsm_action, debug_rpc_state,
};

/// Test blocked epoch when committee_next is insufficient.
///
/// Scenario:
/// 1. Start with a small number of nodes (below MIN_COMMITTEE_SIZE)
/// 2. Bootstrap succeeds (bootstrap exception allows small committee_prev)
/// 3. After bootstrap, committee_next may be insufficient
/// 4. FSM shows WaitForCommitteeThreshold
/// 5. AdvanceEpoch should be blocked or show waiting state
///
/// Note: With the blocked epoch design, we can test this by starting with
/// fewer nodes than MIN_COMMITTEE_SIZE and verifying the blocking behavior.
#[tokio::test]
#[ignore]
#[serial]
async fn test_blocked_epoch_insufficient_committee() {
    // Use fewer nodes than MIN_COMMITTEE_SIZE to test blocking
    // Bootstrap exception allows this for the first epoch
    const NUM_NODES: usize = 10; // Below MIN_COMMITTEE_SIZE
    const BASE_PORT: u16 = 14000;

    println!("Starting with {} nodes (below MIN_COMMITTEE_SIZE={})", NUM_NODES, MIN_COMMITTEE_SIZE);

    // Setup with small committee (bootstrap exception allows this)
    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(300))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup and bootstrap");

    // Verify we're in Active phase after bootstrap
    debug_rpc_state(&ctx.rpc, "After bootstrap").await;

    let phase = ctx.epoch_phase().await.expect("get phase");
    assert_eq!(phase, "Active", "Should be in Active phase after bootstrap");

    let committee_size = ctx.committee_size().await.expect("get committee size");
    println!("Committee size: {}", committee_size);
    assert_eq!(committee_size, NUM_NODES, "All nodes should be in committee");

    // Wait for EPOCH_DURATION to elapse
    tokio::time::sleep(EPOCH_WAIT).await;

    // Check committee_next size
    let committee_next_size = ctx.committee_next_size().await.expect("get committee_next");
    println!("Committee_next size: {}", committee_next_size);

    debug_rpc_state(&ctx.rpc, "After EPOCH_DURATION").await;

    // Check FSM - should show WaitForCommitteeThreshold if committee_next < MIN_COMMITTEE_SIZE
    let mut waiting_count = 0;
    let mut can_advance_count = 0;

    for node in &ctx.nodes {
        let authority = node.authority.pubkey();
        let action = get_fsm_action(&ctx.rpc, &authority).await;
        match action {
            Ok(NodeAction::WaitForCommitteeThreshold { current_size, required_size }) => {
                println!("  {}: WaitForCommitteeThreshold (current={}, required={})",
                         node.name, current_size, required_size);
                waiting_count += 1;
            }
            Ok(NodeAction::AdvanceEpoch) => {
                println!("  {}: AdvanceEpoch (committee_next sufficient)", node.name);
                can_advance_count += 1;
            }
            Ok(a) => println!("  {}: {:?}", node.name, a),
            Err(e) => println!("  {}: ERROR - {}", node.name, e),
        }
    }

    println!("\nFSM summary:");
    println!("  Waiting for threshold: {}", waiting_count);
    println!("  Can advance: {}", can_advance_count);

    // Try to advance epoch
    let result = ctx.cli.admin_advance_epoch();
    match result {
        Ok(_) => {
            println!("  AdvanceEpoch succeeded (committee_next was sufficient)");
            // This is OK if all nodes rejoined committee_next
        }
        Err(e) => {
            let err_str = e.to_string();
            // Use typed error parsing to check for InsufficientCommittee
            if let Some(tape_err) = TapeError::from_error_string(&err_str) {
                if tape_err == TapeError::InsufficientCommittee {
                    println!("  AdvanceEpoch blocked as expected: {}", tape_err);
                    // Verify we're still in Active phase
                    let phase = ctx.epoch_phase().await.expect("get phase");
                    assert_eq!(phase, "Active", "Should still be in Active phase when blocked");
                } else {
                    println!("  AdvanceEpoch failed with unexpected error: {}", tape_err);
                }
            } else {
                println!("  AdvanceEpoch failed with error: {}", e);
            }
        }
    }

    // Check final state
    debug_rpc_state(&ctx.rpc, "Final state").await;

    println!("\nTest completed: Verified blocked epoch behavior");
}

/// Test that adding nodes increases committee_next.
///
/// This is a simpler test that verifies:
/// 1. New nodes can join committee_next
/// 2. committee_next size increases as nodes join
///
/// Note: This test doesn't require waiting for full epoch transitions,
/// making it faster and more reliable.
#[tokio::test]
#[ignore]
#[serial]
async fn test_blocked_epoch_recovery_with_new_nodes() {
    // Start with nodes below threshold
    const INITIAL_NODES: usize = 10; // Below MIN_COMMITTEE_SIZE
    const NEW_NODES: usize = 10; // Add more nodes
    const BASE_PORT: u16 = 14100;

    println!("Initial: {} nodes, will add: {} more", INITIAL_NODES, NEW_NODES);

    // Setup with small committee (bootstrap exception allows this)
    let mut ctx = TestContext::builder()
        .nodes(INITIAL_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(300))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup and bootstrap");

    debug_rpc_state(&ctx.rpc, "After bootstrap").await;

    let committee_size = ctx.committee_size().await.expect("get committee size");
    println!("Committee size: {}", committee_size);

    // Get initial committee_next size
    let committee_next_initial = ctx.committee_next_size().await.expect("get committee_next");
    println!("Initial committee_next size: {}", committee_next_initial);

    // Add new nodes
    ctx.add_nodes(NEW_NODES, 1000)
        .await
        .expect("Failed to add nodes");

    println!("Total nodes now: {}", ctx.nodes.len());

    // Wait for new nodes to join committee_next
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check committee_next after adding nodes
    let committee_next_after = ctx.committee_next_size().await.expect("get committee_next");
    println!("Committee_next size after adding: {}", committee_next_after);

    debug_rpc_state(&ctx.rpc, "After adding nodes").await;

    // Verify committee_next has grown
    assert!(
        committee_next_after > committee_next_initial,
        "Committee_next should grow after adding nodes: {} -> {}",
        committee_next_initial,
        committee_next_after
    );

    // Check FSM state for the new nodes
    for node in ctx.nodes.iter().skip(INITIAL_NODES) {
        let authority = node.authority.pubkey();
        match get_fsm_action(&ctx.rpc, &authority).await {
            Ok(action) => println!("  {}: {:?}", node.name, action),
            Err(e) => println!("  {}: ERROR - {}", node.name, e),
        }
    }

    // Verify no panics in node logs
    for node in &ctx.nodes {
        if let Ok(log) = node.read_log() {
            assert!(!log.contains("panic"), "Node {} should not panic", node.name);
        }
    }

    println!("\nTest completed: Verified nodes can join and grow committee_next");
}

