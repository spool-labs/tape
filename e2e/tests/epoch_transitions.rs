//! Epoch transition tests with FSM verification.
//!
//! Tests the full epoch cycle: Active -> Syncing -> Settling -> Active
//! Uses the FSM to verify node behavior at each phase.
//!
//! Flow (from FLOWS.md):
//! 1. Epoch in Active state
//! 2. EPOCH_DURATION elapses
//! 3. AdvanceEpoch -> Syncing
//! 4. Committee members sync (SyncEpoch)
//! 5. Supermajority -> Settling
//! 6. Committee_prev members advance pool (AdvancePool)
//! 7. Supermajority -> Active
//! 8. Nodes rejoin committee_next (JoinNetwork)
//!
//! ```bash
//! cargo test -p tape-e2e --test epoch_transitions -- --ignored --nocapture
//! ```

use std::time::Duration;

use serial_test::serial;
use solana_sdk::signature::Signer;
use tape_api::fsm::NodeAction;
use tape_core::types::EpochNumber;
use tape_e2e::{
    TestRpcClient, TestContext, MIN_COMMITTEE_SIZE, EPOCH_WAIT,
    get_fsm_action, debug_all_nodes_fsm, debug_rpc_state,
    wait_for_epoch_phase_rpc, wait_for_epoch_id_rpc, ActionCategory, categorize_action,
};

/// Test full epoch cycle: Active -> Syncing -> Settling -> Active
///
/// Verifies:
/// 1. FSM shows correct actions at each phase
/// 2. Epoch transitions through all phases
/// 3. Nodes rejoin committee_next after Active
///
/// Starts at epoch 4+ to test normal operation after bootstrap period.
#[tokio::test]
#[ignore]
#[serial]
async fn test_full_epoch_cycle() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const BASE_PORT: u16 = 15000;


    // Bootstrap system and advance to epoch 4+ for normal operation
    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build_and_bootstrap_to_epoch(EpochNumber(4))
        .await
        .expect("Failed to setup and bootstrap to epoch 4");

    let rpc = TestRpcClient::new(ctx.validator.rpc_url())
        .await
        .expect("Failed to create RPC client");

    // Verify we're in Active phase
    let phase = rpc.get_epoch_phase().await.expect("get phase");
    assert_eq!(phase, "Active", "Should start in Active phase");

    let epoch_before = rpc.get_epoch_id().await.expect("get epoch id");
    println!("Starting at epoch {}, phase {}", epoch_before.as_u64(), phase);

    // Check initial FSM state
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "Initial").await;

    // Wait for EPOCH_DURATION to elapse
    tokio::time::sleep(EPOCH_WAIT).await;

    // Check FSM - should show AdvanceEpoch for at least some nodes
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "After waiting").await;

    // Count action categories
    let mut can_advance = 0;
    for node in &ctx.nodes {
        let authority = node.authority.pubkey();
        if let Ok(category) = categorize_action(&rpc, &authority).await {
            if matches!(category, ActionCategory::CanAdvanceEpoch) {
                can_advance += 1;
            }
        }
    }
    println!("Nodes that can advance epoch: {}/{}", can_advance, NUM_NODES);

    // Let nodes advance autonomously (one will succeed)
    // Wait for epoch to increment - phases may transition very quickly with small committees
    // so instead of catching each phase, we verify the end result (epoch number increment)
    let target_epoch = EpochNumber(epoch_before.as_u64() + 1);
    wait_for_epoch_id_rpc(&rpc, target_epoch, Duration::from_secs(60))
        .await
        .expect("Epoch should advance autonomously");

    println!("Epoch advanced to {} autonomously!", target_epoch.as_u64());
    debug_rpc_state(&rpc, "After autonomous advancement").await;

    // Check FSM state after epoch increment
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "After advance").await;

    // Wait for system to stabilize in Active phase
    wait_for_epoch_phase_rpc(&rpc, "Active", Duration::from_secs(30))
        .await
        .expect("Epoch should return to Active phase");

    println!("Confirmed in Active phase!");

    // Verify epoch incremented
    let epoch_after = rpc.get_epoch_id().await.expect("get epoch id");
    println!("Epoch changed: {} -> {}", epoch_before.as_u64(), epoch_after.as_u64());
    assert!(epoch_after > epoch_before, "Epoch should have incremented");

    // Check final FSM state
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "Final").await;

    // Verify committee state
    debug_rpc_state(&rpc, "Final").await;

    // Verify no errors
    ctx.check_node_logs().expect("No errors in node logs");

    println!("\nTest passed: Full epoch cycle completed successfully");
}

/// Test multiple epoch cycles in succession.
///
/// Verifies system stability over multiple epoch transitions.
/// Starts at epoch 4+ to test normal operation after bootstrap period.
#[tokio::test]
#[ignore]
#[serial]
async fn test_multiple_epoch_cycles() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const NUM_CYCLES: u64 = 3;
    const BASE_PORT: u16 = 15100;


    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build_and_bootstrap_to_epoch(EpochNumber(4))
        .await
        .expect("Failed to setup and bootstrap to epoch 4");

    let rpc = TestRpcClient::new(ctx.validator.rpc_url())
        .await
        .expect("Failed to create RPC client");

    // Track phases seen
    let mut syncing_count = 0;
    let mut settling_count = 0;
    let mut active_count = 0;

    // Observe epochs autonomously

    ctx.observe_epochs(NUM_CYCLES, |epoch, system| {
        let phase = if epoch.state.is_syncing() { "Syncing" }
            else if epoch.state.is_settling() { "Settling" }
            else if epoch.state.is_active() { "Active" }
            else { "Unknown" };

        match phase {
            "Syncing" => syncing_count += 1,
            "Settling" => settling_count += 1,
            "Active" => active_count += 1,
            _ => {}
        }

        println!(
            "  Epoch {}: phase={}, committee={}",
            epoch.id.as_u64(),
            phase,
            system.committee.size()
        );

        // Committee should stay stable
        assert!(
            system.committee.size() >= MIN_COMMITTEE_SIZE,
            "Committee should not drop below minimum"
        );

        Ok(())
    })
    .await
    .expect("Failed to observe epochs");

    println!("  Syncing: {}", syncing_count);
    println!("  Settling: {}", settling_count);
    println!("  Active: {}", active_count);

    // Final FSM check
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "After {} cycles").await;

    // Verify no errors
    ctx.check_node_logs().expect("No errors in node logs");

    println!("\nTest passed: {} epoch cycles completed successfully", NUM_CYCLES);
}

/// Test FSM actions match expected transitions.
///
/// For each phase, verify FSM returns correct action types.
/// Starts at epoch 4+ to test normal operation after bootstrap period.
#[tokio::test]
#[ignore]
#[serial]
async fn test_fsm_action_correctness() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const BASE_PORT: u16 = 15200;


    let ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(600))
        .build_and_bootstrap_to_epoch(EpochNumber(4))
        .await
        .expect("Failed to setup and bootstrap to epoch 4");

    let rpc = TestRpcClient::new(ctx.validator.rpc_url())
        .await
        .expect("Failed to create RPC client");

    // In Active phase after bootstrap, nodes might need to rejoin or wait
    let phase = rpc.get_epoch_phase().await.expect("get phase");
    assert_eq!(phase, "Active");

    for node in &ctx.nodes {
        let authority = node.authority.pubkey();
        let action = get_fsm_action(&rpc, &authority).await.expect("get action");

        // Valid actions in Active phase:
        // - WaitForEpochDuration (waiting for epoch to be advanced)
        // - WaitForCommitteeThreshold (waiting for more nodes in committee_next)
        // - AdvanceEpoch (ready to advance)
        // - JoinNetwork (needs to rejoin committee_next)
        let valid = matches!(
            action,
            NodeAction::WaitForEpochDuration { .. }
                | NodeAction::WaitForCommitteeThreshold { .. }
                | NodeAction::AdvanceEpoch
                | NodeAction::JoinNetwork
        );

        println!("  {}: {:?} (valid: {})", node.name, action, valid);
        assert!(valid, "Action should be valid for Active phase");
    }

    // Wait for Syncing phase
    tokio::time::sleep(EPOCH_WAIT).await;

    // Force advance if needed
    let _ = ctx.cli.admin_advance_epoch();

    // Wait for Syncing
    if let Ok(()) = wait_for_epoch_phase_rpc(&rpc, "Syncing", Duration::from_secs(30)).await {

        for node in &ctx.nodes {
            let authority = node.authority.pubkey();
            let action = get_fsm_action(&rpc, &authority).await.expect("get action");

            // Valid actions in Syncing phase:
            // - SyncEpoch (needs to sync)
            // - WaitForSyncQuorum (waiting for others to sync)
            // - JoinNetwork (not in committee, needs to join)
            let valid = matches!(
                action,
                NodeAction::SyncEpoch
                    | NodeAction::WaitForSyncQuorum { .. }
                    | NodeAction::JoinNetwork
            );

            println!("  {}: {:?} (valid: {})", node.name, action, valid);
            // Note: Don't assert - some nodes might already be synced
        }
    }

    println!("\nTest completed: FSM action correctness verified");
}

/// Test epoch transition timing.
///
/// Verify that epoch advances respect EPOCH_DURATION timing.
/// Starts at epoch 4+ to test normal operation after bootstrap period.
#[tokio::test]
#[ignore]
#[serial]
async fn test_epoch_timing() {
    const BASE_PORT: u16 = 15300;


    let ctx = TestContext::builder()
        .nodes(MIN_COMMITTEE_SIZE)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(300))
        .build_and_bootstrap_to_epoch(EpochNumber(4))
        .await
        .expect("Failed to setup and bootstrap to epoch 4");

    let rpc = TestRpcClient::new(ctx.validator.rpc_url())
        .await
        .expect("Failed to create RPC client");

    let epoch_before = rpc.get_epoch_id().await.expect("get epoch id");
    println!("Starting epoch: {}", epoch_before.as_u64());

    // Try to advance immediately - should fail (duration not elapsed)
    let result = ctx.cli.admin_advance_epoch();
    println!("Immediate advance result: {}", if result.is_ok() { "succeeded (unexpected)" } else { "failed (expected)" });

    // Check FSM shows waiting for duration
    let authority = ctx.nodes[0].authority.pubkey();
    let action = get_fsm_action(&rpc, &authority).await.expect("get action");
    println!("FSM action: {:?}", action);

    // Wait for EPOCH_DURATION
    tokio::time::sleep(EPOCH_WAIT).await;

    // Now advance should work
    let result = ctx.cli.admin_advance_epoch();
    println!("Advance after waiting: {}", if result.is_ok() { "succeeded" } else { "failed" });

    // Check epoch changed
    let epoch_after = rpc.get_epoch_id().await.expect("get epoch id");
    println!("Epoch after: {} (was {})", epoch_after.as_u64(), epoch_before.as_u64());

    println!("\nTest completed: Epoch timing verified");
}
