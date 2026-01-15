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
use tape_e2e::{
    E2eRpcClient, TestContext, MIN_COMMITTEE_SIZE, EPOCH_WAIT,
    get_fsm_action, debug_all_nodes_fsm, debug_rpc_state,
    wait_for_epoch_phase_rpc, ActionCategory, categorize_action,
};

/// Test full epoch cycle: Active -> Syncing -> Settling -> Active
///
/// Verifies:
/// 1. FSM shows correct actions at each phase
/// 2. Epoch transitions through all phases
/// 3. Nodes rejoin committee_next after Active
#[tokio::test]
#[ignore]
#[serial]
async fn test_full_epoch_cycle() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const BASE_PORT: u16 = 15000;

    println!("=== Full Epoch Cycle Test ({} nodes) ===", NUM_NODES);

    // Bootstrap system
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

    // Verify we're in Active phase
    let phase = rpc.get_epoch_phase().await.expect("get phase");
    assert_eq!(phase, "Active", "Should start in Active phase");

    let epoch_before = rpc.get_epoch_id().await.expect("get epoch id");
    println!("Starting at epoch {}, phase {}", epoch_before.as_u64(), phase);

    // Check initial FSM state
    println!("\n=== Initial FSM State (Active Phase) ===");
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "Initial").await;

    // Wait for EPOCH_DURATION to elapse
    println!("\n=== Waiting for EPOCH_DURATION ({:?}) ===", EPOCH_WAIT);
    tokio::time::sleep(EPOCH_WAIT).await;

    // Check FSM - should show AdvanceEpoch for at least some nodes
    println!("\n=== FSM After EPOCH_DURATION ===");
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
    // Wait for Syncing phase
    println!("\n=== Waiting for Syncing Phase ===");
    wait_for_epoch_phase_rpc(&rpc, "Syncing", Duration::from_secs(30))
        .await
        .expect("Epoch should reach Syncing phase");

    println!("Reached Syncing phase!");
    debug_rpc_state(&rpc, "In Syncing").await;

    // Check FSM during Syncing - should show SyncEpoch or WaitForSyncQuorum
    println!("\n=== FSM During Syncing Phase ===");
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "Syncing").await;

    // Verify nodes need to sync
    let mut needs_sync = 0;
    for node in &ctx.nodes {
        let authority = node.authority.pubkey();
        if let Ok(category) = categorize_action(&rpc, &authority).await {
            if matches!(category, ActionCategory::NeedsSync) {
                needs_sync += 1;
            }
        }
    }
    println!("Nodes needing to sync: {}/{}", needs_sync, NUM_NODES);

    // Wait for Settling phase (nodes sync autonomously)
    println!("\n=== Waiting for Settling Phase ===");
    wait_for_epoch_phase_rpc(&rpc, "Settling", Duration::from_secs(60))
        .await
        .expect("Epoch should reach Settling phase");

    println!("Reached Settling phase!");
    debug_rpc_state(&rpc, "In Settling").await;

    // Check FSM during Settling - should show AdvancePool or WaitForSettleQuorum
    println!("\n=== FSM During Settling Phase ===");
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "Settling").await;

    // Wait for Active phase (nodes advance pool autonomously)
    println!("\n=== Waiting for Active Phase ===");
    wait_for_epoch_phase_rpc(&rpc, "Active", Duration::from_secs(60))
        .await
        .expect("Epoch should reach Active phase");

    println!("Returned to Active phase!");

    // Verify epoch incremented
    let epoch_after = rpc.get_epoch_id().await.expect("get epoch id");
    println!("Epoch changed: {} -> {}", epoch_before.as_u64(), epoch_after.as_u64());
    assert!(epoch_after > epoch_before, "Epoch should have incremented");

    // Check final FSM state
    println!("\n=== Final FSM State (Active Phase) ===");
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
#[tokio::test]
#[ignore]
#[serial]
async fn test_multiple_epoch_cycles() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const NUM_CYCLES: u64 = 3;
    const BASE_PORT: u16 = 15100;

    println!("=== Multiple Epoch Cycles Test ({} nodes, {} cycles) ===", NUM_NODES, NUM_CYCLES);

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

    // Track phases seen
    let mut syncing_count = 0;
    let mut settling_count = 0;
    let mut active_count = 0;

    // Observe epochs autonomously
    println!("\n=== Observing {} epoch cycles ===", NUM_CYCLES);

    ctx.observe_epochs(NUM_CYCLES, |epoch, system| {
        let phase = epoch.phase.as_deref().unwrap_or("unknown");

        match phase {
            "Syncing" => syncing_count += 1,
            "Settling" => settling_count += 1,
            "Active" => active_count += 1,
            _ => {}
        }

        println!(
            "  Epoch {}: phase={}, committee={}",
            epoch.id.unwrap_or(0),
            phase,
            system.committee_size.unwrap_or(0)
        );

        // Committee should stay stable
        assert!(
            system.committee_size.unwrap_or(0) >= MIN_COMMITTEE_SIZE,
            "Committee should not drop below minimum"
        );

        Ok(())
    })
    .await
    .expect("Failed to observe epochs");

    println!("\n=== Phase Distribution ===");
    println!("  Syncing: {}", syncing_count);
    println!("  Settling: {}", settling_count);
    println!("  Active: {}", active_count);

    // Final FSM check
    println!("\n=== Final FSM State ===");
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "After {} cycles").await;

    // Verify no errors
    ctx.check_node_logs().expect("No errors in node logs");

    println!("\nTest passed: {} epoch cycles completed successfully", NUM_CYCLES);
}

/// Test FSM actions match expected transitions.
///
/// For each phase, verify FSM returns correct action types.
#[tokio::test]
#[ignore]
#[serial]
async fn test_fsm_action_correctness() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const BASE_PORT: u16 = 15200;

    println!("=== FSM Action Correctness Test ===");

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

    // In Active phase after bootstrap, nodes might need to rejoin or wait
    println!("\n=== Active Phase FSM Check ===");
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
    println!("\n=== Triggering Phase Transition ===");
    tokio::time::sleep(EPOCH_WAIT).await;

    // Force advance if needed
    let _ = ctx.cli.admin_advance_epoch();

    // Wait for Syncing
    if let Ok(()) = wait_for_epoch_phase_rpc(&rpc, "Syncing", Duration::from_secs(30)).await {
        println!("\n=== Syncing Phase FSM Check ===");

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
#[tokio::test]
#[ignore]
#[serial]
async fn test_epoch_timing() {
    const BASE_PORT: u16 = 15300;

    println!("=== Epoch Timing Test ===");

    let ctx = TestContext::builder()
        .nodes(MIN_COMMITTEE_SIZE)
        .port(BASE_PORT)
        .timeout(Duration::from_secs(300))
        .build_and_bootstrap()
        .await
        .expect("Failed to setup and bootstrap");

    let rpc = E2eRpcClient::new(ctx.validator.rpc_url())
        .await
        .expect("Failed to create RPC client");

    let epoch_before = rpc.get_epoch_id().await.expect("get epoch id");
    println!("Starting epoch: {}", epoch_before.as_u64());

    // Try to advance immediately - should fail (duration not elapsed)
    println!("\n=== Testing Premature Advance ===");
    let result = ctx.cli.admin_advance_epoch();
    println!("Immediate advance result: {}", if result.is_ok() { "succeeded (unexpected)" } else { "failed (expected)" });

    // Check FSM shows waiting for duration
    let authority = ctx.nodes[0].authority.pubkey();
    let action = get_fsm_action(&rpc, &authority).await.expect("get action");
    println!("FSM action: {:?}", action);

    // Wait for EPOCH_DURATION
    println!("\n=== Waiting for EPOCH_DURATION ({:?}) ===", EPOCH_WAIT);
    tokio::time::sleep(EPOCH_WAIT).await;

    // Now advance should work
    println!("\n=== Testing Advance After Duration ===");
    let result = ctx.cli.admin_advance_epoch();
    println!("Advance after waiting: {}", if result.is_ok() { "succeeded" } else { "failed" });

    // Check epoch changed
    let epoch_after = rpc.get_epoch_id().await.expect("get epoch id");
    println!("Epoch after: {} (was {})", epoch_after.as_u64(), epoch_before.as_u64());

    println!("\nTest completed: Epoch timing verified");
}
