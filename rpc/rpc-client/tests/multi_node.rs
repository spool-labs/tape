//! Multi-node integration tests
//!
//! These tests verify flows that require MIN_COMMITTEE_SIZE or more nodes,
//! including post-bootstrap epoch advancement and committee transitions.
//!
//! ## Running Tests
//!
//! ```bash
//! cargo test -p rpc-client --test multi_node -- --ignored --nocapture
//! ```

mod common;

use serial_test::serial;

use common::{
    advance_epoch, advance_pool, build_sync_ix_for_node, create_client, debug_fsm, get_fsm_action,
    initialize_system, join_committee, register_node, setup_validator, stake_to_node, sync_epoch,
    transfer_tape, wait_for_epoch_duration, ValidatorGuard,
};
use solana_sdk::signature::Signer;
use tape_api::fsm::NodeAction;
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::NodeId;

/// Multi-epoch test: Test full epoch flow with MIN_COMMITTEE_SIZE nodes.
///
/// This test registers 25 nodes (MIN_COMMITTEE_SIZE) to verify:
/// 1. Bootstrap phase works (epoch 1→2→3 with any number of nodes)
/// 2. Post-bootstrap flow works (epoch 3→4+ with full committee)
/// 3. Node lifecycle across multiple epochs
#[tokio::test]
#[ignore]
#[serial]
async fn test_multi_epoch_flow() {
    use tape_api::program::{EPOCH_DURATION, MIN_COMMITTEE_SIZE};

    println!("Starting test_multi_epoch_flow...");
    println!(
        "EPOCH_DURATION = {} seconds, MIN_COMMITTEE_SIZE = {}",
        EPOCH_DURATION, MIN_COMMITTEE_SIZE
    );

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let mut nodes: Vec<(solana_sdk::signature::Keypair, solana_sdk::pubkey::Pubkey)> = Vec::new();
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);

    println!("Registering {} nodes...", MIN_COMMITTEE_SIZE);
    for i in 0..MIN_COMMITTEE_SIZE {
        let (node_keypair, node_address) =
            register_node(&client, &payer, &format!("node-{}", i)).await;
        transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;
        stake_to_node(&client, &node_keypair, node_address, stake_amount).await;
        nodes.push((node_keypair, node_address));
        if (i + 1) % 5 == 0 {
            println!("  Registered {} nodes", i + 1);
        }
    }
    println!("All {} nodes registered and staked", MIN_COMMITTEE_SIZE);

    println!("All nodes joining committee...");
    for (node_keypair, node_address) in &nodes {
        join_committee(&client, node_keypair, *node_address)
            .await
            .expect("join");
    }

    let system = client.get_system().await.expect("get system");
    println!("committee_next size: {}", system.committee_next.size());
    assert_eq!(
        system.committee_next.size(),
        MIN_COMMITTEE_SIZE,
        "All nodes should be in committee_next"
    );

    // EPOCH 1 -> 2 (Bootstrap)
    wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
    advance_epoch(&client, &payer)
        .await
        .expect("advance epoch 1->2");
    println!("Advanced to epoch 2");

    let epoch = Box::new(client.get_epoch().await.expect("get epoch"));
    let system = Box::new(client.get_system().await.expect("get system"));
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
        "After advance: Epoch {} in {} state",
        epoch.id.as_u64(),
        phase
    );
    println!(
        "  committee_prev: {}, committee: {}, committee_next: {}",
        system.committee_prev.size(),
        system.committee.size(),
        system.committee_next.size()
    );

    // Fetch node IDs first to avoid repeated large allocations
    let mut node_ids: Vec<NodeId> = Vec::new();
    for (node_keypair, _) in &nodes {
        let node_data = client
            .get_node(&node_keypair.pubkey())
            .await
            .expect("get node");
        node_ids.push(node_data.id);
    }

    let epoch_id = epoch.id;
    let mut synced_count = 0;
    for (i, ((node_keypair, node_address), node_id)) in
        nodes.iter().zip(node_ids.iter()).enumerate()
    {
        let current_epoch = client.get_epoch().await.expect("get epoch");
        if !current_epoch.state.is_syncing() {
            println!(
                "Epoch transitioned out of Syncing after {} nodes synced",
                synced_count
            );
            break;
        }

        let in_committee = system.committee.contains(node_id);
        println!(
            "Node {}: id={}, in_committee={}",
            i,
            node_id.as_u64(),
            in_committee
        );
        let sync_ix =
            build_sync_ix_for_node(&system, epoch_id, *node_id, node_keypair, *node_address)
                .expect(&format!("build sync ix for node {}", i));
        client
            .send_instructions(node_keypair, vec![sync_ix])
            .await
            .expect(&format!("sync node {}", i));
        synced_count += 1;
    }
    println!("Synced {} nodes", synced_count);

    let epoch = client.get_epoch().await.expect("get epoch");
    assert!(
        epoch.state.is_active(),
        "Epoch should be Active after sync (bootstrap)"
    );

    for (node_keypair, node_address) in &nodes {
        advance_pool(&client, node_keypair, *node_address)
            .await
            .expect("advance pool");
    }
    for (node_keypair, node_address) in &nodes {
        join_committee(&client, node_keypair, *node_address)
            .await
            .expect("rejoin");
    }

    // EPOCH 2 -> 3 (Still bootstrap, committee_prev empty)
    wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
    advance_epoch(&client, &payer)
        .await
        .expect("advance epoch 2->3");
    println!("Advanced to epoch 3 (bootstrap complete)");

    let system = Box::new(client.get_system().await.expect("get system"));
    let epoch = Box::new(client.get_epoch().await.expect("get epoch"));
    assert!(
        system.committee_prev.size() > 0,
        "committee_prev should be populated after 2->3"
    );
    assert_eq!(
        system.committee.size(),
        MIN_COMMITTEE_SIZE,
        "committee should have all nodes"
    );

    let epoch_id = epoch.id;
    let mut synced_count = 0;
    for ((node_keypair, node_address), node_id) in nodes.iter().zip(node_ids.iter()) {
        let current_epoch = client.get_epoch().await.expect("get epoch");
        if !current_epoch.state.is_syncing() {
            println!(
                "Epoch transitioned out of Syncing after {} nodes synced",
                synced_count
            );
            break;
        }

        let sync_ix =
            build_sync_ix_for_node(&system, epoch_id, *node_id, node_keypair, *node_address)
                .expect("build sync ix");
        client
            .send_instructions(node_keypair, vec![sync_ix])
            .await
            .expect("sync");
        synced_count += 1;
    }
    println!("Synced {} nodes for epoch 3", synced_count);

    let mut epoch = client.get_epoch().await.expect("get epoch");
    if epoch.state.is_settling() {
        println!("Epoch in Settling, waiting for transition to Active...");
        wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
        epoch = client.get_epoch().await.expect("get epoch");
    }
    println!(
        "Epoch {} state: {:?}",
        epoch.id.as_u64(),
        if epoch.state.is_active() {
            "Active"
        } else {
            "Other"
        }
    );

    for (node_keypair, node_address) in &nodes {
        advance_pool(&client, node_keypair, *node_address)
            .await
            .expect("advance pool");
    }
    for (node_keypair, node_address) in &nodes {
        join_committee(&client, node_keypair, *node_address)
            .await
            .expect("rejoin");
    }

    // EPOCH 3 -> 4 (Post-bootstrap, requires MIN_COMMITTEE_SIZE)
    let system = client.get_system().await.expect("get system");
    assert_eq!(
        system.committee_next.size(),
        MIN_COMMITTEE_SIZE,
        "committee_next should have all nodes"
    );

    wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
    advance_epoch(&client, &payer)
        .await
        .expect("advance epoch 3->4");
    println!("Advanced to epoch 4 (post-bootstrap success!)");

    let epoch = client.get_epoch().await.expect("get epoch");
    assert_eq!(epoch.id.as_u64(), 4, "Should be in epoch 4");

    let system = client.get_system().await.expect("get system");
    assert_eq!(
        system.committee.size(),
        MIN_COMMITTEE_SIZE,
        "Committee should have all nodes"
    );

    println!(
        "\nTEST PASSED: Multi-epoch flow with {} nodes",
        MIN_COMMITTEE_SIZE
    );
    println!("  - Bootstrap: Epoch 1 -> 2 -> 3");
    println!("  - Post-bootstrap: Epoch 3 -> 4");
}

/// Test that epoch advancement is blocked after bootstrap when below MIN_COMMITTEE_SIZE.
///
/// This verifies the "hard gate" behavior:
/// - Bootstrap phase (committee_prev empty): advances with any node count
/// - Post-bootstrap: requires MIN_COMMITTEE_SIZE nodes in committee_next
#[tokio::test]
#[ignore]
#[serial]
async fn test_insufficient_committee_blocks_advance() {
    use tape_api::program::EPOCH_DURATION;

    println!("Starting test_insufficient_committee_blocks_advance...");

    let (validator, payer) = setup_validator().await;
    let _guard = ValidatorGuard::new(validator);
    let client = create_client(_guard.validator());

    initialize_system(&client, &payer).await;
    println!("System initialized");

    let (node_keypair, node_address) = register_node(&client, &payer, "single-node").await;
    let stake_amount = Coin::<TAPE>::new(1_000_000_000);
    transfer_tape(&client, &payer, &node_keypair.pubkey(), stake_amount.as_u64()).await;
    stake_to_node(&client, &node_keypair, node_address, stake_amount).await;
    debug_fsm(&client, &node_keypair, "After stake").await;

    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("join");
    debug_fsm(&client, &node_keypair, "After join").await;
    println!("Single node registered, staked, and joined");

    // EPOCH 1 -> 2 (Bootstrap, allowed)
    wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
    advance_epoch(&client, &payer)
        .await
        .expect("advance epoch 1->2");
    debug_fsm(&client, &node_keypair, "After advance 1->2").await;
    println!("Epoch 1->2: Success (bootstrap)");

    let action = get_fsm_action(&client, &node_keypair).await;
    assert!(
        matches!(action, NodeAction::SyncEpoch),
        "FSM should be SyncEpoch, got {:?}",
        action
    );

    sync_epoch(&client, &node_keypair, node_address)
        .await
        .expect("sync");
    debug_fsm(&client, &node_keypair, "After sync").await;

    advance_pool(&client, &node_keypair, node_address)
        .await
        .expect("advance pool");
    debug_fsm(&client, &node_keypair, "After advance pool").await;

    let action = get_fsm_action(&client, &node_keypair).await;
    assert!(
        matches!(action, NodeAction::JoinNetwork),
        "FSM should be JoinNetwork, got {:?}",
        action
    );

    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("rejoin");
    debug_fsm(&client, &node_keypair, "After rejoin").await;

    // EPOCH 2 -> 3 (Still bootstrap, committee_prev was empty)
    wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
    advance_epoch(&client, &payer)
        .await
        .expect("advance epoch 2->3");
    debug_fsm(&client, &node_keypair, "After advance 2->3").await;
    println!("Epoch 2->3: Success (bootstrap continues)");

    let system = client.get_system().await.expect("get system");
    assert!(
        system.committee_prev.size() > 0,
        "committee_prev should be populated"
    );
    println!(
        "Bootstrap complete: committee_prev now has {} members",
        system.committee_prev.size()
    );

    sync_epoch(&client, &node_keypair, node_address)
        .await
        .expect("sync");
    advance_pool(&client, &node_keypair, node_address)
        .await
        .expect("advance pool");
    join_committee(&client, &node_keypair, node_address)
        .await
        .expect("rejoin");
    debug_fsm(&client, &node_keypair, "After epoch 3 setup").await;

    let action = get_fsm_action(&client, &node_keypair).await;
    assert!(
        matches!(action, NodeAction::WaitForEpochDuration { .. }),
        "FSM should be WaitForEpochDuration, got {:?}",
        action
    );

    // EPOCH 3 -> 4 (Should be BLOCKED - InsufficientCommittee)
    wait_for_epoch_duration((EPOCH_DURATION + 1) as u64).await;
    let result = advance_epoch(&client, &payer).await;

    assert!(result.is_err(), "Epoch 3->4 should be blocked");
    let err_str = result.unwrap_err();
    assert!(
        err_str.contains("0x55"),
        "Error should be InsufficientCommittee (0x55), got: {}",
        err_str
    );

    println!("Epoch 3->4: Blocked with InsufficientCommittee (0x55) - correct!");
    println!("\nTEST PASSED: Hard gate verified");
}
