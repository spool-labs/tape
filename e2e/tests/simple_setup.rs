//! Simple setup test for debugging node activation.
//!
//! This test:
//! 1. Creates, registers, stakes, and joins 25 nodes
//! 2. Starts nodes
//! 3. Does a single manual advance epoch call
//! 4. Waits 5 minutes observing state
//!
//! ```bash
//! cargo test -p tape-e2e --test simple_setup -- --ignored --nocapture
//! ```

use std::time::Duration;

use serial_test::serial;
use solana_sdk::signature::Signer;
use tape_e2e::{
    TestContext, TestRpcClient, MIN_COMMITTEE_SIZE, EPOCH_WAIT,
    debug_rpc_state, debug_all_nodes_fsm, get_fsm_action,
};

/// Simple setup test that creates 25 nodes and observes for 5 minutes.
#[tokio::test]
#[ignore]
#[serial]
async fn test_simple_setup() {
    const NUM_NODES: usize = MIN_COMMITTEE_SIZE;
    const BASE_PORT: u16 = 16000;
    const OBSERVE_DURATION: Duration = Duration::from_secs(300); // 5 minutes

    println!("=== Simple Setup Test ({} nodes) ===", NUM_NODES);
    println!("This test will observe the system for {:?}", OBSERVE_DURATION);

    // Build context WITHOUT bootstrapping - we want to manually control the advance
    let mut ctx = TestContext::builder()
        .nodes(NUM_NODES)
        .port(BASE_PORT)
        .stake(1000)
        .timeout(Duration::from_secs(600))
        .build()
        .await
        .expect("Failed to build context");

    let rpc = TestRpcClient::new(ctx.validator.rpc_url())
        .await
        .expect("Failed to create RPC client");

    // Debug initial state
    println!("\n=== Initial State (before starting nodes) ===");
    debug_rpc_state(&rpc, "Initial").await;

    // Show all node stakes
    println!("\n=== Node Stakes (after register/stake/join) ===");
    for node in &ctx.nodes {
        let authority = node.authority.pubkey();
        match rpc.get_node(&authority).await {
            Ok(n) => {
                let scheduled_stake = n.pool.schedule.stake_sum(tape_core::types::EpochNumber(0));
                let scheduled_cancel = n.pool.schedule.cancel_sum(tape_core::types::EpochNumber(0));
                println!(
                    "  {}: id={} stake={} scheduled_stake={} scheduled_cancel={}",
                    node.name,
                    n.id.as_u64(),
                    n.pool.stake.as_u64(),
                    scheduled_stake,
                    scheduled_cancel
                );
            }
            Err(e) => println!("  {}: ERROR - {}", node.name, e),
        }
    }

    // Fund and start all nodes
    println!("\n=== Funding and Starting Nodes ===");
    for (i, node) in ctx.nodes.iter_mut().enumerate() {
        if let Err(e) = ctx.cli.transfer_sol(&node.authority.pubkey(), 1.0) {
            eprintln!("Warning: Failed to fund node {}: {}", i, e);
        }
        if let Err(e) = node.start(&ctx.cli) {
            eprintln!("Warning: Failed to start node {}: {}", i, e);
        }
    }

    // Wait for nodes to initialize
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Show committee state
    println!("\n=== Committee State (after starting nodes) ===");
    debug_rpc_state(&rpc, "After start").await;

    // Show FSM state for all nodes
    println!("\n=== FSM State (before advance) ===");
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "Before advance").await;

    // Wait for EPOCH_DURATION
    println!("\n=== Waiting for EPOCH_DURATION ({:?}) ===", EPOCH_WAIT);
    tokio::time::sleep(EPOCH_WAIT).await;

    // Debug state after waiting
    println!("\n=== State After EPOCH_DURATION ===");
    debug_rpc_state(&rpc, "After EPOCH_DURATION").await;
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "After EPOCH_DURATION").await;

    // Single manual advance epoch call
    println!("\n=== Manually Advancing Epoch ===");
    match ctx.cli.admin_advance_epoch() {
        Ok(_) => println!("Epoch advance succeeded"),
        Err(e) => println!("Epoch advance failed: {}", e),
    }

    // Small delay for transactions to process
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Debug state after advance
    println!("\n=== State After Manual Advance ===");
    debug_rpc_state(&rpc, "After manual advance").await;

    // Now observe for 5 minutes
    println!("\n=== Beginning 5-minute observation period ===");

    let start = std::time::Instant::now();
    let mut last_status = std::time::Instant::now();
    let status_interval = Duration::from_secs(10);

    while start.elapsed() < OBSERVE_DURATION {
        if last_status.elapsed() >= status_interval {
            let elapsed = start.elapsed().as_secs();
            println!("\n--- Status at {}s ---", elapsed);

            // Get current state
            debug_rpc_state(&rpc, &format!("t={}s", elapsed)).await;

            // Count FSM action types
            let mut action_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
            for node in &ctx.nodes {
                let authority = node.authority.pubkey();
                match get_fsm_action(&rpc, &authority).await {
                    Ok(action) => {
                        let key = format!("{:?}", action);
                        // Truncate long action names
                        let key = if key.len() > 30 { key[..30].to_string() + "..." } else { key };
                        *action_counts.entry(key).or_insert(0) += 1;
                    }
                    Err(_) => {
                        *action_counts.entry("ERROR".to_string()).or_insert(0) += 1;
                    }
                }
            }

            println!("  FSM Action Distribution:");
            for (action, count) in &action_counts {
                println!("    {}: {}", action, count);
            }

            // Show a few node details
            println!("  Sample Node States:");
            for node in ctx.nodes.iter().take(3) {
                let authority = node.authority.pubkey();
                if let Ok(n) = rpc.get_node(&authority).await {
                    if let Ok(action) = get_fsm_action(&rpc, &authority).await {
                        println!(
                            "    {}: stake={} action={:?}",
                            node.name,
                            n.pool.stake.as_u64(),
                            action
                        );
                    }
                }
            }

            last_status = std::time::Instant::now();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Final state
    println!("\n=== Final State (after 5 minutes) ===");
    debug_rpc_state(&rpc, "Final").await;
    debug_all_nodes_fsm(&rpc, &ctx.nodes, "Final").await;

    // Show all node stakes at the end
    println!("\n=== Final Node Stakes ===");
    for node in &ctx.nodes {
        let authority = node.authority.pubkey();
        match rpc.get_node(&authority).await {
            Ok(n) => {
                let epoch = rpc.get_epoch().await.map(|e| e.id).unwrap_or(tape_core::types::EpochNumber(0));
                let scheduled_stake = n.pool.schedule.stake_sum(epoch);
                let scheduled_cancel = n.pool.schedule.cancel_sum(epoch);
                println!(
                    "  {}: id={} stake={} scheduled_stake={} scheduled_cancel={} advance_epoch={}",
                    node.name,
                    n.id.as_u64(),
                    n.pool.stake.as_u64(),
                    scheduled_stake,
                    scheduled_cancel,
                    n.latest_advance_epoch.as_u64()
                );
            }
            Err(e) => println!("  {}: ERROR - {}", node.name, e),
        }
    }

    // Check for errors in logs
    if let Err(e) = ctx.check_node_logs() {
        println!("\n=== Node Log Errors ===");
        println!("{}", e);
    }

    println!("\n=== Test Complete ===");
}
