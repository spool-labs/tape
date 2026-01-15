//! FSM (Finite State Machine) helpers for e2e testing.
//!
//! Provides functions to verify node behavior using the FSM from `tape_api::fsm`.
//! This allows tests to verify that nodes are in the expected state and will
//! take the expected actions.

use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;

use tape_api::fsm::{NodeAction, NodeStateMachine};
use tape_api::prelude::{Epoch, Node, System};

use crate::rpc::E2eRpcClient;
use crate::node::TestNode;
use crate::wait::wait_for_with_desc;

/// Get the FSM action for a node.
///
/// Fetches current System, Epoch, and Node state from the chain,
/// then runs the FSM to determine what action the node should take.
pub async fn get_fsm_action(rpc: &E2eRpcClient, authority: &Pubkey) -> Result<NodeAction> {
    let system = rpc.get_system().await?;
    let epoch = rpc.get_epoch().await?;
    let node = rpc.get_node(authority).await?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("Failed to get system time")?
        .as_secs() as i64;

    Ok(NodeStateMachine::determine_action(&system, &epoch, &node, now))
}

/// Get the FSM action for a node with explicit timestamp.
///
/// Use this when you need to control the time (e.g., testing epoch duration).
pub async fn get_fsm_action_at_time(
    rpc: &E2eRpcClient,
    authority: &Pubkey,
    timestamp: i64,
) -> Result<NodeAction> {
    let system = rpc.get_system().await?;
    let epoch = rpc.get_epoch().await?;
    let node = rpc.get_node(authority).await?;

    Ok(NodeStateMachine::determine_action(&system, &epoch, &node, timestamp))
}

/// Get FSM action from pre-fetched state.
///
/// Use this when you already have the state and want to avoid extra RPC calls.
pub fn get_fsm_action_from_state(system: &System, epoch: &Epoch, node: &Node, timestamp: i64) -> NodeAction {
    NodeStateMachine::determine_action(system, epoch, node, timestamp)
}

/// Assert that a node has the expected FSM action.
///
/// Panics with debug state if the action doesn't match.
pub async fn assert_fsm_action(
    rpc: &E2eRpcClient,
    authority: &Pubkey,
    expected: NodeAction,
    context: &str,
) -> Result<()> {
    let actual = get_fsm_action(rpc, authority).await?;

    if actual != expected {
        // Print debug state before panicking
        debug_fsm(rpc, authority, &format!("Mismatch at '{}'", context)).await;
        bail!(
            "FSM action mismatch at '{}': expected {:?}, got {:?}",
            context,
            expected,
            actual
        );
    }
    Ok(())
}

/// Assert that a node's FSM action matches a predicate.
///
/// Use this for flexible matching (e.g., matching multiple valid actions).
pub async fn assert_fsm_action_matches<F>(
    rpc: &E2eRpcClient,
    authority: &Pubkey,
    predicate: F,
    description: &str,
    context: &str,
) -> Result<()>
where
    F: Fn(&NodeAction) -> bool,
{
    let actual = get_fsm_action(rpc, authority).await?;

    if !predicate(&actual) {
        debug_fsm(rpc, authority, &format!("Mismatch at '{}'", context)).await;
        bail!(
            "FSM action mismatch at '{}': expected {}, got {:?}",
            context,
            description,
            actual
        );
    }
    Ok(())
}

/// Wait for a node's FSM action to match the expected action.
pub async fn wait_for_fsm_action(
    rpc: &E2eRpcClient,
    authority: &Pubkey,
    expected: NodeAction,
    timeout: Duration,
) -> Result<()> {
    wait_for_with_desc(
        &format!("FSM action = {:?}", expected),
        || async {
            match get_fsm_action(rpc, authority).await {
                Ok(actual) => Ok(actual == expected),
                Err(_) => Ok(false),
            }
        },
        timeout,
    )
    .await
}

/// Wait for a node's FSM action to match a predicate.
pub async fn wait_for_fsm_action_matches<F>(
    rpc: &E2eRpcClient,
    authority: &Pubkey,
    predicate: F,
    description: &str,
    timeout: Duration,
) -> Result<()>
where
    F: Fn(&NodeAction) -> bool + Clone,
{
    wait_for_with_desc(
        description,
        || {
            let pred = predicate.clone();
            async move {
                match get_fsm_action(rpc, authority).await {
                    Ok(actual) => Ok(pred(&actual)),
                    Err(_) => Ok(false),
                }
            }
        },
        timeout,
    )
    .await
}

/// Debug print FSM state for a node.
pub async fn debug_fsm(rpc: &E2eRpcClient, authority: &Pubkey, label: &str) {
    let system = match rpc.get_system().await {
        Ok(s) => s,
        Err(e) => {
            println!("[FSM {}] ERROR getting system: {}", label, e);
            return;
        }
    };

    let epoch = match rpc.get_epoch().await {
        Ok(e) => e,
        Err(e) => {
            println!("[FSM {}] ERROR getting epoch: {}", label, e);
            return;
        }
    };

    let node = match rpc.get_node(authority).await {
        Ok(n) => n,
        Err(e) => {
            println!("[FSM {}] ERROR getting node: {}", label, e);
            return;
        }
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let action = NodeStateMachine::determine_action(&system, &epoch, &node, now);

    let phase = if epoch.state.is_syncing() {
        "Syncing"
    } else if epoch.state.is_settling() {
        "Settling"
    } else if epoch.state.is_active() {
        "Active"
    } else {
        "Unknown"
    };

    println!("\n[FSM {}]", label);
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

/// Verify all nodes have the expected FSM action.
pub async fn assert_all_nodes_action(
    rpc: &E2eRpcClient,
    nodes: &[TestNode],
    expected: NodeAction,
    context: &str,
) -> Result<()> {
    for node in nodes {
        let authority = node.authority.pubkey();
        assert_fsm_action(rpc, &authority, expected.clone(), &format!("{} ({})", context, node.name)).await?;
    }
    Ok(())
}

/// Verify all nodes' FSM actions match a predicate.
pub async fn assert_all_nodes_action_matches<F>(
    rpc: &E2eRpcClient,
    nodes: &[TestNode],
    predicate: F,
    description: &str,
    context: &str,
) -> Result<()>
where
    F: Fn(&NodeAction) -> bool + Clone,
{
    for node in nodes {
        let authority = node.authority.pubkey();
        assert_fsm_action_matches(
            rpc,
            &authority,
            predicate.clone(),
            description,
            &format!("{} ({})", context, node.name),
        )
        .await?;
    }
    Ok(())
}

/// Debug print FSM state for all nodes.
pub async fn debug_all_nodes_fsm(rpc: &E2eRpcClient, nodes: &[TestNode], label: &str) {
    println!("\n=== FSM State: {} ===", label);
    for node in nodes {
        let authority = node.authority.pubkey();
        match get_fsm_action(rpc, &authority).await {
            Ok(action) => println!("  {}: {:?}", node.name, action),
            Err(e) => println!("  {}: ERROR - {}", node.name, e),
        }
    }
    println!();
}

/// Check if any node has a blocked FSM action.
pub async fn any_node_blocked(rpc: &E2eRpcClient, nodes: &[TestNode]) -> Result<bool> {
    for node in nodes {
        let authority = node.authority.pubkey();
        let action = get_fsm_action(rpc, &authority).await?;
        if action.is_blocked() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Check if all nodes are waiting (not blocked, not ready to act).
pub async fn all_nodes_waiting(rpc: &E2eRpcClient, nodes: &[TestNode]) -> Result<bool> {
    for node in nodes {
        let authority = node.authority.pubkey();
        let action = get_fsm_action(rpc, &authority).await?;
        if !action.is_waiting() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Check if any node requires a transaction submission.
pub async fn any_node_requires_transaction(rpc: &E2eRpcClient, nodes: &[TestNode]) -> Result<bool> {
    for node in nodes {
        let authority = node.authority.pubkey();
        let action = get_fsm_action(rpc, &authority).await?;
        if action.requires_transaction() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Helper enum for categorizing FSM actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionCategory {
    /// Node needs to sync (SyncEpoch)
    NeedsSync,
    /// Node needs to advance pool (AdvancePool)
    NeedsAdvancePool,
    /// Node needs to join network (JoinNetwork)
    NeedsJoin,
    /// Node can advance epoch (AdvanceEpoch)
    CanAdvanceEpoch,
    /// Node is waiting for something
    Waiting,
    /// Node is blocked
    Blocked,
    /// Unknown action
    Unknown,
}

impl From<&NodeAction> for ActionCategory {
    fn from(action: &NodeAction) -> Self {
        match action {
            NodeAction::SyncEpoch => ActionCategory::NeedsSync,
            NodeAction::AdvancePool => ActionCategory::NeedsAdvancePool,
            NodeAction::JoinNetwork => ActionCategory::NeedsJoin,
            NodeAction::AdvanceEpoch => ActionCategory::CanAdvanceEpoch,
            _ if action.is_blocked() => ActionCategory::Blocked,
            _ if action.is_waiting() => ActionCategory::Waiting,
            _ => ActionCategory::Unknown,
        }
    }
}

/// Categorize a node's FSM action.
pub async fn categorize_action(rpc: &E2eRpcClient, authority: &Pubkey) -> Result<ActionCategory> {
    let action = get_fsm_action(rpc, authority).await?;
    Ok(ActionCategory::from(&action))
}
