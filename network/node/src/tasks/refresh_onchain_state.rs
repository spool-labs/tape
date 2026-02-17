//! RefreshOnchainState — fetch and cache current on-chain state.
//!
//! Populates the local store with committee membership, spool assignments,
//! and node identity from on-chain System, Epoch, and Node accounts.

use std::collections::HashSet;
use std::sync::Arc;

use bytemuck::Zeroable;
use rpc::Rpc;
use store::Store;
use tape_api::state::{Epoch, Node, System};
use tape_core::types::NodeId;
use tape_store::ops::{CommitteeOps, MetaOps, SpoolOps};
use tape_store::types::{NodeInfo, NodeStatus, SpoolStatus};
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let system = match context.rpc.get_system().await {
        Ok(s) => s,
        Err(e) => return TaskOutcome::Retryable(format!("get_system: {e}")),
    };

    if cancel.is_cancelled() { return TaskOutcome::Success; }

    let epoch_account = match context.rpc.get_epoch().await {
        Ok(e) => e,
        Err(e) => return TaskOutcome::Retryable(format!("get_epoch: {e}")),
    };

    if cancel.is_cancelled() { return TaskOutcome::Success; }

    let all_nodes = match context.rpc.get_all_nodes().await {
        Ok(n) => n,
        Err(e) => return TaskOutcome::Retryable(format!("get_all_nodes: {e}")),
    };

    apply_refreshed_state(&context, &system, &epoch_account, &all_nodes)
}

/// Pure store logic — testable without RPC.
pub fn apply_refreshed_state<S: Store, R: Rpc>(
    context: &NodeContext<S, R>,
    system: &System,
    epoch_account: &Epoch,
    all_nodes: &[(solana_sdk::pubkey::Pubkey, Node)],
) -> TaskOutcome {
    let epoch = epoch_account.id;

    if let Err(e) = context.store.set_current_epoch(epoch) {
        return TaskOutcome::Retryable(format!("set_current_epoch: {e}"));
    }

    // Build node lookup: NodeId → (on-chain pubkey, Node)
    let node_map: std::collections::HashMap<NodeId, &(solana_sdk::pubkey::Pubkey, Node)> =
        all_nodes.iter().map(|entry| (entry.1.id, entry)).collect();

    // Build current committee
    let current_members = build_committee_members(
        &system.committee,
        &system.spools,
        &node_map,
    );
    if let Err(e) = context.store.put_committee(epoch, current_members) {
        return TaskOutcome::Retryable(format!("put_committee(current): {e}"));
    }

    // Build previous committee (if epoch > 0 and prev committee is non-empty)
    if epoch.0 > 0 && system.committee_prev.size() > 0 {
        let prev_members = build_committee_members(
            &system.committee_prev,
            &system.spools_prev,
            &node_map,
        );
        let prev_epoch = tape_core::types::EpochNumber(epoch.0 - 1);
        if let Err(e) = context.store.put_committee(prev_epoch, prev_members) {
            return TaskOutcome::Retryable(format!("put_committee(prev): {e}"));
        }
    }

    // Find ourselves in the current committee
    let our_bls = match context.bls_keypair.public_key() {
        Ok(pk) => pk,
        Err(e) => return TaskOutcome::Retryable(format!("bls public_key: {e:?}")),
    };

    let our_membership = system.committee.iter().enumerate().find(|(_, cm)| cm.key == our_bls);

    if let Some((member_index, cm)) = our_membership {
        if let Err(e) = context.store.set_node_id(cm.id) {
            return TaskOutcome::Retryable(format!("set_node_id: {e}"));
        }
        if let Err(e) = context.store.set_node_status(NodeStatus::Active) {
            return TaskOutcome::Retryable(format!("set_node_status: {e}"));
        }

        // Reconcile local spool statuses
        let our_spools: HashSet<u16> = system
            .spools
            .spools_for_member(member_index)
            .into_iter()
            .collect();

        let local_spools = match context.store.iter_all_spools() {
            Ok(s) => s,
            Err(e) => return TaskOutcome::Retryable(format!("iter_all_spools: {e}")),
        };
        let local_set: HashSet<u16> = local_spools.iter().map(|(id, _)| *id).collect();

        // Add new spools
        for &spool_id in &our_spools {
            if !local_set.contains(&spool_id) {
                if let Err(e) = context.store.set_spool_status(spool_id, SpoolStatus::ActiveSync) {
                    return TaskOutcome::Retryable(format!("set_spool_status({spool_id}): {e}"));
                }
            }
        }

        // Remove lost spools
        for &spool_id in &local_set {
            if !our_spools.contains(&spool_id) {
                let _ = context.store.remove_spool_status(spool_id);
                let _ = context.store.remove_spool_sync_cursor(spool_id);
            }
        }
    } else {
        if let Err(e) = context.store.set_node_status(NodeStatus::Standby) {
            return TaskOutcome::Retryable(format!("set_node_status: {e}"));
        }
    }

    tracing::info!(?epoch, "refreshed on-chain state");
    TaskOutcome::Success
}

/// Build a Vec<NodeInfo> from an on-chain committee + spool assignment.
fn build_committee_members<const N: usize, const S: usize>(
    committee: &tape_core::system::Committee<N>,
    spools: &tape_core::spooler::SpoolAssignment<S>,
    node_map: &std::collections::HashMap<NodeId, &(solana_sdk::pubkey::Pubkey, Node)>,
) -> Vec<NodeInfo> {
    committee
        .iter()
        .enumerate()
        .map(|(idx, cm)| {
            let (node_address, tls_pubkey, network_address) =
                if let Some(&(pubkey, ref node)) = node_map.get(&cm.id) {
                    (
                        tape_store::types::Pubkey(pubkey.to_bytes()),
                        tape_store::types::Pubkey(node.metadata.network_tls.to_bytes()),
                        node.metadata.network_address,
                    )
                } else {
                    // Node account not found — use zeroed defaults
                    (
                        tape_store::types::Pubkey([0u8; 32]),
                        tape_store::types::Pubkey([0u8; 32]),
                        tape_core::types::network::NetworkAddress::zeroed(),
                    )
                };

            let member_spools = spools.spools_for_member(idx);

            NodeInfo {
                node_address,
                bls_pubkey: cm.key,
                tls_pubkey,
                network_address,
                spools: member_spools,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytemuck::Zeroable;
    use rpc_client::RpcClient;
    use rpc_litesvm::LiteSvmRpc;
    use solana_sdk::signature::Keypair;
    use tape_api::program::MEMBER_COUNT;
    use tape_core::bls::{BlsPrivateKey, BlsPubkey};
    use tape_core::erasure::SPOOL_COUNT;
    use tape_core::system::{Committee, CommitteeMember};
    use tape_core::spooler::SpoolAssignment;
    use tape_core::system::EpochState;
    use tape_core::types::{EpochNumber, NodeId, VersionId};
    use tape_core::types::coin::{Coin, TAPE};
    use tape_core::types::network::NetworkAddress;
    use tape_store::{MemoryStore, TapeStore};

    use crate::core::NodeContext;
    use crate::test_util::test_config;

    fn make_system(
        members: &[CommitteeMember],
        spool_map: [u8; SPOOL_COUNT],
        prev_members: &[CommitteeMember],
        prev_spool_map: [u8; SPOOL_COUNT],
    ) -> System {
        let committee = Committee::<MEMBER_COUNT>::from_members(members);
        let committee_prev = Committee::<MEMBER_COUNT>::from_members(prev_members);
        System {
            version: VersionId(1),
            total_nodes: members.len() as u64,
            committee_prev,
            committee,
            committee_next: Committee::new(),
            spools_prev: SpoolAssignment::new(prev_spool_map),
            spools: SpoolAssignment::new(spool_map),
        }
    }

    fn make_epoch(id: u64) -> Epoch {
        Epoch {
            id: EpochNumber(id),
            state: EpochState::zeroed(),
            last_epoch: 0,
        }
    }

    fn make_node(id: u64, bls_key: BlsPubkey) -> (solana_sdk::pubkey::Pubkey, Node) {
        let pubkey = solana_sdk::pubkey::Pubkey::new_unique();
        let mut node = Node::zeroed();
        node.id = NodeId(id);
        node.authority = pubkey;
        node.metadata.bls_pubkey = bls_key;
        node.metadata.network_address = NetworkAddress::new_ipv4([10, 0, 0, id as u8], 8000);
        node.metadata.network_tls = solana_sdk::pubkey::Pubkey::new_unique();
        (pubkey, node)
    }

    fn make_member(id: u64, stake: u64, bls_key: BlsPubkey) -> CommitteeMember {
        let mut cm = CommitteeMember::new(NodeId(id), Coin::<TAPE>::new(stake));
        cm.key = bls_key;
        cm
    }

    #[test]
    fn apply_committee() {
        let bls_a = BlsPrivateKey::from_random().public_key().unwrap();
        let bls_b = BlsPrivateKey::from_random().public_key().unwrap();

        let mut spool_map = [0u8; SPOOL_COUNT];
        // Member 0 (highest stake, so index 0 after sort) gets spools 0..10
        // Member 1 gets spools 10..20
        for i in 0..10 { spool_map[i] = 0; }
        for i in 10..20 { spool_map[i] = 1; }

        let system = make_system(
            &[make_member(1, 100, bls_a), make_member(2, 50, bls_b)],
            spool_map,
            &[],
            [0u8; SPOOL_COUNT],
        );
        let epoch = make_epoch(5);
        let nodes = vec![make_node(1, bls_a), make_node(2, bls_b)];

        let bls_key = BlsPrivateKey::from_random();
        let store = TapeStore::new(MemoryStore::new());
        let ctx = NodeContext::new(test_config(), Keypair::new(), bls_key, store, RpcClient::from_rpc(LiteSvmRpc::new()));

        let outcome = apply_refreshed_state(&ctx, &system, &epoch, &nodes);
        assert!(matches!(outcome, TaskOutcome::Success));

        let committee = ctx.store.get_committee(EpochNumber(5)).unwrap().unwrap();
        assert_eq!(committee.len(), 2);
        assert!(!committee[0].spools.is_empty());
        assert!(!committee[1].spools.is_empty());
    }

    #[test]
    fn spool_reconciliation() {
        let our_bls_key = BlsPrivateKey::from_random();
        let our_bls = our_bls_key.public_key().unwrap();
        let other_bls = BlsPrivateKey::from_random().public_key().unwrap();

        // Two members: our node (member idx 0, higher stake) gets spools 2,3,4
        // Other node (member idx 1) gets everything else (including spool 1)
        let mut spool_map = [1u8; SPOOL_COUNT]; // default: member 1 owns all
        spool_map[2] = 0;
        spool_map[3] = 0;
        spool_map[4] = 0;

        let system = make_system(
            &[make_member(1, 100, our_bls), make_member(2, 50, other_bls)],
            spool_map,
            &[],
            [0u8; SPOOL_COUNT],
        );
        let epoch = make_epoch(3);
        let nodes = vec![make_node(1, our_bls), make_node(2, other_bls)];

        let store = TapeStore::new(MemoryStore::new());
        // Pre-populate local spools: 1, 2, 3
        store.set_spool_status(1, SpoolStatus::Active).unwrap();
        store.set_spool_status(2, SpoolStatus::Active).unwrap();
        store.set_spool_status(3, SpoolStatus::Active).unwrap();

        let ctx = NodeContext::new(
            test_config(),
            Keypair::new(),
            our_bls_key,
            store,
            RpcClient::from_rpc(LiteSvmRpc::new()),
        );

        let outcome = apply_refreshed_state(&ctx, &system, &epoch, &nodes);
        assert!(matches!(outcome, TaskOutcome::Success));

        let spools = ctx.store.iter_all_spools().unwrap();
        let spool_ids: HashSet<u16> = spools.iter().map(|(id, _)| *id).collect();

        // Spool 1 should be removed (not in new assignment)
        assert!(!spool_ids.contains(&1));
        // Spools 2, 3 should be retained (existing status unchanged)
        assert!(spool_ids.contains(&2));
        assert!(spool_ids.contains(&3));
        // Spool 4 should be added as ActiveSync
        assert!(spool_ids.contains(&4));

        // Verify spool 2 kept its Active status (not overwritten)
        assert_eq!(
            ctx.store.get_spool_status(2).unwrap(),
            Some(SpoolStatus::Active)
        );
        // Verify spool 4 is ActiveSync
        assert_eq!(
            ctx.store.get_spool_status(4).unwrap(),
            Some(SpoolStatus::ActiveSync)
        );
    }

    #[test]
    fn not_in_committee() {
        let other_bls = BlsPrivateKey::from_random().public_key().unwrap();
        let system = make_system(
            &[make_member(1, 100, other_bls)],
            [0u8; SPOOL_COUNT],
            &[],
            [0u8; SPOOL_COUNT],
        );
        let epoch = make_epoch(1);
        let nodes = vec![make_node(1, other_bls)];

        let our_bls_key = BlsPrivateKey::from_random();
        let store = TapeStore::new(MemoryStore::new());
        let ctx = NodeContext::new(
            test_config(),
            Keypair::new(),
            our_bls_key,
            store,
            RpcClient::from_rpc(LiteSvmRpc::new()),
        );

        let outcome = apply_refreshed_state(&ctx, &system, &epoch, &nodes);
        assert!(matches!(outcome, TaskOutcome::Success));

        assert_eq!(
            ctx.store.get_node_status().unwrap(),
            Some(NodeStatus::Standby)
        );
    }

    #[test]
    fn previous_committee() {
        let bls_a = BlsPrivateKey::from_random().public_key().unwrap();
        let bls_b = BlsPrivateKey::from_random().public_key().unwrap();

        let mut prev_spool_map = [0u8; SPOOL_COUNT];
        for i in 0..5 { prev_spool_map[i] = 0; }

        let system = make_system(
            &[make_member(2, 100, bls_b)],
            [0u8; SPOOL_COUNT],
            &[make_member(1, 100, bls_a)],
            prev_spool_map,
        );
        let epoch = make_epoch(5);
        let nodes = vec![make_node(1, bls_a), make_node(2, bls_b)];

        let our_bls_key = BlsPrivateKey::from_random();
        let store = TapeStore::new(MemoryStore::new());
        let ctx = NodeContext::new(
            test_config(),
            Keypair::new(),
            our_bls_key,
            store,
            RpcClient::from_rpc(LiteSvmRpc::new()),
        );

        let outcome = apply_refreshed_state(&ctx, &system, &epoch, &nodes);
        assert!(matches!(outcome, TaskOutcome::Success));

        // Current committee at epoch 5
        let current = ctx.store.get_committee(EpochNumber(5)).unwrap().unwrap();
        assert_eq!(current.len(), 1);

        // Previous committee at epoch 4
        let prev = ctx.store.get_committee(EpochNumber(4)).unwrap().unwrap();
        assert_eq!(prev.len(), 1);
        assert!(!prev[0].spools.is_empty());
    }
}
