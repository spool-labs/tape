use std::collections::HashSet;
use std::sync::Arc;

use rpc::Rpc;
use solana_sdk::signer::Signer;
use store::Store;
use tape_core::spooler::SpoolGroup;
use tape_core::types::EpochNumber;
use tape_store::ops::CommitteeOps;
use tape_store::types::NodeInfo;

use crate::core::NodeContext;
use crate::core::committee::{our_member, our_member_index, our_snapshot_groups};
use crate::snapshot::epoch::{SnapshotNeed, snapshot_epochs};
use crate::supervisor::TaskOutcome;

pub struct SnapshotContext {
    pub current: EpochNumber,
    pub target: EpochNumber,
    pub committee: Vec<NodeInfo>,
    pub groups: HashSet<SpoolGroup>,
    pub member_index: Option<usize>,
    pub owned_spools: Option<usize>,
}

pub fn load_snapshot_context<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    need: SnapshotNeed,
    with_member: bool,
) -> Result<SnapshotContext, TaskOutcome> {
    let (current, target) = match snapshot_epochs(context, need) {
        Ok(value) => value,
        Err(outcome) => return Err(outcome),
    };

    let committee = match context.store.get_committee(current) {
        Ok(Some(committee)) => committee,
        Ok(None) => return Err(TaskOutcome::Retryable("no committee".into())),
        Err(e) => return Err(TaskOutcome::Retryable(format!("read committee: {e}"))),
    };

    let groups = match our_snapshot_groups(&committee, context.keypair.pubkey()) {
        Ok(groups) => groups,
        Err(e) => return Err(TaskOutcome::Retryable(e.into())),
    };

    let member_index = if with_member {
        match our_member_index(&committee, context.keypair.pubkey()) {
            Ok(index) => Some(index),
            Err(e) => return Err(TaskOutcome::Retryable(e.into())),
        }
    } else {
        None
    };

    let owned_spools = if with_member {
        match our_member(&committee, context.keypair.pubkey()) {
            Ok(member) => Some(member.spools.len()),
            Err(e) => return Err(TaskOutcome::Retryable(e.into())),
        }
    } else {
        None
    };

    Ok(SnapshotContext {
        current,
        target,
        committee,
        groups,
        member_index,
        owned_spools,
    })
}
