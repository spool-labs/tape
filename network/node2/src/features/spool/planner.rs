use tape_core::system::{SpoolState, SpoolStatus};
use tape_core::spooler::SpoolIndex;
use tape_core::types::NodeId;
use tape_protocol::ProtocolState;

use crate::features::spool::types::{SpoolTaskKind, SpoolWorkItem};

pub fn desired_work(
    spool_id: SpoolIndex,
    state: Option<SpoolState>,
) -> Option<SpoolWorkItem> {
    let state = state?;
    let kind = match state.status {
        SpoolStatus::Sync => SpoolTaskKind::Sync,
        SpoolStatus::Scan => SpoolTaskKind::Scan,
        SpoolStatus::Recover => SpoolTaskKind::Recover,
        SpoolStatus::Active | SpoolStatus::LockedToMove => return None,
    };

    Some(SpoolWorkItem {
        spool_id,
        epoch: state.epoch,
        kind,
    })
}

pub fn owned_spools_active(
    state: &ProtocolState,
    node_id: NodeId,
    store_states: &[(SpoolIndex, SpoolState)],
) -> bool {
    let owned = match state.find_member(node_id) {
        Some((index, _)) => state.member_spools(index),
        None => return true,
    };

    owned.into_iter().all(|spool_id| {
        store_states
            .iter()
            .find(|(stored_spool_id, _)| *stored_spool_id == spool_id)
            .map(|(_, spool_state)| spool_state.status == SpoolStatus::Active)
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use tape_core::system::{CommitteeMember, SpoolState, SpoolStatus};
    use tape_core::types::coin::{Coin, TAPE};
    use tape_core::types::{EpochNumber, NodeId};
    use tape_protocol::ProtocolState;

    use super::{desired_work, owned_spools_active};
    use crate::features::spool::types::SpoolTaskKind;

    #[test]
    fn planner_maps_sync_scan_and_recover_to_tasks() {
        assert_eq!(
            desired_work(4, Some(SpoolState::new(SpoolStatus::Sync, EpochNumber(3))))
                .unwrap()
                .kind,
            SpoolTaskKind::Sync
        );
        assert_eq!(
            desired_work(4, Some(SpoolState::new(SpoolStatus::Scan, EpochNumber(3))))
                .unwrap()
                .kind,
            SpoolTaskKind::Scan
        );
        assert_eq!(
            desired_work(4, Some(SpoolState::new(SpoolStatus::Recover, EpochNumber(3))))
                .unwrap()
                .kind,
            SpoolTaskKind::Recover
        );
        assert!(desired_work(4, Some(SpoolState::new(SpoolStatus::Active, EpochNumber(3)))).is_none());
    }

    #[test]
    fn owned_spools_active_requires_all_currently_owned_spools() {
        let mut state = ProtocolState::default();
        state
            .committee
            .push(CommitteeMember::new(NodeId(7), Coin::<TAPE>::new(100)));
        state
            .committee
            .push(CommitteeMember::new(NodeId(8), Coin::<TAPE>::new(100)));
        state.spools.0.fill(1);
        state.spools.0[0] = 0;
        state.spools.0[1] = 0;

        assert!(!owned_spools_active(
            &state,
            NodeId(7),
            &[(0, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))]
        ));

        assert!(owned_spools_active(
            &state,
            NodeId(7),
            &[
                (0, SpoolState::new(SpoolStatus::Active, EpochNumber(1))),
                (1, SpoolState::new(SpoolStatus::Active, EpochNumber(1))),
            ]
        ));
    }
}
