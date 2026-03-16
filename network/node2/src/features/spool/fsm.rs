use tape_core::system::{SpoolState, SpoolStatus};
use tape_core::types::{EpochNumber, NodeId};
use tape_slicer::SPOOL_GROUP_SIZE;

use crate::features::spool::types::{SpoolEvent, SpoolTaskSummary};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpoolTransition {
    pub next_state: Option<SpoolState>,
    pub clear_pending_recoveries: bool,
    pub clear_sync_cursor: bool,
    pub purge_local: bool,
}

impl SpoolTransition {
    fn keep(current: Option<SpoolState>) -> Self {
        Self {
            next_state: current,
            clear_pending_recoveries: false,
            clear_sync_cursor: false,
            purge_local: false,
        }
    }

    fn replace(next_state: Option<SpoolState>) -> Self {
        Self {
            next_state,
            clear_pending_recoveries: false,
            clear_sync_cursor: false,
            purge_local: false,
        }
    }
}

pub fn apply(
    current: Option<SpoolState>,
    event: &SpoolEvent,
    locked_spool_retention_epochs: u64,
) -> SpoolTransition {
    match event {
        SpoolEvent::EpochReconcile {
            epoch,
            owned,
            prev_owner,
            prev_helpers,
            ..
        } => apply_epoch_reconcile(
            current,
            *epoch,
            *owned,
            *prev_owner,
            *prev_helpers,
            locked_spool_retention_epochs,
        ),
        SpoolEvent::TaskSummary { summary, .. } => apply_task_summary(current, summary),
        SpoolEvent::MissingCertifiedSlice { .. } => apply_missing_certified_slice(current),
    }
}

fn apply_epoch_reconcile(
    current: Option<SpoolState>,
    epoch: EpochNumber,
    owned: bool,
    prev_owner: Option<NodeId>,
    prev_helpers: [Option<NodeId>; SPOOL_GROUP_SIZE],
    locked_spool_retention_epochs: u64,
) -> SpoolTransition {
    match (current, owned) {

        // Foreign + EpochReconcile(owned=false) -> Foreign
        (None, false) => SpoolTransition::keep(None),

        // Foreign + EpochReconcile(owned=true) -> Sync
        (None, true) => sync_transition(epoch, prev_owner, prev_helpers),

        // Duplicate reconcile within the current epoch: preserve existing owned state.
        (Some(state), true) if state.epoch == epoch && !state.is_locked() => {
            SpoolTransition::keep(Some(state))
        }

        // LockedToMove + EpochReconcile(owned=true) -> Sync
        (Some(state), true) if state.is_locked() => sync_transition(epoch, prev_owner, prev_helpers),

        // Sync|Scan|Recover|Active + EpochReconcile(owned=true) -> Scan
        (Some(_), true) => scan_transition(epoch, prev_owner, prev_helpers),

        // LockedToMove + RetentionExpired(after 4 epochs) -> Foreign + purge
        (Some(state), false)
            if state.is_locked()
                && retention_expired(state, epoch, locked_spool_retention_epochs) =>
        {
            let mut transition = SpoolTransition::replace(None);
            transition.clear_pending_recoveries = true;
            transition.clear_sync_cursor = true;
            transition.purge_local = true;
            transition
        }

        // LockedToMove + EpochReconcile(owned=false) -> LockedToMove
        (Some(state), false) if state.is_locked() => SpoolTransition::keep(Some(state)),

        // Sync|Scan|Recover|Active + EpochReconcile(owned=false) -> LockedToMove
        (Some(_), false) => {
            let mut transition = SpoolTransition::replace(Some(SpoolState::new(
                SpoolStatus::LockedToMove,
                epoch,
            )));
            transition.clear_pending_recoveries = true;
            transition.clear_sync_cursor = true;
            transition
        }
    }
}

fn apply_task_summary(
    current: Option<SpoolState>,
    summary: &SpoolTaskSummary,
) -> SpoolTransition {
    let Some(state) = current else {
        return SpoolTransition::keep(None);
    };

    match (state.status, summary) {
        (SpoolStatus::Sync, SpoolTaskSummary::SyncDone | SpoolTaskSummary::SyncUnavailable) => {
            let mut transition = SpoolTransition::replace(Some(SpoolState {
                status: SpoolStatus::Scan,
                ..state
            }));
            transition.clear_pending_recoveries = true;
            transition.clear_sync_cursor = true;
            transition
        }
        (SpoolStatus::Scan, SpoolTaskSummary::ScanDone { gaps: 0 }) => {
            let mut transition = SpoolTransition::replace(Some(SpoolState {
                status: SpoolStatus::Active,
                ..state
            }));
            transition.clear_pending_recoveries = true;
            transition
        }
        (SpoolStatus::Scan, SpoolTaskSummary::ScanDone { .. }) => {
            SpoolTransition::replace(Some(SpoolState {
                status: SpoolStatus::Recover,
                ..state
            }))
        }
        (SpoolStatus::Recover, SpoolTaskSummary::RecoverDone { remaining: 0 }) => {
            let mut transition = SpoolTransition::replace(Some(SpoolState {
                status: SpoolStatus::Active,
                ..state
            }));
            transition.clear_pending_recoveries = true;
            transition
        }
        (SpoolStatus::Recover, SpoolTaskSummary::RecoverDone { .. }) => {
            SpoolTransition::keep(Some(state))
        }
        _ => SpoolTransition::keep(Some(state)),
    }
}

fn apply_missing_certified_slice(current: Option<SpoolState>) -> SpoolTransition {
    let Some(state) = current else {
        return SpoolTransition::keep(None);
    };

    match state.status {
        SpoolStatus::Active => SpoolTransition::replace(Some(SpoolState {
            status: SpoolStatus::Recover,
            ..state
        })),
        SpoolStatus::Recover => SpoolTransition::keep(Some(state)),
        _ => SpoolTransition::keep(Some(state)),
    }
}

fn sync_transition(
    epoch: EpochNumber,
    prev_owner: Option<NodeId>,
    prev_helpers: [Option<NodeId>; SPOOL_GROUP_SIZE],
) -> SpoolTransition {
    let mut transition = SpoolTransition::replace(Some(SpoolState {
        status: SpoolStatus::Sync,
        epoch,
        prev_owner,
        prev_helpers,
    }));
    transition.clear_pending_recoveries = true;
    transition.clear_sync_cursor = true;
    transition
}

fn scan_transition(
    epoch: EpochNumber,
    prev_owner: Option<NodeId>,
    prev_helpers: [Option<NodeId>; SPOOL_GROUP_SIZE],
) -> SpoolTransition {
    let mut transition = SpoolTransition::replace(Some(SpoolState {
        status: SpoolStatus::Scan,
        epoch,
        prev_owner,
        prev_helpers,
    }));
    transition.clear_pending_recoveries = true;
    transition.clear_sync_cursor = true;
    transition
}

fn retention_expired(state: SpoolState, epoch: EpochNumber, retention_epochs: u64) -> bool {
    state
        .epoch
        .as_u64()
        .saturating_add(retention_epochs)
        <= epoch.0
}

#[cfg(test)]
mod tests {
    use super::apply;

    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_core::types::{EpochNumber, NodeId};
    use tape_slicer::SPOOL_GROUP_SIZE;

    use crate::features::spool::types::{
        SpoolEvent, SpoolTaskKind, SpoolTaskSummary, SpoolWorkItem
    };

    #[test]
    fn foreign_owned_transitions_to_sync() {
        let event = SpoolEvent::EpochReconcile {
            spool_id: 5,
            epoch: EpochNumber(8),
            owned: true,
            prev_owner: Some(NodeId(9)),
            prev_helpers: [None; SPOOL_GROUP_SIZE],
        };

        let transition = apply(None, &event, 4);

        assert!(transition.clear_pending_recoveries);
        assert!(transition.clear_sync_cursor);
        assert_eq!(
            transition.next_state,
            Some(SpoolState {
                status: SpoolStatus::Sync,
                epoch: EpochNumber(8),
                prev_owner: Some(NodeId(9)),
                prev_helpers: [None; SPOOL_GROUP_SIZE],
            })
        );
    }

    #[test]
    fn same_epoch_owned_state_is_preserved() {
        let current = Some(SpoolState::new(SpoolStatus::Recover, EpochNumber(8)));
        let event = SpoolEvent::EpochReconcile {
            spool_id: 5,
            epoch: EpochNumber(8),
            owned: true,
            prev_owner: None,
            prev_helpers: [None; SPOOL_GROUP_SIZE],
        };

        let transition = apply(current, &event, 4);

        assert_eq!(transition.next_state, current);
        assert!(!transition.clear_pending_recoveries);
    }

    #[test]
    fn active_missing_certified_slice_enters_recover() {
        let transition = apply(
            Some(SpoolState::new(SpoolStatus::Active, EpochNumber(9))),
            &SpoolEvent::MissingCertifiedSlice {
                spool_id: 2,
                track: tape_store::types::Pubkey::new_unique(),
            },
            4,
        );

        assert_eq!(
            transition.next_state,
            Some(SpoolState::new(SpoolStatus::Recover, EpochNumber(9)))
        );
    }

    #[test]
    fn stale_locked_spool_expires_on_reconcile() {
        let transition = apply(
            Some(SpoolState::new(SpoolStatus::LockedToMove, EpochNumber(3))),
            &SpoolEvent::EpochReconcile {
                spool_id: 7,
                epoch: EpochNumber(7),
                owned: false,
                prev_owner: None,
                prev_helpers: [None; SPOOL_GROUP_SIZE],
            },
            4,
        );

        assert_eq!(transition.next_state, None);
        assert!(transition.purge_local);
    }

    #[test]
    fn sync_summary_advances_to_scan() {
        let transition = apply(
            Some(SpoolState::new(SpoolStatus::Sync, EpochNumber(4))),
            &SpoolEvent::TaskSummary {
                work: SpoolWorkItem {
                    spool_id: 11,
                    epoch: EpochNumber(4),
                    kind: SpoolTaskKind::Sync,
                },
                summary: SpoolTaskSummary::SyncDone,
            },
            4,
        );

        assert_eq!(
            transition.next_state,
            Some(SpoolState::new(SpoolStatus::Scan, EpochNumber(4)))
        );
        assert!(transition.clear_sync_cursor);
    }
}
