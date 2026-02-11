//! NodeStatus FSM — recovery lifecycle state machine.
//!
//! Separate from the on-chain FSM (`NodeStateMachine::determine_action`) which
//! drives AdvanceEpoch/SyncEpoch/etc. NodeStatus tracks *what state the node is in*
//! and drives recovery decisions.
//!
//! All transitions are pure functions. The caller persists via `MetaOps::set_node_status()`.

use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_store::types::NodeStatus;

/// Events that trigger NodeStatus transitions.
#[derive(Debug, Clone)]
pub enum NodeEvent {
    /// An epoch boundary was processed (block processor finished an epoch).
    EpochChanged {
        processed_epoch: EpochNumber,
        latest_epoch: EpochNumber,
        in_committee: bool,
        new_spools: Vec<SpoolIndex>,
    },

    /// Node recovery scan + repairs completed for an epoch.
    RecoveryComplete {
        epoch: EpochNumber,
    },

    /// Metadata sync for newly assigned spools completed.
    MetadataSyncComplete,

    /// Block processor detected the node is behind by `lag` epochs.
    DetectedLag {
        lag: u64,
    },
}

/// Returns true if the node is in a replay state (RecoveryReplay or PartialReplay).
///
/// While replaying, the node should not start new recovery tasks — it must
/// finish catching up with block processing first.
pub fn is_replaying(status: &NodeStatus) -> bool {
    matches!(
        status,
        NodeStatus::RecoveryReplay | NodeStatus::PartialReplay { .. }
    )
}

/// Evaluate a NodeStatus transition given the current state and an event.
///
/// Returns `Some(new_status)` if a transition should occur, `None` if no change.
///
/// Complete transition table:
///
/// | From | Event | Condition | To |
/// |------|-------|-----------|-----|
/// | Standby | EpochChanged | in_committee, new_spools | RecoverMetadata |
/// | Standby | EpochChanged | in_committee, no new_spools | Active |
/// | Active | EpochChanged | in_committee, new_spools | RecoverMetadata |
/// | Active | EpochChanged | !in_committee | Standby |
/// | Active/Standby | EpochChanged | processed < latest (lag) | RecoveryReplay |
/// | RecoverMetadata | MetadataSyncComplete | | Active |
/// | RecoveryReplay | EpochChanged | caught_up, in_committee, new_spools | RecoverMetadata |
/// | RecoveryReplay | EpochChanged | caught_up, in_committee, no new_spools | RecoveryInProgress(epoch) |
/// | RecoveryReplay | EpochChanged | caught_up, !in_committee | Standby |
/// | PartialReplay | EpochChanged | caught_up, in_committee, new_spools | RecoverMetadata |
/// | PartialReplay | EpochChanged | caught_up, in_committee, no new_spools | RecoveryInProgress(epoch) |
/// | PartialReplay | EpochChanged | caught_up, !in_committee | Standby |
/// | RecoveryInProgress | RecoveryComplete | epoch matches | Active |
/// | Any | DetectedLag | lag >= 2 | RecoveryReplay |
pub fn evaluate_transition(current: &NodeStatus, event: &NodeEvent) -> Option<NodeStatus> {
    match (current, event) {
        // Any state → RecoveryReplay when lagging >= 2 epochs
        (_, NodeEvent::DetectedLag { lag }) if *lag >= 2 => Some(NodeStatus::RecoveryReplay),

        // Active/Standby → RecoveryReplay when epoch changes but still lagging
        (
            NodeStatus::Active | NodeStatus::Standby,
            NodeEvent::EpochChanged {
                processed_epoch,
                latest_epoch,
                ..
            },
        ) if *processed_epoch < *latest_epoch => Some(NodeStatus::RecoveryReplay),

        // Standby → RecoverMetadata when joining committee with new spools
        (
            NodeStatus::Standby,
            NodeEvent::EpochChanged {
                in_committee: true,
                new_spools,
                ..
            },
        ) if !new_spools.is_empty() => Some(NodeStatus::RecoverMetadata),

        // Standby → Active when joining committee without new spools
        (
            NodeStatus::Standby,
            NodeEvent::EpochChanged {
                in_committee: true,
                new_spools,
                ..
            },
        ) if new_spools.is_empty() => Some(NodeStatus::Active),

        // Active → RecoverMetadata when epoch changes and we got new spools
        (
            NodeStatus::Active,
            NodeEvent::EpochChanged {
                in_committee: true,
                new_spools,
                ..
            },
        ) if !new_spools.is_empty() => Some(NodeStatus::RecoverMetadata),

        // Active → Standby when no longer in committee
        (
            NodeStatus::Active,
            NodeEvent::EpochChanged {
                in_committee: false,
                ..
            },
        ) => Some(NodeStatus::Standby),

        // RecoverMetadata → Active when metadata sync completes
        (NodeStatus::RecoverMetadata, NodeEvent::MetadataSyncComplete) => {
            Some(NodeStatus::Active)
        }

        // RecoveryReplay caught up — three paths depending on committee + spools
        (
            NodeStatus::RecoveryReplay,
            NodeEvent::EpochChanged {
                processed_epoch,
                latest_epoch,
                in_committee,
                new_spools,
            },
        ) if *processed_epoch >= *latest_epoch => {
            if !in_committee {
                Some(NodeStatus::Standby)
            } else if !new_spools.is_empty() {
                Some(NodeStatus::RecoverMetadata)
            } else {
                // Caught up + in committee + no new spools → must recover certified tracks
                Some(NodeStatus::RecoveryInProgress {
                    epoch: *processed_epoch,
                })
            }
        }

        // PartialReplay caught up — same paths as RecoveryReplay
        (
            NodeStatus::PartialReplay { .. },
            NodeEvent::EpochChanged {
                processed_epoch,
                latest_epoch,
                in_committee,
                new_spools,
            },
        ) if *processed_epoch >= *latest_epoch => {
            if !in_committee {
                Some(NodeStatus::Standby)
            } else if !new_spools.is_empty() {
                Some(NodeStatus::RecoverMetadata)
            } else {
                Some(NodeStatus::RecoveryInProgress {
                    epoch: *processed_epoch,
                })
            }
        }

        // RecoveryInProgress → Active when recovery completes for the right epoch
        (
            NodeStatus::RecoveryInProgress { epoch },
            NodeEvent::RecoveryComplete {
                epoch: completed_epoch,
            },
        ) if epoch == completed_epoch => Some(NodeStatus::Active),

        // No transition
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standby_to_recover_metadata_on_new_spools() {
        let current = NodeStatus::Standby;
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(5),
            latest_epoch: EpochNumber(5),
            in_committee: true,
            new_spools: vec![10, 20],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::RecoverMetadata));
    }

    #[test]
    fn standby_to_active_no_new_spools() {
        let current = NodeStatus::Standby;
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(5),
            latest_epoch: EpochNumber(5),
            in_committee: true,
            new_spools: vec![],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::Active));
    }

    #[test]
    fn active_to_standby_when_not_in_committee() {
        let current = NodeStatus::Active;
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(5),
            latest_epoch: EpochNumber(5),
            in_committee: false,
            new_spools: vec![],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::Standby));
    }

    #[test]
    fn active_to_recover_metadata_on_new_spools() {
        let current = NodeStatus::Active;
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(6),
            latest_epoch: EpochNumber(6),
            in_committee: true,
            new_spools: vec![30],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::RecoverMetadata));
    }

    #[test]
    fn recover_metadata_to_active() {
        let current = NodeStatus::RecoverMetadata;
        let event = NodeEvent::MetadataSyncComplete;
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::Active));
    }

    #[test]
    fn detected_lag_triggers_replay() {
        let current = NodeStatus::Active;
        let event = NodeEvent::DetectedLag { lag: 3 };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::RecoveryReplay));
    }

    #[test]
    fn recovery_replay_caught_up_no_new_spools_to_recovery_in_progress() {
        // RecoveryReplay + caught_up + in_committee + no new spools
        // → RecoveryInProgress(epoch), NOT Active
        let current = NodeStatus::RecoveryReplay;
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(10),
            latest_epoch: EpochNumber(10),
            in_committee: true,
            new_spools: vec![],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(
            next,
            Some(NodeStatus::RecoveryInProgress {
                epoch: EpochNumber(10)
            })
        );
    }

    #[test]
    fn recovery_replay_caught_up_with_new_spools_to_recover_metadata() {
        let current = NodeStatus::RecoveryReplay;
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(10),
            latest_epoch: EpochNumber(10),
            in_committee: true,
            new_spools: vec![5],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::RecoverMetadata));
    }

    #[test]
    fn recovery_replay_caught_up_not_in_committee_to_standby() {
        let current = NodeStatus::RecoveryReplay;
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(10),
            latest_epoch: EpochNumber(10),
            in_committee: false,
            new_spools: vec![],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::Standby));
    }

    #[test]
    fn partial_replay_caught_up_no_new_spools_to_recovery_in_progress() {
        let current = NodeStatus::PartialReplay {
            first_complete_epoch: EpochNumber(3),
            epoch_at_start: EpochNumber(1),
        };
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(10),
            latest_epoch: EpochNumber(10),
            in_committee: true,
            new_spools: vec![],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(
            next,
            Some(NodeStatus::RecoveryInProgress {
                epoch: EpochNumber(10)
            })
        );
    }

    #[test]
    fn partial_replay_caught_up_not_in_committee_to_standby() {
        let current = NodeStatus::PartialReplay {
            first_complete_epoch: EpochNumber(3),
            epoch_at_start: EpochNumber(1),
        };
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(10),
            latest_epoch: EpochNumber(10),
            in_committee: false,
            new_spools: vec![],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::Standby));
    }

    #[test]
    fn recovery_in_progress_to_active() {
        let current = NodeStatus::RecoveryInProgress {
            epoch: EpochNumber(5),
        };
        let event = NodeEvent::RecoveryComplete {
            epoch: EpochNumber(5),
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::Active));
    }

    #[test]
    fn recovery_in_progress_wrong_epoch_no_transition() {
        let current = NodeStatus::RecoveryInProgress {
            epoch: EpochNumber(5),
        };
        let event = NodeEvent::RecoveryComplete {
            epoch: EpochNumber(4),
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, None);
    }

    #[test]
    fn no_transition_when_active_and_no_changes() {
        let current = NodeStatus::Active;
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(5),
            latest_epoch: EpochNumber(5),
            in_committee: true,
            new_spools: vec![],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, None);
    }

    #[test]
    fn active_to_recovery_replay_when_lagging() {
        let current = NodeStatus::Active;
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(4),
            latest_epoch: EpochNumber(5),
            in_committee: true,
            new_spools: vec![],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::RecoveryReplay));
    }

    #[test]
    fn standby_to_recovery_replay_when_lagging() {
        let current = NodeStatus::Standby;
        let event = NodeEvent::EpochChanged {
            processed_epoch: EpochNumber(3),
            latest_epoch: EpochNumber(5),
            in_committee: false,
            new_spools: vec![],
        };
        let next = evaluate_transition(&current, &event);
        assert_eq!(next, Some(NodeStatus::RecoveryReplay));
    }

    #[test]
    fn is_replaying_helper() {
        assert!(is_replaying(&NodeStatus::RecoveryReplay));
        assert!(is_replaying(&NodeStatus::PartialReplay {
            first_complete_epoch: EpochNumber(1),
            epoch_at_start: EpochNumber(0),
        }));
        assert!(!is_replaying(&NodeStatus::Active));
        assert!(!is_replaying(&NodeStatus::Standby));
        assert!(!is_replaying(&NodeStatus::RecoverMetadata));
        assert!(!is_replaying(&NodeStatus::RecoveryInProgress {
            epoch: EpochNumber(5)
        }));
    }
}
