// Spool FSM
//
// Each spool the node is assigned has an independent lifecycle.
// The FSM is driven by two kinds of input:
//   1. Epoch events  — ownership changes observed from the chain.
//   2. Task results  — outcomes of Sync/Scan/Repair/Recover workers.
//
// ── Epoch events ──────────────────────────────────────────────────
//
//   Foreign  + owned=false  → Foreign           (no-op, not ours)
//   Foreign  + owned=true   → Sync              (newly assigned to us)
//
//   LockedToMove + owned=false → LockedToMove   (still waiting for retention to expire)
//   LockedToMove + owned=true  → Sync           (re-assigned back to us)
//
//   Sync|Scan|Repair|Recover|Active + owned=false → LockedToMove
//       Cancel any in-flight worker. Retain data for `retention_epochs`.
//
//   Sync|Scan|Repair|Recover|Active + owned=true  → Scan
//       New epoch but we still own it. Re-scan to pick up any new tracks
//       or tracks that may have moved during the epoch transition.
//
//   LockedToMove + retention expired (after N epochs) → Foreign + purge
//       Delete all local slice data for this spool.
//
// ── Task results ──────────────────────────────────────────────────
//
//   Sync + Done          → Scan
//   Sync + Unavailable   → Scan
//       Previous owner unreachable. Scan will find the gaps and
//       repair/recover will fetch from the rest of the group.
//
//   Scan + Done { gaps: 0 }   → Active
//   Scan + Done { gaps: > 0 } → Repair
//       Scan populates the pending_repairs queue.
//
//   Repair + Done { unrepairable: 0 } → Active
//   Repair + Done { unrepairable: > 0 } → Recover
//       Repair drains pending_repairs. Tracks it cannot Clay-repair
//       are moved to pending_recoveries for full recovery.
//
//   Recover + Done { remaining: 0 }   → Active
//   Recover + Done { remaining: > 0 } → Recover
//       Full recovery is retried until the queue is empty or the
//       spool is reassigned.

use tape_core::types::EpochNumber;
use tape_store::types::{SpoolState, SpoolStatus};

use crate::features::spool::types::{
    RecoverResult, RepairResult, ScanResult, TaskKind, WorkerDone,
};

/// Action the manager should take for a spool after an epoch event.
pub enum EpochAction {
    /// No worker needed (Active, LockedToMove, Foreign).
    Idle,
    /// We no longer own this spool — mark LockedToMove.
    Lock,
    /// Spawn a worker. If `update` is Some, persist it first.
    Spawn {
        kind: TaskKind,
        update: Option<SpoolState>,
    },
}

/// Decide what to do with a spool at an epoch boundary.
///
/// `persisted` is the last SpoolState from the store (None if the spool
/// has never been ours). `owned` is whether the new epoch assigns it to us.
pub fn on_epoch_event(
    persisted: Option<&SpoolState>,
    owned: bool,
    epoch: EpochNumber,
) -> EpochAction {
    match (persisted, owned) {
        // New spool, assigned to us → Sync
        (None, true) => EpochAction::Spawn {
            kind: TaskKind::Sync,
            update: Some(SpoolState::new(SpoolStatus::Sync, epoch)),
        },

        // No state, not ours → nothing
        (None, false) => EpochAction::Idle,

        // We have state, still own it, same epoch → resume
        (Some(state), true) if state.epoch == epoch => match state.status {
            SpoolStatus::Active | SpoolStatus::LockedToMove => EpochAction::Idle,
            SpoolStatus::Sync => EpochAction::Spawn {
                kind: TaskKind::Sync,
                update: None,
            },
            SpoolStatus::Scan => EpochAction::Spawn {
                kind: TaskKind::Scan,
                update: None,
            },
            SpoolStatus::Repair => EpochAction::Spawn {
                kind: TaskKind::Repair,
                update: None,
            },
            SpoolStatus::Recover => EpochAction::Spawn {
                kind: TaskKind::Recover,
                update: None,
            },
        },

        // We have state, still own it, new epoch → re-scan
        (Some(state), true) => {
            let mut s = *state;
            s.status = SpoolStatus::Scan;
            s.epoch = epoch;
            EpochAction::Spawn {
                kind: TaskKind::Scan,
                update: Some(s),
            }
        }

        // We have state, lost ownership → lock
        (Some(state), false) => {
            if state.is_locked() {
                EpochAction::Idle
            } else {
                EpochAction::Lock
            }
        }
    }
}

/// Decide the next FSM state after a task completes.
///
/// Returns `(next_status, follow_up_task)`. The manager persists the
/// status and spawns the follow-up if present.
pub fn on_task_result(done: &WorkerDone) -> (SpoolStatus, Option<TaskKind>) {
    match done {
        WorkerDone::Sync(_, _, _) => (SpoolStatus::Scan, Some(TaskKind::Scan)),

        WorkerDone::Scan(_, _, ScanResult::Done { gaps: 0 }) => (SpoolStatus::Active, None),
        WorkerDone::Scan(_, _, ScanResult::Done { .. }) => {
            (SpoolStatus::Repair, Some(TaskKind::Repair))
        }

        WorkerDone::Repair(_, _, RepairResult::Done { unrepairable: 0 }) => {
            (SpoolStatus::Active, None)
        }
        WorkerDone::Repair(_, _, RepairResult::Done { .. }) => {
            (SpoolStatus::Recover, Some(TaskKind::Recover))
        }

        WorkerDone::Recover(_, _, RecoverResult::Done { remaining: 0 }) => {
            (SpoolStatus::Active, None)
        }
        WorkerDone::Recover(_, _, RecoverResult::Done { .. }) => {
            (SpoolStatus::Recover, Some(TaskKind::Recover))
        }
    }
}
