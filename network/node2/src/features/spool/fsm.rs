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
//
// ── Runtime events ────────────────────────────────────────────────
//
//   Active  + MissingCertifiedSlice → Repair
//   Repair  + MissingCertifiedSlice → Repair    (already repairing, enqueue)
//   Recover + MissingCertifiedSlice → Recover   (already recovering, enqueue)
//       A certified track is missing its slice locally. Add to
//       pending_repairs and transition to Repair if currently Active.
