//! Node events for inter-thread communication.
//!
//! Events flow from Thread A (live updates) to Thread B (network sync)
//! via an mpsc channel.

use solana_sdk::pubkey::Pubkey;
use tape_core::prelude::*;
use tape_core::spooler::SpoolIndex;
use tape_crypto::Hash;

/// Events emitted by Thread A and consumed by Thread B.
#[derive(Debug, Clone)]
pub enum NodeEvent {
    // -------------------------------------------------------------------------
    // Events from Thread A (block processing)
    // -------------------------------------------------------------------------
    /// A new epoch has started on chain.
    EpochAdvanced {
        /// The new epoch number.
        epoch: EpochNumber,
    },

    /// A node submitted SyncEpoch (including possibly ourselves).
    NodeSynced {
        /// The node that synced.
        node: Pubkey,
        /// The epoch they synced for.
        epoch: EpochNumber,
        /// Hash of their synced spools.
        spools_hash: Hash,
    },

    // -------------------------------------------------------------------------
    // Internal events (from Thread B to itself or future threads)
    // -------------------------------------------------------------------------
    /// Spool sync completed successfully.
    SpoolSyncComplete {
        /// The spool that was synced.
        spool_idx: SpoolIndex,
        /// Number of slices synced.
        slice_count: usize,
    },

    /// Spool sync failed, needs erasure recovery.
    SpoolRecoveryNeeded {
        /// The spool that needs recovery.
        spool_idx: SpoolIndex,
    },

    /// Quorum of nodes have synced, we can submit our SyncEpoch.
    EpochSyncReady {
        /// The epoch we're ready to sync.
        epoch: EpochNumber,
    },
}
