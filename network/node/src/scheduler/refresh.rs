use std::collections::HashSet;
use std::time::Duration;

use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_store::TapeStore;

use tape_store::ops::{CommitteeOps, MetaOps};

use crate::core::committee::our_member_index;
use crate::core::RefreshThrottle;
use crate::Task;

pub struct RefreshPlanner {
    throttle: RefreshThrottle,
}

impl RefreshPlanner {
    pub fn new() -> Self {
        Self {
            throttle: RefreshThrottle::new(),
        }
    }

    pub fn throttle_mut(&mut self) -> &mut RefreshThrottle {
        &mut self.throttle
    }

    /// How often to poll on-chain state. Committee members poll more aggressively
    /// (3s) since they need to observe phase transitions promptly.
    pub fn interval<S: Store>(&self, store: &TapeStore<S>, keypair_pubkey: Pubkey) -> Duration {
        if Self::in_committee(store, keypair_pubkey) {
            Duration::from_secs(3000)
        } else {
            Duration::from_secs(3000)
        }
    }

    /// Whether this node is a member of the current epoch's committee.
    pub fn in_committee<S: Store>(store: &TapeStore<S>, keypair_pubkey: Pubkey) -> bool {
        let Some(epoch) = store.get_chain_epoch().ok().flatten() else {
            return false;
        };
        let Some(committee) = store.get_committee(epoch).ok().flatten() else {
            return false;
        };
        our_member_index(&committee, keypair_pubkey).is_ok()
    }

    /// Add RefreshOnchainState to `desired` if the throttle allows it.
    /// `force` bypasses the throttle (used after epoch transitions and startup).
    /// Returns true if refresh should be scheduled.
    pub fn request<S: Store>(
        &mut self,
        store: &TapeStore<S>,
        keypair_pubkey: Pubkey,
        force: bool,
        desired: &HashSet<Task>,
        scheduled: &HashSet<Task>,
    ) -> bool {
        if desired.contains(&Task::RefreshOnchainState)
            || scheduled.contains(&Task::RefreshOnchainState)
        {
            tracing::trace!("refresh already scheduled");
            return false;
        }

        let current_epoch = store.get_chain_epoch().ok().flatten();
        let interval = self.interval(store, keypair_pubkey);
        let should_schedule = force
            || !self.throttle.should_skip(interval)
            || current_epoch
                .map(|epoch| self.throttle.epoch_changed(epoch))
                .unwrap_or(false);

        if should_schedule {
            tracing::trace!(
                force,
                epoch = ?current_epoch,
                interval_secs = interval.as_secs(),
                "scheduling refresh onchain state"
            );
            self.throttle.record(current_epoch);
            true
        } else {
            tracing::trace!(
                force,
                epoch = ?current_epoch,
                interval_secs = interval.as_secs(),
                "skipping refresh due to throttle"
            );
            false
        }
    }
}
