//ref: https://github.com/MystenLabs/walrus/blob/main/contracts/walrus/sources/staking/staking_inner.move

use std::collections::BTreeMap;
use super::{assign_shards, move_shards, map_shard_indices};
use crate::types::NodeId;

// ========== Core Types ==========


#[derive(Clone, Debug, Default)]
pub struct Committee {
    // Sorted mapping: NodeId -> assigned shards
    pub shards_by_node: BTreeMap<NodeId, Vec<u16>>,
}

impl Committee {
    pub fn new(shards_by_node: BTreeMap<NodeId, Vec<u16>>) -> Self {
        Committee { shards_by_node }
    }
    pub fn size(&self) -> usize {
        self.shards_by_node.len()
    }
    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.shards_by_node.contains_key(node_id)
    }
    pub fn shards(&self, node_id: &NodeId) -> Option<&Vec<u16>> {
        self.shards_by_node.get(node_id)
    }
    pub fn total_shards(&self) -> u16 {
        self.shards_by_node
            .values()
            .map(|v| v.len() as u16)
            .sum::<u16>()
    }
}


// ========== Clock ==========

#[derive(Clone, Debug)]
pub struct Clock {
    ts_ms: u64,
}
impl Clock {
    pub fn new(now_ms: u64) -> Self {
        Self { ts_ms: now_ms }
    }
    pub fn timestamp_ms(&self) -> u64 {
        self.ts_ms
    }
    pub fn increment(&mut self, delta_ms: u64) {
        self.ts_ms = self.ts_ms.saturating_add(delta_ms);
    }
}

// ========== System (minimal) ==========

#[derive(Clone, Debug)]
pub struct System {
    pub epoch: u32,
    pub committee: Committee,
    pub n_shards: u16,
}

impl System {
    pub fn new(n_shards: u16) -> Self {
        Self {
            epoch: 0,
            committee: Committee::default(),
            n_shards,
        }
    }
}

// ========== StorageNodeCap (minimal) ==========

#[derive(Clone, Debug)]
pub struct StorageNodeCap {
    node_id: NodeId,
    last_epoch_sync_done: u32,
}

impl StorageNodeCap {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            last_epoch_sync_done: 0,
        }
    }
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
    pub fn last_epoch_sync_done(&self) -> u32 {
        self.last_epoch_sync_done
    }
    pub fn set_last_epoch_sync_done(&mut self, epoch: u32) {
        self.last_epoch_sync_done = epoch;
    }
}

// ========== Active Set (bounded) ==========

#[derive(Clone, Debug)]
pub struct ActiveSet {
    max_size: usize,
    nodes: BTreeMap<NodeId, u64>, // NodeId -> stake
}

impl ActiveSet {
    pub fn new(max_size: usize) -> Self {
        assert!(max_size > 0);
        Self {
            max_size,
            nodes: BTreeMap::new(),
        }
    }

    pub fn set_max_size(&mut self, max_size: usize) {
        assert!(max_size > 0);
        self.max_size = max_size;
        self.truncate_to_max();
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    pub fn active_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().cloned().collect()
    }

    pub fn stake(&self, id: &NodeId) -> u64 {
        *self.nodes.get(id).unwrap_or(&0)
    }

    // Insert/update node by stake, potentially evicting the smallest if full.
    pub fn insert_or_update(&mut self, id: NodeId, stake: u64) {
        if stake == 0 {
            self.nodes.remove(&id);
            return;
        }
        if self.nodes.contains_key(&id) {
            self.nodes.insert(id, stake);
            self.truncate_to_max();
            return;
        }
        if self.size() < self.max_size {
            self.nodes.insert(id, stake);
            return;
        }
        // At capacity; replace the minimal-stake entry if this one exceeds it
        if let Some((min_id, min_stake)) = self.min_entry() {
            if stake > min_stake {
                self.nodes.remove(&min_id);
                self.nodes.insert(id, stake);
            }
        }
    }

    pub fn as_stake_map(&self) -> BTreeMap<NodeId, u64> {
        self.nodes.clone()
    }

    fn min_entry(&self) -> Option<(NodeId, u64)> {
        self
            .nodes
            .iter()
            .min_by_key(|(_, s)| **s)
            .map(|(k, v)| (*k, *v))
    }

    fn truncate_to_max(&mut self) {
        while self.size() > self.max_size {
            if let Some((min_id, _)) = self.min_entry() {
                self.nodes.remove(&min_id);
            } else {
                break;
            }
        }
    }
}

// ========== Epoch State ==========

#[derive(Clone, Debug)]
pub enum EpochState {
    EpochChangeSync(u16),     // aggregate weight attested
    EpochChangeDone(u64),     // timestamp of done
    NextParamsSelected(u64),  // timestamp of the start of current epoch
}

// ========== Staking (core logic port) ==========

#[derive(Clone, Debug)]
pub struct Staking {
    // Parameters
    n_shards: u16,
    epoch_duration_ms: u64,
    first_epoch_start_ms: u64,

    // State
    epoch: u32,
    active_set: ActiveSet,
    committee: Committee,
    previous_committee: Committee,
    next_committee: Option<Committee>,
    epoch_state: EpochState,

    // stakes-by-node for testing and active set integration
    stakes: BTreeMap<NodeId, u64>,
}

impl Staking {
    pub fn new(epoch_zero_duration_ms: u64, epoch_duration_ms: u64, n_shards: u16, clock: &Clock) -> Self {
        Staking {
            n_shards,
            epoch_duration_ms,
            first_epoch_start_ms: clock.timestamp_ms().saturating_add(epoch_zero_duration_ms),
            epoch: 0,
            active_set: ActiveSet::new(1000), // default max active size
            committee: Committee::default(),
            previous_committee: Committee::default(),
            next_committee: None,
            epoch_state: EpochState::EpochChangeDone(clock.timestamp_ms()),
            stakes: BTreeMap::new(),
        }
    }

    // Helper: 2f+1 quorum
    fn is_quorum(&self, weight: u16) -> bool {
        let w = weight as u64;
        let n = self.n_shards as u64;
        3 * w >= 2 * n + 1
    }

    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    pub fn committee(&self) -> &Committee {
        &self.committee
    }

    pub fn previous_committee(&self) -> &Committee {
        &self.previous_committee
    }

    pub fn set_active_set_max_size(&mut self, max_size: usize) {
        self.active_set.set_max_size(max_size);
    }

    // For tests: set/adjust stake for a node (not the whole staking pool logic).
    pub fn set_stake(&mut self, id: NodeId, stake: u64) {
        if stake == 0 {
            self.stakes.remove(&id);
        } else {
            self.stakes.insert(id, stake);
        }
    }

    pub fn try_join_active_set(&mut self, cap: &StorageNodeCap) {
        let id = cap.node_id();
        let st = *self.stakes.get(&id).unwrap_or(&0);
        self.active_set.insert_or_update(id, st);
    }

    pub fn compute_next_committee(&self) -> Committee {
        let stake_by_node = self.active_set.as_stake_map();

        // Use provided assign_shards helper: NodeId->u16
        let counts = assign_shards(&stake_by_node, self.n_shards);

        if self.committee.size() == 0 {
            // First epoch: sequential assignment
            let shards_by_node = map_shard_indices(counts);
            return Committee::new(shards_by_node);
        }

        // Transition preserving shard placement where possible
        let new_shards_by_node = move_shards(&self.committee.shards_by_node, counts);
        Committee::new(new_shards_by_node)
    }

    pub fn voting_end(&mut self, clock: &Clock) {
        let last_epoch_change_ts = match self.epoch_state {
            EpochState::EpochChangeDone(ts) => ts,
            _ => panic!("Wrong epoch state for voting_end"),
        };
        let now = clock.timestamp_ms();

        if self.epoch != 0 {
            let param_selection_delta = self.epoch_duration_ms / 2;
            assert!(
                now >= last_epoch_change_ts + param_selection_delta,
                "Too early to end voting"
            );
        } else {
            // Epoch zero: allow end-of-voting from the configured first start time
            assert!(now >= self.first_epoch_start_ms, "Too early for epoch zero");
        }

        // Select next committee using current active set and stakes
        let next = self.compute_next_committee();
        self.next_committee = Some(next);
        // Next parameters are conceptually selected here; we skip param details.
        self.epoch_state = EpochState::NextParamsSelected(last_epoch_change_ts);
    }

    pub fn advance_epoch(&mut self) {
        assert!(self.next_committee.is_some(), "Next committee not set");
        self.epoch = self.epoch.saturating_add(1);
        self.previous_committee = std::mem::take(&mut self.committee);
        self.committee = self.next_committee.take().unwrap(); // set new
        self.epoch_state = EpochState::EpochChangeSync(0);
    }

    pub fn initiate_epoch_change(&mut self, system: &mut System, clock: &Clock) {
        let last_epoch_change_ts = match self.epoch_state {
            EpochState::NextParamsSelected(ts) => ts,
            _ => panic!("Wrong epoch state"),
        };
        let now = clock.timestamp_ms();

        if self.epoch == 0 {
            assert!(now >= self.first_epoch_start_ms, "Too early to start first epoch");
        } else {
            assert!(
                now >= last_epoch_change_ts + self.epoch_duration_ms,
                "Too early to change epoch"
            );
        }

        self.advance_epoch();

        // Update system to reflect new epoch and committee.
        system.epoch = self.epoch;
        system.n_shards = self.n_shards;
        system.committee = self.committee.clone();
    }

    pub fn epoch_sync_done(&mut self, cap: &mut StorageNodeCap, epoch: u32, clock: &Clock) {
        assert_eq!(epoch, self.epoch, "Invalid sync epoch");
        assert!(
            cap.last_epoch_sync_done() < self.epoch,
            "Duplicate sync_done for this epoch"
        );
        let id = cap.node_id();
        assert!(
            self.committee.contains(&id),
            "Node is not in the committee"
        );
        cap.set_last_epoch_sync_done(self.epoch);

        let node_weight = self
            .committee
            .shards(&id)
            .map(|v| v.len() as u16)
            .unwrap_or(0);

        match self.epoch_state.clone() {
            EpochState::EpochChangeSync(weight) => {
                let total = weight.saturating_add(node_weight);
                if self.is_quorum(total) {
                    self.epoch_state = EpochState::EpochChangeDone(clock.timestamp_ms());
                } else {
                    self.epoch_state = EpochState::EpochChangeSync(total);
                }
            }
            // If already done or wrong state, we still emit shards_received semantics.
            _ => {
                // It's safe to ignore or retain current state.
            }
        }
    }
}

// ========== Example unit tests (basic flow without relying on shard alloc impls) ==========

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_epoch_progression() {
        let mut clock = Clock::new(1_000);
        let mut staking = Staking::new(500, 1_000, 6, &clock);
        let mut system = System::new(6);

        // Two nodes with enough stake
        let n1 = NodeId(1);
        let n2 = NodeId(2);

        staking.set_stake(n1, 1000);
        staking.set_stake(n2, 1000);
        staking.try_join_active_set(&StorageNodeCap::new(n1));
        staking.try_join_active_set(&StorageNodeCap::new(n2));

        // End voting and select next committee
        clock.increment(500); // half of epoch zero duration satisfied
        staking.voting_end(&clock);

        // Initiate epoch change
        clock.increment(500);
        let mut cap1 = StorageNodeCap::new(n1);
        let mut cap2 = StorageNodeCap::new(n2);
        staking.initiate_epoch_change(&mut system, &clock);

        // Epoch sync done by both nodes -> quorum
        staking.epoch_sync_done(&mut cap1, staking.epoch(), &clock);
        staking.epoch_sync_done(&mut cap2, staking.epoch(), &clock);

        assert_eq!(staking.epoch(), 1);
        assert_eq!(system.epoch, 1);
        assert!(staking.committee().contains(&n1));
        assert!(staking.committee().contains(&n2));
        assert_eq!(staking.committee().total_shards(), 6);
    }

    #[test]
    fn active_set_eviction_and_rejoin() {
        let clock = Clock::new(0);
        let mut staking = Staking::new(0, 1_000, 6, &clock);

        // Limit active set to 2
        staking.set_active_set_max_size(2);
        let n1 = NodeId(10);
        let n2 = NodeId(20);
        let n3 = NodeId(30);

        staking.set_stake(n1, 100);
        staking.set_stake(n2, 200);
        staking.set_stake(n3, 300);

        staking.try_join_active_set(&StorageNodeCap::new(n1));
        staking.try_join_active_set(&StorageNodeCap::new(n2));
        // Active set contains n1, n2
        assert!(staking.active_set.active_ids().contains(&n1));
        assert!(staking.active_set.active_ids().contains(&n2));
        assert!(!staking.active_set.active_ids().contains(&n3));

        // Now try to join n3 (higher stake) -> should evict the lowest (n1)
        staking.try_join_active_set(&StorageNodeCap::new(n3));
        let ids = staking.active_set.active_ids();
        assert!(ids.contains(&n2) && ids.contains(&n3));
        assert!(!ids.contains(&n1));
    }
}
