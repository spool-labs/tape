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

// ========== Helpers mirroring staking_inner.move semantics ==========

// Equivalent to Move is_quorum_for_n_shards(weight, n_shards)
pub fn is_quorum_for_n_shards(weight: u64, n_shards: u64) -> bool {
    3 * weight >= 2 * n_shards + 1
}

// Equivalent to Move quorum_above on a priority queue, but implemented on a vec sorted by value desc.
// Input is (value, weight). Returns highest value such that a quorum voted >= this.
pub fn quorum_above(mut votes: Vec<(u64, u16)>, n_shards: u16) -> u64 {
    votes.sort_by(|a, b| b.0.cmp(&a.0)); // max-first
    let mut sum_weight: u64 = 0;
    for (value, weight) in votes {
        sum_weight = sum_weight.saturating_add(weight as u64);
        if is_quorum_for_n_shards(sum_weight, n_shards as u64) {
            return value;
        }
    }
    0
}

// Equivalent to Move quorum_below: start with full weight, remove highest values until quorum breaks.
// Return the value that just broke quorum (i.e., minimum value with quorum support).
pub fn quorum_below(mut votes: Vec<(u64, u16)>, n_shards: u16) -> u64 {
    votes.sort_by(|a, b| b.0.cmp(&a.0)); // max-first
    let mut sum_weight: u64 = n_shards as u64;
    for (value, weight) in votes {
        sum_weight = sum_weight.saturating_sub(weight as u64);
        if !is_quorum_for_n_shards(sum_weight, n_shards as u64) {
            return value;
        }
    }
    0
}

// ========== Epoch parameters (simplified port) ==========

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EpochParams {
    pub capacity: u64,
    pub storage_price: u64,
    pub write_price: u64,
}

impl EpochParams {
    pub fn new(capacity: u64, storage_price: u64, write_price: u64) -> Self {
        Self { capacity, storage_price, write_price }
    }
}

// ========== Pool voting params (simplified) ==========

#[derive(Clone, Debug)]
pub struct VotingParams {
    pub storage_price: u64,
    pub write_price: u64,
    pub node_capacity: u64,
}

#[derive(Clone, Debug)]
pub enum PoolState {
    Active,
}

#[derive(Clone, Debug)]
pub struct StakingPool {
    pub id: NodeId,
    pub state: PoolState,
    pub voting_params: VotingParams,
    pub next_epoch_public_key: Vec<u8>,
}

impl StakingPool {
    pub fn storage_price(&self) -> u64 { self.voting_params.storage_price }
    pub fn write_price(&self) -> u64 { self.voting_params.write_price }
    pub fn node_capacity(&self) -> u64 { self.voting_params.node_capacity }
    pub fn set_storage_price_vote(&mut self, v: u64) { self.voting_params.storage_price = v; }
    pub fn set_write_price_vote(&mut self, v: u64) { self.voting_params.write_price = v; }
    pub fn set_node_capacity_vote(&mut self, v: u64) { self.voting_params.node_capacity = v; }
    pub fn set_next_public_key(&mut self, pk: Vec<u8>) { self.next_epoch_public_key = pk; }
    pub fn next_pubkey(&self) -> &Vec<u8> { &self.next_epoch_public_key }
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

    // For votes/params/public-keys (simplified)
    next_epoch_params: Option<EpochParams>,
    next_epoch_public_keys: Option<BTreeMap<NodeId, Vec<u8>>>,

    // Pools by NodeId
    pools: BTreeMap<NodeId, StakingPool>,

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
            next_epoch_params: None,
            next_epoch_public_keys: None,
            pools: BTreeMap::new(),
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

    // For tests: expose next epoch params
    pub fn next_epoch_params(&self) -> Option<EpochParams> {
        self.next_epoch_params.clone()
    }

    // Ensure a pool exists for a given node id with reasonable defaults.
    fn ensure_pool(&mut self, id: NodeId) {
        if !self.pools.contains_key(&id) {
            self.pools.insert(id, StakingPool {
                id,
                state: PoolState::Active,
                voting_params: VotingParams {
                    storage_price: 1_000,
                    write_price: 1_000,
                    node_capacity: 1_000_000_000, // default 1TB capacity
                },
                next_epoch_public_key: Vec::new(),
            });
        }
    }

    // For tests: set/adjust stake for a node (not the whole staking pool logic).
    pub fn set_stake(&mut self, id: NodeId, stake: u64) {
        if stake == 0 {
            self.stakes.remove(&id);
        } else {
            self.stakes.insert(id, stake);
            self.ensure_pool(id);
        }
    }

    pub fn set_storage_price_vote(&mut self, cap: &StorageNodeCap, price: u64) {
        let id = cap.node_id();
        self.ensure_pool(id);
        if let Some(pool) = self.pools.get_mut(&id) {
            pool.set_storage_price_vote(price);
        }
    }

    pub fn set_write_price_vote(&mut self, cap: &StorageNodeCap, price: u64) {
        let id = cap.node_id();
        self.ensure_pool(id);
        if let Some(pool) = self.pools.get_mut(&id) {
            pool.set_write_price_vote(price);
        }
    }

    pub fn set_node_capacity_vote(&mut self, cap: &StorageNodeCap, capacity: u64) {
        let id = cap.node_id();
        self.ensure_pool(id);
        if let Some(pool) = self.pools.get_mut(&id) {
            pool.set_node_capacity_vote(capacity);
        }
    }

    pub fn set_next_public_key(&mut self, cap: &StorageNodeCap, pk: Vec<u8>) {
        let id = cap.node_id();
        self.ensure_pool(id);
        if let Some(pool) = self.pools.get_mut(&id) {
            pool.set_next_public_key(pk);
        }
    }

    pub fn try_join_active_set(&mut self, cap: &StorageNodeCap) {
        let id = cap.node_id();
        self.ensure_pool(id);
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

    // Port of Move's select_committee_and_calculate_votes
    pub fn select_committee_and_calculate_votes(&mut self) {
        assert!(self.next_committee.is_none(), "Committee already selected");

        // Prepare next committee
        let committee = self.compute_next_committee();

        // Prepare containers
        let mut public_keys: BTreeMap<NodeId, Vec<u8>> = BTreeMap::new();
        let mut write_price_votes: Vec<(u64, u16)> = Vec::new();
        let mut storage_price_votes: Vec<(u64, u16)> = Vec::new();
        let mut capacity_votes: Vec<(u64, u16)> = Vec::new();

        // iterate next committee members
        for (id, shards) in committee.shards_by_node.iter() {
            let weight = shards.len() as u16;
            assert!(weight > 0, "Zero node weight");
            // pool is ensured on set_stake/try_join but be robust
            self.ensure_pool(*id);
            let pool = self.pools.get(id).expect("pool present");

            // Store public key for the node
            public_keys.insert(*id, pool.next_pubkey().clone());

            // collect votes from pool
            let wp = pool.write_price();
            let sp = pool.storage_price();
            // capacity_vote = (pool.node_capacity * n_shards) / weight (clamped to u64)
            let cap_calc = (pool.node_capacity() as u128)
                .saturating_mul(self.n_shards as u128)
                / (weight as u128);
            let cap_vote = cap_calc.min(u64::MAX as u128) as u64;

            write_price_votes.push((wp, weight));
            storage_price_votes.push((sp, weight));
            capacity_votes.push((cap_vote, weight));
        }

        // store public keys and committee
        self.next_epoch_public_keys = Some(public_keys);
        self.next_committee = Some(committee);

        // derive next epoch params using quorum rules
        let capacity = quorum_above(capacity_votes, self.n_shards);
        let storage_price = quorum_below(storage_price_votes, self.n_shards);
        let write_price = quorum_below(write_price_votes, self.n_shards);
        self.next_epoch_params = Some(EpochParams::new(capacity, storage_price, write_price));
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

        // Select next committee and calculate params
        self.select_committee_and_calculate_votes();

        // Next parameters are selected
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
            _ => { /* ignore */ }
        }
    }

    // Mirrors staking::get_current_node_weight
    pub fn get_current_node_weight(&self, node_id: &NodeId) -> u16 {
        assert!(
            self.committee.contains(node_id),
            "Node is not in the committee"
        );
        let w = self
            .committee
            .shards(node_id)
            .map(|v| v.len() as u64)
            .unwrap_or(0);
        assert!(w <= u16::MAX as u64, "Invalid node weight");
        w as u16
    }

    pub fn is_epoch_sync_done(&self) -> bool {
        matches!(self.epoch_state, EpochState::EpochChangeDone(_))
    }
}

// ========== Tests ==========

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

        // Check get_current_node_weight mirrors shard count
        let w1 = staking.get_current_node_weight(&n1);
        let w2 = staking.get_current_node_weight(&n2);
        assert_eq!(w1 as u16 + w2 as u16, 6);
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

    // ===== Quorum helpers tests mirroring Move =====

    #[test]
    fn test_quorum_above() {
        let votes = vec![
            (1, 5), (2, 5), (3, 4), (4, 6), (5, 3),
            (6, 7), (7, 2), (8, 8), (9, 1), (10, 9),
        ];
        assert_eq!(super::quorum_above(votes, 50), 4);
    }

    #[test]
    fn test_quorum_above_all_above() {
        let votes = vec![
            (1, 17), (2, 1), (3, 1), (4, 1), (5, 3),
            (6, 7), (7, 2), (8, 8), (9, 1), (10, 9),
        ];
        assert_eq!(super::quorum_above(votes, 50), 1);
    }

    #[test]
    fn test_quorum_above_one_value() {
        let votes = vec![(1, 50)];
        assert_eq!(super::quorum_above(votes, 50), 1);
    }

    #[test]
    fn test_quorum_below() {
        let votes = vec![
            (1, 5), (2, 5), (3, 4), (4, 6), (5, 3),
            (6, 7), (7, 4), (8, 6), (9, 1), (10, 9),
        ];
        assert_eq!(super::quorum_below(votes, 50), 7);
    }

    #[test]
    fn test_quorum_below_all_below() {
        let votes = vec![
            (1, 5), (2, 5), (3, 4), (4, 6), (5, 3),
            (6, 7), (7, 1), (8, 1), (9, 1), (10, 17),
        ];
        assert_eq!(super::quorum_below(votes, 50), 10);
    }

    #[test]
    fn test_quorum_below_one_value() {
        let votes = vec![(1, 50)];
        assert_eq!(super::quorum_below(votes, 50), 1);
    }

    // ===== Additional tests: epoch sync flow =====

    #[test]
    fn test_epoch_sync_done_flow() {
        let clock = Clock::new(0);
        let mut staking = Staking::new(0, 10_000, 6, &clock);
        let mut system = System::new(6);

        let n1 = NodeId(100);
        let n2 = NodeId(200);

        // set stakes and join active set
        staking.set_stake(n1, 300_000);
        staking.set_stake(n2, 700_000);
        let mut cap1 = StorageNodeCap::new(n1);
        let mut cap2 = StorageNodeCap::new(n2);
        staking.try_join_active_set(&cap1);
        staking.try_join_active_set(&cap2);

        // end voting and start epoch change
        staking.voting_end(&clock);
        staking.initiate_epoch_change(&mut system, &clock);

        // First node attests, should not be done yet (no quorum)
        let epoch = staking.epoch();
        staking.epoch_sync_done(&mut cap1, epoch, &clock);
        assert!(!staking.is_epoch_sync_done());

        // Second node attests, should reach quorum and be done
        staking.epoch_sync_done(&mut cap2, epoch, &clock);
        assert!(staking.is_epoch_sync_done());
    }

    #[test]
    #[should_panic]
    fn test_epoch_sync_done_duplicate() {
        let clock = Clock::new(0);
        let mut staking = Staking::new(0, 10_000, 4, &clock);
        let mut system = System::new(4);

        let n1 = NodeId(11);
        let n2 = NodeId(22);

        staking.set_stake(n1, 300_000);
        staking.set_stake(n2, 700_000);
        let mut cap1 = StorageNodeCap::new(n1);
        let cap2 = StorageNodeCap::new(n2);
        staking.try_join_active_set(&cap1);
        staking.try_join_active_set(&cap2);

        staking.voting_end(&clock);
        staking.initiate_epoch_change(&mut system, &clock);

        let epoch = staking.epoch();
        // first attestation ok
        staking.epoch_sync_done(&mut cap1, epoch, &clock);
        // duplicate should panic
        staking.epoch_sync_done(&mut cap1, epoch, &clock);
    }

    #[test]
    #[should_panic]
    fn test_epoch_sync_wrong_epoch() {
        let clock = Clock::new(0);
        let mut staking = Staking::new(0, 10_000, 4, &clock);
        let mut system = System::new(4);

        let n1 = NodeId(33);
        staking.set_stake(n1, 100_000);
        let mut cap1 = StorageNodeCap::new(n1);
        staking.try_join_active_set(&cap1);

        staking.voting_end(&clock);
        staking.initiate_epoch_change(&mut system, &clock);

        let wrong_epoch = staking.epoch() - 1;
        staking.epoch_sync_done(&mut cap1, wrong_epoch, &clock);
    }

    #[test]
    fn active_set_size_one_eviction() {
        let clock = Clock::new(0);
        let mut staking = Staking::new(0, 1_000, 6, &clock);

        // Limit active set to 1
        staking.set_active_set_max_size(1);
        let n1 = NodeId(1);
        let n2 = NodeId(2);

        staking.set_stake(n1, 1_000);
        staking.try_join_active_set(&StorageNodeCap::new(n1));
        assert_eq!(staking.active_set.active_ids(), vec![n1]);

        // Add higher stake node, should evict n1
        staking.set_stake(n2, 2_000);
        staking.try_join_active_set(&StorageNodeCap::new(n2));
        let ids = staking.active_set.active_ids();
        assert_eq!(ids, vec![n2]);
    }

    #[test]
    fn kick_out_attack_like_scenario() {
        let clock = Clock::new(0);
        let mut staking = Staking::new(0, 1_000, 6, &clock);

        staking.set_active_set_max_size(3);
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let n4 = NodeId(4);

        staking.set_stake(n1, 100);
        staking.set_stake(n2, 200);
        staking.set_stake(n3, 300);

        staking.try_join_active_set(&StorageNodeCap::new(n1));
        staking.try_join_active_set(&StorageNodeCap::new(n2));
        staking.try_join_active_set(&StorageNodeCap::new(n3));
        assert!(staking.active_set.active_ids().contains(&n1));
        assert!(staking.active_set.active_ids().contains(&n2));
        assert!(staking.active_set.active_ids().contains(&n3));

        // Now insert n4 with 101, which should kick out n1
        staking.set_stake(n4, 101);
        staking.try_join_active_set(&StorageNodeCap::new(n4));
        let ids = staking.active_set.active_ids();
        assert!(!ids.contains(&n1));
        assert!(ids.contains(&n2));
        assert!(ids.contains(&n3));
        assert!(ids.contains(&n4));

        // Then "remove" n4 by setting its stake to zero; it should be removed from active set
        staking.set_stake(n4, 0);
        staking.try_join_active_set(&StorageNodeCap::new(n4)); // ensure update path removes it
        let ids = staking.active_set.active_ids();
        assert!(!ids.contains(&n4));
        // n1 doesn't automatically come back (no reserve in our simplified model)
        assert!(!ids.contains(&n1));
    }

    // ===== Test deriving next epoch params with quorum rules =====

    #[test]
    fn test_next_epoch_params_quorum_selection() {
        let clock = Clock::new(0);
        // 3 shards, so each of three nodes will likely get 1 shard with equal stake
        let mut staking = Staking::new(0, 1_000, 3, &clock);

        let n1 = NodeId(10);
        let n2 = NodeId(20);
        let n3 = NodeId(30);

        // Equal stake
        staking.set_stake(n1, 1000);
        staking.set_stake(n2, 1000);
        staking.set_stake(n3, 1000);

        // Join active set
        let cap1 = StorageNodeCap::new(n1);
        let cap2 = StorageNodeCap::new(n2);
        let cap3 = StorageNodeCap::new(n3);
        staking.try_join_active_set(&cap1);
        staking.try_join_active_set(&cap2);
        staking.try_join_active_set(&cap3);

        // Set pool voting params:
        // storage price votes: 100, 300, 200 -> quorum_below should pick the maximum (300)
        staking.set_storage_price_vote(&cap1, 100);
        staking.set_storage_price_vote(&cap2, 300);
        staking.set_storage_price_vote(&cap3, 200);

        // write price votes: 200, 100, 300 -> quorum_below picks 300
        staking.set_write_price_vote(&cap1, 200);
        staking.set_write_price_vote(&cap2, 100);
        staking.set_write_price_vote(&cap3, 300);

        // capacity votes based on node_capacity * n_shards / weight
        // set capacities 10, 20, 30 -> with weight 1, values: 30, 60, 90
        // quorum_above needs total weight >= 3 (need all three), returns the last (smallest) value 30
        staking.set_node_capacity_vote(&cap1, 10);
        staking.set_node_capacity_vote(&cap2, 20);
        staking.set_node_capacity_vote(&cap3, 30);

        // Trigger selection (voting_end calls select_committee_and_calculate_votes)
        staking.voting_end(&clock);

        let params = staking.next_epoch_params().expect("params set");
        assert_eq!(params.capacity, 30);
        assert_eq!(params.storage_price, 300);
        assert_eq!(params.write_price, 300);
    }
}
