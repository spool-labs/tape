
use std::collections::BTreeMap;
use super::{assign_shards, move_shards, map_shard_indices};
use crate::types::NodeId;
use bytemuck::{Pod, Zeroable};

// ========== Limits for fixed-size POD structs ==========
const MAX_ACTIVE_SET: usize = 128;
const MAX_COMMITTEE_MEMBERS: usize = MAX_ACTIVE_SET;
const MAX_COMMITTEE_SHARDS: usize = 1024;

// ========== Core Types (fixed-size, POD) ==========

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CommitteePod<const M: usize, const S: usize> {
    // Number of active members in this committee [0..=M]
    n_members: u16,
    // Total number of shards assigned [0..=S], fits in u16 by design
    shards_len: u16,
    // Sorted by NodeId ascending (deterministic)
    node_ids: [u64; M],
    // For each member i, offset into shards_flat where its shard list starts
    shard_offsets: [u16; M],
    // For each member i, number of shards assigned
    shard_counts: [u16; M],
    // Flat list of shard indices packed for all members
    shards_flat: [u16; S],
}

unsafe impl<const M: usize, const S: usize> Zeroable for CommitteePod<M, S> {}
unsafe impl<const M: usize, const S: usize> Pod for CommitteePod<M, S> {}

impl<const M: usize, const S: usize> Default for CommitteePod<M, S> {
    fn default() -> Self {
        Self::zeroed()
    }
}

impl<const M: usize, const S: usize> CommitteePod<M, S> {
    pub fn new(shards_by_node: BTreeMap<NodeId, Vec<u16>>) -> Self {
        let n_members = shards_by_node.len();
        assert!(n_members <= M, "committee too large for capacity");

        let total_shards: usize = shards_by_node.values().map(|v| v.len()).sum();
        assert!(total_shards <= S, "too many shards for committee capacity");
        assert!(total_shards <= u16::MAX as usize, "total shards exceed u16");

        let mut c = Self::default();
        let mut offset: usize = 0;

        // BTreeMap iterates in ascending NodeId order; keep arrays sorted by NodeId.
        for (i, (id, shards)) in shards_by_node.iter().enumerate() {
            c.node_ids[i] = id.as_u64();
            c.shard_offsets[i] = offset as u16;
            c.shard_counts[i] = shards.len() as u16;
            for (j, sh) in shards.iter().enumerate() {
                c.shards_flat[offset + j] = *sh;
            }
            offset += shards.len();
        }

        c.n_members = n_members as u16;
        c.shards_len = offset as u16;
        c
    }

    pub fn size(&self) -> usize {
        self.n_members as usize
    }

    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.find_index(node_id).is_some()
    }

    fn find_index(&self, node_id: &NodeId) -> Option<usize> {
        let raw = node_id.as_u64();
        let slice = &self.node_ids[..self.size()];
        slice.binary_search(&raw).ok()
    }

    pub fn shards(&self, node_id: &NodeId) -> Option<&[u16]> {
        self.find_index(node_id).map(|i| {
            let off = self.shard_offsets[i] as usize;
            let cnt = self.shard_counts[i] as usize;
            &self.shards_flat[off..off + cnt]
        })
    }

    pub fn total_shards(&self) -> u16 {
        self.shards_len
    }

    // Iterator over (NodeId, &[u16]) pairs
    pub fn iter(&self) -> CommitteeIter<'_, M, S> {
        CommitteeIter { committee: self, idx: 0 }
    }

    // Build a BTreeMap<NodeId, Vec<u16>> view of this committee (for move_shards integration)
    pub fn as_map(&self) -> BTreeMap<NodeId, Vec<u16>> {
        let mut map = BTreeMap::new();
        for i in 0..self.size() {
            let id: NodeId = self.node_ids[i].into();
            let off = self.shard_offsets[i] as usize;
            let cnt = self.shard_counts[i] as usize;
            map.insert(id, self.shards_flat[off..off + cnt].to_vec());
        }
        map
    }
}

pub struct CommitteeIter<'a, const M: usize, const S: usize> {
    committee: &'a CommitteePod<M, S>,
    idx: usize,
}

impl<'a, const M: usize, const S: usize> Iterator for CommitteeIter<'a, M, S> {
    type Item = (NodeId, &'a [u16]);
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.committee.size() {
            return None;
        }
        let i = self.idx;
        self.idx += 1;
        let id: NodeId = self.committee.node_ids[i].into();
        let off = self.committee.shard_offsets[i] as usize;
        let cnt = self.committee.shard_counts[i] as usize;
        Some((id, &self.committee.shards_flat[off..off + cnt]))
    }
}

// Concrete type alias used by the module
pub type Committee = CommitteePod<MAX_COMMITTEE_MEMBERS, MAX_COMMITTEE_SHARDS>;

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

// ========== Active Set (fixed-size, POD) ==========

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ActiveSetPod<const N: usize> {
    len: u16,
    node_ids: [u64; N],
    stakes: [u64; N],
}

unsafe impl<const N: usize> Zeroable for ActiveSetPod<N> {}
unsafe impl<const N: usize> Pod for ActiveSetPod<N> {}

impl<const N: usize> Default for ActiveSetPod<N> {
    fn default() -> Self {
        Self::zeroed()
    }
}

impl<const N: usize> ActiveSetPod<N> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn size(&self) -> usize {
        self.len as usize
    }

    pub fn active_ids(&self) -> Vec<NodeId> {
        (0..self.size())
            .map(|i| self.node_ids[i].into())
            .collect()
    }

    pub fn stake(&self, id: &NodeId) -> u64 {
        let raw = id.as_u64();
        for i in 0..self.size() {
            if self.node_ids[i] == raw {
                return self.stakes[i];
            }
        }
        0
    }

    fn find_index(&self, id: &NodeId) -> Option<usize> {
        let raw = id.as_u64();
        for i in 0..self.size() {
            if self.node_ids[i] == raw {
                return Some(i);
            }
        }
        None
    }

    fn push(&mut self, id: NodeId, stake: u64) {
        assert!(self.size() < N);
        let i = self.size();
        self.node_ids[i] = id.as_u64();
        self.stakes[i] = stake;
        self.len += 1;
    }

    fn remove_index(&mut self, idx: usize) {
        let last = self.size() - 1;
        if idx != last {
            self.node_ids[idx] = self.node_ids[last];
            self.stakes[idx] = self.stakes[last];
        }
        self.node_ids[last] = 0;
        self.stakes[last] = 0;
        self.len -= 1;
    }

    fn min_index(&self) -> Option<usize> {
        if self.size() == 0 {
            return None;
        }
        let mut min_i = 0;
        let mut min_v = self.stakes[0];
        for i in 1..self.size() {
            if self.stakes[i] < min_v {
                min_v = self.stakes[i];
                min_i = i;
            }
        }
        Some(min_i)
    }

    // Insert/update node by stake, potentially evicting the smallest if at full capacity (N).
    pub fn insert_or_update(&mut self, id: NodeId, stake: u64) {
        if stake == 0 {
            if let Some(i) = self.find_index(&id) {
                self.remove_index(i);
            }
            return;
        }

        if let Some(i) = self.find_index(&id) {
            self.stakes[i] = stake;
            return;
        }

        if self.size() < N {
            self.push(id, stake);
            return;
        }

        if let Some(min_i) = self.min_index() {
            if stake > self.stakes[min_i] {
                // Replace minimal-stake entry
                self.node_ids[min_i] = id.as_u64();
                self.stakes[min_i] = stake;
            }
        }
    }

    pub fn as_stake_map(&self) -> BTreeMap<NodeId, u64> {
        let mut m = BTreeMap::new();
        for i in 0..self.size() {
            m.insert(self.node_ids[i].into(), self.stakes[i]);
        }
        m
    }
}

// Concrete type alias used by the module
pub type ActiveSet = ActiveSetPod<MAX_ACTIVE_SET>;

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
            active_set: ActiveSet::new(),
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
        let current = self.committee.as_map();
        let new_shards_by_node = move_shards(&current, counts);
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
        for (id, shards) in committee.iter() {
            let weight = shards.len() as u16;
            assert!(weight > 0, "Zero node weight");
            // pool is ensured on set_stake/try_join but be robust
            self.ensure_pool(id);
            let pool = self.pools.get(&id).expect("pool present");

            // Store public key for the node
            public_keys.insert(id, pool.next_pubkey().clone());

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
    fn active_set_eviction_and_rejoin_at_capacity() {
        let clock = Clock::new(0);
        let mut staking = Staking::new(0, 1_000, 6, &clock);

        let n1 = NodeId(10);
        let n2 = NodeId(20);
        let n3 = NodeId(30);

        // Seed n1 and n2
        staking.set_stake(n1, 100);
        staking.set_stake(n2, 200);
        staking.try_join_active_set(&StorageNodeCap::new(n1));
        staking.try_join_active_set(&StorageNodeCap::new(n2));

        // Fill up to capacity with higher-stake filler nodes so n1 is the minimal
        for i in 0..(MAX_ACTIVE_SET - 2) {
            let nid = NodeId(1000 + i as u64);
            staking.set_stake(nid, 1000 + i as u64);
            staking.try_join_active_set(&StorageNodeCap::new(nid));
        }
        assert_eq!(staking.active_set.size(), MAX_ACTIVE_SET);

        // Now try to join n3 (higher than n1, lower than fillers) -> should evict the lowest (n1)
        staking.set_stake(n3, 300);
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
    fn kick_out_attack_like_scenario_at_capacity() {
        let clock = Clock::new(0);
        let mut staking = Staking::new(0, 1_000, 6, &clock);

        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let n4 = NodeId(4);

        // Fill with high-stake fillers to just below capacity
        for i in 0..(MAX_ACTIVE_SET - 3) {
            let id = NodeId(1000 + i as u64);
            staking.set_stake(id, 10_000 + i as u64);
            staking.try_join_active_set(&StorageNodeCap::new(id));
        }

        // Add three tracked nodes with increasing stakes
        staking.set_stake(n1, 100);
        staking.set_stake(n2, 200);
        staking.set_stake(n3, 300);
        staking.try_join_active_set(&StorageNodeCap::new(n1));
        staking.try_join_active_set(&StorageNodeCap::new(n2));
        staking.try_join_active_set(&StorageNodeCap::new(n3));
        assert!(staking.active_set.active_ids().contains(&n1));
        assert!(staking.active_set.active_ids().contains(&n2));
        assert!(staking.active_set.active_ids().contains(&n3));
        assert_eq!(staking.active_set.size(), MAX_ACTIVE_SET);

        // Now insert n4 with 101, which should kick out n1 (the minimal)
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

use std::collections::BTreeMap;
use bytemuck::{Pod, Zeroable};
use crate::types::NodeId;
use super::dhondt::allocate_shards;

// Fixed limits used by the simplified, fixed-size helpers.
const MAX_ACTIVE_SET: usize = 128;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ShardCounts<const N: usize> {
    len: u16,
    ids: [NodeId; N],  // Sorted ascending by NodeId
    counts: [u16; N],  // Counts for ids[i]
}

unsafe impl<const N: usize> Zeroable for ShardCounts<N> {}
unsafe impl<const N: usize> Pod for ShardCounts<N> {}

impl<const N: usize> Default for ShardCounts<N> {
    fn default() -> Self {
        Self::zeroed()
    }
}

impl<const N: usize> ShardCounts<N> {
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn ids(&self) -> &[NodeId] {
        &self.ids[..self.len()]
    }
    pub fn counts(&self) -> &[u16] {
        &self.counts[..self.len()]
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn sum(&self) -> u16 {
        self.counts().iter().copied().map(|x| x as u32).sum::<u32>() as u16
    }
    pub fn find(&self, id: &NodeId) -> Option<usize> {
        // ids are kept sorted ascending
        self.ids().binary_search(id).ok()
    }
}

/// Fixed-type version: compute shard counts from parallel slices of ids and stakes.
/// Result has only entries with non-zero assigned shards and is sorted by NodeId.
pub fn assign_shards_fixed<const N: usize>(
    ids: &[NodeId],
    stakes: &[u64],
    shard_count: u16,
) -> ShardCounts<N> {
    assert_eq!(ids.len(), stakes.len(), "ids/stakes length mismatch");
    if ids.is_empty() || shard_count == 0 {
        return ShardCounts::default();
    }
    assert!(ids.len() <= N, "too many nodes for capacity");

    // Sort by NodeId ascending to keep deterministic order and tie-breaking.
    let mut pairs: Vec<(NodeId, u64)> = ids.iter().copied().zip(stakes.iter().copied()).collect();
    pairs.sort_by_key(|(id, _)| *id);

    let node_count = pairs.len();
    let stakes_sorted: Vec<u64> = pairs.iter().map(|(_, s)| *s).collect();
    let node_priorities: Vec<u64> = (0..node_count).map(|i| (node_count - i) as u64).collect();

    let shards_vec = allocate_shards(&node_priorities, &stakes_sorted, shard_count);

    let mut out = ShardCounts::<N>::default();
    let mut out_len = 0usize;
    for (i, &assigned) in shards_vec.iter().enumerate() {
        if assigned > 0 {
            out.ids[out_len] = pairs[i].0;
            out.counts[out_len] = assigned;
            out_len += 1;
        }
    }
    out.len = out_len as u16;
    out
}

/// Produce sequential shard indices [0..sum) using the provided counts.
/// Returns a vector of (NodeId, shard_ids) pairs in ascending NodeId order.
pub fn map_shard_indices_fixed<const N: usize>(
    counts: &ShardCounts<N>,
) -> Vec<(NodeId, Vec<u16>)> {
    let mut shard_idx: u16 = 0;
    let mut out = Vec::with_capacity(counts.len());
    for i in 0..counts.len() {
        let id = counts.ids()[i];
        let cnt = counts.counts()[i] as usize;
        let mut v = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            v.push(shard_idx);
            shard_idx = shard_idx.saturating_add(1);
        }
        out.push((id, v));
    }
    out
}

/// Move shards according to target counts while minimizing movement.
/// Operates on flat vectors and returns a vector of (NodeId, shard_ids) pairs in ascending NodeId order.
pub fn move_shards_fixed<const N: usize>(
    prev_assignments: &[(NodeId, Vec<u16>)],     // assumed sorted ascending by NodeId
    target_counts: &ShardCounts<N>,              // sorted ascending by NodeId
) -> Vec<(NodeId, Vec<u16>)> {
    // Totals must match (conservation of shards)
    let total_prev: u64 = prev_assignments.iter().map(|(_, v)| v.len() as u64).sum();
    let total_target: u64 = target_counts.counts().iter().map(|&x| x as u64).sum();
    assert_eq!(total_prev, total_target, "Total shards mismatch between previous and target");

    let mut to_move: Vec<u16> = Vec::new();
    let mut result: Vec<(NodeId, Vec<u16>)> = Vec::with_capacity(target_counts.len());
    let mut needers: Vec<(usize, u16)> = Vec::new();

    // For each target entry (sorted ascending), preserve as much as possible from previous.
    for ti in 0..target_counts.len() {
        let id = target_counts.ids()[ti];
        let assigned = target_counts.counts()[ti];

        // Find previous shards for this id (linear scan is fine for N<=128)
        let mut prev_shards: &[u16] = &[];
        let mut prev_cnt: u16 = 0;
        if let Some((_, v)) = prev_assignments.iter().find(|(pid, _)| *pid == id) {
            prev_shards = v.as_slice();
            prev_cnt = v.len() as u16;
        }

        let keep = prev_cnt.min(assigned) as usize;
        let mut dst = Vec::with_capacity(assigned as usize);
        if keep > 0 {
            dst.extend_from_slice(&prev_shards[..keep]);
        }
        if prev_cnt > assigned {
            to_move.extend_from_slice(&prev_shards[keep..prev_cnt as usize]);
        } else if assigned > prev_cnt {
            needers.push((result.len(), assigned - prev_cnt));
        }
        result.push((id, dst));
    }

    // Any prev nodes not present in target -> free all their shards.
    for (pid, v) in prev_assignments.iter() {
        if target_counts.find(pid).is_none() {
            to_move.extend_from_slice(v.as_slice());
        }
    }

    // Fill remaining needs from 'to_move' LIFO (matches previous behavior).
    for (idx, need) in needers.into_iter() {
        let dst = &mut result[idx].1;
        for _ in 0..need {
            let shard = to_move.pop().expect("Not enough freed shards to reassign");
            dst.push(shard);
        }
    }

    // All freed shards should be reassigned
    assert!(to_move.is_empty(), "Some freed shards left unassigned");
    result
}

// --------------------- Backward-compatible wrappers ---------------------
// These preserve the original BTreeMap-based API but use the fixed/slice-based
// implementations internally to avoid BTreeMap-heavy logic in the hot path.

/// Assign shards to nodes based on their stake.
/// Returns sorted mapping of NodeId -> shards allocated (>0 only).
pub fn assign_shards(
    stake_by_node: &BTreeMap<NodeId, u64>,
    shard_count: u16,
) -> BTreeMap<NodeId, u16> {
    if stake_by_node.is_empty() || shard_count == 0 {
        return BTreeMap::new();
    }

    // BTreeMap iterates in ascending NodeId order already
    let node_ids: Vec<NodeId> = stake_by_node.keys().copied().collect();
    let stakes: Vec<u64> = node_ids.iter().map(|k| stake_by_node[k]).collect();

    let fixed = assign_shards_fixed::<MAX_ACTIVE_SET>(&node_ids, &stakes, shard_count);

    let mut distribution = BTreeMap::new();
    for i in 0..fixed.len() {
        distribution.insert(fixed.ids()[i], fixed.counts()[i]);
    }
    distribution
}

/// Move shards according to new target counts while minimizing movement.
/// Keep existing shards where possible, free those for removed or reduced assignments,
/// and assign freed shards to nodes that need more or newly added nodes.
pub fn move_shards(
    shards_by_node: &BTreeMap<NodeId, Vec<u16>>,
    target_counts: BTreeMap<NodeId, u16>,
) -> BTreeMap<NodeId, Vec<u16>> {
    // Convert to sorted vectors
    let prev_vec: Vec<(NodeId, Vec<u16>)> = shards_by_node.iter().map(|(k, v)| (*k, v.clone())).collect();

    let mut counts = ShardCounts::<MAX_ACTIVE_SET>::default();
    let mut len = 0usize;
    for (id, count) in target_counts.iter() {
        assert!(len < MAX_ACTIVE_SET, "too many target nodes");
        counts.ids[len] = *id;
        counts.counts[len] = *count;
        len += 1;
    }
    counts.len = len as u16;

    let moved = move_shards_fixed(&prev_vec, &counts);

    let mut out = BTreeMap::new();
    for (id, v) in moved.into_iter() {
        out.insert(id, v);
    }
    out
}

pub fn map_shard_indices(assigned_number: BTreeMap<NodeId, u16>) -> BTreeMap<NodeId, Vec<u16>> {
    let mut counts = ShardCounts::<MAX_ACTIVE_SET>::default();
    let mut len = 0usize;
    for (id, cnt) in assigned_number.iter() {
        assert!(len < MAX_ACTIVE_SET, "too many nodes for capacity");
        counts.ids[len] = *id;
        counts.counts[len] = *cnt;
        len += 1;
    }
    counts.len = len as u16;

    let pairs = map_shard_indices_fixed(&counts);
    let mut map = BTreeMap::new();
    for (id, v) in pairs.into_iter() {
        map.insert(id, v);
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    fn total_shard_count(shards_by_node: &BTreeMap<NodeId, Vec<u16>>) -> usize {
        shards_by_node.values().map(|v| v.len()).sum()
    }

    #[test]
    fn test_single() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(10), 1_000_000)
        ].into();

        let shard_counts = assign_shards(&stake_map, 10);
        let shards_map = map_shard_indices(shard_counts);

        assert_eq!(shards_map.len(), 1);
        assert_eq!(total_shard_count(&shards_map), 10);
        assert_eq!(shards_map.get(&NodeId(10)).unwrap().len(), 10);
    }

    #[test]
    fn test_equal() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 1000),
            (NodeId(3), 1000),
        ].into();

        let res = assign_shards(&stake_map, 10);
        assert_eq!(res.values().copied().sum::<u16>(), 10);

        let v: Vec<u16> = [NodeId(1), NodeId(2), NodeId(3)]
            .iter()
            .map(|nid| *res.get(nid).unwrap_or(&0))
            .collect();

        assert_eq!(v, vec![4, 3, 3]);
    }

    #[test]
    fn test_even() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 1000),
            (NodeId(3), 1000),
        ].into();

        let shard_counts = assign_shards(&stake_map, 6);
        let shards_map = map_shard_indices(shard_counts);

        assert_eq!(shards_map.len(), 3);
        assert_eq!(shards_map.get(&NodeId(1)).unwrap().len(), 2);
        assert_eq!(shards_map.get(&NodeId(2)).unwrap().len(), 2);
        assert_eq!(shards_map.get(&NodeId(3)).unwrap().len(), 2);
    }

    #[test]
    fn test_uneven() {
        let stake_map: BTreeMap<NodeId, u64> = [
            (NodeId(1), 4000),
            (NodeId(2), 2000),
            (NodeId(3), 1000),
        ].into();

        let shard_counts = assign_shards(&stake_map, 10);
        let shards_map = map_shard_indices(shard_counts);

        assert_eq!(shards_map.len(), 3);
        assert_eq!(shards_map.get(&NodeId(1)).unwrap().len(), 6);
        assert_eq!(shards_map.get(&NodeId(2)).unwrap().len(), 3);
        assert_eq!(shards_map.get(&NodeId(3)).unwrap().len(), 1);
    }

    #[test]
    fn test_reassign() {
        let init_map: BTreeMap<NodeId, u16> = [
            (NodeId(3), 2),
            (NodeId(2), 2),
            (NodeId(1), 2),
            (NodeId(0), 2),
        ].into();

        let shards_map1 = map_shard_indices(init_map);

        assert_eq!(shards_map1.len(), 4);
        assert_eq!(shards_map1.get(&NodeId(0)).unwrap(), &vec![0, 1]);
        assert_eq!(shards_map1.get(&NodeId(1)).unwrap(), &vec![2, 3]);
        assert_eq!(shards_map1.get(&NodeId(2)).unwrap(), &vec![4, 5]);
        assert_eq!(shards_map1.get(&NodeId(3)).unwrap(), &vec![6, 7]);

        let target: BTreeMap<NodeId, u16> = [
            (NodeId(3), 4),
            (NodeId(2), 4)
        ].into();

        let shards_map2 = move_shards(&shards_map1, target);
        assert_eq!(shards_map2.len(), 2);

        let s3 = shards_map2.get(&NodeId(3)).unwrap();
        assert!(s3.contains(&6) && s3.contains(&7));
        assert!(s3.contains(&0) && s3.contains(&1));

        let s2 = shards_map2.get(&NodeId(2)).unwrap();
        assert!(s2.contains(&4) && s2.contains(&5));
        assert!(s2.contains(&2) && s2.contains(&3));
    }

    #[test]
    fn test_reassign_reduce() {
        let initial_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(1), 1000),
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let shard_counts = assign_shards(&initial_stakes, 6);
        let initial_shard_map = map_shard_indices(shard_counts);
        assert_eq!(total_shard_count(&initial_shard_map), 6);

        let updated_stakes: BTreeMap<NodeId, u64> = [
            (NodeId(2), 2000),
            (NodeId(3), 3000),
        ].into();

        let shard_counts = assign_shards(&updated_stakes, 6);
        let updated_shard_map = move_shards(&initial_shard_map, shard_counts);

        assert_eq!(updated_shard_map.len(), 2);
        assert_eq!(total_shard_count(&updated_shard_map), 6);
    }

    #[test]
    fn test_reassign_chain() {
        let n1 = NodeId(1);
        let n2 = NodeId(2);
        let n3 = NodeId(3);
        let n4 = NodeId(4);
        let n5 = NodeId(5);

        let shards_map1 = map_shard_indices(
            [(n1, 2), (n2, 2), (n3, 2), (n4, 2), (n5, 2)].into()
        );
        assert_eq!(shards_map1.len(), 5);
        assert_eq!(shards_map1.get(&n1).unwrap(), &vec![0, 1]);
        assert_eq!(shards_map1.get(&n2).unwrap(), &vec![2, 3]);
        assert_eq!(shards_map1.get(&n3).unwrap(), &vec![4, 5]);
        assert_eq!(shards_map1.get(&n4).unwrap(), &vec![6, 7]);
        assert_eq!(shards_map1.get(&n5).unwrap(), &vec![8, 9]);

        let shards_map2 = move_shards(&shards_map1, [(n1, 4), (n2, 3), (n3, 3)].into());
        assert_eq!(shards_map2.len(), 3);
        let s1 = shards_map2.get(&n1).unwrap().clone();
        let s2 = shards_map2.get(&n2).unwrap().clone();
        let s3 = shards_map2.get(&n3).unwrap().clone();
        assert!(s1.contains(&0) && s1.contains(&1));
        assert!(s2.contains(&2) && s2.contains(&3));
        assert!(s3.contains(&4) && s3.contains(&5));

        let shards_map3 = move_shards(&shards_map2, [(n2, 3), (n3, 3), (n4, 2), (n5, 2)].into());
        assert_eq!(shards_map3.len(), 4);
        assert_eq!(shards_map3.get(&n2).unwrap(), &s2);
        assert_eq!(shards_map3.get(&n3).unwrap(), &s3);
        assert_eq!(shards_map3.get(&n4).unwrap().len(), 2);
        assert_eq!(shards_map3.get(&n5).unwrap().len(), 2);

        let shards_map4 = move_shards(&shards_map3, [(n1, 10)].into());
        assert_eq!(shards_map4.len(), 1);
        let s = shards_map4.get(&n1).unwrap();
        for i in 0..10 {
            assert!(s.contains(&(i as u16)));
        }
    }

    #[test]
    fn test_many() {
        fn print_table_header() {
            println!(
                "{:<8} | {:>12} | {:>6} | {}",
                "NodeId", "Stake", "Shards", "ShardIds"
            );
            println!("{}", "-".repeat(8 + 3 + 12 + 3 + 6 + 3 + 40));
        }

        // Generate 100 nodes with stakes from 1000 to 100,000
        let initial_stakes: BTreeMap<NodeId, u64> = (1..=100)
            .map(|i| (NodeId(100 - i), i as u64 * 1000))
            .collect();

        let shard_counts = assign_shards(&initial_stakes, 1000);
        let initial_shard_map = map_shard_indices(shard_counts);
        assert_eq!(total_shard_count(&initial_shard_map), 1000);

        print_table_header();
        for (node_id, shard_ids) in &initial_shard_map {
            let stake = initial_stakes.get(node_id).unwrap_or(&0);
            println!(
                "{:<8} | {:>12} | {:>6} | {:?}",
                format!("{:?}", node_id),
                stake,
                shard_ids.len(),
                shard_ids
            );
        }

        // Updated stakes: keep only nodes 51 to 100
        let updated_stakes: BTreeMap<NodeId, u64> = (51..=100)
            .map(|i| (NodeId(100 - i), i as u64 * 1000))
            .collect();

        let shard_counts = assign_shards(&updated_stakes, 1000);
        let updated_shard_map = move_shards(&initial_shard_map, shard_counts);

        assert_eq!(updated_shard_map.len(), 50);
        assert_eq!(total_shard_count(&updated_shard_map), 1000);

        // Print updated shard map
        println!("\nAfter reassignment:");
        print_table_header();
        for (node_id, shard_ids) in &updated_shard_map {
            let stake = updated_stakes.get(node_id).unwrap_or(&0);
            println!(
                "{:<8} | {:>12} | {:>6} | {:?}",
                format!("{:?}", node_id),
                stake,
                shard_ids.len(),
                shard_ids
            );
        }
    }
}
