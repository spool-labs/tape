use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use tape_api::program::EPOCH_DURATION;
use tape_api::program::tapedrive;
use tape_api::program::MIN_COMMITTEE_SIZE;
use tape_core::erasure::GROUP_SIZE;
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::Address;
use tape_protocol::ProtocolState;

use crate::fixture::ChainFixture;
use crate::node::HarnessNode;
use crate::seed::{SeededWorld, build_seeded_world};
use crate::spec::{
    HarnessNodeSpec, HarnessSpec, default_last_epoch, elapsed_last_epoch, onchain_elapsed_last_epoch,
};

const DEFAULT_NODES: usize = 20;
const DEFAULT_EPOCH: EpochNumber = EpochNumber(3);
const DEFAULT_AIRDROP_LAMPORTS: u64 = 10_000_000_000;

pub trait IntoEpochNumber {
    fn into_epoch_number(self) -> EpochNumber;
}

impl IntoEpochNumber for u64 {
    fn into_epoch_number(self) -> EpochNumber {
        EpochNumber(self)
    }
}

impl IntoEpochNumber for EpochNumber {
    fn into_epoch_number(self) -> EpochNumber {
        self
    }
}

pub struct ChainHarness {
    fixture: ChainFixture,
    spec: HarnessSpec,
    protocol_state: ProtocolState,
    nodes: Vec<HarnessNode>,
}

impl ChainHarness {
    pub fn builder() -> ChainHarnessBuilder {
        ChainHarnessBuilder::default()
    }

    pub fn rpc(&self) -> &rpc_litesvm::LiteSvmRpc {
        self.fixture.rpc()
    }

    pub fn epoch(&self) -> EpochNumber {
        self.spec.epoch
    }

    pub fn phase(&self) -> EpochPhase {
        self.spec.phase
    }

    pub fn protocol_state(&self) -> &ProtocolState {
        &self.protocol_state
    }

    pub fn node(&self, index: usize) -> &HarnessNode {
        &self.nodes[index]
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn owned_spools(&self, index: usize) -> Vec<SpoolIndex> {
        self.protocol_state
            .member_spools(Address::from(self.nodes[index].node_address))
    }
}

#[derive(Clone, Debug)]
pub struct ChainHarnessBuilder {
    node_count: usize,
    epoch: EpochNumber,
    phase: EpochPhase,
    last_epoch: Option<i64>,
    current_committee_size: Option<usize>,
    prev_committee_size: Option<usize>,
    next_committee_size: Option<usize>,
    current_committee_nodes: Option<Vec<usize>>,
    prev_committee_nodes: Option<Vec<usize>>,
    next_committee_nodes: Option<Vec<usize>>,
    current_group_count: Option<u64>,
    prev_group_count: Option<u64>,
    node_specs: Vec<HarnessNodeSpec>,
    next_assignment_ready: bool,
    candidate_ready: bool,
    seed_prev_snapshot_tape: bool,
}

impl Default for ChainHarnessBuilder {
    fn default() -> Self {
        Self {
            node_count: DEFAULT_NODES,
            epoch: DEFAULT_EPOCH,
            phase: EpochPhase::Active,
            last_epoch: None,
            current_committee_size: None,
            prev_committee_size: None,
            next_committee_size: None,
            current_committee_nodes: None,
            prev_committee_nodes: None,
            next_committee_nodes: None,
            current_group_count: None,
            prev_group_count: None,
            node_specs: default_node_specs(DEFAULT_NODES, DEFAULT_EPOCH),
            next_assignment_ready: false,
            candidate_ready: false,
            seed_prev_snapshot_tape: true,
        }
    }
}

impl ChainHarnessBuilder {
    pub fn nodes(mut self, count: usize) -> Self {
        self.node_count = count;
        self.node_specs = resize_node_specs(self.node_specs, count, self.epoch);
        self
    }

    pub fn epoch(mut self, epoch: impl IntoEpochNumber) -> Self {
        let old_epoch = self.epoch;
        let new_epoch = epoch.into_epoch_number();
        let old_default = HarnessNodeSpec::default_for(old_epoch);
        let new_default = HarnessNodeSpec::default_for(new_epoch);

        for spec in &mut self.node_specs {
            if spec.latest_sync_epoch == old_default.latest_sync_epoch {
                spec.latest_sync_epoch = new_default.latest_sync_epoch;
            }
            if spec.latest_advance_epoch == old_default.latest_advance_epoch {
                spec.latest_advance_epoch = new_default.latest_advance_epoch;
            }
        }

        self.epoch = new_epoch;
        self
    }

    pub fn phase(mut self, phase: EpochPhase) -> Self {
        self.phase = phase;
        self
    }

    pub fn last_epoch(mut self, timestamp: i64) -> Self {
        self.last_epoch = Some(timestamp);
        self
    }

    pub fn time_elapsed(mut self) -> Self {
        self.last_epoch = Some(elapsed_last_epoch(unix_now()));
        self
    }

    pub fn onchain_time_elapsed(mut self) -> Self {
        self.last_epoch = Some(onchain_elapsed_last_epoch(EPOCH_DURATION));
        self
    }

    pub fn current_committee_size(mut self, size: usize) -> Self {
        self.current_committee_size = Some(size);
        self.current_committee_nodes = None;
        self
    }

    pub fn prev_committee_size(mut self, size: usize) -> Self {
        self.prev_committee_size = Some(size);
        self.prev_committee_nodes = None;
        self
    }

    pub fn next_committee_size(mut self, size: usize) -> Self {
        self.next_committee_size = Some(size);
        self.next_committee_nodes = None;
        self
    }

    pub fn current_committee_nodes<I>(mut self, nodes: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        self.current_committee_nodes = Some(nodes.into_iter().collect());
        self.current_committee_size = None;
        self
    }

    pub fn prev_committee_nodes<I>(mut self, nodes: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        self.prev_committee_nodes = Some(nodes.into_iter().collect());
        self.prev_committee_size = None;
        self
    }

    pub fn next_committee_nodes<I>(mut self, nodes: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        self.next_committee_nodes = Some(nodes.into_iter().collect());
        self.next_committee_size = None;
        self
    }

    pub fn node<F>(mut self, index: usize, f: F) -> Self
    where
        F: FnOnce(&mut HarnessNodeSpec),
    {
        if index >= self.node_count {
            self.node_count = index + 1;
            self.node_specs = resize_node_specs(self.node_specs, self.node_count, self.epoch);
        }

        f(&mut self.node_specs[index]);
        self
    }

    pub fn current_group_count(mut self, count: u64) -> Self {
        self.current_group_count = Some(count);
        self
    }

    pub fn prev_group_count(mut self, count: u64) -> Self {
        self.prev_group_count = Some(count);
        self
    }

    pub fn next_assignment_ready(mut self) -> Self {
        self.next_assignment_ready = true;
        self
    }

    pub fn candidate_ready(mut self) -> Self {
        self.candidate_ready = true;
        self
    }

    pub fn advance_ready(mut self) -> Self {
        self.next_assignment_ready = true;
        self.candidate_ready = true;
        self
    }

    /// Disable seeding of the previous-epoch snapshot tape.
    pub fn no_prev_snapshot_tape(mut self) -> Self {
        self.seed_prev_snapshot_tape = false;
        self
    }

    pub async fn build(self) -> Result<ChainHarness> {
        let spec = self.finalize_spec()?;
        let fixture = ChainFixture::new();

        let workspace_root = ChainFixture::workspace_root_from_manifest(Path::new(env!("CARGO_MANIFEST_DIR")))
            .context("derive workspace root for chain harness")?;
        fixture
            .load_default_programs(&workspace_root)
            .context("load default programs for chain harness")?;

        let seeded = build_seeded_world(&spec)?;
        seed_fixture(&fixture, &seeded)?;

        Ok(ChainHarness {
            fixture,
            spec,
            protocol_state: seeded.protocol_state,
            nodes: seeded.nodes,
        })
    }

    fn finalize_spec(mut self) -> Result<HarnessSpec> {
        ensure!(self.node_count > 0, "chain harness requires at least one node");
        self.node_specs = resize_node_specs(self.node_specs, self.node_count, self.epoch);

        let current_committee_nodes = resolve_committee_selection(
            self.node_count,
            self.current_committee_nodes,
            self.current_committee_size
                .unwrap_or_else(|| self.node_count.min(MIN_COMMITTEE_SIZE)),
            "current committee",
        )?;
        let prev_committee_nodes = resolve_committee_selection(
            self.node_count,
            self.prev_committee_nodes,
            self.prev_committee_size.unwrap_or(0),
            "previous committee",
        )?;
        let next_committee_nodes = resolve_committee_selection(
            self.node_count,
            self.next_committee_nodes,
            self.next_committee_size.unwrap_or(0),
            "next committee",
        )?;

        let current_group_count = resolve_group_count(
            self.current_group_count,
            current_committee_nodes.len(),
            "current group count",
        )?;
        let prev_group_count = resolve_group_count(
            self.prev_group_count,
            prev_committee_nodes.len(),
            "previous group count",
        )?;
        if self.next_assignment_ready && current_group_count > 0 {
            ensure!(
                next_committee_nodes.len() >= GROUP_SIZE,
                "next assignment requires at least {GROUP_SIZE} committee members"
            );
        }

        Ok(HarnessSpec {
            epoch: self.epoch,
            phase: self.phase,
            last_epoch: self.last_epoch.unwrap_or_else(default_last_epoch),
            nodes: self.node_specs,
            current_committee_nodes,
            prev_committee_nodes,
            next_committee_nodes,
            current_group_count,
            prev_group_count,
            next_assignment_ready: self.next_assignment_ready,
            candidate_ready: self.candidate_ready,
            seed_prev_snapshot_tape: self.seed_prev_snapshot_tape,
        })
    }
}

fn seed_fixture(fixture: &ChainFixture, seeded: &SeededWorld) -> Result<()> {
    for node in &seeded.nodes {
        fixture
            .airdrop(&node.authority, DEFAULT_AIRDROP_LAMPORTS)
            .with_context(|| format!("airdrop {}", node.authority))?;
    }

    fixture.seed_account(&seeded.system.address, &tapedrive::ID, &seeded.system.data)?;
    for account in &seeded.epochs {
        fixture.seed_account(&account.address, &tapedrive::ID, &account.data)?;
    }
    for account in &seeded.committees {
        fixture.seed_account(&account.address, &tapedrive::ID, &account.data)?;
    }
    fixture.seed_account(&seeded.peer_set.address, &tapedrive::ID, &seeded.peer_set.data)?;
    for account in &seeded.groups {
        fixture.seed_account(&account.address, &tapedrive::ID, &account.data)?;
    }
    fixture.seed_account(&seeded.archive.address, &tapedrive::ID, &seeded.archive.data)?;
    fixture.seed_account(&seeded.mint.address, &spl_token::ID, &seeded.mint.data)?;
    fixture.seed_account(
        &seeded.archive_ata.address,
        &spl_token::ID,
        &seeded.archive_ata.data,
    )?;
    fixture.seed_account(
        &seeded.subsidy_ata.address,
        &spl_token::ID,
        &seeded.subsidy_ata.data,
    )?;

    if let Some(snapshot_tape) = &seeded.prev_snapshot_tape {
        fixture.seed_account(&snapshot_tape.address, &tapedrive::ID, &snapshot_tape.data)?;
    }

    for account in &seeded.node_accounts {
        fixture.seed_account(&account.address, &tapedrive::ID, &account.data)?;
    }

    for account in &seeded.history_accounts {
        fixture.seed_account(&account.address, &tapedrive::ID, &account.data)?;
    }

    for account in &seeded.blacklist_accounts {
        fixture.seed_account(&account.address, &tapedrive::ID, &account.data)?;
    }

    Ok(())
}

fn resolve_committee_selection(
    node_count: usize,
    nodes: Option<Vec<usize>>,
    size: usize,
    label: &str,
) -> Result<Vec<usize>> {
    let selected = match nodes {
        Some(nodes) => nodes,
        None => (0..size).collect(),
    };

    if selected.len() > node_count {
        bail!("{label} size {} exceeds node count {node_count}", selected.len());
    }

    let mut seen = std::collections::BTreeSet::new();
    for &index in &selected {
        ensure!(index < node_count, "{label} index {index} out of range 0..{node_count}");
        ensure!(seen.insert(index), "{label} contains duplicate node index {index}");
    }

    Ok(selected)
}

fn resolve_group_count(
    count: Option<u64>,
    member_count: usize,
    label: &str,
) -> Result<u64> {
    if member_count == 0 {
        let count = count.unwrap_or(0);
        ensure!(count == 0, "{label} must be 0 when the committee is empty");
        return Ok(0);
    }

    let count = count.unwrap_or(1);
    if count > 0 {
        ensure!(
            member_count >= GROUP_SIZE,
            "{label} requires at least {GROUP_SIZE} committee members"
        );
    }
    Ok(count)
}

fn default_node_specs(count: usize, epoch: EpochNumber) -> Vec<HarnessNodeSpec> {
    (0..count)
        .map(|_| HarnessNodeSpec::default_for(epoch))
        .collect()
}

fn resize_node_specs(
    current: Vec<HarnessNodeSpec>,
    count: usize,
    epoch: EpochNumber,
) -> Vec<HarnessNodeSpec> {
    let mut next = default_node_specs(count, epoch);
    for (index, spec) in current.into_iter().enumerate().take(count) {
        next[index] = spec;
    }
    next
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_builder_uses_current_committee_threshold() {
        let spec = ChainHarness::builder()
            .nodes(25)
            .finalize_spec()
            .expect("spec");

        assert_eq!(spec.current_committee_nodes.len(), MIN_COMMITTEE_SIZE);
        assert!(spec.prev_committee_nodes.is_empty());
        assert!(spec.next_committee_nodes.is_empty());
    }

    #[test]
    fn explicit_committee_nodes_drive_group_ownership() {
        let selected = [
            7, 3, 1, 0, 2, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ];
        let spec = ChainHarness::builder()
            .nodes(20)
            .current_committee_nodes(selected)
            .current_group_count(1)
            .finalize_spec()
            .expect("spec");

        let seeded = build_seeded_world(&spec).expect("seeded world");

        assert_eq!(seeded.nodes[7].member_index, Some(0));
        assert_eq!(seeded.nodes[3].member_index, Some(1));
        assert_eq!(seeded.nodes[1].member_index, Some(2));
        assert_eq!(
            seeded
                .protocol_state
                .member_spools(seeded.nodes[7].node_address.into())
                .len(),
            1
        );
    }
}
