use std::time::{Duration, Instant};

use rpc::Rpc;
use rpc_client::RpcClient;
use solana_sdk::program_pack::Pack;
use solana_sdk::signer::Signer;
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::build_claim_commission_ix;
use tape_api::program::EPOCH_DURATION;
use tape_api::program::tapedrive::track_pda;
use tape_api::utils::ata;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::system::Member;
use tape_core::types::coin::TAPE;
use tape_core::types::{BasisPoints, EpochNumber, StorageUnits};
use tape_crypto::{Address, hash};
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, SimnetScenario, run_simnet_test,
};
use tape_sdk::keys::stake_key::StakeKey;
use tape_sdk::keys::tape_key::TapeKey;
use tape_store::ops::{ObjectInfoOps, TrackDataOps, TrackOps};

const NODE_COUNT: usize = GROUP_SIZE;
const TARGET_GROUPS: u64 = 1;
const POOL: usize = 0;
const COMMISSION: BasisPoints = BasisPoints(1_000);
const DATA_EPOCH: EpochNumber = EpochNumber(2);
const CUTOFF_EPOCH: EpochNumber = EpochNumber(3);
const ASSIGNED_EPOCH: EpochNumber = EpochNumber(4);
const FIRST_CLAIM_EPOCH: EpochNumber = EpochNumber(5);
const WITHDRAW_EPOCH: EpochNumber = EpochNumber(7);

#[derive(Clone, Copy, Debug, Default)]
struct PoolClaim {
    gross: TAPE,
    commission: TAPE,
    rewards: TAPE,
}

impl PoolClaim {
    fn add(&mut self, claim: PoolClaim) {
        self.gross = self.gross.saturating_add(claim.gross);
        self.commission = self.commission.saturating_add(claim.commission);
        self.rewards = self.rewards.saturating_add(claim.rewards);
    }
}

#[test]
fn staking_rewards() {
    run_simnet_test(run);
}

async fn run() {
    let mut harness = SimnetBuilder::new()
        .node_count(NODE_COUNT)
        .runtime_mode(NodeRuntimeMode::Full)
        .base_port(0)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..NODE_COUNT).collect();
    let stake_key = StakeKey::generate();
    let node_stake = tape("1000");
    let user_stake = tape("500");
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(EPOCH_DURATION as u64 * 5);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(COMMISSION)
            .await
            .expect("register nodes");
        scenario.stake_all(1_000).await.expect("stake nodes");

        let sdk = scenario.sdk(harness.admin());
        let pool = Address::from(scenario.node_address(POOL));
        sdk.stake_with_pool(&stake_key, pool, user_stake)
            .await
            .expect("stake with pool");

        let node = read_node(&harness, POOL).await;
        let expected_pool_stake = node_stake.saturating_add(user_stake);
        assert_eq!(
            node.pool.stake, expected_pool_stake,
            "bootstrap stake should be active before rewards"
        );
        assert_eq!(
            node.pool.shares.as_u64(),
            expected_pool_stake.as_u64(),
            "flat bootstrap rate should mint one share per flux"
        );

        scenario
            .set_spool_groups_many(&all, TARGET_GROUPS)
            .await
            .expect("set spool group preferences");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let scenario = harness.scenario();
    scenario
        .wait_nodes_healthy(health_timeout)
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active");

    let epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to data epoch");
    assert_eq!(epoch, DATA_EPOCH.0, "unexpected data epoch");
    scenario
        .advance_pool_ok(POOL)
        .await
        .expect("close activation rate span");

    let track = write_data(&harness).await;
    wait_track(&harness, track, active_timeout).await;

    let epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to cutoff epoch");
    assert_eq!(epoch, CUTOFF_EPOCH.0, "unexpected cutoff epoch");
    scenario
        .advance_pool_ok(POOL)
        .await
        .expect("advance target pool at cutoff");

    let epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to assigned epoch");
    assert_eq!(epoch, ASSIGNED_EPOCH.0, "unexpected assigned epoch");
    assert_target_assigned(&harness, &scenario, track).await;
    scenario
        .advance_pool_ok(POOL)
        .await
        .expect("advance target pool at assigned epoch");
    let initial_pool = read_node(&harness, POOL).await.pool;
    let initial_pool_stake = initial_pool.stake;
    assert_eq!(
        initial_pool_stake,
        node_stake.saturating_add(user_stake),
        "pool stake should not earn rewards before the first claim epoch"
    );

    let epoch = scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to first claim epoch");
    assert_eq!(epoch, FIRST_CLAIM_EPOCH.0, "unexpected claim epoch");

    let mut total_claim = PoolClaim::default();
    let claim = wait_pool_claimed(
        &harness,
        &scenario,
        POOL,
        EpochNumber(epoch),
        COMMISSION,
        active_timeout,
    )
    .await;
    assert!(
        claim.gross > TAPE::zero(),
        "target pool should earn rewards from assigned storage"
    );
    total_claim.add(claim);
    assert_pool_rewards(&harness, total_claim).await;

    let sdk = scenario.sdk(harness.admin());
    sdk.request_stake_unlock(&stake_key)
        .await
        .expect("request unlock");

    let stake = sdk
        .rpc()
        .get_stake(&stake_key.pubkey().into())
        .await
        .expect("stake account after unlock");
    assert!(
        stake.inner.is_withdrawing(),
        "stake should be unlocking after request"
    );
    assert_eq!(
        stake.inner.withdraw_epoch(),
        Some(WITHDRAW_EPOCH),
        "unexpected withdraw epoch"
    );

    while scenario
        .current_epoch_number()
        .await
        .expect("read current epoch")
        < WITHDRAW_EPOCH.0
    {
        scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance epoch before withdraw");
        let current = scenario
            .read_system()
            .await
            .expect("read current epoch after advance")
            .current_epoch;
        let claim = wait_pool_claimed(
            &harness,
            &scenario,
            POOL,
            current,
            COMMISSION,
            active_timeout,
        )
        .await;
        total_claim.add(claim);
        assert_pool_rewards(&harness, total_claim).await;
    }

    assert_pool_rewards(&harness, total_claim).await;

    let expected_user_rewards = expected_staker_rewards(
        user_stake,
        initial_pool_stake,
        total_claim.rewards,
    );
    assert!(
        expected_user_rewards > TAPE::zero(),
        "user stake should earn a positive reward"
    );

    let authority = Address::from(harness.node(POOL).expect("target node").authority());
    let commission_before = token_balance(&harness, authority).await;
    claim_commission(&harness, POOL).await;
    let commission_after = token_balance(&harness, authority).await;
    assert_eq!(
        commission_after.saturating_sub(commission_before),
        total_claim.commission,
        "claim commission should transfer the full commission to the node authority"
    );
    assert_eq!(
        read_node(&harness, POOL).await.pool.commission,
        TAPE::zero(),
        "commission should be cleared after claim"
    );

    let user = Address::from(stake_key.pubkey());
    let user_before = token_balance(&harness, user).await;
    sdk.unstake_from_pool(&stake_key)
        .await
        .expect("unstake from pool");
    let user_after = token_balance(&harness, user).await;
    let expected_unstake = user_stake.saturating_add(expected_user_rewards);
    assert_eq!(
        user_after.saturating_sub(user_before),
        expected_unstake,
        "unstake should return principal plus the user's reward share"
    );
    assert!(
        matches!(
            sdk.rpc().get_stake(&stake_key.pubkey().into()).await,
            Err(rpc::RpcError::AccountNotFound(_))
        ),
        "stake account should be closed"
    );
    assert_eq!(
        read_node(&harness, POOL)
            .await
            .pool
            .rewards,
        total_claim.rewards.saturating_sub(expected_user_rewards),
        "unstake should debit only the user's earned rewards from the pool"
    );

    scenario
        .wait_nodes_active(&all, active_timeout)
        .await
        .expect("all nodes active");

    drop(scenario);
    harness.stop_all().await.expect("stop harness");
}

async fn write_data(harness: &SimnetHarness) -> Address {
    let scenario = harness.scenario();
    let sdk = scenario.sdk(harness.admin());
    let tape_key = TapeKey::generate();
    let data = vec![0x42; 512];

    sdk.reserve(&tape_key, StorageUnits::mb(1), 8)
        .await
        .expect("reserve data tape");

    let track = sdk
        .write_raw(&tape_key, hash::hash(b"staking-rewards"), &data)
        .await
        .expect("write raw track");
    assert_eq!(track.group, GroupIndex(0), "unexpected raw track group");

    track_pda(track.tape, track.track_number).0
}

async fn assert_target_assigned(
    harness: &SimnetHarness,
    scenario: &SimnetScenario<'_>,
    track: Address,
) {
    let track_info = harness
        .nodes()
        .iter()
        .find(|node| node.is_running())
        .and_then(|node| {
            node.context()
                .store
                .get_track(track)
                .expect("read track")
        })
        .expect("track metadata");
    let members = scenario
        .read_committee(ASSIGNED_EPOCH)
        .await
        .expect("read assigned committee");
    let node = Address::from(scenario.node_address(POOL));
    let member = members
        .iter()
        .find(|member| member.node == node)
        .expect("target node is in committee");
    assert!(
        member.assigned >= track_info.size,
        "target node should be assigned the active user track"
    );
}

async fn wait_pool_claimed(
    harness: &SimnetHarness,
    scenario: &SimnetScenario<'_>,
    node_index: usize,
    current_epoch: EpochNumber,
    commission_rate: BasisPoints,
    timeout: Duration,
) -> PoolClaim {
    let expected = expected_pool_claim_at(scenario, node_index, current_epoch).await;
    let _ = wait_pool_advanced(harness, node_index, current_epoch, timeout).await;

    PoolClaim {
        gross: expected,
        commission: expected_commission(expected, commission_rate),
        rewards: expected_rewards(expected, commission_rate),
    }
}

async fn wait_pool_advanced(
    harness: &SimnetHarness,
    node_index: usize,
    current_epoch: EpochNumber,
    timeout: Duration,
) -> tape_api::state::Node {
    let expected_advance = current_epoch.prev();
    let start = Instant::now();

    loop {
        let node = read_node(harness, node_index).await;
        if node.latest_advance_epoch >= expected_advance {
            return node;
        }

        assert!(
            start.elapsed() < timeout,
            "timed out waiting for pool {node_index} to advance at epoch {}, latest {}",
            current_epoch.0,
            node.latest_advance_epoch.0,
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn assert_pool_rewards(harness: &SimnetHarness, claim: PoolClaim) {
    let node = read_node(harness, POOL).await;
    assert_eq!(
        node.pool.commission, claim.commission,
        "pool should accrue the expected commission"
    );
    assert_eq!(
        node.pool.rewards, claim.rewards,
        "pool should accrue net rewards for stakers"
    );
}

async fn expected_pool_claim_at(
    scenario: &SimnetScenario<'_>,
    node_index: usize,
    current_epoch: EpochNumber,
) -> TAPE {
    let prev = current_epoch.prev();
    let epoch = scenario
        .read_epoch_at(prev)
        .await
        .expect("read previous epoch");
    let archive = scenario.read_archive().await.expect("read archive");
    let members = scenario
        .read_committee(prev)
        .await
        .expect("read previous committee");
    let node = Address::from(scenario.node_address(node_index));

    member_reward(node, &members, epoch.total_assigned, archive.rewards_pool)
}

fn member_reward(
    node: Address,
    members: &[Member],
    total_assigned: StorageUnits,
    rewards_pool: TAPE,
) -> TAPE {
    if total_assigned.is_zero() {
        return TAPE::zero();
    }

    let Some(member) = members.iter().find(|member| member.node == node) else {
        return TAPE::zero();
    };
    let weight = member
        .assigned
        .checked_sub(member.blacklisted)
        .expect("blacklisted weight exceeds assigned weight");

    let raw = rewards_pool.as_u128() * weight.as_u128() / total_assigned.as_u128();
    TAPE(u64::try_from(raw).expect("reward overflow"))
}

fn expected_commission(gross: TAPE, commission_rate: BasisPoints) -> TAPE {
    let raw = gross.as_u128() * commission_rate.as_u128() / BasisPoints::MAX as u128;
    TAPE(u64::try_from(raw).expect("commission overflow"))
}

fn expected_rewards(gross: TAPE, commission_rate: BasisPoints) -> TAPE {
    gross.saturating_sub(expected_commission(gross, commission_rate))
}

fn expected_staker_rewards(stake: TAPE, total_stake: TAPE, rewards: TAPE) -> TAPE {
    let pool_value = total_stake.saturating_add(rewards);
    let raw = stake.as_u128() * pool_value.as_u128() / total_stake.as_u128();
    TAPE(u64::try_from(raw).expect("staker amount overflow")).saturating_sub(stake)
}

async fn read_node(
    harness: &SimnetHarness,
    node_index: usize,
) -> tape_api::state::Node {
    let client = RpcClient::from_rpc(harness.chain().rpc().clone());
    let node = Address::from(harness.scenario().node_address(node_index));
    client
        .get_node_by_address(&node)
        .await
        .expect("read node")
}

async fn claim_commission(harness: &SimnetHarness, node_index: usize) {
    let payer = harness.admin();
    let node = harness.node(node_index).expect("node");
    let authority = Address::from(node.authority());
    let node_address = Address::from(harness.scenario().node_address(node_index));
    let mut ixs = build_authority_with_tokens_ix(
        payer.pubkey().into(),
        authority,
        TAPE::zero(),
    )
    .expect("build authority ATA instruction");
    ixs.push(build_claim_commission_ix(
        payer.pubkey().into(),
        authority,
        node_address,
    ));

    harness
        .chain()
        .send_instructions_with_signers_and_advance(
            payer,
            ixs,
            &[node.keypair()],
            harness.config().slot_advance_per_tx,
        )
        .await
        .expect("claim commission");
}

async fn token_balance(harness: &SimnetHarness, owner: Address) -> TAPE {
    let token_account = ata(&owner);
    let account = match harness.chain().rpc().get_account(&token_account).await {
        Ok(account) => account,
        Err(rpc::RpcError::AccountNotFound(_)) => return TAPE::zero(),
        Err(error) => panic!("read token account: {error}"),
    };
    let account = spl_token::state::Account::unpack(&account.data)
        .expect("decode token account");
    TAPE(account.amount)
}

async fn wait_track(
    harness: &SimnetHarness,
    track: Address,
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        let seen = harness
            .nodes()
            .iter()
            .filter(|node| node.is_running())
            .filter(|node| {
                let store = &node.context().store;
                let has_track = store.has_track(track).expect("read track");
                let has_data = store.has_track_data(track).expect("read track data");
                let has_object_info = store
                    .has_object_info(track)
                    .expect("read object info");
                has_track && has_data && has_object_info
            })
            .count();

        if seen == harness.nodes().iter().filter(|node| node.is_running()).count() {
            return;
        }

        assert!(
            start.elapsed() < timeout,
            "timed out waiting for track {track} on running nodes, seen {seen}"
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn tape(amount: &str) -> TAPE {
    TAPE::parse(amount).expect("valid TAPE amount")
}
