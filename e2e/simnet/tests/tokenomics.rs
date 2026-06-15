use std::time::{Duration, Instant};

use rpc::Rpc;
use rpc_client::RpcClient;
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer;
use spl_token::instruction::transfer_checked;
use tape_chain_harness::{TEST_EPOCH_DURATION, TEST_MAX_EPOCH_DURATION};
use tape_api::program::tapedrive::{
    ARCHIVE_ATA, DEFAULT_SUBSIDY_DECAY_BPS, SUBSIDY_ATA, track_pda,
};
use tape_api::program::token::{MINT_ADDRESS, TOKEN_DECIMALS};
use tape_api::utils::ata;
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::system::{Member, NodePreferences};
use tape_core::tape::tape_reservation_cost;
use tape_core::types::coin::TAPE;
use tape_core::types::{BasisPoints, EpochNumber, StorageUnits};
use tape_crypto::Address;
use tape_e2e_simnet::{
    NodeRuntimeMode, SimnetBuilder, SimnetHarness, SimnetScenario, run_simnet_test,
};
use tape_sdk::keys::tape_key::TapeKey;
use tape_store::ops::{ObjectInfoOps, TrackDataOps, TrackOps};

const NODE_COUNT: usize = GROUP_SIZE;
const TARGET_GROUPS: u64 = 1;
const BURN_FEE_BPS: BasisPoints = BasisPoints(1_000);
const SUBSIDY_DECAY_BPS: BasisPoints = DEFAULT_SUBSIDY_DECAY_BPS;
const DATA_EPOCH: EpochNumber = EpochNumber(2);
const CUTOFF_EPOCH: EpochNumber = EpochNumber(3);
const ASSIGNED_EPOCH: EpochNumber = EpochNumber(4);
const CLAIM_EPOCH: EpochNumber = EpochNumber(5);
const RESERVE_CAPACITY: StorageUnits = StorageUnits(3 * StorageUnits::MB);
const RESERVE_EPOCHS: u64 = 4;
const SUBSIDY_TOPUP: TAPE = TAPE(10_000);

#[derive(Clone, Copy, Debug)]
struct ReservationEconomics {
    gross: TAPE,
    policy_burn: TAPE,
    reward_per_epoch: TAPE,
    scheduled: TAPE,
    dust: TAPE,
    total_burn: TAPE,
}

#[derive(Clone, Copy, Debug)]
struct WrittenTrack {
    authority: Address,
    track: Address,
    size: StorageUnits,
}

#[test]
fn tokenomics() {
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
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(1_000).await.expect("stake nodes");
        scenario
            .set_spool_groups_many(&all, TARGET_GROUPS)
            .await
            .expect("set spool group preferences");
        scenario
            .start_network()
            .await
            .expect("start network");
    }

    assert_start_policy(&harness, EpochNumber(1)).await;

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    {
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
    }

    assert_start_policy(&harness, DATA_EPOCH).await;

    let archive = harness
        .scenario()
        .read_archive()
        .await
        .expect("read archive before reserve");
    assert_eq!(
        archive.rewards_pool,
        TAPE::zero(),
        "rewards pool should start empty before any reservation settles"
    );

    let economics = reservation_economics(
        archive.storage_price,
        RESERVE_CAPACITY,
        RESERVE_EPOCHS,
        BURN_FEE_BPS,
    );
    assert_eq!(
        economics.gross,
        economics.total_burn.saturating_add(economics.scheduled),
        "gross reservation cost should split into burned value plus scheduled rewards"
    );
    assert_eq!(
        economics.total_burn,
        economics.policy_burn.saturating_add(economics.dust),
        "total burn should include policy burn plus reward dust"
    );
    assert!(
        economics.policy_burn > TAPE::zero(),
        "test fixture should exercise non-zero policy burn"
    );
    assert!(
        economics.scheduled > TAPE::zero(),
        "test fixture should schedule non-zero storage rewards"
    );

    let admin = Address::from(harness.admin().pubkey());
    let admin_before = owner_token_balance(&harness, admin).await;
    let archive_ata_before = token_account_balance(&harness, ARCHIVE_ATA).await;
    let mint_supply_before = mint_supply(&harness).await;

    let written = reserve_and_write(&harness).await;

    let admin_after_reserve = owner_token_balance(&harness, admin).await;
    let archive_ata_after_reserve = token_account_balance(&harness, ARCHIVE_ATA).await;
    let mint_supply_after_reserve = mint_supply(&harness).await;

    assert_eq!(
        admin_before.saturating_sub(admin_after_reserve),
        economics.gross,
        "reserve should debit the gross reservation cost from the user"
    );
    assert_eq!(
        archive_ata_after_reserve.saturating_sub(archive_ata_before),
        economics.scheduled,
        "archive ATA should receive only scheduled rewards at reserve time"
    );
    assert_eq!(
        mint_supply_before.saturating_sub(mint_supply_after_reserve),
        economics.total_burn,
        "reserve burn should reduce mint supply by policy burn plus reward dust"
    );

    let tape_authority_balance = owner_token_balance(&harness, written.authority).await;
    assert_eq!(
        tape_authority_balance,
        TAPE::zero(),
        "tape authority ATA should not retain reservation funds"
    );
    assert_eq!(
        harness
            .scenario()
            .read_archive()
            .await
            .expect("read archive after reserve")
            .rewards_pool,
        TAPE::zero(),
        "reserve should schedule rewards without moving them into the current pool"
    );

    wait_track(&harness, written.track, active_timeout).await;
    top_up_subsidy(&harness, SUBSIDY_TOPUP).await;
    assert_eq!(
        token_account_balance(&harness, SUBSIDY_ATA).await,
        SUBSIDY_TOPUP,
        "subsidy top-up should land in the subsidy vault"
    );

    let mut expected_pool = TAPE::zero();
    let mut expected_released = TAPE::zero();
    let mut expected_subsidy_balance = SUBSIDY_TOPUP;

    advance_and_assert_rewards(
        &harness,
        epoch_timeout,
        CUTOFF_EPOCH,
        &economics,
        &mut expected_pool,
        &mut expected_released,
        &mut expected_subsidy_balance,
        true,
    )
    .await;

    advance_and_assert_rewards(
        &harness,
        epoch_timeout,
        ASSIGNED_EPOCH,
        &economics,
        &mut expected_pool,
        &mut expected_released,
        &mut expected_subsidy_balance,
        true,
    )
    .await;

    advance_and_assert_rewards(
        &harness,
        epoch_timeout,
        CLAIM_EPOCH,
        &economics,
        &mut expected_pool,
        &mut expected_released,
        &mut expected_subsidy_balance,
        false,
    )
    .await;

    harness.stop_nodes(&all).await.expect("stop node runtimes");

    {
        let scenario = harness.scenario();
        assert_assigned_weights(&scenario, written).await;

        let archive = scenario.read_archive().await.expect("read claim archive");
        assert_eq!(
            archive.rewards_pool, expected_pool,
            "claim pool should contain scheduled rewards plus released subsidy"
        );

        let gross_window = (economics.gross / TAPE(RESERVE_EPOCHS))
            .saturating_mul(TAPE(CLAIM_EPOCH.0 - DATA_EPOCH.0))
            .saturating_add(expected_released);
        assert!(
            archive.rewards_pool < gross_window,
            "claim pool should not include the burned reservation value"
        );

        let members = scenario
            .read_committee(ASSIGNED_EPOCH)
            .await
            .expect("read assigned committee");
        let epoch = scenario
            .read_epoch_at(ASSIGNED_EPOCH)
            .await
            .expect("read assigned epoch");
        let expected_paid = expected_paid(&members, epoch.total_assigned, archive.rewards_pool);
        assert!(expected_paid > TAPE::zero(), "expected non-zero payout");

        scenario.pool_many(&all).await.expect("advance all pools");

        let archive = scenario.read_archive().await.expect("read archive after pool");
        assert_eq!(
            archive.rewards_paid, expected_paid,
            "paid rewards should be computed from scheduled rewards plus subsidy"
        );
    }

    harness.stop_all().await.expect("stop harness");
}

async fn assert_start_policy(harness: &SimnetHarness, expected_epoch: EpochNumber) {
    let scenario = harness.scenario();
    let system = scenario.read_system().await.expect("read system");
    let archive = scenario.read_archive().await.expect("read archive");
    let epoch = scenario.read_epoch().await.expect("read current epoch");
    let peers = read_peer_set(harness).await;

    assert_eq!(
        system.current_epoch, expected_epoch,
        "unexpected current epoch"
    );
    assert_eq!(
        archive.burn_fee_bps, BURN_FEE_BPS,
        "archive burn policy mismatch"
    );
    assert_eq!(
        archive.subsidy_decay_bps, SUBSIDY_DECAY_BPS,
        "archive subsidy decay policy mismatch"
    );

    let expected = NodePreferences {
        min_version: system.min_version,
        committee_size: GROUP_SIZE as u64,
        spool_groups: TARGET_GROUPS,
        burn_fee_bps: BURN_FEE_BPS,
        subsidy_decay_bps: SUBSIDY_DECAY_BPS,
        storage_capacity: archive.storage_capacity,
        storage_price: archive.storage_price,
        epoch_duration: TEST_EPOCH_DURATION,
    };

    assert_eq!(
        epoch.preferences, expected,
        "current epoch preferences should match start_network policy"
    );
    assert_eq!(peers.len(), NODE_COUNT, "unexpected peer count");
    assert!(
        peers.iter().all(|peer| peer.preferences == expected),
        "genesis peer preferences should match start_network policy"
    );
}

async fn reserve_and_write(harness: &SimnetHarness) -> WrittenTrack {
    let scenario = harness.scenario();
    let sdk = scenario.sdk(harness.admin());
    let tape_key = TapeKey::generate();
    let data = vec![0x42; 512];

    sdk.reserve(&tape_key, RESERVE_CAPACITY, RESERVE_EPOCHS)
        .await
        .expect("reserve tokenomics tape");

    let track = sdk
        .write_raw(&tape_key, &data)
        .await
        .expect("write tokenomics track");
    assert_eq!(track.group, GroupIndex(0), "unexpected raw track group");

    WrittenTrack {
        authority: Address::from(tape_key.pubkey()),
        track: track_pda(track.tape, track.track_number).0,
        size: track.size,
    }
}

async fn advance_and_assert_rewards(
    harness: &SimnetHarness,
    timeout: Duration,
    expected_epoch: EpochNumber,
    economics: &ReservationEconomics,
    expected_pool: &mut TAPE,
    expected_released: &mut TAPE,
    expected_subsidy_balance: &mut TAPE,
    assert_archive_ata_balance: bool,
) {
    let scenario = harness.scenario();
    let epoch = scenario
        .self_advance_epoch(timeout)
        .await
        .unwrap_or_else(|error| panic!("advance to epoch {}: {error}", expected_epoch.0));
    assert_eq!(epoch, expected_epoch.0, "unexpected epoch after advance");

    let subsidy_release = bps_amount(*expected_subsidy_balance, SUBSIDY_DECAY_BPS);
    *expected_subsidy_balance = expected_subsidy_balance.saturating_sub(subsidy_release);
    *expected_released = expected_released.saturating_add(subsidy_release);
    *expected_pool = expected_pool
        .saturating_add(economics.reward_per_epoch)
        .saturating_add(subsidy_release);

    let archive = scenario.read_archive().await.expect("read archive");
    assert_eq!(
        archive.rewards_pool, *expected_pool,
        "archive rewards pool should include one scheduled reward slice and subsidy decay"
    );
    assert_eq!(
        token_account_balance(harness, SUBSIDY_ATA).await,
        *expected_subsidy_balance,
        "subsidy vault should decay by the expected amount"
    );
    if assert_archive_ata_balance {
        assert_eq!(
            token_account_balance(harness, ARCHIVE_ATA).await,
            economics.scheduled.saturating_add(*expected_released),
            "archive ATA should hold scheduled rewards plus released subsidy"
        );
    }
}

async fn assert_assigned_weights(scenario: &SimnetScenario<'_>, written: WrittenTrack) {
    let epoch = scenario
        .read_epoch_at(ASSIGNED_EPOCH)
        .await
        .expect("read assigned epoch");
    assert!(
        epoch.total_assigned >= written.size,
        "assigned epoch should include the active user track"
    );

    let members = scenario
        .read_committee(ASSIGNED_EPOCH)
        .await
        .expect("read assigned committee");
    assert_eq!(members.len(), NODE_COUNT, "unexpected committee size");
    assert!(
        members.iter().any(|member| member.assigned >= written.size),
        "at least one committee member should be assigned the active user track"
    );
}

fn reservation_economics(
    storage_price: TAPE,
    capacity: StorageUnits,
    epochs: u64,
    burn_fee_bps: BasisPoints,
) -> ReservationEconomics {
    let gross = tape_reservation_cost(storage_price, capacity, epochs)
        .expect("reservation cost should fit");
    let policy_burn = bps_amount(gross, burn_fee_bps);
    let rewards = gross.saturating_sub(policy_burn);
    let reward_per_epoch = rewards / TAPE(epochs);
    let scheduled = reward_per_epoch * TAPE(epochs);
    let dust = rewards.saturating_sub(scheduled);
    let total_burn = policy_burn.saturating_add(dust);

    ReservationEconomics {
        gross,
        policy_burn,
        reward_per_epoch,
        scheduled,
        dust,
        total_burn,
    }
}

fn bps_amount(amount: TAPE, bps: BasisPoints) -> TAPE {
    assert!(bps.is_valid(), "invalid basis points");
    let raw = amount.as_u128() * bps.as_u128() / BasisPoints::MAX as u128;
    TAPE(u64::try_from(raw).expect("basis point amount overflow"))
}

fn expected_paid(
    members: &[Member],
    total_assigned: StorageUnits,
    rewards_pool: TAPE,
) -> TAPE {
    if total_assigned == StorageUnits::zero() {
        return TAPE::zero();
    }

    let paid = members.iter().fold(0u128, |paid, member| {
        let weight = member
            .assigned
            .checked_sub(member.blacklisted)
            .expect("member blacklisted weight exceeds assigned weight");
        paid + rewards_pool.as_u128() * weight.as_u128() / total_assigned.as_u128()
    });

    TAPE(u64::try_from(paid).expect("expected rewards overflow"))
}

async fn top_up_subsidy(harness: &SimnetHarness, amount: TAPE) {
    let admin = Address::from(harness.admin().pubkey());
    let admin_ata: Pubkey = ata(&admin).into();
    let mint: Pubkey = MINT_ADDRESS.into();
    let subsidy_ata: Pubkey = SUBSIDY_ATA.into();
    let authority: Pubkey = admin.into();

    let ix = transfer_checked(
        &spl_token::ID,
        &admin_ata,
        &mint,
        &subsidy_ata,
        &authority,
        &[],
        amount.as_u64(),
        TOKEN_DECIMALS,
    )
    .expect("build subsidy transfer");

    harness
        .chain()
        .send_instructions_and_advance(
            harness.admin(),
            vec![ix],
            harness.config().slot_advance_per_tx,
        )
        .await
        .expect("top up subsidy vault");
}

async fn read_peer_set(harness: &SimnetHarness) -> Vec<tape_core::system::Peer> {
    RpcClient::from_rpc(harness.chain().rpc().clone())
        .get_peer_set()
        .await
        .expect("read peer set")
}

async fn owner_token_balance(harness: &SimnetHarness, owner: Address) -> TAPE {
    token_account_balance(harness, ata(&owner)).await
}

async fn token_account_balance(harness: &SimnetHarness, address: Address) -> TAPE {
    let account = match harness.chain().rpc().get_account(&address).await {
        Ok(account) => account,
        Err(rpc::RpcError::AccountNotFound(_)) => return TAPE::zero(),
        Err(error) => panic!("read token account {address}: {error}"),
    };
    let account = spl_token::state::Account::unpack(&account.data)
        .expect("decode token account");
    TAPE(account.amount)
}

async fn mint_supply(harness: &SimnetHarness) -> TAPE {
    let account = harness
        .chain()
        .rpc()
        .get_account(&MINT_ADDRESS)
        .await
        .expect("read mint");
    let mint = spl_token::state::Mint::unpack(&account.data)
        .expect("decode mint");
    TAPE(mint.supply)
}

async fn wait_track(harness: &SimnetHarness, track: Address, timeout: Duration) {
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

        if seen == harness.nodes().len() {
            return;
        }

        assert!(
            start.elapsed() < timeout,
            "timed out waiting for track {track} on all nodes, seen {seen}/{}",
            harness.nodes().len()
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
