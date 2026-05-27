use std::time::{Duration, Instant};

use peer_http::HttpApi;
use rpc_litesvm::LiteSvmRpc;
use tape_chain_harness::TEST_EPOCH_DURATION;
use tape_api::program::tapedrive::{history_pda, track_pda};
use tape_core::staking::RateSpan;
use tape_core::types::coin::TAPE;
use tape_core::types::{BasisPoints, EpochNumber};
use tape_crypto::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, run_simnet_test};
use tape_sdk::error::TapedriveError;
use tape_sdk::keys::stake_key::StakeKey;
use tape_sdk::tapedrive::Tapedrive;

const NODE_COUNT: usize = 20;
const POOL: usize = 0;
const ACTIVATION_EPOCH: EpochNumber = EpochNumber(0);
const FIRST_HISTORY_EPOCH: EpochNumber = EpochNumber(2);
const WITHDRAW_EPOCH: EpochNumber = EpochNumber(4);
const CLOSED_WITHDRAW_EPOCH: EpochNumber = EpochNumber(5);

#[test]
fn history_tape() {
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
    let health_timeout = Duration::from_secs(30);
    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_EPOCH_DURATION.0 * 5);

    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(1_000).await.expect("stake nodes");

        let sdk = scenario.sdk(harness.admin());
        let pool = Address::from(scenario.node_address(POOL));
        sdk.stake_with_pool(&stake_key, pool, TAPE(100))
            .await
            .expect("stake with pool");

        let stake = sdk
            .rpc()
            .get_stake(&stake_key.pubkey().into())
            .await
            .expect("stake account");
        assert_eq!(
            stake.inner.activation_epoch, ACTIVATION_EPOCH,
            "stake should activate in the bootstrap epoch"
        );

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
        .expect("advance to first history epoch");
    assert_eq!(
        epoch, FIRST_HISTORY_EPOCH.0,
        "unexpected first history epoch"
    );

    scenario
        .pool_many(&[POOL])
        .await
        .expect("advance pool for first history span");

    let pool = Address::from(scenario.node_address(POOL));
    let sdk = scenario.sdk(harness.admin());
    wait_span(&sdk, pool, ACTIVATION_EPOCH, active_timeout).await;

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
        "stake should be in unlocking state"
    );
    assert_eq!(
        stake.inner.withdraw_epoch(),
        Some(WITHDRAW_EPOCH),
        "unexpected withdraw epoch"
    );
    assert!(
        !stake.inner.unlock_shares.is_zero(),
        "unlock should store the activation shares"
    );

    while scenario
        .current_epoch_number()
        .await
        .expect("read current epoch")
        < CLOSED_WITHDRAW_EPOCH.0
    {
        scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance epoch");
        scenario
            .pool_many(&[POOL])
            .await
            .expect("advance pool");
    }

    wait_span(&sdk, pool, WITHDRAW_EPOCH, active_timeout).await;

    sdk.unstake_from_pool(&stake_key)
        .await
        .expect("unstake from pool");

    assert!(
        matches!(
            sdk.rpc().get_stake(&stake_key.pubkey().into()).await,
            Err(rpc::RpcError::AccountNotFound(_))
        ),
        "stake account should be closed"
    );

    harness.stop_all().await.expect("stop harness");
}

async fn wait_span(
    sdk: &Tapedrive<LiteSvmRpc, HttpApi>,
    pool: Address,
    epoch: EpochNumber,
    timeout: Duration,
) {
    let start = Instant::now();
    let mut last_error = None;

    loop {
        match find_span(sdk, pool, epoch).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(error) => last_error = Some(error.to_string()),
        }

        assert!(
            start.elapsed() < timeout,
            "timed out waiting for SDK-visible history span containing epoch {}{}",
            epoch.0,
            last_error
                .as_deref()
                .map(|error| format!(", last error: {error}"))
                .unwrap_or_default(),
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn find_span(
    sdk: &Tapedrive<LiteSvmRpc, HttpApi>,
    pool: Address,
    epoch: EpochNumber,
) -> Result<bool, TapedriveError> {
    let history = history_pda(pool).0;
    let mut cursor = None;

    loop {
        let (tracks, next_cursor) = sdk.list_tracks_by_tape(&history, cursor, 128).await?;

        for track in tracks {
            let track_address = track_pda(history, track.track_number).0;
            let data = match sdk.read(&track_address).await {
                Ok(data) => data,
                Err(TapedriveError::NotFound) => continue,
                Err(error) => return Err(error),
            };
            if data.len() != core::mem::size_of::<RateSpan>() {
                continue;
            }

            let span = bytemuck::try_from_bytes::<RateSpan>(&data)
                .map_err(|error| TapedriveError::Encoding(error.to_string()))?;
            if span.node == pool && span.contains(epoch) {
                return Ok(true);
            }
        }

        match next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(false),
        }
    }
}
