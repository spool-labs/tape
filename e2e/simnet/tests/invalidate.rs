use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_signer::Signer;
use tape_api::compute::INVALIDATE_TRACK_CU;
use tape_api::instruction::build_invalidate_track_ix;
use tape_api::program::tapedrive::track_pda;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::bls::BlsSignature;
use tape_core::cert::track::TrackInvalidateMessage;
use tape_core::erasure::GROUP_SIZE;
use tape_core::track::types::CompressedTrackProof;
use tape_core::types::{BasisPoints, SpoolBitmap, StorageUnits};
use tape_crypto::address::Address;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, SimnetHarness, run_simnet_test};
use tape_sdk::keys::tape_key::TapeKey;

const TARGET_GROUPS: u64 = 5;

#[test]
fn invalidate_track() {
    run_simnet_test(invalidate_track_inner);
}

async fn invalidate_track_inner() {
    let node_count = 20;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..node_count).collect();
    let health_timeout = Duration::from_secs(30);

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
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let active_timeout = Duration::from_secs(60);
    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
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

        let epoch2 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 2");
        assert_eq!(epoch2, 2, "expected epoch 2");

        let epoch3 = scenario
            .self_advance_epoch(epoch_timeout)
            .await
            .expect("advance to epoch 3");
        assert_eq!(epoch3, 3, "expected epoch 3");
        scenario
            .wait_nodes_active(&all, active_timeout)
            .await
            .expect("all nodes active at epoch 3");
    }

    let scenario = harness.scenario();
    let sdk = scenario.sdk(harness.admin());

    let mut rng = StdRng::seed_from_u64(0x1BAD_7AC4);
    let blob_data = random_bytes(&mut rng, 128 * 1024);
    let reserve_capacity = StorageUnits::from_bytes(4 * blob_data.len() as u64)
        + StorageUnits::mb(2);

    let tape_key = TapeKey::generate();
    let tape_address = tape_key.address();
    sdk.reserve(&tape_key, reserve_capacity, 4)
        .await
        .expect("reserve tape");

    let track = sdk
        .write_track(&tape_key, &blob_data)
        .await
        .expect("write blob track");
    assert!(track.is_certified(), "blob track should be certified");

    let track_address = track_pda(track.tape, track.track_number).0;

    // Peers only serve the proof once they have ingested the finalized track.
    let start = Instant::now();
    let proof = loop {
        if let Ok(proof) = sdk.get_track_proof(&track_address).await {
            break proof;
        }
        if start.elapsed() >= active_timeout {
            panic!("timed out waiting for a track proof");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let tape_before = sdk.get_tape(&tape_address).await.expect("tape before");
    assert_eq!(
        tape_before.used, track.size,
        "tape should charge the track size"
    );

    submit_invalidate(&harness, &proof)
        .await
        .expect("invalidate track");

    let tape_after = sdk.get_tape(&tape_address).await.expect("tape after");
    assert_eq!(
        tape_after.used,
        StorageUnits(0),
        "invalidation should return the track capacity"
    );
    assert_eq!(
        tape_after.capacity, tape_before.capacity,
        "capacity itself should not change"
    );

    // Nodes converge on the invalidated leaf once they ingest the event; the
    // proof only verifies against the updated on-chain root.
    let start = Instant::now();
    let invalidated_proof = loop {
        if let Ok(proof) = sdk.get_track_proof(&track_address).await {
            if proof.state.is_invalidated() {
                break proof;
            }
        }
        if start.elapsed() >= active_timeout {
            panic!("timed out waiting for nodes to serve the invalidated track");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let result = submit_invalidate(&harness, &invalidated_proof).await;
    assert!(result.is_err(), "re-invalidation must be rejected");

    let tape_after_retry = sdk.get_tape(&tape_address).await.expect("tape after retry");
    assert_eq!(
        tape_after_retry.used,
        StorageUnits(0),
        "rejected re-invalidation must not touch capacity"
    );

    let blob_data_b = random_bytes(&mut rng, 128 * 1024);
    let track_b = sdk
        .write_track(&tape_key, &blob_data_b)
        .await
        .expect("write blob track after invalidate");
    assert!(track_b.is_certified(), "second blob track should be certified");

    let tape_final = sdk.get_tape(&tape_address).await.expect("tape final");
    assert_eq!(
        tape_final.used, track_b.size,
        "returned capacity should be reusable"
    );

    drop(sdk);
    harness.stop_all().await.expect("stop runtimes");
}

// Sign the invalidate message with every group spool owned by a harness node
// and submit the instruction as the admin.
async fn submit_invalidate(
    harness: &SimnetHarness,
    proof: &CompressedTrackProof,
) -> anyhow::Result<()> {
    let scenario = harness.scenario();
    let sdk = scenario.sdk(harness.admin());

    let system = sdk.rpc().get_system().await?;
    let epoch = system.current_epoch;
    let group = sdk.rpc().get_group(epoch, proof.state.group).await?;

    let message = TrackInvalidateMessage::new(epoch, proof.state.get_hash()).to_bytes();

    let mut indices = Vec::new();
    let mut partials = Vec::new();
    for (spool_index, spool) in group.spools.iter().enumerate() {
        let owner = harness.nodes().iter().find(|node| {
            node.bls_keypair()
                .public_key()
                .map(|pubkey| pubkey == spool.bls_pubkey)
                .unwrap_or(false)
        });
        let Some(owner) = owner else { continue };

        partials.push(owner.bls_keypair().sign(message).expect("sign invalidate"));
        indices.push(spool_index);
    }
    assert_eq!(
        indices.len(),
        GROUP_SIZE,
        "every group spool should belong to a harness node"
    );

    let bitmap = SpoolBitmap::from_indices(&indices);
    let signature = BlsSignature::aggregate(&partials).expect("aggregate signatures");

    let admin: Address = harness.admin().pubkey().into();
    let ix = build_invalidate_track_ix(admin, *proof, epoch, bitmap, signature);
    let budget = ComputeBudgetInstruction::set_compute_unit_limit(INVALIDATE_TRACK_CU);

    harness
        .chain()
        .send_instructions_and_advance(
            harness.admin(),
            vec![budget, ix],
            harness.config().slot_advance_per_tx,
        )
        .await?;

    Ok(())
}

fn random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    rng.fill_bytes(&mut bytes);
    bytes
}
