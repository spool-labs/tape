use solana_sdk::signature::Signer;
use std::time::Duration;
use tape_api::program::tapedrive::node_pda;
use tape_crypto::merkle::hash_leaf;
use tape_crypto::Hash;
use tape_node::features::storage::TrackInfo;
use tape_node_api::SlicePayload;
use tape_node_client::NodeError;
use tape_store::ops::MetaOps;
use tape_store::types::Pubkey as StorePubkey;

use crate::harness::fixture::SimNet;

#[tokio::test]
async fn one_node_boot() {
    let mut net = SimNet::new(1).await.unwrap();

    net.refresh_nodes().await;
    let sim = net.get_node(0);
    let on_chain_node = sim.ctx.rpc.get_node(&sim.ctx.pubkey()).await.unwrap();

    assert!(sim.ctx.control_plane.current_epoch().as_u64() > 0);
    assert_eq!(sim.ctx.control_plane.our_node_id(), on_chain_node.id);
    assert_eq!(sim.ctx.pubkey(), sim.ctx.keypair.pubkey());
    assert!(sim.ctx.storage.store.get_node_status().unwrap().is_none());
    let _status = sim.ctx.control_plane.get_node_status();

    net.stop_nodes().await;
}

#[tokio::test]
async fn node_api_mtls() {
    let mut net = SimNet::new(1).await.unwrap();
    if !net.start_node(0).await {
        return;
    }

    let client = net.build_client(0, 0);
    assert!(client.health_check().await.unwrap());

    net.stop_nodes().await;
}

#[tokio::test]
async fn two_nodes_resolve() {
    let mut net = SimNet::new(2).await.unwrap();

    net.refresh_nodes().await;

    let node_a = net.get_node(0);
    let node_b = net.get_node(1);
    let node_b_chain = node_b.ctx.rpc.get_node(&node_b.ctx.pubkey()).await.unwrap();
    let (got_address, got_node) = node_a.ctx.rpc.get_node_by_id(node_b_chain.id).await.unwrap();
    let (expected_address, _) = node_pda(node_b.ctx.pubkey());
    assert_eq!(got_address, expected_address);
    assert_eq!(got_node.id, node_b_chain.id);

    net.stop_nodes().await;
}

#[tokio::test]
async fn two_nodes_health() {
    let mut net = SimNet::new(2).await.unwrap();
    if !net.start_pair(0, 1).await {
        return;
    }

    let client_b_to_a = net.build_client(1, 0);
    let client_a_to_b = net.build_client(0, 1);
    assert!(client_b_to_a.health_check().await.unwrap());
    assert!(client_a_to_b.health_check().await.unwrap());

    net.stop_nodes().await;
}

#[tokio::test]
async fn two_nodes_runtime_log() {
    let mut net = SimNet::new(2).await.unwrap();
    if !net.start_pair(0, 1).await {
        return;
    }

    net.wait_runtime_log(0, "Runtime starting", Duration::from_secs(5))
        .await
        .unwrap();
    net.wait_runtime_log(1, "Runtime starting", Duration::from_secs(5))
        .await
        .unwrap();

    net.stop_nodes().await;
}

#[tokio::test]
async fn meta_ingest_ok() {
    let mut net = SimNet::new(2).await.unwrap();
    if !net.start_pair(0, 1).await {
        return;
    }

    net.seed_authorization(0, 1);

    let client = net.build_client(0, 1);
    let track = tape_crypto::Pubkey::new_unique();
    let track_info = TrackInfo {
        tape_address: StorePubkey::new([9u8; 32]),
        spool_group: 0,
        original_size: 1234,
        stripe_size: 1234,
        stripe_count: 1,
        encoding_type: 1,
        encoding_params: 0,
        commitment: vec![Hash::default(); 20],
    };
    let track_data = wincode::serialize(&track_info).unwrap();

    client
        .put_metadata_internal(&track.to_string(), track_data)
        .await
        .unwrap();

    let stored_info = net.get_node(1).ctx.storage.get_track(track).unwrap().unwrap();
    assert_eq!(stored_info.original_size, track_info.original_size);
    assert_eq!(stored_info.tape_address, track_info.tape_address);

    net.stop_nodes().await;
}

#[tokio::test]
async fn slice_ingest_ok() {
    let mut net = SimNet::new(2).await.unwrap();
    if !net.start_pair(0, 1).await {
        return;
    }

    net.seed_authorization(0, 1);

    let Some(spool_id) = net.owned_spool(1) else {
        eprintln!("skipping: receiver owns no spools in current committee");
        net.stop_nodes().await;
        return;
    };

    let client = net.build_client(0, 1);
    let track = tape_crypto::Pubkey::new_unique();
    let slice_data = b"sim-slice-data".to_vec();
    let payload = SlicePayload {
        data: slice_data.clone(),
        leaf_hash: hash_leaf(&slice_data),
        merkle_proof: [Hash::default(); tape_node_api::MERKLE_HEIGHT],
    };

    client
        .put_slice_internal(&track.to_string(), spool_id, &payload)
        .await
        .unwrap();

    let stored_slice = net
        .get_node(1)
        .ctx
        .storage
        .get_slice(spool_id, track)
        .unwrap()
        .unwrap();
    assert_eq!(stored_slice, slice_data);

    net.stop_nodes().await;
}

#[tokio::test]
async fn slice_bad_hash() {
    let mut net = SimNet::new(2).await.unwrap();
    if !net.start_pair(0, 1).await {
        return;
    }

    net.seed_authorization(0, 1);

    let Some(spool_id) = net.owned_spool(1) else {
        eprintln!("skipping: receiver owns no spools in current committee");
        net.stop_nodes().await;
        return;
    };

    let client = net.build_client(0, 1);
    let track = tape_crypto::Pubkey::new_unique();
    let payload = SlicePayload {
        data: b"bad-hash-slice".to_vec(),
        leaf_hash: Hash::default(),
        merkle_proof: [Hash::default(); tape_node_api::MERKLE_HEIGHT],
    };

    let err = client
        .put_slice_internal(&track.to_string(), spool_id, &payload)
        .await
        .unwrap_err();
    match err {
        NodeError::ServerError { status, .. } => assert_eq!(status, 400),
        other => panic!("expected server error, got {other:?}"),
    }
    assert!(
        net.get_node(1)
            .ctx
            .storage
            .get_slice(spool_id, track)
            .unwrap()
            .is_none()
    );

    net.stop_nodes().await;
}
