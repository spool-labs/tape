use tape_api::program::tapedrive::node_pda;
use tape_node_client::{NodeClient, NodeClientBuilder};
use tape_store::ops::CommitteeOps;
use tape_store::types::{NodeInfo, Pubkey as StorePubkey};

use crate::harness::node::SimNode;

pub fn seed_authorization(source: &SimNode, target: &SimNode) {
    let epoch = target.ctx.control_plane.current_epoch();
    let (source_address, _) = node_pda(source.ctx.pubkey());
    let source_node = source.ctx.control_plane.get_node();

    target
        .ctx
        .storage
        .store
        .put_committee(
            epoch,
            vec![NodeInfo {
                node_address: StorePubkey::new(source_address.to_bytes()),
                bls_pubkey: source
                    .ctx
                    .bls_keypair
                    .public_key()
                    .map_err(|err| anyhow::anyhow!("derive bls pubkey: {err:?}"))
                    .unwrap(),
                tls_pubkey: StorePubkey::new(source_node.metadata.network_tls.to_bytes()),
                network_address: source_node.metadata.network_address,
                spools: vec![],
            }],
        )
        .unwrap();
}

pub fn build_client(source: &SimNode, target: &SimNode) -> NodeClient {
    let target_node = target.ctx.control_plane.get_node();
    let target_address = target_node
        .metadata
        .network_address
        .to_socket_addr()
        .unwrap()
        .to_string();

    NodeClientBuilder::new()
        .server_tls_key(target_node.metadata.network_tls)
        .with_client_paths(Some(source.tls_crt.clone()), Some(source.tls_key.clone()))
        .build(&target_address)
        .unwrap()
}

pub fn owned_spool(node: &SimNode) -> Option<u16> {
    node.ctx.control_plane.get_our_spools().first().copied()
}
