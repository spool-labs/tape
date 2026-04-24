use rpc::{Rpc, RpcError};
use rpc_client::RpcClient;
use tape_api::consts::NAME_LENGTH;
use tape_api::instruction::build_register_node_ix;
use tape_core::bls::{BlsPubkey, BlsSignature};
use tape_core::types::BasisPoints;
use tape_core::types::network::NetworkAddress;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::ed25519::Keypair;
use tape_crypto::tx::Txid;

pub async fn submit_register_node<Blockchain: Rpc>(
    rpc: &RpcClient<Blockchain>,
    keypair: &Keypair,
    name: [u8; NAME_LENGTH],
    commission: BasisPoints,
    network_address: NetworkAddress,
    network_tls: NetworkTlsPubkey,
    bls_pubkey: BlsPubkey,
    bls_pop: BlsSignature,
) -> Result<Txid, RpcError> {
    let authority = keypair.address();

    let ix = build_register_node_ix(
        authority,
        authority,
        name,
        commission,
        network_address,
        network_tls,
        bls_pubkey,
        bls_pop,
    );

    rpc.send_instructions(keypair, vec![ix]).await
}

#[cfg(test)]
mod tests {
    use rpc_client::RpcClient;
    use tape_api::utils::to_name;
    use tape_core::bls::BlsPrivateKey;
    use tape_core::types::BasisPoints;
    use tape_core::types::network::NetworkAddress;
    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_core::types::EpochNumber;
    use tape_crypto::ed25519::Keypair;

    use super::submit_register_node;
    use crate::harness::NodeHarness;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EpochNumber(3))
            .build()
            .await
            .expect("build harness");

        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let authority = keypair.to_solana_pubkey();
        harness
            .rpc()
            .airdrop(&authority, 10_000_000_000)
            .expect("airdrop");

        let bls = BlsPrivateKey::from_random();
        let bls_pubkey = bls.public_key().expect("bls pubkey");
        let bls_pop = bls.proof_of_possession().expect("bls pop");
        let name = to_name("test-register");
        let commission = BasisPoints(500);
        let address = NetworkAddress::new_ipv4([10, 0, 0, 1], 443);
        let tls_keypair = Keypair::new(&mut rng);
        let network_tls = NetworkTlsPubkey::new(tls_keypair.pubkey().to_bytes());

        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        submit_register_node(
            &rpc, &keypair, name, commission, address, network_tls, bls_pubkey, bls_pop,
        )
        .await
        .expect("register node");

        let node = rpc.get_node(&keypair.address()).await.expect("get node");
        assert_eq!(node.metadata.bls_pubkey, bls_pubkey);
        assert_eq!(node.metadata.network_address, address);
        assert_eq!(node.metadata.network_tls, network_tls);
        assert_eq!(node.pool.commission_rate, commission);
    }
}
