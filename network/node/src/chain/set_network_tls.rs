use rpc::{Rpc, RpcError};
use rpc_client::RpcClient;
use tape_api::instruction::build_set_network_tls_ix;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair;
use tape_crypto::tx::Txid;

/// Submit `SetNetworkTls` to overwrite the on-chain `network_tls` field for
/// the authority's Node account.
pub async fn submit_set_network_tls<Blockchain: Rpc>(
    rpc: &RpcClient<Blockchain>,
    authority: &Keypair,
    node_address: Address,
    network_tls: NetworkTlsPubkey,
) -> Result<Txid, RpcError> {
    let authority_addr = authority.address();
    let ix = build_set_network_tls_ix(authority_addr, authority_addr, node_address, network_tls);
    rpc.send_instructions(authority, vec![ix]).await
}

#[cfg(test)]
mod tests {
    use tape_core::types::EpochNumber;
    use tape_core::types::tls::NetworkTlsPubkey;

    use super::submit_set_network_tls;
    use crate::harness::NodeHarness;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EpochNumber(3))
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(7);
        let network_tls = NetworkTlsPubkey::new_unique();

        submit_set_network_tls(ctx.rpc.as_ref(), ctx.signer(), ctx.node_address(), network_tls)
            .await
            .expect("submit set network tls");

        let node = ctx
            .rpc
            .get_node(&ctx.pubkey().address())
            .await
            .expect("fetch node");
        assert_eq!(node.metadata.network_tls, network_tls);
    }
}
