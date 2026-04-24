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
