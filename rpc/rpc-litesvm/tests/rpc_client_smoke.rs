use rpc::Rpc;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::system_instruction;

#[tokio::test]
async fn rpc_client_can_use_litesvm_backend_for_basic_transfer() {
    let rpc = LiteSvmRpc::new();
    let client = RpcClient::from_rpc(rpc.clone());

    let payer = Keypair::new();
    let recipient = Pubkey::new_unique();

    rpc.airdrop(&payer.pubkey(), 10_000_000)
        .expect("airdrop payer");

    let transfer_ix = system_instruction::transfer(&payer.pubkey(), &recipient, 1_000_000);
    let sig = client
        .send_instructions(&payer, vec![transfer_ix])
        .await
        .expect("transfer should succeed");

    let status = client
        .rpc()
        .get_signature_status(&sig)
        .await
        .expect("status call")
        .expect("status exists");
    assert!(status.is_ok(), "tx should be confirmed successfully");

    let recipient_account = client
        .rpc()
        .get_account(&recipient)
        .await
        .expect("recipient account");
    assert_eq!(recipient_account.lamports, 1_000_000);
}
