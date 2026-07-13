use rpc::Rpc;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use solana_system_interface::instruction as system_instruction;
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as TapeKeypair;

#[tokio::test]
async fn rpc_client_can_use_litesvm_backend_for_basic_transfer() {
    let rpc = LiteSvmRpc::new();
    let client = RpcClient::from_rpc(rpc.clone());

    let payer = Keypair::new();
    let recipient = Pubkey::new_unique();

    rpc.airdrop(&payer.pubkey(), 10_000_000)
        .expect("airdrop payer");

    // RpcClient signs with tape's own Signer; derive it from the funded payer.
    let signer = TapeKeypair::from_solana_keypair(&payer).expect("derive tape keypair");

    let transfer_ix = system_instruction::transfer(&payer.pubkey(), &recipient, 1_000_000);
    let sig = client
        .send_instructions(&signer, vec![transfer_ix])
        .await
        .expect("transfer should succeed");

    let status = client
        .rpc()
        .get_signature_status(&sig)
        .await
        .expect("status call")
        .expect("status exists");
    assert!(status.is_ok(), "tx should be confirmed successfully");

    let recipient_addr: Address = recipient.into();
    let recipient_account = client
        .rpc()
        .get_account(&recipient_addr)
        .await
        .expect("recipient account");
    assert_eq!(recipient_account.lamports, 1_000_000);
}
