use rpc::Rpc;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::system_instruction;
use solana_sdk::transaction::Transaction;

#[tokio::test]
async fn basic_slot_and_blockhash_are_available() {
    let rpc = LiteSvmRpc::new();
    let slot = rpc.get_slot().await.expect("slot available");
    let hash = rpc
        .get_latest_blockhash()
        .await
        .expect("blockhash available");

    assert!(slot < u64::MAX);
    assert_ne!(hash, solana_sdk::hash::Hash::default());
}

#[tokio::test]
async fn send_transfer_records_block_and_status() {
    let rpc = LiteSvmRpc::new();
    let payer = Keypair::new();
    let recipient = Pubkey::new_unique();

    rpc.airdrop(&payer.pubkey(), 10_000_000)
        .expect("airdrop payer");

    let blockhash = rpc
        .get_latest_blockhash()
        .await
        .expect("blockhash available");
    let slot = rpc.get_slot().await.expect("slot available");

    let ix = system_instruction::transfer(&payer.pubkey(), &recipient, 1_000_000);
    let tx = Transaction::new(
        &[&payer],
        Message::new(&[ix], Some(&payer.pubkey())),
        blockhash,
    );

    let sig = rpc
        .send_and_confirm_transaction(&tx)
        .await
        .expect("transfer should succeed");

    let status = rpc
        .get_signature_status(&sig)
        .await
        .expect("status call")
        .expect("status exists");
    assert!(status.is_ok(), "transaction should be successful");

    let block = rpc.get_block(slot).await.expect("block exists");
    let txs = block.transactions.expect("full tx details present");
    assert_eq!(txs.len(), 1, "one tx recorded in block");

    let recipient_account = rpc.get_account(&recipient).await.expect("recipient account");
    assert_eq!(recipient_account.lamports, 1_000_000);
}
