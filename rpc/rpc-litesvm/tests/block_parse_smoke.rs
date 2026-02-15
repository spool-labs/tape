use rpc::Rpc;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::system_instruction;
use solana_sdk::transaction::Transaction;

#[tokio::test]
async fn fetched_block_is_parseable_by_tape_blocks() {
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

    rpc.send_and_confirm_transaction(&tx)
        .await
        .expect("transfer should succeed");

    let block = rpc.get_block(slot).await.expect("block exists");

    let parsed = tape_blocks::parse(&block).expect("block should parse");
    assert_eq!(parsed.tx_count, 1, "block parser should see one transaction");
}
