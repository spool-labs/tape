use tape_rpc::Rpc;
use rpc_litesvm::LiteSvmRpc;
use solana_keypair::Keypair;
use solana_message::v1::{Message, TransactionConfig};
use solana_message::VersionedMessage;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use solana_system_interface::instruction as system_instruction;
use solana_transaction::versioned::VersionedTransaction;
use tape_api::compute::{
    DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT, MAX_LOADED_ACCOUNTS_DATA_SIZE,
};
use tape_api::prelude::TAPENET_PROFILE;

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
    let config = TransactionConfig::empty()
        .with_compute_unit_limit(DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT)
        .with_loaded_accounts_data_size_limit(MAX_LOADED_ACCOUNTS_DATA_SIZE);
    let message = Message::try_compile_with_config(&payer.pubkey(), &[ix], blockhash, config)
        .expect("compile transfer");
    let tx = VersionedTransaction::try_new(VersionedMessage::V1(message), &[&payer])
        .expect("sign transfer");

    rpc.send_and_confirm_transaction(&tx, rpc.commitment(), false)
        .await
        .expect("transfer should succeed");

    // The tx was recorded at pending_slot (slot + 1). Warp to make it visible.
    rpc.warp_to_slot(slot + 1).expect("warp past tx slot");
    let block = rpc.get_block(slot + 1).await.expect("block exists");

    let parsed = tape_blocks::parse(&block, TAPENET_PROFILE).expect("block should parse");
    assert_eq!(parsed.tx_count, 1, "block parser should see one transaction");
}
