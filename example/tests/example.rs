#![cfg(test)]
#![allow(unused)]

pub mod utils;
use utils::*;
use steel::*;
use tape_api::prelude::*;

use solana_sdk::{
    signature::Keypair, 
    signer::Signer,
    transaction::Transaction,
    clock::Clock,
    pubkey::Pubkey
};

use litesvm::{types::{TransactionMetadata, TransactionResult}, LiteSVM};

#[test]
fn run_integration() {
    let mut svm = setup_svm();

    let payer = create_payer(&mut svm);
    let payer_pk = payer.pubkey();

    // Create a tape that we can write to
    let (tape_address, writer_address) = create_tape(&mut svm, &payer, "tape-name");

    // Call our example program
    let ix = Instruction {
        program_id: example::ID,
        accounts: vec![
            AccountMeta::new(payer_pk, true),
            AccountMeta::new(tape_address, false),
            AccountMeta::new(writer_address, false),
            AccountMeta::new_readonly(tape_api::ID, false),
        ],
        data: vec![],
    };

    let blockhash = svm.latest_blockhash();
    let tx = Transaction::new_signed_with_payer(&[ix], Some(&payer_pk), &[&payer], blockhash);
    let res = send_tx(&mut svm, tx);
    assert!(res.is_ok());

    // Verify the on-chain state matches the local expectations

    let account = svm.get_account(&tape_address).unwrap();
    let tape = Tape::unpack(&account.data).unwrap();

    let mut local_tree = SegmentTree::new(&[tape_address.as_ref()]); 

    let data = vec![42; 1024];

    let segments = data.chunks(SEGMENT_SIZE);
    for (segment_number, segment) in segments.enumerate() {
        let canonical_segment = padded_array::<SEGMENT_SIZE>(segment);

        assert!(write_segment(
            &mut local_tree,
            segment_number as u64,
            &canonical_segment,
        ).is_ok());
    }

    assert_eq!(tape.total_segments, 1024 / SEGMENT_SIZE as u64);
    assert_eq!(tape.merkle_root, local_tree.get_root().as_ref());
}

