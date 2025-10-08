#![cfg(test)]

pub mod utils;
use utils::*;

use tape_api::prelude::*;
use solana_sdk::signer::Signer;

#[test]
fn test_storage() {
    let (mut svm, payer) = setup_environment();

    initialize_program(&mut svm, &payer);

    let tape_address = reserve_tape(&mut svm, &payer, StorageUnits(10), EpochNumber(1), EpochNumber(3));

    let tape = get_tape_state(&svm, &tape_address);
    assert_eq!(tape.authority, payer.pubkey());
    assert_eq!(tape.capacity, StorageUnits(10));
    assert_eq!(tape.used, StorageUnits(0));
    assert_eq!(tape.active_epoch, EpochNumber(1));
    assert_eq!(tape.expiry_epoch, EpochNumber(3));
    assert_eq!(tape.total_blobs, 0);

    let (archive_address, _) = archive_pda();
    let archive = get_archive_state(&svm, &archive_address);

    assert_eq!(archive.future_usage.get(EpochNumber(0)).unwrap(), StorageUnits(0));
    assert_eq!(archive.future_usage.get(EpochNumber(1)).unwrap(), StorageUnits(10));
    assert_eq!(archive.future_usage.get(EpochNumber(2)).unwrap(), StorageUnits(10));
    assert_eq!(archive.future_usage.get(EpochNumber(3)).unwrap(), StorageUnits(0));

    let (treasury_address, _) = treasury_pda();
    let treasury = get_treasury_state(&svm, &treasury_address);

    assert_eq!(treasury.future_rewards.get(EpochNumber(0)).unwrap(), TAPE::zero());
    assert_eq!(treasury.future_rewards.get(EpochNumber(1)).unwrap(), TAPE::from("0.001"));
    assert_eq!(treasury.future_rewards.get(EpochNumber(2)).unwrap(), TAPE::from("0.001"));
    assert_eq!(treasury.future_rewards.get(EpochNumber(3)).unwrap(), TAPE::zero());

    //println!("{:?}", archive);

    //assert!(false);
}

