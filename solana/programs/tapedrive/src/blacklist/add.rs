use bytemuck::bytes_of;
use tape_api::program::prelude::*;
use tape_core::track::data::TrackMeta;
use tape_core::track::types::{TrackKind, TrackState};
use tape_crypto::hash::hash;

use crate::track::helpers::append_track;

pub fn process_add_to_blacklist(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = AddToBlacklist::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        node_info,
        system_info,
        blacklist_info,
        slot_hashes_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    if !args.entry.is_valid() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let node = node_info.as_account::<Node>(&tapedrive::ID)?;
    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let node_address = (*node_info.key).into();
    let (blacklist_address, _) = blacklist_pda(node_address);

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let tape = blacklist_info
        .is_writable()?
        .has_address(&blacklist_address.into())?
        .as_account_mut::<Tape>(&tapedrive::ID)?;

    if !tape.is_blacklist_tape(node.id) {
        return Err(ProgramError::InvalidAccountData);
    }

    let entry_bytes = bytes_of(&args.entry);
    let meta = TrackMeta {
        kind: TrackKind::Inline,
        state: TrackState::Certified,
        size: StorageUnits::from_bytes(entry_bytes.len() as u64),
        value_hash: hash(entry_bytes),
    };

    append_track(
        system,
        tape,
        slot_hashes_info,
        blacklist_address,
        args.entry.key(),
        meta,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::size_of;
    use solana_sdk::account::Account;
    use tape_core::track::TRACK_TREE_HEIGHT;
    use tape_core::track::archive::TrackArchive;
    use tape_core::track::types::CompressedTrack;
    use tape_crypto::hash::hashv;
    use tape_crypto::merkle::MerkleTree;
    use tape_test::*;

    fn slot_hashes_account(seed: Hash) -> (Pubkey, Account) {
        let mut data = vec![0u8; 48];
        data[0] = 1;
        data[16..48].copy_from_slice(&seed.0);

        (
            sysvar::slot_hashes::ID,
            Account {
                lamports: 1,
                data,
                owner: sysvar::ID,
                executable: false,
                rent_epoch: 0,
            },
        )
    }

    fn selected_group(
        seed: Hash,
        tape_address: Address,
        tape_id: TapeNumber,
        track_number: TrackNumber,
        groups: u64,
    ) -> GroupIndex {
        let mixed_hash = hashv(&[
            seed.as_ref(),
            tape_address.as_ref(),
            &tape_id.pack(),
            &track_number.pack(),
        ]);
        let mixed = u64::from_le_bytes(mixed_hash.0[..8].try_into().unwrap());
        GroupIndex(mixed % groups)
    }

    #[test]
    fn add_to_blacklist() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let node_address = Pubkey::new_unique();
        let (system_address, _) = system_pda();
        let (blacklist_address, _) = blacklist_pda(node_address.into());

        let entry = BlacklistEntry::track(Address::new_unique());
        let entry_size = size_of::<BlacklistEntry>() as u64;
        let track_number = TrackNumber(0);
        let seed = Hash::from([0x42; 32]);
        let live_group_count = 50;

        let instruction = build_add_to_blacklist_ix(
            fee_payer.into(),
            authority.into(),
            node_address.into(),
            entry,
        );

        let system = System {
            current_epoch: EpochNumber(2),
            live_group_count,
            ..System::zeroed()
        };
        let node = Node {
            id: NodeId(9),
            authority: authority.into(),
            ..Node::zeroed()
        };
        let mut tape = Tape::blacklist(node.id, EpochNumber(0));
        tape.tracks = TrackArchive {
            num_tracks: 0,
            next_number: track_number,
            tree: MerkleTree::<TRACK_TREE_HEIGHT>::new(),
        };

        let group = selected_group(seed, blacklist_address, tape.id, track_number, live_group_count);
        let track = CompressedTrack {
            tape: blacklist_address,
            key: entry.key(),
            track_number,
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(entry_size),
            group,
            value_hash: hash(bytes_of(&entry)),
        };
        let mut expected_tree = MerkleTree::<TRACK_TREE_HEIGHT>::new();
        expected_tree.add_leaf_hash(track.get_hash()).unwrap();

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(blacklist_address, tape.pack(), tapedrive::ID),
            slot_hashes_account(seed),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(blacklist_address))
                    .data(
                        Tape {
                            used: StorageUnits::from_bytes(entry_size),
                            tracks: TrackArchive {
                                num_tracks: 1,
                                next_number: TrackNumber(1),
                                tree: expected_tree,
                            },
                            ..tape
                        }
                        .pack()
                        .as_ref(),
                    )
                    .build(),
            ],
        );
    }
}
