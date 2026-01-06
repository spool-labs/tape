use steel::*;
use tape_api::prelude::*;
use crate::error::*;

pub fn process_add_to_blacklist(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AddToBlacklist::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        node_info,
        track_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != *authority_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Load the track we are blacklisting
    let track = track_info
        .as_account::<Track>(&tapedrive::ID)?;

    // Add to blacklist as (track_hash, units) = (track.key, track.size)
    node.blacklist
        .add(track.key, track.size)
        .map_err(|_| TapeError::ListFull)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_add_to_blacklist() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        // PDAs
        let blob_hash = Hash::new_unique();
        let (node_address, _) = node_pda(authority);
        let (tape_address, _) = tape_pda(authority);
        let (track_address, _) = track_pda(authority, blob_hash);

        // Instruction
        let instruction = build_add_to_blacklist_ix(fee_payer, authority, node_address, track_address);

        // Prepare node with initialized blacklist
        let mut node = Node::zeroed();
        node.authority = authority;
        node.blacklist = Blacklist::new();

        // Prepare a track
        let track = Track {
            id: TrackNumber(69),
            tape: tape_address,
            key: blob_hash,
            size: StorageUnits(500),
            data: TrackData::new(
                EpochNumber(10),
                Hash::new_unique(),
            )
        };

        // Build accounts
        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(track_address, track.pack(), tapedrive::ID),
        ];

        // Expected node after blacklisting
        let mut expected_node = node.clone();
        expected_node
            .blacklist
            .add(track.key, track.size)
            .expect("blacklist add");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),

                // Verify node updated with blacklist containing the track
                Check::account(&node_address)
                    .data(expected_node.pack().as_ref())
                    .build(),

                // Track remains unchanged
                Check::account(&track_address)
                    .data(track.pack().as_ref())
                    .build(),
            ],
        );
    }
}
