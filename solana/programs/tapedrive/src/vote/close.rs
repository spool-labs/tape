use tape_api::program::prelude::*;

pub fn process_close_vote(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = CloseVote::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        node_info,
        epoch_info,
        vote_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?
        .is_writable()?;

    let node = node_info
        .as_account::<Node>(&tapedrive::ID)?;

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let (vote_epoch, kind, registered_by) = {
        let vote = vote_info
            .is_writable()?
            .as_account::<Vote>(&tapedrive::ID)?;
        (vote.epoch, vote.kind, vote.registered_by)
    };

    if registered_by != node.id {
        return Err(ProgramError::InvalidAccountData);
    }

    if current_epoch(epoch) < vote_epoch.saturating_add(EpochNumber(2)) {
        return Err(TapeError::VoteStillActive.into());
    }

    close_account(vote_info, authority_info)?;

    VoteClosed {
        epoch: vote_epoch,
        kind,
        vote: (*vote_info.key).into(),
        registered_by,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_crypto::Hash;
    use tape_test::*;

    #[test]
    fn test_close_vote() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let vote_epoch = EpochNumber(9);
        let current_epoch = EpochNumber(11);
        let node_id = NodeId(7);
        let group = SpoolGroup(0);
        let chunk = ChunkNumber(0);

        let (node_address, _) = node_pda(authority.into());
        let (epoch_address, _) = epoch_pda();
        let (vote_address, _) = snapshot_vote_pda(vote_epoch, group, chunk);

        let node = Node {
            id: node_id,
            authority: authority.into(),
            ..Node::zeroed()
        };
        let epoch = Epoch {
            id: current_epoch,
            ..Epoch::zeroed()
        };
        let vote = Vote {
            epoch: vote_epoch,
            kind: VoteKind::Snapshot as u64,
            message_hash: Hash::from([0x11; 32]),
            registered_by: node_id,
        };

        let instruction = build_close_vote_ix(
            fee_payer.into(),
            authority.into(),
            node_address,
            vote_address,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(vote_address, vote.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&authority)
                    .lamports(rent(Vote::get_size()))
                    .build(),
                Check::account(&Pubkey::from(vote_address))
                    .lamports(0)
                    .closed()
                    .build(),
            ],
        );
    }

    #[test]
    fn test_close_vote_requires_vote_two_epochs_old() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let vote_epoch = EpochNumber(9);
        let current_epoch = EpochNumber(10);
        let node_id = NodeId(7);
        let group = SpoolGroup(0);
        let chunk = ChunkNumber(0);

        let (node_address, _) = node_pda(authority.into());
        let (epoch_address, _) = epoch_pda();
        let (vote_address, _) = snapshot_vote_pda(vote_epoch, group, chunk);

        let node = Node {
            id: node_id,
            authority: authority.into(),
            ..Node::zeroed()
        };
        let epoch = Epoch {
            id: current_epoch,
            ..Epoch::zeroed()
        };
        let vote = Vote {
            epoch: vote_epoch,
            kind: VoteKind::Snapshot as u64,
            message_hash: Hash::from([0x11; 32]),
            registered_by: node_id,
        };

        let instruction = build_close_vote_ix(
            fee_payer.into(),
            authority.into(),
            node_address,
            vote_address,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(vote_address, vote.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::VoteStillActive.into())],
        );
    }
}
