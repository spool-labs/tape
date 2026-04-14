use tape_solana::*;
use tape_api::program::prelude::*;
use tape_core::erasure::{SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
use tape_core::snapshot::types::SnapshotState;
use tape_crypto::bls12254::min_sig::*;

pub fn process_sign_snapshot(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SignSnapshot::try_from_bytes(data)?;
    let [
        fee_payer_info,
        system_info,
        epoch_info,
        snapshot_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    let system = system_info
        .is_system()?
        .as_account::<System>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let snapshot_epoch = prev_epoch(epoch);
    let snapshot_address = snapshot_pda(snapshot_epoch).0;

    let snapshot = snapshot_info
        .is_writable()?
        .has_address(&snapshot_address.into())?
        .as_account_mut::<Snapshot>(&tapedrive::ID)?;

    let spool_group = SpoolGroup::unpack(args.group);

    if snapshot.group_bitmap.is_set(spool_group.0 as usize) {
        return Err(TapeError::AlreadySigned.into());
    }

    // verify signature

    let committee = &system.committee;
    let weight = args.bitmap.count_ones() as u64;

    if !is_supermajority(weight, SPOOL_GROUP_SIZE as u64) {
        return Err(TapeError::NoQuorum.into());
    }

    let indices = args.bitmap.indices(SPOOL_GROUP_SIZE);
    if indices.is_empty() {
        return Err(TapeError::NoSigners.into());
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    let group_offset = spool_group.0 * SPOOL_GROUP_SIZE as u64;
    for member_index in &indices {
        // convert from group-local indices to committee-wide indices
        let member_index = member_index + group_offset as usize;
        if let Some(member) = committee.member_at(member_index) {
            pubkeys.push(member.key.0);
        } else {
            return Err(TapeError::BadMember.into());
        }
    }

    let decompressed_sig = G1Point::try_from(&args.signature.0)
        .map_err(|_| TapeError::BadSignature)?;

    let message = SnapshotSignMessage::new(
        snapshot_epoch,
        spool_group,
    );
    let message_bytes = message.to_bytes();

    verify_aggregate(
        &message_bytes,
        &pubkeys,
        &decompressed_sig,
    ).map_err(|_| TapeError::BadSignature)?;

    snapshot.group_bitmap.set(spool_group.0 as usize);

    if snapshot.group_bitmap.count_ones() == SPOOL_GROUP_COUNT {
        snapshot.state = SnapshotState::Finalized as u64;
    }

    SnapshotSigned {
        epoch: snapshot_epoch,
        group: spool_group,
        state: snapshot.state,
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::types::SpoolGroupBitmap;
    use tape_test::*;

    const SIGNERS: usize = 14;

    fn make_committee() -> (Vec<BlsPrivateKey>, System) {
        let keypairs: Vec<(BlsPrivateKey, BlsPubkey)> = (0..SPOOL_GROUP_SIZE)
            .map(|_| {
                let sk = BlsPrivateKey::from_random();
                let pk = sk.public_key().unwrap();
                (sk, pk)
            })
            .collect();

        let members: Vec<CommitteeMember> = keypairs
            .iter()
            .enumerate()
            .map(|(i, (_, pk))| CommitteeMember {
                id: NodeId::from(i as u64),
                stake: TAPE(1),
                key: *pk,
                ..CommitteeMember::zeroed()
            })
            .collect();

        let mut system = System::zeroed();
        system.committee = Committee::from_members(&members);

        (keypairs.into_iter().map(|(sk, _)| sk).collect(), system)
    }

    #[test]
    fn test_sign_snapshot() {
        let fee_payer = Pubkey::new_unique();
        let current_epoch = EpochNumber(10);
        let snapshot_epoch = EpochNumber(9);
        let spool_group = SpoolGroup(0);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (snapshot_address, _) = snapshot_pda(snapshot_epoch);

        let (private_keys, system) = make_committee();

        let epoch = Epoch {
            id: current_epoch,
            ..Epoch::zeroed()
        };

        let snapshot = Snapshot {
            epoch: snapshot_epoch,
            state: SnapshotState::Registered as u64,
            group_bitmap: GroupBitmap::zeroed(),
        };

        let signed_indices: Vec<usize> = (0..SIGNERS).collect();
        let bitmap = SpoolGroupBitmap::from_indices(&signed_indices, SPOOL_GROUP_SIZE);
        let message = SnapshotSignMessage::new(snapshot_epoch, spool_group).to_bytes();

        let partials: Vec<BlsSignature> = signed_indices
            .iter()
            .map(|&i| private_keys[i].sign(&message).unwrap())
            .collect();
        let agg_sig = BlsSignature::aggregate(&partials).unwrap();

        let instruction = build_sign_snapshot_ix(
            fee_payer.into(),
            snapshot_epoch,
            spool_group,
            bitmap,
            agg_sig,
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(snapshot_address, snapshot.pack(), tapedrive::ID),
        ];

        let mut expected_bitmap = GroupBitmap::zeroed();
        expected_bitmap.set(spool_group.0 as usize);

        let expected_snapshot = Snapshot {
            group_bitmap: expected_bitmap,
            ..snapshot
        };

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(snapshot_address))
                    .data(expected_snapshot.pack().as_ref())
                    .build(),
            ],
        );
    }
}
