use tape_api::program::prelude::*;


pub fn process_finalize_snapshot_epoch(
    accounts: &[AccountInfo<'_>],
    data: &[u8],
) -> ProgramResult {
    let args = FinalizeSnapshotEpoch::try_from_bytes(data)?;
    let [
        fee_payer_info,
        epoch_info,
        manifest_info,
    ] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info.is_signer()?.is_writable()?;

    let epoch = epoch_info.is_epoch()?.as_account::<Epoch>(&tapedrive::ID)?;

    // Snapshot finalize is the natural mate of init: same epoch arithmetic,
    // no SnapshotState singleton to advance. Idempotent — calling finalize
    // twice on the same fully-sealed manifest just re-emits the event.
    let snapshot_epoch = EpochNumber::unpack(args.epoch);
    let current_epoch = current_epoch(epoch);
    if current_epoch <= EpochNumber(1) {
        return Err(TapeError::SnapshotEpochClosed.into());
    }
    let expected_epoch = current_epoch - EpochNumber(1);
    if snapshot_epoch != expected_epoch {
        return Err(TapeError::SnapshotEpochClosed.into());
    }

    let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);
    let manifest = manifest_info
        .has_address(&manifest_address.into())?
        .is_snapshot_manifest()?
        .as_account::<SnapshotManifest>(&tapedrive::ID)?;

    if manifest.group_bitmap.count_ones() != SPOOL_GROUP_COUNT {
        return Err(TapeError::SnapshotIncomplete.into());
    }

    SnapshotFinalized {
        epoch: snapshot_epoch,
    }
    .log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;
    use tape_core::types::SnapshotGroupBitmap;

    #[test]
    fn snapshot_manifest_pack_size_matches_layout() {
        let manifest = SnapshotManifest {
            group_bitmap: SnapshotGroupBitmap::zeroed(),
            chunk_size: StorageUnits::zero(),
            groups: [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT],
        };

        assert_eq!(core::mem::size_of::<SnapshotGroupBitmap>(), 8);
        assert_eq!(manifest.pack().len(), SnapshotManifest::get_size());
    }

    fn full_group_bitmap() -> SnapshotGroupBitmap {
        let mut group_bitmap = SnapshotGroupBitmap::zeroed();
        for group_index in 0..SPOOL_GROUP_COUNT {
            group_bitmap.set(group_index);
        }
        group_bitmap
    }

    #[test]
    fn test_finalize_snapshot_epoch() {
        let fee_payer = Pubkey::new_unique();
        let snapshot_epoch = EpochNumber(42);

        let (epoch_address, _) = epoch_pda();
        let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);

        let epoch = Epoch {
            id: EpochNumber(43),
            ..Epoch::zeroed()
        };
        let manifest = SnapshotManifest {
            group_bitmap: full_group_bitmap(),
            chunk_size: StorageUnits::from_bytes(1_024),
            groups: [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT],
        };

        let instruction = build_finalize_snapshot_epoch_ix(fee_payer.into(), snapshot_epoch);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(manifest_address, manifest.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::success()],
        );
    }

    #[test]
    fn test_finalize_snapshot_epoch_rejects_incomplete_manifest() {
        let fee_payer = Pubkey::new_unique();
        let snapshot_epoch = EpochNumber(42);

        let (epoch_address, _) = epoch_pda();
        let (manifest_address, _) = snapshot_manifest_pda(snapshot_epoch);

        let epoch = Epoch {
            id: EpochNumber(43),
            ..Epoch::zeroed()
        };
        let manifest = SnapshotManifest {
            group_bitmap: SnapshotGroupBitmap::zeroed(),
            chunk_size: StorageUnits::zero(),
            groups: [SnapshotChunkRecord::zeroed(); SPOOL_GROUP_COUNT],
        };

        let instruction = build_finalize_snapshot_epoch_ix(fee_payer.into(), snapshot_epoch);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(manifest_address, manifest.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[Check::err(TapeError::SnapshotIncomplete.into())],
        );
    }
}
