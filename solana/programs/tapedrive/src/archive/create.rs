use tape_solana::*;
use tape_api::program::prelude::*;

pub fn process_create_archive(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = CreateArchive::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,

        system_info,
        archive_info,
        archive_ata_info,
        subsidy_info,
        subsidy_ata_info,
        peer_set_info,

        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;

    authority_info
        .is_signer()?;

    system_program_info
        .is_program(&system_program::ID)?;
    token_program_info
        .is_program(&spl_token::ID)?;
    associated_token_program_info
        .is_program(&spl_associated_token_account::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    system_info
        .is_writable()?
        .is_system()?;

    let (archive_address, _) = archive_pda();
    let (archive_ata_address, _) = archive_ata();
    let (subsidy_ata_address, _) = subsidy_ata();

    archive_info
        .is_empty()?
        .is_writable()?
        .has_address(&archive_address.into())?;

    archive_ata_info
        .is_empty()?
        .is_writable()?
        .has_address(&archive_ata_address.into())?;

    subsidy_info
        .is_subsidy()?;

    subsidy_ata_info
        .is_empty()?
        .is_writable()?
        .has_address(&subsidy_ata_address.into())?;

    peer_set_info
        .is_peer_set()?;

    mint_info
        .is_mint()?;

    create_program_account::<Archive>(
        archive_info,
        system_program_info,
        fee_payer_info,
        &tapedrive::ID,
        &[ARCHIVE],
    )?;

    create_associated_token_account(
        fee_payer_info,
        archive_info,
        archive_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    create_associated_token_account(
        fee_payer_info,
        subsidy_info,
        subsidy_ata_info,
        mint_info,
        system_program_info,
        token_program_info,
        associated_token_program_info,
    )?;

    let system = system_info.as_account_mut::<System>(&tapedrive::ID)?;
    system.total_nodes = 0;
    system.current_epoch = EpochNumber(0);
    system.min_version = VersionId(0);
    // committee_size + spool_groups are seeded by start_network

    let archive = archive_info.as_account_mut::<Archive>(&tapedrive::ID)?;
    archive.storage_capacity = DEFAULT_STORAGE_CAPACITY;
    archive.storage_price = DEFAULT_STORAGE_PRICE;
    archive.burn_fee_bps = DEFAULT_BURN_FEE_BPS;
    archive.subsidy_decay_bps = DEFAULT_SUBSIDY_DECAY_BPS;
    archive.schedule = EpochSchedule::new_at(EpochNumber(0));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_api::state::PeerSet;
    use tape_test::*;

    #[test]
    fn create_archive() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();
        let (subsidy_address, _) = subsidy_pda();
        let (subsidy_ata, _) = subsidy_ata();
        let (peer_set_address, _) = peer_set_pda();

        let system = System::zeroed();
        let peer_set = PeerSet::zeroed();

        let instruction = build_create_archive_ix(
            fee_payer.into(),
            authority.into(),
        );

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),

            pda(system_address, system.pack(), tapedrive::ID),

            empty(archive_address),
            empty(archive_ata),
            empty(subsidy_address),
            empty(subsidy_ata),
            pda(peer_set_address, peer_set.pack(), tapedrive::ID),

            mint(MAX_SUPPLY),
            system_program(),
            token_program(),
            ata_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),

                Check::account(&Pubkey::from(system_address)).data(
                    System {
                        total_nodes: 0,
                        current_epoch: EpochNumber(0),
                        min_version: VersionId(0),
                        ..system
                    }.pack().as_ref()
                ).build(),

                Check::account(&Pubkey::from(archive_address)).data(
                    Archive {
                        storage_capacity: DEFAULT_STORAGE_CAPACITY,
                        storage_price: DEFAULT_STORAGE_PRICE,
                        burn_fee_bps: DEFAULT_BURN_FEE_BPS,
                        subsidy_decay_bps: DEFAULT_SUBSIDY_DECAY_BPS,
                        schedule: EpochSchedule::new_at(EpochNumber(0)),
                        ..Archive::zeroed()
                    }.pack().as_ref()
                ).build(),

                Check::account(&Pubkey::from(archive_ata)).data(
                    token(archive_ata, archive_address, 0).1.data.as_ref()
                ).build(),
                Check::account(&Pubkey::from(subsidy_ata)).data(
                    token(subsidy_ata, subsidy_address, 0).1.data.as_ref()
                ).build(),
            ],
        );
    }
}
