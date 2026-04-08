use tape_solana::*;
use tape_api::program::prelude::*;
use tape_api::event::CommissionClaimed;

pub fn process_claim_commission(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = ClaimCommission::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        authority_ata_info,

        archive_info,
        archive_ata_info,

        node_info,

        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    authority_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *authority_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS.into())?;

    archive_info
        .is_archive()?;
    archive_ata_info
        .is_writable()?
        .is_archive_ata()?;

    token_program_info
        .is_program(&spl_token::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    // Claim full commission from pool (errors if none)
    let commission: TAPE = node.pool
        .claim_commission()
        .map_err(|_| TapeError::NoCommission)?;

    // Pay out from Archive ATA to authority ATA
    transfer_signed(
        archive_info,
        archive_ata_info,
        authority_ata_info,
        token_program_info,
        commission.into(),
        &[ARCHIVE],
    )?;

    CommissionClaimed {
        node: (*node_info.key).into(),
        authority: (*authority_info.key).into(),
        amount: commission.as_u64().to_le_bytes(),
    }.log();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_claim_commission() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let authority_ata = ata_address(&authority);
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();
        let (node_address, _) = node_pda(authority.into());

        // Build instruction
        let instruction = build_claim_commission_ix(fee_payer.into(), authority.into(), node_address);

        // Commission to be claimed
        let commission_amount: u64 = 1_234;

        // Minimal archive account
        let archive = Archive::zeroed();

        // Node with claimable commission
        let mut node = Node::zeroed();
        node.authority = authority.into();
        node.pool = StakingPool {
            commission: TAPE(commission_amount),
            ..StakingPool::zeroed()
        };

        // Accounts
        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            token(authority_ata, authority, 0),

            // archive and its funded ATA (enough to pay commission)
            pda(archive_address, archive.pack(), tapedrive::ID),
            token(archive_ata, archive_address, commission_amount),

            // node state with commission
            pda(node_address, node.pack(), tapedrive::ID),

            token_program(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),

                // Authority receives the full commission
                Check::account(&Pubkey::from(authority_ata)).data(
                    token(authority_ata, authority, commission_amount).1.data.as_ref()
                ).build(),

                // Archive ATA reduced to zero
                Check::account(&Pubkey::from(archive_ata)).data(
                    token(archive_ata, archive_address, 0).1.data.as_ref()
                ).build(),

                // Node commission should be zero after claim
                Check::account(&Pubkey::from(node_address)).data(
                    Node {
                        pool: StakingPool {
                            commission: TAPE(0),
                            ..node.pool
                        },
                        ..node
                    }.pack().as_ref()
                ).build(),
            ],
        );
    }
}
