use steel::*;
use tape_api::prelude::*;
use crate::error::*;

pub fn process_claim_commission(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = ClaimCommission::try_from_bytes(data)?;
    let [
        signer_info,
        signer_ata_info,

        archive_info,
        archive_ata_info,

        node_info,

        token_program_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    signer_ata_info
        .is_writable()?
        .as_token_account()?
        .assert(|t| t.owner() == *signer_info.key)?
        .assert(|t| t.mint() == MINT_ADDRESS)?;

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

    if node.authority != *signer_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    // Claim full commission from pool (errors if none)
    let commission: TAPE = node.pool
        .claim_commission()
        .map_err(|_| TapeError::NoCommission)?;

    // Pay out from Archive ATA to signer ATA
    transfer_signed(
        archive_info,
        archive_ata_info,
        signer_ata_info,
        token_program_info,
        commission.into(),
        &[ARCHIVE],
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_claim_commission() {
        let signer = Pubkey::new_unique();

        let signer_ata = ata_address(&signer);
        let (archive_address, _) = archive_pda();
        let (archive_ata, _) = archive_ata();
        let (node_address, _) = node_pda(signer);

        // Build instruction
        let instruction = build_claim_commission_ix(signer, node_address);

        // Commission to be claimed
        let commission_amount: u64 = 1_234;

        // Minimal archive account
        let archive = Archive::zeroed();

        // Node with claimable commission
        let mut node = Node::zeroed();
        node.authority = signer;
        node.pool = StakingPool {
            commission: TAPE(commission_amount),
            ..StakingPool::zeroed()
        };

        // Accounts
        let accounts = vec![
            sol(signer, 1_000_000_000),
            token(signer_ata, signer, 0),

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

                // Signer receives the full commission
                Check::account(&signer_ata).data(
                    token(signer_ata, signer, commission_amount).1.data.as_ref()
                ).build(),

                // Archive ATA reduced to zero
                Check::account(&archive_ata).data(
                    token(archive_ata, archive_address, 0).1.data.as_ref()
                ).build(),

                // Node commission should be zero after claim
                Check::account(&node_address).data(
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
