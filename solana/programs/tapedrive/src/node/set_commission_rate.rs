use tape_api::prelude::*;

pub fn process_set_commission_rate(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetCommissionRate::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        node_info,
        epoch_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let commission_rate = BasisPoints::unpack(args.commission_rate);

    // Commission rate must be <= 10000 bps (100%)
    const MAX_COMMISSION_RATE: u64 = 10_000;
    if commission_rate.as_u64() > MAX_COMMISSION_RATE {
        return Err(ProgramError::InvalidArgument);
    }

    // Even if the node is not in the current or next committee, we force
    // the change to take effect in 2 epochs to avoid commission rate
    // abuse by nodes joining and leaving committees.

    let activation_epoch = current_epoch(epoch) + EpochNumber(2);
    node.pool.schedule
        .set_commission(activation_epoch, commission_rate)
        .map_err(|_| ProgramError::Custom(0))?;


    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_crypto::Hash;
    use tape_test::*;

    #[test]
    fn test_set_commission_rate_cap() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority.into());

        // Try to set commission above 10000 bps
        let invalid_commission = BasisPoints(15000);
        let instruction = build_set_commission_ix(fee_payer.into(), authority.into(), node_address, invalid_commission);

        let epoch = Epoch {
            id: EpochNumber(42),
            state: EpochState::new(),
            last_epoch: 0,
            nonce: Hash::default(),
        };

        let node = Node {
            id: NodeId(9000),
            authority: authority.into(),
            pool: StakingPool::new(BasisPoints(500)),
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::err(ProgramError::InvalidArgument),
            ],
        );
    }

    #[test]
    fn test_set_commission_rate() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let old_commission = BasisPoints(500);
        let new_commission = BasisPoints(200);

        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(authority.into());

        let instruction = build_set_commission_ix(fee_payer.into(), authority.into(), node_address, new_commission);

        // Setup existing accounts

        let epoch = Epoch {
            id: EpochNumber(42),
            state: EpochState::new(),
            last_epoch: 0,
            nonce: Hash::default(),
        };

        let mut node = Node {
            id: NodeId(9000),
            authority: authority.into(),
            pool: StakingPool::new(old_commission),
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        // Expected state changes
        node.pool.schedule.set_commission(
            EpochNumber(44),
            new_commission,
        ).expect("schedule");

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(node_address))
                    .data(Node {
                        pool: StakingPool {
                            commission_rate: old_commission,
                            ..node.pool
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
