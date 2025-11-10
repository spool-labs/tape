use tape_api::prelude::*;
use steel::*;

pub fn process_set_commission_rate(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetCommissionRate::try_from_bytes(data)?;
    let [
        signer_info,
        node_info,
        epoch_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != *signer_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let commission_rate = BasisPoints::unpack(args.commission_rate);

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
    use tape_test::*;


    #[test]
    fn test_set_commission_rate() {
        let signer = Pubkey::new_unique();
        let old_commission = BasisPoints(500);
        let new_commission = BasisPoints(200);

        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(signer);

        let instruction = build_set_commission_ix(signer, node_address, new_commission);

        // Setup existing accounts

        let epoch = Epoch {
            id: EpochNumber(42),
            state: EpochState::new(),
            last_epoch_ms: 0,
        };

        let mut node = Node {
            id: NodeId(9000),
            authority: signer,
            pool: StakingPool::new(old_commission),
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(signer, 1_000_000_000),
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
                Check::account(&node_address)
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
