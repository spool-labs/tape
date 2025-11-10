use tape_api::prelude::*;
use steel::*;

pub fn process_set_commission_rate(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetCommissionRate::try_from_bytes(data)?;
    let [
        signer_info,
        node_info,
        system_info,
        epoch_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != *signer_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let commission_rate = BasisPoints::unpack(args.commission_rate);

    // If the node is currently in the committee or the next committee,
    // schedule the commission rate change to take effect in two epochs.

    if system.committee.contains(&node.id) || 
       system.committee_next.contains(&node.id) {

        let activation_epoch = current_epoch(epoch) + EpochNumber(2);
        node.pool.schedule
            .set_commission(activation_epoch, commission_rate)
            .map_err(|_| ProgramError::Custom(0))?;

    // Otherwise, apply the commission rate change immediately.
    } else {
        node.pool.commission_rate = commission_rate;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    fn member(id: u64, stake: u64) -> CommitteeMember {
        CommitteeMember {
            id: NodeId(id),
            stake: TAPE(stake),
            key: BlsPubkey::zeroed(),
            blacklist: StorageUnits(0),
        }
    }


    #[test]
    fn test_set_commission_rate() {
        let signer = Pubkey::new_unique();
        let old_commission = BasisPoints(500);
        let new_commission = BasisPoints(200);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(signer);

        let instruction = build_set_commission_ix(signer, node_address, new_commission);

        // Setup existing accounts

        let system = System {
            committee_next: Committee::from_members(&[
                member(9000, 100),
            ]),
            ..System::zeroed()
        };

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
            pda(system_address, system.pack(), tapedrive::ID),
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


        // Test immediate commission rate change

        let system = System::zeroed(); // Node is not in committee
        let node = Node {
            authority: signer,
            pool: StakingPool::new(old_commission),
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&node_address)
                    .data(Node {
                        pool: StakingPool {
                            commission_rate: new_commission,
                            ..node.pool
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
