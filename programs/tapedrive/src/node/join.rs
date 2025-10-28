use crate::error::*;
use tape_api::prelude::*;
use steel::*;

pub fn process_join_network(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = JoinNetwork::try_from_bytes(data)?;
    let [
        signer_info,
        system_info,
        epoch_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info.is_signer()?;

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    let node = node_info
        .as_account::<Node>(&tapedrive::ID)?
        .assert(|n| n.authority.eq(signer_info.key))?;

    // Find the stake balance at activation epoch (1 epoch from now)
    let activation_epoch = next_epoch(epoch);
    let balance = node.pool.stake_at(activation_epoch);

    system.committee_next
        .try_join(&node.id, balance)
        .map_err(|_| TapeError::UnexpectedState)?;

    system.committee_next
        .set_bls_key(&node.id, node.metadata.bls_pubkey)
        .map_err(|_| TapeError::UnexpectedState)?;

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
        }
    }

    #[test]
    fn test_join_network() {
        let signer = Pubkey::new_unique();
        let instruction = build_join_network_ix(signer);

        let (system_address, _) = system_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(signer);

        // Setup existing accounts

        let mut system = System::zeroed();
        let mut epoch = Epoch::zeroed();
        let mut node = Node::zeroed();

        system.committee_prev = Committee::from_members(&[ member(2, 2_000), member(1, 1_000), ]);
        system.committee      = Committee::from_members(&[ member(3, 3_000), member(2, 2_000), member(1, 1_000), ]);
        system.committee_next = Committee::from_members(&[ member(3, 3_500), member(4, 2_100), member(2, 2_000), member(1, 1_000), ]);

        epoch.id = EpochNumber(42);

        node.id = NodeId(5);
        node.authority = signer;

        let e0: EpochNumber = epoch.id;
        let e1: EpochNumber = e0 + EpochNumber(1);
        let e2: EpochNumber = e1 + EpochNumber(1);

        node.pool.stake = TAPE(5000);
        node.pool.schedule.incoming_tokens = EpochValues::try_from(
            &[e1, e2],
            &[1000, 200],
        ).expect("schedule incoming");

        node.pool.schedule.outgoing_tokens = EpochValues::try_from(
            &[e1, e2],
            &[100, 50],
        ).expect("schedule outgoing");

        assert_eq!(node.pool.stake_at(e0), TAPE(5000));
        assert_eq!(node.pool.stake_at(e1), TAPE(5900));
        assert_eq!(node.pool.stake_at(e2), TAPE(6050));

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(system_address, system.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            pda(node_address, node.pack(), tapedrive::ID),
            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                //Check::account(&epoch_address).data(
                //    Epoch {
                //        leaders: {
                //            // Same as before, but with our node evicting the lowest stake node
                //            let mut leaders = epoch.leaders;
                //
                //            // Nudge all values over by one
                //            let last_index = (COMMITTEE_SIZE - 1) as usize;
                //            for i in (1..COMMITTEE_SIZE).rev() {
                //                leaders.stakes[i] = leaders.stakes[i - 1];
                //                leaders.members[i] = leaders.members[i - 1];
                //            }
                //
                //            leaders.members[0] = node.id;
                //            leaders.stakes[0] = TAPE(5900);
                //            leaders.keys[0] = node.metadata.bls_pubkey;
                //
                //            leaders
                //        },
                //        ..epoch
                //    }.pack().as_ref()
                //).build(),
            ]
        );
    }

}
