use tape_api::prelude::*;
use steel::*;

pub fn process_advance_epoch(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let _args = AdvanceEpoch::try_from_bytes(data)?;
    let [
        signer_info,
        epoch_info,
        committee_info,
        previous_committee_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    let mut epoch = epoch_info
        .is_writable()?
        .is_epoch()?
        .as_account_mut::<Epoch>(&tape_api::ID)?;

    let mut committee = committee_info
        .is_writable()?
        .is_current_committee()?
        .as_account_mut::<Committee>(&tape_api::ID)?;

    let mut previous_committee = previous_committee_info
        .is_writable()?
        .is_previous_committee()?
        .as_account::<Committee>(&tape_api::ID)?;

    // Advance to the next epoch
    epoch.id = next_epoch(epoch);

    // Epoch phases: Syncing -> Active -> NextEpoch (this instruction)
    // - Syncing: nodes move recovery symbols based on seat assignments for the new committee
    // - Active: old committee stops serving reads for the previous epoch, new committee starts
    // serving reads for the current epoch. Rewards are distributed to the old committee. Voting
    // may start for features to be activated in E+2.
    // - NextEpoch: called once the epoch duration has elapsed (epoch duration starts at the Active
    // transition, not Syncing).
    
    // LeaderSet -> Next Committee
    // - Update seat assignments

    // Update future accounting
    // - pop a value off the ring buffer (storage and rewards)

    // Update archive
    // - Set total_capacity_size = max(next_capacity_size, used_capacity_size)
    // - Set storage_price_per_unit from features

    // Distribute rewards (during "Syncing" -> "Active" transition)
    // - Let total_rewards = old_epoch_balance
    // - For each node in old_epoch_leaders:
    //    - weight = seats(from previous committee)
    //    - stored = old_epoch_used_capacity - node.blacklist_size
    //    - node.score = weight * stored
    // - Split total epoch rewards proportionally to node scores
    // - Leftover rounding remainder is carried into the next epoch

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_advance_epoch() {
        let signer = Pubkey::new_unique();

        let instruction = build_advance_epoch_ix(signer);

        let (epoch_address, _) = epoch_pda();

        // Setup existing accounts

        let epoch = Epoch {
            id: EpochNumber(42),
            state: EpochState::zeroed(),
            last_epoch_ms: 0,
            leaders: LeaderSet::zeroed(),
        };

    }
}
