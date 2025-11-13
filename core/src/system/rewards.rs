use crate::types::*;
use crate::spooler::SpoolAssignment;
use super::Committee;

pub fn get_pool_score(
    allocated: StorageUnits,
    blacklisted: StorageUnits,
    weight: u16,
) -> u128 {
    if weight == 0 {
        return 0;
    }

    let weight = weight as u128;
    let stored = allocated
        .saturating_sub(blacklisted)
        .as_u128();
    let score = weight
        .saturating_mul(stored);

    score
}

pub fn get_committee_score<const N: usize, const S: usize>(
    allocated: StorageUnits,
    committee: &Committee<N>,
    spools: &SpoolAssignment<S>,
) -> u128 {
    let mut score: u128 = 0;

    for (i, member) in committee.iter().enumerate() {
        let blacklist = member.blacklist;
        let weight = spools.weight(i);

        if blacklist >= allocated {
            continue;
        }

        if weight == 0 {
            continue;
        }

        let member_score = get_pool_score(
            allocated,
            blacklist,
            weight,
        );

        score = score
            .saturating_add(member_score);
    }

    score
}

pub fn calc_rewards<const N: usize, const S: usize>(
    id: NodeId,
    allocated: StorageUnits,
    committee: &Committee<N>,
    spools: &SpoolAssignment<S>,
    reward_pool: Coin<TAPE>,
) -> Coin<TAPE> {
    if allocated.is_zero() {
        return TAPE::zero();
    }
    
    if let Some((member, index)) = committee.get_member(&id) {
        let weight = spools.weight(index);
        let blacklist = member.blacklist;

        // No rewards if weight is zero (pool is not assigned any spools)
        if weight == 0 {
            return TAPE::zero();
        }

        // No rewards if the pool has blacklisted all of allocated
        if blacklist >= allocated {
            return TAPE::zero();
        }

        // Calculate a weighted score for this pool
        let pool_score = get_pool_score(
            allocated,
            blacklist,
            weight
        );

        // Calculate a total weighted score for all committee members
        let total_score = get_committee_score(
            allocated,
            committee,
            spools,
        );

        // rewards = floor(reward_pool * pool_score / total_score)
        let rewards = reward_pool.as_u128()
            .saturating_mul(pool_score)
            .checked_div(total_score)
            .unwrap_or(0);

        return TAPE(rewards as u64);
    }

    TAPE::zero()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_basic() {
        // weight * max(allocated - blacklist, 0)
        let s = get_pool_score(StorageUnits(1000), StorageUnits(200), 3);
        assert_eq!(s, 3u128 * 800u128);

        // Fully blacklisted
        let s2 = get_pool_score(StorageUnits(1000), StorageUnits(1000), 5);
        assert_eq!(s2, 0);

        // Over-blacklisted (saturating_sub → 0)
        let s3 = get_pool_score(StorageUnits(1000), StorageUnits(1200), 7);
        assert_eq!(s3, 0);

        // Zero weight
        let s4 = get_pool_score(StorageUnits(1000), StorageUnits(100), 0);
        assert_eq!(s4, 0);
    }
}
