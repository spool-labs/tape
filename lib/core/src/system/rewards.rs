use tape_crypto::address::Address;

use crate::types::*;
use super::Member;

pub fn get_pool_score(
    allocated: StorageUnits,
    blacklisted: StorageUnits,
    weight: u64,
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

pub fn get_committee_score(
    allocated: StorageUnits,
    members: &[Member],
) -> u128 {
    let mut score: u128 = 0;

    for member in members {
        if member.blacklist >= allocated || member.spools == 0 {
            continue;
        }

        let member_score = get_pool_score(
            allocated,
            member.blacklist,
            member.spools,
        );

        score = score.saturating_add(member_score);
    }

    score
}

pub fn calc_rewards(
    node: Address,
    allocated: StorageUnits,
    members: &[Member],
    reward_pool: Coin<TAPE>,
) -> Coin<TAPE> {
    if allocated.is_zero() {
        return TAPE::zero();
    }

    let Some(member) = members.iter().find(|m| m.node == node) else {
        return TAPE::zero();
    };

    if member.spools == 0 || member.blacklist >= allocated {
        return TAPE::zero();
    }

    let pool_score = get_pool_score(
        allocated,
        member.blacklist,
        member.spools,
    );

    let total_score = get_committee_score(
        allocated,
        members,
    );

    // rewards = floor(reward_pool * pool_score / total_score)
    let rewards = reward_pool.as_u128()
        .saturating_mul(pool_score)
        .checked_div(total_score)
        .unwrap_or(0);

    TAPE(rewards as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_basic() {
        // weight * max(allocated - blacklist, 0)
        let s = get_pool_score(StorageUnits::mb(1000), StorageUnits::mb(200), 3);
        assert_eq!(s, 3u128 * 800u128 * StorageUnits::MB as u128);

        // Fully blacklisted
        let s2 = get_pool_score(StorageUnits::mb(1000), StorageUnits::mb(1000), 5);
        assert_eq!(s2, 0);

        // Over-blacklisted (saturating_sub → 0)
        let s3 = get_pool_score(StorageUnits::mb(1000), StorageUnits::mb(1200), 7);
        assert_eq!(s3, 0);

        // Zero weight
        let s4 = get_pool_score(StorageUnits::mb(1000), StorageUnits::mb(100), 0);
        assert_eq!(s4, 0);
    }
}
