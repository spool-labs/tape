use tape_crypto::address::Address;

use super::Member;
use crate::types::*;

pub fn get_pool_score(assigned: StorageUnits, blacklisted: StorageUnits) -> u128 {
    if blacklisted >= assigned {
        return 0;
    }

    assigned.saturating_sub(blacklisted).as_u128()
}

pub fn get_committee_score(members: &[Member]) -> u128 {
    let mut score: u128 = 0;

    for member in members {
        let member_score = get_pool_score(member.assigned, member.blacklisted);

        score = score.saturating_add(member_score);
    }

    score
}

pub fn calc_rewards(
    node: Address,
    members: &[Member],
    reward_pool: Coin<TAPE>,
) -> Coin<TAPE> {
    let Some(member) = members.iter().find(|m| m.node == node) else {
        return TAPE::zero();
    };

    let pool_score = get_pool_score(member.assigned, member.blacklisted);
    if pool_score == 0 {
        return TAPE::zero();
    }

    let total_score = get_committee_score(members);

    // rewards = floor(reward_pool * pool_score / total_score)
    let rewards = reward_pool
        .as_u128()
        .checked_mul(pool_score)
        .unwrap_or(u128::MAX)
        .checked_div(total_score)
        .unwrap_or(0);

    TAPE(rewards as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_basic() {
        let s = get_pool_score(StorageUnits::mb(1000), StorageUnits::mb(200));
        assert_eq!(s, 800u128 * StorageUnits::MB as u128);

        // Fully blacklisted
        let s2 = get_pool_score(StorageUnits::mb(1000), StorageUnits::mb(1000));
        assert_eq!(s2, 0);

        // Over-blacklisted
        let s3 = get_pool_score(StorageUnits::mb(1000), StorageUnits::mb(1200));
        assert_eq!(s3, 0);
    }
}
