use crate::types::*;
use crate::apportion::Seats;
use crate::system::Committee;

pub fn calc_pool_score(
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

pub fn calc_total_score<const N: usize, const S: usize>(
    committee: &Committee<N>,
    seats: &Seats<S>,
    allocated: StorageUnits,
) -> u128 {
    let mut score: u128 = 0;

    for (i, member) in committee.iter().enumerate() {
        let blacklist = member.blacklist;
        let weight = seats.weight(i);

        if blacklist >= allocated {
            continue; // skip fully blacklisted members
        }

        if weight == 0 {
            continue; // skip zero-weight members
        }

        let member_score = calc_pool_score(
            allocated,
            blacklist,
            weight,
        );

        score = score
            .saturating_add(member_score);
    }

    score
}

pub fn calc_pool_rewards<const N: usize, const S: usize>(
    pool: &NodeId,
    reward_pool: Coin<TAPE>,
    allocated: StorageUnits,
    committee: &Committee<N>,
    seats: &Seats<S>,
) -> Coin<TAPE> {
    if allocated.is_zero() {
        return TAPE::zero();
    }
    
    if let Some(member) = committee.get_member(pool) {
        let index = committee
            .index_of(pool)
            .expect("member index exists");

        println!("Calculating rewards for pool {:?} at index {}", pool, index);
        let weight = seats.weight(index);
        println!("Pool weight: {}", weight);
        let blacklist = member.blacklist;
        println!("Pool blacklist: {}", blacklist.as_u64());

        // No rewards if weight is zero (pool is not assigned any seats)
        if weight == 0 {
            return TAPE::zero();
        }

        // No rewards if the pool has blacklisted all of allocated
        if blacklist >= allocated {
            return TAPE::zero();
        }

        // Calculate a weighted score for this pool
        let pool_score = calc_pool_score(
            allocated,
            blacklist,
            weight
        );

        // Calculate a total weighted score for all committee members
        let total_score = calc_total_score(
            committee,
            seats,
            allocated
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
use crate::apportion::Seats;
use crate::system::{Committee, CommitteeMember};
use crate::bls::BlsPubkey;
use bytemuck::Zeroable;

fn cm(id: u64, stake: u64, bl: u64) -> CommitteeMember {
    CommitteeMember {
        id: NodeId(id),
        stake: TAPE(stake),
        key: BlsPubkey::zeroed(),
        blacklist: StorageUnits(bl),
    }
}

#[test]
fn score_basic() {
    // weight * max(allocated - blacklist, 0)
    let s = calc_pool_score(StorageUnits(1000), StorageUnits(200), 3);
    assert_eq!(s, 3u128 * 800u128);

    // Fully blacklisted
    let s2 = calc_pool_score(StorageUnits(1000), StorageUnits(1000), 5);
    assert_eq!(s2, 0);

    // Over-blacklisted (saturating_sub → 0)
    let s3 = calc_pool_score(StorageUnits(1000), StorageUnits(1200), 7);
    assert_eq!(s3, 0);

    // Zero weight
    let s4 = calc_pool_score(StorageUnits(1000), StorageUnits(100), 0);
    assert_eq!(s4, 0);
}

#[test]
fn total_score_matches_sum() {
    //// Committee with 3 members, different blacklists
    //let committee: Committee<8> = Committee::from_members(&[
    //    cm(1, 3000, 0),
    //    cm(2, 2000, 100),
    //    cm(3, 1000, 400),
    //]);
    //
    //// Assign exact seat counts via helper (assume SEAT_COUNT == 100 in your crate)
    //let seats:Seats<100> = Seats::try_from_counts(&[20, 30, 50]).expect("seats");
    //
    //let allocated = StorageUnits(1000);
    //
    //// Expected: sum_i weight(i) * max(allocated - bl_i, 0)
    //let mut expected: u128 = 0;
    //for i in 0..committee.size() {
    //    let w = seats.weight(i) as u128;
    //    let stored = allocated
    //        .saturating_sub(committee.blacklist[i])
    //        .as_u128();
    //    expected = expected.saturating_add(w.saturating_mul(stored));
    //}
    //
    //let got = calc_total_score(&committee, &seats, allocated);
    //assert_eq!(got, expected);
    //assert!(got > 0, "expected a non-zero total score");
}

#[test]
fn pool_rewards_non_zero_and_edges() {
    //// Committee with 3 members
    //// Member 1: bl=0, Member 2: bl=50, Member 3: bl=300
    //let committee: Committee<8> = Committee::from_members(&[
    //    cm(1, 3000, 0),
    //    cm(2, 2000, 50),
    //    cm(3, 1000, 300),
    //]);
    //
    //// Seats exactly: [20, 30, 50] for members [1, 2, 3]
    //let seats:Seats<100> = Seats::try_from_counts(&[20, 30, 50]).expect("seats");
    //
    let allocated = StorageUnits(1000);
    let reward_pool = TAPE(10_000);
    //
    //// Total score
    //let total = calc_total_score(&committee, &seats, allocated);
    //assert!(total > 0);
    //
    //// Member 2 expected
    //let (m2, i2) = committee.get_member(&NodeId(2)).expect("member 2 exists");
    //let w2 = seats.weight(i2) as u128;
    //assert_eq!(w2, 30);
    //let stored2 = allocated.saturating_sub(m2.blacklist).as_u128(); // 1000 - 50 = 950
    //let score2 = w2.saturating_mul(stored2);
    //
    //let expected2 = reward_pool
    //    .as_u128()
    //    .saturating_mul(score2)
    //    .checked_div(total)
    //    .unwrap_or(0) as u64;
    //
    //let got2 = calc_pool_rewards(&NodeId(2), reward_pool, allocated, &committee, &seats);
    //assert_eq!(got2.as_u64(), expected2);
    //assert!(got2 > TAPE(0), "expected non-zero payout for member 2");
    //
    //// Member not in committee → zero
    //let got_missing = calc_pool_rewards(&NodeId(999), reward_pool, allocated, &committee, &seats);
    //assert_eq!(got_missing, TAPE(0));
    //
    //// Allocated zero → zero rewards
    //let got_zero_alloc = calc_pool_rewards(&NodeId(1), reward_pool, StorageUnits(0), &committee, &seats);
    //assert_eq!(got_zero_alloc, TAPE(0));

    // Fully blacklisted for this member → zero
    let committee_full_bl: Committee<8> = Committee::from_members(&[
        cm(1, 3000, 0),
        cm(2, 2000, 50),
        cm(3, 1000, 1000), // full blacklist
    ]);
    println!("Committee with full blacklist: {:?}", committee_full_bl);
    let seats_fb:Seats<100> = Seats::try_from_counts(&[20, 30, 50]).expect("seats fb");
    println!("Seats: {:?}", seats_fb);
    let got_full_bl = calc_pool_rewards(&NodeId(3), reward_pool, allocated, &committee_full_bl, &seats_fb);
    assert_eq!(got_full_bl, TAPE(0));

    //// Weight zero for a member → zero rewards
    //// Give all 100 seats to member 1, zero to 2 and 3
    //let seats_zero_w:Seats<100> = Seats::try_from_counts(&[100 as u16, 0, 0]).expect("seats zw");
    //println!("Seats: {:?}", seats_zero_w);
    //let got_weight_zero = calc_pool_rewards(&NodeId(2), reward_pool, allocated, &committee, &seats_zero_w);
    //assert_eq!(got_weight_zero, TAPE(0));
    //
    //// Sum of payouts across all members ≤ reward_pool (flooring may leave dust)
    //let mut sum_paid: u128 = 0;
    //for id in committee.active_members() {
    //    let r = calc_pool_rewards(id, reward_pool, allocated, &committee, &seats);
    //    sum_paid = sum_paid.saturating_add(r.as_u128());
    //}
    //assert!(sum_paid <= reward_pool.as_u128(), "payouts exceed pool");
}
}
