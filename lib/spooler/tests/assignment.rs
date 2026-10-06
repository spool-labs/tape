//! End-to-end checks of `migrate_sainte_lague` on wide and narrow groups.

use std::collections::BTreeSet;

use tape_core::encoding::{ErasureCode, RSParams, TAPENET_PROFILE};
use tape_core::spooler::{GroupFold, SpoolerError, VirtualGroup};
use tape_core::system::Member;
use tape_core::types::TAPE;
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_spooler::migrate_sainte_lague;

const NARROW: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));

/// Repeatable pseudo-random numbers, so a failing case can be run again.
struct Sequence(u64);

impl Sequence {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

fn node(id: u64) -> Address {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&id.to_le_bytes());
    Address::new(bytes)
}

fn members(stakes: &[(u64, u64)]) -> Vec<Member> {
    stakes
        .iter()
        .map(|(id, stake)| Member::new(node(*id), TAPE(*stake)))
        .collect()
}

fn equal_stakes(count: u64) -> Vec<(u64, u64)> {
    (1..=count).map(|id| (id, 1_000)).collect()
}

fn seed(value: u64) -> Hash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&value.to_le_bytes());
    Hash::from(bytes)
}

fn holders(assignment: &[Address]) -> Vec<Option<Address>> {
    assignment.iter().copied().map(Some).collect()
}

fn spools_of(assignment: &[Address], members: &[Member]) -> Vec<u64> {
    members
        .iter()
        .map(|member| assignment.iter().filter(|holder| **holder == member.node).count() as u64)
        .collect()
}

fn moved(before: &[Option<Address>], after: &[Address]) -> u64 {
    before
        .iter()
        .zip(after)
        .filter(|(before, after)| **before != Some(**after))
        .count() as u64
}

// Every group holds n different committee nodes.
fn assert_valid(assignment: &[Address], committee: &[Member], groups: usize, profile: ErasureCode) {
    let width = usize::from(profile.n());
    let nodes: BTreeSet<Address> = committee.iter().map(|member| member.node).collect();

    assert_eq!(assignment.len(), groups * width);
    for group in assignment.chunks_exact(width) {
        let distinct: BTreeSet<Address> = group.iter().copied().collect();

        assert_eq!(distinct.len(), width);
        assert!(distinct.is_subset(&nodes));
    }
}

// The holders each group of a doubled count starts with: a group that was
// live keeps its own, and a new one starts on the group that carried it.
fn after_doubling(current: &[Address], groups: usize, profile: ErasureCode) -> Vec<Option<Address>> {
    let width = usize::from(profile.n());
    let fold = GroupFold::new(groups as u64).expect("live groups");
    let mut owners = Vec::with_capacity(2 * groups * width);

    for target in 0..2 * groups {
        let carrier = fold.live_group(VirtualGroup(target as u64)).as_usize();
        for position in 0..width {
            owners.push(Some(current[carrier * width + position]));
        }
    }

    owners
}

// a committee of n or more staked nodes gets an assignment at every group
// count, through churn in the committee, the stakes and the holders
#[test]
fn staked_committee_assigned() {
    let mut random = Sequence(7);

    for profile in [TAPENET_PROFILE, NARROW] {
        let width = u64::from(profile.n());

        for groups in [1usize, 2, 3, 4, 5, 8, 16] {
            for extra in [0u64, 1, 2, 5, 17, 40] {
                let mut ids: Vec<u64> = (1..=width + extra).collect();
                let mut next_id = width + extra + 1;
                let mut current: Vec<Option<Address>> = vec![None; groups * width as usize];

                for epoch in 0..6u64 {
                    let stakes: Vec<(u64, u64)> = ids
                        .iter()
                        .map(|id| (*id, 1 + random.below(1_000_000) * (1 + random.below(50))))
                        .collect();
                    let committee = members(&stakes);

                    let assignment =
                        migrate_sainte_lague(groups, &current, &committee, &seed(epoch), profile)
                            .expect("an assignment");

                    assert_valid(&assignment, &committee, groups, profile);
                    for count in spools_of(&assignment, &committee) {
                        assert!(count <= groups as u64);
                    }

                    // The next epoch swaps a few nodes out and forgets a few holders.
                    current = holders(&assignment);
                    for _ in 0..random.below(4) {
                        let index = random.below(ids.len() as u64) as usize;
                        ids[index] = next_id;
                        next_id += 1;
                    }
                    for _ in 0..random.below(6) {
                        let index = random.below(current.len() as u64) as usize;
                        current[index] = None;
                    }
                }
            }
        }
    }
}

// a group needs n nodes, and a node with no stake takes no spool
#[test]
fn too_few_nodes() {
    let nineteen = members(&equal_stakes(19));
    let mut one_unstaked = equal_stakes(20);
    one_unstaked[3].1 = 0;

    assert_eq!(
        migrate_sainte_lague(1, &[None; 20], &nineteen, &seed(1), TAPENET_PROFILE),
        Err(SpoolerError::InsufficientNodes),
    );
    assert_eq!(
        migrate_sainte_lague(1, &[None; 20], &members(&one_unstaked), &seed(1), TAPENET_PROFILE),
        Err(SpoolerError::Infeasible),
    );

    let three = members(&equal_stakes(3));
    let four = members(&equal_stakes(4));

    assert_eq!(
        migrate_sainte_lague(2, &[None; 8], &three, &seed(1), NARROW),
        Err(SpoolerError::InsufficientNodes),
    );
    assert!(migrate_sainte_lague(2, &[None; 8], &four, &seed(1), NARROW).is_ok());
}

// Spools the new counts take away from nodes that hold more than their share.
fn spools_to_shed(owners: &[Option<Address>], after: &[Address], committee: &[Member]) -> u64 {
    committee
        .iter()
        .map(|member| {
            let held = owners.iter().filter(|holder| **holder == Some(member.node)).count() as u64;
            let wanted = after.iter().filter(|holder| **holder == member.node).count() as u64;
            held.saturating_sub(wanted)
        })
        .sum()
}

// Three tiers of stake, each node within a percent of its tier. Nodes of a
// tier sit close enough that a small drift changes which of them rounds up.
fn drifting_stakes(count: u64, random: &mut Sequence) -> Vec<(u64, u64)> {
    (1..=count)
        .map(|id| {
            let tier = 1_000_000 * (1 + id % 3);
            (id, tier + random.below(tier / 100))
        })
        .collect()
}

// a committee of exactly n holds every group, so doubling the count moves nothing
#[test]
fn doubling_full_committee() {
    for profile in [TAPENET_PROFILE, NARROW] {
        let committee = members(&equal_stakes(u64::from(profile.n())));

        for groups in [1usize, 2, 4, 8] {
            let empty = vec![None; groups * usize::from(profile.n())];
            let before = migrate_sainte_lague(groups, &empty, &committee, &seed(1), profile)
                .expect("an assignment");

            let owners = after_doubling(&before, groups, profile);
            let after = migrate_sainte_lague(2 * groups, &owners, &committee, &seed(2), profile)
                .expect("an assignment");

            assert_valid(&after, &committee, 2 * groups, profile);
            assert_eq!(moved(&owners, &after), 0);
        }
    }
}

// in a larger committee a doubling leaves every spool where it is except the
// ones a node's rounded count no longer covers
#[test]
fn doubling_large_committee() {
    let mut random = Sequence(11);

    for profile in [TAPENET_PROFILE, NARROW] {
        let width = usize::from(profile.n());

        for nodes in [width as u64 + 3, 2 * width as u64, 3 * width as u64 + 4] {
            let committee = members(&drifting_stakes(nodes, &mut random));

            for groups in [1usize, 2, 4, 8] {
                let empty = vec![None; groups * width];
                let before = migrate_sainte_lague(groups, &empty, &committee, &seed(1), profile)
                    .expect("an assignment");

                let owners = after_doubling(&before, groups, profile);
                let after = migrate_sainte_lague(2 * groups, &owners, &committee, &seed(2), profile)
                    .expect("an assignment");

                let moves = moved(&owners, &after);
                let shed = spools_to_shed(&owners, &after, &committee);

                assert_valid(&after, &committee, 2 * groups, profile);
                assert!(moves >= shed);
                assert!(moves <= 2 * shed, "{moves} moved for {shed} spools shed");
            }
        }
    }
}

// with the committee unchanged, spools move only in an epoch where stake
// drift shifted a spool from one node's count to another's
#[test]
fn stake_drift() {
    let mut random = Sequence(23);

    for profile in [TAPENET_PROFILE, NARROW] {
        let width = usize::from(profile.n());
        let groups = 5;
        // Enough nodes that the last spools go to only some nodes of a tier.
        let nodes = 2 * width as u64 + 5;
        let mut current: Vec<Option<Address>> = vec![None; groups * width];
        let mut total_moves = 0;
        let mut total_shed = 0;

        for epoch in 0..60u64 {
            let committee = members(&drifting_stakes(nodes, &mut random));
            let assignment = migrate_sainte_lague(groups, &current, &committee, &seed(epoch), profile)
                .expect("an assignment");

            if epoch > 0 {
                let moves = moved(&current, &assignment);
                let shed = spools_to_shed(&current, &assignment, &committee);

                assert!(moves >= shed);
                assert!(moves <= 2 * shed, "epoch {epoch}: {moves} moved for {shed} spools shed");
                total_moves += moves;
                total_shed += shed;
            }

            assert_valid(&assignment, &committee, groups, profile);
            current = holders(&assignment);
        }

        // The drift is large enough to move some spools and small enough to leave most alone.
        assert!(total_shed > 0);
        assert!(total_moves < 60 * (groups * width) as u64 / 10);
    }
}
