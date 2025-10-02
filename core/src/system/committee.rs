use crate::coin::*;
use crate::types::*;
use crate::bls::Bn128PublicKey;
use bytemuck::{Pod, Zeroable};

/// Relative NodeId within a committee
pub type RelativeNodeId = u8;

/// A CommitteeMember represents a staking pool that can be part of a committee. Each member has a
/// unique NodeId and a BLS public key used for verifying aggregate signatures from the many
/// committee members.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct CommitteeMember {
    pub id: NodeId,
    pub key: Bn128PublicKey,
}

/// A CandidateSet defines a set of committee members that will be considered for appointment
/// during an upcoming epoch. Each member has an associated stake, which influences their
/// likelihood of being assigned seats in the committee.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CandidateSet<const N: usize> {
    pub member_count: u64,
    pub members: [CommitteeMember; N],
    pub stakes: [Coin<TAPE>; N], // (member_index -> stake)
}

unsafe impl<const N: usize> Zeroable for CandidateSet<N> {}
unsafe impl<const N: usize> Pod for CandidateSet<N> {}

impl<const N: usize> CandidateSet<N> {
}

/// An AppointedSet defines a set of committee members and their assigned seats. The number of
/// seats assigned depends on the originating CandidateSet stakes. More stake usually means more
/// seats assigned to that member. The number of seats is finite and is distributed using the
/// Jefferson method (a.k.a. d'Hondt method). A single committee member is likely to be assigned
/// multiple seats and the seat count influences the weight of that node's signature in the
/// committee.
///
/// Each seat is uniquely identified by its index in the seats array and is not interchangeable.
/// Seat movement between epochs is minimized to reduce disruption (a pool dropping out and coming
/// back the next epoch will likely get the same set of seat indicies if all stake remains equal).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AppointedSet<const N: usize, const M: usize> {
    pub member_count: u64,
    pub members: [CommitteeMember; N],
    pub seats: [RelativeNodeId; M], // (seat_index -> member_index)
}

unsafe impl<const N: usize, const M: usize> Zeroable for AppointedSet<N, M> {}
unsafe impl<const N: usize, const M: usize> Pod for AppointedSet<N, M> {}

impl<const N: usize, const M: usize> AppointedSet<N, M> {
}
