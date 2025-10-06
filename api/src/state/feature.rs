use steel::*;
use super::AccountType;
use crate::state;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Feature {
    ///// The name of this feature.
    //pub name: [u8; NAME_LENGTH],
    //
    ///// The epoch for which this feature can be voted on.
    //pub voting_epoch: EpochNumber,
    //
    ///// The epoch this feature will be activated if approved.
    //pub activation_epoch: EpochNumber,
    //
    ///// The total votes for this feature.
    //pub votes: Vote<COMMITTEE_SIZE>,
    //
    ///// The kind of vote being conducted.
    //pub kind: VoteKind,
    //
    ///// The result of the vote, if concluded.
    //pub result: VoteResult,
}

state!(AccountType, Feature);

