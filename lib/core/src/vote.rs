use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum VoteKind {
    Unknown = 0,
    Update,
    Snapshot,
    NodeRemoval,
}
