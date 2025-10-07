use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum BlobState {
    Unknown = 0,
    Registered,
    Certified,
    Invalidated,
}

