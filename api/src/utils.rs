use steel::*;

use tape_core::types::EpochNumber;
use crate::state::Epoch;
use crate::consts::NAME_LENGTH;

/// Helper: check a condition is true and return an error if not
#[inline(always)]
pub fn check_condition<E>(condition: bool, err: E) -> ProgramResult
where
    E: Into<ProgramError>,
{
    if !condition {
        return Err(err.into());
    }
    Ok(())
}

/// Helper: convert a slice to a fixed-size array, truncating or padding with zeros as needed
#[inline(always)]
pub fn padded_array<const N: usize>(input: &[u8]) -> [u8; N] {
    let mut out = [0u8; N];
    let len = input.len().min(N);
    out[..len].copy_from_slice(&input[..len]);
    out
}

/// Helper: convert a name to a fixed-size array
#[inline(always)]
pub fn to_name<T>(val: T) -> [u8; NAME_LENGTH]
where
    T: AsRef<[u8]>,
{
    let bytes = val.as_ref();
    assert!(
        bytes.len() <= NAME_LENGTH,
        "name too long ({} > {})",
        bytes.len(),
        NAME_LENGTH
    );
    padded_array::<NAME_LENGTH>(bytes)
}

/// Helper: convert a name to a string
#[inline(always)]
pub fn from_name(val: &[u8; NAME_LENGTH]) -> String {
    let mut name_bytes = val.to_vec();
    name_bytes.retain(|&x| x != 0);
    String::from_utf8(name_bytes).unwrap()
}

/// Helper: get the current epoch from an Epoch account
#[inline(always)]
pub fn current_epoch(epoch: &Epoch) -> EpochNumber {
    epoch.id
}

/// Helper: get the next epoch from an Epoch account
#[inline(always)]
pub fn next_epoch(epoch: &Epoch) -> EpochNumber {
    epoch.id.checked_add(EpochNumber::one()).unwrap()
}
