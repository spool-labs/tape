//! Utility helpers for instruction parsing and validation.

use core::mem::size_of;
use bytemuck::{Pod, Zeroable};
use tape_solana::ProgramError;

/// Read a POD instruction payload from bytes after validating payload size.
///
/// Returns a decoding error if the byte slice is not exactly the expected size.
pub fn read_instruction_pod<T>(data: &[u8]) -> Result<T, ProgramError>
where
    T: Pod + Zeroable,
{
    if data.len() != size_of::<T>() {
        return Err(ProgramError::InvalidInstructionData);
    }

    let mut value = T::zeroed();
    bytemuck::bytes_of_mut(&mut value).copy_from_slice(data);
    Ok(value)
}
