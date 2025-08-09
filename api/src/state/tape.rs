use steel::*;
use crate::consts::*;
use crate::state;
use super::AccountType;

#[repr(C)] 
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Tape {
    pub number: u64,
    pub state: u64,

    pub authority: Pubkey,

    pub name:        [u8; NAME_LEN],
    pub merkle_seed: [u8; 32],
    pub merkle_root: [u8; 32],
    pub header:      [u8; HEADER_SIZE],

    pub first_slot:      u64,
    pub tail_slot:       u64,
    pub balance:         u64,
    pub last_rent_block: u64,
    pub total_segments:  u64,

    // +Phantom Vec<Hash> for merkle subtree nodes (up to 4096).
}

#[repr(u64)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum TapeState {
    Unknown = 0,
    Created,
    Writing,
    Finalized,
}

state!(AccountType, Tape);

//pub fn encode_tape(tape: &Tape, nodes: &[[u8; 32]]) -> Vec<u8> {
//    let mut out = Vec::with_capacity(core::mem::size_of::<Tape>() + nodes.len() * 32);
//    out.extend_from_slice(bytemuck::bytes_of(tape));
//    out.extend_from_slice(bytemuck::cast_slice(nodes));
//    out
//}
//
//pub fn decode_tape(data: &[u8]) -> Result<(&Tape, &[[u8; 32]]), ProgramError> {
//    let need = core::mem::size_of::<Tape>();
//    if data.len() < need {
//        return Err(ProgramError::AccountDataTooSmall);
//    }
//    let (head_bytes, tail) = data.split_at(need);
//    if tail.len() % 32 != 0 {
//        return Err(ProgramError::AccountDataTooSmall);
//    }
//    let header: &Tape = bytemuck::from_bytes(head_bytes);
//    let nodes: &[[u8; 32]] = bytemuck::try_cast_slice(tail).expect("len checked");
//    Ok((header, nodes))
//}
