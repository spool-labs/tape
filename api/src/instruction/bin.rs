use steel::*;
// use crate::consts::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum BinInstruction {
    Create = 0x40,   // Create a bin to store tapes
    Destory,         // Destroy a bin, returning the rent to the miner
    Pack,            // Pack a tape into a bin
    Unpack,          // Unpack a tape from a bin
}

// instruction!(BinInstruction, Create);
// instruction!(BinInstruction, Destroy);
// instruction!(BinInstruction, Pack);
// instruction!(BinInstruction, Unpack);


// #[repr(C)]
// #[derive(Clone, Copy, Debug, Pod, Zeroable)]
// pub struct Pack {
//     pub index: [u8; 8],
//     pub old_tape: [u8; 32],
//     pub new_tape: [u8; 32],
//     pub proof: [[u8; 32]; TAPE_PROOF_LEN],
// }

