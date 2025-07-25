use steel::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum ProgramInstruction {
    Unknown = 0,
    Initialize,     // Initialize the program, setting up necessary accounts
}

instruction!(ProgramInstruction, Initialize);


#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct Initialize {}

