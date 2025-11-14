use core::marker::PhantomData;
//use tape_core::prelude::*;
//use crate::program::tapedrive::*;
use steel::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CreateStream {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterStream {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DeleteStream {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AppendToStream {
    pub size: [u8; 8],
    pub data: PhantomData<[u8]>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct UpdateStream {
    pub segment_number: [u8; 8],
    //pub old_data: [u8; SEGMENT_SIZE],
    //pub new_data: [u8; SEGMENT_SIZE],
    //pub proof: ProofPath,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct FinalizeStream {}
