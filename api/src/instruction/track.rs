use core::marker::PhantomData;
use tape_core::prelude::*;
use steel::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterTrack {
    pub id: Hash,
    pub kind: [u8; 8],
    pub size: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DeleteTrack {}

// Blob

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CertifyBlob {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct InvalidateBlob {}

// Stream

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct AppendStreamSegment {
    pub size: [u8; 8],
    pub data: PhantomData<[u8]>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct UpdateStreamSegment {
    pub segment_number: [u8; 8],
    //pub old_data: [u8; SEGMENT_SIZE],
    //pub new_data: [u8; SEGMENT_SIZE],
    //pub proof: ProofPath,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct FinalizeStream {}
