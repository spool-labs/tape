use tape_solana::*;
use crate::pda::*;
use tape_core::prelude::*;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RegisterFeature {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct CertifyFeature {}
