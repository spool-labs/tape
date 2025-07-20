use bytemuck::{Pod, Zeroable};
use num_enum::TryFromPrimitive;
use crate::event;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum EventType {
    Unknown = 0,

    WriteEvent,
    UpdateEvent,
    FinalizeEvent,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct WriteEvent {
    pub num_added: u64,
    pub num_total: u64,
    pub prev_slot: u64,
    pub address: [u8; 32],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct UpdateEvent {
    pub segment_number: u64,
    pub prev_slot: u64,
    pub address: [u8; 32],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct FinalizeEvent {
    pub tape: u64,
    pub address: [u8; 32],
}

event!(EventType, WriteEvent);
event!(EventType, UpdateEvent);
event!(EventType, FinalizeEvent);
