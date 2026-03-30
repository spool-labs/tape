pub mod blob;
pub mod data;
pub mod store;
pub mod types;

pub const TRACK_TREE_HEIGHT: usize = 10;
pub const TRACK_LEAF_V1: &[u8; 8] = b"TRACK_V1";
