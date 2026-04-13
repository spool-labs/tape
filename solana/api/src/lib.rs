#![allow(unexpected_cfgs)]

pub mod compute;
pub mod consts;
pub mod errors;
pub mod event;
pub mod helpers;
pub mod program;
pub mod instruction;
pub mod loaders;
pub mod state;
pub mod utils;

pub mod prelude {
    pub use tape_core::prelude::*;
    pub use tape_crypto::prelude::*;

    pub use crate::event::EventType;
    pub use crate::program::{exchange, staking, tapedrive, token};
    pub use crate::program::{
        archive_ata, archive_pda, epoch_pda, exchange_ata, exchange_pda, history_pda,
        metadata_pda, mint_pda, node_pda, snapshot_manifest_pda, snapshot_tape_pda, stake_pda,
        system_pda, tape_pda, track_pda, treasury_pda, vault_pda,
    };
    pub use crate::state::{
        AccountType, Archive, Epoch, Exchange, History, Node, SnapshotManifest, Stake, System,
        Tape, Treasury,
    };
}
