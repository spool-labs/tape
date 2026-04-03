use const_crypto::ed25519;
use solana_program::pubkey::Pubkey;
use tape_core::{
    prelude::Bitmap,
    types::{EpochNumber, TrackNumber},
};
use tape_crypto::address::Address;
use tape_crypto::Hash;

use super::token::MINT_ADDRESS;

pub use tape_core::erasure::MEMBER_COUNT;

pub const MIN_COMMITTEE_SIZE:     usize = 20;   // 20 for production (matches SPOOL_GROUP_SIZE)
pub const FUTURE_EPOCHS:          usize = 256;
pub const EPOCH_HISTORY:          usize = 256;
pub const EPOCH_VALUES:           usize = 4;    // Epoch N, N+1, N+2, N+3
pub const EPOCH_DURATION:           i64 = 100;  // 100 seconds for local testing (60s testnet, 604800s mainnet)
pub const BLACKLIST_SIZE:         usize = 24;   // 2^24 blob entries in blocklist
pub const STREAM_SEGMENTS:        usize = 18;   // 2^18 = 262,144 segments (32MiB with 128B segments)

// Emergency unstaking trigger
pub const STUCK_SYSTEM_THRESHOLD:   i64 = EPOCH_DURATION * 2;  

tape_solana::declare_id!("tajZ1QndNonM3teK59PdUfiF9ZAQT6xqucipbs8mN8W"); 

pub const PROGRAM_ID: [u8; 32] = 
    unsafe { *(&id() as *const Pubkey as *const [u8; 32]) };
pub const SPL_TOKEN_PROGRAM_ID: [u8; 32] =
    unsafe { *(&spl_token::ID as *const Pubkey as *const [u8; 32]) };
pub const ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID: [u8; 32] =
    unsafe { *(&spl_associated_token_account::ID as *const Pubkey as *const [u8; 32]) };

pub const SYSTEM:          &[u8] = b"system";
pub const ARCHIVE:         &[u8] = b"archive";
pub const EPOCH:           &[u8] = b"epoch";
pub const NODE:            &[u8] = b"node";
pub const HISTORY:         &[u8] = b"history";
pub const RESOURCE:        &[u8] = b"resource";
pub const TRACK:           &[u8] = b"track";
pub const STAKE:           &[u8] = b"stake";
pub const CERTIFICATE:     &[u8] = b"certificate";
pub const SNAPSHOT_STATE:  &[u8] = b"snapshot_state";
pub const SNAPSHOT_MANIFEST: &[u8] = b"snapshot_manifest";
pub const SNAPSHOT_TAPE:   &[u8] = b"snapshot_tape";

pub type CommitteeBitmap = Bitmap<{ (MEMBER_COUNT + 7) / 8 }>;

pub const SYSTEM_ADDRESS: Address =
    Address::new(ed25519::derive_program_address(&[SYSTEM], &PROGRAM_ID).0);

pub const SYSTEM_BUMP: u8 =
    ed25519::derive_program_address(&[SYSTEM], &PROGRAM_ID).1;

pub const EPOCH_ADDRESS: Address =
    Address::new(ed25519::derive_program_address(&[EPOCH], &PROGRAM_ID).0);

pub const EPOCH_BUMP: u8 =
    ed25519::derive_program_address(&[EPOCH], &PROGRAM_ID).1;

pub const ARCHIVE_ADDRESS: Address =
    Address::new(ed25519::derive_program_address(&[ARCHIVE], &PROGRAM_ID).0);

pub const ARCHIVE_BUMP: u8 =
    ed25519::derive_program_address(&[ARCHIVE], &PROGRAM_ID).1;

pub const ARCHIVE_ATA: Address = Address::new(
    ed25519::derive_program_address(
        &[
            ARCHIVE_ADDRESS.as_bytes(),
            &SPL_TOKEN_PROGRAM_ID,
            MINT_ADDRESS.as_bytes(),
        ],
        &ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID,
    )
    .0,
);

pub const ARCHIVE_ATA_BUMP: u8 = 
    ed25519::derive_program_address(
        &[
            ARCHIVE_ADDRESS.as_bytes(),
            &SPL_TOKEN_PROGRAM_ID,
            MINT_ADDRESS.as_bytes(),
        ],
        &ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID,
    )
    .1;

pub const SNAPSHOT_STATE_ADDRESS: Address =
    Address::new(ed25519::derive_program_address(&[SNAPSHOT_STATE], &PROGRAM_ID).0);

pub const SNAPSHOT_STATE_BUMP: u8 =
    ed25519::derive_program_address(&[SNAPSHOT_STATE], &PROGRAM_ID).1;

// ====================================================================
// PDA Functions
// ====================================================================

#[cfg(debug_assertions)]
pub fn system_pda() -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(&[SYSTEM], &program_id)
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn system_pda() -> (Address, u8) {
    (SYSTEM_ADDRESS, SYSTEM_BUMP)
}

#[cfg(debug_assertions)]
pub fn epoch_pda() -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(&[EPOCH], &program_id)
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn epoch_pda() -> (Address, u8) {
    (EPOCH_ADDRESS, EPOCH_BUMP)
}


#[cfg(debug_assertions)]
#[inline(always)]
pub fn archive_pda() -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(&[ARCHIVE], &program_id)
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn archive_pda() -> (Address, u8) {
    (ARCHIVE_ADDRESS, ARCHIVE_BUMP)
}

#[cfg(debug_assertions)]
#[inline(always)]
pub fn archive_ata() -> (Address, u8) {
    let associated_token_account_program_id: Address = spl_associated_token_account::ID.into();

    Address::find_program_address(
        &[
            ARCHIVE_ADDRESS.as_ref(),
            spl_token::ID.as_ref(),
            MINT_ADDRESS.as_ref(),
        ],
        &associated_token_account_program_id,
    )
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn archive_ata() -> (Address, u8) {
    (ARCHIVE_ATA, ARCHIVE_ATA_BUMP)
}

#[cfg(debug_assertions)]
pub fn snapshot_state_pda() -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(&[SNAPSHOT_STATE], &program_id)
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn snapshot_state_pda() -> (Address, u8) {
    (SNAPSHOT_STATE_ADDRESS, SNAPSHOT_STATE_BUMP)
}

#[inline(always)]
pub fn node_pda(authority: Address) -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(&[NODE, authority.as_ref()], &program_id)
}

#[inline(always)]
pub fn stake_pda(authority: Address) -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(&[STAKE, authority.as_ref()], &program_id)
}

#[inline(always)]
pub fn history_pda(node: Address) -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(&[HISTORY, node.as_ref()], &program_id)
}

#[inline(always)]
pub fn tape_pda(authority: Address) -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(&[RESOURCE, authority.as_ref()], &program_id)
}

#[inline(always)]
pub fn track_pda(tape: Address, track_number: TrackNumber) -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(&[TRACK, tape.as_ref(), &track_number.pack()], &program_id)
}

#[inline(always)]
pub fn cert_pda(parent: Address, message: Hash, epoch: EpochNumber) -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(
        &[CERTIFICATE, parent.as_ref(), message.as_ref(), &epoch.pack()],
        &program_id,
    )
}

#[inline(always)]
pub fn snapshot_manifest_pda(epoch: EpochNumber) -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(&[SNAPSHOT_MANIFEST, &epoch.pack()], &program_id)
}

#[inline(always)]
pub fn snapshot_tape_pda(epoch: EpochNumber) -> (Address, u8) {
    let program_id: Address = id().into();
    Address::find_program_address(&[SNAPSHOT_TAPE, &epoch.pack()], &program_id)
}

#[cfg(test)]
mod tests {
    use solana_program::pubkey::Pubkey;

    use super::*;

    #[test]
    fn test_pda_against_consts() {
        // These tests, as nonsensical as they seem, are to ensure that the PDAs generated by the
        // consts match the ones generated by the official functions. The consts are generated by
        // external deps, so if we straight up use the consts, we are trusting that the external
        // deps are working as expected, which is not a good idea. Always be testing.
        
        let (pda, bump) = system_pda();
        assert_eq!(pda, SYSTEM_ADDRESS);
        assert_eq!(bump, SYSTEM_BUMP);

        let (pda, bump) = epoch_pda();
        assert_eq!(pda, EPOCH_ADDRESS);
        assert_eq!(bump, EPOCH_BUMP);

        let (pda, bump) = archive_pda();
        assert_eq!(pda, ARCHIVE_ADDRESS);
        assert_eq!(bump, ARCHIVE_BUMP);

        let (pda, bump) = archive_ata();
        assert_eq!(pda, ARCHIVE_ATA);
        assert_eq!(bump, ARCHIVE_ATA_BUMP);

        let (pda, bump) = snapshot_state_pda();
        assert_eq!(pda, SNAPSHOT_STATE_ADDRESS);
        assert_eq!(bump, SNAPSHOT_STATE_BUMP);
    }

    #[test]
    fn test_snapshot_epoch_pdas_are_distinct_and_stable() {
        let epoch = EpochNumber(42);

        let (manifest, manifest_bump) = snapshot_manifest_pda(epoch);
        let (tape, tape_bump) = snapshot_tape_pda(epoch);

        assert_ne!(manifest, tape);
        assert_eq!(
            (manifest, manifest_bump),
            {
                let (address, bump) =
                    Pubkey::find_program_address(&[SNAPSHOT_MANIFEST, &epoch.pack()], &id());
                (address.into(), bump)
            },
        );
        assert_eq!(
            (tape, tape_bump),
            {
                let (address, bump) =
                    Pubkey::find_program_address(&[SNAPSHOT_TAPE, &epoch.pack()], &id());
                (address.into(), bump)
            },
        );
    }
}
