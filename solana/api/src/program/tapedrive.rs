use const_crypto::ed25519;
use solana_program::pubkey::Pubkey;
use tape_core::{
    spooler::SpoolGroup,
    types::{ChunkNumber, EpochNumber, TrackNumber},
};
use tape_crypto::address::Address;

use super::token::MINT_ADDRESS;
pub const MIN_COMMITTEE_SIZE:     usize = 20;   // 20 for production (matches SPOOL_GROUP_SIZE)
pub const FUTURE_EPOCHS:          usize = 256;
pub const EPOCH_HISTORY:          usize = 256;
pub const EPOCH_VALUES:           usize = 4;    // Epoch N, N+1, N+2, N+3
pub const EPOCH_DURATION:           i64 = 100;  // 100 seconds for local testing (60s testnet, 604800s mainnet)
pub const BLACKLIST_SIZE:         usize = 24;   // 2^24 blob entries in blocklist
pub const STREAM_SEGMENTS:        usize = 18;   // 2^18 = 262,144 segments (32MiB with 128B segments)

// Emergency unstaking trigger
pub const STUCK_SYSTEM_THRESHOLD:   i64 = EPOCH_DURATION * 2;  

tape_solana::declare_id!("GWKaEaG35pmHMXxpjyGPcikkJQfo3CcJ25r9wJDuAfNS");

pub const PROGRAM_ID: [u8; 32] = 
    unsafe { *(&id() as *const Pubkey as *const [u8; 32]) };
pub const SPL_TOKEN_PROGRAM_ID: [u8; 32] =
    unsafe { *(&spl_token::ID as *const Pubkey as *const [u8; 32]) };
pub const ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID: [u8; 32] =
    unsafe { *(&spl_associated_token_account::ID as *const Pubkey as *const [u8; 32]) };

pub const SYSTEM:             &[u8] = b"system";
pub const ARCHIVE:            &[u8] = b"archive";
pub const EPOCH:              &[u8] = b"epoch";
pub const NODE:               &[u8] = b"node";
pub const HISTORY:            &[u8] = b"history";
pub const CASSETTE:           &[u8] = b"cassette";
pub const TRACK:              &[u8] = b"track";
pub const VOTE:               &[u8] = b"vote";
pub const SNAPSHOT_VOTE:      &[u8] = b"snapshot";
pub const STAKE:              &[u8] = b"stake";
pub const SNAPSHOT_MANIFEST:  &[u8] = b"snapshot_manifest";
pub const SNAPSHOT_TAPE:      &[u8] = b"snapshot_tape";

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

// ====================================================================
// PDA Functions
// ====================================================================

#[cfg(debug_assertions)]
pub fn system_pda() -> (Address, u8) {
    Address::find_program_address(&[SYSTEM], id())
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn system_pda() -> (Address, u8) {
    (SYSTEM_ADDRESS, SYSTEM_BUMP)
}

#[cfg(debug_assertions)]
pub fn epoch_pda() -> (Address, u8) {
    Address::find_program_address(&[EPOCH], id())
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn epoch_pda() -> (Address, u8) {
    (EPOCH_ADDRESS, EPOCH_BUMP)
}


#[cfg(debug_assertions)]
#[inline(always)]
pub fn archive_pda() -> (Address, u8) {
    Address::find_program_address(&[ARCHIVE], id())
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn archive_pda() -> (Address, u8) {
    (ARCHIVE_ADDRESS, ARCHIVE_BUMP)
}

#[cfg(debug_assertions)]
#[inline(always)]
pub fn archive_ata() -> (Address, u8) {
    Address::find_program_address(
        &[
            ARCHIVE_ADDRESS.as_ref(),
            spl_token::ID.as_ref(),
            MINT_ADDRESS.as_ref(),
        ],
        spl_associated_token_account::ID,
    )
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn archive_ata() -> (Address, u8) {
    (ARCHIVE_ATA, ARCHIVE_ATA_BUMP)
}

#[inline(always)]
pub fn node_pda(authority: Address) -> (Address, u8) {
    Address::find_program_address(&[NODE, authority.as_ref()], id())
}

#[inline(always)]
pub fn stake_pda(authority: Address) -> (Address, u8) {
    Address::find_program_address(&[STAKE, authority.as_ref()], id())
}

#[inline(always)]
pub fn history_pda(node: Address) -> (Address, u8) {
    Address::find_program_address(&[HISTORY, node.as_ref()], id())
}

#[inline(always)]
pub fn tape_pda(authority: Address) -> (Address, u8) {
    Address::find_program_address(&[CASSETTE, authority.as_ref()], id())
}

#[inline(always)]
pub fn track_pda(tape: Address, track_number: TrackNumber) -> (Address, u8) {
    Address::find_program_address(&[TRACK, tape.as_ref(), &track_number.pack()], id())
}

#[inline(always)]
pub fn snapshot_pda(epoch: EpochNumber) -> (Address, u8) {
    Address::find_program_address(&[SNAPSHOT_MANIFEST, &epoch.pack()], id())
}

#[inline(always)]
pub fn snapshot_tape_pda(epoch: EpochNumber) -> (Address, u8) {
    Address::find_program_address(&[SNAPSHOT_TAPE, &epoch.pack()], id())
}

#[inline(always)]
pub fn snapshot_vote_pda(epoch: EpochNumber, group: SpoolGroup, chunk: ChunkNumber) -> (Address, u8) {
    Address::find_program_address(&[VOTE, SNAPSHOT_VOTE, &epoch.pack(), &group.pack(), &chunk.pack()], id())
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
    }

    #[test]
    fn test_snapshot_epoch_pdas_are_distinct_and_stable() {
        let epoch = EpochNumber(42);

        let (snapshot, snapshot_bump) = snapshot_pda(epoch);
        let (tape, tape_bump) = snapshot_tape_pda(epoch);

        assert_ne!(snapshot, tape);
        assert_eq!(
            (snapshot, snapshot_bump),
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

        let group = SpoolGroup(7);
        let chunk = ChunkNumber(3);
        let (vote_address, vote_bump) = snapshot_vote_pda(epoch, group, chunk);
        assert_ne!(snapshot, vote_address);
        assert_ne!(tape, vote_address);
        assert_eq!(
            (vote_address, vote_bump),
            {
                let (address, bump) = Pubkey::find_program_address(
                    &[VOTE, SNAPSHOT_VOTE, &epoch.pack(), &group.pack(), &chunk.pack()],
                    &id(),
                );
                (address.into(), bump)
            },
        );
    }
}
