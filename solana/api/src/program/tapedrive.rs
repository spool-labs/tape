use const_crypto::ed25519;
use solana_program::pubkey::Pubkey;
use tape_core::{
    spooler::GroupIndex,
    types::{coin::TAPE, BasisPoints, EpochNumber, SlotNumber, StorageUnits, TrackNumber},
};
use tape_crypto::{Address, Hash};

use super::token::MINT_ADDRESS;

pub const MIN_COMMITTEE_SIZE:     usize = 20;
pub const MIN_STORAGE_CAPACITY:   usize = 1 << 30; // 1GiB
pub const MIN_STORAGE_PRICE:      usize = 1;       // per GiB in TAPE per epoch

pub const DEFAULT_STORAGE_CAPACITY:    StorageUnits = StorageUnits(100 * StorageUnits::TB);
pub const DEFAULT_STORAGE_PRICE:               TAPE = TAPE(954); // ~1 TAPE / TiB-epoch
pub const DEFAULT_BURN_FEE_BPS:         BasisPoints = BasisPoints(1_000);
pub const DEFAULT_SUBSIDY_DECAY_BPS:    BasisPoints = BasisPoints(100);
pub const MAX_SUBSIDY_DECAY_BPS:        BasisPoints = BasisPoints(500);

pub const EPOCH_VALUES:           usize = 4;    // Epoch N, N+1, N+2, N+3
pub const FUTURE_EPOCHS:          usize = 256;  // ~5 years at 1 week epochs

tape_solana::declare_id!("Gyc2KxCpNrikfdDGyeMa3tJeS2FQ4MxNrD5TW9Z5ZCSQ");

pub const PROGRAM_ID: [u8; 32] =
    unsafe { *(&id() as *const Pubkey as *const [u8; 32]) };
pub const SPL_TOKEN_PROGRAM_ID: [u8; 32] =
    unsafe { *(&spl_token::ID as *const Pubkey as *const [u8; 32]) };
pub const ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID: [u8; 32] =
    unsafe { *(&spl_associated_token_account::ID as *const Pubkey as *const [u8; 32]) };

pub const SYSTEM:             &[u8] = b"system";
pub const ARCHIVE:            &[u8] = b"archive";
pub const SUBSIDY:            &[u8] = b"subsidy";
pub const EPOCH:              &[u8] = b"epoch";
pub const COMMITTEE:          &[u8] = b"committee";
pub const GROUP:              &[u8] = b"group";
pub const PEER_SET:           &[u8] = b"peer_set";
pub const NODE:               &[u8] = b"node";
pub const BLACKLIST:          &[u8] = b"blacklist";
pub const HISTORY:            &[u8] = b"history";
pub const CASSETTE:           &[u8] = b"cassette";
pub const TRACK:              &[u8] = b"track";
pub const STAKE:              &[u8] = b"stake";
pub const STAKE_AUTHORITY:    &[u8] = b"stake_authority";
pub const VOTE:               &[u8] = b"vote";
pub const VOTE_SNAPSHOT:      &[u8] = b"snapshot";
pub const VOTE_ASSIGNMENT:    &[u8] = b"assignment";
pub const SNAPSHOT_TAPE:      &[u8] = b"snapshot_tape";
pub const EVENT:              &[u8] = b"event";

pub const SYSTEM_ADDRESS: Address =
    Address::new(ed25519::derive_program_address(&[SYSTEM], &PROGRAM_ID).0);

pub const SYSTEM_BUMP: u8 =
    ed25519::derive_program_address(&[SYSTEM], &PROGRAM_ID).1;

pub const ARCHIVE_ADDRESS: Address =
    Address::new(ed25519::derive_program_address(&[ARCHIVE], &PROGRAM_ID).0);

pub const ARCHIVE_BUMP: u8 =
    ed25519::derive_program_address(&[ARCHIVE], &PROGRAM_ID).1;

pub const SUBSIDY_ADDRESS: Address =
    Address::new(ed25519::derive_program_address(&[SUBSIDY], &PROGRAM_ID).0);

pub const SUBSIDY_BUMP: u8 =
    ed25519::derive_program_address(&[SUBSIDY], &PROGRAM_ID).1;

pub const PEER_SET_ADDRESS: Address =
    Address::new(ed25519::derive_program_address(&[PEER_SET], &PROGRAM_ID).0);

pub const PEER_SET_BUMP: u8 =
    ed25519::derive_program_address(&[PEER_SET], &PROGRAM_ID).1;

pub const STAKE_AUTHORITY_ADDRESS: Address =
    Address::new(ed25519::derive_program_address(&[STAKE_AUTHORITY], &PROGRAM_ID).0);

pub const STAKE_AUTHORITY_BUMP: u8 =
    ed25519::derive_program_address(&[STAKE_AUTHORITY], &PROGRAM_ID).1;

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

pub const SUBSIDY_ATA: Address = Address::new(
    ed25519::derive_program_address(
        &[
            SUBSIDY_ADDRESS.as_bytes(),
            &SPL_TOKEN_PROGRAM_ID,
            MINT_ADDRESS.as_bytes(),
        ],
        &ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID,
    )
    .0,
);

pub const SUBSIDY_ATA_BUMP: u8 =
    ed25519::derive_program_address(
        &[
            SUBSIDY_ADDRESS.as_bytes(),
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

#[inline(always)]
pub fn epoch_pda(epoch: EpochNumber) -> (Address, u8) {
    Address::find_program_address(&[EPOCH, &epoch.pack()], id())
}

#[inline(always)]
pub fn committee_pda(epoch: EpochNumber) -> (Address, u8) {
    Address::find_program_address(&[COMMITTEE, &epoch.pack()], id())
}

#[inline(always)]
pub fn group_pda(epoch: EpochNumber, group: GroupIndex) -> (Address, u8) {
    Address::find_program_address(&[GROUP, &epoch.pack(), &group.pack()], id())
}

#[cfg(debug_assertions)]
pub fn peer_set_pda() -> (Address, u8) {
    Address::find_program_address(&[PEER_SET], id())
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn peer_set_pda() -> (Address, u8) {
    (PEER_SET_ADDRESS, PEER_SET_BUMP)
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
pub fn subsidy_pda() -> (Address, u8) {
    Address::find_program_address(&[SUBSIDY], id())
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn subsidy_pda() -> (Address, u8) {
    (SUBSIDY_ADDRESS, SUBSIDY_BUMP)
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

#[cfg(debug_assertions)]
#[inline(always)]
pub fn subsidy_ata() -> (Address, u8) {
    Address::find_program_address(
        &[
            SUBSIDY_ADDRESS.as_ref(),
            spl_token::ID.as_ref(),
            MINT_ADDRESS.as_ref(),
        ],
        spl_associated_token_account::ID,
    )
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn subsidy_ata() -> (Address, u8) {
    (SUBSIDY_ATA, SUBSIDY_ATA_BUMP)
}

#[inline(always)]
pub fn node_pda(authority: Address) -> (Address, u8) {
    Address::find_program_address(&[NODE, authority.as_ref()], id())
}

#[inline(always)]
pub fn stake_pda(authority: Address) -> (Address, u8) {
    Address::find_program_address(&[STAKE, authority.as_ref()], id())
}

#[cfg(debug_assertions)]
#[inline(always)]
pub fn stake_authority_pda() -> (Address, u8) {
    Address::find_program_address(&[STAKE_AUTHORITY], id())
}

#[cfg(not(debug_assertions))]
#[inline(always)]
pub fn stake_authority_pda() -> (Address, u8) {
    (STAKE_AUTHORITY_ADDRESS, STAKE_AUTHORITY_BUMP)
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
pub fn blacklist_pda(node: Address) -> (Address, u8) {
    Address::find_program_address(&[BLACKLIST, node.as_ref()], id())
}

#[inline(always)]
pub fn snapshot_tape_pda(epoch: EpochNumber) -> (Address, u8) {
    Address::find_program_address(&[SNAPSHOT_TAPE, &epoch.pack()], id())
}

#[inline(always)]
pub fn event_pda(slot: SlotNumber, seq: u16) -> (Address, u8) {
    Address::find_program_address(&[EVENT, &slot.pack(), &seq.to_le_bytes()], id())
}

#[inline(always)]
pub fn snapshot_vote_pda(voting: EpochNumber, target: EpochNumber, hash: Hash) -> (Address, u8) {
    Address::find_program_address(&[VOTE, VOTE_SNAPSHOT, &voting.pack(), &target.pack(), hash.as_ref()], id())
}

#[inline(always)]
pub fn assignment_vote_pda(voting: EpochNumber, target: EpochNumber, hash: Hash) -> (Address, u8) {
    Address::find_program_address(&[VOTE, VOTE_ASSIGNMENT, &voting.pack(), &target.pack(), hash.as_ref()], id())
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

        let (pda, bump) = archive_pda();
        assert_eq!(pda, ARCHIVE_ADDRESS);
        assert_eq!(bump, ARCHIVE_BUMP);

        let (pda, bump) = archive_ata();
        assert_eq!(pda, ARCHIVE_ATA);
        assert_eq!(bump, ARCHIVE_ATA_BUMP);

        let (pda, bump) = peer_set_pda();
        assert_eq!(pda, PEER_SET_ADDRESS);
        assert_eq!(bump, PEER_SET_BUMP);

        let (pda, bump) = stake_authority_pda();
        assert_eq!(pda, STAKE_AUTHORITY_ADDRESS);
        assert_eq!(bump, STAKE_AUTHORITY_BUMP);
    }

    #[test]
    fn epoch_scoped_pdas_distinct() {
        let epoch = EpochNumber(42);
        let group = GroupIndex(7);

        let (epoch_pda_addr, _) = epoch_pda(epoch);
        let (committee, _) = committee_pda(epoch);
        let (group_addr, _) = group_pda(epoch, group);
        let (peers, _) = peer_set_pda();
        let (snapshot_tape, _) = snapshot_tape_pda(epoch);

        assert_ne!(epoch_pda_addr, committee);
        assert_ne!(epoch_pda_addr, group_addr);
        assert_ne!(epoch_pda_addr, peers);
        assert_ne!(committee, group_addr);
        assert_ne!(committee, peers);
        assert_ne!(group_addr, peers);
        assert_ne!(snapshot_tape, epoch_pda_addr);

        // Different epochs produce distinct addresses.
        let other = EpochNumber(43);
        let (epoch_pda_other, _) = epoch_pda(other);
        let (committee_other, _) = committee_pda(other);
        let (group_other, _) = group_pda(other, group);
        assert_ne!(epoch_pda_addr, epoch_pda_other);
        assert_ne!(committee, committee_other);
        assert_ne!(group_addr, group_other);

        // Different groups within the same epoch are distinct.
        let (group_neighbor, _) = group_pda(epoch, GroupIndex(8));
        assert_ne!(group_addr, group_neighbor);

        // PDAs are reproducible.
        let (committee_recomputed, committee_bump) =
            Pubkey::find_program_address(&[COMMITTEE, &epoch.pack()], &id());
        let (committee_check, _) = committee_pda(epoch);
        assert_eq!(committee_check, Address::from(committee_recomputed));
        let (_, expected_bump) = committee_pda(epoch);
        assert_eq!(expected_bump, committee_bump);
    }
}
