use const_crypto::ed25519;
use solana_program::pubkey::Pubkey;

// ====================================================================
// PDA Seed Constants
// ====================================================================
/// Program-derived address (PDA) seeds
pub const ARCHIVE:  &[u8] = b"archive";
pub const BLOCK:    &[u8] = b"block";
pub const EPOCH:    &[u8] = b"epoch";
pub const MINER:    &[u8] = b"miner";
pub const WRITER:   &[u8] = b"writer";
pub const TAPE:     &[u8] = b"tape";
pub const TREASURY: &[u8] = b"treasury";
pub const MINT:     &[u8] = b"mint";
pub const METADATA: &[u8] = b"metadata";

/// Mint PDA seed (raw bytes)
pub const MINT_SEED: &[u8] = &[152, 68, 212, 200, 25, 113, 221, 71];

// ====================================================================
// Metadata Constants
// ====================================================================
/// On-chain metadata for the TAPE token
pub const METADATA_NAME:   &str = "TAPE";
pub const METADATA_SYMBOL: &str = "TAPE";
pub const METADATA_URI:    &str = "https://tapedrive.io/metadata.json";

// ====================================================================
// Merkle Tree Configuration
// ====================================================================
/// Height of the Merkle tree (number of levels)
pub const TREE_HEIGHT: usize = 18;
/// Number of hashes in a Merkle proof (equal to TREE_HEIGHT)
pub const PROOF_LEN: usize = TREE_HEIGHT;

// ====================================================================
// Tape & Segment Sizing
// ====================================================================
/// Segment size in bytes
pub const SEGMENT_SIZE: usize = 128;
/// Maximum tape size in bytes = 2^TREE_HEIGHT segments
pub const MAX_TAPE_SIZE: usize = (1 << TREE_HEIGHT) * SEGMENT_SIZE;

// ====================================================================
// Token Economics
// ====================================================================
/// Number of decimal places for TAPE
pub const TOKEN_DECIMALS: u8 = 10;
/// Smallest on-chain unit = 10^TOKEN_DECIMALS
pub const ONE_TAPE: u64 = 10u64.pow(TOKEN_DECIMALS as u32);
/// Maximum total TAPE supply
pub const MAX_SUPPLY: u64 = 7_000_000 * ONE_TAPE;

/// Minimum PoW solution difficulty
pub const MIN_DIFFICULTY: u64              = 1;
/// Minimum block participation required to solve a block
pub const MIN_PARTICIPATION_TARGET: u64    = 1;
/// Maximum block participation required to solve a block
pub const MAX_PARTICIPATION_TARGET: u64    = 100;
/// Minimum reward scaling factor for miners
pub const MIN_CONSISTENCY_MULTIPLIER: u64  = 1;
/// Maximum reward scaling factor for miners
pub const MAX_CONSISTENCY_MULTIPLIER: u64  = 32;

// ====================================================================
// Time & Epoch Constants
// ====================================================================
/// Duration of one block in seconds (~1 minute)
pub const BLOCK_DURATION_SECONDS: u64 = 60;
/// Number of blocks per epoch (~10 minutes)
pub const EPOCH_BLOCKS: u64 = 10;
/// Adjustment interval (in epochs)
pub const ADJUSTMENT_INTERVAL: u64 = 50;

// ====================================================================
// Rent Model Constants
// ====================================================================
/// Rent charged per segment per block
pub const RENT_PER_SEGMENT: u64 = 100; // TODO: adjust this value
                                       ///
/// Empty segment of SEGMENT_SIZE bytes for tapes that don't have minimum rent
pub const EMPTY_SEGMENT: [u8; SEGMENT_SIZE] = [0; SEGMENT_SIZE];
/// Empty Merkle proof for tapes that don't have minimum rent
pub const EMPTY_PROOF: [[u8; 32]; PROOF_LEN] = [[0; 32]; PROOF_LEN];

// ====================================================================
// Miscellaneous
// ====================================================================
/// Maximum length for names
pub const NAME_LEN:   usize = 32;
/// Header size in bytes
pub const HEADER_SIZE: usize = 64;

// ====================================================================
// Const Addresses
// There isn't a better way to do this yet; maybe a build.rs + include
// ====================================================================

pub const PROGRAM_ID: [u8; 32] = 
    unsafe { *(&crate::id() as *const Pubkey as *const [u8; 32]) };

pub const ARCHIVE_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[ARCHIVE], &PROGRAM_ID).0);

pub const ARCHIVE_BUMP: u8 =
    ed25519::derive_program_address(&[ARCHIVE], &PROGRAM_ID).1;

pub const EPOCH_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[EPOCH], &PROGRAM_ID).0);

pub const EPOCH_BUMP: u8 =
    ed25519::derive_program_address(&[EPOCH], &PROGRAM_ID).1;

pub const BLOCK_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[BLOCK], &PROGRAM_ID).0);

pub const BLOCK_BUMP: u8 =
    ed25519::derive_program_address(&[BLOCK], &PROGRAM_ID).1;

pub const MINT_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[MINT, MINT_SEED], &PROGRAM_ID).0);

pub const MINT_BUMP: u8 = 
    ed25519::derive_program_address(&[MINT, MINT_SEED], &PROGRAM_ID).1;

pub const TREASURY_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[TREASURY], &PROGRAM_ID).0);

pub const TREASURY_BUMP: u8 = 
    ed25519::derive_program_address(&[TREASURY], &PROGRAM_ID).1;

pub const TREASURY_ATA: Pubkey = Pubkey::new_from_array(
    ed25519::derive_program_address(
        &[
            unsafe { &*(&TREASURY_ADDRESS as *const Pubkey as *const [u8; 32]) },
            unsafe { &*(&spl_token::id() as *const Pubkey as *const [u8; 32]) },
            unsafe { &*(&MINT_ADDRESS as *const Pubkey as *const [u8; 32]) },
        ],
        unsafe { &*(&spl_associated_token_account::id() as *const Pubkey as *const [u8; 32]) },
    )
    .0,
);

