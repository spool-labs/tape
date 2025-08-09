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
pub const SPOOL:    &[u8] = b"spool";
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
/// Height of the Merkle tree containing segments (number of levels)
pub const SEGMENT_TREE_HEIGHT: usize = 33; // 1TiB of data
/// Number of hashes in a Merkle proof for a segment tree
pub const SEGMENT_PROOF_LEN: usize = SEGMENT_TREE_HEIGHT;

/// Height of the Merkle tree containing tapes (number of levels)
pub const TAPE_TREE_HEIGHT: usize = 10;
/// Number of hashes in a Merkle proof for the tape tree
pub const TAPE_PROOF_LEN: usize = TAPE_TREE_HEIGHT;

// ====================================================================
// Sizing
// ====================================================================
/// Segment size in bytes
pub const SEGMENT_SIZE: usize = 128;
/// Packed Segment size in bytes
pub const PACKED_SEGMENT_SIZE: usize = 152; // packx::SOLUTION_SIZE

/// Maximum number of segments in a tape
pub const MAX_SEGMENTS_PER_TAPE: usize = 1 << SEGMENT_TREE_HEIGHT - 1;
/// Maximum number of tapes in a spool
pub const MAX_TAPES_PER_SPOOL: usize = 1 << TAPE_TREE_HEIGHT - 1;

// ====================================================================
// Token Economics
// ====================================================================
/// Number of decimal places for TAPE
pub const TOKEN_DECIMALS: u8 = 10;
/// Smallest on-chain unit = 10^TOKEN_DECIMALS
pub const ONE_TAPE: u64 = 10u64.pow(TOKEN_DECIMALS as u32);
/// Maximum total TAPE supply
pub const MAX_SUPPLY: u64 = 7_000_000 * ONE_TAPE;

/// Minimum mining difficulty
pub const MIN_MINING_DIFFICULTY: u64       = 1;
/// Minimum packing difficulty
pub const MIN_PACKING_DIFFICULTY: u64      = 0;
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

/// Empty segment of SEGMENT_SIZE bytes for tapes that don't have minimum rent
pub const EMPTY_SEGMENT: [u8; SEGMENT_SIZE] = [0; SEGMENT_SIZE];
/// Empty Merkle proof for tapes that don't have minimum rent
pub const EMPTY_PROOF: [[u8; 32]; SEGMENT_PROOF_LEN] = [[0; 32]; SEGMENT_PROOF_LEN];

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

pub const TREASURY_ATA_BUMP: u8 = 
    ed25519::derive_program_address(
        &[
            unsafe { &*(&TREASURY_ADDRESS as *const Pubkey as *const [u8; 32]) },
            unsafe { &*(&spl_token::id() as *const Pubkey as *const [u8; 32]) },
            unsafe { &*(&MINT_ADDRESS as *const Pubkey as *const [u8; 32]) },
        ],
        unsafe { &*(&spl_associated_token_account::id() as *const Pubkey as *const [u8; 32]) },
    )
    .1;
