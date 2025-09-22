use const_crypto::ed25519;
use solana_program::pubkey::Pubkey;

pub const SYSTEM:    &[u8] = b"system";
pub const TREASURY:  &[u8] = b"treasury";
pub const ARCHIVE:   &[u8] = b"archive";
pub const EPOCH:     &[u8] = b"epoch";
pub const POOL:      &[u8] = b"pool";
pub const STAKE:     &[u8] = b"stake";
pub const BLOB:      &[u8] = b"blob";

// ====================================================================
// Token
// ====================================================================

pub const MINT:      &[u8] = b"mint";
pub const MINT_SEED: &[u8] = &[152, 68, 212, 200, 25, 113, 221, 71];
pub const METADATA:  &[u8] = b"metadata";

pub const TOKEN_DECIMALS: u8 = 6;
pub const ONE_TAPE: u64 = 10u64.pow(TOKEN_DECIMALS as u32);
pub const MAX_SUPPLY: u64 = 7_000_000_000 * ONE_TAPE;

pub const METADATA_NAME:   &str = "TAPE";
pub const METADATA_SYMBOL: &str = "TAPE";
pub const METADATA_URI:    &str = "https://tapedrive.io/metadata.json";

// ====================================================================
// Miscellaneous
// ====================================================================

pub const NAME_LENGTH: usize = 32;

// ====================================================================
// Const Addresses
// There isn't a better way to do this yet; maybe a build.rs + include
// ====================================================================

pub const PROGRAM_ID: [u8; 32] = 
    unsafe { *(&crate::id() as *const Pubkey as *const [u8; 32]) };

pub const SYSTEM_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[SYSTEM], &PROGRAM_ID).0);

pub const ARCHIVE_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[ARCHIVE], &PROGRAM_ID).0);

pub const ARCHIVE_BUMP: u8 =
    ed25519::derive_program_address(&[ARCHIVE], &PROGRAM_ID).1;

pub const EPOCH_ADDRESS: Pubkey =
    Pubkey::new_from_array(ed25519::derive_program_address(&[EPOCH], &PROGRAM_ID).0);

pub const EPOCH_BUMP: u8 =
    ed25519::derive_program_address(&[EPOCH], &PROGRAM_ID).1;

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
