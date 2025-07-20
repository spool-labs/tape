use steel::*;
use crate::consts::*;

pub fn archive_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[ARCHIVE], &crate::id())
}

pub fn epoch_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[EPOCH], &crate::id())
}

pub fn block_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[BLOCK], &crate::id())
}

pub fn treasury_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[TREASURY], &crate::id())
}

pub fn treasury_ata() -> (Pubkey, u8) {
    let (treasury_pda, _bump) = treasury_pda();
    let (mint_pda, _bump) = mint_pda();

    Pubkey::find_program_address(
        &[
            treasury_pda.as_ref(), 
            spl_token::ID.as_ref(),
            mint_pda.as_ref()
        ],
        &spl_associated_token_account::ID,
    )
}

pub fn mint_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[MINT, MINT_SEED], &crate::id())
}

pub fn metadata_pda(mint: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[ METADATA, mpl_token_metadata::ID.as_ref(), mint.as_ref() ],
        &mpl_token_metadata::ID,
    )
}

pub fn tape_pda(authority: Pubkey, name: &[u8; NAME_LEN]) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[TAPE, authority.as_ref(), name.as_ref()], &crate::id())
}

pub fn writer_pda(tape: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[WRITER, tape.as_ref()], &crate::id())
}

pub fn miner_pda(authority: Pubkey, name: [u8; NAME_LEN]) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[MINER, authority.as_ref(), name.as_ref()], &crate::id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pda_against_consts() {
        let (pda, bump) = archive_pda();
        assert_eq!(bump, ARCHIVE_BUMP);
        assert_eq!(pda, ARCHIVE_ADDRESS);

        let (pda, bump) = epoch_pda();
        assert_eq!(bump, EPOCH_BUMP);
        assert_eq!(pda, EPOCH_ADDRESS);

        let (pda, bump) = block_pda();
        assert_eq!(bump, BLOCK_BUMP);
        assert_eq!(pda, BLOCK_ADDRESS);

        let (pda, bump) = mint_pda();
        assert_eq!(bump, MINT_BUMP);
        assert_eq!(pda, MINT_ADDRESS);

        let (pda, bump) = treasury_pda();
        assert_eq!(bump, TREASURY_BUMP);
        assert_eq!(pda, TREASURY_ADDRESS);

        let (pda, _bump) = treasury_ata();
        assert_eq!(pda, TREASURY_ATA);
    }
}
