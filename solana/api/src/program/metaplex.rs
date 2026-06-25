use solana_program::{
    account_info::AccountInfo,
    entrypoint::ProgramResult,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};

tape_solana::declare_id!("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");

pub const PROGRAM_ID: [u8; 32] = ID.to_bytes();

const CREATE_METADATA_ACCOUNT_V3_DISCRIMINATOR: u8 = 33;

#[inline]
pub fn create_metadata_account_v3_signed<'info>(
    metadata_program_info: &AccountInfo<'info>,
    metadata_info: &AccountInfo<'info>,
    mint_info: &AccountInfo<'info>,
    mint_authority_info: &AccountInfo<'info>,
    payer_info: &AccountInfo<'info>,
    update_authority_info: &AccountInfo<'info>,
    system_program_info: &AccountInfo<'info>,
    rent_sysvar_info: Option<&AccountInfo<'info>>,
    data: MetadataData<'_>,
    signers_seeds: &[&[&[u8]]],
) -> ProgramResult {
    let mut accounts = Vec::with_capacity(7);
    accounts.push(AccountMeta::new(*metadata_info.key, false));
    accounts.push(AccountMeta::new_readonly(*mint_info.key, false));
    accounts.push(AccountMeta::new_readonly(*mint_authority_info.key, true));
    accounts.push(AccountMeta::new(*payer_info.key, true));
    accounts.push(AccountMeta::new_readonly(*update_authority_info.key, true));
    accounts.push(AccountMeta::new_readonly(*system_program_info.key, false));
    if let Some(rent_sysvar_info) = rent_sysvar_info {
        accounts.push(AccountMeta::new_readonly(*rent_sysvar_info.key, false));
    }

    let instruction = Instruction {
        program_id: ID,
        accounts,
        data: data.to_create_metadata_account_v3_bytes(),
    };

    let mut account_infos = Vec::with_capacity(8);
    account_infos.push(metadata_program_info.clone());
    account_infos.push(metadata_info.clone());
    account_infos.push(mint_info.clone());
    account_infos.push(mint_authority_info.clone());
    account_infos.push(payer_info.clone());
    account_infos.push(update_authority_info.clone());
    account_infos.push(system_program_info.clone());
    if let Some(rent_sysvar_info) = rent_sysvar_info {
        account_infos.push(rent_sysvar_info.clone());
    }

    if signers_seeds.is_empty() {
        solana_program::program::invoke(&instruction, &account_infos)
    } else {
        solana_program::program::invoke_signed(&instruction, &account_infos, signers_seeds)
    }
}

pub struct MetadataData<'a> {
    pub name: &'a str,
    pub symbol: &'a str,
    pub uri: &'a str,
    pub seller_fee_basis_points: u16,
    pub is_mutable: bool,
}

impl MetadataData<'_> {
    fn to_create_metadata_account_v3_bytes(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(
            1 + 4 + self.name.len() + 4 + self.symbol.len() + 4 + self.uri.len() + 7,
        );
        data.push(CREATE_METADATA_ACCOUNT_V3_DISCRIMINATOR);
        push_borsh_string(&mut data, self.name);
        push_borsh_string(&mut data, self.symbol);
        push_borsh_string(&mut data, self.uri);
        data.extend_from_slice(&self.seller_fee_basis_points.to_le_bytes());
        data.push(0); // creators: None
        data.push(0); // collection: None
        data.push(0); // uses: None
        data.push(u8::from(self.is_mutable));
        data.push(0); // collection_details: None
        data
    }
}

fn push_borsh_string(data: &mut Vec<u8>, value: &str) {
    let len = u32::try_from(value.len()).expect("metadata string too long");
    data.extend_from_slice(&len.to_le_bytes());
    data.extend_from_slice(value.as_bytes());
}
