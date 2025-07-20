use anyhow::{Result, anyhow};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig};
use solana_sdk::{pubkey::Pubkey, account::Account};
use tape_api::pda::{archive_pda, epoch_pda, block_pda};
use tape_api::state::{Tape, Writer, Miner, Epoch, Block, Archive};
use crate::utils::{deserialize, get_account, get_program_account};

pub async fn get_tape_account(client: &RpcClient, tape_address: &Pubkey) -> Result<(Tape, Pubkey)> {
    let account_bytes = get_account(client, tape_address).await?;
    let account: Account = deserialize(&account_bytes)?;
    let account = Tape::unpack(&account.data)
        .map_err(|e| anyhow!("Failed to unpack tape account: {}", e))
        .copied()?;
    Ok((account, *tape_address))
}

pub async fn find_tape_account(client: &RpcClient, number: u64) -> Result<Option<(Pubkey, Account)>> {
    let number_bytes = number.to_le_bytes();
    let number_base64 = base64::encode(&number_bytes);

    let config = RpcProgramAccountsConfig {
        
        filters: Some(vec![
            RpcFilterType::DataSize(Tape::get_size() as u64),
            RpcFilterType::Memcmp(Memcmp::new(
                8, // Offset of `number` field
                MemcmpEncodedBytes::Base64(number_base64),
            )),
        ]),

        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            data_slice: Some(UiDataSliceConfig { offset: 0, length: 0, }),
            commitment: None,
            min_context_slot: None,
        },
        with_context: None,
        sort_results: true.into(),
    };

    let accounts = get_program_account(client, config).await?;

    // Return the first matching account, if any
    Ok(accounts.into_iter().next())
}

pub async fn get_writer_account(client: &RpcClient, writer_address: &Pubkey) -> Result<(Writer, Pubkey)> {
    let account_bytes = get_account(client, writer_address).await?;
    let account: Account = deserialize(&account_bytes)?;
    let account = Writer::unpack(&account.data)
        .map_err(|e| anyhow!("Failed to unpack writer account: {}", e))
        .copied()?;
    Ok((account, *writer_address))
}

pub async fn get_miner_account(client: &RpcClient, miner_address: &Pubkey) -> Result<(Miner, Pubkey)> {
    let account_bytes = get_account(client, miner_address).await?;
    let account: Account = deserialize(&account_bytes)?;
    let account = Miner::unpack(&account.data)
        .map_err(|e| anyhow!("Failed to unpack miner account: {}", e))
        .copied()?;
    Ok((account, *miner_address))
}

pub async fn get_epoch_account(client: &RpcClient) -> Result<(Epoch, Pubkey)> {
    let (epoch_address, _bump) = epoch_pda();
    let account_bytes = get_account(client, &epoch_address).await?;
    let account: Account = deserialize(&account_bytes)?;
    let account = Epoch::unpack(&account.data)
        .map_err(|e| anyhow!("Failed to unpack epoch account: {}", e))
        .copied()?;
    Ok((account, epoch_address))
}

pub async fn get_block_account(client: &RpcClient) -> Result<(Block, Pubkey)> {
    let (block_address, _bump) = block_pda();
    let account_bytes = get_account(client, &block_address).await?;
    let account: Account = deserialize(&account_bytes)?;
    let account = Block::unpack(&account.data)
        .map_err(|e| anyhow!("Failed to unpack block account: {}", e))
        .copied()?;
    Ok((account, block_address))
}

pub async fn get_archive_account(client: &RpcClient) -> Result<(Archive, Pubkey)> {
    let (archive_address, _bump) = archive_pda();
    let account_bytes = get_account(client, &archive_address).await?;
    let account: Account = deserialize(&account_bytes)?;
    let account = Archive::unpack(&account.data)
        .map_err(|e| anyhow!("Failed to unpack archive account: {}", e))
        .copied()?;
    Ok((account, archive_address))
}
