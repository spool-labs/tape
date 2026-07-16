//! SOL and TAPE balance readers shared by the client and the CLIs.

use rpc::{Rpc, RpcError};
use rpc_client::RpcClient;
use solana_program::program_pack::Pack;
use tape_api::utils::ata;
use tape_core::types::coin::{SOL, TAPE};
use tape_crypto::prelude::Address;

use crate::error::TapedriveError;

/// The SOL balance of an account in lamports. A missing account reads as zero.
pub async fn sol_balance_of<Blockchain: Rpc>(
    rpc: &RpcClient<Blockchain>,
    address: &Address,
) -> Result<SOL, TapedriveError> {
    match rpc.rpc().get_account(address).await {
        Ok(account) => Ok(SOL(account.lamports)),
        Err(RpcError::AccountNotFound(_)) => Ok(SOL(0)),
        Err(error) => Err(error.into()),
    }
}

/// The TAPE balance of an owner in flux, read from its associated token
/// account. A missing token account reads as zero.
pub async fn tape_balance_of<Blockchain: Rpc>(
    rpc: &RpcClient<Blockchain>,
    owner: &Address,
) -> Result<TAPE, TapedriveError> {
    match rpc.rpc().get_account(&ata(owner)).await {
        Ok(account) => spl_token::state::Account::unpack(&account.data)
            .map(|token_account| TAPE(token_account.amount))
            .map_err(|error| TapedriveError::Encoding(format!("token account: {error}"))),
        Err(RpcError::AccountNotFound(_)) => Ok(TAPE(0)),
        Err(error) => Err(error.into()),
    }
}
