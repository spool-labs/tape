//! SOL and TAPE balance readers shared by the client and the CLIs.

use std::ops::Range;

use tape_rpc::{Rpc, RpcError};
use tape_rpc_client::RpcClient;
use solana_program::program_pack::Pack;
use solana_program::rent::Rent;
use solana_program::sysvar::rent::ID as RENT_SYSVAR;
use tape_api::utils::ata;
use tape_core::types::coin::{SOL, TAPE};
use tape_crypto::prelude::Address;

use crate::error::TapedriveError;

const RENT_RATE_BYTES: Range<usize> = 0..8;
const RENT_THRESHOLD_BYTES: Range<usize> = 8..16;
const RENT_BURN_BYTE: usize = 16;

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

/// The cluster's current rent schedule, read from the rent sysvar.
pub async fn get_rent<Blockchain: Rpc>(rpc: &RpcClient<Blockchain>) -> Result<Rent, TapedriveError> {
    let account = rpc.rpc().get_account(&RENT_SYSVAR.into()).await?;
    parse_rent(&account.data)
}

/// Decode rent sysvar account data.
pub fn parse_rent(data: &[u8]) -> Result<Rent, TapedriveError> {
    let lamports_per_byte = rent_field(data, RENT_RATE_BYTES)?;
    let exemption_threshold = rent_field(data, RENT_THRESHOLD_BYTES)?;
    let burn_percent = data
        .get(RENT_BURN_BYTE)
        .copied()
        .ok_or_else(|| TapedriveError::Encoding("rent sysvar too short".to_string()))?;

    // The threshold and burn fields are deprecated but still decide the
    // exempt minimum on clusters that have not adopted SIMD-0194.
    #[allow(deprecated)]
    let rent = Rent {
        lamports_per_byte: u64::from_le_bytes(lamports_per_byte),
        exemption_threshold,
        burn_percent,
    };
    Ok(rent)
}

/// The lamports that keep an account of `data_len` bytes rent exempt.
pub fn rent_exempt_minimum(rent: &Rent, data_len: usize) -> Result<SOL, TapedriveError> {
    rent.try_minimum_balance(data_len)
        .map(SOL)
        .ok_or_else(|| TapedriveError::InvalidArgument(format!("account of {data_len} bytes is too large")))
}

fn rent_field(data: &[u8], range: Range<usize>) -> Result<[u8; 8], TapedriveError> {
    data.get(range)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| TapedriveError::Encoding("rent sysvar too short".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // devnet's rent sysvar reproduces the minimums the cluster reports
    #[test]
    fn devnet_rent() {
        let data = hex::decode("d813000000000000000000000000f03f32").expect("rent bytes");

        let rent = parse_rent(&data).expect("rent");

        assert_eq!(rent_exempt_minimum(&rent, 165).expect("token account").lamports(), 1_488_440);
        assert_eq!(rent_exempt_minimum(&rent, 688).expect("tape account").lamports(), 4_145_280);
    }

    // truncated sysvar data is an encoding error, not a zero rate
    #[test]
    fn short_data() {
        assert!(parse_rent(&[0u8; 16]).is_err());
    }
}
