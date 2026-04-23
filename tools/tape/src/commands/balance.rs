//! `tape balance` — print SOL + TAPE balance of the active wallet.

use rpc::Rpc;
use serde::Serialize;
use tape_api::utils::ata;
use tape_crypto::address::Address;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::output::CliOutput;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
const TAPE_UNIT: f64 = 1_000_000.0;
const TOKEN_ACCOUNT_AMOUNT_OFFSET: usize = 64;

#[derive(Serialize)]
pub struct BalanceOutput {
    pub pubkey: String,
    pub rpc_url: String,
    pub lamports: u64,
    pub sol: f64,
    pub flux: u64,
    pub tape: f64,
}

impl CliOutput for BalanceOutput {
    fn print_text(&self) {
        println!("pubkey:  {}", self.pubkey);
        println!("rpc:     {}", self.rpc_url);
        println!("sol:     {:.9}  ({} lamports)", self.sol, self.lamports);
        println!("tape:    {:.6}  ({} flux)", self.tape, self.flux);
    }
}

pub async fn run(ctx: &Context) -> Result<BalanceOutput> {
    let rpc = ctx.rpc_client()?;
    let wallet: Address = ctx.payer.pubkey().into();

    let lamports = match rpc.rpc().get_account(&wallet).await {
        Ok(acc) => acc.lamports,
        Err(rpc::RpcError::AccountNotFound(_)) => 0,
        Err(e) => return Err(Error::Rpc(e)),
    };

    let ata_addr = ata(&wallet);
    let flux = match rpc.rpc().get_account(&ata_addr).await {
        Ok(acc) => {
            if acc.data.len() < TOKEN_ACCOUNT_AMOUNT_OFFSET + 8 {
                0
            } else {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(
                    &acc.data[TOKEN_ACCOUNT_AMOUNT_OFFSET..TOKEN_ACCOUNT_AMOUNT_OFFSET + 8],
                );
                u64::from_le_bytes(bytes)
            }
        }
        Err(rpc::RpcError::AccountNotFound(_)) => 0,
        Err(e) => return Err(Error::Rpc(e)),
    };

    Ok(BalanceOutput {
        pubkey: wallet.to_string(),
        rpc_url: ctx.rpc_url.clone(),
        lamports,
        sol: lamports as f64 / LAMPORTS_PER_SOL,
        flux,
        tape: flux as f64 / TAPE_UNIT,
    })
}
