use std::path::Path;

use rpc::Rpc;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::system_instruction;
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::utils::ata;
use tape_core::types::coin::{Coin, TAPE};
use tape_crypto::address::Address;
use tape_sdk::keys::helpers::load_ed25519_keypair;
use tracing::info;

use crate::context::Context;
use crate::error::{Error, Result};

/// SPL token account layout: mint (32) + owner (32) + amount (8, LE).
const TOKEN_ACCOUNT_AMOUNT_OFFSET: usize = 64;

/// Disburse SOL (in lamports) and TAPE (in flux, the smallest unit) from the
/// payer to each recipient so each ends up with at least `lamports_each`
/// lamports and `flux_each` flux.
///
/// Balance-aware: each recipient's current SOL balance and ATA balance are
/// queried first. If already at or above target, the corresponding transfer
/// is skipped. Otherwise, the *difference* is transferred (so a partially-
/// funded wallet only costs the top-up, not another full payout). This
/// makes re-running bootstrap across crash cycles safe.
///
/// Each recipient gets its own transaction (SOL transfer + ATA-create-
/// idempotent + TAPE transfer) so a single rebalance failure doesn't roll
/// back others.
pub async fn fund(
    ctx: &Context,
    recipients: &[Pubkey],
    lamports_each: u64,
    flux_each: u64,
) -> Result<()> {
    let payer_addr: Address = ctx.payer.pubkey().into();
    let payer_pk: Pubkey = payer_addr.into();

    for recipient in recipients {
        let mut ixs: Vec<Instruction> = Vec::new();
        let recipient_addr = Address::from(recipient.to_bytes());

        if lamports_each > 0 {
            let current = get_sol_balance(ctx, &recipient_addr).await?;
            if current < lamports_each {
                let delta = lamports_each - current;
                info!(
                    recipient = %recipient,
                    current,
                    target = lamports_each,
                    delta,
                    "topping up SOL"
                );
                ixs.push(system_instruction::transfer(&payer_pk, recipient, delta));
            } else {
                info!(recipient = %recipient, current, "SOL target met; skipping");
            }
        }

        if flux_each > 0 {
            let current = get_tape_balance(ctx, &recipient_addr).await?;
            if current < flux_each {
                let delta = flux_each - current;
                info!(
                    recipient = %recipient,
                    current,
                    target = flux_each,
                    delta,
                    "topping up TAPE"
                );
                let amount: Coin<TAPE> = Coin::from(delta);
                let tape_ixs =
                    build_authority_with_tokens_ix(payer_addr, recipient_addr, amount)
                        .map_err(|e| Error::Other(format!("tape transfer build: {e}")))?;
                ixs.extend(tape_ixs);
            } else {
                info!(recipient = %recipient, current, "TAPE target met; skipping");
            }
        }

        if ixs.is_empty() {
            continue;
        }

        ctx.rpc.send_instructions(&ctx.payer, ixs).await?;
    }
    Ok(())
}

async fn get_sol_balance(ctx: &Context, addr: &Address) -> Result<u64> {
    match ctx.rpc.rpc().get_account(addr).await {
        Ok(account) => Ok(account.lamports),
        Err(rpc::RpcError::AccountNotFound(_)) => Ok(0),
        Err(e) => Err(e.into()),
    }
}

async fn get_tape_balance(ctx: &Context, authority: &Address) -> Result<u64> {
    let ata_addr = ata(authority);
    match ctx.rpc.rpc().get_account(&ata_addr).await {
        Ok(account) => {
            let data = &account.data;
            if data.len() < TOKEN_ACCOUNT_AMOUNT_OFFSET + 8 {
                return Err(Error::Other(format!(
                    "TAPE ATA data too short: {} bytes",
                    data.len()
                )));
            }
            let mut amount_bytes = [0u8; 8];
            amount_bytes.copy_from_slice(
                &data[TOKEN_ACCOUNT_AMOUNT_OFFSET..TOKEN_ACCOUNT_AMOUNT_OFFSET + 8],
            );
            Ok(u64::from_le_bytes(amount_bytes))
        }
        Err(rpc::RpcError::AccountNotFound(_)) => Ok(0),
        Err(e) => Err(e.into()),
    }
}

/// Load ed25519 identity keypair files and return their pubkeys in the same
/// order. Convenience for callers that just want to feed a list of node
/// key-dirs into `fund`.
pub fn load_pubkeys_from_identities<I, P>(paths: I) -> Result<Vec<Pubkey>>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    let mut out = Vec::new();
    for path in paths {
        let kp = load_ed25519_keypair(path.as_ref())
            .map_err(|e| Error::Keypair(e.to_string()))?;
        let address: Address = kp.pubkey().into();
        out.push(address.into());
    }
    Ok(out)
}

/// Parse a file of one pubkey per line. Blank lines and `#` comments are
/// ignored.
pub fn parse_pubkeys_file(path: &Path) -> Result<Vec<Pubkey>> {
    let contents = std::fs::read_to_string(path).map_err(|source| Error::Io {
        path: path.display().to_string(),
        source,
    })?;

    let mut out = Vec::new();
    for (idx, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let pk: Pubkey = line
            .parse()
            .map_err(|e| Error::Invalid(format!("line {}: {}: {e}", idx + 1, line)))?;
        out.push(pk);
    }
    Ok(out)
}
