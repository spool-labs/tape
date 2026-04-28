//! Chain-level genesis: create the tapedrive `System`, expand it to full size,
//! and initialize archive/epoch PDAs. Mirrors the sequence in
//! `e2e/devnet/src/simnet.rs::init_chain`.

use rpc::Rpc;
use tape_api::instruction::{
    build_create_system_ix, build_expand_system_ix, build_initialize_ix,
};
use tape_api::program::tapedrive::system_pda;
use tape_api::program::token::MINT_ADDRESS;
use tape_crypto::address::Address;

use crate::context::Context;
use crate::error::{Error, Result};

/// Verify that the on-chain cluster has been initialized: the tapedrive
/// `System` PDA and the TAPE mint must both exist. Bootstrap requires this
/// because the node + cache services read those accounts at startup. Run
/// `tape-network genesis init` to create them.
pub async fn ensure_initialized(ctx: &Context) -> Result<()> {
    let (system_addr, _) = system_pda();
    require_account(ctx, &system_addr, "System PDA").await?;
    require_account(ctx, &MINT_ADDRESS, "TAPE mint").await?;
    Ok(())
}

async fn require_account(ctx: &Context, addr: &Address, label: &str) -> Result<()> {
    match ctx.rpc.rpc().get_account(addr).await {
        Ok(_) => Ok(()),
        Err(rpc::RpcError::AccountNotFound(_)) => Err(Error::Invalid(format!(
            "{label} ({addr}) not found — run `tape-network genesis init` after `genesis deploy-programs`"
        ))),
        Err(e) => Err(e.into()),
    }
}

/// Run the full chain init sequence. Each step is idempotent: failures due to
/// the relevant account already being initialized are downgraded to warnings.
pub async fn init_all(ctx: &Context) -> Result<()> {
    let admin: Address = ctx.payer.pubkey().into();

    try_send(ctx, "create_system", build_create_system_ix(admin, admin)).await?;

    // ExpandSystem is called repeatedly until it errors with "already
    // initialized" — each call expands the system by one chunk until fully
    // populated. Matches the simnet pattern.
    for _ in 0..64 {
        match send(ctx, vec![build_expand_system_ix(admin, admin)]).await {
            Ok(()) => {}
            Err(e) => {
                if is_already_initialized(&e) {
                    break;
                }
                return Err(e);
            }
        }
    }

    try_send(ctx, "initialize", build_initialize_ix(admin, admin)).await?;

    Ok(())
}

async fn try_send(
    ctx: &Context,
    label: &str,
    ix: solana_sdk::instruction::Instruction,
) -> Result<()> {
    match send(ctx, vec![ix]).await {
        Ok(()) => Ok(()),
        Err(e) if is_already_initialized(&e) => {
            tracing::info!(%label, "skipped (already initialized)");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

async fn send(ctx: &Context, ixs: Vec<solana_sdk::instruction::Instruction>) -> Result<()> {
    ctx.rpc.send_instructions(&ctx.payer, ixs).await?;
    Ok(())
}

/// These init calls can fail either because a Solana-runtime-owned account
/// was already initialized (runtime-level error, string-only) or because the
/// tapedrive program itself returned a typed error. Handle both.
fn is_already_initialized(e: &Error) -> bool {
    let rpc_err = match e {
        Error::Rpc(rpc) => rpc,
        _ => return false,
    };
    if let Some(tape_err) = crate::error::as_tape_error(rpc_err) {
        return matches!(tape_err, tape_api::program::prelude::TapeError::UnexpectedState);
    }
    let s = format!("{rpc_err:?}");
    s.contains("AccountAlreadyInitialized")
        || s.contains("already initialized")
        || s.contains("Account already initialized")
        // ExpandSystem returns this once the system is fully populated: the
        // next slot it wants to touch doesn't yet have an expectable
        // "uninitialized" state because we're past the end.
        || s.contains("requires an uninitialized account")
}
