use std::path::{Path, PathBuf};

use tape_api::consts::NAME_LENGTH;
use tape_api::program::prelude::TapeError;
use tape_api::instruction::{
    build_advance_pool_ix, build_join_network_ix, build_register_node_ix,
    build_set_network_address_ix, build_stake_with_pool_ix,
};
use tape_api::program::tapedrive::node_pda;
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::network::NetworkAddress;
use tape_core::types::BasisPoints;
use tape_sdk::keys::helpers::{load_bls_keypair, load_ed25519_keypair};

use crate::context::Context;
use crate::error::{as_tape_error, Error, Result};

/// `AccountAlreadyInitialized` comes from the Solana runtime (not tapedrive),
/// so it's not a `TapeError` — we still need a string check for this one.
fn is_already_initialized_runtime(e: &rpc::RpcError) -> bool {
    let s = format!("{e:?}");
    s.contains("AccountAlreadyInitialized")
        || s.contains("already initialized")
        || s.contains("Account already initialized")
        || s.contains("requires an uninitialized account")
}

pub struct RegisterParams {
    pub name: String,
    pub identity_path: PathBuf,
    pub bls_path: PathBuf,
    pub tls_path: PathBuf,
    pub address: String,
    pub commission_bp: u16,
}

/// Register a node on-chain using its own identity keypair as fee payer.
///
/// The identity keypair must hold enough devnet SOL to cover the tx; this is
/// the job of `treasury fund` upstream. The node account and history PDA are
/// created as part of this instruction.
pub async fn register(ctx: &Context, params: RegisterParams) -> Result<()> {
    let identity = load_ed25519_keypair(&params.identity_path)
        .map_err(|e| Error::Keypair(e.to_string()))?;
    let bls = load_bls_keypair(&params.bls_path)
        .map_err(|e| Error::Keypair(e.to_string()))?;
    let tls = load_ed25519_keypair(&params.tls_path)
        .map_err(|e| Error::Keypair(e.to_string()))?;

    let bls_pubkey = bls.public_key().map_err(|e| Error::Bls(format!("{e:?}")))?;
    let bls_pop = bls
        .proof_of_possession()
        .map_err(|e| Error::Bls(format!("{e:?}")))?;

    let network_address = NetworkAddress::from(&params.address)
        .map_err(|_| Error::Address(params.address.clone()))?;

    let name_bytes = pack_name(&params.name)?;

    let authority = identity.pubkey().into();
    let tls_pubkey = tls.pubkey().into();
    let ix = build_register_node_ix(
        authority,
        authority,
        name_bytes,
        BasisPoints(params.commission_bp as u64),
        network_address,
        tls_pubkey,
        bls_pubkey,
        bls_pop,
    );

    match ctx.rpc.send_instructions(&identity, vec![ix]).await {
        Ok(_) => Ok(()),
        Err(e) if is_already_initialized_runtime(&e) => {
            tracing::info!("register skipped (node already registered)");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// Join the network as a registered node, making the node eligible for the
/// next committee assignment. Called after `register`.
///
/// Tolerated errors:
/// - `UnexpectedState` — node already in `committee_next`
/// - `NodeStale` — pool hasn't advanced to the current epoch yet; the
///   node's own lifecycle task will retry join_network after advance_pool
///   succeeds
pub async fn join_network(ctx: &Context, identity_path: &Path) -> Result<()> {
    let identity = load_ed25519_keypair(identity_path)
        .map_err(|e| Error::Keypair(e.to_string()))?;
    let authority = identity.pubkey().into();
    let (node_address, _) = node_pda(authority);
    let ix = build_join_network_ix(authority, authority, node_address);
    match ctx.rpc.send_instructions(&identity, vec![ix]).await {
        Ok(_) => Ok(()),
        Err(e) => match as_tape_error(&e) {
            Some(TapeError::UnexpectedState) => {
                tracing::info!("join_network skipped (likely already joined)");
                Ok(())
            }
            Some(TapeError::NodeStale) => {
                tracing::info!(
                    "join_network skipped (node stale; waits on pool advance, node lifecycle will retry)"
                );
                Ok(())
            }
            _ => Err(e.into()),
        },
    }
}

/// Stake `amount_flux` TAPE (in flux units) with the node's pool. The caller
/// is the node identity acting as both fee-payer and stake authority.
pub async fn stake(ctx: &Context, identity_path: &Path, amount_flux: u64) -> Result<()> {
    let identity = load_ed25519_keypair(identity_path)
        .map_err(|e| Error::Keypair(e.to_string()))?;
    let authority = identity.pubkey().into();
    let (node_address, _) = node_pda(authority);
    let amount: Coin<TAPE> = Coin::from(amount_flux);
    let ix = build_stake_with_pool_ix(authority, authority, node_address, amount);
    match ctx.rpc.send_instructions(&identity, vec![ix]).await {
        Ok(_) => Ok(()),
        Err(e) if is_already_initialized_runtime(&e) => {
            tracing::info!("stake skipped (stake account already exists)");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// Update the node's on-chain `network_address` to `address` (IP:PORT). Used
/// when a node is resurrected on a new droplet and its peers need to find it
/// at the new IP. Idempotent: overwriting with the same value is a no-op.
pub async fn set_address(ctx: &Context, identity_path: &Path, address: &str) -> Result<()> {
    let identity = load_ed25519_keypair(identity_path)
        .map_err(|e| Error::Keypair(e.to_string()))?;
    let authority = identity.pubkey().into();
    let (node_address, _) = node_pda(authority);
    let network_address = NetworkAddress::from(address)
        .map_err(|_| Error::Address(address.to_string()))?;
    let ix = build_set_network_address_ix(authority, authority, node_address, network_address);
    ctx.rpc.send_instructions(&identity, vec![ix]).await?;
    Ok(())
}

/// Advance the node's pool to the current epoch. Tolerates
/// `TapeError::AlreadyAdvanced` (pool already at current epoch) and
/// `TapeError::BadEpochState` (epoch hasn't finished its snapshot phase
/// yet — node lifecycle will retry on its own schedule).
pub async fn advance_pool(ctx: &Context, identity_path: &Path) -> Result<()> {
    let identity = load_ed25519_keypair(identity_path)
        .map_err(|e| Error::Keypair(e.to_string()))?;
    let authority = identity.pubkey().into();
    let (node_address, _) = node_pda(authority);
    let ix = build_advance_pool_ix(authority, authority, node_address);
    match ctx.rpc.send_instructions(&identity, vec![ix]).await {
        Ok(_) => Ok(()),
        Err(e) => match as_tape_error(&e) {
            Some(TapeError::AlreadyAdvanced) => {
                tracing::info!("advance_pool skipped (already advanced)");
                Ok(())
            }
            Some(TapeError::BadEpochState) => {
                tracing::info!(
                    "advance_pool skipped (epoch state not ready; node lifecycle will retry)"
                );
                Ok(())
            }
            _ => Err(e.into()),
        },
    }
}

fn pack_name(name: &str) -> Result<[u8; NAME_LENGTH]> {
    let src = name.as_bytes();
    if src.is_empty() {
        return Err(Error::Invalid("name must not be empty".into()));
    }
    if src.len() > NAME_LENGTH {
        return Err(Error::Invalid(format!(
            "name exceeds {NAME_LENGTH} bytes"
        )));
    }
    let mut out = [0u8; NAME_LENGTH];
    out[..src.len()].copy_from_slice(src);
    Ok(out)
}
