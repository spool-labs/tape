use anyhow::Result;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::task::JoinSet;
use tape_client::{get_archive_account, find_tape_account};
use crate::store::TapeStore;
use crate::utils::peer;

/// Syncs missing tape addresses from either a trusted peer or Solana RPC.
pub async fn get_tape_addresses(
    store: &Arc<TapeStore>,
    client: &Arc<RpcClient>,
    trusted_peer: Option<String>,
) -> Result<()> {
    log::debug!("Syncing missing tape addresses");
    log::debug!("This may take a while... please be patient");

    if let Some(peer_url) = trusted_peer {
        log::debug!("Using trusted peer: {}", peer_url);
        sync_addresses_from_trusted_peer(store, client, &peer_url).await?;
    } else {
        log::debug!("No trusted peer provided, syncing against Solana directly");
        sync_addresses_from_solana(store, client).await?;
    }

    Ok(())
}

/// Syncs tape addresses from a trusted peer.
pub async fn sync_addresses_from_trusted_peer(
    store: &Arc<TapeStore>,
    client: &Arc<RpcClient>,
    trusted_peer_url: &str,
) -> Result<()> {
    let (archive, _) = get_archive_account(client).await?;
    let total = archive.tapes_stored;
    let http = reqwest::Client::new();
    let mut tasks = JoinSet::new();
    let mut tape_pubkeys_with_numbers = Vec::with_capacity(total as usize);

    for tape_number in 1..=total {
        if store.read_tape_address(tape_number).is_ok() {
            continue;
        }

        if tasks.len() >= 10 {
            if let Some(Ok(Ok((pubkey, number)))) = tasks.join_next().await {
                tape_pubkeys_with_numbers.push((pubkey, number));
            }
        }

        let trusted_peer_url = trusted_peer_url.to_string();
        let http = http.clone();
        tasks.spawn(async move {
            let pubkey = peer::fetch_tape_address(&http, &trusted_peer_url, tape_number).await?;
            Ok((pubkey, tape_number))
        });
    }

    let results: Vec<Result<(Pubkey, u64), anyhow::Error>> = tasks.join_all().await;
    let pairs: Vec<(Pubkey, u64)> = results.into_iter().filter_map(|r| r.ok()).collect();
    tape_pubkeys_with_numbers.extend(pairs.into_iter());

    let (pubkeys, tape_numbers): (Vec<Pubkey>, Vec<u64>) = tape_pubkeys_with_numbers.into_iter().unzip();
    store.write_tapes_batch(&tape_numbers, &pubkeys)?;

    Ok(())
}

/// Syncs tape addresses from Solana RPC.
pub async fn sync_addresses_from_solana(
    store: &Arc<TapeStore>,
    client: &Arc<RpcClient>
    ) -> Result<()> {
    let (archive, _) = get_archive_account(client).await?;
    let total = archive.tapes_stored;
    let mut tasks = JoinSet::new();
    let mut tape_pubkeys_with_numbers = Vec::with_capacity(total as usize);

    for tape_number in 1..=total {
        if store.read_tape_address(tape_number).is_ok() {
            continue;
        }

        if tasks.len() >= 10 {
            if let Some(Ok(Ok((pubkey, number)))) = tasks.join_next().await {
                tape_pubkeys_with_numbers.push((pubkey, number));
            }
        }

        let client = client.clone();
        tasks.spawn(async move {
            let (pubkey, _) = find_tape_account(&client, tape_number)
                .await?
                .ok_or(anyhow::anyhow!("Tape account not found for number {}", tape_number))?;
            Ok((pubkey, tape_number))
        });
    }

    let results: Vec<Result<(Pubkey, u64), anyhow::Error>> = tasks.join_all().await;
    let pairs: Vec<(Pubkey, u64)> = results.into_iter().filter_map(|r| r.ok()).collect();
    tape_pubkeys_with_numbers.extend(pairs.into_iter());

    let (pubkeys, tape_numbers): (Vec<Pubkey>, Vec<u64>) = tape_pubkeys_with_numbers.into_iter().unzip();
    store.write_tapes_batch(&tape_numbers, &pubkeys)?;

    Ok(())
}
