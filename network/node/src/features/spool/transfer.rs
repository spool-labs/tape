//! Transfer: our slice whole from a holder
//!
//! Build the request, send it, check what came back, write it, report.

use std::sync::Arc;
use std::time::Duration;

use tape_core::erasure::leaf_position;
use tape_core::types::{SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_protocol::api::{SyncSlicesReq, SyncSlicesRes};
use tape_protocol::Api;
use tape_rpc::Rpc;
use tape_store::error::TapeStoreError;
use tape_store::ops::SliceOps;
use tape_store_api::Store;
use tracing::warn;

use crate::context::NodeContext;
use crate::features::spool::fetch::{
    send, Checked, Fetch, FetchReport, FetchTrack, Fetched, Written,
};

/// Run one transfer fetch to its one holder
pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    fetch: Fetch,
    limit: Duration,
) -> FetchReport {
    let mut report = FetchReport::new(fetch);
    let Some(helper) = report.fetch.peers.first().copied() else {
        return report;
    };

    let request = build_request(&report.fetch, report.fetch.spool);
    let call = ctx.api.sync_slices(helper.peer, &request);
    let answer = send(limit, call).await;
    let response = match answer.response {
        Ok(response) => response,
        Err(failure) => {
            report.failed_call(helper.peer, failure, answer.elapsed);
            return report;
        }
    };

    let position = leaf_position(report.fetch.spool);
    let mut checked = check(&report.fetch, position, response);
    let written = write(&ctx, &mut checked);
    count(&ctx, &checked, &written);
    report.answered(helper.peer, &checked, &written.keys, answer.elapsed);
    report
}

/// Ask for the fetch's tracks' slices at the live spool `spool`
pub fn build_request(fetch: &Fetch, spool: SpoolIndex) -> SyncSlicesReq {
    SyncSlicesReq {
        spool_index: spool,
        live_groups: fetch.live_groups,
        tracks: fetch.addresses(),
        max_bytes: fetch.bytes,
    }
}

/// A slice passes when it matches its leaf in the track's encoding
pub fn check(
    fetch: &Fetch,
    position: SpoolIndex,
    response: SyncSlicesRes,
) -> Checked {
    let mut checked = Checked::default();
    for entry in response.entries {
        let address = Address::new(entry.track_address);
        let Some(track) = fetch.track(address) else {
            continue;
        };
        let size = StorageUnits(entry.slice_data.len() as u64);
        checked.received = checked.received.saturating_add(size);
        if !is_slice_of(track, position, &entry.slice_data) {
            checked.wrong.push(track.key);
            continue;
        }
        checked.good.push(Fetched { key: track.key, bytes: entry.slice_data });
    }
    for address in response.missing {
        if let Some(track) = fetch.track(address) {
            checked.not_held.push(track.key);
        }
    }
    checked
}

/// Whether bytes are the slice at `position` of a track
pub fn is_slice_of(
    track: &FetchTrack,
    position: SpoolIndex,
    bytes: &[u8],
) -> bool {
    match &track.blob {
        Some(blob) => blob.verify_slice(position, bytes),
        None => false,
    }
}

/// Write each slice that passed, unless one is already on disk
pub fn write<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    checked: &mut Checked,
) -> Written {
    let mut written = Written::default();
    for fetched in std::mem::take(&mut checked.good) {
        let key = fetched.key;
        let size = StorageUnits(fetched.bytes.len() as u64);
        let track = key.track;
        match write_slice(ctx, key.spool, track, fetched.bytes) {
            Ok(()) => written.add(key, size),
            Err(error) => warn!(%track, %error, "spool: slice write failed"),
        }
    }
    written
}

/// Write a checked slice, never over one already on disk
pub fn write_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    track: Address,
    bytes: Vec<u8>,
) -> Result<(), TapeStoreError> {
    if ctx.store.has_slice(spool, track)? {
        return Ok(());
    }
    ctx.store.put_slice(spool, track, bytes)
}

fn count<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    checked: &Checked,
    written: &Written,
) {
    let metrics = &ctx.metrics;
    metrics.add_sync_fetched(checked.received.as_u64());
    metrics.add_sync_persisted(written.bytes.as_u64());
    metrics.add_sync_stored(written.keys.len() as u64);
    metrics.add_sync_missing(checked.not_held.len() as u64);
    metrics.add_sync_invalid(checked.wrong.len() as u64);
}
