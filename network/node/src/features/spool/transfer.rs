//! Transfer is the method that copies our whole slice from a node that owned
//! the spool before us. A node keeps a spool's slices for 4 epochs after it
//! loses the spool, and we try this method first because it reads the fewest
//! bytes, one slice for each track.
//!
//! ```text
//! ask one of the spool's past owners for our slice of each track in the fetch
//! keep each slice that matches its hash in the track's encoding
//! write the slices that we don't have yet
//! ```

use std::sync::Arc;
use std::time::Duration;

use tape_core::erasure::leaf_position;
use tape_core::types::StorageUnits;
use tape_crypto::address::Address;
use tape_protocol::api::{SyncSliceEntry, SyncSlicesReq, SyncSlicesRes};
use tape_protocol::Api;
use tape_recover::attempt::AttemptFailure;
use tape_recover::fetch::{FetchReq, FetchRes};
use tape_rpc::Rpc;
use tape_store::TapeStore;
use tape_store_api::Store;
use tracing::warn;

use crate::context::NodeContext;
use crate::features::spool::fetch::{
    get_call_failure, get_call_outcome, is_slice_of, send_with_time_limit,
    write_slice_once,
};

/// Run one transfer fetch to its one holder
pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    req: FetchReq,
    limit: Duration,
) -> FetchRes {
    let mut res = FetchRes::new(req);
    let Some(peer) = res.req.peers.first().copied() else {
        return res;
    };

    let request = build_transfer_request(&res.req);
    let call = context.api.sync_slices(peer.peer, &request);
    let (response, elapsed) = send_with_time_limit(limit, call).await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            let outcome = get_call_outcome(&error, elapsed);
            res.add_failed_call(peer.peer, outcome, get_call_failure(&error));
            return res;
        }
    };

    let slices = check_slices(&res.req, response);
    let written = write_slices(&context.store, &res.req, &slices);
    add_transfer_metrics(&context, &slices, &written);
    let is_wrong = !slices.wrong.is_empty();
    res.add_peer_answer(peer.peer, slices.bytes, elapsed, is_wrong);
    res.add_track_results(&written, Ok(()));
    res.add_track_results(&slices.wrong, Err(AttemptFailure::WrongData));
    res.add_track_results(&slices.not_held, Err(AttemptFailure::NotHeld));
    res
}

/// Ask for the fetch's tracks' slices at our live spool
fn build_transfer_request(req: &FetchReq) -> SyncSlicesReq {
    SyncSlicesReq {
        spool_index: req.spool,
        live_groups: req.live_groups,
        tracks: req.get_addresses(),
        max_bytes: req.bytes,
    }
}

/// A slice passes when it matches our position's leaf in the encoding
fn check_slices(req: &FetchReq, response: SyncSlicesRes) -> ReceivedSlices {
    let position = leaf_position(req.spool);
    let mut slices = ReceivedSlices::default();
    for entry in response.entries {
        let address = Address::new(entry.track_address);
        let Some(track) = req.get_track(address) else {
            continue;
        };
        let size = StorageUnits(entry.slice_data.len() as u64);
        slices.bytes = slices.bytes.saturating_add(size);
        if !is_slice_of(track, position, &entry.slice_data) {
            slices.wrong.push(address);
            continue;
        }
        slices.good.push(entry);
    }
    for address in response.missing {
        if req.get_track(address).is_some() {
            slices.not_held.push(address);
        }
    }
    slices
}

/// Write each slice that passed, unless one is already on disk, giving the
/// tracks written
fn write_slices<Db: Store>(
    store: &TapeStore<Db>,
    req: &FetchReq,
    slices: &ReceivedSlices,
) -> Vec<Address> {
    let mut written = Vec::with_capacity(slices.good.len());
    for entry in &slices.good {
        let track = Address::new(entry.track_address);
        let Some(fetched) = req.get_track(track) else {
            continue;
        };
        let spool = fetched.missing_track.spool;
        let slice = &entry.slice_data;
        match write_slice_once(store, spool, track, slice) {
            Ok(()) => written.push(track),
            Err(error) => warn!(%track, %error, "spool: slice write failed"),
        }
    }
    written
}

fn add_transfer_metrics<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    slices: &ReceivedSlices,
    written: &[Address],
) {
    let mut persisted = 0;
    for entry in &slices.good {
        let track = Address::new(entry.track_address);
        if written.contains(&track) {
            persisted += entry.slice_data.len() as u64;
        }
    }
    let metrics = &context.metrics;
    metrics.add_sync_fetched(slices.bytes.as_u64());
    metrics.add_sync_persisted(persisted);
    metrics.add_sync_stored(written.len() as u64);
    metrics.add_sync_missing(slices.not_held.len() as u64);
    metrics.add_sync_invalid(slices.wrong.len() as u64);
}

/// What one holder sent, sorted by the check
#[derive(Debug, Default)]
struct ReceivedSlices {
    good: Vec<SyncSliceEntry>,
    wrong: Vec<Address>,
    not_held: Vec<Address>,
    bytes: StorageUnits,
}
