//! Sync is the method that gets a small inline track's bytes from a peer. A
//! node keeps those bytes only for the groups it owns, so it has to fetch
//! them for every inline track on a spool it gains.
//!
//! ```text
//! ask one peer for the bytes of each inline track in the fetch
//! keep each payload whose hash matches the track's value hash
//! write the payloads that we don't have yet
//! ```

use std::sync::Arc;
use std::time::Duration;

use tape_core::track::data::BlobData;
use tape_core::types::StorageUnits;
use tape_crypto::address::Address;
use tape_crypto::hash::hash;
use tape_protocol::api::{
    SyncTrackDataEntry, SyncTrackDataReq, SyncTrackDataRes,
};
use tape_protocol::Api;
use tape_recover::attempt::AttemptFailure;
use tape_recover::fetch::{FetchReq, FetchRes};
use tape_rpc::Rpc;
use tape_store::error::TapeStoreError;
use tape_store::ops::TrackDataOps;
use tape_store::TapeStore;
use tape_store_api::Store;
use tracing::warn;

use crate::context::NodeContext;
use crate::features::spool::fetch::{
    get_call_failure, get_call_outcome, send_with_time_limit,
};

/// Run one sync fetch to its one peer
pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    req: FetchReq,
    limit: Duration,
) -> FetchRes {
    let mut res = FetchRes::new(req);
    let Some(peer) = res.req.peers.first().copied() else {
        return res;
    };

    let request = build_sync_request(&res.req);
    let call = context.api.sync_track_data(peer.peer, &request);
    let (response, elapsed) = send_with_time_limit(limit, call).await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            let outcome = get_call_outcome(&error, elapsed);
            res.add_failed_call(peer.peer, outcome, get_call_failure(&error));
            return res;
        }
    };

    let payloads = check_payloads(&res.req, response);
    let written = write_payloads(&context.store, &payloads);
    add_sync_metrics(&context, &payloads, &written);
    let is_wrong = !payloads.wrong.is_empty();
    res.add_peer_answer(peer.peer, payloads.bytes, elapsed, is_wrong);
    res.add_track_results(&written, Ok(()));
    res.add_track_results(&payloads.wrong, Err(AttemptFailure::WrongData));
    let not_held = Err(AttemptFailure::NotHeld);
    res.add_track_results(&payloads.not_held, not_held);
    res
}

fn build_sync_request(req: &FetchReq) -> SyncTrackDataReq {
    SyncTrackDataReq {
        tracks: req.get_addresses(),
        max_bytes: req.bytes,
    }
}

/// A payload passes when it hashes to the track's value hash
fn check_payloads(
    req: &FetchReq,
    response: SyncTrackDataRes,
) -> ReceivedPayloads {
    let mut payloads = ReceivedPayloads::default();
    for entry in response.entries {
        let Some(track) = req.get_track(entry.track) else {
            continue;
        };
        let size = StorageUnits(entry.payload.len() as u64);
        payloads.bytes = payloads.bytes.saturating_add(size);
        if hash(&entry.payload) != track.track.value_hash {
            payloads.wrong.push(entry.track);
            continue;
        }
        payloads.good.push(entry);
    }
    for address in response.missing {
        if req.get_track(address).is_some() {
            payloads.not_held.push(address);
        }
    }
    payloads
}

/// Write each payload that passed and isn't on disk yet, giving the tracks
/// written
fn write_payloads<Db: Store>(
    store: &TapeStore<Db>,
    payloads: &ReceivedPayloads,
) -> Vec<Address> {
    let mut written = Vec::with_capacity(payloads.good.len());
    for entry in &payloads.good {
        let track = entry.track;
        match write_payload_once(store, entry) {
            Ok(()) => written.push(track),
            Err(error) => warn!(%track, %error, "spool: sync write failed"),
        }
    }
    written
}

fn write_payload_once<Db: Store>(
    store: &TapeStore<Db>,
    entry: &SyncTrackDataEntry,
) -> Result<(), TapeStoreError> {
    if store.has_track_data(entry.track)? {
        return Ok(());
    }
    let data = BlobData::Inline(entry.payload.clone());
    store.put_track_data(entry.track, data)
}

fn add_sync_metrics<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    payloads: &ReceivedPayloads,
    written: &[Address],
) {
    let mut persisted = 0;
    for entry in &payloads.good {
        if written.contains(&entry.track) {
            persisted += entry.payload.len() as u64;
        }
    }
    let metrics = &context.metrics;
    metrics.add_sync_fetched(payloads.bytes.as_u64());
    metrics.add_sync_persisted(persisted);
    metrics.add_sync_inline_stored(written.len() as u64);
    metrics.add_sync_inline_missing(payloads.not_held.len() as u64);
    metrics.add_sync_invalid(payloads.wrong.len() as u64);
}

/// What one sync_peer sent, sorted by the check
#[derive(Debug, Default)]
struct ReceivedPayloads {
    good: Vec<SyncTrackDataEntry>,
    wrong: Vec<Address>,
    not_held: Vec<Address>,
    bytes: StorageUnits,
}

#[cfg(test)]
mod tests {
    use tape_core::spooler::VirtualGroup;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::EpochNumber;
    use tape_crypto::Hash;
    use tape_recover::attempt::Method;
    use tape_recover::fetch::FetchTrack;
    use tape_recover::track::MissingTrack;
    use tape_recover::time::WallTime;

    use super::*;

    fn inline(byte: u8, payload: &[u8]) -> FetchTrack {
        let track = CompressedTrack {
            tape: Address::default(),
            tape_number: Default::default(),
            key: Hash::default(),
            track_number: Default::default(),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            flags: 0,
            size: StorageUnits(payload.len() as u64),
            group: VirtualGroup(0),
            value_hash: hash(payload),
        };
        let spool = VirtualGroup(0).spool_at(0);
        let address = Address::new([byte; 32]);
        let added = WallTime(1);
        let missing_track = MissingTrack::new_handover(spool, address, added);
        let holder_epoch = EpochNumber(0);
        FetchTrack { missing_track, track, encoding: None, holder_epoch }
    }

    // a payload passes on its value hash, a wrong one fails, and a missing
    // one is not held
    #[test]
    fn checks_payloads() {
        let spool = VirtualGroup(0).spool_at(0);
        let mut req = FetchReq::new(Method::Sync, spool, 1, Vec::new());
        for (byte, payload) in [(1, b"good"), (2, b"lost"), (3, b"none")] {
            req.add_track(inline(byte, payload), StorageUnits(4));
        }
        let good = b"good".to_vec();
        let evil = b"evil".to_vec();
        let entries = vec![
            SyncTrackDataEntry { track: Address::new([1; 32]), payload: good },
            SyncTrackDataEntry { track: Address::new([2; 32]), payload: evil },
        ];
        let missing = vec![Address::new([3; 32])];

        let response = SyncTrackDataRes { entries, missing };
        let payloads = check_payloads(&req, response);

        assert_eq!(payloads.good.len(), 1);
        assert_eq!(payloads.wrong, vec![Address::new([2; 32])]);
        assert_eq!(payloads.not_held, vec![Address::new([3; 32])]);
        assert_eq!(payloads.bytes, StorageUnits(8));
    }
}
