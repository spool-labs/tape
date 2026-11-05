//! Sync: track_data from one of the spool's sync_peers
//!
//! Build the request, send it, check what came back, write it, report.

use std::sync::Arc;
use std::time::Duration;

use tape_core::track::data::BlobData;
use tape_core::types::StorageUnits;
use tape_crypto::address::Address;
use tape_crypto::hash::hash;
use tape_protocol::api::{SyncTrackDataReq, SyncTrackDataRes};
use tape_protocol::Api;
use tape_rpc::Rpc;
use tape_store::error::TapeStoreError;
use tape_store::ops::TrackDataOps;
use tape_store_api::Store;
use tracing::warn;

use crate::context::NodeContext;
use crate::features::spool::fetch::{
    send, Checked, Fetch, FetchReport, Fetched, Written,
};

/// Run one sync fetch to its one peer
pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    fetch: Fetch,
    limit: Duration,
) -> FetchReport {
    let mut report = FetchReport::new(fetch);
    let Some(helper) = report.fetch.peers.first().copied() else {
        return report;
    };

    let request = build_request(&report.fetch);
    let call = ctx.api.sync_track_data(helper.peer, &request);
    let answer = send(limit, call).await;
    let response = match answer.response {
        Ok(response) => response,
        Err(failure) => {
            report.failed_call(helper.peer, failure, answer.elapsed);
            return report;
        }
    };

    let mut checked = check(&report.fetch, response);
    let written = write(&ctx, &mut checked);
    count(&ctx, &checked, &written);
    report.answered(helper.peer, &checked, &written.keys, answer.elapsed);
    report
}

fn build_request(fetch: &Fetch) -> SyncTrackDataReq {
    SyncTrackDataReq {
        tracks: fetch.addresses(),
        max_bytes: fetch.bytes,
    }
}

/// A payload passes when it hashes to the track's value hash
fn check(fetch: &Fetch, response: SyncTrackDataRes) -> Checked {
    let mut checked = Checked::default();
    for entry in response.entries {
        let Some(track) = fetch.track(entry.track) else {
            continue;
        };
        let size = StorageUnits(entry.payload.len() as u64);
        checked.received = checked.received.saturating_add(size);
        if hash(&entry.payload) != track.track.value_hash {
            checked.wrong.push(track.key);
            continue;
        }
        let fetched = Fetched { key: track.key, bytes: entry.payload };
        checked.good.push(fetched);
    }
    for address in response.missing {
        if let Some(track) = fetch.track(address) {
            checked.not_held.push(track.key);
        }
    }
    checked
}

/// Write each payload that passed and isn't on disk yet
fn write<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    checked: &mut Checked,
) -> Written {
    let mut written = Written::default();
    for fetched in std::mem::take(&mut checked.good) {
        let track = fetched.key.track;
        let size = StorageUnits(fetched.bytes.len() as u64);
        let data = BlobData::Inline(fetched.bytes);
        match write_once(ctx, track, data) {
            Ok(()) => written.add(fetched.key, size),
            Err(error) => warn!(%track, %error, "spool: sync write failed"),
        }
    }
    written
}

fn write_once<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    track: Address,
    data: BlobData,
) -> Result<(), TapeStoreError> {
    if ctx.store.has_track_data(track)? {
        return Ok(());
    }
    ctx.store.put_track_data(track, data)
}

fn count<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    checked: &Checked,
    written: &Written,
) {
    let metrics = &ctx.metrics;
    metrics.add_sync_fetched(checked.received.as_u64());
    metrics.add_sync_persisted(written.bytes.as_u64());
    metrics.add_sync_inline_stored(written.keys.len() as u64);
    metrics.add_sync_inline_missing(checked.not_held.len() as u64);
    metrics.add_sync_invalid(checked.wrong.len() as u64);
}

#[cfg(test)]
mod tests {
    use tape_core::spooler::VirtualGroup;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::EpochNumber;
    use tape_crypto::Hash;
    use tape_protocol::api::SyncTrackDataEntry;
    use tape_store::types::{Entry, EntryKey, Method};

    use super::*;
    use crate::features::spool::fetch::FetchTrack;

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
        let key = EntryKey { spool, is_handover: true, track: address };
        let holder_epoch = EpochNumber(0);
        let entry = Entry::never_tried();
        FetchTrack { key, entry, track, blob: None, holder_epoch }
    }

    // a payload passes on its value hash, a wrong one fails, and a missing
    // one is not held
    #[test]
    fn checks_payloads() {
        let spool = VirtualGroup(0).spool_at(0);
        let mut fetch = Fetch::new(Method::Sync, spool, 1, Vec::new());
        for (byte, payload) in [(1, b"good"), (2, b"lost"), (3, b"none")] {
            fetch.push(inline(byte, payload), StorageUnits(4));
        }
        let good = b"good".to_vec();
        let evil = b"evil".to_vec();
        let entries = vec![
            SyncTrackDataEntry { track: Address::new([1; 32]), payload: good },
            SyncTrackDataEntry { track: Address::new([2; 32]), payload: evil },
        ];
        let missing = vec![Address::new([3; 32])];

        let checked = check(&fetch, SyncTrackDataRes { entries, missing });

        assert_eq!(checked.good.len(), 1);
        assert_eq!(checked.wrong, vec![fetch.tracks[1].key]);
        assert_eq!(checked.not_held, vec![fetch.tracks[2].key]);
        assert_eq!(checked.received, StorageUnits(8));
    }
}
