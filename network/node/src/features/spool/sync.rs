use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::erasure::leaf_position;
use tape_core::track::data::BlobData;
use tape_core::spooler::{GroupFold, VirtualGroup};
use tape_core::system::{SpoolState, SpoolStatus};
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_core::types::StorageUnits;
use tape_core::track::blob::BlobEncoding;
use tape_crypto::address::Address;
use tape_crypto::hash::hash;
use tape_protocol::{Api, ApiError};
use tape_protocol::api::ops::{SyncSlicesReq, SyncSlicesRes, SyncTrackDataReq};
use tape_protocol::api::{SYNC_MAX_BYTES, SYNC_MAX_TRACKS};
use tape_store::error::TapeStoreError;
use tape_store::ops::{SliceOps, SpoolOps, TrackDataOps};
use tape_retry::RetryConfig;

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::spool::manager::carried_rows;
use crate::features::spool::missing::{Ask, MissingList, OwnerList};
use crate::features::spool::scan::{sweep_gaps, Gap, InlineGap};
use crate::features::spool::types::SyncResult;

// Purpose: Bring a live spool's missing slices over from their previous
//          owners after a spool reassignment. One run covers every stored
//          spool the live spool carries.
//
// Algorithm:
// 1. Load the rows waiting to sync under the live spool. If none, return Done.
// 2. Sweep the store for the coded tracks with no local slice and the inline
//    tracks with no payload (sweep_gaps).
// 3. Split the coded tracks by the previous owner in each stored spool's row
//    (MissingList). Certified gaps with no other node to sync from are
//    queued for repair at once.
// 4. Beside step 5, ask the group's holders for the missing inline payloads
//    in batches by address, turning through the holders, and keep a payload
//    only when it hashes to the track's value hash.
// 5. For each previous owner, at the same time:
//    a. Keep two requests in flight, each a batch of track addresses from the
//       front of the owner's list, with a byte budget.
//    b. Check each served slice against the track's commitment, and write it
//       only when it isn't on disk already.
//    c. Ask again for tracks the owner left out to stay within the budget.
//    d. Queue for repair the certified tracks the owner reports missing, and
//       a whole batch when its request fails, then go on with the next batch.
//    e. Keep up to 32 certified tracks from the back of the list in the
//       repair queue, refilled once fewer than 16 of them lack their slice,
//       after each answer and once a second. Repair works them from the back
//       while sync works from the front. Sync still asks for them, and
//       deletes the row of each one it stores.
//    f. Once every track has been asked for, queue the certified tracks
//       still in requests too. Stop when the requests return or when every
//       one of those tracks is on disk, whichever comes first.
// 6. Return Done. Scan runs next and queues whatever is still missing.
//
// A failed request costs only its own batch, so a flaky owner keeps being
// asked, and a dead one sends the list to repair one batch at a time.

/// Requests each previous owner has in flight at once
const REQUESTS_IN_FLIGHT: usize = 2;

/// How often sync looks at what is on disk between answers. Before the ends
/// meet it refills the window. After, it finishes the run once every
/// certified track still in requests is on disk
const PRESENCE_CHECK: Duration = Duration::from_secs(1);

/// A group's holders in the order to ask them, and how many lead as last epoch's
type Holders = (Vec<Address>, usize);

pub async fn run<Db: Store, Cluster: Api + 'static, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &RecoveryConfig,
    spool: SpoolIndex,
    epoch: EpochNumber,
    token: &CancellationToken,
) -> SyncResult {

    let nothing_synced = SyncResult::Done {
        synced_tracks: 0,
        synced_slices: 0,
    };

    // The spool is a live one. The fold of the epoch the rows were written for
    // says which stored spools it carries.
    let Some(fold) = ctx.state().fold_at(epoch) else {
        warn!(spool = %spool, epoch = epoch.0, "no fold for the epoch");
        return nothing_synced;
    };

    // If no carried row waits to sync, we don't own this spool
    let rows = match carried_rows(&ctx.store, fold, spool, epoch, SpoolStatus::Sync) {
        Ok(rows) => rows,
        Err(error) => {
            warn!(spool = %spool, %error, "failed to read spool state");
            return nothing_synced;
        }
    };
    if rows.is_empty() {
        return nothing_synced;
    }

    // One sweep finds the coded tracks with no slice and the inline tracks with
    // no payload.
    let gaps = sweep_gaps(ctx.store.as_ref(), fold, spool, config.scan_batch.max(1), true, token);
    if gaps.has_error {
        warn!(spool = %spool, "sync sweep hit a read error, scan picks up what it missed");
    }

    let list = missing_list(ctx.as_ref(), spool, gaps.coded, &rows);
    for gap in &list.unsourced {
        queue_repair(ctx.as_ref(), *gap);
    }

    // Slice checks use the coded encodings ingest writes on every node, so the
    // inline payloads need nothing from the slices and both run at once.
    let owners = futures::future::join_all(
        list.owners
            .into_iter()
            .map(|owner| sync_from_owner(ctx.as_ref(), spool, fold, owner, token)),
    );
    let inline = sync_inline(ctx.as_ref(), spool, gaps.inline, token);
    let (synced, (synced_tracks, _)) = futures::future::join(owners, inline).await;

    SyncResult::Done {
        synced_tracks,
        synced_slices: synced.into_iter().sum(),
    }
}

/// The live spool's coded tracks with no local slice, split by the previous
/// owner of each stored spool
fn missing_list<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    coded: Vec<Gap>,
    rows: &[(SpoolIndex, SpoolState)],
) -> MissingList {
    // The stored spools under a live spool share one previous owner, except in
    // the epoch after the group count drops, when each retired group had its
    // own. A row with no previous owner, or with this node as the owner, has no
    // slice peer to sync from.
    let node = ctx.node_address();
    let owners: HashMap<SpoolIndex, Address> = rows
        .iter()
        .filter_map(|(stored, state)| {
            state.prev_owner.filter(|owner| *owner != node).map(|owner| (*stored, owner))
        })
        .collect();

    // Only the stored spools with any queue row need a look per gap
    let mut queued: HashSet<SpoolIndex> = HashSet::new();
    match ctx.store.pending_repair_spools() {
        Ok(spools) => queued.extend(spools),
        Err(error) => warn!(spool = %spool, %error, "pending_repair_spools failed"),
    }
    match ctx.store.pending_recovery_spools() {
        Ok(spools) => queued.extend(spools),
        Err(error) => warn!(spool = %spool, %error, "pending_recovery_spools failed"),
    }

    let has_row = |gap: &Gap| {
        queued.contains(&gap.stored)
            && (ctx.store.has_pending_repair(gap.stored, gap.track).unwrap_or(false)
                || ctx.store.has_pending_recovery(gap.stored, gap.track).unwrap_or(false))
    };

    MissingList::build(coded, |stored| owners.get(&stored).copied(), has_row)
}

/// Ask one previous owner for its share of the list, two requests at a time,
/// while repair works from the back. Returns the slices written
async fn sync_from_owner<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    fold: GroupFold,
    mut list: OwnerList,
    token: &CancellationToken,
) -> usize {
    let owner = list.owner();
    let position = leaf_position(spool).as_usize();
    let mut requests = FuturesUnordered::new();
    let mut check = tokio::time::interval(PRESENCE_CHECK);
    check.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut synced = 0;
    let mut queued = 0;

    loop {
        if token.is_cancelled() {
            break;
        }

        while requests.len() < REQUESTS_IN_FLIGHT {
            let batch = next_batch(ctx, &mut list);
            if batch.is_empty() {
                break;
            }
            requests.push(ask_owner(ctx, owner, spool, fold, batch, token));
        }

        if list.has_met() {
            // Every track has been asked for. Repair takes the ones still in
            // requests too, so an owner can hold the row only until repair has
            // written them.
            let in_flight = list.meet();
            for gap in &in_flight {
                queue_repair(ctx, *gap);
            }
            ctx.metrics.add_sync_in_flight(in_flight.len() as u64);
            queued += in_flight.len();
            if requests.is_empty() || list.in_flight(|gap| is_on_disk(ctx, gap)) == 0 {
                break;
            }
        } else {
            let window = list.refill(|gap| is_on_disk(ctx, gap));
            for gap in &window {
                queue_repair(ctx, *gap);
            }
            ctx.metrics.add_sync_window(window.len() as u64);
            queued += window.len();
        }

        tokio::select! {
            next = requests.next(), if !requests.is_empty() => {
                let Some((batch, answer)) = next else {
                    continue;
                };
                // A request cut short by the cancel isn't the owner's failure
                if token.is_cancelled() {
                    break;
                }
                let (written, passed) = take_answer(ctx, position, &mut list, &batch, answer);
                synced += written;
                queued += passed;
            }
            _ = check.tick() => {}
        }
    }

    info!(spool = %spool, peer = %owner, stored = synced, queued, "sync from previous owner ended");
    synced
}

/// The next batch to ask the owner for, less the queued tracks repair has
/// already written
fn next_batch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    list: &mut OwnerList,
) -> Vec<Ask> {
    loop {
        let mut batch = list.next_batch(SYNC_MAX_TRACKS);
        if batch.is_empty() {
            return batch;
        }

        batch.retain(|ask| {
            if !ask.is_queued || !is_on_disk(ctx, &ask.gap) {
                return true;
            }
            if list.stored(ask.index) {
                remove_repair(ctx, ask.gap);
            }
            false
        });

        if !batch.is_empty() {
            return batch;
        }
    }
}

fn is_on_disk<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    gap: &Gap,
) -> bool {
    ctx.store.has_slice(gap.stored, gap.track).unwrap_or(false)
}

async fn ask_owner<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    owner: Address,
    spool: SpoolIndex,
    fold: GroupFold,
    batch: Vec<Ask>,
    token: &CancellationToken,
) -> (Vec<Ask>, Result<SyncSlicesRes, ApiError>) {
    let req = SyncSlicesReq {
        spool_index: spool,
        live_groups: fold.live(),
        tracks: batch.iter().map(|ask| ask.gap.track).collect(),
        max_bytes: SYNC_MAX_BYTES,
    };

    let answer = call_peer(
        &ctx.peer_manager,
        RetryConfig::ten(),
        owner,
        Some(token),
        || ctx.api.sync_slices(owner, &req),
    )
    .await;

    (batch, answer)
}

/// Write what the owner served, ask again for what it left out, and queue for
/// repair what it couldn't serve. Returns the slices written and the tracks queued
fn take_answer<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    position: usize,
    list: &mut OwnerList,
    batch: &[Ask],
    answer: Result<SyncSlicesRes, ApiError>,
) -> (usize, usize) {
    let owner = list.owner();
    let res = match answer {
        Ok(res) => res,
        Err(error) => {
            warn!(peer = %owner, tracks = batch.len(), %error, "sync request failed, queueing its batch for repair");
            return (0, pass_all(ctx, list, batch));
        }
    };

    let asked: HashMap<Address, Ask> = batch.iter().map(|ask| (ask.gap.track, *ask)).collect();
    let mut answered: HashSet<usize> = HashSet::new();

    // A slice nobody asked for is dropped, so the owner can't add tracks
    let mut served: Vec<(Ask, Vec<u8>)> = Vec::with_capacity(res.entries.len());
    for entry in res.entries {
        let Some(ask) = asked.get(&Address::new(entry.track_address)) else {
            continue;
        };
        if answered.insert(ask.index) {
            served.push((*ask, entry.slice_data));
        }
    }

    let mut missing = 0;
    let mut queued = 0;
    for track in &res.missing {
        let Some(ask) = asked.get(track) else {
            continue;
        };
        if !answered.insert(ask.index) {
            continue;
        }
        missing += 1;
        if let Some(gap) = list.passed(ask.index) {
            queue_repair(ctx, gap);
            queued += 1;
        }
    }
    ctx.metrics.add_sync_missing(missing as u64);

    if answered.is_empty() {
        warn!(peer = %owner, tracks = batch.len(), "owner served nothing and reported nothing missing, queueing the batch for repair");
        return (0, pass_all(ctx, list, batch));
    }

    for ask in batch {
        if !answered.contains(&ask.index) {
            list.left_out(ask.index);
        }
    }

    let (written, passed) = write_served(ctx, position, list, served);
    (written, queued + passed)
}

/// Check and write the served slices. Returns the slices written and the tracks queued
fn write_served<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    position: usize,
    list: &mut OwnerList,
    served: Vec<(Ask, Vec<u8>)>,
) -> (usize, usize) {
    let addresses: Vec<Address> = served.iter().map(|(ask, _)| ask.gap.track).collect();
    // A read that fails for the batch fails for every slice in it, so the
    // tracks are asked for again.
    let encodings = match ctx.store.get_track_datas(&addresses) {
        Ok(encodings) => encodings,
        Err(error) => {
            warn!(%error, "failed to read track data, asking for the batch again");
            for (ask, _) in &served {
                list.left_out(ask.index);
            }
            return (0, 0);
        }
    };

    let mut stored = 0;
    let mut taken_back = 0;
    let mut invalid = 0;
    let mut queued = 0;
    let mut fetched_bytes = 0u64;
    let mut persisted_bytes = 0u64;

    for ((ask, data), encoding) in served.into_iter().zip(encodings) {
        let gap = ask.gap;
        let slice_len = data.len() as u64;
        fetched_bytes += slice_len;

        let Some(BlobData::Coded(blob)) = encoding else {
            warn!(spool = %gap.stored, track = %gap.track, "missing blob track data for synced slice");
            if let Some(gap) = list.passed(ask.index) {
                queue_repair(ctx, gap);
                queued += 1;
            }
            continue;
        };

        if !verify_slice(position, &blob, &data) {
            warn!(spool = %gap.stored, track = %gap.track, "invalid synced slice, queueing for repair");
            invalid += 1;
            if let Some(gap) = list.passed(ask.index) {
                queue_repair(ctx, gap);
                queued += 1;
            }
            continue;
        }

        match write_slice(ctx, gap, data) {
            Ok(is_written) => {
                // Sync queued this track itself, so its row goes now the slice is in
                let is_ours = list.stored(ask.index);
                if is_ours {
                    remove_repair(ctx, gap);
                }
                if is_written {
                    persisted_bytes += slice_len;
                    if is_ours {
                        taken_back += 1;
                    } else {
                        stored += 1;
                    }
                }
            }
            Err(error) => {
                warn!(spool = %gap.stored, track = %gap.track, %error, "put_slice failed, queueing for repair");
                if let Some(gap) = list.passed(ask.index) {
                    queue_repair(ctx, gap);
                    queued += 1;
                }
            }
        }
    }

    if fetched_bytes > 0 {
        ctx.metrics.add_sync_fetched(fetched_bytes);
    }
    if persisted_bytes > 0 {
        ctx.metrics.add_sync_persisted(persisted_bytes);
    }
    ctx.metrics.add_sync_stored(stored as u64);
    ctx.metrics.add_sync_taken_back(taken_back as u64);
    ctx.metrics.add_sync_invalid(invalid as u64);

    (stored + taken_back, queued)
}

/// Write a checked slice unless one is already on disk. True when it was written
fn write_slice<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    gap: Gap,
    data: Vec<u8>,
) -> Result<bool, TapeStoreError> {
    if ctx.store.has_slice(gap.stored, gap.track)? {
        return Ok(false);
    }

    ctx.store.put_slice(gap.stored, gap.track, data)?;
    Ok(true)
}

/// Queue a whole batch's certified tracks for repair. Returns the tracks queued
fn pass_all<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    list: &mut OwnerList,
    batch: &[Ask],
) -> usize {
    let mut queued = 0;
    for ask in batch {
        if let Some(gap) = list.passed(ask.index) {
            queue_repair(ctx, gap);
            queued += 1;
        }
    }
    ctx.metrics.add_sync_failed(batch.len() as u64);
    queued
}

fn queue_repair<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    gap: Gap,
) {
    if let Err(error) = ctx.store.add_pending_repair(gap.stored, gap.track) {
        warn!(spool = %gap.stored, track = %gap.track, %error, "add_pending_repair failed");
    }
}

fn remove_repair<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    gap: Gap,
) {
    if let Err(error) = ctx.store.remove_pending_repair(gap.stored, gap.track) {
        warn!(spool = %gap.stored, track = %gap.track, %error, "remove_pending_repair failed");
    }
}

fn verify_slice(
    position: usize,
    track_data: &BlobEncoding,
    data: &[u8],
) -> bool {
    if track_data.size.0 > 0 && data.is_empty() {
        return false;
    }

    if let Some(max_len) = track_data
        .stripe_size
        .checked_mul(StorageUnits::from_bytes(track_data.stripe_count.as_u64()))
    {
        if max_len > StorageUnits::zero() && data.len() as u64 > max_len.as_u64() {
            return false;
        }
    }

    track_data.verify_slice(SpoolIndex::from(position as u64), data)
}

/// Bring over the inline payloads this node is missing from the group's
/// holders, a batch at a time. Returns the payloads written and the ones no
/// holder sent
async fn sync_inline<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    gaps: Vec<InlineGap>,
    token: &CancellationToken,
) -> (usize, usize) {
    if gaps.is_empty() {
        return (0, 0);
    }

    // Tracks with the same holders share batches
    let mut holders_of: HashMap<VirtualGroup, Holders> = HashMap::new();
    let mut by_holders: Vec<(Holders, Vec<InlineGap>)> = Vec::new();
    for gap in gaps {
        let holders = holders_of
            .entry(gap.group)
            .or_insert_with(|| track_data_peers(ctx, gap.group))
            .clone();
        match by_holders.iter_mut().find(|(known, _)| *known == holders) {
            Some((_, gaps)) => gaps.push(gap),
            None => by_holders.push((holders, vec![gap])),
        }
    }

    let mut synced = 0;
    let mut missing = 0;
    let mut turn = leaf_position(spool).as_usize();
    'groups: for ((peers, leading), gaps) in by_holders {
        for batch in gaps.chunks(SYNC_MAX_TRACKS) {
            if token.is_cancelled() {
                break 'groups;
            }

            // Each batch starts one holder further on, so new holders of a group
            // spread their asks over it. Only the leading set turns, so it stays first.
            let mut holders = peers.clone();
            let leading = if leading > 0 { leading } else { holders.len() };
            if leading > 0 {
                holders[..leading].rotate_left(turn % leading);
            }
            turn += 1;

            let (written, lost) = fetch_inline_batch(ctx, batch, &holders, token).await;
            synced += written;
            missing += lost;
        }
    }

    info!(spool = %spool, stored = synced, missing, "inline payload sync ended");
    (synced, missing)
}

/// Ask the holders in turn for a batch of inline payloads, each one for what the
/// holders before it couldn't send. Returns the payloads written and the ones no
/// holder sent
async fn fetch_inline_batch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    batch: &[InlineGap],
    holders: &[Address],
    token: &CancellationToken,
) -> (usize, usize) {
    let mut pending: Vec<InlineGap> = batch.to_vec();
    let mut written = 0;

    for &holder in holders {
        // Tracks this holder lacks or sent wrong, for the next holder
        let mut passed = Vec::new();

        while !pending.is_empty() && !token.is_cancelled() {
            let req = SyncTrackDataReq {
                tracks: pending.iter().map(|gap| gap.track).collect(),
                max_bytes: SYNC_MAX_BYTES,
            };
            // One ask per holder. The next holder in the list is the retry.
            let Ok(res) = call_peer(
                &ctx.peer_manager,
                RetryConfig::none(),
                holder,
                Some(token),
                || ctx.api.sync_track_data(holder, &req),
            )
            .await
            else {
                break;
            };

            let asked: HashMap<Address, InlineGap> = pending.iter().map(|gap| (gap.track, *gap)).collect();
            let mut stored: HashSet<Address> = HashSet::new();
            let mut unusable: HashSet<Address> = res.missing.into_iter().collect();
            for entry in res.entries {
                let Some(gap) = asked.get(&entry.track) else {
                    continue;
                };
                // A payload is kept only when it hashes to the track's value hash
                if hash(&entry.payload) != gap.value_hash {
                    warn!(peer = %holder, track = %gap.track, "inline payload doesn't match its value hash");
                    unusable.insert(gap.track);
                    continue;
                }
                match write_payload(ctx, gap.track, entry.payload) {
                    Ok(is_written) => {
                        stored.insert(gap.track);
                        if is_written {
                            written += 1;
                        }
                    }
                    Err(error) => {
                        warn!(track = %gap.track, %error, "put_track_data failed");
                        unusable.insert(gap.track);
                    }
                }
            }

            // A track the holder left out for its budget is asked of it again,
            // as long as it keeps sending something
            let mut left_out = Vec::new();
            for gap in pending.drain(..) {
                if stored.contains(&gap.track) {
                    continue;
                }
                if unusable.contains(&gap.track) {
                    passed.push(gap);
                } else {
                    left_out.push(gap);
                }
            }
            if stored.is_empty() {
                passed.append(&mut left_out);
                break;
            }
            pending = left_out;
        }

        pending.append(&mut passed);
        if pending.is_empty() || token.is_cancelled() {
            break;
        }
    }

    let mut missing = 0;
    if !token.is_cancelled() {
        for gap in &pending {
            warn!(track = %gap.track, "inline payload missing from every holder");
        }
        missing = pending.len();
    }
    ctx.metrics.add_sync_inline_stored(written as u64);
    ctx.metrics.add_sync_inline_missing(missing as u64);

    (written, missing)
}

/// Write a checked inline payload unless one is already on disk. True when it was written
fn write_payload<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    track: Address,
    payload: Vec<u8>,
) -> Result<bool, TapeStoreError> {
    if ctx.store.has_track_data(track)? {
        return Ok(false);
    }

    ctx.store.put_track_data(track, BlobData::Inline(payload))?;
    Ok(true)
}

/// The holders to ask for a group's inline payloads, leaving out this node,
/// and how many of them lead the list as last epoch's holders
fn track_data_peers<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    group: VirtualGroup,
) -> Holders {
    let state = ctx.state();
    let node = ctx.node_address();
    let mut peers = Vec::new();

    // Last epoch's holders come first. After the group count drops they are
    // the only nodes that hold a retired group's inline payloads.
    for (_, node_id) in state.group_peers_prev(group) {
        if node_id != node && !peers.contains(&node_id) {
            peers.push(node_id);
        }
    }
    let leading = peers.len();
    for (_, node_id) in state.group_peers(group) {
        if node_id != node && !peers.contains(&node_id) {
            peers.push(node_id);
        }
    }

    (peers, leading)
}

#[cfg(test)]
mod tests {
    use peer_memory::MemoryApi;
    use tape_core::encoding::TAPENET_PROFILE;
    use std::sync::Mutex;

    use tape_core::erasure::{SLICE_TREE_HEIGHT, slice_root};
    use tape_core::spooler::VirtualGroup;
    use tape_core::tape::TapeFlags;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{SlotNumber, StorageUnits, StripeCount, TapeNumber, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_protocol::api::ops::{PeerReq, PeerRes, SyncSlicesRes, SyncTrackDataRes};
    use tape_protocol::api::types::{SyncSliceEntry, SyncTrackDataEntry};
    use tape_slicer::{ErasureCoder, SliceMetadata, Slicer};
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_store::ops::{ObjectInfoOps, SpoolOps, TapeOps, TrackOps};
    use tape_store::types::{ObjectInfo, TapeInfo};

    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    const SPOOL: SpoolIndex = SpoolIndex(5);

    const WIDTH: usize = TAPENET_PROFILE.n() as usize;

    const TAPE: Address = Address::new([0; 32]);

    type Requests = Arc<Mutex<Vec<(Address, Vec<Address>)>>>;

    /// Each request's tracks, and whether the owner failed it
    type Calls = Arc<Mutex<Vec<(Vec<Address>, bool)>>>;

    fn addr(n: u8) -> Address {
        Address::from([n; 32])
    }

    fn many(n: u16) -> Address {
        let mut bytes = [0u8; 32];
        bytes[..2].copy_from_slice(&n.to_be_bytes());
        bytes[31] = 0xFF;
        Address::new(bytes)
    }

    fn peer() -> Address {
        addr(99)
    }

    async fn test_context() -> TestContext {
        test_context_with_api(MemoryApi::noop()).await
    }

    async fn test_context_with_api(api: MemoryApi) -> TestContext {
        context_with_groups(api, 1).await
    }

    async fn context_with_groups(api: MemoryApi, live_groups: u64) -> TestContext {
        let ctx = NodeHarness::builder()
            .nodes(live_groups as usize * WIDTH + 5)
            .current_group_count(live_groups)
            .no_prev_snapshot_tape()
            .api(api)
            .build()
            .await
            .expect("build harness")
            .ctx_for(SPOOL.as_usize());

        let tape = TapeInfo::new(TapeNumber(1), TapeFlags::NONE, EpochNumber(u64::MAX), TrackNumber(1));
        ctx.store.put_tape(TAPE, tape).expect("put tape");
        ctx
    }

    /// Last epoch ran one group on nodes 0 to 19, and this epoch runs the same
    async fn context_with_prev(api: MemoryApi) -> TestContext {
        let ctx = NodeHarness::builder()
            .nodes(WIDTH + 5)
            .prev_committee_nodes(0..WIDTH)
            .prev_group_count(1)
            .current_group_count(1)
            .no_prev_snapshot_tape()
            .api(api)
            .build()
            .await
            .expect("build harness")
            .ctx_for(SPOOL.as_usize());

        let tape = TapeInfo::new(TapeNumber(1), TapeFlags::NONE, EpochNumber(u64::MAX), TrackNumber(1));
        ctx.store.put_tape(TAPE, tape).expect("put tape");
        ctx
    }

    async fn sync(ctx: &TestContext) -> SyncResult {
        let epoch = ctx.state().epoch();
        run(ctx.clone(), &RecoveryConfig::default(), SPOOL, epoch, &CancellationToken::new()).await
    }

    fn carried_spool(group: VirtualGroup) -> SpoolIndex {
        group.spool_at(SPOOL.as_usize())
    }

    fn entry(track: Address, data: &[u8]) -> SyncSliceEntry {
        SyncSliceEntry {
            track_address: track.to_bytes(),
            slice_data: data.to_vec(),
        }
    }

    fn sync_state(epoch: EpochNumber, prev: Option<Address>) -> SpoolState {
        let mut state = SpoolState::new(SpoolStatus::Sync, epoch);
        state.prev_owner = prev;
        state
    }

    fn sync_rows(ctx: &TestContext, spools: &[SpoolIndex], prev: Option<Address>) {
        for spool in spools {
            ctx.store
                .set_spool_state(*spool, sync_state(ctx.state().epoch(), prev))
                .expect("set spool state");
        }
    }

    fn clay_slices(fill: u8, size: usize) -> Vec<Vec<u8>> {
        let slicer = Slicer::try_from_profile(TAPENET_PROFILE).unwrap();
        slicer.encode(&vec![fill; size]).unwrap()
    }

    fn clay_blob(size: u64, slices: &[Vec<u8>]) -> BlobEncoding {
        let metadata = SliceMetadata::parse(&slices[0]).unwrap();
        let stripe_size = metadata.stripe_size() as u64;
        let leaves = core::array::from_fn(|index| slice_root(&slices[index]).expect("slice within capacity"));
        let commitment = root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves);

        BlobEncoding {
            size: StorageUnits::from_bytes(size),
            commitment,
            stripe_size: StorageUnits::from_bytes(stripe_size),
            stripe_count: StripeCount(size.div_ceil(stripe_size)),
            leaves,
        }
    }

    fn clay_track(size: u64, slices: &[Vec<u8>]) -> CompressedTrack {
        let blob = clay_blob(size, slices);

        CompressedTrack {
            tape: TAPE,
            tape_number: TapeNumber(1),
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(size),
            group: VirtualGroup::containing(SPOOL),
            value_hash: blob.get_hash(),
        }
    }

    /// The payload every inline test track carries
    const PAYLOAD: [u8; 4] = [1; 4];

    fn inline_track() -> CompressedTrack {
        CompressedTrack {
            tape: TAPE,
            tape_number: TapeNumber(1),
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(PAYLOAD.len() as u64),
            group: VirtualGroup::containing(SPOOL),
            value_hash: hash(&PAYLOAD),
        }
    }

    fn seed_inline(ctx: &TestContext, count: u16) -> Vec<Address> {
        let tracks: Vec<Address> = (0..count).map(many).collect();
        for track in &tracks {
            ctx.store.put_track(*track, inline_track()).unwrap();
        }
        tracks
    }

    /// Answer an inline batch with `payload` for every track
    fn inline_answer(req: SyncTrackDataReq, payload: &[u8]) -> PeerRes {
        let entries = req
            .tracks
            .iter()
            .map(|track| SyncTrackDataEntry { track: *track, payload: payload.to_vec() })
            .collect();
        PeerRes::SyncTrackData(Ok(SyncTrackDataRes { entries, missing: Vec::new() }))
    }

    fn inline_missing(req: SyncTrackDataReq) -> PeerRes {
        PeerRes::SyncTrackData(Ok(SyncTrackDataRes { entries: Vec::new(), missing: req.tracks }))
    }

    /// Last epoch's holders of the spool's group, leaving out this node, in position order
    fn prev_holders(ctx: &TestContext) -> Vec<Address> {
        ctx.state()
            .group_peers_prev(VirtualGroup::containing(SPOOL))
            .into_iter()
            .map(|(_, node)| node)
            .filter(|node| *node != ctx.node_address())
            .collect()
    }

    /// Sweep for the inline tracks with no payload and fetch them
    async fn inline(ctx: &TestContext) -> (usize, usize) {
        let state = ctx.state();
        let fold = state.fold_at(state.epoch()).expect("fold for the epoch");
        let token = CancellationToken::new();
        let gaps = sweep_gaps(ctx.store.as_ref(), fold, SPOOL, 100, true, &token);
        sync_inline(ctx.as_ref(), SPOOL, gaps.inline, &token).await
    }

    /// Record which holder each inline ask went to, and answer with `respond`
    fn inline_api(
        asked: Arc<Mutex<Vec<Address>>>,
        respond: impl Fn(usize, SyncTrackDataReq) -> PeerRes + Send + Sync + 'static,
    ) -> MemoryApi {
        MemoryApi::new(move |node, req| match req {
            PeerReq::SyncTrackData(req) => {
                let mut asked = asked.lock().unwrap();
                asked.push(node);
                respond(asked.len(), req)
            }
            _ => panic!("unexpected request"),
        })
    }

    /// A coded track, its encoding, and the slice this node's position holds
    struct Coded {
        track: CompressedTrack,
        blob: BlobEncoding,
        slice: Vec<u8>,
    }

    fn coded(fill: u8, group: VirtualGroup) -> Coded {
        let slices = clay_slices(fill, 1024);
        let track = CompressedTrack {
            group,
            ..clay_track(1024, &slices)
        };
        let blob = clay_blob(1024, &slices);
        let slice = slices[leaf_position(SPOOL).as_usize()].clone();
        Coded { track, blob, slice }
    }

    // a registered track this node has metadata for, certified or not, with no slice
    fn seed(ctx: &TestContext, track_address: Address, coded: &Coded, is_certified: bool) {
        ctx.store.put_track(track_address, coded.track).expect("put track");
        ctx.store
            .put_track_data(track_address, BlobData::Coded(coded.blob))
            .expect("put track data");
        let info = ObjectInfo::Valid {
            track_address,
            registered_epoch: EpochNumber(1),
            certified_epoch: is_certified.then_some(EpochNumber(2)),
            slot: SlotNumber(10),
        };
        ctx.store.put_object_info(track_address, info).expect("put object info");
    }

    /// Serve every asked-for slice in `slices`, report the rest missing, and record each request
    fn serve(slices: HashMap<Address, Vec<u8>>, requests: Requests) -> MemoryApi {
        MemoryApi::new(move |node, req| match req {
            PeerReq::SyncSlices(req) => {
                requests.lock().expect("requests").push((node, req.tracks.clone()));
                let mut entries = Vec::new();
                let mut missing = Vec::new();
                for track in req.tracks {
                    match slices.get(&track) {
                        Some(slice) => entries.push(entry(track, slice)),
                        None => missing.push(track),
                    }
                }
                PeerRes::SyncSlices(Ok(SyncSlicesRes { entries, missing }))
            }
            _ => panic!("unexpected request"),
        })
    }

    fn requests() -> Requests {
        Arc::new(Mutex::new(Vec::new()))
    }

    fn done(synced_slices: usize) -> SyncResult {
        SyncResult::Done {
            synced_tracks: 0,
            synced_slices,
        }
    }

    fn gap(track: Address, is_certified: bool) -> Gap {
        Gap {
            track,
            stored: SPOOL,
            is_certified,
        }
    }

    /// One owner's list over `gaps`, none of them queued
    fn owner_list(gaps: Vec<Gap>) -> OwnerList {
        let mut list = MissingList::build(gaps, |_| Some(peer()), |_| false);
        list.owners.remove(0)
    }

    fn tracks_of(batch: &[Ask]) -> Vec<Address> {
        batch.iter().map(|ask| ask.gap.track).collect()
    }

    fn take(
        ctx: &TestContext,
        list: &mut OwnerList,
        batch: &[Ask],
        answer: Result<SyncSlicesRes, ApiError>,
    ) -> (usize, usize) {
        take_answer(ctx.as_ref(), leaf_position(SPOOL).as_usize(), list, batch, answer)
    }

    fn is_queued(ctx: &TestContext, track: Address) -> bool {
        ctx.store.has_pending_repair(SPOOL, track).expect("has pending repair")
    }

    // a row with no other node to sync from queues its certified gaps and asks no one
    #[tokio::test]
    async fn no_prev_owner() {
        let ctx = test_context().await;
        sync_rows(&ctx, &[SPOOL], None);
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        seed(&ctx, addr(1), &track, true);
        seed(&ctx, addr(2), &track, false);

        assert_eq!(sync(&ctx).await, done(0));
        assert!(ctx.store.has_pending_repair(SPOOL, addr(1)).unwrap());
        assert!(!ctx.store.has_pending_repair(SPOOL, addr(2)).unwrap());
    }

    #[tokio::test]
    async fn pulls_slices() {
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        let asked = requests();
        let ctx = test_context_with_api(serve(HashMap::from([(addr(1), track.slice.clone())]), asked.clone())).await;
        sync_rows(&ctx, &[SPOOL], Some(peer()));
        seed(&ctx, addr(1), &track, true);

        assert_eq!(sync(&ctx).await, done(1));
        assert_eq!(ctx.store.get_slice(SPOOL, addr(1)).unwrap(), Some(track.slice));
        assert_eq!(*asked.lock().unwrap(), [(peer(), vec![addr(1)])]);
        assert!(!ctx.store.has_pending_repair(SPOOL, addr(1)).unwrap());
    }

    // a served slice is written only when it passes its check and no slice is on
    // disk, and one that fails is queued
    #[tokio::test]
    async fn invalid_slice_queued() {
        let ctx = test_context().await;
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        let stale = coded(0xCD, VirtualGroup::containing(SPOOL)).slice;
        for address in [addr(1), addr(2), addr(3)] {
            seed(&ctx, address, &track, true);
        }
        let mut list = owner_list(vec![gap(addr(1), true), gap(addr(2), true), gap(addr(3), true)]);
        let batch = list.next_batch(SYNC_MAX_TRACKS);
        ctx.store.put_slice(SPOOL, addr(3), stale.clone()).expect("put slice");
        let res = SyncSlicesRes {
            entries: vec![
                entry(addr(1), &[0xEE; 64]),
                entry(addr(2), &track.slice),
                entry(addr(3), &track.slice),
            ],
            missing: Vec::new(),
        };

        assert_eq!(take(&ctx, &mut list, &batch, Ok(res)), (1, 1));
        assert!(!ctx.store.has_slice(SPOOL, addr(1)).expect("has slice"));
        assert!(is_queued(&ctx, addr(1)));
        assert_eq!(ctx.store.get_slice(SPOOL, addr(2)).expect("get slice"), Some(track.slice));
        assert!(!is_queued(&ctx, addr(2)));
        assert_eq!(ctx.store.get_slice(SPOOL, addr(3)).expect("get slice"), Some(stale));
        assert!(list.next_batch(SYNC_MAX_TRACKS).is_empty());
    }

    // tracks the owner reports missing are queued if certified and aren't asked
    // for again, while the ones it left out come back
    #[tokio::test]
    async fn missing_queued() {
        let ctx = test_context().await;
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        seed(&ctx, addr(3), &track, true);
        let mut list = owner_list(vec![
            gap(addr(1), true),
            gap(addr(2), false),
            gap(addr(3), true),
            gap(addr(4), true),
        ]);
        let batch = list.next_batch(SYNC_MAX_TRACKS);
        let res = SyncSlicesRes {
            entries: vec![entry(addr(3), &track.slice)],
            missing: vec![addr(1), addr(2)],
        };

        assert_eq!(take(&ctx, &mut list, &batch, Ok(res)), (1, 1));
        assert!(is_queued(&ctx, addr(1)));
        assert!(!is_queued(&ctx, addr(2)));
        assert!(!is_queued(&ctx, addr(4)));
        assert_eq!(tracks_of(&list.next_batch(SYNC_MAX_TRACKS)), [addr(4)]);
    }

    // a failed request, a NotResponsible and an empty answer each queue the
    // certified tracks of their own batch, and the list goes on to the next batch
    #[tokio::test]
    async fn failed_batch_only_itself() {
        let empty = SyncSlicesRes {
            entries: Vec::new(),
            missing: Vec::new(),
        };
        for answer in [Err(ApiError::Timeout), Err(ApiError::NotResponsible), Ok(empty)] {
            let ctx = test_context().await;
            let mut list = owner_list(vec![
                gap(addr(1), true),
                gap(addr(2), false),
                gap(addr(3), true),
                gap(addr(4), true),
            ]);
            let failed = list.next_batch(2);
            let other = list.next_batch(1);

            assert_eq!(take(&ctx, &mut list, &failed, answer), (0, 1));
            assert!(is_queued(&ctx, addr(1)));
            assert!(!is_queued(&ctx, addr(2)));
            assert!(!is_queued(&ctx, other[0].gap.track));
            assert_eq!(tracks_of(&list.next_batch(SYNC_MAX_TRACKS)), [addr(4)]);
        }
    }

    // a slice the batch didn't ask for is dropped, even when another batch asked for it
    #[tokio::test]
    async fn unasked_dropped() {
        let ctx = test_context().await;
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        seed(&ctx, addr(1), &track, true);
        seed(&ctx, addr(2), &track, true);
        let mut list = owner_list(vec![gap(addr(1), true), gap(addr(2), true)]);
        let batch = list.next_batch(1);
        let other = list.next_batch(1);
        let res = SyncSlicesRes {
            entries: vec![entry(addr(1), &track.slice), entry(other[0].gap.track, &track.slice)],
            missing: Vec::new(),
        };

        assert_eq!(take(&ctx, &mut list, &batch, Ok(res)), (1, 0));
        assert!(ctx.store.has_slice(SPOOL, addr(1)).expect("has slice"));
        assert!(!ctx.store.has_slice(SPOOL, other[0].gap.track).expect("has slice"));
    }

    // tracks the owner leaves out for its byte budget are asked for again
    #[tokio::test]
    async fn asks_again_for_left_out() {
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        let slice = track.slice.clone();
        let asked = requests();
        let seen = asked.clone();
        let api = MemoryApi::new(move |node, req| match req {
            PeerReq::SyncSlices(req) => {
                seen.lock().unwrap().push((node, req.tracks.clone()));
                // Each answer carries one slice and leaves out the rest.
                PeerRes::SyncSlices(Ok(SyncSlicesRes {
                    entries: vec![entry(req.tracks[0], &slice)],
                    missing: Vec::new(),
                }))
            }
            _ => panic!("unexpected request"),
        });
        let ctx = test_context_with_api(api).await;
        sync_rows(&ctx, &[SPOOL], Some(peer()));
        seed(&ctx, addr(1), &track, true);
        seed(&ctx, addr(2), &track, true);

        assert_eq!(sync(&ctx).await, done(2));
        assert!(ctx.store.has_slice(SPOOL, addr(2)).unwrap());
        assert_eq!(
            *asked.lock().unwrap(),
            [(peer(), vec![addr(1), addr(2)]), (peer(), vec![addr(2)])],
        );
    }

    // the owner is still asked after a failed request, and the other batches' slices land
    #[tokio::test]
    async fn asks_after_failure() {
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        let slice = track.slice.clone();
        let calls: Calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let api = MemoryApi::new(move |_, req| match req {
            PeerReq::SyncSlices(req) => {
                let mut seen = seen.lock().unwrap();
                let is_failed = seen.is_empty();
                seen.push((req.tracks.clone(), is_failed));
                if is_failed {
                    return PeerRes::SyncSlices(Err(ApiError::NotResponsible));
                }
                let entries = req.tracks.iter().map(|track| entry(*track, &slice)).collect();
                PeerRes::SyncSlices(Ok(SyncSlicesRes { entries, missing: Vec::new() }))
            }
            _ => panic!("unexpected request"),
        });
        let ctx = test_context_with_api(api).await;
        sync_rows(&ctx, &[SPOOL], Some(peer()));
        // Three batches, so the last one goes out only after an answer is back.
        let count = 2 * SYNC_MAX_TRACKS as u16 + 1;
        for n in 0..count {
            seed(&ctx, many(n), &track, true);
        }

        sync(&ctx).await;

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 3);
        for (tracks, is_failed) in calls.iter() {
            for track in tracks {
                assert_eq!(ctx.store.has_pending_repair(SPOOL, *track).unwrap(), *is_failed);
                assert_eq!(ctx.store.has_slice(SPOOL, *track).unwrap(), !*is_failed);
            }
        }
    }

    // `count` certified tracks sharing one encoding, under the node's own stored spool
    fn seed_many(ctx: &TestContext, track: &Coded, count: u16) -> Vec<Address> {
        let tracks: Vec<Address> = (0..count).map(many).collect();
        for address in &tracks {
            seed(ctx, *address, track, true);
        }
        tracks
    }

    /// Serve every asked-for track with the same slice
    fn serve_all(slice: Vec<u8>) -> MemoryApi {
        MemoryApi::new(move |_, req| match req {
            PeerReq::SyncSlices(req) => {
                let entries = req.tracks.iter().map(|track| entry(*track, &slice)).collect();
                PeerRes::SyncSlices(Ok(SyncSlicesRes { entries, missing: Vec::new() }))
            }
            _ => panic!("unexpected request"),
        })
    }

    // a healthy owner sends the whole list, window tracks included, and every
    // queue row sync wrote is gone at the end
    #[tokio::test]
    async fn fast_owner_takes_back_window() {
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        let ctx = test_context_with_api(serve_all(track.slice.clone())).await;
        sync_rows(&ctx, &[SPOOL], Some(peer()));
        // More than two batches, so the window fills before the front gets there.
        let tracks = seed_many(&ctx, &track, 2 * SYNC_MAX_TRACKS as u16 + 100);

        assert_eq!(sync(&ctx).await, done(tracks.len()));
        for address in &tracks {
            assert!(ctx.store.has_slice(SPOOL, *address).unwrap());
            assert!(!ctx.store.has_pending_repair(SPOOL, *address).unwrap());
        }
    }

    // a list smaller than two batches still comes from the owner
    #[tokio::test]
    async fn small_list_from_owner() {
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        let ctx = test_context_with_api(serve_all(track.slice.clone())).await;
        sync_rows(&ctx, &[SPOOL], Some(peer()));
        let tracks = seed_many(&ctx, &track, 3);

        assert_eq!(sync(&ctx).await, done(3));
        for address in &tracks {
            assert!(!ctx.store.has_pending_repair(SPOOL, *address).unwrap());
        }
    }

    // a dead owner sends every batch to repair, the window's tracks among them
    #[tokio::test]
    async fn dead_owner_queues_all() {
        let ctx = test_context().await;
        sync_rows(&ctx, &[SPOOL], Some(peer()));
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        let tracks = seed_many(&ctx, &track, 2 * SYNC_MAX_TRACKS as u16 + 100);

        assert_eq!(sync(&ctx).await, done(0));
        for address in &tracks {
            assert!(ctx.store.has_pending_repair(SPOOL, *address).unwrap());
        }
    }

    // a queued track repair has already written isn't asked for
    #[tokio::test]
    async fn repaired_not_asked() {
        let ctx = test_context().await;
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        let gap = Gap {
            track: addr(1),
            stored: SPOOL,
            is_certified: true,
        };
        let mut list = MissingList::build(vec![gap], |_| Some(peer()), |_| true);
        ctx.store.put_slice(SPOOL, addr(1), track.slice).unwrap();

        let owner = &mut list.owners[0];
        assert!(next_batch(ctx.as_ref(), owner).is_empty());
        assert!(owner.has_met());
    }

    // after a restart, only the tracks still missing a slice are asked for
    #[tokio::test]
    async fn asks_only_missing() {
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        let asked = requests();
        let ctx = test_context_with_api(serve(HashMap::from([(addr(2), track.slice.clone())]), asked.clone())).await;
        sync_rows(&ctx, &[SPOOL], Some(peer()));
        seed(&ctx, addr(1), &track, true);
        seed(&ctx, addr(2), &track, true);
        ctx.store.put_slice(SPOOL, addr(1), track.slice.clone()).unwrap();

        assert_eq!(sync(&ctx).await, done(1));
        assert_eq!(*asked.lock().unwrap(), [(peer(), vec![addr(2)])]);
    }

    // a gap with a queue row already keeps that row as it was
    #[tokio::test]
    async fn queued_gap_kept() {
        let ctx = test_context_with_api(serve(HashMap::new(), requests())).await;
        sync_rows(&ctx, &[SPOOL], Some(peer()));
        let track = coded(0xAB, VirtualGroup::containing(SPOOL));
        seed(&ctx, addr(1), &track, true);
        ctx.store
            .defer_pending_repair(SPOOL, addr(1), SlotNumber(u64::MAX))
            .unwrap();

        sync(&ctx).await;

        // Still deferred, so the repair path can't take it yet.
        assert!(ctx.store.has_pending_repair(SPOOL, addr(1)).unwrap());
        assert!(ctx.store.repairs_due(SPOOL, None, SlotNumber(u64::MAX - 1), 10).unwrap().is_empty());
    }

    // slices of two carried groups go in one request and land under their own stored spools
    #[tokio::test]
    async fn one_request_for_carried() {
        let own = coded(0x11, VirtualGroup::containing(SPOOL));
        let carried = coded(0x22, VirtualGroup(1));
        let asked = requests();
        let slices = HashMap::from([(addr(1), own.slice.clone()), (addr(2), carried.slice.clone())]);
        let ctx = test_context_with_api(serve(slices, asked.clone())).await;
        sync_rows(&ctx, &[SPOOL, carried_spool(VirtualGroup(1))], Some(peer()));
        seed(&ctx, addr(1), &own, true);
        seed(&ctx, addr(2), &carried, true);

        assert_eq!(sync(&ctx).await, done(2));
        assert!(ctx.store.has_slice(SPOOL, addr(1)).unwrap());
        assert!(ctx.store.has_slice(carried_spool(VirtualGroup(1)), addr(2)).unwrap());
        assert!(!ctx.store.has_slice(SPOOL, addr(2)).unwrap());
        assert_eq!(asked.lock().unwrap().len(), 1);
    }

    // after the group count drops, each owner is asked for its own stored spools' tracks
    #[tokio::test]
    async fn asks_each_owner() {
        let kept = addr(98);
        let retired = addr(99);
        let own = coded(0x11, VirtualGroup::containing(SPOOL));
        let first = coded(0x22, VirtualGroup(1));
        let second = coded(0x33, VirtualGroup(3));
        let asked = requests();
        let ctx = test_context_with_api(serve(HashMap::new(), asked.clone())).await;
        sync_rows(&ctx, &[SPOOL], Some(kept));
        sync_rows(&ctx, &[carried_spool(VirtualGroup(1)), carried_spool(VirtualGroup(3))], Some(retired));
        seed(&ctx, addr(1), &own, true);
        seed(&ctx, addr(2), &first, true);
        seed(&ctx, addr(3), &second, true);

        sync(&ctx).await;

        let mut asked = asked.lock().unwrap().clone();
        asked.sort();
        assert_eq!(asked, [(kept, vec![addr(1)]), (retired, vec![addr(2), addr(3)])]);
    }

    // with two live groups, a track of the other one isn't asked for
    #[tokio::test]
    async fn skips_other_group() {
        let other = coded(0xAB, VirtualGroup(1));
        let asked = requests();
        let ctx = context_with_groups(serve(HashMap::new(), asked.clone()), 2).await;
        sync_rows(&ctx, &[SPOOL], Some(peer()));
        seed(&ctx, addr(1), &other, true);

        assert_eq!(sync(&ctx).await, done(0));
        assert!(asked.lock().unwrap().is_empty());
        assert!(!ctx.store.has_pending_repair(carried_spool(VirtualGroup(1)), addr(1)).unwrap());
    }

    // inline payloads go in batches, and each batch starts one holder further on
    #[tokio::test]
    async fn inline_batches_turn_holders() {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let ctx = context_with_prev(inline_api(asked.clone(), |_, req| inline_answer(req, &PAYLOAD))).await;
        let tracks = seed_inline(&ctx, 2 * SYNC_MAX_TRACKS as u16 + 1);

        let synced = inline(&ctx).await;

        let holders = prev_holders(&ctx);
        let start = leaf_position(SPOOL).as_usize();
        let expected: Vec<Address> = (0..3).map(|k| holders[(start + k) % holders.len()]).collect();

        assert_eq!(synced, (tracks.len(), 0));
        assert_eq!(*asked.lock().unwrap(), expected);
        for track in &tracks {
            assert_eq!(ctx.store.get_track_data(*track).unwrap(), Some(BlobData::Inline(PAYLOAD.to_vec())));
        }
    }

    // a holder that fails is asked once, and the next holder answers
    #[tokio::test]
    async fn inline_next_on_failure() {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let ctx = context_with_prev(inline_api(asked.clone(), |call, req| match call {
            1 => PeerRes::SyncTrackData(Err(ApiError::Timeout)),
            _ => inline_answer(req, &PAYLOAD),
        }))
        .await;
        let tracks = seed_inline(&ctx, 1);

        assert_eq!(inline(&ctx).await, (1, 0));
        assert!(ctx.store.has_track_data(tracks[0]).unwrap());

        let holders = prev_holders(&ctx);
        let start = leaf_position(SPOOL).as_usize();
        assert_eq!(
            *asked.lock().unwrap(),
            [holders[start % holders.len()], holders[(start + 1) % holders.len()]],
        );
    }

    // tracks a holder reports missing go to the next holder
    #[tokio::test]
    async fn inline_missing_to_next() {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let ctx = context_with_prev(inline_api(asked.clone(), |call, req| match call {
            1 => inline_missing(req),
            _ => inline_answer(req, &PAYLOAD),
        }))
        .await;
        let tracks = seed_inline(&ctx, 2);

        assert_eq!(inline(&ctx).await, (2, 0));
        assert_eq!(asked.lock().unwrap().len(), 2);
        assert!(ctx.store.has_track_data(tracks[1]).unwrap());
    }

    // a payload that doesn't hash to the track's value hash is refused
    #[tokio::test]
    async fn inline_wrong_payload_refused() {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let ctx = context_with_prev(inline_api(asked.clone(), |call, req| match call {
            1 => inline_answer(req, &[9; 4]),
            _ => inline_answer(req, &PAYLOAD),
        }))
        .await;
        let tracks = seed_inline(&ctx, 1);

        assert_eq!(inline(&ctx).await, (1, 0));
        assert_eq!(asked.lock().unwrap().len(), 2);
        assert_eq!(ctx.store.get_track_data(tracks[0]).unwrap(), Some(BlobData::Inline(PAYLOAD.to_vec())));
    }

    // a payload no holder has is skipped and counted once every holder has been asked
    #[tokio::test]
    async fn inline_missing_everywhere() {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let ctx = context_with_prev(inline_api(asked.clone(), |_, req| inline_missing(req))).await;
        let tracks = seed_inline(&ctx, 1);

        assert_eq!(inline(&ctx).await, (0, 1));
        assert!(!ctx.store.has_track_data(tracks[0]).unwrap());
        assert_eq!(asked.lock().unwrap().len(), track_data_peers(ctx.as_ref(), VirtualGroup::containing(SPOOL)).0.len());
    }
}
