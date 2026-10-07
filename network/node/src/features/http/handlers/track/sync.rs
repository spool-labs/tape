use std::fmt::Display;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::spooler::GroupFold;
use tape_core::system::BlacklistEntry;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_protocol::api::{
    BINARY_CONTENT, SyncSliceEntry, SyncSlicesRequest, SyncSlicesResponse,
};
use tape_store::TapeStore;
use tape_store::ops::{SliceOps, SpoolOps, TrackOps};

use crate::features::http::auth::ActivePeer;
use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

const MAX_SYNC_BATCH: usize = 1000;

pub async fn sync_slices<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    _active_peer: ActivePeer,
    body: Bytes,
) -> Result<impl IntoResponse, RouteError> {
    let request: SyncSlicesRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("sync request: {error}")))?;

    // The fold comes from the count the caller sent, so a node that has not
    // applied the caller's epoch yet answers for the same stored spools.
    let fold = GroupFold::new(request.live_groups).ok_or_else(|| {
        RouteError::BadRequest(format!("sync request: {} live groups", request.live_groups))
    })?;
    let carried = fold.carried_spools(request.spool_index);

    let limit = (request.limit as usize).clamp(1, MAX_SYNC_BATCH);
    let (slices, next_cursor) = sweep_carried(
        state.context.store.as_ref(),
        &carried,
        request.cursor.as_deref(),
        limit,
    )?;

    let current_epoch = state.context.state().epoch();

    // The blacklist is the same set for every row, and reading it per row read
    // the whole thing per row. The track metadata comes back in one call too.
    let refused = state
        .context
        .blacklist
        .entries(
            state.context.store.as_ref(),
            state.context.node_address(),
            current_epoch,
        )
        .map_err(store_error)?;
    let mut addresses: Vec<Address> = Vec::with_capacity(slices.len());
    for (track_address, _) in &slices {
        addresses.push(*track_address);
    }
    let metadata = state
        .context
        .store
        .get_tracks(&addresses)
        .map_err(store_error)?;

    let mut entries = Vec::with_capacity(slices.len());
    for ((track_address, slice_data), held) in slices.into_iter().zip(metadata) {
        let Some(track) = held else {
            continue;
        };

        if refused.contains(&BlacklistEntry::track(track_address))
            || refused.contains(&BlacklistEntry::tape(track.tape))
        {
            continue;
        }

        entries.push(SyncSliceEntry {
            track_address: track_address.to_bytes(),
            slice_data,
        });
    }

    let response = SyncSlicesResponse {
        entries,
        next_cursor,
    };

    let bytes = wincode::serialize(&response)
        .map_err(|error| RouteError::Internal(format!("serialize sync response: {error}")))?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, BINARY_CONTENT)],
        bytes,
    ))
}

fn sweep_carried<Db: Store>(
    store: &TapeStore<Db>,
    carried: &[SpoolIndex],
    cursor: Option<&[u8]>,
    limit: usize,
) -> Result<(Vec<(Address, Vec<u8>)>, Option<Vec<u8>>), RouteError> {
    // The cursor is the stored spool a page stopped in and that spool's own
    // mark, so a page can end mid-spool or span several.
    let (mut index, mut mark) = match cursor {
        Some(cursor) => {
            let (spool, mark): (SpoolIndex, Option<Vec<u8>>) = wincode::deserialize(cursor)
                .map_err(|error| RouteError::BadRequest(format!("sync cursor: {error}")))?;
            let index = carried
                .iter()
                .position(|stored| *stored == spool)
                .ok_or_else(|| RouteError::BadRequest("sync cursor: outside the spool".into()))?;

            (index, mark)
        }
        None => (0, None),
    };

    let mut slices = Vec::new();
    // A cursor is only handed out by a node that held a row.
    let mut is_holder = cursor.is_some();

    while slices.len() < limit {
        let Some(&stored) = carried.get(index) else {
            break;
        };

        if store.get_spool_state(stored).map_err(store_error)?.is_some() {
            is_holder = true;

            let (page, next) = store
                .sweep_slices_by_spool(stored, mark.as_deref(), limit - slices.len())
                .map_err(store_error)?;
            slices.extend(page);

            if next.is_some() {
                mark = next;
                continue;
            }
        }

        index += 1;
        mark = None;
    }

    if !is_holder {
        return Err(RouteError::NotResponsible);
    }

    let next_cursor = match carried.get(index) {
        Some(stored) => {
            let cursor = wincode::serialize(&(*stored, mark))
                .map_err(|error| RouteError::Internal(format!("serialize sync cursor: {error}")))?;
            Some(cursor)
        }
        None => None,
    };

    Ok((slices, next_cursor))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use tape_core::spooler::VirtualGroup;
    use tape_core::system::{SpoolState, SpoolStatus};
    use tape_core::types::EpochNumber;

    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    const SPOOL: SpoolIndex = SpoolIndex(5);

    async fn test_context() -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(SPOOL.as_usize())
    }

    fn addr(n: u8) -> Address {
        Address::from([n; 32])
    }

    fn carried() -> Vec<SpoolIndex> {
        GroupFold::new(1).expect("fold").carried_spools(SPOOL)
    }

    fn stored(group: u64) -> SpoolIndex {
        VirtualGroup(group).spool_at(SPOOL.as_usize())
    }

    // a stored spool the node keeps for its next holder, with one slice per track
    fn hold(ctx: &TestContext, spool: SpoolIndex, tracks: &[Address]) {
        ctx.store
            .set_spool_state(spool, SpoolState::new(SpoolStatus::LockedToMove, EpochNumber(1)))
            .expect("set spool state");

        for track in tracks {
            ctx.store.put_slice(spool, *track, vec![7; 8]).expect("put slice");
        }
    }

    // every page until the node hands out no cursor, one slice a page
    fn all_pages(ctx: &TestContext) -> Vec<Address> {
        let mut served = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;

        loop {
            let (slices, next) =
                sweep_carried(&ctx.store, &carried(), cursor.as_deref(), 1).expect("page");
            served.extend(slices.into_iter().map(|(track, _)| track));

            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        served.sort();
        served
    }

    // pages run through each stored spool the node has a row for and pass over the rest
    #[tokio::test]
    async fn pages_across_spools() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL, &[addr(1), addr(2)]);
        hold(&ctx, stored(3), &[addr(3)]);
        ctx.store.put_slice(stored(2), addr(9), vec![7; 8]).expect("put slice");

        assert_eq!(all_pages(&ctx), [addr(1), addr(2), addr(3)]);
    }

    // a page with room left goes on into the next stored spool
    #[tokio::test]
    async fn page_spans_spools() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL, &[addr(1)]);
        hold(&ctx, stored(3), &[addr(3)]);

        let (slices, next) = sweep_carried(&ctx.store, &carried(), None, 10).expect("page");

        assert_eq!(slices.len(), 2);
        assert_eq!(next, None);
    }

    // a node with no row under the live spool refuses the first page
    #[tokio::test]
    async fn refuses_without_rows() {
        let ctx = test_context().await;

        let result = sweep_carried(&ctx.store, &carried(), None, 10);

        assert!(matches!(result, Err(RouteError::NotResponsible)));
    }

    // a cursor for a stored spool outside the live spool is refused
    #[tokio::test]
    async fn refuses_foreign_cursor() {
        let ctx = test_context().await;
        hold(&ctx, SPOOL, &[addr(1)]);
        let mark: Option<Vec<u8>> = None;
        let cursor = wincode::serialize(&(SpoolIndex(6), mark)).expect("cursor");

        let result = sweep_carried(&ctx.store, &carried(), Some(&cursor), 10);

        assert!(matches!(result, Err(RouteError::BadRequest(_))));
    }
}
