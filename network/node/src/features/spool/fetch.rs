//! A fetch is one request to one or more peers for a batch of up to 64 tracks
//! that we get the same way: by sync, transfer, repair or recover. Each fetch
//! runs in its own task, so that the spool manager never has to wait for a
//! peer to answer.
//!
//! ```text
//! run the fetch's method: sync, transfer, repair or recover
//! each method:
//!   asks its peers, and gives up when the fetch's time limit runs out
//!   checks what came back against the track's encoding
//!   writes everything that passed the check
//!   reports how each track went:
//!     we got it, the peer didn't have it, or the data was wrong
//! when a call fails, it counts against the peer as down, too slow or refused
//! ```

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tape_core::types::{SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_protocol::api::ApiError;
use tape_protocol::Api;
use tape_recover::attempt::{AttemptFailure, Method};
use tape_recover::fetch::{get_time_limit, FetchReq, FetchRes, FetchTrack};
use tape_recover::limits::SpoolLimits;
use tape_recover::peer::PeerOutcome;
use tape_rpc::Rpc;
use tape_store::error::TapeStoreError;
use tape_store::ops::SliceOps;
use tape_store::TapeStore;
use tape_store_api::Store;
use tokio::task::{Id, JoinSet};
use tokio::time::timeout;

use crate::context::NodeContext;
use crate::features::spool::{recover, repair, sync, transfer};

/// Start a fetch as its own task, by its method, and give its task's id
pub fn start_fetch<Db, Cluster, Blockchain>(
    tasks: &mut JoinSet<FetchRes>,
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    req: FetchReq,
    limits: &SpoolLimits,
) -> Id
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let limit = get_time_limit(req.bytes, limits);
    let handle = match req.method {
        Method::Sync => tasks.spawn(sync::run(context, req, limit)),
        Method::Transfer => tasks.spawn(transfer::run(context, req, limit)),
        Method::Repair => tasks.spawn(repair::run(context, req, limit)),
        Method::Recover => tasks.spawn(recover::run(context, req, limit)),
    };
    handle.id()
}

/// Send one call, giving up after `limit`, and say how long it took
pub async fn send_with_time_limit<Response, Call>(
    limit: Duration,
    call: Call,
) -> (Result<Response, ApiError>, Duration)
where
    Call: Future<Output = Result<Response, ApiError>>,
{
    let started = Instant::now();
    let sent = timeout(limit, call).await;
    let elapsed = started.elapsed();
    let response = match sent {
        Ok(response) => response,
        Err(_) => Err(ApiError::Timeout),
    };
    (response, elapsed)
}

/// How a peer did in a call that failed: one that answered it doesn't hold
/// the data worked; anything else is the peer down, too slow, or refusing
pub fn get_call_outcome(error: &ApiError, elapsed: Duration) -> PeerOutcome {
    let bytes = StorageUnits(0);
    match error {
        ApiError::NotFound | ApiError::NotResponsible => {
            PeerOutcome::Worked { bytes, elapsed }
        }
        ApiError::ConnectionFailed(_) | ApiError::NodeUnresolved(_) => {
            PeerOutcome::Down
        }
        ApiError::Timeout => PeerOutcome::TooSlow,
        ApiError::RateLimited { retry_after } => {
            PeerOutcome::Refused { wait: *retry_after }
        }
        ApiError::Serialization(_)
        | ApiError::ServerError { .. }
        | ApiError::BlacklistedObject
        | ApiError::NotInCommittee
        | ApiError::StaleTrackProof
        | ApiError::Other(_) => PeerOutcome::Refused { wait: None },
    }
}

/// What a call that failed counts as for the tracks it asked for
pub fn get_call_failure(error: &ApiError) -> AttemptFailure {
    match error {
        ApiError::NotFound | ApiError::NotResponsible => {
            AttemptFailure::NotHeld
        }
        ApiError::ConnectionFailed(_) | ApiError::NodeUnresolved(_) => {
            AttemptFailure::Down
        }
        ApiError::Timeout => AttemptFailure::TooSlow,
        ApiError::RateLimited { .. }
        | ApiError::Serialization(_)
        | ApiError::ServerError { .. }
        | ApiError::BlacklistedObject
        | ApiError::NotInCommittee
        | ApiError::StaleTrackProof
        | ApiError::Other(_) => AttemptFailure::Refused,
    }
}

/// Whether bytes are the slice at `position` of a track, by its encoding
pub fn is_slice_of(
    track: &FetchTrack,
    position: SpoolIndex,
    bytes: &[u8],
) -> bool {
    match &track.encoding {
        Some(encoding) => encoding.verify_slice(position, bytes),
        None => false,
    }
}

/// Write a checked slice, never over one already on disk
pub fn write_slice_once<Db: Store>(
    store: &TapeStore<Db>,
    spool: SpoolIndex,
    track: Address,
    slice: &[u8],
) -> Result<(), TapeStoreError> {
    if store.has_slice(spool, track)? {
        return Ok(());
    }
    store.put_slice_from(spool, track, slice)
}

#[cfg(test)]
mod tests {
    use super::*;

    // a peer that holds none of it worked and the tracks aren't held; a
    // refusal keeps the wait it asked for
    #[test]
    fn failures_read() {
        let elapsed = Duration::from_secs(1);
        let wait = Some(Duration::from_secs(9));
        let limited = ApiError::RateLimited { retry_after: wait };
        let lacking = ApiError::NotResponsible;
        let worked = PeerOutcome::Worked { bytes: StorageUnits(0), elapsed };

        assert_eq!(get_call_outcome(&lacking, elapsed), worked);
        assert_eq!(get_call_failure(&lacking), AttemptFailure::NotHeld);
        let refused = PeerOutcome::Refused { wait };
        assert_eq!(get_call_outcome(&limited, elapsed), refused);
        let slow = get_call_outcome(&ApiError::Timeout, elapsed);
        assert_eq!(slow, PeerOutcome::TooSlow);
        assert_eq!(get_call_failure(&ApiError::Timeout), AttemptFailure::TooSlow);
    }
}
