//! Verified inline track fetch from a group's peers

use std::collections::HashSet;

use tape_core::track::data::BlobData;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_crypto::hash::hash;
use tape_crypto::Hash;
use thiserror::Error;
use tracing::{debug, warn};

use crate::api::GetTrackDataReq;
use crate::Api;

/// Inline track bytes that matched the expected value hash, and the peer that served them
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlinePayload {
    pub node: Address,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum InlineFetchError {
    #[error("no peers hold track {track}")]
    NoPeers { track: Address },

    #[error("no peer returned verified inline data for track {track}")]
    Unavailable { track: Address },
}

/// Ask a group's peers in order for a track's inline bytes and return the first verified answer
///
/// Takes peers rather than protocol state so callers pick the epoch and can leave
/// themselves out. A node holding several spools in the group is asked once.
pub async fn fetch_inline_from_peers<Cluster: Api>(
    api: &Cluster,
    peers: &[(SpoolIndex, Address)],
    track: Address,
    value_hash: Hash,
) -> Result<InlinePayload, InlineFetchError> {
    if peers.is_empty() {
        return Err(InlineFetchError::NoPeers { track });
    }

    let mut asked = HashSet::with_capacity(peers.len());
    for (_, node) in peers {
        if !asked.insert(*node) {
            continue;
        }

        let request = GetTrackDataReq { track };
        let response = match api.get_track_data(*node, &request).await {
            Ok(response) => response,
            Err(error) => {
                debug!(%track, node = %node, ?error, "inline fetch: peer request failed");
                continue;
            }
        };

        let bytes = match response.data {
            BlobData::Inline(bytes) => bytes,
            BlobData::Coded(_) => continue,
        };

        if hash(&bytes) != value_hash {
            warn!(%track, node = %node, "inline fetch: skipped payload with mismatched hash");
            continue;
        }

        return Ok(InlinePayload { node: *node, bytes });
    }

    Err(InlineFetchError::Unavailable { track })
}
