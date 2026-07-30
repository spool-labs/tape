//! Judge a peer by asking it to prove it still holds a sample of its data.
//!
//! This is a strictly stronger probe than a health ping: it shows the peer is
//! reachable and can produce a valid proof over assigned bytes at answer time.
//! It does not show the peer has held those bytes since the write, and nothing
//! here should be read as proving storage.
//!
//! The sample set comes from this node's own spool in the same group. Every
//! position in a group holds a slice of the same tracks, so our track list is
//! the one the target should have, and a target that dropped data cannot shrink
//! the set it is drawn from. The entropy is a finalized block hash, so the target
//! cannot know which leaf it will be asked for until it is asked.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::challenge::{self, SampleEntry};
use tape_core::spooler::GroupIndex;
use tape_core::track::data::BlobData;
use tape_core::types::{SlotNumber, SpoolIndex};
use tape_crypto::hash::Hash;
use tape_crypto::Address;
use tape_protocol::api::GetSampleReq;
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::{SliceOps, TrackDataOps};
use tracing::{debug, trace};

use crate::context::NodeContext;

/// The finalized block a round draws its sample from.
#[derive(Clone, Copy, Debug)]
pub struct Entropy {
    pub slot: SlotNumber,
    pub hash: Hash,
}

/// Ask a target to prove one sample leaf, and verify what comes back.
///
/// Returns None when this node cannot pose the question, which is a target in no
/// shared group, a shared group holding nothing, or a track whose encoding we do
/// not have. A None is not evidence against the target and must not be scored.
pub async fn challenge_target<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    state: &ProtocolState,
    target: Address,
    entropy: Entropy,
) -> Option<bool> {
    let (mine, theirs) = shared_group_spools(state, context.node_address(), target)?;

    let mut entries: Vec<SampleEntry> = context
        .store
        .iter_slice_sizes_by_spool(mine)
        .map_err(|error| debug!(%error, "challenge: sample set unavailable"))
        .ok()?
        .into_iter()
        .map(|(track, slice_len)| SampleEntry { track, slice_len })
        .collect();
    challenge::sort_entries(&mut entries);

    let seed = challenge::round_seed(&entropy.hash, state.epoch(), entropy.slot, theirs);
    let sample = challenge::draw(&seed, theirs, &entries)?;

    // Verify against our own copy of the registered encoding, never against
    // anything the target sends alongside its proof.
    let Some(BlobData::Coded(encoding)) = context
        .store
        .get_track_data(sample.track)
        .map_err(|error| debug!(%error, "challenge: track encoding unavailable"))
        .ok()?
    else {
        return None;
    };

    trace!(node = %target, spool = %theirs, track = %sample.track, sub_leaf = sample.sub_leaf, "challenge: asking");

    let answered = context
        .api
        .get_sample(
            target,
            &GetSampleReq {
                track: sample.track,
                spool: theirs,
                sub_leaf: sample.sub_leaf as u64,
            },
        )
        .await;

    let Ok(response) = answered else {
        return Some(false);
    };

    Some(encoding.verify_sub_leaf(theirs, sample.sub_leaf, &response.proof))
}

/// Our spool and the target's in the first group both of us own a position in.
fn shared_group_spools(
    state: &ProtocolState,
    me: Address,
    target: Address,
) -> Option<(SpoolIndex, SpoolIndex)> {
    let theirs = state.member_spools(target);
    state.member_spools(me).into_iter().find_map(|mine| {
        theirs
            .iter()
            .copied()
            .find(|spool| GroupIndex::containing(*spool) == GroupIndex::containing(mine))
            .map(|spool| (mine, spool))
    })
}
