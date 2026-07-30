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
use tape_core::erasure::{group_for_spool, leaf_position};
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

    // Already in track-address order: the slice key is the spool prefix followed
    // by the address, so a prefix scan yields the canonical enumeration and
    // sorting it again would be a second pass over every track we hold.
    let entries: Vec<SampleEntry> = context
        .store
        .iter_slice_sizes_by_spool(mine)
        .inspect_err(|error| debug!(%error, "challenge: sample set unavailable"))
        .ok()?
        .into_iter()
        .map(|(track, slice_len)| SampleEntry { track, slice_len })
        .collect();

    let seed = challenge::round_seed(&entropy.hash, state.epoch(), entropy.slot, theirs);
    let sample = challenge::draw(&seed, &entries)?;

    trace!(node = %target, spool = %theirs, track = %sample.track, sub_leaf = sample.sub_leaf, "challenge: asking");

    // Ask before reading the encoding, so a target that never answers costs one
    // round trip rather than a round trip and a store read.
    let Ok(response) = context
        .api
        .get_sample(
            target,
            &GetSampleReq {
                track: sample.track,
                spool: theirs,
                sub_leaf: sample.sub_leaf as u64,
            },
        )
        .await
    else {
        return Some(false);
    };

    // Verify against our own copy of the registered encoding, never against
    // anything the target sends alongside its proof.
    let Some(BlobData::Coded(encoding)) = context
        .store
        .get_track_data(sample.track)
        .inspect_err(|error| debug!(%error, "challenge: track encoding unavailable"))
        .ok()?
    else {
        return None;
    };

    // The commitment indexes leaves by position in the group, not by the
    // network-wide spool index the request carries.
    let position = leaf_position(theirs);
    Some(encoding.verify_sub_leaf(position, sample.sub_leaf, &response.proof))
}

/// Our spool and the target's in the first group both of us own a position in.
fn shared_group_spools(
    state: &ProtocolState,
    me: Address,
    target: Address,
) -> Option<(SpoolIndex, SpoolIndex)> {
    state.member_spools(me).into_iter().find_map(|mine| {
        state
            .spool_for_node_in_group(group_for_spool(mine), target)
            .map(|(theirs, _)| (mine, theirs))
    })
}

#[cfg(test)]
mod tests {
    use peer_memory::MemoryApi;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::TrackNumber;
    use tape_protocol::api::{GetSampleRes, PeerReq, PeerRes};
    use tape_store::ops::{TrackDataOps, TrackOps};

    use super::*;
    use crate::harness::{NodeHarness, TestContext, coded_track};

    /// Both this node and the target sit in the first group, which is what makes
    /// them challengeable at all.
    const ME: usize = 0;
    const TARGET: usize = 1;
    const PAYLOAD_BYTES: usize = 200_000;

    /// Register the track and store our own slice of it, which is the sample set
    /// the draw runs over. The target's slice never touches our store.
    fn seed(ctx: &TestContext, mine: SpoolIndex, slices: &[Vec<u8>], encoding: BlobEncoding) -> Address {
        let track = Address::new_unique();
        ctx.store
            .put_track(
                track,
                CompressedTrack {
                    tape: Address::new_unique(),
                    track_number: TrackNumber(0),
                    key: Hash::new_unique(),
                    kind: TrackKind::Coded as u64,
                    state: TrackState::Certified as u64,
                    size: encoding.size,
                    group: group_for_spool(mine),
                    value_hash: encoding.get_hash(),
                },
            )
            .expect("put track");
        ctx.store
            .put_track_data(track, BlobData::Coded(encoding))
            .expect("put track data");
        ctx.store
            .put_slice(mine, track, slices[leaf_position(mine).as_usize()].clone())
            .expect("put slice");
        track
    }

    async fn harness_with(api: MemoryApi) -> NodeHarness {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .api(api)
            .build()
            .await
            .expect("build harness")
    }

    fn entropy() -> Entropy {
        Entropy {
            slot: SlotNumber(1_000),
            hash: Hash::from([9u8; 32]),
        }
    }

    fn target_of(harness: &NodeHarness) -> Address {
        Address::from(harness.node(TARGET).node_address.to_bytes())
    }

    /// A prover that answers every sample from the target's real slice, at the
    /// leaf index it is handed or at `offset` leaves away from it.
    fn prover(slices: Vec<Vec<u8>>, encoding: BlobEncoding, offset: usize) -> MemoryApi {
        MemoryApi::new(move |_, req| match req {
            PeerReq::GetSample(ref req) => {
                let position = leaf_position(req.spool);
                let leaf = (req.sub_leaf as usize + offset) % 8;
                let proof = encoding
                    .prove_sub_leaf(position, leaf, &slices[position.as_usize()])
                    .expect("prove sample");
                PeerRes::GetSample(Ok(GetSampleRes { proof }))
            }
            _ => panic!("unexpected request"),
        })
    }

    /// Run one challenge against a prover with the given leaf offset, zero being
    /// honest.
    async fn challenge_with_offset(offset: usize) -> Option<bool> {
        let (slices, encoding) = coded_track(PAYLOAD_BYTES, 0x9E37_79B9_7F4A_7C15);
        let harness = harness_with(prover(slices.clone(), encoding, offset)).await;
        let ctx = harness.ctx_for(ME);
        seed(&ctx, harness.owned_spools(ME)[0], &slices, encoding);

        challenge_target(&ctx, &ctx.state(), target_of(&harness), entropy()).await
    }

    #[tokio::test]
    async fn an_honest_answer_passes() {
        assert_eq!(challenge_with_offset(0).await, Some(true));
    }

    #[tokio::test]
    async fn a_proof_for_the_wrong_leaf_fails() {
        // A valid proof for a leaf nobody asked about, which is what a node
        // replaying one cached response would send.
        assert_eq!(challenge_with_offset(1).await, Some(false));
    }

    #[tokio::test]
    async fn a_silent_target_fails() {
        let (slices, encoding) = coded_track(PAYLOAD_BYTES, 0x9E37_79B9_7F4A_7C15);
        let harness = harness_with(MemoryApi::noop()).await;
        let ctx = harness.ctx_for(ME);
        seed(&ctx, harness.owned_spools(ME)[0], &slices, encoding);

        assert_eq!(
            challenge_target(&ctx, &ctx.state(), target_of(&harness), entropy()).await,
            Some(false)
        );
    }

    #[tokio::test]
    async fn an_empty_sample_set_reaches_no_judgement() {
        // Nothing stored means no question to ask. That is not a failed probe,
        // and scoring it as one would evict every peer on a fresh node.
        let harness = harness_with(MemoryApi::noop()).await;
        let ctx = harness.ctx_for(ME);

        assert_eq!(
            challenge_target(&ctx, &ctx.state(), target_of(&harness), entropy()).await,
            None
        );
    }

    #[tokio::test]
    async fn a_target_in_no_shared_group_reaches_no_judgement() {
        let (slices, encoding) = coded_track(PAYLOAD_BYTES, 0x9E37_79B9_7F4A_7C15);
        let harness = harness_with(MemoryApi::noop()).await;
        let ctx = harness.ctx_for(ME);
        seed(&ctx, harness.owned_spools(ME)[0], &slices, encoding);

        // A node that owns nothing shares no group with us.
        assert_eq!(
            challenge_target(&ctx, &ctx.state(), Address::new_unique(), entropy()).await,
            None
        );
    }
}
