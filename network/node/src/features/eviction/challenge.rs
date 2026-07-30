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
use tape_core::erasure::leaf_position;
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

    // The commitment indexes leaves by position in the group, not by the
    // network-wide spool index the request carries.
    let position = leaf_position(theirs)?;
    Some(encoding.verify_sub_leaf(position, sample.sub_leaf, &response.proof))
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

#[cfg(test)]
mod tests {
    use peer_memory::MemoryApi;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{GROUP_SIZE, SLICE_TREE_HEIGHT, slice_root};
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{SlotNumber, StorageUnits, StripeCount, TrackNumber};
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_protocol::api::{GetSampleRes, PeerReq, PeerRes};
    use tape_slicer::{ErasureCoder, Slicer};
    use tape_store::ops::{TrackDataOps, TrackOps};

    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    /// Both this node and the target sit in the first group, which is what makes
    /// them challengeable at all.
    const ME: usize = 0;
    const TARGET: usize = 1;

    fn coded_track() -> (Address, Vec<Vec<u8>>, BlobEncoding) {
        // Xorshift rather than a counter. A payload that repeats inside one
        // sample leaf makes every leaf identical, and a merkle tree over
        // identical leaves accepts any path at any index, so a counter fill
        // would quietly pass the wrong-leaf test below.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let payload: Vec<u8> = (0..200_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect();
        let mut slicer = Slicer::clay_default();
        let slices = slicer.encode(&payload).expect("clay encode");

        let leaves: [Hash; GROUP_SIZE] =
            core::array::from_fn(|index| slice_root(&slices[index]).expect("slice within capacity"));
        let encoding = BlobEncoding {
            size: StorageUnits::from_bytes(payload.len() as u64),
            commitment: root_from_leaf_hashes::<SLICE_TREE_HEIGHT>(&leaves),
            profile: EncodingProfile::clay_default(),
            stripe_size: StorageUnits::from_bytes(slicer.stripe_size() as u64),
            stripe_count: StripeCount(1),
            leaves,
        };

        (Address::new_unique(), slices, encoding)
    }

    /// Seed the track and our own slice of it, which is the sample set the draw
    /// runs over. The target's slice never touches our store.
    fn seed(ctx: &TestContext, mine: SpoolIndex, track: Address, slices: &[Vec<u8>], encoding: BlobEncoding) {
        let position = leaf_position(mine).expect("spool in a group").as_usize();
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
                    group: GroupIndex::containing(mine),
                    value_hash: encoding.get_hash(),
                },
            )
            .expect("put track");
        ctx.store
            .put_track_data(track, BlobData::Coded(encoding))
            .expect("put track data");
        ctx.store
            .put_slice(mine, track, slices[position].clone())
            .expect("put slice");
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

    /// A prover that answers every sample honestly from the target's real slice.
    fn honest_prover(slices: Vec<Vec<u8>>, encoding: BlobEncoding) -> MemoryApi {
        MemoryApi::new(move |_, req| match req {
            PeerReq::GetSample(ref req) => {
                let position = leaf_position(req.spool).expect("spool in a group");
                let proof = encoding
                    .prove_sub_leaf(position, req.sub_leaf as usize, &slices[position.as_usize()])
                    .expect("prove sample");
                PeerRes::GetSample(Ok(GetSampleRes { proof }))
            }
            _ => panic!("unexpected request"),
        })
    }

    #[tokio::test]
    async fn an_honest_answer_passes() {
        let (track, slices, encoding) = coded_track();
        let harness = harness_with(honest_prover(slices.clone(), encoding)).await;
        let ctx = harness.ctx_for(ME);
        let mine = harness.owned_spools(ME)[0];
        seed(&ctx, mine, track, &slices, encoding);

        let state = ctx.state();
        let target = Address::from(harness.node(TARGET).node_address.to_bytes());
        assert_eq!(
            challenge_target(&ctx, &state, target, entropy()).await,
            Some(true)
        );
    }

    #[tokio::test]
    async fn a_proof_for_the_wrong_leaf_fails() {
        // The prover answers with a valid proof for a leaf nobody asked about,
        // which is what a node replaying one cached response would do.
        let (track, slices, encoding) = coded_track();
        let cached = encoding;
        let cached_slices = slices.clone();
        let api = MemoryApi::new(move |_, req| match req {
            PeerReq::GetSample(ref req) => {
                let position = leaf_position(req.spool).expect("spool in a group");
                let elsewhere = (req.sub_leaf as usize + 1) % 8;
                let proof = cached
                    .prove_sub_leaf(position, elsewhere, &cached_slices[position.as_usize()])
                    .expect("prove sample");
                PeerRes::GetSample(Ok(GetSampleRes { proof }))
            }
            _ => panic!("unexpected request"),
        });

        let harness = harness_with(api).await;
        let ctx = harness.ctx_for(ME);
        let mine = harness.owned_spools(ME)[0];
        seed(&ctx, mine, track, &slices, encoding);

        let state = ctx.state();
        let target = Address::from(harness.node(TARGET).node_address.to_bytes());
        assert_eq!(
            challenge_target(&ctx, &state, target, entropy()).await,
            Some(false)
        );
    }

    #[tokio::test]
    async fn a_silent_target_fails() {
        let (track, slices, encoding) = coded_track();
        let harness = harness_with(MemoryApi::noop()).await;
        let ctx = harness.ctx_for(ME);
        let mine = harness.owned_spools(ME)[0];
        seed(&ctx, mine, track, &slices, encoding);

        let state = ctx.state();
        let target = Address::from(harness.node(TARGET).node_address.to_bytes());
        assert_eq!(
            challenge_target(&ctx, &state, target, entropy()).await,
            Some(false)
        );
    }

    #[tokio::test]
    async fn an_empty_sample_set_reaches_no_judgement() {
        // Nothing stored means no question to ask. That is not a failed probe,
        // and scoring it as one would evict every peer on a fresh node.
        let harness = harness_with(MemoryApi::noop()).await;
        let ctx = harness.ctx_for(ME);

        let state = ctx.state();
        let target = Address::from(harness.node(TARGET).node_address.to_bytes());
        assert_eq!(challenge_target(&ctx, &state, target, entropy()).await, None);
    }

    #[tokio::test]
    async fn a_target_in_no_shared_group_reaches_no_judgement() {
        let (track, slices, encoding) = coded_track();
        let harness = harness_with(MemoryApi::noop()).await;
        let ctx = harness.ctx_for(ME);
        let mine = harness.owned_spools(ME)[0];
        seed(&ctx, mine, track, &slices, encoding);

        let state = ctx.state();
        // A node that owns nothing shares no group with us.
        assert_eq!(
            challenge_target(&ctx, &state, Address::new_unique(), entropy()).await,
            None
        );
    }

    #[tokio::test]
    async fn the_sample_follows_the_entropy() {
        // A different block must ask a different question, or a target could
        // retain one leaf and answer forever.
        let (track, slices, encoding) = coded_track();
        let mut asked = Vec::new();
        for byte in [1u8, 2, 3, 4] {
            let harness = harness_with(honest_prover(slices.clone(), encoding)).await;
            let ctx = harness.ctx_for(ME);
            let mine = harness.owned_spools(ME)[0];
            seed(&ctx, mine, track, &slices, encoding);

            let state = ctx.state();
            let target = Address::from(harness.node(TARGET).node_address.to_bytes());
            let theirs = shared_group_spools(&state, ctx.node_address(), target).expect("shared group").1;

            let entries: Vec<SampleEntry> = ctx
                .store
                .iter_slice_sizes_by_spool(mine)
                .expect("sample set")
                .into_iter()
                .map(|(track, slice_len)| SampleEntry { track, slice_len })
                .collect();
            let seed_hash = challenge::round_seed(
                &Hash::from([byte; 32]),
                state.epoch(),
                SlotNumber(1_000),
                theirs,
            );
            asked.push(challenge::draw(&seed_hash, theirs, &entries).expect("draw").sub_leaf);
        }
        assert!(asked.iter().any(|leaf| *leaf != asked[0]), "asked {asked:?}");
    }
}
