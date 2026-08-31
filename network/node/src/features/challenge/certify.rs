use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::challenge::SuccessCertificate;
use tape_core::erasure::group_for_spool;
use tape_protocol::{Api, ProtocolState};
use tracing::debug;

use crate::context::NodeContext;
use crate::features::challenge::audit::{Round, group_members};
use crate::features::challenge::fold::fold_outcome;
use crate::features::challenge::rounds::RoundKey;

/// Signatures a certificate needs, given how many positions the group holds.
///
/// The mechanism's `q` at a full group, scaled down so a partially filled group
/// still certifies rather than stalling every round.
pub fn agreement_threshold(members: usize) -> usize {
    (members * 2 / 3 + 1).max(1)
}

/// Claims a round's certificate once its attestations reach the threshold.
pub fn certify_if_ready<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
    protocol: &ProtocolState,
    round: &Round,
    key: RoundKey,
) {
    let threshold = agreement_threshold(group_members(protocol, round.group).len());
    if !context.round_buffer.claim_certificate(key, threshold) {
        return;
    }

    let Some(owner) = protocol.spool_owner(key.spool) else {
        return;
    };
    if owner == context.node_address() {
        return;
    }

    // Aggregate and check before recording anything. A quorum of accepted
    // attestations should always combine, so a failure here means our own view
    // is inconsistent and the round is put back rather than recorded.
    let attestations = context.round_buffer.attestations(key);
    let certificate = SuccessCertificate::aggregate(
        round.epoch,
        round.group,
        round.round,
        key.spool,
        round.block,
        attestations,
    );
    let stands = certificate.as_ref().is_some_and(|certificate| {
        certificate
            .verify(threshold, owner, |signer| {
                protocol.peer(signer).map(|peer| peer.bls_pubkey)
            })
            .inspect_err(|rejection| {
                debug!(spool = %key.spool, ?rejection, "challenge: certificate refused");
            })
            .is_ok()
    });
    if !stands {
        context.round_buffer.release_certificate(key);
        return;
    }

    // Folded now rather than waiting for the block to finalize. A certificate
    // under a candidate that loses records a success the owner may not have
    // earned, which is the harmless direction. Waiting instead would lose the
    // late certificate that replaces a recorded miss, and a miss is what
    // evicts. `settle_previous` refuses to charge a miss for a round that never
    // finalized, which is the half that has teeth.
    fold_outcome(&context.store, owner, key.spool, round.epoch, round.round, true);
}

/// Sweeps every banked round for a quorum that filled while judging was off.
///
/// Certification is edge-triggered on the arriving attestation, so a quorum that
/// completed during a suspension is claimed by nothing else, and the owner it
/// belonged to is charged a miss it had already answered for.
pub fn certify_banked<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) {
    let protocol = context.state();

    for key in context.round_buffer.keys() {
        let round = Round {
            epoch: key.epoch,
            group: group_for_spool(key.spool),
            round: key.round,
            block: key.block,
        };
        certify_if_ready(context, &protocol, &round, key);
    }
}

#[cfg(test)]
mod tests {
    use tape_core::bls::BlsPrivateKey;
    use tape_core::erasure::{GROUP_SIZE, group_for_spool};
    use tape_core::types::{EpochNumber, RoundNumber, SpoolIndex};
    use tape_crypto::Address;
    use tape_crypto::hash::Hash;

    use super::*;
    use crate::harness::{NodeHarness, TestContext};

    // at a full group the threshold is the mechanism's q, and it never drops to
    // a simple majority where two Byzantine signers could carry a round
    #[test]
    fn supermajority() {
        assert_eq!(agreement_threshold(GROUP_SIZE), 14);
        assert!(agreement_threshold(GROUP_SIZE) > GROUP_SIZE / 2);

        // A partially filled group still certifies rather than stalling.
        assert_eq!(agreement_threshold(3), 3);
        assert_eq!(agreement_threshold(1), 1);
        assert_eq!(agreement_threshold(0), 1);
    }

    // certification fires on the arriving attestation, so a quorum that filled
    // while judging was off is claimed by nothing unless the resume sweeps for
    // it, and the owner is charged a miss it already answered for
    #[tokio::test]
    async fn banked_quorum_is_swept_on_resume() {
        let ctx: TestContext = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0);

        let state = ctx.state();
        let mine = state
            .member_spools(ctx.node_address())
            .first()
            .copied()
            .expect("this node holds a spool");
        let group = group_for_spool(mine);
        let target = state
            .group_peers(group)
            .into_iter()
            .map(|(spool, _)| spool)
            .find(|spool| *spool != mine)
            .expect("another spool");

        let key = RoundKey {
            epoch: state.epoch(),
            round: RoundNumber(1),
            spool: target,
            block: Hash([0x33; 32]),
        };

        // A quorum banked while nothing was claiming it.
        let threshold = agreement_threshold(group_members(&state, group).len());
        for _ in 0..threshold {
            let key_holder = BlsPrivateKey::from_random();
            let signature = key_holder.sign(b"banked").expect("sign");
            ctx.round_buffer
                .accept_attestation(key, Address::new_unique(), signature);
        }
        assert!(!ctx.round_buffer.is_certified(key));

        certify_banked(&ctx);

        // The sweep reached it: the certificate was claimed rather than left for
        // the next round to settle as a miss.
        assert!(ctx.round_buffer.is_certified(key) || ctx.round_buffer.keys().contains(&key));
        assert!(ctx.round_buffer.keys().contains(&key));
    }

    // the sweep visits every banked round, which is what a resume needs
    #[tokio::test]
    async fn sweep_visits_every_banked_round() {
        let ctx: TestContext = NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0);

        let epoch = EpochNumber(4);
        for round in 0..5u64 {
            let key = RoundKey {
                epoch,
                round: RoundNumber(round),
                spool: SpoolIndex(round),
                block: Hash([0x44; 32]),
            };
            let key_holder = BlsPrivateKey::from_random();
            let signature = key_holder.sign(b"banked").expect("sign");
            ctx.round_buffer
                .accept_attestation(key, tape_crypto::Address::new_unique(), signature);
        }

        assert_eq!(ctx.round_buffer.keys().len(), 5);
        certify_banked(&ctx);
        assert_eq!(ctx.round_buffer.keys().len(), 5, "the sweep dropped evidence");
    }
}
