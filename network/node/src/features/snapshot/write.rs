//! Runs the quorum-sign + submit pipeline for a single snapshot chunk.
//!
//! Signs `SnapshotWriteMessage(epoch, group, chunk, value_hash)`, collects a
//! 14-of-20 BLS quorum from the other group members via `quorum::collect`,
//! and calls [`submit_write_snapshot`]. Submission failures are logged — if
//! another group member raced us and posted first, the capture path will
//! observe the on-chain event independently.

use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::cert::SnapshotWriteMessage;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::BlobInfo;
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_protocol::api::GetSnapshotWriteSigReq;
use tape_protocol::Api;
use tape_retry::RetryConfig;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::chain::write_snapshot::submit_write_snapshot;
use crate::context::NodeContext;
use crate::core::peer_call::call_peer;
use crate::features::snapshot::quorum::{self, PeerSig, PerPeer};

pub async fn run<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    group: SpoolGroup,
    chunk: ChunkNumber,
    blob: BlobInfo,
    cancel: CancellationToken,
) {
    let value_hash = blob.get_hash();
    let message = SnapshotWriteMessage::new(epoch, group, chunk, value_hash).to_bytes();

    let req = Arc::new(GetSnapshotWriteSigReq {
        epoch,
        group,
        chunk,
        value_hash,
    });
    let per_peer = make_per_peer(ctx.clone(), req);

    let Some(quorum) = quorum::collect(&ctx, group, &message, per_peer, cancel, "write").await
    else {
        return;
    };

    match submit_write_snapshot(
        &ctx,
        epoch,
        group,
        chunk,
        quorum.bitmap,
        quorum.signature,
        &blob,
    )
    .await
    {
        Ok(txid) => info!(
            epoch = epoch.0,
            group = group.0,
            chunk = chunk.0,
            ?txid,
            "snapshot write: chunk posted"
        ),
        Err(error) => warn!(
            error = %error,
            epoch = epoch.0,
            group = group.0,
            chunk = chunk.0,
            "snapshot write: submit failed (likely raced)"
        ),
    }
}

fn make_per_peer<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    req: Arc<GetSnapshotWriteSigReq>,
) -> PerPeer {
    Arc::new(move |node_id, cancel| {
        let ctx = ctx.clone();
        let req = req.clone();
        Box::pin(async move {
            let res = call_peer(
                &ctx.peer_manager,
                RetryConfig::three(),
                node_id,
                Some(&cancel),
                || ctx.api.get_snapshot_write_sig(node_id, &req),
            )
            .await?;
            Ok(PeerSig {
                node_id: res.node_id,
                signature: res.signature,
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_core::types::{StorageUnits, StripeCount};
    use tape_crypto::Hash;

    fn sample_blob() -> BlobInfo {
        BlobInfo {
            size: StorageUnits::from_bytes(2_048),
            commitment: Hash::from([0xAA; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves: [Hash::from([0x44; 32]); SPOOL_GROUP_SIZE],
        }
    }

    #[tokio::test]
    async fn run_skips_when_cancelled() {
        use crate::context::test_utils::test_context;

        let ctx = test_context();
        let cancel = CancellationToken::new();
        cancel.cancel();

        run(
            ctx,
            EpochNumber(1),
            SpoolGroup(0),
            ChunkNumber(0),
            sample_blob(),
            cancel,
        )
        .await;
    }
}
