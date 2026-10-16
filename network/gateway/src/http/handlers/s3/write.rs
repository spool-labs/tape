//! S3 write context: delegate-signed writes through the SDK write engine.
//!
//! S3 writes (PutObject/DeleteObject/multipart) are authorized by a configured
//! Ed25519 *delegate* keypair (`gateway.s3.delegate_key`) rather than by each
//! tape's own authority key, which the gateway never holds.

use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_api::program::tapedrive::TrackAddress;
use tape_core::share::ShareToken;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_core::types::{ContentType, StorageUnits};
use tape_crypto::address::Address;
use tape_crypto::ed25519::{Keypair, Pubkey};
use tape_crypto::Hash;
use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_protocol::Api;
use tape_sdk::error::TapedriveError;
use tape_sdk::keys::helpers::load_ed25519_keypair;
use tape_sdk::keys::operator::TapeDelegate;
use tape_sdk::stream::manifest::max_track_size;
use tape_sdk::bootstrap::Reputation;
use tape_sdk::object::{ObjectBatchItem, ObjectBatchReceipt};
use tape_sdk::track::write::UploadPlan;
use tape_sdk::write_options::WriteOptions;
use tape_sdk::Tapedrive;
use tokio::io::AsyncRead;
use zeroize::Zeroizing;

/// Delegate signing context for the S3 write path.
pub struct S3WriteContext {
    /// Solana-compatible 64-byte encoding of the delegate keypair, wiped on drop.
    delegate_bytes: Zeroizing<[u8; 64]>,
    /// Public key of the delegate.
    delegate_pubkey: Pubkey,
    /// Peer reputation shared across requests.
    ///
    /// A client is rebuilt per S3 write, so without a shared table here every
    /// request would order peers from an empty one and learn nothing.
    reputation: Arc<Reputation>,
}

impl S3WriteContext {
    /// Load the delegate keypair from a Solana-compatible JSON keypair file.
    pub fn load(path: &Path) -> Result<Self, NodeError> {
        let keypair = load_ed25519_keypair(path)
            .map_err(|error| NodeError::Keypair(format!("s3 delegate key {path:?}: {error}")))?;
        Ok(Self {
            delegate_bytes: Zeroizing::new(keypair.to_keypair_bytes()),
            delegate_pubkey: keypair.pubkey(),
            reputation: Arc::new(Reputation::detached()),
        })
    }

    /// The delegate identity as an Address.
    pub fn delegate_address(&self) -> Address {
        Address::from(self.delegate_pubkey)
    }

    /// Reconstruct an owned delegate Keypair to sign one set of transactions.
    /// Short-lived (per write); callers should let it drop promptly.
    fn delegate_keypair(&self) -> Result<Keypair, TapedriveError> {
        Keypair::from_keypair_bytes(*self.delegate_bytes).map_err(|error| {
            TapedriveError::InvalidArgument(format!(
                "delegate keypair reconstruction failed: {error}"
            ))
        })
    }

    /// Sign a share token as the tape's delegate, for one reader on one track key
    ///
    /// Nodes accept it only for tapes that delegate to this key; the caller checks that first.
    pub fn issue_share_token(
        &self,
        tape: Address,
        track_key: Hash,
        reader: NetworkTlsPubkey,
        nonce: Hash,
        expiry: i64,
    ) -> Result<ShareToken, TapedriveError> {
        Ok(ShareToken::issue(
            &self.delegate_keypair()?,
            tape,
            track_key,
            reader,
            nonce,
            expiry,
        ))
    }

    /// Build an SDK `Tapedrive`` write client over the gateway's shared node
    /// resources, with the delegate as fee payer.
    fn client<Db, Cluster, Blockchain>(
        &self,
        context: &NodeContext<Db, Cluster, Blockchain>,
    ) -> Result<Tapedrive<Blockchain, Cluster>, TapedriveError>
    where
        Db: Store,
        Cluster: Api,
        Blockchain: Rpc,
    {
        Ok(Tapedrive::from_parts(
            ArcSwap::new(context.state()),
            context.peer_manager.clone(),
            context.api.clone(),
            context.rpc.clone(),
            Some(self.delegate_keypair()?),
        )
        .with_reputation(self.reputation.clone()))
    }

    /// Build the delegate operator bound to a specific target `tape`.
    fn operator(&self, tape: Address) -> Result<TapeDelegate, TapedriveError> {
        Ok(TapeDelegate::new(self.delegate_keypair()?, tape))
    }

    /// Write an in-memory object to `tape` as the delegate, returning its ETag and track.
    ///
    /// `existing` is the object's current track address, if the caller resolved
    /// it (an S3 overwrite). A single-track write then resumes a matching
    /// incomplete track, skips a matching complete one, or overwrites and
    /// deletes a differing one, instead of always appending a duplicate.
    /// `plan` is the PUT's encode of these bytes, when it is still held.
    pub async fn write_object<Db, Cluster, Blockchain>(
        &self,
        context: &NodeContext<Db, Cluster, Blockchain>,
        tape: Address,
        name: &[u8],
        content_type: ContentType,
        data: &[u8],
        existing: Option<Address>,
        plan: Option<UploadPlan>,
    ) -> Result<(Hash, Address), TapedriveError>
    where
        Db: Store,
        Cluster: Api,
        Blockchain: Rpc,
    {
        let client = self.client(context)?;
        let operator = self.operator(tape)?;

        if data.len() <= max_track_size(context.params().profile) {
            // The canonical ETag, the same one the object index will record, so
            // a client is never told one value now and served another once the
            // index catches up.
            let written = client
                .write_or_resume_track_as(&operator, name, content_type, data, existing, plan)
                .await?;
            Ok((written.etag, written.track.address()))
        } else {
            // A stream is written fresh (its manifest embeds per-chunk track
            // numbers, so it cannot resume in place), then the old object this
            // overwrite orphaned is deleted whole via its manifest.
            let receipt = client
                .write_named_bytes_as(&operator, name, content_type, data)
                .await?;
            if let Some(old_track) = existing {
                match client.delete_object_as(&operator, old_track).await {
                    Ok(()) => {}
                    // An immutable old object stays, and the name now resolves to the new one.
                    Err(TapedriveError::Immutable) => {}
                    Err(error) => {
                        tracing::warn!(%error, %tape, %old_track, "overwrite delete failed; old object left for later sweep");
                    }
                }
            }
            Ok((receipt.etag, receipt.manifest))
        }
    }

    /// Write a run of in-memory objects to `tape` as the delegate in one batch, one receipt per object in order
    pub async fn write_objects<Db, Cluster, Blockchain>(
        &self,
        context: &NodeContext<Db, Cluster, Blockchain>,
        tape: Address,
        objects: Vec<ObjectBatchItem>,
    ) -> Result<Vec<ObjectBatchReceipt>, TapedriveError>
    where
        Db: Store,
        Cluster: Api,
        Blockchain: Rpc,
    {
        let client = self.client(context)?;
        let operator = self.operator(tape)?;
        client.put_objects_batch(&operator, objects).await
    }

    /// Stream a large object onto chunk tracks as the delegate, bounded in memory.
    ///
    /// With `resume`, the write lists the tape first and finishes or reuses the
    /// tracks that already hold this content, segment by segment.
    pub async fn write_object_stream<Db, Cluster, Blockchain, Reader>(
        &self,
        context: &NodeContext<Db, Cluster, Blockchain>,
        tape: Address,
        name: &[u8],
        content_type: ContentType,
        size: StorageUnits,
        reader: Reader,
        resume: bool,
    ) -> Result<(Hash, Address), TapedriveError>
    where
        Db: Store,
        Cluster: Api,
        Blockchain: Rpc,
        Reader: AsyncRead + Unpin,
    {
        let options = WriteOptions {
            resume,
            ..WriteOptions::default()
        };
        let client = self.client(context)?.with_write_options(options);
        let operator = self.operator(tape)?;
        let receipt = client
            .write_named_stream_as(&operator, name, content_type, size, reader)
            .await?;
        Ok((receipt.etag, receipt.manifest))
    }

    /// Delete the `track` backing an object on `tape` as the delegate.
    pub async fn delete_object<Db, Cluster, Blockchain>(
        &self,
        context: &NodeContext<Db, Cluster, Blockchain>,
        tape: Address,
        track: Address,
    ) -> Result<(), TapedriveError>
    where
        Db: Store,
        Cluster: Api,
        Blockchain: Rpc,
    {
        let client = self.client(context)?;
        let operator = self.operator(tape)?;
        client.delete_as(&operator, track).await
    }
}
