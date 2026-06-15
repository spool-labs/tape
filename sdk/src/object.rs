use rpc::Rpc;
use tape_api::compute::TRACK_WRITE_CU;
use tape_api::instruction::build_delete_track_ix;
use tape_api::program::tapedrive::track_pda;
use tape_core::prelude::CompressedTrack;
use tape_core::types::ContentType;
use tape_crypto::hash::hash;
use tape_crypto::prelude::Address;
use tape_crypto::Hash;
use tape_protocol::api::FindTrackVersion;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::stream::manifest::{ChunkManifest, CHUNK_SIZE};
use crate::tapedrive::Tapedrive;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    pub size: u64,
    pub etag: Hash,
    pub content_type: String,
}

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {

    /// Write a named object into a bucket.
    pub async fn put_object(
        &self,
        bucket: &TapeKey,
        name: &str,
        data: &[u8],
        content_type: Option<&str>,
    ) -> Result<Address, TapedriveError> {
        let content_type = content_type
            .map(ContentType::from_str)
            .unwrap_or(ContentType::Unknown);

        if data.len() > CHUNK_SIZE {
            let receipt = self.write_bytes(bucket, name, content_type, data).await?;
            Ok(receipt.manifest)
        } else {
            let track = self.write_track(bucket, name, content_type, data).await?;
            Ok(track_pda(track.tape, track.track_number).0.into())
        }
    }

    /// Read a named object from a bucket.
    pub async fn get_object(&self, bucket: &Address, name: &str) -> Result<Vec<u8>, TapedriveError> {
        let address = self.resolve_object(bucket, name).await?;
        let bytes = self.read(&address).await?;

        // A stream's representing track is its manifest; detect and follow it.
        // (Hardening: a magic-prefixed manifest would make this unambiguous.)
        if ChunkManifest::from_bytes(&bytes).is_ok() {
            return self.read_bytes(&address).await;
        }

        Ok(bytes)
    }

    /// Fetch metadata for a named object without downloading its data.
    pub async fn head_object(&self, bucket: &Address, name: &str) -> Result<ObjectMeta, TapedriveError> {
        let track = self.lookup_object(bucket, name).await?;
        Ok(ObjectMeta {
            size: track.size.to_bytes(),
            etag: track.value_hash,
            content_type: ContentType::Unknown.to_str().to_string(),
        })
    }

    /// Delete a named object from a bucket.
    pub async fn delete_object(&self, bucket: &TapeKey, name: &str) -> Result<(), TapedriveError> {
        let address = self.resolve_object(&bucket.address(), name).await?;
        let proof = self.get_track_proof(&address).await?;

        let payer = self.payer()?;
        let tape_signer = bucket.keypair();
        let ix = build_delete_track_ix(payer.pubkey().into(), bucket.pubkey().into(), proof);

        self.rpc()
            .send_instructions_with_signers_and_compute_unit_limit(
                payer,
                TRACK_WRITE_CU,
                vec![ix],
                &[tape_signer],
            )
            .await?;

        Ok(())
    }

    /// List objects in a bucket by name prefix.
    pub async fn list_objects(
        &self,
        _bucket: &Address,
        _prefix: &str,
    ) -> Result<Vec<String>, TapedriveError> {
        Err(TapedriveError::InvalidArgument(
            "list_objects requires the node-side object index (not yet wired)".into(),
        ))
    }

    /// Resolve a name to its representing track's address (latest version).
    async fn resolve_object(&self, bucket: &Address, name: &str) -> Result<Address, TapedriveError> {
        let track = self.lookup_object(bucket, name).await?;
        Ok(track_pda(track.tape, track.track_number).0.into())
    }

    /// Resolve a name to its representing track (latest version) via `hash(name)`.
    async fn lookup_object(&self, bucket: &Address, name: &str) -> Result<CompressedTrack, TapedriveError> {
        let key = hash(name.as_bytes());
        self.find_track(bucket, key, FindTrackVersion::Latest).await
    }
}
