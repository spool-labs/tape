use rpc::Rpc;
use tape_crypto::prelude::Address;
use tape_protocol::Api;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::error::TapedriveError;
use crate::stream::manifest::ChunkManifest;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
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

    /// Read a named object from a bucket into an async sink.
    ///
    /// Stream objects are reconstructed directly into `writer`. Direct-track
    /// objects still pass through the single-track read path.
    pub async fn get_object_into<Writer: AsyncWrite + Unpin>(
        &self,
        bucket: &Address,
        name: &str,
        mut writer: Writer,
    ) -> Result<u64, TapedriveError> {
        let address = self.resolve_object(bucket, name).await?;
        let bytes = self.read(&address).await?;

        if let Ok(manifest) = ChunkManifest::from_bytes(&bytes) {
            self.read_into(&address, &mut writer).await?;
            return Ok(manifest.total_size.to_bytes());
        }

        writer.write_all(&bytes).await?;
        writer.flush().await?;
        Ok(bytes.len() as u64)
    }
}
