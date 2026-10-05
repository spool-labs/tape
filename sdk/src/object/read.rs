use tape_rpc::Rpc;
use tape_api::program::tapedrive::track_pda;
use tape_crypto::prelude::Address;
use tape_protocol::Api;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::error::TapedriveError;
use crate::metrics::{Operation, Phase};
use crate::stream::manifest::ChunkManifest;
use crate::stream::read::{read_manifest_bytes, read_manifest_into};
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Read a named object from a bucket.
    pub async fn get_object(&self, bucket: &Address, name: &str) -> Result<Vec<u8>, TapedriveError> {
        let track = self.lookup_object(bucket, name).await?;
        let address = track_pda(track.tape, track.track_number).0;
        let bytes = self.read_known(&address, track, Operation::ReadTrack).await?;

        // A stream's representing track is its manifest; detect and follow it.
        // (Hardening: a magic-prefixed manifest would make this unambiguous.)
        if let Ok(manifest) = ChunkManifest::from_bytes(&bytes) {
            let timer = self.timer(Operation::ReadStream, Phase::Total);
            let result = read_manifest_bytes(self, &manifest, &track).await;
            let timer = match &result {
                Ok(bytes) => timer.bytes(bytes.len() as u64),
                Err(_) => timer,
            };
            timer.finish_result(&result);
            return result;
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
        let track = self.lookup_object(bucket, name).await?;
        let address = track_pda(track.tape, track.track_number).0;
        let bytes = self.read_known(&address, track, Operation::ReadTrack).await?;

        if let Ok(manifest) = ChunkManifest::from_bytes(&bytes) {
            let timer = self.timer(Operation::ReadStream, Phase::Total);
            let result = read_manifest_into(self, &manifest, &track, &mut writer).await;
            timer.finish_result(&result);
            result?;
            return Ok(manifest.total_size.to_bytes());
        }

        writer.write_all(&bytes).await?;
        writer.flush().await?;
        Ok(bytes.len() as u64)
    }
}
