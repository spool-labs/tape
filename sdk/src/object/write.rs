use rpc::Rpc;
use tape_api::program::tapedrive::track_pda;
use tape_core::types::ContentType;
use tape_crypto::prelude::Address;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::stream::manifest::MAX_TRACK_SIZE;
use crate::tapedrive::Tapedrive;

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

        if data.len() > MAX_TRACK_SIZE {
            let receipt = self
                .write_named_bytes(bucket, name, content_type, data)
                .await?;

            Ok(receipt.manifest)
        } else {
            let track = self
                .write_named_track(bucket, name, content_type, data)
                .await?;

            Ok(track_pda(track.tape, track.track_number).0)
        }
    }
}
