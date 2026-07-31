use rpc::Rpc;
use tape_api::program::tapedrive::track_pda;
use tape_core::types::ContentType;
use tape_crypto::prelude::Address;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::stream::manifest::MAX_TRACK_SIZE;
use crate::track::write::UploadPlan;
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
        self.put_object_with_plan(bucket, name, data, content_type, None)
            .await
    }

    /// Write a named object, reusing an encode of the same bytes.
    ///
    /// Deciding whether an object changed already costs a full encode for a
    /// coded payload, so a caller holding that plan passes it here instead of
    /// paying twice. Streaming payloads re-chunk, so they take no plan.
    pub async fn put_object_with_plan(
        &self,
        bucket: &TapeKey,
        name: &str,
        data: &[u8],
        content_type: Option<&str>,
        plan: Option<UploadPlan>,
    ) -> Result<Address, TapedriveError> {
        let content_type = content_type
            .map(ContentType::from_str)
            .unwrap_or(ContentType::Unknown);

        if data.len() > MAX_TRACK_SIZE {
            debug_assert!(
                plan.is_none(),
                "a whole-payload plan cannot describe a streaming write"
            );
            let receipt = self
                .write_named_bytes(bucket, name, content_type, data)
                .await?;

            Ok(receipt.manifest)
        } else {
            let track = self
                .write_named_track_as(bucket, name, content_type, data, plan)
                .await?;

            Ok(track_pda(track.tape, track.track_number).0)
        }
    }
}
