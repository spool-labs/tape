use tape_rpc::Rpc;
use tape_api::program::tapedrive::TrackAddress;
use tape_core::encoding::ErasureCode;
use tape_core::types::{ContentType, StorageUnits};
use tape_crypto::prelude::Address;
use tape_protocol::{api::CertifyRes, Api};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::stream::manifest::max_track_size;
use crate::track::write::{inline_write_fits, UploadPlan, WrittenTrack};
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Store a named object from a reader.
    ///
    /// The SDK chooses the smallest representation that fits: an atomic inline
    /// track, one coded track, or a multi-track stream. Inline results are
    /// already certified. Coded results have landed on storage peers and can be
    /// passed to [`Self::certify_with_receipts`] for final certification.
    /// Readers for single-track objects are buffered in memory; larger objects
    /// are consumed incrementally by the stream writer.
    pub async fn store_named_object<Reader: AsyncRead + Unpin>(
        &self,
        bucket: &TapeKey,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
        size: StorageUnits,
        reader: Reader,
    ) -> Result<(WrittenTrack, Vec<CertifyRes>), TapedriveError> {
        let name = name.as_ref();
        let mode = object_write_mode(name, size, self.params().await?.profile);
        match mode {
            ObjectWriteMode::Inline | ObjectWriteMode::Blob => {
                let expected = size.to_bytes() as usize;
                let mut data = Vec::with_capacity(expected);
                reader
                    .take(size.to_bytes().saturating_add(1))
                    .read_to_end(&mut data)
                    .await?;
                if data.len() != expected {
                    return Err(TapedriveError::InvalidArgument(
                        "object reader size did not match declared size".into(),
                    ));
                }

                if mode == ObjectWriteMode::Inline {
                    let track = self
                        .write_named_raw(bucket, name, content_type, &data)
                        .await?;
                    return Ok((
                        WrittenTrack {
                            address: track.address(),
                            track,
                        },
                        Vec::new(),
                    ));
                }

                let (written, plan) = self
                    .write_owned_blob(bucket, name, content_type, data)
                    .await?;
                let receipts = self.upload(&written, &plan).await?;
                Ok((written, receipts))
            }
            ObjectWriteMode::Stream => {
                self.store_named_stream(bucket, name, content_type, size, reader)
                    .await
            }
        }
    }

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

        if data.len() > max_track_size(self.params().await?.profile) {
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

            Ok(track.address())
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectWriteMode {
    Inline,
    Blob,
    Stream,
}

/// Pick how an object is written. One above the profile's largest track streams.
fn object_write_mode(name: &[u8], size: StorageUnits, profile: ErasureCode) -> ObjectWriteMode {
    let bytes = size.to_bytes();
    if bytes > max_track_size(profile) as u64 {
        return ObjectWriteMode::Stream;
    }
    if inline_write_fits(name, bytes as usize) {
        ObjectWriteMode::Inline
    } else {
        ObjectWriteMode::Blob
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_api::instruction::MAX_INLINE_WRITE;
    use tape_core::encoding::{RepParams, TAPENET_PROFILE};

    #[test]
    fn write_mode() {
        let largest = max_track_size(TAPENET_PROFILE) as u64;
        let mode = |name: &[u8], bytes: u64| {
            object_write_mode(name, StorageUnits::from_bytes(bytes), TAPENET_PROFILE)
        };

        assert_eq!(mode(b"tiny.txt", 0), ObjectWriteMode::Inline);
        assert_eq!(mode(b"tiny.txt", 128), ObjectWriteMode::Inline);
        // The name trailer spends part of the inline budget.
        assert_eq!(mode(b"tiny.txt", MAX_INLINE_WRITE as u64), ObjectWriteMode::Blob);
        assert_eq!(mode(b"large.bin", largest), ObjectWriteMode::Blob);
        assert_eq!(mode(b"large.bin", largest + 1), ObjectWriteMode::Stream);
    }

    // a mirror streams an object that fits one track on tapenet
    #[test]
    fn mirror_streams_sooner() {
        let mirror = ErasureCode::Replication(RepParams::new(2));
        let size = StorageUnits::from_bytes(max_track_size(mirror) as u64 + 1);

        assert_eq!(object_write_mode(b"large.bin", size, mirror), ObjectWriteMode::Stream);
        assert_eq!(object_write_mode(b"large.bin", size, TAPENET_PROFILE), ObjectWriteMode::Blob);
    }
}
