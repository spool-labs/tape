use rpc::Rpc;
use tape_api::program::tapedrive::track_pda;
use tape_core::types::{ContentType, StorageUnits};
use tape_crypto::prelude::Address;
use tape_protocol::{api::CertifyRes, Api};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::stream::manifest::MAX_TRACK_SIZE;
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
        let mode = object_write_mode(name, size);
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
                            address: track_pda(track.tape, track.track_number).0,
                            track,
                        },
                        Vec::new(),
                    ));
                }

                let (written, plan) = self
                    .write_named_blob(bucket, name, content_type, &data)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectWriteMode {
    Inline,
    Blob,
    Stream,
}

fn object_write_mode(name: &[u8], size: StorageUnits) -> ObjectWriteMode {
    let bytes = size.to_bytes();
    if bytes > MAX_TRACK_SIZE as u64 {
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

    #[test]
    fn write_mode() {
        assert_eq!(
            object_write_mode(b"tiny.txt", StorageUnits::from_bytes(0)),
            ObjectWriteMode::Inline
        );
        assert_eq!(
            object_write_mode(b"tiny.txt", StorageUnits::from_bytes(128)),
            ObjectWriteMode::Inline
        );
        assert_eq!(
            object_write_mode(b"tiny.txt", StorageUnits::from_bytes(1_024)),
            ObjectWriteMode::Blob
        );
        assert_eq!(
            object_write_mode(
                b"large.bin",
                StorageUnits::from_bytes(MAX_TRACK_SIZE as u64)
            ),
            ObjectWriteMode::Blob
        );
        assert_eq!(
            object_write_mode(
                b"large.bin",
                StorageUnits::from_bytes(MAX_TRACK_SIZE as u64 + 1)
            ),
            ObjectWriteMode::Stream
        );
    }
}
