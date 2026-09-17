use std::collections::HashSet;

use futures::stream::{self, StreamExt, TryStreamExt};
use tokio::sync::Mutex;

use rpc::Rpc;
use tape_core::track::mirror::ArchiveMirror;
use tape_core::types::{ContentType, StorageUnits};
use tape_crypto::prelude::Address;
use tape_crypto::Hash;
use tape_protocol::{Api, api::CertifyRes};

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::metrics::{Operation, Phase};
use crate::stream::manifest::MAX_TRACK_SIZE;
use crate::stream::write::{append_to_mirror, certify_chunk, verify_mirror_root};
use crate::tapedrive::Tapedrive;
use crate::track::write::{
    UploadPlan, WrittenTrack, collect_certification, encode_blob, inline_write_fits,
    register_blob_processed, register_raw_processed, resolve_sent_blob, resolve_sent_raw,
    upload_with_retry, wait_for_certified_track,
};
use crate::transfer::certify::CollectedSignatures;

const MAX_ENCODE_WORKERS: usize = 4;
const RESOLVE_CONCURRENCY: usize = 3;
const COLLECT_CONCURRENCY: usize = 2;
pub const MAX_OBJECT_BATCH_ITEMS: usize = 32;
pub const MAX_OBJECT_BATCH_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone)]
pub struct ObjectBatchItem {
    pub name: String,
    pub data: Vec<u8>,
    pub content_type: ContentType,
    pub plan: Option<UploadPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectBatchReceipt {
    pub name: String,
    pub address: Address,
    pub bytes_written: u64,
    pub etag: Hash,
}

enum PreparedItem {
    Inline {
        index: usize,
        name: String,
        content_type: ContentType,
        data: Vec<u8>,
    },
    Coded {
        index: usize,
        name: String,
        content_type: ContentType,
        bytes_written: u64,
        plan: Box<UploadPlan>,
    },
}

enum SentItem {
    Inline {
        index: usize,
        name: String,
        bytes_written: u64,
        sent: crate::track::write::SentRaw,
    },
    Coded {
        index: usize,
        name: String,
        bytes_written: u64,
        sent: Box<crate::track::write::SentBlob>,
    },
}

enum ResolvedItem {
    Inline {
        index: usize,
        name: String,
        bytes_written: u64,
        written: WrittenTrack,
    },
    Coded {
        index: usize,
        name: String,
        bytes_written: u64,
        written: WrittenTrack,
        plan: Box<UploadPlan>,
    },
}

enum StoredItem {
    Inline {
        index: usize,
        name: String,
        bytes_written: u64,
        written: WrittenTrack,
    },
    Coded {
        index: usize,
        name: String,
        bytes_written: u64,
        written: WrittenTrack,
        receipts: Vec<CertifyRes>,
        etag: Hash,
    },
}

struct PendingCertification {
    written: WrittenTrack,
    collected: CollectedSignatures,
    receipts: Vec<CertifyRes>,
}

struct StoredCertification {
    written: WrittenTrack,
    receipts: Vec<CertifyRes>,
}

/// The coded tracks left to certify after an object batch has landed on peers.
///
/// Pass this handle to [`Tapedrive::certify_objects_batch`] after publishing
/// the batch's optimistic result. Inline objects are certified atomically at
/// registration and therefore do not appear in this handle.
#[must_use = "coded batch objects remain uncertified until this handle is consumed"]
pub struct ObjectBatchVerification {
    pending: Vec<StoredCertification>,
    mirror: Mutex<ArchiveMirror>,
}

/// A batch whose registrations have confirmed and whose coded slices have
/// landed on storage peers.
///
/// `receipts` are safe to expose at the optimistic publication boundary.
/// Final coded-track certification remains in `verification`.
#[must_use = "coded batch objects may still require certification"]
pub struct StoredObjectBatch {
    pub receipts: Vec<ObjectBatchReceipt>,
    pub verification: ObjectBatchVerification,
}

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Write a bounded group of named, single-track objects as one ordered
    /// tape mutation pipeline.
    ///
    /// Registration order matches input order. Encoding, register-event
    /// resolution, slice storage, signature collection, and final visibility
    /// checks overlap where they do not mutate the tape tree. Register submits
    /// and coded-track certifications remain ordered. Callers that need a
    /// publication barrier (for example a site's `index.html`) should place it
    /// last; the batch does not reveal a later item before earlier items have
    /// been registered.
    pub async fn put_objects_batch(
        &self,
        bucket: &TapeKey,
        objects: Vec<ObjectBatchItem>,
    ) -> Result<Vec<ObjectBatchReceipt>, TapedriveError> {
        validate_batch(&objects)?;
        if objects.is_empty() {
            return Ok(Vec::new());
        }

        let total_bytes = objects.iter().map(|object| object.data.len() as u64).sum();
        let timer = self
            .timer(Operation::WriteBatch, Phase::Total)
            .bytes(total_bytes)
            .chunks(objects.len() as u64);
        let result = async {
            let stored = self.store_objects_batch_inner(bucket, objects).await?;
            self.certify_objects_batch_inner(bucket, stored.verification)
                .await?;
            Ok(stored.receipts)
        }
        .await;
        timer.finish_result(&result);
        result
    }

    /// Store a bounded group of named, single-track objects without waiting
    /// for final coded-track certification.
    ///
    /// Registrations are submitted in input order at processed commitment,
    /// their confirmed event resolution overlaps, and coded slice uploads run
    /// concurrently. The returned receipts can be published immediately; pass
    /// the returned verification handle to [`Self::certify_objects_batch`] to
    /// finish the batch afterward.
    pub async fn store_objects_batch(
        &self,
        bucket: &TapeKey,
        objects: Vec<ObjectBatchItem>,
    ) -> Result<StoredObjectBatch, TapedriveError> {
        validate_batch(&objects)?;

        let total_bytes = objects.iter().map(|object| object.data.len() as u64).sum();
        let timer = self
            .timer(Operation::WriteBatch, Phase::Total)
            .bytes(total_bytes)
            .chunks(objects.len() as u64);
        let result = self.store_objects_batch_inner(bucket, objects).await;
        timer.finish_result(&result);
        result
    }

    /// Certify every coded object represented by a stored batch handle.
    ///
    /// Tape mutations remain ordered while signature collection and final
    /// visibility checks overlap. Proofs come from the mirror the store phase
    /// left in the handle; a writer that touched the tape in between stales
    /// them and the certify falls back to a peer proof.
    pub async fn certify_objects_batch(
        &self,
        bucket: &TapeKey,
        verification: ObjectBatchVerification,
    ) -> Result<(), TapedriveError> {
        self.certify_objects_batch_inner(bucket, verification).await
    }

    async fn store_objects_batch_inner(
        &self,
        bucket: &TapeKey,
        objects: Vec<ObjectBatchItem>,
    ) -> Result<StoredObjectBatch, TapedriveError> {
        let tape = self.get_tape(&bucket.address()).await?;
        let mirror = Mutex::new(ArchiveMirror::new(&tape.tracks));
        let encode_workers = std::thread::available_parallelism()
            .map(|cores| cores.get())
            .unwrap_or(1)
            .min(MAX_ENCODE_WORKERS);

        let prepared: Vec<PreparedItem> = stream::iter(objects.into_iter().enumerate())
            .map(|(index, object)| async move {
                if inline_write_fits(object.name.as_bytes(), object.data.len()) {
                    return Ok(PreparedItem::Inline {
                        index,
                        name: object.name,
                        content_type: object.content_type,
                        data: object.data,
                    });
                }
                let bytes_written = object.data.len() as u64;
                let plan = match object.plan {
                    Some(plan)
                        if plan.storage_units == StorageUnits::from_bytes(bytes_written) =>
                    {
                        plan
                    }
                    Some(_) => {
                        return Err(TapedriveError::InvalidArgument(format!(
                            "batch upload plan size does not match {}",
                            object.name
                        )));
                    }
                    None => encode_blob(self, object.data, Operation::WriteBatch).await?,
                };
                Ok(PreparedItem::Coded {
                    index,
                    name: object.name,
                    content_type: object.content_type,
                    bytes_written,
                    plan: Box::new(plan),
                })
            })
            .buffered(encode_workers)
            .try_collect()
            .await?;

        let mut sent = Vec::with_capacity(prepared.len());
        for item in prepared {
            match item {
                PreparedItem::Inline {
                    index,
                    name,
                    content_type,
                    data,
                } => {
                    let bytes_written = data.len() as u64;
                    let submitted = register_raw_processed(
                        self,
                        bucket,
                        name.as_bytes(),
                        content_type,
                        StorageUnits::from_bytes(bytes_written),
                        &data,
                        Operation::WriteBatch,
                    )
                    .await?;
                    sent.push(SentItem::Inline {
                        index,
                        name,
                        bytes_written,
                        sent: submitted,
                    });
                }
                PreparedItem::Coded {
                    index,
                    name,
                    content_type,
                    bytes_written,
                    plan,
                } => {
                    let submitted = register_blob_processed(
                        self,
                        bucket,
                        name.as_bytes(),
                        content_type,
                        StorageUnits::from_bytes(bytes_written),
                        *plan,
                        Operation::WriteBatch,
                    )
                    .await?;
                    sent.push(SentItem::Coded {
                        index,
                        name,
                        bytes_written,
                        sent: Box::new(submitted),
                    });
                }
            }
        }

        let resolved: Vec<ResolvedItem> = stream::iter(sent)
            .map(|item| async move {
                match item {
                    SentItem::Inline {
                        index,
                        name,
                        bytes_written,
                        sent,
                    } => Ok::<_, TapedriveError>(ResolvedItem::Inline {
                        index,
                        name,
                        bytes_written,
                        written: resolve_sent_raw(self, sent).await?,
                    }),
                    SentItem::Coded {
                        index,
                        name,
                        bytes_written,
                        sent,
                    } => {
                        let (written, plan) = resolve_sent_blob(self, *sent).await?;
                        Ok(ResolvedItem::Coded {
                            index,
                            name,
                            bytes_written,
                            written,
                            plan: Box::new(plan),
                        })
                    }
                }
            })
            .buffered(RESOLVE_CONCURRENCY)
            .try_collect()
            .await?;

        for item in &resolved {
            let written = match item {
                ResolvedItem::Inline { written, .. } | ResolvedItem::Coded { written, .. } => {
                    written
                }
            };
            append_to_mirror(&mirror, written).await?;
        }

        let store_depth = self.write_options.store_depth.max(1);
        let stored: Vec<StoredItem> = stream::iter(resolved)
            .map(|item| async move {
                match item {
                    ResolvedItem::Coded {
                        index,
                        name,
                        bytes_written,
                        written,
                        plan,
                    } => {
                        let etag = plan.commitment_hash;
                        let receipts = upload_with_retry(
                            self,
                            &written,
                            &plan,
                            Operation::WriteBatch,
                        )
                        .await?;
                        Ok::<_, TapedriveError>(StoredItem::Coded {
                            index,
                            name,
                            bytes_written,
                            written,
                            receipts,
                            etag,
                        })
                    }
                    ResolvedItem::Inline {
                        index,
                        name,
                        bytes_written,
                        written,
                    } => Ok::<_, TapedriveError>(StoredItem::Inline {
                        index,
                        name,
                        bytes_written,
                        written,
                    }),
                }
            })
            .buffered(store_depth)
            .try_collect()
            .await?;

        let mut receipts = vec![None; stored.len()];
        let mut pending = Vec::new();
        for item in stored {
            match item {
                StoredItem::Inline {
                    index,
                    name,
                    bytes_written,
                    written,
                } => {
                    receipts[index] = Some(ObjectBatchReceipt {
                        name,
                        address: written.address,
                        bytes_written,
                        etag: written.track.value_hash,
                    });
                }
                StoredItem::Coded {
                    index,
                    name,
                    bytes_written,
                    written,
                    receipts: certify_receipts,
                    etag,
                } => {
                    receipts[index] = Some(ObjectBatchReceipt {
                        name,
                        address: written.address,
                        bytes_written,
                        etag,
                    });
                    pending.push(StoredCertification {
                        written,
                        receipts: certify_receipts,
                    });
                }
            }
        }

        let receipts = receipts
            .into_iter()
            .map(|receipt| {
                receipt.ok_or_else(|| {
                    TapedriveError::InvalidArgument("batch receipt was lost".into())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(StoredObjectBatch {
            receipts,
            verification: ObjectBatchVerification { pending, mirror },
        })
    }

    async fn certify_objects_batch_inner(
        &self,
        bucket: &TapeKey,
        verification: ObjectBatchVerification,
    ) -> Result<(), TapedriveError> {
        let ObjectBatchVerification { pending, mirror } = verification;
        if pending.is_empty() {
            return Ok(());
        }

        let pending: Vec<PendingCertification> = stream::iter(pending)
            .map(|stored| async move {
                let collected = collect_certification(
                    self,
                    &stored.written,
                    Operation::WriteBatch,
                    &stored.receipts,
                )
                .await?;
                Ok::<_, TapedriveError>(PendingCertification {
                    written: stored.written,
                    collected,
                    receipts: stored.receipts,
                })
            })
            .buffered(COLLECT_CONCURRENCY)
            .try_collect()
            .await?;

        let mut certified = Vec::with_capacity(pending.len());
        for pending in pending {
            certify_chunk(
                self,
                bucket,
                &mirror,
                &pending.written,
                pending.collected,
                &pending.receipts,
                Operation::WriteBatch,
            )
            .await?;
            certified.push(pending.written.track.track_number);
        }

        let visibility = self
            .timer(Operation::WriteBatch, Phase::CertifyVisible)
            .chunks(certified.len() as u64);
        let tape_address = bucket.address();
        let result = futures::future::try_join_all(certified.iter().map(|track_number| {
            wait_for_certified_track(self, &tape_address, *track_number)
        }))
        .await;
        visibility.finish_result(&result);
        result?;
        verify_mirror_root(self, bucket, &mirror).await
    }
}

fn validate_batch(objects: &[ObjectBatchItem]) -> Result<(), TapedriveError> {
    if objects.len() > MAX_OBJECT_BATCH_ITEMS {
        return Err(TapedriveError::InvalidArgument(format!(
            "object batch exceeds {MAX_OBJECT_BATCH_ITEMS} items"
        )));
    }
    let mut names = HashSet::with_capacity(objects.len());
    let mut total_bytes = 0_u64;
    for object in objects {
        if object.name.is_empty() || object.name.contains(['\0', '\n', '\r']) {
            return Err(TapedriveError::InvalidArgument(
                "batch object name is empty or contains control characters".into(),
            ));
        }
        if object.data.len() > MAX_TRACK_SIZE {
            return Err(TapedriveError::InvalidArgument(format!(
                "batch object {} exceeds the single-track size limit",
                object.name
            )));
        }
        total_bytes = total_bytes
            .checked_add(object.data.len() as u64)
            .filter(|total| *total <= MAX_OBJECT_BATCH_BYTES)
            .ok_or_else(|| {
                TapedriveError::InvalidArgument(format!(
                    "object batch exceeds {MAX_OBJECT_BATCH_BYTES} bytes"
                ))
            })?;
        if !names.insert(object.name.as_str()) {
            return Err(TapedriveError::InvalidArgument(format!(
                "batch contains duplicate object name {}",
                object.name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(name: &str, bytes: usize) -> ObjectBatchItem {
        ObjectBatchItem {
            name: name.into(),
            data: vec![7; bytes],
            content_type: ContentType::Unknown,
            plan: None,
        }
    }

    #[test]
    fn batch_validation_rejects_ambiguous_or_unbounded_inputs() {
        assert!(validate_batch(&[item("asset.js", 1)]).is_ok());
        assert!(validate_batch(&[item("", 1)]).is_err());
        assert!(validate_batch(&[item("same.js", 1), item("same.js", 2)]).is_err());
        assert!(validate_batch(&[item("huge.bin", MAX_TRACK_SIZE + 1)]).is_err());
        let too_many = (0..=MAX_OBJECT_BATCH_ITEMS)
            .map(|index| item(&format!("{index}.js"), 1))
            .collect::<Vec<_>>();
        assert!(validate_batch(&too_many).is_err());
    }
}
