use std::collections::HashSet;

use futures::stream::{self, StreamExt, TryStreamExt};
use tokio::sync::Mutex;

use tape_rpc::Rpc;
use tape_core::encoding::ErasureCode;
use tape_core::track::mirror::ArchiveMirror;
use tape_core::types::{ContentType, StorageUnits};
use tape_crypto::prelude::Address;
use tape_crypto::Hash;
use tape_protocol::{Api, api::CertifyRes};

use crate::error::TapedriveError;
use crate::keys::operator::TapeOperator;
use crate::metrics::{Operation, Phase};
use crate::stream::manifest::max_track_size;
use crate::stream::write::{ChunkCertification, append_to_mirror, certify_chunk_run};
use crate::tapedrive::Tapedrive;
use crate::track::write::{
    SentRegister, UploadPlan, WrittenTrack, collect_certification, confirm_certifies, encode_blob,
    inline_write_fits, prepare_blob_register, prepare_raw_register, resolve_sent_blob,
    resolve_sent_raw, send_registers, upload_with_retry,
};

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

/// What the batch knows about one register of a run, beside the transaction itself.
struct RunItem {
    index: usize,
    name: String,
    bytes_written: u64,
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
        bucket: &impl TapeOperator,
        objects: Vec<ObjectBatchItem>,
    ) -> Result<Vec<ObjectBatchReceipt>, TapedriveError> {
        validate_batch(&objects, self.params().await?.profile)?;
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
            // Store and certify share this call; a rare outside write still lands on the conflict fallback.
            self.certify_objects_batch_inner(bucket, stored.verification, false)
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
        bucket: &impl TapeOperator,
        objects: Vec<ObjectBatchItem>,
    ) -> Result<StoredObjectBatch, TapedriveError> {
        validate_batch(&objects, self.params().await?.profile)?;

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
    /// left in the handle; a writer that moved the tape root in between drops
    /// that mirror and every track certifies against a fresh peer proof.
    pub async fn certify_objects_batch(
        &self,
        bucket: &impl TapeOperator,
        verification: ObjectBatchVerification,
    ) -> Result<(), TapedriveError> {
        self.certify_objects_batch_inner(bucket, verification, true)
            .await
    }

    async fn store_objects_batch_inner(
        &self,
        bucket: &impl TapeOperator,
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

        let mut run = Vec::with_capacity(prepared.len());
        let mut registers = Vec::with_capacity(prepared.len());
        for item in prepared {
            match item {
                PreparedItem::Inline {
                    index,
                    name,
                    content_type,
                    data,
                } => {
                    let bytes_written = data.len() as u64;
                    registers.push(prepare_raw_register(
                        self,
                        bucket,
                        name.as_bytes(),
                        content_type,
                        StorageUnits::from_bytes(bytes_written),
                        &data,
                        self.write_options.flags,
                    )?);
                    run.push(RunItem { index, name, bytes_written });
                }
                PreparedItem::Coded {
                    index,
                    name,
                    content_type,
                    bytes_written,
                    plan,
                } => {
                    registers.push(prepare_blob_register(
                        self,
                        bucket,
                        name.as_bytes(),
                        content_type,
                        StorageUnits::from_bytes(bytes_written),
                        *plan,
                        self.write_options.flags,
                    )?);
                    run.push(RunItem { index, name, bytes_written });
                }
            }
        }

        let registered = send_registers(self, bucket, registers, Operation::WriteBatch).await?;
        let mut sent = Vec::with_capacity(run.len());
        for (item, register) in run.into_iter().zip(registered) {
            sent.push(match register {
                SentRegister::Raw(raw) => SentItem::Inline {
                    index: item.index,
                    name: item.name,
                    bytes_written: item.bytes_written,
                    sent: raw,
                },
                SentRegister::Blob(blob) => SentItem::Coded {
                    index: item.index,
                    name: item.name,
                    bytes_written: item.bytes_written,
                    sent: blob,
                },
            });
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
            append_to_mirror(self, bucket, &mirror, written).await?;
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
        bucket: &impl TapeOperator,
        verification: ObjectBatchVerification,
        should_recheck_root: bool,
    ) -> Result<(), TapedriveError> {
        let ObjectBatchVerification { pending, mirror } = verification;
        if pending.is_empty() {
            return Ok(());
        }

        let pending: Vec<ChunkCertification> = stream::iter(pending)
            .map(|stored| async move {
                let collected = collect_certification(
                    self,
                    &stored.written,
                    Operation::WriteBatch,
                    &stored.receipts,
                )
                .await?;
                Ok::<_, TapedriveError>(ChunkCertification {
                    written: stored.written,
                    collected,
                    receipts: stored.receipts,
                })
            })
            .buffered(COLLECT_CONCURRENCY)
            .try_collect()
            .await?;

        if should_recheck_root {
            // A root moved by another writer stales every proof the mirror holds.
            let tape = self.get_tape(&bucket.address()).await?;
            let mut mirror = mirror.lock().await;
            if !mirror.is_empty() && tape.tracks.tree.root() != mirror.root() {
                *mirror = ArchiveMirror::new(&tape.tracks);
            }
        }

        // The certifies go out in one run against the mirror, once every register sent ahead of them has its leaf.
        let processed =
            certify_chunk_run(self, bucket, &mirror, pending, Operation::WriteBatch).await?;

        // Readers poll peers, so the batch ends at confirmation without waiting on peer lag.
        confirm_certifies(self, &processed, Operation::WriteBatch).await
    }
}

/// Check a batch before any write. Each object has to fit the profile's largest track.
fn validate_batch(objects: &[ObjectBatchItem], profile: ErasureCode) -> Result<(), TapedriveError> {
    let largest = max_track_size(profile);
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
        if object.data.len() > largest {
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
    use tape_core::encoding::{RepParams, TAPENET_PROFILE};

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
        let largest = max_track_size(TAPENET_PROFILE);

        assert!(validate_batch(&[item("asset.js", 1)], TAPENET_PROFILE).is_ok());
        assert!(validate_batch(&[item("", 1)], TAPENET_PROFILE).is_err());
        assert!(
            validate_batch(&[item("same.js", 1), item("same.js", 2)], TAPENET_PROFILE).is_err()
        );
        assert!(validate_batch(&[item("huge.bin", largest + 1)], TAPENET_PROFILE).is_err());
        let too_many = (0..=MAX_OBJECT_BATCH_ITEMS)
            .map(|index| item(&format!("{index}.js"), 1))
            .collect::<Vec<_>>();
        assert!(validate_batch(&too_many, TAPENET_PROFILE).is_err());
    }

    // an object that fits one tapenet track is too big for a mirror's batch
    #[test]
    fn mirror_limit() {
        let mirror = ErasureCode::Replication(RepParams::new(2));
        let object = [item("wide.bin", max_track_size(mirror) + 1)];

        assert!(validate_batch(&object, TAPENET_PROFILE).is_ok());
        assert!(validate_batch(&object, mirror).is_err());
    }
}
