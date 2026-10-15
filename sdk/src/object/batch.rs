use std::collections::HashSet;

use futures::stream::{self, FuturesOrdered, StreamExt, TryStreamExt};
use tokio::sync::{Mutex, mpsc};

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
use crate::progress::{Step, UnitKind};
use crate::stream::manifest::max_track_size;
use crate::stream::write::{ChunkCertification, append_to_mirror, certify_chunk_run};
use crate::tapedrive::Tapedrive;
use crate::track::write::{
    PreparedRegister, ResolvedRegister, SentRegister, UploadPlan, WrittenTrack,
    collect_certification, confirm_certifies, encode_blob, inline_write_fits, pack_registers,
    prepare_blob_register, prepare_raw_register, resolve_registers, send_registers,
    upload_with_retry,
};
use crate::transfer::certify::CollectedSignatures;

pub const MAX_OBJECT_BATCH_ITEMS: usize = 32;
pub const MAX_OBJECT_BATCH_BYTES: u64 = 256 * 1024 * 1024;

const MAX_ENCODE_WORKERS: usize = 4;
const RESOLVE_CONCURRENCY: usize = 3;
const COLLECT_CONCURRENCY: usize = 2;
const BATCH_STORE_DEPTH: usize = 4;

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

/// One object encoded and ready to register.
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

/// One object whose register landed, with what it still has to store.
struct ResolvedItem {
    index: usize,
    written: WrittenTrack,
    plan: Option<Box<UploadPlan>>,
}

/// One object stored on peers. A coded object keeps what its certify needs.
struct StoredItem {
    index: usize,
    address: Address,
    etag: Hash,
    certification: Option<StoredCertification>,
}

struct StoredCertification {
    written: WrittenTrack,
    receipts: Vec<CertifyRes>,
    collected: Option<CollectedSignatures>,
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
            let stored = self.store_objects_batch_inner(bucket, objects, true).await?;
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
    /// several to a transaction, while their confirmed event resolution and
    /// the coded slice uploads overlap them. The returned receipts can be published immediately; pass
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
        let result = self.store_objects_batch_inner(bucket, objects, false).await;
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
        should_collect: bool,
    ) -> Result<StoredObjectBatch, TapedriveError> {
        if self.progress.is_some() {
            let sizes = objects
                .iter()
                .map(|object| StorageUnits::from_bytes(object.data.len() as u64))
                .collect();
            self.report(Step::Planned { kind: UnitKind::Track, sizes });
        }

        let tape = self.get_tape(&bucket.address()).await?;
        let mirror = Mutex::new(ArchiveMirror::new(&tape.tracks));
        let encode_workers = std::thread::available_parallelism()
            .map(|cores| cores.get())
            .unwrap_or(1)
            .min(MAX_ENCODE_WORKERS);
        let store_depth = self.write_options.store_depth.unwrap_or(BATCH_STORE_DEPTH).max(1);
        let count = objects.len();
        let labels: Vec<(String, u64)> = objects
            .iter()
            .map(|object| (object.name.clone(), object.data.len() as u64))
            .collect();

        // Every channel holds the whole batch, so no stage waits on a slower one to send.
        let (prepared_sender, prepared_receiver) = mpsc::channel(count.max(1));
        let (sent_sender, sent_receiver) = mpsc::channel(count.max(1));
        let (resolved_sender, resolved_receiver) = mpsc::channel(count.max(1));

        let encode_stage = async move {
            let mut prepared = stream::iter(objects.into_iter().enumerate())
                .map(|(index, object)| self.prepare_object(index, object))
                .buffered(encode_workers);
            while let Some(item) = prepared.next().await {
                if prepared_sender.send(item?).await.is_err() {
                    break;
                }
            }
            Ok::<_, TapedriveError>(())
        };
        let register_stage = self.register_stage(bucket, prepared_receiver, sent_sender);
        let resolve_stage = self.resolve_stage(bucket, &mirror, sent_receiver, resolved_sender);
        let store_stage = self.store_stage(resolved_receiver, store_depth, should_collect);

        let ((), (), (), stored) =
            tokio::try_join!(encode_stage, register_stage, resolve_stage, store_stage)?;

        let mut receipts = vec![None; count];
        let mut pending = Vec::new();
        for item in stored {
            let (name, bytes_written) = labels
                .get(item.index)
                .ok_or_else(|| TapedriveError::InvalidArgument("batch receipt was lost".into()))?;
            if let Some(slot) = receipts.get_mut(item.index) {
                *slot = Some(ObjectBatchReceipt {
                    name: name.clone(),
                    address: item.address,
                    bytes_written: *bytes_written,
                    etag: item.etag,
                });
            }
            if let Some(certification) = item.certification {
                pending.push(certification);
            }
        }
        // Certifies prove against the mirror in track order.
        pending.sort_by_key(|certification| certification.written.track.track_number);

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

    /// Encode one object, or keep it whole when it fits inline.
    async fn prepare_object(
        &self,
        index: usize,
        object: ObjectBatchItem,
    ) -> Result<PreparedItem, TapedriveError> {
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
            Some(plan) if plan.storage_units == StorageUnits::from_bytes(bytes_written) => plan,
            Some(_) => {
                return Err(TapedriveError::InvalidArgument(format!(
                    "batch upload plan size does not match {}",
                    object.name
                )));
            }
            None => {
                self.report(Step::Encoding { unit: index });
                encode_blob(self, object.data, Operation::WriteBatch).await?
            }
        };
        Ok(PreparedItem::Coded {
            index,
            name: object.name,
            content_type: object.content_type,
            bytes_written,
            plan: Box::new(plan),
        })
    }

    /// Build the register for one prepared object.
    fn prepare_register(
        &self,
        bucket: &impl TapeOperator,
        item: PreparedItem,
    ) -> Result<PreparedRegister, TapedriveError> {
        let register = match item {
            PreparedItem::Inline { index, name, content_type, data } => prepare_raw_register(
                self,
                bucket,
                name.as_bytes(),
                content_type,
                StorageUnits::from_bytes(data.len() as u64),
                &data,
                self.write_options.flags,
            )?
            .for_unit(index),
            PreparedItem::Coded { index, name, content_type, bytes_written, plan } => {
                prepare_blob_register(
                    self,
                    bucket,
                    name.as_bytes(),
                    content_type,
                    StorageUnits::from_bytes(bytes_written),
                    *plan,
                    self.write_options.flags,
                )?
                .for_unit(index)
            }
        };
        Ok(register)
    }

    /// Send registers in input order, one transaction at a time, each as full
    /// as the objects encoded so far allow.
    ///
    /// A transaction goes out once the one before it is processed, and its
    /// confirmation wait moves to the resolve stage.
    async fn register_stage(
        &self,
        bucket: &impl TapeOperator,
        mut prepared: mpsc::Receiver<PreparedItem>,
        sent: mpsc::Sender<Vec<(usize, SentRegister)>>,
    ) -> Result<(), TapedriveError> {
        let mut waiting: Vec<PreparedRegister> = Vec::new();
        loop {
            if waiting.is_empty() {
                match prepared.recv().await {
                    Some(item) => waiting.push(self.prepare_register(bucket, item)?),
                    None => return Ok(()),
                }
            }
            while let Ok(item) = prepared.try_recv() {
                waiting.push(self.prepare_register(bucket, item)?);
            }

            let mut packed = pack_registers(self, bucket, std::mem::take(&mut waiting))?.into_iter();
            let Some(transaction) = packed.next() else {
                continue;
            };
            waiting = packed.flatten().collect();

            let mut units = Vec::with_capacity(transaction.len());
            for register in &transaction {
                units.push(register.unit().ok_or_else(|| {
                    TapedriveError::InvalidArgument("batch register has no unit".into())
                })?);
            }
            let registered = send_registers(self, bucket, transaction, Operation::WriteBatch).await?;
            if sent.send(units.into_iter().zip(registered).collect()).await.is_err() {
                return Ok(());
            }
        }
    }

    /// Read each register transaction's events once it confirms, and mirror
    /// its tracks in order.
    async fn resolve_stage(
        &self,
        bucket: &impl TapeOperator,
        mirror: &Mutex<ArchiveMirror>,
        mut sent: mpsc::Receiver<Vec<(usize, SentRegister)>>,
        resolved: mpsc::Sender<ResolvedItem>,
    ) -> Result<(), TapedriveError> {
        let mut in_flight = FuturesOrdered::new();
        let mut is_registering = true;
        while is_registering || !in_flight.is_empty() {
            tokio::select! {
                // Safe: recv is cancellation-safe and a transaction only leaves the
                // channel when this branch completes.
                transaction = sent.recv(), if is_registering && in_flight.len() < RESOLVE_CONCURRENCY => {
                    match transaction {
                        Some(transaction) => in_flight.push_back(self.resolve_transaction(transaction)),
                        None => is_registering = false,
                    }
                }
                // Safe: FuturesOrdered::next only removes a future once it
                // completes; a cancelled poll leaves every resolve in place.
                transaction = in_flight.next(), if !in_flight.is_empty() => {
                    let Some(transaction) = transaction else { continue };
                    for item in transaction? {
                        append_to_mirror(self, bucket, mirror, &item.written).await?;
                        self.report(Step::Storing { unit: item.index, track: item.written.address });
                        // An inline object is whole and certified once its register lands.
                        if item.plan.is_none() {
                            self.report(Step::Done { track: item.written.address });
                        }
                        if resolved.send(item).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn resolve_transaction(
        &self,
        transaction: Vec<(usize, SentRegister)>,
    ) -> Result<Vec<ResolvedItem>, TapedriveError> {
        let resolve = self
            .timer(Operation::WriteBatch, Phase::Resolve)
            .chunks(transaction.len() as u64);
        let (indexes, registers): (Vec<usize>, Vec<SentRegister>) = transaction.into_iter().unzip();
        let result = resolve_registers(self, registers).await;
        resolve.finish_result(&result);

        let mut items = Vec::with_capacity(indexes.len());
        for (index, register) in indexes.into_iter().zip(result?) {
            items.push(match register {
                ResolvedRegister::Raw(written) => ResolvedItem { index, written, plan: None },
                ResolvedRegister::Blob(written, plan) => {
                    ResolvedItem { index, written, plan: Some(plan) }
                }
            });
        }
        Ok(items)
    }

    /// Upload each coded object as its register resolves, and collect its
    /// certify signatures when the caller certifies straight after.
    async fn store_stage(
        &self,
        resolved: mpsc::Receiver<ResolvedItem>,
        store_depth: usize,
        should_collect: bool,
    ) -> Result<Vec<StoredItem>, TapedriveError> {
        stream::unfold(resolved, |mut resolved| async move {
            resolved.recv().await.map(|item| (item, resolved))
        })
            .map(|item| self.store_item(item, should_collect))
            .buffer_unordered(store_depth)
            .try_collect()
            .await
    }

    async fn store_item(
        &self,
        item: ResolvedItem,
        should_collect: bool,
    ) -> Result<StoredItem, TapedriveError> {
        let ResolvedItem { index, written, plan } = item;
        let Some(plan) = plan else {
            let etag = written.track.value_hash;
            return Ok(StoredItem { index, address: written.address, etag, certification: None });
        };

        let receipts = upload_with_retry(self, &written, &plan, Operation::WriteBatch).await?;
        let collected = match should_collect {
            true => Some(
                collect_certification(self, &written, Operation::WriteBatch, &receipts).await?,
            ),
            false => None,
        };
        Ok(StoredItem {
            index,
            address: written.address,
            etag: plan.commitment_hash,
            certification: Some(StoredCertification { written, receipts, collected }),
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
                let collected = match stored.collected {
                    Some(collected) => collected,
                    None => {
                        collect_certification(
                            self,
                            &stored.written,
                            Operation::WriteBatch,
                            &stored.receipts,
                        )
                        .await?
                    }
                };
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
