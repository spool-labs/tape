use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use rand::RngCore;
use rpc_solana::{RpcConfig, SolanaRpc};
use tape_api::program::tapedrive::track_pda;
use tape_core::types::StorageUnits;
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_retry::{Backoff, RetryConfig};
use tape_sdk::error::TapedriveError;
use tape_sdk::keys::helpers::load_solana_keypair;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::tapedrive::Tapedrive;
use tokio::spawn;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::view::UploadView;

/// Uploads kept for reporting. A soak run wants every row it started, not a
/// window of the last few.
const MAX_UPLOAD_HISTORY: usize = 4096;
const DEFAULT_UPLOAD_EPOCHS: u64 = 100;
const MAX_RAW_UPLOAD_BYTES: usize = 825;
const MIN_RAW_UPLOAD_BYTES: usize = 64;
const MIN_BLOB_UPLOAD_BYTES: usize = 1024 * 1024;
const MAX_BLOB_UPLOAD_BYTES: usize = 64 * 1024 * 1024;

struct UploadResult {
    certified: bool,
    track: Address,
}

struct DeleteHandle {
    key: Arc<TapeKey>,
    track: Option<Address>,
}

pub struct UploadManager {
    rpc_url: String,
    admin_keypair_path: PathBuf,
    uploads: Arc<Mutex<VecDeque<UploadView>>>,
    deletable: Arc<Mutex<HashMap<String, DeleteHandle>>>,
    upload_seq: AtomicUsize,
}

impl UploadManager {
    pub fn new(rpc_url: String, admin_keypair_path: PathBuf) -> Self {
        Self {
            rpc_url,
            admin_keypair_path,
            uploads: Arc::new(Mutex::new(VecDeque::new())),
            deletable: Arc::new(Mutex::new(HashMap::new())),
            upload_seq: AtomicUsize::new(0),
        }
    }

    pub fn snapshot(&self) -> Vec<UploadView> {
        self.uploads
            .lock()
            .expect("upload state mutex poisoned")
            .iter()
            .cloned()
            .collect()
    }

    pub fn start_random_upload(&self) -> Result<UploadView> {
        let upload_number = self.upload_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let force_raw = upload_number.is_multiple_of(5);
        let data = random_blob(force_raw);
        let tape_key = Arc::new(TapeKey::generate());
        let tape_address = tape_key.address().to_string();

        let upload = UploadView {
            size_bytes: data.len() as u64,
            cert_status: "pending".into(),
            tape_address: tape_address.clone(),
            track_address: None,
            last_error: None,
            started_ms: now_ms(),
            settled_ms: None,
        };

        let evicted = {
            let mut uploads = self.uploads.lock().expect("upload state mutex poisoned");
            uploads.push_front(upload.clone());
            let mut evicted = Vec::new();
            while uploads.len() > MAX_UPLOAD_HISTORY {
                if let Some(old) = uploads.pop_back() {
                    evicted.push(old.tape_address);
                }
            }
            evicted
        };
        {
            let mut deletable = self.deletable.lock().expect("delete state mutex poisoned");
            for old in evicted {
                deletable.remove(&old);
            }
            deletable.insert(
                tape_address.clone(),
                DeleteHandle {
                    key: tape_key.clone(),
                    track: None,
                },
            );
        }

        info!(
            tape = %upload.tape_address,
            mode = if force_raw { "raw" } else { "blob" },
            upload_number,
            size_bytes = upload.size_bytes,
            "starting localnet upload"
        );

        let rpc_url = self.rpc_url.clone();
        let admin_keypair_path = self.admin_keypair_path.clone();
        let uploads = self.uploads.clone();
        let deletable = self.deletable.clone();
        spawn(async move {
            match run_upload(
                &rpc_url,
                &admin_keypair_path,
                &tape_key,
                &data,
                &uploads,
            )
            .await
            {
                Ok(result) => {
                    let status = if result.certified { "yes" } else { "no" };
                    update_upload_status(
                        &uploads,
                        &tape_address,
                        status,
                        Some(result.track.to_string()),
                        None,
                    );
                    let mut deletable = deletable.lock().expect("delete state mutex poisoned");
                    if let Some(handle) = deletable.get_mut(&tape_address) {
                        handle.track = Some(result.track);
                    }
                    info!(tape = %tape_address, certified = result.certified, "localnet upload completed");
                }
                Err(err) => {
                    let details = format_error_chain(&err);
                    update_upload_status(&uploads, &tape_address, "failed", None, Some(details.clone()));
                    error!(tape = %tape_address, error = %details, "localnet upload failed");
                }
            }
        });

        Ok(upload)
    }

    /// Delete the newest completed upload's track, returning its tape address.
    pub fn delete_latest(&self) -> Result<Option<String>> {
        let target = {
            let uploads = self.uploads.lock().expect("upload state mutex poisoned");
            uploads
                .iter()
                .find(|upload| matches!(upload.cert_status.as_str(), "yes" | "no"))
                .cloned()
        };
        let Some(view) = target else {
            return Ok(None);
        };

        let handle = {
            let deletable = self.deletable.lock().expect("delete state mutex poisoned");
            deletable
                .get(&view.tape_address)
                .and_then(|handle| handle.track.map(|track| (handle.key.clone(), track)))
        };
        let Some((key, track)) = handle else {
            return Ok(None);
        };

        update_upload_status(&self.uploads, &view.tape_address, "deleting", None, None);
        info!(tape = %view.tape_address, track = %track, "deleting localnet track");

        let rpc_url = self.rpc_url.clone();
        let admin_keypair_path = self.admin_keypair_path.clone();
        let uploads = self.uploads.clone();
        let tape_address = view.tape_address.clone();
        spawn(async move {
            match run_delete(&rpc_url, &admin_keypair_path, &key, track).await {
                Ok(()) => {
                    update_upload_status(&uploads, &tape_address, "deleted", None, None);
                    info!(tape = %tape_address, "localnet track deleted");
                }
                Err(err) => {
                    let details = format_error_chain(&err);
                    update_upload_status(&uploads, &tape_address, "delfail", None, Some(details.clone()));
                    error!(tape = %tape_address, error = %details, "localnet delete failed");
                }
            }
        });

        Ok(Some(view.tape_address))
    }
}

async fn run_delete(
    rpc_url: &str,
    admin_keypair_path: &Path,
    tape_key: &TapeKey,
    track: Address,
) -> Result<()> {
    let admin = load_solana_keypair(admin_keypair_path)
        .with_context(|| format!("load deleter keypair: {}", admin_keypair_path.display()))?;
    let rpc = SolanaRpc::new(RpcConfig {
        endpoints: vec![rpc_url.to_string()],
        ..Default::default()
    })
    .context("create delete rpc client")?;
    let admin = CryptoKeypair::from_solana_keypair(&admin).context("convert deleter keypair")?;

    let sdk = Tapedrive::new(rpc, admin);
    sdk.delete(tape_key, track).await.context("delete track")?;
    Ok(())
}

/// Environment variable naming the sizes a run cycles through, in bytes
///
/// Comma separated, and taken in order rather than at random, so a run of N
/// uploads covers each size the same number of times. Unset keeps the random
/// spread, which is what an unattended fleet run wants.
const SIZES_VAR: &str = "LOCALNET_UPLOAD_SIZES";

/// The sizes a run was asked to cycle through, empty unless it named any
fn asked_sizes() -> Vec<usize> {
    let Ok(raw) = std::env::var(SIZES_VAR) else {
        return Vec::new();
    };
    let mut sizes = Vec::new();
    for field in raw.split(',') {
        match field.trim().parse::<usize>() {
            Ok(size) if size > 0 => sizes.push(size),
            _ => continue,
        }
    }
    sizes
}

fn random_blob(force_raw: bool) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let size = {
        let asked = asked_sizes();
        match asked.is_empty() {
            false => asked[(rng.next_u32() as usize) % asked.len()],
            true if force_raw => {
                let span = MAX_RAW_UPLOAD_BYTES - MIN_RAW_UPLOAD_BYTES + 1;
                (rng.next_u32() as usize % span) + MIN_RAW_UPLOAD_BYTES
            }
            true => {
                let span = MAX_BLOB_UPLOAD_BYTES - MIN_BLOB_UPLOAD_BYTES + 1;
                (rng.next_u32() as usize % span) + MIN_BLOB_UPLOAD_BYTES
            }
        }
    };
    let mut data = vec![0u8; size];
    rng.fill_bytes(&mut data);
    data
}

fn format_error_chain(error: &anyhow::Error) -> String {
    format!("{error:#}")
}

/// Round a track size up to the next power-of-ten MiB (35 MiB -> 100 MiB).
fn reserve_capacity_for(size: StorageUnits) -> StorageUnits {
    let mib = size.to_mb();
    let mut tens = 1u64;
    while tens < mib {
        tens = tens.saturating_mul(10);
    }
    StorageUnits::mb(tens)
}

async fn run_upload(
    rpc_url: &str,
    admin_keypair_path: &Path,
    tape_key: &TapeKey,
    data: &[u8],
    uploads: &Arc<Mutex<VecDeque<UploadView>>>,
) -> Result<UploadResult> {
    let tape_address = tape_key.address().to_string();
    let admin = load_solana_keypair(admin_keypair_path)
        .with_context(|| format!("load uploader keypair: {}", admin_keypair_path.display()))?;
    let rpc = SolanaRpc::new(RpcConfig {
        endpoints: vec![rpc_url.to_string()],
        ..Default::default()
    })
    .context("create upload rpc client")?;

    let admin = CryptoKeypair::from_solana_keypair(&admin)
        .context("convert uploader keypair")?;
    let sdk = Tapedrive::new(rpc, admin);
    let reserve_capacity = reserve_capacity_for(StorageUnits::from_bytes(data.len() as u64));
    let mut backoff = Backoff::new(RetryConfig {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(60),
        max_retries: None,
    });

    loop {
        update_upload_status(uploads, &tape_address, "pending", None, None);

        match sdk
            .reserve(tape_key, reserve_capacity, DEFAULT_UPLOAD_EPOCHS)
            .await
        {
            Ok(_) => break,
            Err(error) if is_retriable_upload_error(&error) => {
                if let Some(delay) = backoff.next_delay() {
                    let details = error.to_string();
                    update_upload_status(
                        uploads,
                        &tape_address,
                        "retry",
                        None,
                        Some(details.clone()),
                    );
                    warn!(
                        tape = %tape_address,
                        delay_ms = delay.as_millis() as u64,
                        error = %details,
                        "localnet reserve failed, retrying"
                    );
                    sleep(delay).await;
                    continue;
                }
                return Err(error).context("reserve tape");
            }
            Err(error) => return Err(error).context("reserve tape"),
        }
    }

    update_upload_status(uploads, &tape_address, "pending", None, None);

    let track = sdk
        .write_track(tape_key, data)
        .await
        .context("write track")?;
    Ok(UploadResult {
        certified: track.is_certified(),
        track: track_pda(track.tape, track.track_number).0,
    })
}

/// Unix milliseconds now
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

fn update_upload_status(
    uploads: &Arc<Mutex<VecDeque<UploadView>>>,
    tape_address: &str,
    cert_status: &str,
    track_address: Option<String>,
    last_error: Option<String>,
) {
    let mut uploads = uploads.lock().expect("upload state mutex poisoned");
    if let Some(upload) = uploads
        .iter_mut()
        .find(|upload| upload.tape_address == tape_address)
    {
        upload.cert_status.clear();
        upload.cert_status.push_str(cert_status);
        if let Some(track_address) = track_address {
            upload.track_address = Some(track_address);
        }
        upload.last_error = last_error;

        // Timed once, at the first terminal answer, so a later status write does
        // not restate the elapsed time as something longer.
        if upload.settled_ms.is_none() && matches!(cert_status, "yes" | "no") {
            upload.settled_ms = Some(now_ms().saturating_sub(upload.started_ms));
        }
    }
}

fn is_retriable_upload_error(error: &TapedriveError) -> bool {
    !matches!(
        error,
        TapedriveError::CommitmentMismatch
            | TapedriveError::InvalidArgument(_)
            | TapedriveError::InsufficientCapacity { .. }
    )
}
