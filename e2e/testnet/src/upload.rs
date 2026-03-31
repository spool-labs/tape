use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use rand::RngCore;
use rpc_solana::RpcConfig;
use tape_api::program::tapedrive::track_pda;
use tape_core::types::StorageUnits;
use tape_crypto::Hash;
use tape_retry::{Backoff, RetryConfig};
use tape_sdk::{
    SDK_INLINE_RAW_MAX_BYTES, TapeKey, Tapedrive, TapedriveError, load_solana_keypair,
};
use tracing::{error, info, warn};

use crate::view::UploadView;

const MAX_UPLOAD_HISTORY: usize = 16;
const DEFAULT_UPLOAD_EPOCHS: u64 = 4;
const MIN_RAW_UPLOAD_BYTES: usize = 64;
const MIN_BLOB_UPLOAD_BYTES: usize = 1024 * 1024;
const MAX_BLOB_UPLOAD_BYTES: usize = 28 * 1024 * 1024;

struct UploadResult {
    certified: bool,
    track_address: String,
}

pub struct UploadManager {
    rpc_url: String,
    admin_keypair_path: PathBuf,
    uploads: Arc<Mutex<VecDeque<UploadView>>>,
    upload_seq: AtomicUsize,
}

impl UploadManager {
    pub fn new(rpc_url: String, admin_keypair_path: PathBuf) -> Self {
        Self {
            rpc_url,
            admin_keypair_path,
            uploads: Arc::new(Mutex::new(VecDeque::new())),
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
        let force_raw = upload_number % 5 == 0;
        let (key, data) = random_blob(force_raw);
        let tape_key = TapeKey::generate();
        let tape_address = tape_key.address().to_string();

        let upload = UploadView {
            size_bytes: data.len() as u64,
            cert_status: "pending".into(),
            tape_address: tape_address.clone(),
            track_address: None,
            last_error: None,
        };

        {
            let mut uploads = self.uploads.lock().expect("upload state mutex poisoned");
            uploads.push_front(upload.clone());
            while uploads.len() > MAX_UPLOAD_HISTORY {
                uploads.pop_back();
            }
        }

        info!(
            tape = %upload.tape_address,
            mode = if force_raw { "raw" } else { "blob" },
            upload_number,
            size_bytes = upload.size_bytes,
            "starting testnet upload"
        );

        let rpc_url = self.rpc_url.clone();
        let admin_keypair_path = self.admin_keypair_path.clone();
        let uploads = self.uploads.clone();
        tokio::spawn(async move {
            match run_upload(
                &rpc_url,
                &admin_keypair_path,
                &tape_key,
                key,
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
                        Some(result.track_address),
                        None,
                    );
                    info!(tape = %tape_address, certified = result.certified, "testnet upload completed");
                }
                Err(err) => {
                    let details = format_error_chain(&err);
                    update_upload_status(&uploads, &tape_address, "failed", None, Some(details.clone()));
                    error!(tape = %tape_address, error = %details, "testnet upload failed");
                }
            }
        });

        Ok(upload)
    }
}

fn random_blob(force_raw: bool) -> (Hash, Vec<u8>) {
    let mut rng = rand::thread_rng();
    let size = if force_raw {
        let span = SDK_INLINE_RAW_MAX_BYTES - MIN_RAW_UPLOAD_BYTES + 1;
        (rng.next_u32() as usize % span) + MIN_RAW_UPLOAD_BYTES
    } else {
        let span = MAX_BLOB_UPLOAD_BYTES - MIN_BLOB_UPLOAD_BYTES + 1;
        (rng.next_u32() as usize % span) + MIN_BLOB_UPLOAD_BYTES
    };
    let mut data = vec![0u8; size];
    rng.fill_bytes(&mut data);
    let key = tape_crypto::hash::hash(&data[..32.min(data.len())]);
    (key, data)
}

fn format_error_chain(error: &anyhow::Error) -> String {
    format!("{error:#}")
}

async fn run_upload(
    rpc_url: &str,
    admin_keypair_path: &Path,
    tape_key: &TapeKey,
    key: Hash,
    data: &[u8],
    uploads: &Arc<Mutex<VecDeque<UploadView>>>,
) -> Result<UploadResult> {
    let tape_address = tape_key.address().to_string();
    let admin = load_solana_keypair(admin_keypair_path)
        .with_context(|| format!("load uploader keypair: {}", admin_keypair_path.display()))?;
    let rpc = rpc_solana::SolanaRpc::new(RpcConfig {
        endpoints: vec![rpc_url.to_string()],
        ..Default::default()
    })
    .context("create upload rpc client")?;

    let sdk = Tapedrive::new(rpc, &admin);
    let capacity = StorageUnits::from_bytes(data.len() as u64);
    let reserve_capacity = capacity + StorageUnits::mb(1);
    let mut backoff = Backoff::new(RetryConfig {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(5),
        max_retries: Some(10),
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
                        "testnet reserve failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(error).context("reserve tape");
            }
            Err(error) => return Err(error).context("reserve tape"),
        }
    }

    update_upload_status(uploads, &tape_address, "pending", None, None);

    let track = sdk.write_track(tape_key, key, data)
        .await
        .context("write track")?;
    let track_address = track_pda(track.tape, track.track_number).0.to_string();
    Ok(UploadResult {
        certified: track.is_certified(),
        track_address,
    })
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
