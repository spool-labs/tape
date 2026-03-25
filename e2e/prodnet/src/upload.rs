use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use rand::RngCore;
use rpc_solana::RpcConfig;
use tape_core::types::StorageUnits;
use tape_crypto::Hash;
use tape_retry::{Backoff, RetryConfig};
use tape_sdk::{TapeKey, Tapedrive, TapedriveError, load_solana_keypair};
use tracing::{error, info, warn};

use crate::view::UploadView;

const MAX_UPLOAD_HISTORY: usize = 16;
const DEFAULT_UPLOAD_EPOCHS: u64 = 4;
const MIN_UPLOAD_BYTES: usize = 1024;
const MAX_UPLOAD_BYTES: usize = 1024 * 1024;

pub struct UploadManager {
    rpc_url: String,
    admin_keypair_path: PathBuf,
    uploads: Arc<Mutex<VecDeque<UploadView>>>,
}

impl UploadManager {
    pub fn new(rpc_url: String, admin_keypair_path: PathBuf) -> Self {
        Self {
            rpc_url,
            admin_keypair_path,
            uploads: Arc::new(Mutex::new(VecDeque::new())),
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
        let (key, data) = random_blob();
        let tape_key = TapeKey::generate();
        let tape_id = tape_key.pubkey().to_string();

        let upload = UploadView {
            tape_id: tape_id.clone(),
            size_bytes: data.len() as u64,
            cert_status: "pending".into(),
            tape_address: tape_key.address().to_string(),
            track_address: tape_key.track_address(&key).to_string(),
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
            tape_id = %upload.tape_id,
            size_bytes = upload.size_bytes,
            "starting prodnet upload"
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
                Ok(certified) => {
                    let status = if certified { "yes" } else { "no" };
                    update_upload_status(&uploads, &tape_id, status, None);
                    info!(tape_id = %tape_id, certified, "prodnet upload completed");
                }
                Err(err) => {
                    update_upload_status(&uploads, &tape_id, "failed", Some(err.to_string()));
                    error!(tape_id = %tape_id, error = %err, "prodnet upload failed");
                }
            }
        });

        Ok(upload)
    }
}

fn random_blob() -> (Hash, Vec<u8>) {
    let mut rng = rand::thread_rng();
    let size = (rng.next_u32() as usize % (MAX_UPLOAD_BYTES - MIN_UPLOAD_BYTES))
        + MIN_UPLOAD_BYTES;
    let mut data = vec![0u8; size];
    rng.fill_bytes(&mut data);
    let key = tape_crypto::hash::hash(&data[..32.min(data.len())]);
    (key, data)
}

async fn run_upload(
    rpc_url: &str,
    admin_keypair_path: &Path,
    tape_key: &TapeKey,
    key: Hash,
    data: &[u8],
    uploads: &Arc<Mutex<VecDeque<UploadView>>>,
) -> Result<bool> {
    let tape_id = tape_key.pubkey().to_string();
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
    let mut reserved = false;
    let mut backoff = Backoff::new(RetryConfig {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(5),
        max_retries: Some(10),
    });

    loop {
        update_upload_status(uploads, &tape_id, "pending", None);

        if !reserved {
            match sdk
                .reserve(tape_key, reserve_capacity, DEFAULT_UPLOAD_EPOCHS)
                .await
            {
                Ok(_) => reserved = true,
                Err(error) if is_retriable_upload_error(&error) => {
                    if let Some(delay) = backoff.next_delay() {
                        update_upload_status(uploads, &tape_id, "retry", Some(error.to_string()));
                        warn!(
                            tape_id = %tape_id,
                            delay_ms = delay.as_millis() as u64,
                            error = %error,
                            "prodnet reserve failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(error).context("reserve tape");
                }
                Err(error) => return Err(error).context("reserve tape"),
            }
        }

        match sdk.write_track(tape_key, key, data).await {
            Ok(track) => return Ok(track.data.is_certified()),
            Err(error) if is_retriable_upload_error(&error) => {
                if let Some(delay) = backoff.next_delay() {
                    update_upload_status(uploads, &tape_id, "retry", Some(error.to_string()));
                    warn!(
                        tape_id = %tape_id,
                        delay_ms = delay.as_millis() as u64,
                        error = %error,
                        "prodnet upload failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(error).context("write track");
            }
            Err(error) => return Err(error).context("write track"),
        }
    }
}

fn update_upload_status(
    uploads: &Arc<Mutex<VecDeque<UploadView>>>,
    tape_id: &str,
    cert_status: &str,
    last_error: Option<String>,
) {
    let mut uploads = uploads.lock().expect("upload state mutex poisoned");
    if let Some(upload) = uploads.iter_mut().find(|upload| upload.tape_id == tape_id) {
        upload.cert_status.clear();
        upload.cert_status.push_str(cert_status);
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
