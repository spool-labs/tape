use std::collections::HashMap;
use std::time::{Duration, Instant};

use solana_sdk::pubkey::Pubkey;
use tape_crypto::Hash;
use tracing::Level;

#[derive(Debug, Clone)]
pub struct UploadRecord {
    pub key: Hash,
    pub data: Vec<u8>,
    pub track_address: Pubkey,
    pub epoch: u64,
    pub expiry_epoch: u64,
}

#[derive(Debug, Clone)]
pub struct EpochStats {
    pub epoch: u64,
    pub wall_duration: Duration,
    pub uploads: usize,
    pub uploaded_bytes: u64,
    pub network_size_bytes: u64,
    pub alive_count: usize,
    pub churn_stopped: usize,
    pub churn_started: usize,
    pub spools_active: usize,
    pub spools_sync: usize,
    pub spools_recover: usize,
    pub spools_locked: usize,
    pub committee_count: usize,
    pub sync_bytes: u64,
    pub repair_bytes: u64,
    pub log_counts: HashMap<(Level, String), u64>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FuzzStats {
    pub seed: u64,
    pub target_epochs: u64,
    pub node_count: usize,
    pub epochs: Vec<EpochStats>,
    pub upload_registry: Vec<UploadRecord>,
    pub download_results: Vec<(Pubkey, bool)>,
    pub phase: FuzzPhase,
    pub start_time: Instant,
}

#[derive(Debug, Clone)]
pub enum FuzzPhase {
    Bootstrap,
    Warmup,
    Fuzzing {
        current_epoch: u64,
        target_epoch: u64,
    },
    Done {
        passed: bool,
    },
}

impl FuzzStats {
    pub fn new(seed: u64, target_epochs: u64, node_count: usize) -> Self {
        Self {
            seed,
            target_epochs,
            node_count,
            epochs: Vec::new(),
            upload_registry: Vec::new(),
            download_results: Vec::new(),
            phase: FuzzPhase::Bootstrap,
            start_time: Instant::now(),
        }
    }

    pub fn uploaded_bytes_total(&self) -> u64 {
        self.upload_registry
            .iter()
            .map(|record| u64::try_from(record.data.len()).unwrap_or(u64::MAX))
            .sum()
    }

    pub fn epoch_durations_secs(&self) -> Vec<f64> {
        self.epochs
            .iter()
            .map(|epoch| epoch.wall_duration.as_secs_f64())
            .collect()
    }

    pub fn downloaded_count(&self) -> (usize, usize) {
        let checked = self.download_results.len();
        let passed = self.download_results.iter().filter(|(_, ok)| *ok).count();
        (checked, passed)
    }

    pub fn total_uploads(&self) -> usize {
        self.epochs.iter().map(|e| e.uploads).sum()
    }

    pub fn total_churn_stopped(&self) -> usize {
        self.epochs.iter().map(|e| e.churn_stopped).sum()
    }

    pub fn total_churn_started(&self) -> usize {
        self.epochs.iter().map(|e| e.churn_started).sum()
    }

    pub fn total_warnings(&self) -> usize {
        self.epochs.iter().map(|e| e.warnings.len()).sum()
    }
}
