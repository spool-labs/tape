//! Supervisor — centralized task scheduler with retry, cancellation, and concurrency limits.
//!
//! The supervisor owns:
//! - A `BinaryHeap` of due times for retry scheduling (scales to millions of entries)
//! - A `JoinSet` tracking all spawned worker futures
//! - Per-category `Semaphore`s for concurrency limits
//! - Per-task `CancellationToken`s for cancellation
//!
//! A single scheduler loop does `sleep_until(next_due)`, pops due items, acquires
//! the appropriate semaphore, and dispatches to workers. On retryable failure,
//! `BackoffConfig` computes the next delay and the item is pushed back to the heap.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use solana_sdk::pubkey::Pubkey;
use store::Store;
use tape_core::spooler::SpoolIndex;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use rand::Rng;

use crate::core::{BackoffConfig, NodeContext};
use crate::reconciler::Directive;

/// Identifies a scheduled or running task.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TaskKey {
    /// Advance the on-chain epoch.
    AdvanceEpoch,
    /// Sync this node's epoch state on-chain.
    SyncEpoch,
    /// Join the network on-chain.
    JoinNetwork,
    /// Advance a staking pool on-chain.
    AdvancePool,
    /// Register a snapshot commitment on-chain.
    RegisterSnapshot,
    /// Certify a snapshot with BLS aggregate on-chain.
    CertifySnapshot,
    /// Invalidate a track on-chain.
    InvalidateTrack { track: Pubkey },
    /// Sync a spool from a peer.
    SpoolSync { spool: SpoolIndex },
    /// Scan for missing slices in a spool.
    RecoveryScan { spool: SpoolIndex },
    /// Recover missing slices for a spool.
    SpoolRecovery { spool: SpoolIndex },
    /// Build a snapshot for the current epoch.
    SnapshotBuild,
    /// Certify a snapshot by collecting BLS signatures.
    SnapshotCertify,
    /// Bootstrap from a snapshot (new node joining).
    SnapshotBootstrap,
    /// Refresh cached on-chain state.
    RefreshOnchainState,
}

impl TaskKey {
    fn category(&self) -> TaskCategory {
        match self {
            TaskKey::AdvanceEpoch
            | TaskKey::SyncEpoch
            | TaskKey::JoinNetwork
            | TaskKey::AdvancePool
            | TaskKey::RegisterSnapshot
            | TaskKey::CertifySnapshot
            | TaskKey::InvalidateTrack { .. } => TaskCategory::SolanaTx,

            TaskKey::SpoolSync { .. }
            | TaskKey::SpoolRecovery { .. }
            | TaskKey::RecoveryScan { .. } => TaskCategory::PeerHttp,

            TaskKey::SnapshotBuild | TaskKey::SnapshotCertify => TaskCategory::CpuHeavy,

            TaskKey::SnapshotBootstrap => TaskCategory::PeerHttp,
            TaskKey::RefreshOnchainState => TaskCategory::Internal,
        }
    }

    /// One-shot tasks complete once and are removed from desired.
    /// Continuous tasks remain desired until state changes remove them.
    pub fn is_one_shot(&self) -> bool {
        matches!(
            self,
            TaskKey::AdvanceEpoch
                | TaskKey::SyncEpoch
                | TaskKey::JoinNetwork
                | TaskKey::AdvancePool
                | TaskKey::RegisterSnapshot
                | TaskKey::CertifySnapshot
                | TaskKey::InvalidateTrack { .. }
                | TaskKey::RefreshOnchainState
                | TaskKey::RecoveryScan { .. }
                | TaskKey::SpoolRecovery { .. }
                | TaskKey::SnapshotBuild
                | TaskKey::SnapshotCertify
                | TaskKey::SnapshotBootstrap
                | TaskKey::SpoolSync { .. }
        )
    }
}

/// Classifies tasks for concurrency limiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskCategory {
    /// Solana transaction submission (semaphore: 5)
    SolanaTx,
    /// HTTP calls to peer nodes (semaphore: 50)
    PeerHttp,
    /// CPU-heavy work like erasure coding (semaphore: num_cpus)
    CpuHeavy,
    /// Internal bookkeeping with no concurrency limit
    Internal,
}

/// Outcome of a single task execution attempt.
#[derive(Debug)]
pub enum TaskOutcome {
    Success,
    Retryable(String),
    Permanent(String),
}

/// Result of a completed task, returned to the reconciler.
#[derive(Debug)]
pub enum TaskResult {
    /// Task completed successfully.
    Success(TaskKey),
    /// Task failed with a retryable error.
    RetryableError(TaskKey, String),
    /// Task failed permanently.
    PermanentError(TaskKey, String),
}

/// Entry in the retry heap.
struct RetryEntry {
    due: tokio::time::Instant,
    key: TaskKey,
    attempt: u32,
}

impl PartialEq for RetryEntry {
    fn eq(&self, other: &Self) -> bool {
        self.due == other.due
    }
}

impl Eq for RetryEntry {}

impl PartialOrd for RetryEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RetryEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.due.cmp(&other.due)
    }
}

/// Tracking state for a running task.
struct RunningTask {
    #[allow(dead_code)]
    category: TaskCategory,
    #[allow(dead_code)]
    started_at: tokio::time::Instant,
    attempt: u32,
}

fn backoff_for(category: TaskCategory) -> BackoffConfig {
    match category {
        TaskCategory::SolanaTx => BackoffConfig {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_retries: Some(20),
        },
        TaskCategory::PeerHttp => BackoffConfig {
            min_delay: Duration::from_secs(2),
            max_delay: Duration::from_secs(300),
            max_retries: Some(50),
        },
        TaskCategory::CpuHeavy => BackoffConfig {
            min_delay: Duration::from_secs(30),
            max_delay: Duration::from_secs(300),
            max_retries: None,
        },
        TaskCategory::Internal => BackoffConfig {
            min_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(60),
            max_retries: Some(10),
        },
    }
}

/// Compute the delay for a given attempt using exponential backoff with half-jitter.
fn compute_delay(config: &BackoffConfig, attempt: u32) -> Option<Duration> {
    if let Some(max) = config.max_retries {
        if attempt >= max {
            return None;
        }
    }
    let base = config.min_delay * 2u32.saturating_pow(attempt);
    let base = base.min(config.max_delay);

    // Half-jitter: uniform(base/2, base) to break thundering herd
    let half = base / 2;
    let jitter = Duration::from_millis(
        rand::thread_rng().gen_range(0..=half.as_millis() as u64)
    );
    Some(half + jitter)
}

fn far_future() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(365 * 24 * 3600)
}

/// Centralized task scheduler.
pub struct Supervisor<S: Store> {
    context: Arc<NodeContext<S>>,
    running: HashMap<TaskKey, RunningTask>,
    tokens: HashMap<TaskKey, CancellationToken>,
    join_set: JoinSet<(TaskKey, TaskOutcome)>,
    retry_queue: BinaryHeap<Reverse<RetryEntry>>,
    pending_retry: HashSet<TaskKey>,
    result_tx: mpsc::Sender<TaskResult>,

    sem_solana_tx: Arc<Semaphore>,
    sem_peer_http: Arc<Semaphore>,
    sem_cpu_heavy: Arc<Semaphore>,
}

impl<S: Store + 'static> Supervisor<S> {
    pub fn new(context: Arc<NodeContext<S>>, result_tx: mpsc::Sender<TaskResult>) -> Self {
        let cpu_count = std::thread::available_parallelism()
            .map_or(4, |n| n.get());

        Self {
            context,
            running: HashMap::new(),
            tokens: HashMap::new(),
            join_set: JoinSet::new(),
            retry_queue: BinaryHeap::new(),
            pending_retry: HashSet::new(),
            result_tx,
            sem_solana_tx: Arc::new(Semaphore::new(5)),
            sem_peer_http: Arc::new(Semaphore::new(50)),
            sem_cpu_heavy: Arc::new(Semaphore::new(cpu_count)),
        }
    }

    pub async fn run(
        mut self,
        mut directive_rx: mpsc::Receiver<Directive>,
        cancel: CancellationToken,
    ) {
        loop {
            let next_retry = self.next_retry_instant();

            tokio::select! {
                directive = directive_rx.recv() => {
                    match directive {
                        Some(Directive::Schedule(key)) => self.handle_schedule(key, 0),
                        Some(Directive::Cancel(key)) => self.handle_cancel(&key),
                        None => break,
                    }
                }

                Some(result) = self.join_set.join_next() => {
                    match result {
                        Ok((key, outcome)) => self.handle_completion(key, outcome).await,
                        Err(e) => tracing::error!("task panicked: {e}"),
                    }
                }

                _ = tokio::time::sleep_until(next_retry) => {
                    self.process_retries();
                }

                _ = cancel.cancelled() => {
                    self.shutdown().await;
                    break;
                }
            }
        }
    }

    fn handle_schedule(&mut self, key: TaskKey, attempt: u32) {
        if self.running.contains_key(&key) || self.pending_retry.contains(&key) {
            return;
        }
        self.spawn_task(key, attempt);
    }

    fn handle_cancel(&mut self, key: &TaskKey) {
        if let Some(token) = self.tokens.remove(key) {
            token.cancel();
        }
        self.running.remove(key);
        self.pending_retry.remove(key);
    }

    async fn handle_completion(&mut self, key: TaskKey, outcome: TaskOutcome) {
        let attempt = self
            .running
            .remove(&key)
            .map(|r| r.attempt)
            .unwrap_or(0);
        self.tokens.remove(&key);

        match outcome {
            TaskOutcome::Success => {
                if self.result_tx.send(TaskResult::Success(key)).await.is_err() {
                    tracing::debug!("result channel closed");
                }
            }
            TaskOutcome::Retryable(err) => {
                let category = key.category();
                let config = backoff_for(category);
                match compute_delay(&config, attempt) {
                    Some(delay) => {
                        let due = tokio::time::Instant::now() + delay;
                        tracing::warn!(
                            task = ?key,
                            attempt,
                            delay_secs = delay.as_secs(),
                            error = %err,
                            "scheduling retry"
                        );
                        self.retry_queue.push(Reverse(RetryEntry {
                            due,
                            key: key.clone(),
                            attempt: attempt + 1,
                        }));
                        self.pending_retry.insert(key.clone());
                        if self
                            .result_tx
                            .send(TaskResult::RetryableError(key, err))
                            .await
                            .is_err()
                        {
                            tracing::debug!("result channel closed");
                        }
                    }
                    None => {
                        tracing::error!(
                            task = ?key,
                            attempt,
                            "max retries exceeded, treating as permanent failure"
                        );
                        if self
                            .result_tx
                            .send(TaskResult::PermanentError(key, err))
                            .await
                            .is_err()
                        {
                            tracing::debug!("result channel closed");
                        }
                    }
                }
            }
            TaskOutcome::Permanent(err) => {
                if self
                    .result_tx
                    .send(TaskResult::PermanentError(key, err))
                    .await
                    .is_err()
                {
                    tracing::debug!("result channel closed");
                }
            }
        }
    }

    fn process_retries(&mut self) {
        let now = tokio::time::Instant::now();
        while let Some(entry) = self.retry_queue.peek() {
            if entry.0.due > now {
                break;
            }
            let entry = self.retry_queue.pop().unwrap().0;
            if self.pending_retry.remove(&entry.key) {
                self.spawn_task(entry.key, entry.attempt);
            }
        }
    }

    async fn shutdown(&mut self) {
        for token in self.tokens.values() {
            token.cancel();
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            tokio::select! {
                result = self.join_set.join_next() => {
                    match result {
                        Some(Ok((key, _))) => {
                            tracing::debug!(task = ?key, "task finished during shutdown");
                        }
                        Some(Err(e)) => {
                            tracing::warn!("task panicked during shutdown: {e}");
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let remaining = self.running.len();
                    if remaining > 0 {
                        tracing::warn!(
                            remaining,
                            "shutdown timeout, tasks did not complete"
                        );
                    }
                    break;
                }
            }
        }

        self.running.clear();
        self.tokens.clear();
    }

    fn spawn_task(&mut self, key: TaskKey, attempt: u32) {
        let token = CancellationToken::new();
        let sem = self.semaphore_for(key.category());
        let ctx = self.context.clone();
        let token_clone = token.clone();
        let category = key.category();
        let k = key.clone();

        self.join_set
            .spawn(crate::tasks::execute_task(ctx, k, token_clone, sem));

        self.running.insert(
            key.clone(),
            RunningTask {
                category,
                started_at: tokio::time::Instant::now(),
                attempt,
            },
        );
        self.tokens.insert(key, token);
    }

    fn semaphore_for(&self, category: TaskCategory) -> Arc<Semaphore> {
        match category {
            TaskCategory::SolanaTx => self.sem_solana_tx.clone(),
            TaskCategory::PeerHttp => self.sem_peer_http.clone(),
            TaskCategory::CpuHeavy => self.sem_cpu_heavy.clone(),
            // Internal tasks have no concurrency limit — use a large semaphore
            TaskCategory::Internal => Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
        }
    }

    fn next_retry_instant(&self) -> tokio::time::Instant {
        self.retry_queue
            .peek()
            .map(|e| e.0.due)
            .unwrap_or_else(far_future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use tape_core::bls::BlsPrivateKey;
    use tape_store::{MemoryStore, TapeStore};

    use crate::core::config::RecoveryConfig;
    use crate::core::{NodeApiConfig, NodeConfig, NodeContext, TlsConfig};

    fn test_config() -> NodeConfig {
        NodeConfig {
            version: 1,
            name: "test-node".to_string(),
            tls_keypair: PathBuf::from("/dev/null"),
            bls_keypair: PathBuf::from("/dev/null"),
            node_keypair: String::new(),
            bind_address: "127.0.0.1:0".parse().unwrap(),
            public_host: "localhost".to_string(),
            public_port: 0,
            tls: TlsConfig::default(),
            storage_path: "/tmp".to_string(),
            poll_interval_ms: None,
            sync_concurrency: None,
            sync_batch_size: None,
            commission: None,
            recovery: RecoveryConfig::default(),
            node_api: NodeApiConfig::default(),
        }
    }

    fn test_context() -> Arc<NodeContext<MemoryStore>> {
        let config = test_config();
        let keypair = solana_sdk::signature::Keypair::new();
        let bls_keypair = BlsPrivateKey::from_random();
        let store = TapeStore::new(MemoryStore::new());
        NodeContext::new(config, keypair, bls_keypair, store)
    }

    #[tokio::test]
    async fn schedule_and_complete() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (directive_tx, directive_rx) = mpsc::channel(16);

        let supervisor = Supervisor::new(ctx, result_tx);
        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move {
            supervisor.run(directive_rx, cancel_clone).await;
        });

        let key = TaskKey::RecoveryScan { spool: 0 };
        directive_tx
            .send(Directive::Schedule(key.clone()))
            .await
            .unwrap();

        let result = result_rx.recv().await.unwrap();
        assert!(matches!(result, TaskResult::Success(ref k) if *k == key));

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_running() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (directive_tx, directive_rx) = mpsc::channel(16);

        let supervisor = Supervisor::new(ctx, result_tx);
        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move {
            supervisor.run(directive_rx, cancel_clone).await;
        });

        let key = TaskKey::SpoolSync { spool: 42 };

        // Schedule then immediately cancel
        directive_tx
            .send(Directive::Schedule(key.clone()))
            .await
            .unwrap();
        directive_tx
            .send(Directive::Cancel(key.clone()))
            .await
            .unwrap();

        // Give the supervisor time to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The task may have completed before cancel was processed (stub is instant),
        // so we drain whatever results arrived. The key point is that the supervisor
        // doesn't panic and handles the cancel gracefully.
        result_rx.close();
        while result_rx.recv().await.is_some() {}

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn retry_on_failure() {
        let ctx = test_context();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let mut supervisor = Supervisor::new(ctx, result_tx);

        let key = TaskKey::RefreshOnchainState;

        // Simulate a running task
        supervisor.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::Internal,
                started_at: tokio::time::Instant::now(),
                attempt: 0,
            },
        );
        supervisor
            .tokens
            .insert(key.clone(), CancellationToken::new());

        // Handle a retryable completion
        supervisor
            .handle_completion(key.clone(), TaskOutcome::Retryable("transient".into()))
            .await;

        // Should be in retry queue
        assert!(supervisor.pending_retry.contains(&key));
        assert!(!supervisor.retry_queue.is_empty());

        // Result should have been sent
        let result = result_rx.try_recv().unwrap();
        assert!(matches!(result, TaskResult::RetryableError(..)));
    }

    #[tokio::test]
    async fn permanent_failure() {
        let ctx = test_context();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let mut supervisor = Supervisor::new(ctx, result_tx);

        let key = TaskKey::AdvanceEpoch;

        supervisor.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::SolanaTx,
                started_at: tokio::time::Instant::now(),
                attempt: 0,
            },
        );
        supervisor
            .tokens
            .insert(key.clone(), CancellationToken::new());

        supervisor
            .handle_completion(key.clone(), TaskOutcome::Permanent("fatal".into()))
            .await;

        assert!(!supervisor.running.contains_key(&key));
        assert!(!supervisor.tokens.contains_key(&key));

        let result = result_rx.try_recv().unwrap();
        assert!(matches!(result, TaskResult::PermanentError(..)));
    }

    #[tokio::test]
    async fn dedup_schedule() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (directive_tx, directive_rx) = mpsc::channel(16);

        let supervisor = Supervisor::new(ctx, result_tx);
        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move {
            supervisor.run(directive_rx, cancel_clone).await;
        });

        let key = TaskKey::RecoveryScan { spool: 0 };

        // Schedule the same key twice rapidly
        directive_tx
            .send(Directive::Schedule(key.clone()))
            .await
            .unwrap();
        directive_tx
            .send(Directive::Schedule(key.clone()))
            .await
            .unwrap();

        // Should only get one result (second schedule is deduped while first is running)
        let result = result_rx.recv().await.unwrap();
        assert!(matches!(result, TaskResult::Success(..)));

        // Give time for any duplicate to arrive (should not)
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(result_rx.try_recv().is_err());

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        let (result_tx, _result_rx) = mpsc::channel(16);
        let (directive_tx, directive_rx) = mpsc::channel(16);

        let supervisor = Supervisor::new(ctx, result_tx);
        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move {
            supervisor.run(directive_rx, cancel_clone).await;
        });

        // Schedule a task
        directive_tx
            .send(Directive::Schedule(TaskKey::RefreshOnchainState))
            .await
            .unwrap();

        // Small delay to let it be processed
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Cancel everything
        cancel.cancel();
        handle.await.unwrap();
    }

    #[test]
    fn backoff_config() {
        let solana = backoff_for(TaskCategory::SolanaTx);
        assert_eq!(solana.min_delay, Duration::from_secs(1));
        assert_eq!(solana.max_delay, Duration::from_secs(60));
        assert_eq!(solana.max_retries, Some(20));

        let peer = backoff_for(TaskCategory::PeerHttp);
        assert_eq!(peer.min_delay, Duration::from_secs(2));
        assert_eq!(peer.max_delay, Duration::from_secs(300));
        assert_eq!(peer.max_retries, Some(50));

        let cpu = backoff_for(TaskCategory::CpuHeavy);
        assert_eq!(cpu.min_delay, Duration::from_secs(30));
        assert_eq!(cpu.max_delay, Duration::from_secs(300));
        assert_eq!(cpu.max_retries, None);

        let internal = backoff_for(TaskCategory::Internal);
        assert_eq!(internal.min_delay, Duration::from_secs(5));
        assert_eq!(internal.max_delay, Duration::from_secs(60));
        assert_eq!(internal.max_retries, Some(10));
    }

    #[test]
    fn categories() {
        assert_eq!(TaskKey::AdvanceEpoch.category(), TaskCategory::SolanaTx);
        assert_eq!(TaskKey::SyncEpoch.category(), TaskCategory::SolanaTx);
        assert_eq!(
            TaskKey::SpoolSync { spool: 0 }.category(),
            TaskCategory::PeerHttp
        );
        assert_eq!(TaskKey::SnapshotBuild.category(), TaskCategory::CpuHeavy);
        assert_eq!(
            TaskKey::RefreshOnchainState.category(),
            TaskCategory::Internal
        );
    }

    #[test]
    fn one_shot() {
        assert!(TaskKey::AdvanceEpoch.is_one_shot());
        assert!(TaskKey::SyncEpoch.is_one_shot());
        assert!(TaskKey::RefreshOnchainState.is_one_shot());
        assert!(TaskKey::RecoveryScan { spool: 0 }.is_one_shot());
        assert!(TaskKey::SpoolRecovery { spool: 0 }.is_one_shot());
        assert!(TaskKey::SpoolSync { spool: 0 }.is_one_shot());
        assert!(TaskKey::SnapshotBuild.is_one_shot());
        assert!(TaskKey::SnapshotCertify.is_one_shot());
    }

    #[test]
    fn delay_exponential() {
        let config = BackoffConfig {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_retries: Some(3),
        };

        // With half-jitter, delay is in [base/2, base]
        let d = compute_delay(&config, 0).unwrap(); // base=1s
        assert!(d >= Duration::from_millis(500));
        assert!(d <= Duration::from_secs(1));

        let d = compute_delay(&config, 1).unwrap(); // base=2s
        assert!(d >= Duration::from_secs(1));
        assert!(d <= Duration::from_secs(2));

        let d = compute_delay(&config, 2).unwrap(); // base=4s
        assert!(d >= Duration::from_secs(2));
        assert!(d <= Duration::from_secs(4));

        assert_eq!(compute_delay(&config, 3), None); // max retries exceeded
    }

    #[test]
    fn delay_capped() {
        let config = BackoffConfig {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(5),
            max_retries: None,
        };
        let d = compute_delay(&config, 10).unwrap(); // base capped at 5s
        assert!(d >= Duration::from_millis(2500));
        assert!(d <= Duration::from_secs(5));
    }

    #[tokio::test]
    async fn max_retries_exhausted() {
        let ctx = test_context();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let mut supervisor = Supervisor::new(ctx, result_tx);

        let key = TaskKey::RefreshOnchainState;

        // Internal category has max_retries: Some(10)
        // Simulate attempt 10 (already at max)
        supervisor.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::Internal,
                started_at: tokio::time::Instant::now(),
                attempt: 10,
            },
        );
        supervisor
            .tokens
            .insert(key.clone(), CancellationToken::new());

        supervisor
            .handle_completion(key.clone(), TaskOutcome::Retryable("transient".into()))
            .await;

        // Should NOT be in retry queue — max retries exceeded
        assert!(!supervisor.pending_retry.contains(&key));

        // Should get PermanentError
        let result = result_rx.try_recv().unwrap();
        assert!(matches!(result, TaskResult::PermanentError(..)));
    }
}
