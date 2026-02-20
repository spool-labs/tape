//! Supervisor — centralized task runner with retry, cancellation, and concurrency limits.
//!
//! The supervisor owns:
//! - A `BinaryHeap` of due times for retry timing (scales to millions of entries)
//! - A `JoinSet` tracking all spawned worker futures
//! - Per-category `Semaphore`s for concurrency limits
//! - Per-task `CancellationToken`s for cancellation
//!
//! A single runner loop does `sleep_until(next_due)`, pops due items, acquires
//! the appropriate semaphore, and dispatches to workers. On retryable failure,
//! `BackoffConfig` computes the next delay and the item is pushed back to the heap.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::core::{BackoffConfig, compute_delay};
use crate::runtime::NodeContext;
use crate::runtime::PeerHandle;
use crate::scheduler::Directive;
use crate::tasks::execute_task;

/// Fallback sleep target when no retries are pending.
const FAR_FUTURE_SECS: u64 = 365 * 24 * 3600;
/// How long to wait for in-flight tasks during shutdown before giving up.
const SHUTDOWN_TIMEOUT_SECS: u64 = 10;

pub use crate::runtime::{TaskCategory, TaskKey, TaskOutcome, TaskResult};

/// Centralized task runner with retry, cancellation, and per-category concurrency limits.
///
/// All background work is spawned through the supervisor. The scheduler sends
/// `Directive::Schedule` / `Directive::Cancel` commands; the supervisor manages
/// the full lifecycle: spawn, track, retry on failure, cancel on request, and
/// report outcomes back via `result_tx`.
pub struct Supervisor<S: Store, R: Rpc> {
    /// Shared node state (store, RPC client, identity, config).
    context: Arc<NodeContext<S, R>>,
    /// Handle for making peer HTTP requests.
    peer_handle: PeerHandle,
    /// Currently executing tasks, keyed for dedup and attempt tracking.
    running: HashMap<TaskKey, RunningTask>,
    /// Cancellation token for each running task.
    tokens: HashMap<TaskKey, CancellationToken>,
    /// Collects completions from all spawned task futures.
    join_set: JoinSet<(TaskKey, TaskOutcome)>,
    /// Min-heap of tasks waiting to be retried, ordered by due time.
    retry_queue: BinaryHeap<Reverse<RetryEntry>>,
    /// Keys present in `retry_queue`. Used to skip stale entries after cancel.
    pending_retry: HashSet<TaskKey>,
    /// Keys whose cancel was processed while the future was still in-flight.
    /// When the JoinSet yields these, their completion is silently dropped.
    canceled_running: HashSet<TaskKey>,
    /// Channel to send task outcomes back to the scheduler/FSM.
    result_tx: mpsc::Sender<TaskResult>,

    /// Limits concurrent Solana transaction submissions (capacity: 5).
    sem_solana_tx: Arc<Semaphore>,
    /// Limits concurrent peer HTTP requests (capacity: 50).
    sem_peer_http: Arc<Semaphore>,
    /// Limits concurrent CPU-heavy work like snapshot builds (capacity: num_cpus).
    sem_cpu_heavy: Arc<Semaphore>,
    /// Separate semaphore for internal tasks.
    sem_internal: Arc<Semaphore>,
}

impl<S: Store + 'static, R: Rpc + 'static> Supervisor<S, R> {
    pub fn new(
        context: Arc<NodeContext<S, R>>,
        peer_handle: PeerHandle,
        result_tx: mpsc::Sender<TaskResult>,
    ) -> Self {
        let cpu_count = std::thread::available_parallelism()
            .map_or(4, |n| n.get());

        Self {
            context,
            peer_handle,
            running: HashMap::new(),
            tokens: HashMap::new(),
            join_set: JoinSet::new(),
            retry_queue: BinaryHeap::new(),
            pending_retry: HashSet::new(),
            canceled_running: HashSet::new(),
            result_tx,
            sem_solana_tx: Arc::new(Semaphore::new(5)),
            sem_peer_http: Arc::new(Semaphore::new(50)),
            sem_cpu_heavy: Arc::new(Semaphore::new(cpu_count)),
            sem_internal: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
        }
    }

    /// Main event loop. Selects over four sources:
    /// 1. Directives from the scheduler (schedule / cancel)
    /// 2. Completions from the JoinSet (task finished)
    /// 3. Retry timer (re-spawn a failed task after backoff)
    /// 4. Global cancellation token (graceful shutdown)
    ///
    /// Exits when the directive channel closes or the cancel token fires.
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
                        Some(Directive::Cancel(key)) => self.handle_cancel(&key).await,
                        None => {
                            self.shutdown().await;
                            break;
                        }
                    }
                }

                Some(result) = self.join_set.join_next() => {
                    match result {
                        Ok((key, outcome)) => {
                            if self.canceled_running.remove(&key) {
                                tracing::debug!(task = ?key, "dropped completion for canceled task");
                                continue;
                            }
                            self.handle_completion(key, outcome).await;
                        }
                        Err(e) => tracing::error!("task panicked: {e}"),
                    }
                }

                _ = sleep_until(next_retry) => {
                    self.process_retries();
                }

                _ = cancel.cancelled() => {
                    self.shutdown().await;
                    break;
                }
            }
        }
    }

    /// Schedule a task for execution. Silently deduplicates: if the key is
    /// already running or awaiting retry, the request is dropped.
    fn handle_schedule(&mut self, key: TaskKey, attempt: u32) {
        if self.running.contains_key(&key) || self.pending_retry.contains(&key) {
            return;
        }
        self.spawn_task(key, attempt);
    }

    /// Cancel a task. Fires the cancellation token, removes from running/retry
    /// tracking, and sends `TaskResult::Canceled` back if the task was known.
    /// If the task's future is still in-flight on the JoinSet, its key is added
    /// to `canceled_running` so the eventual completion is silently dropped.
    async fn handle_cancel(&mut self, key: &TaskKey) {
        let had_running = self.running.remove(key).is_some();
        if had_running {
            self.canceled_running.insert(key.clone());
        }
        if let Some(token) = self.tokens.remove(key) {
            token.cancel();
        }
        let had_pending = self.pending_retry.remove(key);
        self.purge_retry_queue(key);
        if had_running || had_pending {
            self.send_result(TaskResult::Canceled(key.clone())).await;
        }
    }

    /// Route a completed task to the appropriate handler based on its outcome.
    async fn handle_completion(&mut self, key: TaskKey, outcome: TaskOutcome) {
        let attempt = self
            .running
            .remove(&key)
            .map(|r| r.attempt)
            .unwrap_or(0);
        self.tokens.remove(&key);

        match outcome {
            TaskOutcome::Success => self.complete_success(key).await,
            TaskOutcome::Pending(delay) => self.complete_pending(key, attempt, delay),
            TaskOutcome::Retryable(err) => self.complete_retry(key, attempt, err).await,
            TaskOutcome::Permanent(err) => self.complete_permanent(key, err).await,
        }
    }

    async fn complete_success(&self, key: TaskKey) {
        self.send_result(TaskResult::Success(key)).await;
    }

    /// Re-enqueue with the same attempt number and a task-specified delay.
    /// Unlike `Retryable`, `Pending` is a normal polling state (e.g. waiting
    /// for on-chain confirmation) so no result is sent and the attempt counter
    /// is not incremented.
    fn complete_pending(&mut self, key: TaskKey, attempt: u32, delay: Duration) {
        tracing::debug!(
            task = ?key,
            attempt,
            delay_secs = delay.as_secs(),
            "scheduling pending retry"
        );
        self.enqueue_retry(key, attempt, delay);
    }

    /// Compute backoff delay and re-enqueue, or escalate to permanent failure
    /// if max retries for this category have been exhausted.
    async fn complete_retry(&mut self, key: TaskKey, attempt: u32, err: String) {
        let config = backoff_for(key.category());
        match compute_delay(&config, attempt) {
            Some(delay) => {
                tracing::warn!(
                    task = ?key,
                    attempt,
                    delay_secs = delay.as_secs(),
                    error = %err,
                    "scheduling retry"
                );
                self.enqueue_retry(key.clone(), attempt + 1, delay);
                self.send_result(TaskResult::RetryableError(key, err)).await;
            }
            None => {
                tracing::error!(
                    task = ?key,
                    attempt,
                    "max retries exceeded, treating as permanent failure"
                );
                self.complete_permanent(key, err).await;
            }
        }
    }

    /// Report an unrecoverable failure. The task will not be retried.
    async fn complete_permanent(&self, key: TaskKey, err: String) {
        self.send_result(TaskResult::PermanentError(key, err)).await;
    }

    /// Push a task onto the retry heap with a computed due time.
    fn enqueue_retry(&mut self, key: TaskKey, attempt: u32, delay: Duration) {
        let due = Instant::now() + delay;
        self.retry_queue.push(Reverse(RetryEntry {
            due,
            key: key.clone(),
            attempt,
        }));
        self.pending_retry.insert(key);
    }

    /// Forward a task result to the scheduler/FSM. Silently drops if the
    /// channel is closed (happens during shutdown).
    async fn send_result(&self, result: TaskResult) {
        if self.result_tx.send(result).await.is_err() {
            tracing::debug!("result channel closed");
        }
    }

    /// Drain all retry entries whose due time has passed and re-spawn them.
    /// Entries whose key was removed from `pending_retry` (by a cancel) are
    /// silently skipped.
    fn process_retries(&mut self) {
        let now = Instant::now();
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

    /// Cancel all tasks and drain the JoinSet, giving in-flight futures up to
    /// `SHUTDOWN_TIMEOUT_SECS` to finish before abandoning them.
    async fn shutdown(&mut self) {
        for token in self.tokens.values() {
            token.cancel();
        }

        let deadline = Instant::now() + Duration::from_secs(SHUTDOWN_TIMEOUT_SECS);
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
                _ = sleep_until(deadline) => {
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
        self.canceled_running.clear();
    }

    /// Spawn a task future onto the JoinSet. Acquires the category semaphore
    /// inside the future so the permit is held for the task's lifetime.
    fn spawn_task(&mut self, key: TaskKey, attempt: u32) {
        let token = CancellationToken::new();
        let sem = self.semaphore_for(key.category());
        let ctx = self.context.clone();
        let peer_handle = self.peer_handle.clone();
        let token_clone = token.clone();
        let category = key.category();
        let key_to_run = key.clone();
        let span = tracing::info_span!(
            "task",
            task_key = ?key_to_run,
            task_type = ?key_to_run.category(),
            spool_id = ?key_to_run.spool_id(),
            attempt,
            duration_ms = tracing::field::Empty,
        );

        self.join_set.spawn(
            execute_task(ctx, peer_handle, key_to_run.clone(), token_clone, sem).instrument(span),
        );

        self.running.insert(
            key.clone(),
            RunningTask {
                category,
                started_at: Instant::now(),
                attempt,
            },
        );
        self.tokens.insert(key, token);
    }

    /// Return the concurrency-limiting semaphore for a task category.
    fn semaphore_for(&self, category: TaskCategory) -> Arc<Semaphore> {
        match category {
            TaskCategory::SolanaTx => self.sem_solana_tx.clone(),
            TaskCategory::PeerHttp => self.sem_peer_http.clone(),
            TaskCategory::CpuHeavy => self.sem_cpu_heavy.clone(),
            // Internal tasks have a dedicated high-capacity semaphore.
            TaskCategory::Internal => self.sem_internal.clone(),
        }
    }

    /// Remove stale retry entries for a canceled key to avoid heap growth.
    fn purge_retry_queue(&mut self, key: &TaskKey) {
        self.retry_queue = self
            .retry_queue
            .drain()
            .filter(|entry| entry.0.key != *key)
            .collect();
    }

    /// Earliest due time in the retry heap, or far-future if empty.
    fn next_retry_instant(&self) -> Instant {
        self.retry_queue
            .peek()
            .map(|e| e.0.due)
            .unwrap_or_else(far_future)
    }
}

/// Return the backoff configuration for a task category.
pub fn backoff_for(category: TaskCategory) -> BackoffConfig {
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

/// Entry in the retry min-heap. Ordered by `due` time only.
struct RetryEntry {
    /// When this retry becomes eligible to run.
    due: Instant,
    /// Which task to re-spawn.
    key: TaskKey,
    /// Attempt number to pass to the next spawn.
    attempt: u32,
}

impl PartialEq for RetryEntry {
    fn eq(&self, other: &Self) -> bool {
        self.due == other.due
    }
}

impl Eq for RetryEntry {}

impl PartialOrd for RetryEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RetryEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.due.cmp(&other.due)
    }
}

/// Metadata for a task that is currently executing on the JoinSet.
struct RunningTask {
    /// Task category (for future observability/metrics).
    #[allow(dead_code)]
    category: TaskCategory,
    /// When this attempt was spawned (for future duration metrics).
    #[allow(dead_code)]
    started_at: Instant,
    /// Current attempt number (0-based). Incremented on retryable failure.
    attempt: u32,
}

/// Returns an `Instant` ~1 year in the future, used as a no-op sleep target.
fn far_future() -> Instant {
    Instant::now() + Duration::from_secs(FAR_FUTURE_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::BackoffConfig;
    use crate::runtime::PeerService;
    use crate::runtime::test_utils::test_context;
    use tape_core::types::EpochNumber;
    use tokio::time::sleep;

    #[tokio::test]
    async fn schedule_complete() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (directive_tx, directive_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();

        let supervisor = Supervisor::new(ctx, peer_handle, result_tx);
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
        let (_peer_service, peer_handle) = PeerService::new();

        let supervisor = Supervisor::new(ctx, peer_handle, result_tx);
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
        sleep(Duration::from_millis(50)).await;

        // The task may have completed before cancel was processed (stub is instant),
        // so we drain whatever results arrived. The key point is that the supervisor
        // doesn't panic and handles the cancel gracefully.
        result_rx.close();
        while result_rx.recv().await.is_some() {}

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_result() {
        let ctx = test_context();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();
        let mut supervisor = Supervisor::new(ctx, peer_handle, result_tx);

        let key = TaskKey::SnapshotBuild { epoch: EpochNumber(0) };
        supervisor.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::CpuHeavy,
                started_at: Instant::now(),
                attempt: 0,
            },
        );
        supervisor
            .tokens
            .insert(key.clone(), CancellationToken::new());

        supervisor.handle_cancel(&key).await;

        let result = result_rx.try_recv().unwrap();
        assert!(matches!(result, TaskResult::Canceled(ref k) if *k == key));
        assert!(supervisor.canceled_running.contains(&key));
        assert!(!supervisor.running.contains_key(&key));
        assert!(!supervisor.tokens.contains_key(&key));
    }

    #[tokio::test]
    async fn retry_failure() {
        let ctx = test_context();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();
        let mut supervisor = Supervisor::new(ctx, peer_handle, result_tx);

        let key = TaskKey::RefreshOnchainState;

        // Simulate a running task
        supervisor.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::Internal,
                started_at: Instant::now(),
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
    async fn pending_retry() {
        let ctx = test_context();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();
        let mut supervisor = Supervisor::new(ctx, peer_handle, result_tx);

        let key = TaskKey::SnapshotSubmit { epoch: EpochNumber(0) };
        supervisor.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::SolanaTx,
                started_at: Instant::now(),
                attempt: 3,
            },
        );
        supervisor
            .tokens
            .insert(key.clone(), CancellationToken::new());

        supervisor
            .handle_completion(key.clone(), TaskOutcome::Pending(Duration::from_secs(2)))
            .await;

        assert!(supervisor.pending_retry.contains(&key));
        assert_eq!(supervisor.retry_queue.len(), 1);
        let queued = &supervisor.retry_queue.peek().unwrap().0;
        assert_eq!(queued.key, key);
        assert_eq!(queued.attempt, 3);
        assert!(result_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn permanent_failure() {
        let ctx = test_context();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();
        let mut supervisor = Supervisor::new(ctx, peer_handle, result_tx);

        let key = TaskKey::AdvanceEpoch { epoch: EpochNumber(0) };

        supervisor.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::SolanaTx,
                started_at: Instant::now(),
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
        let (_peer_service, peer_handle) = PeerService::new();

        let supervisor = Supervisor::new(ctx, peer_handle, result_tx);
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
        sleep(Duration::from_millis(50)).await;
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
        let (_peer_service, peer_handle) = PeerService::new();

        let supervisor = Supervisor::new(ctx, peer_handle, result_tx);
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
        sleep(Duration::from_millis(10)).await;

        // Cancel everything
        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn close_channel() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        let (result_tx, _result_rx) = mpsc::channel(16);
        let (directive_tx, directive_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();

        let supervisor = Supervisor::new(ctx, peer_handle, result_tx);
        let handle = tokio::spawn(async move {
            supervisor.run(directive_rx, cancel).await;
        });

        drop(directive_tx);
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
        assert_eq!(
            TaskKey::AdvanceEpoch { epoch: EpochNumber(0) }.category(),
            TaskCategory::SolanaTx
        );
        assert_eq!(
            TaskKey::SyncEpoch { epoch: EpochNumber(0) }.category(),
            TaskCategory::SolanaTx
        );
        assert_eq!(
            TaskKey::SpoolSync { spool: 0 }.category(),
            TaskCategory::PeerHttp
        );
        assert_eq!(
            TaskKey::SnapshotBuild { epoch: EpochNumber(0) }.category(),
            TaskCategory::CpuHeavy
        );
        assert_eq!(
            TaskKey::RefreshOnchainState.category(),
            TaskCategory::Internal
        );
    }

    #[test]
    fn one_shot() {
        assert!(TaskKey::AdvanceEpoch { epoch: EpochNumber(0) }.is_one_shot());
        assert!(TaskKey::SyncEpoch { epoch: EpochNumber(0) }.is_one_shot());
        assert!(TaskKey::RefreshOnchainState.is_one_shot());
        assert!(TaskKey::SnapshotBuild { epoch: EpochNumber(0) }.is_one_shot());
        assert!(TaskKey::SnapshotCollect { epoch: EpochNumber(0) }.is_one_shot());
        assert!(!TaskKey::RecoveryScan { spool: 0 }.is_one_shot());
        assert!(!TaskKey::SpoolRecovery { spool: 0 }.is_one_shot());
        assert!(!TaskKey::SpoolSync { spool: 0 }.is_one_shot());
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
    async fn retries_exhausted() {
        let ctx = test_context();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();
        let mut supervisor = Supervisor::new(ctx, peer_handle, result_tx);

        let key = TaskKey::RefreshOnchainState;

        // Internal category has max_retries: Some(10)
        // Simulate attempt 10 (already at max)
        supervisor.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::Internal,
                started_at: Instant::now(),
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
