//! TaskRunner — centralized task runner with retry, cancellation, and concurrency limits.
//!
//! The task_runner owns:
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
use crate::core::{NodeContext, PeerHandle};
use crate::{TaskCategory, TaskResult};
use crate::task_scheduler::Action;
use crate::tasks::execute_task;

pub use crate::{Task, TaskOutcome};

/// Fallback sleep target when no retries are pending.
const FAR_FUTURE_SECS: u64 = 365 * 24 * 3600;
/// How long to wait for in-flight tasks during shutdown before giving up.
const SHUTDOWN_TIMEOUT_SECS: u64 = 10;

/// Centralized task runner with retry, cancellation, and per-category concurrency limits.
///
/// All background work is spawned through the task_runner. The scheduler sends
/// `Action::Schedule` / `Action::Cancel` commands; the task_runner manages
/// the full lifecycle: spawn, track, retry on failure, cancel on request, and
/// report outcomes back via `result_tx`.
pub struct TaskRunner<S: Store, R: Rpc> {
    /// Shared node state (store, RPC client, identity, config).
    context: Arc<NodeContext<S, R>>,
    /// Currently executing tasks, keyed for dedup and attempt tracking.
    running: HashMap<Task, RunningTask>,
    /// Keys whose cancel was processed while the future was still in-flight.
    canceled: HashSet<Task>,
    /// Tasks waiting to be retried after backoff.
    retries: RetryQueue,
    /// Per-category concurrency semaphores.
    limits: ConcurrencyLimits,
    /// Handle for making peer HTTP requests.
    peers: PeerHandle,
    /// Cancellation token for each running task.
    cancel_tokens: HashMap<Task, CancellationToken>,
    /// Collects completions from all spawned task futures.
    futures: JoinSet<(Task, TaskOutcome)>,
    /// Channel to send task outcomes back to the scheduler/FSM.
    result_tx: mpsc::Sender<TaskResult>,
}

impl<S: Store + 'static, R: Rpc + 'static> TaskRunner<S, R> {
    pub fn new(
        context: Arc<NodeContext<S, R>>,
        peer_handle: PeerHandle,
        result_tx: mpsc::Sender<TaskResult>,
    ) -> Self {
        Self {
            context,
            peers: peer_handle,
            running: HashMap::new(),
            canceled: HashSet::new(),
            retries: RetryQueue::new(),
            limits: ConcurrencyLimits::new(),
            cancel_tokens: HashMap::new(),
            futures: JoinSet::new(),
            result_tx,
        }
    }

    /// Main event loop. Selects over four sources:
    /// 1. Actions from the scheduler (schedule / cancel)
    /// 2. Completions from the JoinSet (task finished)
    /// 3. Retry timer (re-spawn a failed task after backoff)
    /// 4. Global cancellation token (graceful shutdown)
    ///
    /// Exits when the action channel closes or the cancel token fires.
    pub async fn run(
        mut self,
        mut action_rx: mpsc::Receiver<Action>,
        cancel: CancellationToken,
    ) {
        loop {
            let next_retry = self.retries.next_due_instant();

            tokio::select! {
                action = action_rx.recv() => {
                    match action {
                        Some(Action::Schedule(key)) => {
                            tracing::trace!(task = ?key, attempt = 0, "task_runner received schedule action");
                            self.handle_schedule(key, 0)
                        }
                        Some(Action::Cancel(key)) => {
                            tracing::trace!(task = ?key, "task_runner received cancel action");
                            self.handle_cancel(&key).await;
                        }
                        None => {
                            tracing::trace!("task_runner action channel closed");
                            self.shutdown().await;
                            break;
                        }
                    }
                }

                Some(result) = self.futures.join_next() => {
                    match result {
                        Ok((key, outcome)) => {
                            tracing::trace!(task = ?key, outcome = ?outcome, "task_runner task completed");
                            if self.canceled.remove(&key) {
                                tracing::debug!(task = ?key, "dropped completion for canceled task");
                                continue;
                            }
                            self.handle_completion(key, outcome).await;
                        }
                        Err(e) => tracing::error!("task panicked: {e}"),
                    }
                }

                _ = sleep_until(next_retry) => {
                    tracing::trace!("task_runner processing retry queue");
                    self.process_retries();
                }

                _ = cancel.cancelled() => {
                    tracing::trace!("task_runner received cancellation signal");
                    self.shutdown().await;
                    break;
                }
            }
        }
    }

    /// Schedule a task for execution. Silently deduplicates: if the key is
    /// already running or awaiting retry, the request is dropped.
    fn handle_schedule(&mut self, key: Task, attempt: u32) {
        if self.running.contains_key(&key) || self.retries.contains(&key) {
            tracing::trace!(
                task = ?key,
                attempt,
                "task_runner skipping schedule due to dedupe"
            );
            return;
        }
        tracing::trace!(task = ?key, attempt, "task_runner spawning task");
        self.spawn_task(key, attempt);
    }

    /// Cancel a task. Fires the cancellation token, removes from running/retry
    /// tracking, and sends `TaskResult::Canceled` back if the task was known.
    /// If the task's future is still in-flight on the JoinSet, its key is added
    /// to `canceled_running` so the eventual completion is silently dropped.
    async fn handle_cancel(&mut self, key: &Task) {
        tracing::trace!(task = ?key, "task_runner canceling task");
        let had_running = self.running.remove(key).is_some();
        if had_running {
            self.canceled.insert(key.clone());
        }
        if let Some(token) = self.cancel_tokens.remove(key) {
            token.cancel();
        }
        let had_pending = self.retries.cancel(key);
        tracing::trace!(
            task = ?key,
            had_running,
            had_pending,
            "task_runner cancel state"
        );
        if had_running || had_pending {
            self.send_result(TaskResult::Canceled(key.clone())).await;
        }
    }

    /// Route a completed task to the appropriate handler based on its outcome.
    async fn handle_completion(&mut self, key: Task, outcome: TaskOutcome) {
        tracing::trace!(task = ?key, outcome = ?outcome, "task_runner handling completion");
        let attempt = self
            .running
            .remove(&key)
            .map(|r| r.attempt)
            .unwrap_or(0);
        self.cancel_tokens.remove(&key);

        match outcome {
            TaskOutcome::Success => self.complete_success(key).await,
            TaskOutcome::Pending(delay) => self.complete_pending(key, attempt, delay),
            TaskOutcome::Retryable(err) => self.complete_retry(key, attempt, err).await,
            TaskOutcome::Permanent(err) => self.complete_permanent(key, err).await,
        }
    }

    async fn complete_success(&self, key: Task) {
        tracing::trace!(task = ?key, "task_runner completed successfully");
        self.send_result(TaskResult::Success(key)).await;
    }

    /// Re-enqueue with the same attempt number and a task-specified delay.
    /// Unlike `Retryable`, `Pending` is a normal polling state (e.g. waiting
    /// for on-chain confirmation) so no result is sent and the attempt counter
    /// is not incremented.
    fn complete_pending(&mut self, key: Task, attempt: u32, delay: Duration) {
        tracing::debug!(
            task = ?key,
            attempt,
            delay_secs = delay.as_secs(),
            "scheduling pending retry"
        );
        tracing::trace!(
            task = ?key,
            attempt,
            delay_secs = delay.as_secs(),
            "task_runner pending retry"
        );
        self.retries.enqueue(key, attempt, delay);
    }

    /// Compute backoff delay and re-enqueue, or escalate to permanent failure
    /// if max retries for this category have been exhausted.
    async fn complete_retry(&mut self, key: Task, attempt: u32, err: String) {

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
                self.retries.enqueue(key.clone(), attempt + 1, delay);
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
    async fn complete_permanent(&self, key: Task, err: String) {
        tracing::error!(task = ?key, error = %err, "task_runner permanent failure");
        self.send_result(TaskResult::PermanentError(key, err)).await;
    }

    /// Forward a task result to the scheduler/FSM. Silently drops if the
    /// channel is closed (happens during shutdown).
    async fn send_result(&self, result: TaskResult) {
        let task = match &result {
            TaskResult::Success(key) | TaskResult::Canceled(key) => key.clone(),
            TaskResult::RetryableError(key, _) | TaskResult::PermanentError(key, _) => key.clone(),
        };
        if self.result_tx.send(result).await.is_err() {
            tracing::debug!("result channel closed");
        } else {
            tracing::trace!(task = ?task, "task_runner sent result");
        }
    }

    /// Drain all retry entries whose due time has passed and re-spawn them.
    fn process_retries(&mut self) {
        for (key, attempt) in self.retries.drain_due() {
            tracing::trace!(task = ?key, attempt, "task_runner retry due");
            self.spawn_task(key, attempt);
        }
    }

    /// Cancel all tasks and drain the JoinSet, giving in-flight futures up to
    /// `SHUTDOWN_TIMEOUT_SECS` to finish before abandoning them.
    async fn shutdown(&mut self) {
        tracing::trace!("task_runner shutdown start");
        for token in self.cancel_tokens.values() {
            token.cancel();
        }

        let deadline = Instant::now() + Duration::from_secs(SHUTDOWN_TIMEOUT_SECS);
        loop {
            tokio::select! {
                result = self.futures.join_next() => {
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
        self.cancel_tokens.clear();
        self.canceled.clear();
    }

    /// Spawn a task future onto the JoinSet. Acquires the category semaphore
    /// inside the future so the permit is held for the task's lifetime.
    fn spawn_task(&mut self, key: Task, attempt: u32) {
        let token = CancellationToken::new();
        let sem = self.limits.get(key.category());
        let ctx = self.context.clone();
        let peers = self.peers.clone();
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

        tracing::trace!(
            task = ?key_to_run,
            attempt,
            "task_runner spawning background task"
        );

        self.futures.spawn(
            execute_task(ctx, peers, key_to_run.clone(), token_clone, sem).instrument(span),
        );

        self.running.insert(
            key.clone(),
            RunningTask {
                category,
                started_at: Instant::now(),
                attempt,
            },
        );

        self.cancel_tokens.insert(key, token);
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

/// Per-category concurrency limits.
///
/// Each task category gets its own `Semaphore` so that, e.g., a burst of
/// peer HTTP requests cannot starve Solana transaction submissions.
struct ConcurrencyLimits {
    solana_tx: Arc<Semaphore>,
    peer_http: Arc<Semaphore>,
    cpu_heavy: Arc<Semaphore>,
    internal: Arc<Semaphore>,
}

impl ConcurrencyLimits {
    fn new() -> Self {
        let cpu_count = std::thread::available_parallelism()
            .map_or(4, |n| n.get());
        Self {
            solana_tx: Arc::new(Semaphore::new(5)),
            peer_http: Arc::new(Semaphore::new(50)),
            cpu_heavy: Arc::new(Semaphore::new(cpu_count)),
            internal: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
        }
    }

    fn get(&self, category: TaskCategory) -> Arc<Semaphore> {
        match category {
            TaskCategory::SolanaTx => self.solana_tx.clone(),
            TaskCategory::PeerHttp => self.peer_http.clone(),
            TaskCategory::CpuHeavy => self.cpu_heavy.clone(),
            TaskCategory::Internal => self.internal.clone(),
        }
    }
}

/// Min-heap of tasks waiting to be retried, ordered by due time.
///
/// Maintains a parallel `HashSet` of pending keys so that canceled entries
/// can be skipped without a linear scan of the heap.
struct RetryQueue {
    /// Min-heap ordered by due time.
    heap: BinaryHeap<Reverse<RetryEntry>>,
    /// Keys present in the heap. Used to skip canceled entries after cancel.
    pending: HashSet<Task>,
}

impl RetryQueue {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            pending: HashSet::new(),
        }
    }

    /// Push a task onto the retry heap with a computed due time.
    fn enqueue(&mut self, key: Task, attempt: u32, delay: Duration) {
        let due = Instant::now() + delay;
        self.heap.push(Reverse(RetryEntry {
            due,
            key: key.clone(),
            attempt,
        }));
        self.pending.insert(key);
    }

    /// Returns true if the key is awaiting retry.
    fn contains(&self, key: &Task) -> bool {
        self.pending.contains(key)
    }

    /// Remove a key from pending and purge its heap entries.
    fn cancel(&mut self, key: &Task) -> bool {
        let had = self.pending.remove(key);
        if had {
            self.heap = self
                .heap
                .drain()
                .filter(|entry| entry.0.key != *key)
                .collect();
        }
        had
    }

    /// Drain all entries whose due time has passed. Returns them so the
    /// caller can re-spawn.
    fn drain_due(&mut self) -> Vec<(Task, u32)> {
        let now = Instant::now();
        let mut due = Vec::new();
        while let Some(entry) = self.heap.peek() {
            if entry.0.due > now {
                break;
            }
            let entry = self.heap.pop().unwrap().0;
            if self.pending.remove(&entry.key) {
                due.push((entry.key, entry.attempt));
            }
        }
        due
    }

    /// Earliest due time in the heap, or far-future if empty.
    fn next_due_instant(&self) -> Instant {
        self.heap
            .peek()
            .map(|e| e.0.due)
            .unwrap_or_else(far_future)
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.heap.len()
    }

    #[cfg(test)]
    fn peek(&self) -> Option<&Reverse<RetryEntry>> {
        self.heap.peek()
    }
}

/// Entry in the retry min-heap. Ordered by `due` time only.
struct RetryEntry {
    /// When this retry becomes eligible to run.
    due: Instant,
    /// Which task to re-spawn.
    key: Task,
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
    use crate::core::PeerService;
    use crate::core::test_utils::test_context;
    use tape_core::types::EpochNumber;
    use tokio::time::sleep;

    #[tokio::test]
    async fn schedule_complete() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (action_tx, action_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();

        let task_runner = TaskRunner::new(ctx, peer_handle, result_tx);
        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move {
            task_runner.run(action_rx, cancel_clone).await;
        });

        let key = Task::RecoveryScan { spool: 0 };
        action_tx
            .send(Action::Schedule(key.clone()))
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
        let (action_tx, action_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();

        let task_runner = TaskRunner::new(ctx, peer_handle, result_tx);
        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move {
            task_runner.run(action_rx, cancel_clone).await;
        });

        let key = Task::SpoolSync { spool: 42 };

        // Schedule then immediately cancel
        action_tx
            .send(Action::Schedule(key.clone()))
            .await
            .unwrap();
        action_tx
            .send(Action::Cancel(key.clone()))
            .await
            .unwrap();

        // Give the task_runner time to process
        sleep(Duration::from_millis(50)).await;

        // The task may have completed before cancel was processed (stub is instant),
        // so we drain whatever results arrived. The key point is that the task_runner
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
        let mut task_runner = TaskRunner::new(ctx, peer_handle, result_tx);

        let key = Task::SnapshotBuild { epoch: EpochNumber(0) };
        task_runner.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::CpuHeavy,
                started_at: Instant::now(),
                attempt: 0,
            },
        );
        task_runner
            .cancel_tokens
            .insert(key.clone(), CancellationToken::new());

        task_runner.handle_cancel(&key).await;

        let result = result_rx.try_recv().unwrap();
        assert!(matches!(result, TaskResult::Canceled(ref k) if *k == key));
        assert!(task_runner.canceled.contains(&key));
        assert!(!task_runner.running.contains_key(&key));
        assert!(!task_runner.cancel_tokens.contains_key(&key));
    }

    #[tokio::test]
    async fn retry_failure() {
        let ctx = test_context();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();
        let mut task_runner = TaskRunner::new(ctx, peer_handle, result_tx);

        let key = Task::RefreshOnchainState;

        // Simulate a running task
        task_runner.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::Internal,
                started_at: Instant::now(),
                attempt: 0,
            },
        );
        task_runner
            .cancel_tokens
            .insert(key.clone(), CancellationToken::new());

        // Handle a retryable completion
        task_runner
            .handle_completion(key.clone(), TaskOutcome::Retryable("transient".into()))
            .await;

        // Should be in retry queue
        assert!(task_runner.retries.contains(&key));
        assert!(!task_runner.retries.is_empty());

        // Result should have been sent
        let result = result_rx.try_recv().unwrap();
        assert!(matches!(result, TaskResult::RetryableError(..)));
    }

    #[tokio::test]
    async fn pending_retry() {
        let ctx = test_context();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();
        let mut task_runner = TaskRunner::new(ctx, peer_handle, result_tx);

        let key = Task::SnapshotSubmit { epoch: EpochNumber(0) };
        task_runner.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::SolanaTx,
                started_at: Instant::now(),
                attempt: 3,
            },
        );
        task_runner
            .cancel_tokens
            .insert(key.clone(), CancellationToken::new());

        task_runner
            .handle_completion(key.clone(), TaskOutcome::Pending(Duration::from_secs(2)))
            .await;

        assert!(task_runner.retries.contains(&key));
        assert_eq!(task_runner.retries.len(), 1);
        let queued = &task_runner.retries.peek().unwrap().0;
        assert_eq!(queued.key, key);
        assert_eq!(queued.attempt, 3);
        assert!(result_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn permanent_failure() {
        let ctx = test_context();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();
        let mut task_runner = TaskRunner::new(ctx, peer_handle, result_tx);

        let key = Task::AdvanceEpoch { epoch: EpochNumber(0) };

        task_runner.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::SolanaTx,
                started_at: Instant::now(),
                attempt: 0,
            },
        );
        task_runner
            .cancel_tokens
            .insert(key.clone(), CancellationToken::new());

        task_runner
            .handle_completion(key.clone(), TaskOutcome::Permanent("fatal".into()))
            .await;

        assert!(!task_runner.running.contains_key(&key));
        assert!(!task_runner.cancel_tokens.contains_key(&key));

        let result = result_rx.try_recv().unwrap();
        assert!(matches!(result, TaskResult::PermanentError(..)));
    }

    #[tokio::test]
    async fn dedup_schedule() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        let (result_tx, mut result_rx) = mpsc::channel(16);
        let (action_tx, action_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();

        let task_runner = TaskRunner::new(ctx, peer_handle, result_tx);
        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move {
            task_runner.run(action_rx, cancel_clone).await;
        });

        let key = Task::RecoveryScan { spool: 0 };

        // Schedule the same key twice rapidly
        action_tx
            .send(Action::Schedule(key.clone()))
            .await
            .unwrap();
        action_tx
            .send(Action::Schedule(key.clone()))
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
        let (action_tx, action_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();

        let task_runner = TaskRunner::new(ctx, peer_handle, result_tx);
        let cancel_clone = cancel.clone();
        let handle = tokio::spawn(async move {
            task_runner.run(action_rx, cancel_clone).await;
        });

        // Schedule a task
        action_tx
            .send(Action::Schedule(Task::RefreshOnchainState))
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
        let (action_tx, action_rx) = mpsc::channel(16);
        let (_peer_service, peer_handle) = PeerService::new();

        let task_runner = TaskRunner::new(ctx, peer_handle, result_tx);
        let handle = tokio::spawn(async move {
            task_runner.run(action_rx, cancel).await;
        });

        drop(action_tx);
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
            Task::AdvanceEpoch { epoch: EpochNumber(0) }.category(),
            TaskCategory::SolanaTx
        );
        assert_eq!(
            Task::SyncEpoch { epoch: EpochNumber(0) }.category(),
            TaskCategory::SolanaTx
        );
        assert_eq!(
            Task::SpoolSync { spool: 0 }.category(),
            TaskCategory::PeerHttp
        );
        assert_eq!(
            Task::SnapshotBuild { epoch: EpochNumber(0) }.category(),
            TaskCategory::CpuHeavy
        );
        assert_eq!(
            Task::RefreshOnchainState.category(),
            TaskCategory::Internal
        );
    }

    #[test]
    fn one_shot() {
        assert!(Task::AdvanceEpoch { epoch: EpochNumber(0) }.is_one_shot());
        assert!(Task::SyncEpoch { epoch: EpochNumber(0) }.is_one_shot());
        assert!(Task::RefreshOnchainState.is_one_shot());
        assert!(Task::SnapshotBuild { epoch: EpochNumber(0) }.is_one_shot());
        assert!(Task::SnapshotCollect { epoch: EpochNumber(0) }.is_one_shot());
        assert!(!Task::RecoveryScan { spool: 0 }.is_one_shot());
        assert!(!Task::SpoolRecovery { spool: 0 }.is_one_shot());
        assert!(!Task::SpoolSync { spool: 0 }.is_one_shot());
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
        let mut task_runner = TaskRunner::new(ctx, peer_handle, result_tx);

        let key = Task::RefreshOnchainState;

        // Internal category has max_retries: Some(10)
        // Simulate attempt 10 (already at max)
        task_runner.running.insert(
            key.clone(),
            RunningTask {
                category: TaskCategory::Internal,
                started_at: Instant::now(),
                attempt: 10,
            },
        );
        task_runner
            .cancel_tokens
            .insert(key.clone(), CancellationToken::new());

        task_runner
            .handle_completion(key.clone(), TaskOutcome::Retryable("transient".into()))
            .await;

        // Should NOT be in retry queue — max retries exceeded
        assert!(!task_runner.retries.contains(&key));

        // Should get PermanentError
        let result = result_rx.try_recv().unwrap();
        assert!(matches!(result, TaskResult::PermanentError(..)));
    }
}
