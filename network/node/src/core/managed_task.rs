//! Exclusive background task lifecycle management.
//!
//! `ManagedTask` ensures at most one task runs at a time per slot. Spawning a
//! new task aborts the previous one. Handles are always stored — never dropped —
//! so completion, failure, and panics are observable.

use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinHandle};
use tracing::debug;

/// An exclusive background task slot.
///
/// At most one task runs at a time. Spawning a new task aborts the previous one.
/// The handle is always stored so completion, failure, and panics are observable.
pub struct ManagedTask {
    name: &'static str,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl ManagedTask {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            handle: Mutex::new(None),
        }
    }

    /// Spawn a new task, aborting any existing one.
    pub async fn spawn<F: std::future::Future<Output = ()> + Send + 'static>(&self, future: F) {
        let mut guard = self.handle.lock().await;
        if let Some(old) = guard.take() {
            debug!(task = self.name, "aborting previous task");
            old.abort();
            // Wait for the old task to finish (abort is asynchronous)
            let _ = old.await;
        }
        *guard = Some(tokio::spawn(future));
        debug!(task = self.name, "spawned new task");
    }

    /// Check if the task has finished. Returns:
    /// - `None`: no task spawned or task still running
    /// - `Some(Ok(()))`: task completed successfully
    /// - `Some(Err(e))`: task panicked or was cancelled
    ///
    /// Consumes the handle on completion so subsequent calls return `None`.
    pub async fn poll(&self) -> Option<Result<(), JoinError>> {
        let mut guard = self.handle.lock().await;
        let handle = guard.as_ref()?;
        if handle.is_finished() {
            // Task is done — take the handle and return the result
            let handle = guard.take().unwrap();
            Some(handle.await)
        } else {
            None
        }
    }

    /// Abort the running task and wait for it to finish.
    pub async fn abort(&self) {
        let mut guard = self.handle.lock().await;
        if let Some(handle) = guard.take() {
            handle.abort();
            let _ = handle.await;
            debug!(task = self.name, "task aborted");
        }
    }

    /// Is a task currently running (spawned and not yet finished)?
    pub async fn is_running(&self) -> bool {
        let guard = self.handle.lock().await;
        match guard.as_ref() {
            Some(h) => !h.is_finished(),
            None => false,
        }
    }

    /// Name for logging and metrics.
    pub fn name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn spawn_and_complete() {
        let task = ManagedTask::new("test");
        let completed = Arc::new(AtomicBool::new(false));
        let completed2 = completed.clone();

        task.spawn(async move {
            completed2.store(true, Ordering::SeqCst);
        })
        .await;

        // Wait for completion
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let result = task.poll().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());
        assert!(completed.load(Ordering::SeqCst));
        assert!(!task.is_running().await);
    }

    #[tokio::test]
    async fn spawn_aborts_previous() {
        let task = ManagedTask::new("test");
        let first_ran = Arc::new(AtomicBool::new(false));
        let first_ran2 = first_ran.clone();

        // Spawn a long-running task
        task.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(100)).await;
            first_ran2.store(true, Ordering::SeqCst);
        })
        .await;

        assert!(task.is_running().await);

        // Spawn a replacement — should abort the first
        let second_done = Arc::new(AtomicBool::new(false));
        let second_done2 = second_done.clone();
        task.spawn(async move {
            second_done2.store(true, Ordering::SeqCst);
        })
        .await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(!first_ran.load(Ordering::SeqCst));
        assert!(second_done.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn abort_stops_task() {
        let task = ManagedTask::new("test");

        task.spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(100)).await;
        })
        .await;

        assert!(task.is_running().await);
        task.abort().await;
        assert!(!task.is_running().await);
    }

    #[tokio::test]
    async fn poll_returns_none_when_empty() {
        let task = ManagedTask::new("test");
        assert!(task.poll().await.is_none());
    }

    #[tokio::test]
    async fn poll_returns_none_while_running() {
        let task = ManagedTask::new("test");
        task.spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(100)).await;
        })
        .await;

        assert!(task.poll().await.is_none());
        task.abort().await;
    }

    #[tokio::test]
    async fn poll_propagates_panic() {
        let task = ManagedTask::new("test");
        task.spawn(async {
            panic!("test panic");
        })
        .await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let result = task.poll().await;
        assert!(result.is_some());
        let err = result.unwrap().unwrap_err();
        assert!(err.is_panic());
    }

    #[tokio::test]
    async fn name_is_accessible() {
        let task = ManagedTask::new("my_task");
        assert_eq!(task.name(), "my_task");
    }
}
