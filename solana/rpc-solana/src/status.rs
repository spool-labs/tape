//! Signature status polls shared by every transaction waiting on confirmation

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use solana_client::client_error::Result as ClientResult;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_signature::Signature;
use solana_transaction::TransactionError;
use solana_transaction_status::TransactionStatus;

/// getSignatureStatuses answers at most this many signatures per call
const MAX_SIGNATURES_PER_CALL: usize = 256;

/// A poll that started this close before a waiter was due still answers it
const DUE_GRACE: Duration = Duration::from_millis(1);

/// One waited signature and the last poll that answered it
struct Waiter {
    waits: usize,
    answer: Option<(Instant, Option<TransactionStatus>)>,
}

/// Polls every signature in flight on one client with a single call per tick
pub struct StatusBatch {
    waiters: Mutex<HashMap<Signature, Waiter>>,
    poll: tokio::sync::Mutex<()>,
    origin: Instant,
}

/// Keeps a signature in the batch until its wait ends or is dropped
struct Registration<'batch> {
    batch: &'batch StatusBatch,
    signature: Signature,
}

impl StatusBatch {
    /// An empty batch
    pub fn new() -> Self {
        Self {
            waiters: Mutex::new(HashMap::new()),
            poll: tokio::sync::Mutex::new(()),
            origin: Instant::now(),
        }
    }

    /// Wait until `signature` reaches `commitment`, probing on the ramp and then on the shared `steady` ms grid
    pub async fn wait(
        &self,
        client: &RpcClient,
        signature: Signature,
        commitment: CommitmentConfig,
        ramp: &[u64],
        steady: u64,
    ) -> ClientResult<Result<(), TransactionError>> {
        let _registration = self.register(signature);
        let mut due = Instant::now();
        let mut probe = 0usize;
        loop {
            if let Some(status) = self.probe(client, signature, due).await? {
                if status.satisfies_commitment(commitment) {
                    return Ok(status.status);
                }
            }
            // Waiters past the ramp share one grid, so one of them polls for all.
            due = match ramp.get(probe) {
                Some(ms) => Instant::now() + Duration::from_millis(*ms),
                None => self.next_tick(steady),
            };
            probe += 1;
            tokio::time::sleep_until(due.into()).await;
        }
    }

    fn register(&self, signature: Signature) -> Registration<'_> {
        let mut waiters = self.lock();
        let waiter = waiters.entry(signature).or_insert(Waiter {
            waits: 0,
            answer: None,
        });
        waiter.waits += 1;
        Registration { batch: self, signature }
    }

    fn deregister(&self, signature: &Signature) {
        let mut waiters = self.lock();
        let Some(waiter) = waiters.get_mut(signature) else {
            return;
        };
        waiter.waits -= 1;
        if waiter.waits == 0 {
            waiters.remove(signature);
        }
    }

    /// Answer from the latest poll, polling every waiter when none has run since `due`
    async fn probe(
        &self,
        client: &RpcClient,
        signature: Signature,
        due: Instant,
    ) -> ClientResult<Option<TransactionStatus>> {
        let _turn = self.poll.lock().await;
        if let Some(status) = self.answer_since(&signature, due) {
            return Ok(status);
        }

        let started = Instant::now();
        let signatures: Vec<Signature> = self.lock().keys().copied().collect();
        let mut statuses = Vec::with_capacity(signatures.len());

        for chunk in signatures.chunks(MAX_SIGNATURES_PER_CALL) {
            statuses.extend(client.get_signature_statuses_with_history(chunk).await?.value);
        }

        let mut waiters = self.lock();
        let mut own = None;
        for (polled, status) in signatures.iter().zip(statuses) {
            if *polled == signature {
                own = status.clone();
            }
            if let Some(waiter) = waiters.get_mut(polled) {
                waiter.answer = Some((started, status));
            }
        }
        Ok(own)
    }

    fn answer_since(&self, signature: &Signature, due: Instant) -> Option<Option<TransactionStatus>> {
        let waiters = self.lock();
        let (started, status) = waiters.get(signature)?.answer.as_ref()?;
        (*started + DUE_GRACE >= due).then(|| status.clone())
    }

    fn next_tick(&self, steady: u64) -> Instant {
        let steady = steady.max(1);
        let elapsed = self.origin.elapsed().as_millis() as u64;
        self.origin + Duration::from_millis(elapsed - elapsed % steady + steady)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Signature, Waiter>> {
        self.waiters.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for StatusBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.batch.deregister(&self.signature);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use async_trait::async_trait;
    use serde_json::{json, Value};
    use solana_client::client_error::Result as SenderResult;
    use solana_client::rpc_client::RpcClientConfig;
    use solana_client::rpc_request::RpcRequest;
    use solana_client::rpc_sender::{RpcSender, RpcTransportStats};

    use super::*;

    const RAMP: [u64; 4] = [5, 15, 50, 130];
    const STEADY: u64 = 200;
    const IN_FLIGHT: usize = 16;
    const ROUND_TRIP: Duration = Duration::from_millis(2);

    /// Answers every status call, confirming everything once `confirm_at` has passed
    struct CountingSender {
        calls: Arc<AtomicUsize>,
        confirm_at: Instant,
    }

    #[async_trait]
    impl RpcSender for CountingSender {
        async fn send(&self, request: RpcRequest, params: Value) -> SenderResult<Value> {
            assert_eq!(request, RpcRequest::GetSignatureStatuses);
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(ROUND_TRIP).await;
            let count = params[0].as_array().expect("signature list").len();
            let status = match Instant::now() >= self.confirm_at {
                true => json!({
                    "slot": 1,
                    "confirmations": null,
                    "status": {"Ok": null},
                    "err": null,
                    "confirmationStatus": "finalized",
                }),
                false => Value::Null,
            };
            Ok(json!({"context": {"slot": 1}, "value": vec![status; count]}))
        }

        fn get_transport_stats(&self) -> RpcTransportStats {
            RpcTransportStats::default()
        }

        fn url(&self) -> String {
            "counting".into()
        }
    }

    // many transactions in flight share one status call per tick
    #[tokio::test]
    async fn one_call_per_tick() {
        let calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingSender {
            calls: calls.clone(),
            confirm_at: Instant::now() + Duration::from_millis(300),
        };
        let client = Arc::new(RpcClient::new_sender(
            sender,
            RpcClientConfig::with_commitment(CommitmentConfig::confirmed()),
        ));
        let batch = Arc::new(StatusBatch::new());

        let mut waits = Vec::new();
        for index in 0..IN_FLIGHT {
            let signature = Signature::from([index as u8 + 1; 64]);
            let client = client.clone();
            let batch = batch.clone();
            waits.push(tokio::spawn(async move {
                batch
                    .wait(
                        &client,
                        signature,
                        CommitmentConfig::confirmed(),
                        &RAMP,
                        STEADY,
                    )
                    .await
            }));
        }
        for wait in waits {
            let status = wait.await.expect("wait task").expect("status call");
            assert!(status.is_ok());
        }

        let calls = calls.load(Ordering::SeqCst);
        assert!(calls < IN_FLIGHT, "{calls} status calls");
    }
}
