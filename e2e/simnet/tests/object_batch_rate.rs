use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use solana_account::Account;
use solana_hash::Hash;
use solana_transaction::versioned::VersionedTransaction;
use rpc_litesvm::LiteSvmRpc;
use tape_blocks::wire::Block;
use tape_chain_harness::TEST_MAX_EPOCH_DURATION;
use tape_core::types::{BasisPoints, ContentType, StorageUnits};
use tape_crypto::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::tx::Txid;
use tape_e2e_simnet::{NodeRuntimeMode, SimnetBuilder, TAPENET_GROUP_SIZE, run_simnet_test};
use tape_peer_http::HttpApi;
use tape_rpc::{
    CommitmentLevel, EncodedConfirmedTransactionWithStatusMeta, Rpc, RpcError,
    RpcProgramAccountsConfig, SimulationResult, TransactionError, async_trait,
};
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::metrics::{InMemory, Operation, Outcome};
use tape_sdk::object::ObjectBatchItem;
use tape_sdk::tapedrive::Tapedrive;
use tokio::time::{sleep, sleep_until};

const TARGET_GROUPS: u64 = 1;

/// Objects in the batch, the gateway drain's largest batch
const BATCH_ITEMS: usize = 32;

/// Each object is coded: 4 KiB is past the inline limit
const OBJECT_BYTES: usize = 4 * 1024;

/// One request to a remote RPC
const ROUND_TRIP: Duration = Duration::from_millis(50);

/// A devnet slot, which a processed send waits out
const SLOT: Duration = Duration::from_millis(400);

/// How long a sent transaction takes to reach confirmed
const CONFIRM_LAG: Duration = Duration::from_millis(1200);

/// An agave chain where every call pays a round trip and a signature confirms a few slots after it is sent
#[derive(Clone)]
struct RemoteChain {
    inner: LiteSvmRpc,
    sent: Arc<Mutex<HashMap<Txid, Instant>>>,
}

impl RemoteChain {
    fn new(inner: LiteSvmRpc) -> Self {
        Self { inner, sent: Arc::new(Mutex::new(HashMap::new())) }
    }

    fn mark_sent(&self, txid: Txid) {
        self.sent.lock().expect("sent lock").insert(txid, Instant::now());
    }

    async fn wait_confirmed(&self, txid: &Txid) {
        let sent = self.sent.lock().expect("sent lock").get(txid).copied();
        if let Some(sent) = sent {
            sleep_until((sent + CONFIRM_LAG).into()).await;
        }
    }
}

#[async_trait]
impl Rpc for RemoteChain {
    fn commitment(&self) -> CommitmentLevel {
        self.inner.commitment()
    }

    // The inner litesvm stands in for tapesvm, and this chain stands in for agave.
    async fn is_tapesvm(&self) -> bool {
        false
    }

    async fn get_slot(&self) -> Result<u64, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_slot().await
    }

    async fn get_finalized_slot(&self) -> Result<u64, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_finalized_slot().await
    }

    async fn get_confirmed_slot(&self) -> Result<u64, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_confirmed_slot().await
    }

    async fn get_latest_blockhash(&self) -> Result<Hash, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_latest_blockhash().await
    }

    async fn get_genesis_hash(&self) -> Result<Hash, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_genesis_hash().await
    }

    async fn get_block(&self, slot: u64) -> Result<Block, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_block(slot).await
    }

    async fn get_blocks(&self, start: u64, end: u64) -> Result<Vec<u64>, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_blocks(start, end).await
    }

    async fn get_first_available_block(&self) -> Result<u64, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_first_available_block().await
    }

    async fn get_transaction(
        &self,
        txid: &Txid,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_transaction(txid).await
    }

    async fn get_block_height(&self) -> Result<u64, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_block_height().await
    }

    async fn get_account(&self, pubkey: &Address) -> Result<Account, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_account(pubkey).await
    }

    async fn get_account_with_commitment(
        &self,
        pubkey: &Address,
        commitment: CommitmentLevel,
        min_slot: Option<u64>,
    ) -> Result<Account, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_account_with_commitment(pubkey, commitment, min_slot).await
    }

    async fn get_multiple_accounts(
        &self,
        pubkeys: &[Address],
    ) -> Result<Vec<Option<Account>>, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_multiple_accounts(pubkeys).await
    }

    async fn get_program_accounts(
        &self,
        program_id: &Address,
        config: RpcProgramAccountsConfig,
    ) -> Result<Vec<(Address, Account)>, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_program_accounts(program_id, config).await
    }

    async fn send_transaction(
        &self,
        transaction: &VersionedTransaction,
    ) -> Result<Txid, RpcError> {
        sleep(ROUND_TRIP).await;
        let txid = self.inner.send_transaction(transaction).await?;
        self.mark_sent(txid);
        Ok(txid)
    }

    async fn simulate_transaction(
        &self,
        transaction: &VersionedTransaction,
    ) -> Result<SimulationResult, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.simulate_transaction(transaction).await
    }

    async fn send_and_confirm_transaction(
        &self,
        transaction: &VersionedTransaction,
        commitment: CommitmentLevel,
        skip_preflight: bool,
    ) -> Result<Txid, RpcError> {
        sleep(ROUND_TRIP).await;
        let txid = self
            .inner
            .send_and_confirm_transaction(transaction, commitment, skip_preflight)
            .await?;
        self.mark_sent(txid);
        match commitment {
            CommitmentLevel::Processed => sleep(SLOT).await,
            _ => self.wait_confirmed(&txid).await,
        }
        Ok(txid)
    }

    async fn get_signature_status(
        &self,
        txid: &Txid,
    ) -> Result<Option<Result<(), TransactionError>>, RpcError> {
        sleep(ROUND_TRIP).await;
        self.inner.get_signature_status(txid).await
    }

    async fn wait_for_signature(
        &self,
        txid: &Txid,
    ) -> Result<Result<(), TransactionError>, RpcError> {
        self.wait_confirmed(txid).await;
        sleep(ROUND_TRIP).await;
        self.inner.wait_for_signature(txid).await
    }
}

#[test]
fn object_batch_rate() {
    run_simnet_test(object_batch_rate_inner);
}

/// Time one batch of small coded objects on the in-process chain and on a devnet-latency chain, and read each back
async fn object_batch_rate_inner() {
    let node_count = TAPENET_GROUP_SIZE;
    let mut harness = SimnetBuilder::new()
        .node_count(node_count)
        .runtime_mode(NodeRuntimeMode::Full)
        .file_log(true)
        .build()
        .expect("build harness");

    let all: Vec<usize> = (0..node_count).collect();
    {
        let scenario = harness.scenario();
        scenario.init_system().await.expect("init system");
        scenario
            .register_nodes(BasisPoints(100))
            .await
            .expect("register nodes");
        scenario.stake_all(1_000).await.expect("stake nodes");
        scenario
            .set_spool_groups_many(&all, TARGET_GROUPS)
            .await
            .expect("set spool group preferences");
        scenario.start_network().await.expect("start network");
    }

    harness
        .start_all_with_retry(3, Duration::from_millis(200))
        .await
        .expect("start runtimes");

    let epoch_timeout = Duration::from_secs(TEST_MAX_EPOCH_DURATION.0 * 5);
    let scenario = harness.scenario();
    scenario
        .wait_nodes_healthy(Duration::from_secs(30))
        .await
        .expect("nodes healthy");
    scenario
        .wait_nodes_active(&all, Duration::from_secs(60))
        .await
        .expect("all nodes active");
    scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance to epoch 2");

    // Each batch starts on a fresh epoch, so a turn under it is the exception.
    let metrics = Arc::new(InMemory::new());
    let local = scenario.sdk(harness.admin()).with_metrics(metrics.clone());
    run_batch("local", &local, &metrics).await;
    let epoch = scenario.current_epoch_number().await.expect("read epoch");
    eprintln!("local: ended in epoch {epoch}");

    scenario
        .self_advance_epoch(epoch_timeout)
        .await
        .expect("advance before the remote batch");
    let started = scenario.current_epoch_number().await.expect("read epoch");
    let metrics = Arc::new(InMemory::new());
    let payer = CryptoKeypair::from_solana_keypair(harness.admin()).expect("convert payer");
    let remote = Tapedrive::new(RemoteChain::new(harness.chain().rpc().clone()), payer)
        .with_metrics(metrics.clone());
    run_batch("remote", &remote, &metrics).await;
    let epoch = scenario.current_epoch_number().await.expect("read epoch");
    eprintln!("remote: started in epoch {started}, ended in epoch {epoch}");

    harness.stop_all().await.expect("stop runtimes");
}

async fn run_batch<Blockchain: Rpc>(
    label: &str,
    sdk: &Tapedrive<Blockchain, HttpApi>,
    metrics: &InMemory,
) {
    let prefix = label;
    let bucket = TapeKey::generate();
    sdk.reserve(&bucket, StorageUnits::mb(8), 4)
        .await
        .expect("reserve bucket tape");

    let objects: Vec<ObjectBatchItem> = (0..BATCH_ITEMS)
        .map(|index| ObjectBatchItem {
            name: format!("{prefix}/object-{index:02}.bin"),
            data: object_bytes(index),
            content_type: ContentType::Unknown,
            plan: None,
        })
        .collect();

    let started = Instant::now();
    let receipts = sdk
        .put_objects_batch(&bucket, objects)
        .await
        .expect("put the batch");
    let wall = started.elapsed();
    assert_eq!(receipts.len(), BATCH_ITEMS, "one receipt per object");

    eprintln!(
        "{label}: {BATCH_ITEMS} objects in {:.2}s, {:.2} objects/s",
        wall.as_secs_f64(),
        BATCH_ITEMS as f64 / wall.as_secs_f64()
    );
    let mut rows: Vec<_> = metrics
        .snapshot()
        .into_iter()
        .filter(|(key, _)| key.operation == Operation::WriteBatch && key.outcome == Outcome::Ok)
        .collect();
    rows.sort_by_key(|(key, _)| key.phase.as_str());
    for (key, summary) in rows {
        eprintln!(
            "{label}: {:<16} count {:>3} total {:>8.3}s max {:>7.3}s",
            key.phase.as_str(),
            summary.count,
            summary.total.as_secs_f64(),
            summary.max.as_secs_f64()
        );
    }

    for (index, receipt) in receipts.iter().enumerate() {
        assert_eq!(receipt.name, format!("{prefix}/object-{index:02}.bin"), "receipts keep input order");
        assert!(
            is_certified(sdk, &receipt.address, Duration::from_secs(10)).await,
            "{} is certified",
            receipt.name
        );
        let read = sdk
            .get_object(&bucket.address(), &receipt.name)
            .await
            .expect("read the object back");
        assert_eq!(read, object_bytes(index), "{} round-trips", receipt.name);
    }
}

/// Peers learn of a certify a little after the chain, so poll for it
async fn is_certified<Blockchain: Rpc>(
    sdk: &Tapedrive<Blockchain, HttpApi>,
    track: &Address,
    timeout: Duration,
) -> bool {
    let started = Instant::now();
    loop {
        let current = sdk.get_track(track).await.expect("fetch batch track");
        if current.is_certified() {
            return true;
        }
        if started.elapsed() >= timeout {
            return false;
        }
        sleep(Duration::from_millis(200)).await;
    }
}

fn object_bytes(index: usize) -> Vec<u8> {
    (0..OBJECT_BYTES).map(|offset| (offset * 31 + index * 7) as u8).collect()
}
