use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::watch::Receiver;

use peer_manager::{PeerManager, PeerManagerError};
use peer_http::HttpApi;
use rpc::Rpc;
use rpc_client::RpcClient;
use rpc_solana::SolanaRpc;
use store::{DiskTier, Store, StoreTier};
use store_rocks::SplitStore;
use tape_api::program::tapedrive::node_pda;
use tape_core::bls::{BlsPrivateKey, BlsPubkey, BlsSignature};
use tape_core::prelude::{EpochPhase, NodeId, NodeStatus, SpoolIndex};
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::prelude::{Address, BLSError, Keypair, Signature};
use tape_crypto::ed25519::Pubkey;
use tape_protocol::{Api, ProtocolState};
use tape_store::{TapeStore, ops::MetaOps};

use crate::config::node::NodeConfig;
use crate::core::error::NodeError;
use crate::core::ingest::{IngestBus, IngestState};
use crate::core::metrics::NodeMetrics;
use crate::core::state::StateBus;
use crate::features::block::pending_tracks::PendingTracks;
use crate::features::http::admission::AdmissionLimiter;

pub type AppContext = Arc<NodeContext<SplitStore, HttpApi, SolanaRpc>>;

pub struct NodeContext<Db: Store, Cluster: Api, Blockchain: Rpc> {
    pub config: Arc<NodeConfig>,
    pub store: Arc<TapeStore<Db>>,
    pub rpc: Arc<RpcClient<Blockchain>>,
    pub state: StateBus,
    pub ingest: IngestBus,
    pub pending: Arc<PendingTracks>,
    pub peer_manager: Arc<PeerManager>,
    pub api: Arc<Cluster>,
    pub admission: Arc<AdmissionLimiter>,
    pub metrics: NodeMetrics,

    node_id: NodeId,
    node_address: Address,
    keypair: Arc<Keypair>,
    bls_keypair: Arc<BlsPrivateKey>,
    tls_keypair: Arc<Keypair>,
    reclaim_pending: AtomicBool,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> NodeContext<Db, Cluster, Blockchain> {
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn node_address(&self) -> Address {
        self.node_address
    }

    pub fn pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    pub fn signer(&self) -> &Keypair {
        self.keypair.as_ref()
    }

    pub fn sign(&self, message: &[u8]) -> Signature {
        self.keypair.sign(message)
    }

    pub fn bls_pubkey(&self) -> Result<BlsPubkey, BLSError> {
        self.bls_keypair.public_key()
    }

    pub fn bls_sign(&self, message: &[u8]) -> Result<BlsSignature, BLSError> {
        self.bls_keypair.sign(message)
    }

    pub fn tls_keypair(&self) -> &Keypair {
        self.tls_keypair.as_ref()
    }

    pub fn tls_pubkey(&self) -> NetworkTlsPubkey {
        NetworkTlsPubkey::new(self.tls_keypair.pubkey().to_bytes())
    }

    pub fn state(&self) -> Arc<ProtocolState> {
        self.state.current()
    }

    pub fn phase(&self) -> EpochPhase {
        self.state().phase()
    }

    pub fn set_state(&self, state: ProtocolState) -> Result<(), NodeError> {
        self.state.publish(state)
    }

    pub fn update_phase(&self, phase: EpochPhase) -> Result<(), NodeError> {
        let mut state = (*self.state()).clone();
        state.current.epoch.state.phase = phase as u64;
        self.set_state(state)
    }

    pub fn subscribe_state(&self) -> Receiver<Arc<ProtocolState>> {
        self.state.subscribe()
    }

    pub fn ingest_state(&self) -> IngestState {
        self.ingest.current()
    }

    pub fn subscribe_ingest(&self) -> Receiver<IngestState> {
        self.ingest.subscribe()
    }

    pub fn is_at_tip(&self) -> bool {
        self.ingest.is_at_tip()
    }

    pub async fn refresh_peers(&self) -> Result<(), PeerManagerError> {
        let state = self.state();
        self.peer_manager.resolve_peers(state.as_ref())?;
        self.peer_manager.refresh_registered_nodes(&self.rpc).await
    }

    pub fn node_status(&self) -> NodeStatus {
        let state = self.state();
        let in_committee = state.find_member(self.node_address).is_some();
        let bootstrap_in_next =
            state.current.committee.is_empty() && state.find_member_next(self.node_address).is_some();

        if in_committee || bootstrap_in_next {
            NodeStatus::Active
        } else {
            NodeStatus::Standby
        }
    }

    pub fn my_spools(&self) -> HashSet<SpoolIndex> {
        let state = self.state();
        match state.find_member(self.node_address) {
            Some(_) => state.member_spools(self.node_address).into_iter().collect(),
            None => HashSet::new(),
        }
    }

    pub fn is_reclaim_pending(&self) -> bool {
        self.reclaim_pending.load(Ordering::Relaxed)
    }

    pub fn set_reclaim_pending(&self, is_pending: bool) {
        self.reclaim_pending.store(is_pending, Ordering::Relaxed);
    }

    /// Whether new uploads should be rejected because a tier is low on space.
    ///
    /// Disabled (always false) unless a free-space floor is configured.
    pub fn is_write_throttled(&self) -> bool {
        let min_free = self.config.store.min_free_bytes;
        let bulk_min_free = self.config.store.bulk_min_free_bytes;
        if min_free == 0 && bulk_min_free == 0 {
            return false;
        }
        match self.store.inner().inner().disk_tiers() {
            Ok(tiers) => tier_below_threshold(&tiers, min_free, bulk_min_free),
            Err(_) => false,
        }
    }
}

/// Whether any tier's free space sits below its configured floor. A zero floor
/// or an unknown free figure never throttles.
fn tier_below_threshold(tiers: &[DiskTier], min_free: u64, bulk_min_free: u64) -> bool {
    for tier in tiers {
        let floor = match tier.tier {
            StoreTier::Primary => min_free,
            StoreTier::Bulk => bulk_min_free,
        };
        if floor == 0 {
            continue;
        }
        if tier.free_bytes.is_some_and(|free| free < floor) {
            return true;
        }
    }
    false
}

pub struct NodeContextBuilder<Db: Store, Cluster: Api, Blockchain: Rpc> {
    config: NodeConfig,
    keypair: Keypair,
    bls_keypair: BlsPrivateKey,
    tls_keypair: Arc<Keypair>,
    store: TapeStore<Db>,
    rpc: RpcClient<Blockchain>,
    peer_manager: Arc<PeerManager>,
    api: Arc<Cluster>,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> NodeContextBuilder<Db, Cluster, Blockchain> {
    pub fn new(
        config: NodeConfig,
        keypair: Keypair,
        bls_keypair: BlsPrivateKey,
        tls_keypair: Arc<Keypair>,
        store: TapeStore<Db>,
        rpc: RpcClient<Blockchain>,
        peer_manager: Arc<PeerManager>,
        api: Arc<Cluster>,
    ) -> Self {
        Self {
            config,
            keypair,
            bls_keypair,
            tls_keypair,
            store,
            rpc,
            peer_manager,
            api,
        }
    }

    async fn resolve_node_id(
        rpc: &RpcClient<Blockchain>,
        keypair: &Keypair,
    ) -> Result<NodeId, NodeError> {
        let authority = keypair.address();
        let node = rpc.get_node(&authority).await?;
        Ok(node.id)
    }

    pub async fn build(self) -> Result<Arc<NodeContext<Db, Cluster, Blockchain>>, NodeError> {
        let node_id = Self::resolve_node_id(&self.rpc, &self.keypair).await?;
        let (node_address, _) = node_pda(self.keypair.address());
        let admission = Arc::new(AdmissionLimiter::new(self.config.http.admission.clone()));

        self.store
            .set_node_id(node_id)
            .map_err(|error| NodeError::Store(format!("set_node_id: {error}")))?;

        self.store
            .set_node_address(node_address.into())
            .map_err(|error| NodeError::Store(format!("set_node_address: {error}")))?;

        Ok(Arc::new(NodeContext {
            node_id,
            node_address,
            config: Arc::new(self.config),
            keypair: Arc::new(self.keypair),
            bls_keypair: Arc::new(self.bls_keypair),
            tls_keypair: self.tls_keypair,
            store: Arc::new(self.store),
            rpc: Arc::new(self.rpc),
            state: StateBus::default(),
            ingest: IngestBus::default(),
            pending: Arc::new(PendingTracks::new()),
            peer_manager: self.peer_manager,
            api: self.api,
            admission,
            metrics: NodeMetrics::default(),
            reclaim_pending: AtomicBool::new(false),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use peer_manager::PeerManager;
    use peer_memory::MemoryApi;
    use rpc_client::RpcClient;
    use store_memory::MemoryStore;
    use tape_chain_harness::ChainHarness;
    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_crypto::ed25519::Keypair;
    use tape_store::ops::MetaOps;
    use tape_store::TapeStore;

    use super::{tier_below_threshold, NodeConfig, NodeContextBuilder};
    use store::{DiskTier, StoreTier};

    fn tier(role: StoreTier, free: Option<u64>) -> DiskTier {
        DiskTier { tier: role, used_bytes: 0, free_bytes: free }
    }

    // a tier below its floor throttles; zero floor or unknown free never does
    #[test]
    fn throttle_thresholds() {
        let tiers = vec![
            tier(StoreTier::Primary, Some(10_000)),
            tier(StoreTier::Bulk, Some(500)),
        ];

        assert!(!tier_below_threshold(&tiers, 0, 0));
        assert!(tier_below_threshold(&tiers, 0, 1_000));
        assert!(!tier_below_threshold(&tiers, 0, 400));
        assert!(tier_below_threshold(&tiers, 20_000, 0));
        assert!(!tier_below_threshold(&[tier(StoreTier::Bulk, None)], 0, 1_000));
    }

    #[tokio::test]
    async fn resolves_identity() {
        let harness = ChainHarness::builder()
            .nodes(25)
            .epoch(EpochNumber(3))
            .build()
            .await
            .expect("build harness");
        let node = harness.node(7);
        let store = TapeStore::new(MemoryStore::new());
        let rpc = RpcClient::from_rpc(harness.rpc().clone());
        let mut rng = rand::thread_rng();
        let tls = Arc::new(Keypair::new(&mut rng));
        let ctx = NodeContextBuilder::new(
            test_config(),
            clone_keypair(node.keypair()),
            *node.bls_keypair(),
            tls,
            store,
            rpc,
            Arc::new(PeerManager::new()),
            Arc::new(MemoryApi::noop()),
        )
        .build()
        .await
        .expect("build context");

        assert_eq!(ctx.node_id(), node.node_id);
        assert_eq!(ctx.node_address(), node.node_address.into());
        assert_eq!(ctx.store.get_node_id().expect("get node id"), Some(node.node_id));
        assert_eq!(
            ctx.store.get_node_address().expect("get node address"),
            Some(node.node_address.into())
        );
    }

    fn clone_keypair(keypair: &solana_keypair::Keypair) -> Keypair {
        Keypair::from_solana_keypair(keypair).expect("clone keypair")
    }

    fn test_config() -> NodeConfig {
        let mut config = NodeConfig::default();
        config.node.node_keypair = PathBuf::from("/dev/null");
        config.node.bls_keypair = PathBuf::from("/dev/null");
        config.solana.rpc = "http://localhost:8899".into();
        config.solana.start_slot = Some(SlotNumber(0));
        config.store.path = PathBuf::from("/tmp");
        config
    }
}
