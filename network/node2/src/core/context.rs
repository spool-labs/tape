use std::collections::HashSet;
use std::sync::Arc;
use solana_sdk::signature::{Keypair, Signature};
use solana_sdk::signer::Signer;

use peer_manager::{PeerManager, PeerManagerError};
use peer_http::HttpApi;
use rpc::Rpc;
use rpc_client::RpcClient;
use rpc_solana::SolanaRpc;
use store::Store;
use store_rocks::RocksStore;
use tape_api::program::tapedrive::node_pda;
use tape_core::bls::{BlsPrivateKey, BlsPubkey, BlsSignature};
use tape_core::spooler::SpoolIndex;
use tape_core::system::EpochPhase;
use tape_core::types::NodeId;
use tape_crypto::Pubkey;
use tape_crypto::bls12254::BLSError;
use tape_protocol::{Api, ProtocolState};
use tape_store::TapeStore;
use tape_store::ops::MetaOps;
use tape_store::types::NodeStatus;

use crate::core::config::NodeConfig;
use crate::core::error::NodeError;
use crate::core::state::StateBus;

pub type AppContext = Arc<NodeContext<RocksStore, HttpApi, SolanaRpc>>;

pub struct NodeContext<Db: Store, Cluster: Api, Blockchain: Rpc> {
    pub config: Arc<NodeConfig>,
    pub store: Arc<TapeStore<Db>>,
    pub rpc: Arc<RpcClient<Blockchain>>,
    pub state: StateBus,
    pub peer_manager: Arc<PeerManager>,
    pub api: Arc<Cluster>,

    node_id: NodeId,
    node_address: Pubkey,
    keypair: Arc<Keypair>,
    bls_keypair: Arc<BlsPrivateKey>,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> NodeContext<Db, Cluster, Blockchain> {
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn node_address(&self) -> Pubkey {
        self.node_address
    }

    pub fn pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    pub fn sign(&self, message: &[u8]) -> Signature {
        self.keypair.sign_message(message)
    }

    pub fn bls_pubkey(&self) -> BlsPubkey {
        self.bls_keypair.public_key().expect("bls public key")
    }

    pub fn bls_sign(&self, message: &[u8]) -> Result<BlsSignature, BLSError> {
        self.bls_keypair.sign(message)
    }

    pub fn state(&self) -> Arc<ProtocolState> {
        self.state.current()
    }

    pub fn set_state(&self, state: ProtocolState) -> Result<(), NodeError> {
        self.state.publish(state)
    }

    pub fn update_phase(&self, phase: EpochPhase) -> Result<(), NodeError> {
        let mut state = (*self.state()).clone();
        state.phase = phase;
        self.set_state(state)
    }

    pub fn subscribe_state(&self) -> tokio::sync::watch::Receiver<Arc<ProtocolState>> {
        self.state.subscribe()
    }

    pub async fn refresh_peers(&self) -> Result<(), PeerManagerError> {
        let state = self.state();
        self.peer_manager.resolve_peers(&self.rpc, state.as_ref()).await
    }

    pub fn node_status(&self) -> NodeStatus {
        let state = self.state();
        let in_committee = state.find_member(self.node_id).is_some();
        let bootstrap_in_next =
            state.committee.is_empty() && state.find_member_next(self.node_id).is_some();

        if in_committee || bootstrap_in_next {
            NodeStatus::Active
        } else {
            NodeStatus::Standby
        }
    }

    pub fn my_spools(&self) -> HashSet<SpoolIndex> {
        let state = self.state();
        match state.find_member(self.node_id) {
            Some((index, _)) => state.member_spools(index).into_iter().collect(),
            None => HashSet::new(),
        }
    }
}

pub struct NodeContextBuilder<Db: Store, Cluster: Api, Blockchain: Rpc> {
    config: NodeConfig,
    keypair: Keypair,
    bls_keypair: BlsPrivateKey,
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
        store: TapeStore<Db>,
        rpc: RpcClient<Blockchain>,
        peer_manager: Arc<PeerManager>,
        api: Arc<Cluster>,
    ) -> Self {
        Self {
            config,
            keypair,
            bls_keypair,
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
        let node = rpc.get_node(&keypair.pubkey()).await?;
        Ok(node.id)
    }

    pub async fn build(self) -> Result<Arc<NodeContext<Db, Cluster, Blockchain>>, NodeError> {
        let node_id = Self::resolve_node_id(&self.rpc, &self.keypair).await?;
        let (node_address, _) = node_pda(self.keypair.pubkey());

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
            store: Arc::new(self.store),
            rpc: Arc::new(self.rpc),
            state: StateBus::default(),
            peer_manager: self.peer_manager,
            api: self.api,
        }))
    }
}
