//! High-level client for the Tapedrive storage network.

use std::sync::Arc;

use arc_swap::ArcSwap;
use solana_sdk::{pubkey::Pubkey, signature::Keypair};

use rpc::{Rpc};
use rpc_client::RpcClient;
use peer_http::HttpApi;
use peer_manager::PeerManager;
use tape_core::track::types::CompressedTrack;
use tape_core::types::StorageUnits;
use tape_crypto::Hash;
use tape_protocol::{Api, ProtocolState};

use crate::error::TapedriveError;
use crate::file::{read::read_file, receipt::FileReceipt, write::write_file};
use crate::keys::tape_key::TapeKey;

/// High-level client for the Tapedrive storage network.
///
/// Generic over `Blockchain: Rpc` (on-chain) and `Cluster: Api` (storage nodes).
pub struct Tapedrive<Blockchain: Rpc, Cluster: Api> {
    pub state: ArcSwap<ProtocolState>,
    pub peer_manager: Arc<PeerManager>,
    pub api: Arc<Cluster>,
    pub rpc: Arc<RpcClient<Blockchain>>,
    pub payer: Option<Keypair>,
}

/// Default constructor using `HttpApi`.
impl<Blockchain: Rpc> Tapedrive<Blockchain, HttpApi> {
    /// Create a new Tapedrive client.
    ///
    /// Takes an RPC backend and a payer keypair. Uses the default HTTP
    /// peer client for storage node communication.
    pub fn new(rpc: Blockchain, payer: &Keypair) -> Self {
        Self::new_read_only(rpc).with_payer(payer)
    }

    /// Create a read-only Tapedrive client.
    pub fn new_read_only(rpc: Blockchain) -> Self {
        let rpc_client = Arc::new(RpcClient::from_rpc(rpc));
        let peer_manager = Arc::new(PeerManager::new());
        let api = Arc::new(HttpApi::with_default_timeouts(peer_manager.clone()));
        Self {
            state: ArcSwap::from_pointee(ProtocolState::default()),
            peer_manager,
            api,
            rpc: rpc_client,
            payer: None,
        }
    }
}

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Create a Tapedrive client from existing parts.
    pub fn from_parts(
        state: ArcSwap<ProtocolState>,
        peer_manager: Arc<PeerManager>,
        api: Arc<Cluster>,
        rpc: Arc<RpcClient<Blockchain>>,
        payer: Option<&Keypair>,
    ) -> Self {
        Self {
            state,
            peer_manager,
            api,
            rpc,
            payer: payer.map(clone_keypair),
        }
    }

    /// Attach or replace the payer used for mutating operations.
    pub fn with_payer(mut self, payer: &Keypair) -> Self {
        self.payer = Some(clone_keypair(payer));
        self
    }

    /// Access the underlying RPC client.
    pub fn rpc(&self) -> &RpcClient<Blockchain> {
        &self.rpc
    }

    /// Load the current protocol state (lock-free).
    pub fn state(&self) -> arc_swap::Guard<Arc<ProtocolState>> {
        self.state.load()
    }

    /// Return the payer keypair required for mutating operations.
    pub fn payer(&self) -> Result<&Keypair, TapedriveError> {
        self.payer.as_ref().ok_or(TapedriveError::MissingPayer)
    }

    /// Write data to the network in one call.
    ///
    /// Creates a tape sized to fit `data` exactly, registers a track,
    /// uploads erasure-coded slices to storage nodes, and certifies the
    /// track with BLS signatures.
    ///
    /// Returns the tape key (save it!) and the registered track.
    pub async fn write(
        &self,
        key: Hash,
        data: &[u8],
        epochs: u64,
    ) -> Result<(TapeKey, CompressedTrack), TapedriveError> {
        let tape_key = TapeKey::generate();
        let capacity = StorageUnits::from_bytes(data.len() as u64);
        let reserve_capacity = capacity + StorageUnits::mb(1);

        self.reserve(&tape_key, reserve_capacity, epochs).await?;

        let track = self.write_track(&tape_key, key, data).await?;

        Ok((tape_key, track))
    }

    /// Write a file to an existing tape, chunking automatically if needed.
    ///
    /// Always writes a manifest track as the last track. For files that fit
    /// in a single chunk, one data track + one manifest track are written.
    ///
    /// Returns a [`FileReceipt`] whose `manifest` field is the file's handle.
    pub async fn write_file(
        &self,
        tape_key: &TapeKey,
        key: Hash,
        data: &[u8],
    ) -> Result<FileReceipt, TapedriveError> {
        write_file(self, tape_key, key, data).await
    }

    /// Read a file by its manifest track address.
    ///
    /// Reads the manifest, fetches all chunks in parallel, and reassembles
    /// the original data.
    pub async fn read_file(
        &self,
        manifest: &Pubkey,
    ) -> Result<Vec<u8>, TapedriveError> {
        read_file(self, manifest).await
    }
}

fn clone_keypair(payer: &Keypair) -> Keypair {
    Keypair::try_from(payer.to_bytes().as_ref()).unwrap()
}
