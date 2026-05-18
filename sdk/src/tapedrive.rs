//! High-level client for the Tapedrive storage network.

use std::sync::Arc;

use arc_swap::ArcSwap;
use rpc::Rpc;
use rpc_client::RpcClient;
use peer_http::HttpApi;
use peer_manager::PeerManager;
use tape_core::prelude::{CompressedTrack, StorageUnits};
use tape_crypto::prelude::{Address, Hash, Keypair};
use tape_protocol::{Api, ProtocolState};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::metrics::{Metrics, Noop, Operation, Outcome, Phase, Timer};
use crate::stream::{
    read::{read_bytes, read_into},
    receipt::StreamReceipt,
    write::{write_bytes, write_stream},
};

/// High-level client for the Tapedrive storage network.
///
/// Generic over `Blockchain: Rpc` (on-chain) and `Cluster: Api` (storage nodes).
pub struct Tapedrive<Blockchain: Rpc, Cluster: Api> {
    pub state: ArcSwap<ProtocolState>,
    pub peer_manager: Arc<PeerManager>,
    pub api: Arc<Cluster>,
    pub rpc: Arc<RpcClient<Blockchain>>,
    pub payer: Option<Keypair>,
    pub metrics: Arc<dyn Metrics>,
}

/// Default constructor using `HttpApi`.
impl<Blockchain: Rpc> Tapedrive<Blockchain, HttpApi> {
    /// Create a new Tapedrive client.
    ///
    /// Takes an RPC backend and a payer keypair. Uses the default HTTP
    /// peer client for storage node communication.
    pub fn new(rpc: Blockchain, payer: Keypair) -> Self {
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
            metrics: Arc::new(Noop),
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
        payer: Option<Keypair>,
    ) -> Self {
        Self {
            state,
            peer_manager,
            api,
            rpc,
            payer,
            metrics: Arc::new(Noop),
        }
    }

    /// Attach or replace the payer used for mutating operations.
    pub fn with_payer(mut self, payer: Keypair) -> Self {
        self.payer = Some(payer);
        self
    }

    /// Attach or replace the metrics recorder.
    pub fn with_metrics(mut self, metrics: Arc<dyn Metrics>) -> Self {
        self.metrics = metrics;
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

    pub(crate) fn timer(&self, operation: Operation, phase: Phase) -> Timer<'_> {
        Timer::start(self.metrics.as_ref(), operation, phase)
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
        let total = self
            .timer(Operation::Write, Phase::Total)
            .bytes(data.len() as u64);

        let tape_key = TapeKey::generate();
        let capacity = StorageUnits::from_bytes(data.len() as u64);
        let reserve_capacity = capacity + StorageUnits::mb(1);

        let reserve = self.timer(Operation::Write, Phase::Reserve);
        let result = self.reserve(&tape_key, reserve_capacity, epochs).await;
        reserve.finish_result(&result);
        if let Err(error) = result {
            total.finish(Outcome::Error);
            return Err(error);
        }

        let result = self.write_track(&tape_key, key, data).await;
        total.finish_result(&result);
        let track = result?;

        Ok((tape_key, track))
    }

    /// Write in-memory bytes to an existing tape as a logical stream.
    ///
    /// Always writes a manifest track as the last track. For streams that fit
    /// in a single chunk, one data track and one manifest track are written.
    pub async fn write_bytes(
        &self,
        tape_key: &TapeKey,
        key: Hash,
        data: &[u8],
    ) -> Result<StreamReceipt, TapedriveError> {
        let timer = self
            .timer(Operation::WriteStream, Phase::Total)
            .bytes(data.len() as u64);
        let result = write_bytes(self, tape_key, key, data).await;
        timer.finish_result(&result);
        result
    }

    /// Write a byte stream from an async reader into an existing tape.
    ///
    /// The reader must yield exactly `size` bytes.
    pub async fn write_stream<Reader: AsyncRead + Unpin>(
        &self,
        tape_key: &TapeKey,
        key: Hash,
        size: StorageUnits,
        reader: Reader,
    ) -> Result<StreamReceipt, TapedriveError> {
        let timer = self
            .timer(Operation::WriteStream, Phase::Total)
            .bytes(size.to_bytes());
        let result = write_stream(self, tape_key, key, size, reader).await;
        timer.finish_result(&result);
        result
    }

    /// Read a stored stream by its manifest track address into memory.
    pub async fn read_bytes(
        &self,
        manifest: &Address,
    ) -> Result<Vec<u8>, TapedriveError> {
        let timer = self.timer(Operation::ReadStream, Phase::Total);
        let result = read_bytes(self, manifest).await;
        let timer = match &result {
            Ok(bytes) => timer.bytes(bytes.len() as u64),
            Err(_) => timer,
        };
        timer.finish_result(&result);
        result
    }

    /// Read a stored stream by its manifest track address into an async sink.
    pub async fn read_into<Writer: AsyncWrite + Unpin>(
        &self,
        manifest: &Address,
        writer: Writer,
    ) -> Result<(), TapedriveError> {
        let timer = self.timer(Operation::ReadStream, Phase::Total);
        let result = read_into(self, manifest, writer).await;
        timer.finish_result(&result);
        result
    }
}
