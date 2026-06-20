//! High-level client for the Tapedrive storage network.

use std::sync::Arc;

use arc_swap::ArcSwap;
use peer_http::{GatewayApi, HttpApi};
use peer_manager::PeerManager;
use rpc::Rpc;
use rpc_client::RpcClient;
use tape_core::prelude::{CompressedTrack, StorageUnits};
use tape_core::types::ContentType;
use tape_crypto::prelude::{Address, Keypair};
use tape_protocol::{Api, ProtocolState};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::metrics::{Metrics, Noop, Operation, Outcome, Phase, Timer};
use crate::stream::{
    read::{read_bytes, read_into},
    receipt::StreamReceipt,
    write::{write_bytes as write_stream_bytes, write_stream as write_reader_stream},
};
use crate::track::write::{UNNAMED_TRACK, UNTYPED_TRACK};

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

/// Read-only constructor using a public gateway endpoint.
impl<Blockchain: Rpc> Tapedrive<Blockchain, GatewayApi> {
    /// Create a read-only Tapedrive client that sends storage reads through a
    /// gateway URL instead of directly contacting storage nodes.
    pub fn new_gateway_read_only(
        rpc: Blockchain,
        gateway_url: impl Into<String>,
    ) -> Result<Self, TapedriveError> {
        let rpc_client = Arc::new(RpcClient::from_rpc(rpc));
        let peer_manager = Arc::new(PeerManager::new());
        let api = Arc::new(GatewayApi::new(gateway_url)?);
        Ok(Self {
            state: ArcSwap::from_pointee(ProtocolState::default()),
            peer_manager,
            api,
            rpc: rpc_client,
            payer: None,
            metrics: Arc::new(Noop),
        })
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

    /// Write unnamed content-addressed data to the network in one call.
    ///
    /// Creates a tape sized to fit `data` exactly, registers a track,
    /// uploads erasure-coded slices to storage nodes, and certifies the
    /// track with BLS signatures. Unnamed tracks are excluded from object
    /// listings.
    ///
    /// Returns the tape key (save it!) and the registered track.
    pub async fn write(
        &self,
        data: &[u8],
        epochs: u64,
    ) -> Result<(TapeKey, CompressedTrack), TapedriveError> {
        self.write_named(
            UNNAMED_TRACK,
            UNTYPED_TRACK,
            data,
            epochs,
        )
        .await
    }

    /// Write named data to the network in one call.
    ///
    /// Named tracks on non-system tapes are materialized into object listings.
    pub async fn write_named(
        &self,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
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

        let result = self
            .write_named_track(&tape_key, name, content_type, data)
            .await;
        total.finish_result(&result);
        let track = result?;

        Ok((tape_key, track))
    }

    /// Write unnamed in-memory bytes to an existing tape as a logical stream.
    ///
    /// Always writes a manifest track as the last track. For streams that fit
    /// in a single chunk, one data track and one manifest track are written.
    /// The manifest and chunks are excluded from object listings.
    pub async fn write_bytes(
        &self,
        tape_key: &TapeKey,
        data: &[u8],
    ) -> Result<StreamReceipt, TapedriveError> {
        self.write_named_bytes(
            tape_key,
            UNNAMED_TRACK,
            UNTYPED_TRACK,
            data,
        )
        .await
    }

    /// Write named in-memory bytes to an existing tape as a logical stream.
    ///
    /// The manifest track carries the object's name and content type; internal
    /// chunk tracks remain unnamed.
    pub async fn write_named_bytes(
        &self,
        tape_key: &TapeKey,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
        data: &[u8],
    ) -> Result<StreamReceipt, TapedriveError> {
        let timer = self
            .timer(Operation::WriteStream, Phase::Total)
            .bytes(data.len() as u64);
        let result = write_stream_bytes(self, tape_key, name.as_ref(), content_type, data).await;
        timer.finish_result(&result);
        result
    }

    /// Write an unnamed byte stream from an async reader into an existing tape.
    ///
    /// The reader must yield exactly `size` bytes. The manifest and chunks are
    /// excluded from object listings.
    pub async fn write_stream<Reader: AsyncRead + Unpin>(
        &self,
        tape_key: &TapeKey,
        size: StorageUnits,
        reader: Reader,
    ) -> Result<StreamReceipt, TapedriveError> {
        self.write_named_stream(
            tape_key,
            UNNAMED_TRACK,
            UNTYPED_TRACK,
            size,
            reader,
        )
        .await
    }

    /// Write a named byte stream from an async reader into an existing tape.
    ///
    /// The manifest track carries the object's name and content type; internal
    /// chunk tracks remain unnamed.
    pub async fn write_named_stream<Reader: AsyncRead + Unpin>(
        &self,
        tape_key: &TapeKey,
        name: impl AsRef<[u8]>,
        content_type: ContentType,
        size: StorageUnits,
        reader: Reader,
    ) -> Result<StreamReceipt, TapedriveError> {
        let timer = self
            .timer(Operation::WriteStream, Phase::Total)
            .bytes(size.to_bytes());
        let result =
            write_reader_stream(self, tape_key, name.as_ref(), content_type, size, reader).await;
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

#[cfg(test)]
mod tests {
    use rpc_litesvm::LiteSvmRpc;

    use super::Tapedrive;

    #[test]
    fn gateway_constructor_builds_read_client() {
        let client =
            Tapedrive::new_gateway_read_only(LiteSvmRpc::new(), "http://127.0.0.1:8080///")
                .unwrap();

        assert_eq!(client.api.base_url(), "http://127.0.0.1:8080");
        assert!(client.payer.is_none());
    }
}
