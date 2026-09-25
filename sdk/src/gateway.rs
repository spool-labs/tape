//! Gateway-backed read client.

use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use arc_swap::ArcSwap;
use tape_peer_http::GatewayApi;
use tape_peer_manager::PeerManager;
use tape_rpc::Rpc;
use tape_rpc_client::RpcClient;
use tape_crypto::prelude::Address;
use tape_protocol::api::ApiError;
use tape_protocol::ProtocolState;
use tokio::io::AsyncWrite;

use crate::error::TapedriveError;
use crate::metrics::{Metrics, Operation, Phase};
use crate::tapedrive::Tapedrive;

/// Read-only client for gateway-backed reads.
///
/// The inner `GatewayApi` still implements the node `Api` trait for metadata
/// and compatibility, but whole-track and whole-object reads use the gateway's
/// decoded byte endpoints directly.
pub struct Gateway<Blockchain: Rpc> {
    inner: Tapedrive<Blockchain, GatewayApi>,
}

impl<Blockchain: Rpc> Gateway<Blockchain> {
    pub fn inner(&self) -> &Tapedrive<Blockchain, GatewayApi> {
        &self.inner
    }

    pub fn into_inner(self) -> Tapedrive<Blockchain, GatewayApi> {
        self.inner
    }

    pub fn api(&self) -> &GatewayApi {
        self.inner.api.as_ref()
    }

    /// Attach or replace the metrics recorder.
    pub fn with_metrics(mut self, metrics: Arc<dyn Metrics>) -> Self {
        self.inner.metrics = metrics;
        self
    }

    /// Read exact decoded bytes for one track through the gateway.
    pub async fn read(&self, track: &Address) -> Result<Vec<u8>, TapedriveError> {
        self.read_track(track).await
    }

    /// Read exact decoded bytes for one track through the gateway.
    pub async fn read_track(&self, track: &Address) -> Result<Vec<u8>, TapedriveError> {
        self.timed_bytes(Operation::ReadTrack, self.inner.api.get_track_bytes(*track))
            .await
    }

    /// Time a gateway byte fetch, recording the byte count on success.
    async fn timed_bytes<Request>(
        &self,
        operation: Operation,
        request: Request,
    ) -> Result<Vec<u8>, TapedriveError>
    where
        Request: Future<Output = Result<Vec<u8>, ApiError>>,
    {
        let timer = self.inner.timer(operation, Phase::Total);
        let result = request.await.map_err(TapedriveError::from);
        let timer = match &result {
            Ok(bytes) => timer.bytes(bytes.len() as u64),
            Err(_) => timer,
        };
        timer.finish_result(&result);
        result
    }

    /// Read logical object bytes for a representing track through the gateway.
    ///
    /// If the track is a stream manifest, the gateway follows its chunk tracks
    /// and returns the full object.
    pub async fn read_object_by_track(&self, track: &Address) -> Result<Vec<u8>, TapedriveError> {
        self.timed_bytes(Operation::ReadStream, self.inner.api.get_object_bytes(*track))
            .await
    }

    /// Read a stored stream by its manifest track address through the gateway.
    pub async fn read_bytes(&self, manifest: &Address) -> Result<Vec<u8>, TapedriveError> {
        self.read_object_by_track(manifest).await
    }

    /// Read a byte window of a logical object through the gateway: `len` bytes
    /// starting at `start`. A window whose end runs past the object is
    /// clamped, so the result can be shorter than `len`; a `start` at or past
    /// the object end is an error. A stream manifest decodes only the chunks
    /// the window touches.
    pub async fn read_range(
        &self,
        track: &Address,
        start: u64,
        len: u64,
    ) -> Result<Vec<u8>, TapedriveError> {
        self.timed_bytes(
            Operation::ReadStream,
            self.inner.api.get_object_bytes_range(*track, start, len),
        )
        .await
    }

    /// Copy logical object bytes for a representing track into a sink as they arrive
    pub async fn read_object_into<Writer: AsyncWrite + Unpin>(
        &self,
        track: &Address,
        writer: &mut Writer,
    ) -> Result<u64, TapedriveError> {
        let timer = self.inner.timer(Operation::ReadStream, Phase::Total);
        let result = self
            .inner
            .api
            .copy_object_into(*track, writer)
            .await
            .map_err(TapedriveError::from);
        let timer = match &result {
            Ok(copied) => timer.bytes(*copied),
            Err(_) => timer,
        };
        timer.finish_result(&result);
        result
    }

    /// Copy an object from a bucket by key into a sink as its bytes arrive
    pub async fn get_object_into<Writer: AsyncWrite + Unpin>(
        &self,
        bucket: &Address,
        name: &str,
        writer: &mut Writer,
    ) -> Result<u64, TapedriveError> {
        let timer = self.inner.timer(Operation::ReadStream, Phase::Total);
        let result = self
            .inner
            .api
            .copy_bucket_object_into(*bucket, name, writer)
            .await
            .map_err(TapedriveError::from);
        let timer = match &result {
            Ok(copied) => timer.bytes(*copied),
            Err(_) => timer,
        };
        timer.finish_result(&result);
        result
    }

    /// Read a named object from a bucket through the gateway.
    pub async fn get_object(&self, bucket: &Address, name: &str) -> Result<Vec<u8>, TapedriveError> {
        self.timed_bytes(Operation::ReadStream, self.inner.api.get_bucket_object_bytes(*bucket, name))
            .await
    }
}

impl<Blockchain: Rpc> Deref for Gateway<Blockchain> {
    type Target = Tapedrive<Blockchain, GatewayApi>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<Blockchain: Rpc> DerefMut for Gateway<Blockchain> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Read-only constructor using a public gateway endpoint.
impl<Blockchain: Rpc> Tapedrive<Blockchain, GatewayApi> {
    /// Create a read-only gateway client that uses decoded gateway endpoints
    /// for whole-track and whole-object reads.
    pub fn new_gateway_read_only(
        rpc: Blockchain,
        gateway_url: impl Into<String>,
    ) -> Result<Gateway<Blockchain>, TapedriveError> {
        let rpc_client = Arc::new(RpcClient::from_rpc(rpc));
        let peer_manager = Arc::new(PeerManager::new());
        let api = Arc::new(GatewayApi::new(gateway_url)?);
        Ok(Gateway {
            inner: Self::from_parts(
                ArcSwap::from_pointee(ProtocolState::default()),
                peer_manager,
                api,
                rpc_client,
                None,
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use axum::Router;
    use axum::routing::{get, post};
    use bytemuck::Zeroable;
    use rpc_litesvm::LiteSvmRpc;
    use tape_core::track::types::CompressedTrack;
    use tape_crypto::hash::hash;
    use tape_crypto::prelude::Address;
    use tape_protocol::api::{FindTrackVersion, TAPE_TRACK_FIND_PATH, TrackResponse};

    use crate::bootstrap::HEDGE_DELAY;
    use crate::tapedrive::Tapedrive;

    /// Serve find-track and bucket reads slower than the hedge delay, counting every request
    async fn slow_gateway(requests: Arc<AtomicUsize>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let base = format!("http://{}", listener.local_addr().expect("address"));
        let (finds, reads) = (requests.clone(), requests);
        let router = Router::new()
            .route(
                TAPE_TRACK_FIND_PATH,
                post(move || async move {
                    finds.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(HEDGE_DELAY * 3).await;
                    let response = TrackResponse { track: CompressedTrack::zeroed().pack() };
                    wincode::serialize(&response).expect("encode track")
                }),
            )
            .route(
                "/bucket/{tape}/{*key}",
                get(move || async move {
                    reads.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(HEDGE_DELAY * 3).await;
                    b"object bytes".to_vec()
                }),
            );
        tokio::spawn(async move { axum::serve(listener, router).await });
        base
    }

    // a slow gateway still gets one request per lookup
    #[tokio::test]
    async fn one_request_lookup() {
        let requests = Arc::new(AtomicUsize::new(0));
        let base = slow_gateway(requests.clone()).await;
        let gateway = Tapedrive::new_gateway_read_only(LiteSvmRpc::new(), base).expect("client");

        let found = gateway.find_track(&Address::new_unique(), hash(b"key"), FindTrackVersion::Latest).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(found.is_ok());
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    // a read by bucket and key through a gateway is one request
    #[tokio::test]
    async fn one_request_object() {
        let requests = Arc::new(AtomicUsize::new(0));
        let base = slow_gateway(requests.clone()).await;
        let gateway = Tapedrive::new_gateway_read_only(LiteSvmRpc::new(), base).expect("client");

        let read = gateway.get_object(&Address::new_unique(), "photos/june.png").await;

        assert_eq!(read.expect("read"), b"object bytes");
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn gateway_constructor_builds_read_client() {
        let client =
            Tapedrive::new_gateway_read_only(LiteSvmRpc::new(), "http://127.0.0.1:8080///")
                .unwrap();

        assert_eq!(client.api.base_url(), "http://127.0.0.1:8080");
        assert!(client.payer.is_none());
    }
}
