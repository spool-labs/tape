//! Gateway-backed read client.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use arc_swap::ArcSwap;
use peer_http::GatewayApi;
use peer_manager::PeerManager;
use rpc::Rpc;
use rpc_client::RpcClient;
use tape_api::program::tapedrive::track_pda;
use tape_crypto::hash::hash;
use tape_crypto::prelude::Address;
use tape_protocol::api::FindTrackVersion;
use tape_protocol::ProtocolState;

use crate::error::TapedriveError;
use crate::metrics::{Metrics, Noop, Operation, Phase};
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
        let timer = self.inner.timer(Operation::ReadTrack, Phase::Total);
        let result = self
            .inner
            .api
            .get_track_bytes(*track)
            .await
            .map_err(TapedriveError::Peer);
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
        let timer = self.inner.timer(Operation::ReadStream, Phase::Total);
        let result = self
            .inner
            .api
            .get_object_bytes(*track)
            .await
            .map_err(TapedriveError::Peer);
        let timer = match &result {
            Ok(bytes) => timer.bytes(bytes.len() as u64),
            Err(_) => timer,
        };
        timer.finish_result(&result);
        result
    }

    /// Read a stored stream by its manifest track address through the gateway.
    pub async fn read_bytes(&self, manifest: &Address) -> Result<Vec<u8>, TapedriveError> {
        self.read_object_by_track(manifest).await
    }

    /// Read a named object from a bucket through the gateway.
    pub async fn get_object(&self, bucket: &Address, name: &str) -> Result<Vec<u8>, TapedriveError> {
        let key = hash(name.as_bytes());
        let track = self
            .inner
            .find_track(bucket, key, FindTrackVersion::Latest)
            .await?;
        let address = track_pda(track.tape, track.track_number).0;
        self.read_object_by_track(&address).await
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
            inner: Self {
                state: ArcSwap::from_pointee(ProtocolState::default()),
                peer_manager,
                api,
                rpc: rpc_client,
                payer: None,
                metrics: Arc::new(Noop),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use rpc_litesvm::LiteSvmRpc;

    use crate::tapedrive::Tapedrive;

    #[test]
    fn gateway_constructor_builds_read_client() {
        let client =
            Tapedrive::new_gateway_read_only(LiteSvmRpc::new(), "http://127.0.0.1:8080///")
                .unwrap();

        assert_eq!(client.api.base_url(), "http://127.0.0.1:8080");
        assert!(client.payer.is_none());
    }
}
