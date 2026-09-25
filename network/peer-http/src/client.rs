use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tape_peer_manager::{PeerManager, PeerNode};
use tape_core::track::types::{CompressedTrack, CompressedTrackProof};
use tape_core::types::network::NetworkAddress;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_protocol::api::*;
use tape_crypto::Address;
use tape_crypto::ed25519::Keypair;

use crate::builder::HttpApiBuilder;
use crate::metrics::ApiMetrics;

/// Per-request timeout for vote calls.
const VOTE_TIMEOUT: Duration = Duration::from_secs(3);

/// Connections a peer's slice bodies spread over; one h2 client holds one socket.
const SLICE_CONNECTIONS: usize = 8;

/// Receive windows wide enough for a whole slice body in one round trip.
const H2_STREAM_WINDOW: u32 = 16 * 1024 * 1024;
const H2_CONNECTION_WINDOW: u32 = 64 * 1024 * 1024;

/// The clients pinned to one peer's TLS public key. Cached on the `HttpApi` and
/// invalidated when PeerManager observes a change to the peer's on-chain
/// `network_tls` field.
#[derive(Clone)]
pub struct PinnedPeerClient {
    pub ring: Arc<Vec<reqwest::Client>>,
    pub cursor: Arc<AtomicUsize>,
    pub in_flight: Arc<Vec<AtomicUsize>>,
    pub tls_pubkey: NetworkTlsPubkey,
    pub network_address: NetworkAddress,
}

/// Keeps a ring connection counted busy until the slice push on it ends
pub struct SlotLease {
    in_flight: Arc<Vec<AtomicUsize>>,
    slot: usize,
}

impl Drop for SlotLease {
    fn drop(&mut self) {
        self.in_flight[self.slot].fetch_sub(1, Ordering::Relaxed);
    }
}

impl PinnedPeerClient {
    /// The connection every call outside a slice body speaks over.
    pub fn first(&self) -> reqwest::Client {
        self.ring[0].clone()
    }

    /// The next connection in the ring, for one slice body.
    pub fn next(&self) -> reqwest::Client {
        let step = self.cursor.fetch_add(1, Ordering::Relaxed);
        self.ring[step % self.ring.len()].clone()
    }

    /// The first idle connection for one slice push, so a later slot opens only when the earlier ones are busy
    pub fn lease(&self) -> (reqwest::Client, SlotLease) {
        let mut chosen = 0;
        let mut least = usize::MAX;
        for (slot, count) in self.in_flight.iter().enumerate() {
            let busy = count.load(Ordering::Relaxed);
            if busy < least {
                chosen = slot;
                least = busy;
            }
            if busy == 0 {
                break;
            }
        }
        self.in_flight[chosen].fetch_add(1, Ordering::Relaxed);
        let lease = SlotLease {
            in_flight: self.in_flight.clone(),
            slot: chosen,
        };
        (self.ring[chosen].clone(), lease)
    }
}

/// One completed peer call that moved payload bytes.
#[derive(Clone, Copy, Debug)]
pub struct PeerTransfer {
    /// The remote node.
    pub node: Address,
    /// Which api call moved the bytes.
    pub op: &'static str,
    /// True when this node sent the bytes, false when it received them.
    pub sent: bool,
    /// Payload bytes moved.
    pub bytes: u64,
}

/// Callback receiving every byte-moving peer call, for live traffic feeds.
pub type TransferSink = Arc<dyn Fn(PeerTransfer) + Send + Sync>;

pub struct HttpApi {
    pub peer_manager: Arc<PeerManager>,
    pub clients: Arc<DashMap<Address, PinnedPeerClient>>,
    pub metrics: Option<Arc<ApiMetrics>>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub put_slice_timeout: Duration,
    pub get_slice_timeout: Duration,
    pub local_identity: Option<Arc<Keypair>>,
    pub transfer_sink: Option<TransferSink>,
}

impl HttpApi {
    /// Build an HttpApi with default timeouts. Equivalent to
    /// `HttpApiBuilder::new().build(peer_manager)` and cannot fail in practice
    /// since the builder installs the rustls crypto provider once.
    pub fn with_default_timeouts(peer_manager: Arc<PeerManager>) -> Self {
        HttpApiBuilder::new()
            .build(peer_manager)
            .expect("default peer HTTP client config should build")
    }

    /// Get-or-build a pinned HTTPS client for the given peer. Rebuilds when the
    /// cached entry's TLS pubkey or network address differ from the current
    /// PeerNode snapshot (which is how we handle peer key rotations).
    fn client_for(&self, peer: &PeerNode) -> Result<(PinnedPeerClient, String), ApiError> {
        let url = https_base_url(peer.network_address)?;
        // Callers racing on a new peer share one ring, and so its connections.
        let pinned = match self.clients.entry(peer.node) {
            Entry::Occupied(entry)
                if entry.get().tls_pubkey == peer.tls_pubkey
                    && entry.get().network_address == peer.network_address =>
            {
                entry.get().clone()
            }
            Entry::Occupied(mut entry) => {
                let pinned = self.build_ring(peer)?;
                entry.insert(pinned.clone());
                pinned
            }
            Entry::Vacant(entry) => entry.insert(self.build_ring(peer)?).clone(),
        };
        Ok((pinned, url))
    }

    fn build_ring(&self, peer: &PeerNode) -> Result<PinnedPeerClient, ApiError> {
        let mut ring = Vec::with_capacity(SLICE_CONNECTIONS);
        let mut in_flight = Vec::with_capacity(SLICE_CONNECTIONS);
        for _ in 0..SLICE_CONNECTIONS {
            ring.push(self.build_pinned_client(peer.tls_pubkey)?);
            in_flight.push(AtomicUsize::new(0));
        }
        Ok(PinnedPeerClient {
            ring: Arc::new(ring),
            cursor: Arc::new(AtomicUsize::new(0)),
            in_flight: Arc::new(in_flight),
            tls_pubkey: peer.tls_pubkey,
            network_address: peer.network_address,
        })
    }

    fn build_pinned_client(&self, tls_pubkey: NetworkTlsPubkey) -> Result<reqwest::Client, ApiError> {
        let builder = reqwest::Client::builder()
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout)
            .http2_initial_stream_window_size(H2_STREAM_WINDOW)
            .http2_initial_connection_window_size(H2_CONNECTION_WINDOW);
        let builder = match &self.local_identity {
            Some(identity) => {
                tape_peer_tls::apply_pinned_tls_with_identity(builder, tls_pubkey, identity.as_ref())
                    .map_err(|e| ApiError::Other(format!("tls build: {e}")))?
            }
            None => tape_peer_tls::apply_pinned_tls(builder, tls_pubkey)
                .map_err(|e| ApiError::Other(format!("tls build: {e}")))?,
        };
        builder
            .build()
            .map_err(|e| ApiError::Other(format!("client build: {e}")))
    }

    fn resolve(&self, node: Address) -> Result<(reqwest::Client, String), ApiError> {
        let peer = self.resolve_peer(node)?;
        let (pinned, url) = self.client_for(&peer)?;
        Ok((pinned.first(), url))
    }

    /// A peer's first idle connection, for one slice push.
    fn lease_slice(&self, node: Address) -> Result<(reqwest::Client, SlotLease, String), ApiError> {
        let peer = self.resolve_peer(node)?;
        let (pinned, url) = self.client_for(&peer)?;
        let (client, lease) = pinned.lease();
        Ok((client, lease, url))
    }

    /// A peer's next connection, for one slice body in either direction.
    fn resolve_slice(&self, node: Address) -> Result<(reqwest::Client, String), ApiError> {
        let peer = self.resolve_peer(node)?;
        let (pinned, url) = self.client_for(&peer)?;
        Ok((pinned.next(), url))
    }

    fn resolve_peer(&self, node: Address) -> Result<PeerNode, ApiError> {
        self.peer_manager
            .get(node)
            .ok_or(ApiError::NodeUnresolved(node))
    }

    fn record(&self, node: Address, op: &'static str, resp: &reqwest::Response, start: Instant, bytes_sent: u64) {
        if let Some(m) = &self.metrics {
            let duration = start.elapsed().as_secs_f64();
            let status = resp.status().as_u16().to_string();
            m.record_request(op, &status, duration);
            if bytes_sent > 0 {
                m.record_bytes_sent(op, bytes_sent);
            }
        }
        if bytes_sent > 0 && resp.status().is_success() {
            if let Some(sink) = &self.transfer_sink {
                sink(PeerTransfer { node, op, sent: true, bytes: bytes_sent });
            }
        }
    }

    fn record_rx(&self, node: Address, op: &'static str, bytes: u64) {
        if let Some(m) = &self.metrics {
            m.record_bytes_received(op, bytes);
        }
        if bytes > 0 {
            if let Some(sink) = &self.transfer_sink {
                sink(PeerTransfer { node, op, sent: false, bytes });
            }
        }
    }
}

fn https_base_url(addr: NetworkAddress) -> Result<String, ApiError> {
    let authority = addr
        .authority()
        .map_err(|e| ApiError::ConnectionFailed(e.to_string()))?;
    Ok(format!("https://{authority}"))
}

#[async_trait]
impl Api for HttpApi {
    async fn put_slice(&self, node: Address, req: &PutSliceReq) -> Result<PutSliceRes, ApiError> {
        let (client, _lease, base) = self.lease_slice(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", slice_url(&track_id, req.spool));
        let body = req.body.clone();
        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .put(&url)
            .timeout(self.put_slice_timeout)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "put_slice", &resp, start, bytes_sent);

        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "put_slice", bytes.len() as u64);

        let receipt: BlsSignResponse = wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(PutSliceRes {
            receipt: CertifyRes {
                signature: receipt.signature,
                node: receipt.node,
                epoch: receipt.epoch,
            },
        })
    }

    async fn get_slice(&self, node: Address, req: &GetSliceReq) -> Result<GetSliceRes, ApiError> {
        let (client, base) = self.resolve_slice(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", slice_url(&track_id, req.spool));

        let start = Instant::now();
        let resp = stamp_request_scope(client.get(&url).timeout(self.get_slice_timeout))
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "get_slice", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "get_slice", bytes.len() as u64);
        Ok(GetSliceRes { data: bytes })
    }

    async fn get_track(&self, node: Address, req: &GetTrackReq) -> Result<GetTrackRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", track_url(&track_id));

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "get_track", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "get_track", bytes.len() as u64);
        let wire: TrackResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(GetTrackRes {
            track: CompressedTrack::unpack(wire.track),
        })
    }

    async fn get_track_by_number(
        &self,
        node: Address,
        req: &GetTrackByNumberReq,
    ) -> Result<GetTrackByNumberRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let tape_id = req.tape.to_string();
        let url = format!("{base}{}", tape_track_url(&tape_id, req.track_number));

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "get_track_by_number", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "get_track_by_number", bytes.len() as u64);
        let wire: TrackResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(GetTrackByNumberRes {
            track: CompressedTrack::unpack(wire.track),
        })
    }

    async fn find_track(&self, node: Address, req: &FindTrackReq) -> Result<FindTrackRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let tape_id = req.tape.to_string();
        let url = format!("{base}{}", find_track_url(&tape_id));
        let wire_req = FindTrackRequest {
            key: req.key,
            version: req.version.clone(),
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "find_track", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "find_track", bytes.len() as u64);
        let wire: TrackResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;
        Ok(FindTrackRes {
            track: CompressedTrack::unpack(wire.track),
        })
    }

    async fn list_tracks_by_tape(
        &self,
        node: Address,
        req: &ListTracksByTapeReq,
    ) -> Result<ListTracksByTapeRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let tape_id = req.tape.to_string();
        let url = format!("{base}{}", list_tracks_by_tape_url(&tape_id));
        let wire_req = ListTracksByTapeRequest {
            cursor: req.cursor,
            limit: req.limit,
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "list_tracks_by_tape", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "list_tracks_by_tape", bytes.len() as u64);
        let wire: ListTracksByTapeResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(ListTracksByTapeRes {
            tracks: wire
                .tracks
                .into_iter()
                .map(CompressedTrack::unpack)
                .collect(),
            next_cursor: wire.next_cursor,
        })
    }

    async fn list_objects(
        &self,
        node: Address,
        req: &ListObjectsReq,
    ) -> Result<ListObjectsRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let tape_id = req.bucket.to_string();
        let url = format!("{base}{}", list_objects_url(&tape_id));
        let wire_req = ListObjectsRequest {
            prefix: req.prefix.clone(),
            delimiter: req.delimiter.clone(),
            cursor: req.cursor.clone(),
            limit: req.limit,
        };
        let body = wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "list_objects", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "list_objects", bytes.len() as u64);
        let wire: ListObjectsResponse =
            wincode::deserialize(&bytes).map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(ListObjectsRes {
            objects: wire.objects,
            common_prefixes: wire.common_prefixes,
            next_cursor: wire.next_cursor,
            is_truncated: wire.is_truncated,
        })
    }

    async fn get_track_data(
        &self,
        node: Address,
        req: &GetTrackDataReq,
    ) -> Result<GetTrackDataRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", track_data_url(&track_id));

        let start = Instant::now();
        let resp = stamp_request_scope(client.get(&url))
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "get_track_data", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "get_track_data", bytes.len() as u64);
        let wire: TrackDataResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(GetTrackDataRes { data: wire.data })
    }

    async fn get_track_proof(
        &self,
        node: Address,
        req: &GetTrackProofReq,
    ) -> Result<GetTrackProofRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", track_proof_url(&track_id));

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "get_track_proof", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "get_track_proof", bytes.len() as u64);
        let wire: TrackProofResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(GetTrackProofRes {
            proof: CompressedTrackProof::unpack(wire.proof),
        })
    }

    async fn sync_slices(
        &self,
        node: Address,
        req: &SyncSlicesReq,
    ) -> Result<SyncSlicesRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{}", SYNC_SLICES_PATH);
        let wire_req = SyncSlicesRequest {
            spool_index: req.spool_index,
            cursor: req.cursor.clone(),
            limit: req.limit,
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "sync_slices", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "sync_slices", bytes.len() as u64);
        let wire_res: SyncSlicesResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(SyncSlicesRes {
            entries: wire_res.entries,
            next_cursor: wire_res.next_cursor,
        })
    }

    async fn repair(&self, node: Address, req: &RepairReq) -> Result<RepairRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", repair_url(&track_id));
        let wire_req = RepairRequest {
            helper_spool: req.helper_spool,
            stripes: req.stripes.clone(),
        };

        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "repair", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "repair", bytes.len() as u64);
        Ok(RepairRes {
            data: bytes.to_vec(),
        })
    }

    async fn certify(&self, node: Address, req: &CertifyReq) -> Result<CertifyRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", sign_url(&track_id));

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "certify", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "certify", bytes.len() as u64);
        let wire: BlsSignResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(CertifyRes {
            signature: wire.signature,
            node: wire.node,
            epoch: wire.epoch,
        })
    }

    async fn vote(&self, node: Address, req: &VoteReq) -> Result<VoteRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{VOTE_PATH}");
        let wire_req = VoteRequest {
            signer: req.signer,
            candidate: req.candidate,
            group: req.group,
            signature: req.signature,
        };
        let body = wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .timeout(VOTE_TIMEOUT)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "vote", &resp, start, bytes_sent);
        check_status(resp).await?;
        Ok(VoteRes)
    }

    async fn invalidate(
        &self,
        node: Address,
        req: &InvalidateReq,
    ) -> Result<InvalidateRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let track_id = req.track.to_string();
        let url = format!("{base}{}", inconsistency_url(&track_id));
        let wire_req = InconsistencyRequest {
            proof: req.proof.clone(),
        };
        let body =
            wincode::serialize(&wire_req)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "invalidate", &resp, start, bytes_sent);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "invalidate", bytes.len() as u64);
        let wire: BlsInconsistencyResponse =
            wincode::deserialize(&bytes)
            .map_err(|e| ApiError::Serialization(e.to_string()))?;

        Ok(InvalidateRes {
            signature: wire.signature,
            node: wire.node,
            epoch: wire.epoch,
        })
    }

    async fn proof_of_access(
        &self,
        node: Address,
        req: &ProofOfAccessReq,
    ) -> Result<ProofOfAccessRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{CHALLENGE_PROOF_PATH}");
        let wire = ProofOfAccessPayload::from(req.answer.clone());
        let body =
            wincode::serialize(&wire).map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .timeout(VOTE_TIMEOUT)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "proof_of_access", &resp, start, bytes_sent);
        check_status(resp).await?;
        Ok(ProofOfAccessRes)
    }

    async fn attest(&self, node: Address, req: &AttestReq) -> Result<AttestRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{CHALLENGE_ATTEST_PATH}");
        let wire = AttestationPayload {
            epoch: req.epoch,
            group: req.group,
            round: req.round,
            block: req.block,
            signer: req.signer,
            digest: req.digest,
            digest_signature: req.digest_signature,
            attests: req.attests.clone(),
        };
        let body =
            wincode::serialize(&wire).map_err(|e| ApiError::Serialization(e.to_string()))?;

        let bytes_sent = body.len() as u64;
        let start = Instant::now();
        let resp = client
            .post(&url)
            .timeout(VOTE_TIMEOUT)
            .header("content-type", BINARY_CONTENT)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "attest", &resp, start, bytes_sent);
        check_status(resp).await?;
        Ok(AttestRes)
    }

    async fn get_health(
        &self,
        node: Address,
        _req: &GetHealthReq,
    ) -> Result<GetHealthRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{}", NODE_HEALTH_PATH);

        let start = Instant::now();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "get_health", &resp, start, 0);
        Ok(GetHealthRes {
            ok: resp.status().is_success(),
        })
    }

    async fn get_stats(
        &self,
        node: Address,
        _req: &GetStatsReq,
    ) -> Result<GetStatsRes, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{}", NODE_STATS_PATH);

        let start = Instant::now();
        let resp = client
            .get(&url)
            .header("accept", JSON_CONTENT)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "get_stats", &resp, start, 0);
        let resp = check_status(resp).await?;
        let stats = resp
            .json()
            .await
            .map_err(|e| ApiError::Serialization(format!("json: {e}")))?;
        Ok(GetStatsRes { stats })
    }

    async fn get_observe_board(&self, node: Address) -> Result<Vec<u8>, ApiError> {
        let (client, base) = self.resolve(node)?;
        let url = format!("{base}{}", OBSERVE_BOARD_PATH);

        let start = Instant::now();
        let resp = client
            .get(&url)
            .header("accept", JSON_CONTENT)
            .send()
            .await
            .map_err(map_reqwest)?;

        self.record(node, "get_observe_board", &resp, start, 0);
        let resp = check_status(resp).await?;
        let bytes = resp.bytes().await.map_err(map_reqwest)?;
        self.record_rx(node, "get_observe_board", bytes.len() as u64);
        Ok(bytes.to_vec())
    }
}

/// Stamp the in-scope correlation id and share token on an outbound request, so
/// the node's logs tie back to the originating request and a hidden track serves.
/// A no-op outside a request scope (background sync, repair, votes).
fn stamp_request_scope(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let request = match current_request_id() {
        Some(id) => request.header(REQUEST_ID_HEADER, id),
        None => request,
    };

    match current_share_token() {
        Some(token) => request.header(SHARE_TOKEN_HEADER, encode_share_token(&token)),
        None => request,
    }
}

fn map_reqwest(e: reqwest::Error) -> ApiError {
    let msg = error_chain(&e);
    if e.is_timeout() {
        ApiError::Timeout
    } else if e.is_connect() {
        ApiError::ConnectionFailed(msg)
    } else {
        ApiError::Other(msg)
    }
}

fn error_chain(e: &dyn std::error::Error) -> String {
    let mut msg = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        msg.push_str(": ");
        msg.push_str(&cause.to_string());
        source = cause.source();
    }
    msg
}

async fn check_status(resp: reqwest::Response) -> Result<reqwest::Response, ApiError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    match status.as_u16() {
        404 => Err(ApiError::NotFound),
        429 => Err(crate::rate_limited_error(&resp)),
        403 => {
            let body = resp.text().await.unwrap_or_default();
            if body.contains("not responsible") {
                Err(ApiError::NotResponsible)
            } else if body.contains("blacklisted object") {
                Err(ApiError::BlacklistedObject)
            } else if body.contains("not in committee") {
                Err(ApiError::NotInCommittee)
            } else {
                Err(ApiError::ServerError {
                    status: 403,
                    message: body,
                })
            }
        }
        s => {
            let body = resp.text().await.unwrap_or_default();
            Err(ApiError::ServerError {
                status: s,
                message: body,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use std::collections::HashSet;

    use axum::body::Bytes;
    use axum::extract::ConnectInfo;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::Router;
    use axum_server::tls_rustls::RustlsConfig;
    use tape_peer_manager::PeerNode;
    use tape_peer_tls::{build_server_config, install_default_provider};
    use rand::thread_rng;
    use tape_core::bls::{BlsPrivateKey, BlsPubkey};
    use tape_core::spooler::GroupIndex;
    use tape_core::system::VoteKind;
    use tape_core::system::NodePreferences;
    use tape_core::types::coin::TAPE;
    use tape_core::types::{BasisPoints, EpochDuration, EpochNumber, SpoolIndex, StorageUnits};
    use tape_crypto::address::Address;
    use tape_crypto::ed25519::Keypair as EdKeypair;
    use tape_crypto::Hash;
    use tokio::net::TcpListener;

    fn address(byte: u8) -> Address {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Address::new(bytes)
    }

    fn test_preferences() -> NodePreferences {
        NodePreferences {
            storage_capacity: StorageUnits::zero(),
            storage_price: TAPE(0),
            committee_size: 0,
            spool_groups: 0,
            burn_fee_bps: BasisPoints(0),
            subsidy_decay_bps: BasisPoints(0),
            epoch_duration: EpochDuration(0),
            access_threshold: TAPE(0),
        }
    }

    fn make_peer(node: Address, port: u16, tls_pubkey: NetworkTlsPubkey) -> PeerNode {
        PeerNode {
            node,
            bls_pubkey: BlsPubkey::new_unique(),
            tls_pubkey,
            network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], port),
            preferences: test_preferences(),
            stake: TAPE(0),
            name: [0u8; 32],
        }
    }

    fn pubkey_of(kp: &EdKeypair) -> NetworkTlsPubkey {
        NetworkTlsPubkey::new(kp.pubkey().to_bytes())
    }

    fn candidate(kind: VoteKind) -> VoteCandidate {
        VoteCandidate {
            kind,
            voting_epoch: EpochNumber(11),
            target_epoch: EpochNumber(10),
            hash: Hash::from([0xAB; 32]),
        }
    }

    async fn serve_tls(
        tls_keypair: EdKeypair,
        router: Router,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let addr = std_listener.local_addr().unwrap();

        let server_config =
            build_server_config(&tls_keypair, &[IpAddr::V4(Ipv4Addr::LOCALHOST)]).expect("cfg");
        let rustls = RustlsConfig::from_config(server_config);

        let handle = tokio::spawn(async move {
            axum_server::from_tcp_rustls(std_listener, rustls)
                .serve(router.into_make_service())
                .await
                .unwrap();
        });
        (addr, handle)
    }

    #[test]
    fn resolves_peers_added_after_api_construction() {
        install_default_provider();
        let peer_manager = Arc::new(PeerManager::new());
        let api = HttpApi::with_default_timeouts(peer_manager.clone());
        let node = address(7);

        assert!(matches!(
            api.resolve_peer(node),
            Err(ApiError::NodeUnresolved(unresolved)) if unresolved == node
        ));

        peer_manager.add_peer(make_peer(node, 8080, NetworkTlsPubkey::new_unique()));
        let peer = api.resolve_peer(node).expect("resolve");
        assert_eq!(peer.node, node);
    }

    #[test]
    fn default_timeout_builder_constructs_http_api() {
        install_default_provider();
        let peer_manager = Arc::new(PeerManager::new());
        let api = HttpApi::with_default_timeouts(peer_manager.clone());
        assert!(Arc::ptr_eq(&api.peer_manager, &peer_manager));
    }

    #[test]
    fn https_base_url_uses_address_authority() {
        let ipv4 = NetworkAddress::new_ipv4([127, 0, 0, 1], 3430);
        let domain = NetworkAddress::new_domain("node07.devnet.tape.network", 3430)
            .expect("domain");

        assert_eq!(
            https_base_url(ipv4).expect("url"),
            "https://127.0.0.1:3430"
        );
        assert_eq!(
            https_base_url(domain).expect("url"),
            "https://node07.devnet.tape.network:3430"
        );
    }

    #[tokio::test]
    async fn vote_snapshot_roundtrip_over_tls() {
        install_default_provider();
        let mut rng = thread_rng();
        let tls = EdKeypair::new(&mut rng);
        let tls_pubkey = pubkey_of(&tls);
        let target = address(7);
        let signer = address(9);

        let request = VoteRequest {
            signer,
            candidate: candidate(VoteKind::Snapshot),
            group: GroupIndex(4),
            signature: BlsPrivateKey::from_random()
                .sign(b"snapshot")
                .unwrap(),
        };
        let api_request = VoteReq {
            signer: request.signer,
            candidate: request.candidate,
            group: request.group,
            signature: request.signature,
        };

        let expected_request = Arc::new(request.clone());
        let router = Router::new().route(
            VOTE_PATH,
            post({
                let expected_request = Arc::clone(&expected_request);
                move |body: Bytes| {
                    let expected_request = Arc::clone(&expected_request);
                    async move {
                        let decoded: VoteRequest = wincode::deserialize(&body).unwrap();
                        assert_eq!(decoded, *expected_request);
                        StatusCode::OK
                    }
                }
            }),
        );

        let (addr, _handle) = serve_tls(tls, router).await;

        let peer_manager = Arc::new(PeerManager::new());
        peer_manager.add_peer(make_peer(target, addr.port(), tls_pubkey));
        let api = HttpApi::with_default_timeouts(peer_manager);

        api.vote(target, &api_request).await.unwrap();
    }

    #[tokio::test]
    async fn vote_assignment_roundtrip_over_tls() {
        install_default_provider();
        let mut rng = thread_rng();
        let tls = EdKeypair::new(&mut rng);
        let tls_pubkey = pubkey_of(&tls);
        let target = address(7);
        let signer = address(9);

        let request = VoteRequest {
            signer,
            candidate: candidate(VoteKind::Assignment),
            group: GroupIndex(8),
            signature: BlsPrivateKey::from_random()
                .sign(b"assignment")
                .unwrap(),
        };
        let api_request = VoteReq {
            signer: request.signer,
            candidate: request.candidate,
            group: request.group,
            signature: request.signature,
        };

        let expected_request = Arc::new(request.clone());
        let router = Router::new().route(
            VOTE_PATH,
            post({
                let expected_request = Arc::clone(&expected_request);
                move |body: Bytes| {
                    let expected_request = Arc::clone(&expected_request);
                    async move {
                        let decoded: VoteRequest = wincode::deserialize(&body).unwrap();
                        assert_eq!(decoded, *expected_request);
                        StatusCode::OK
                    }
                }
            }),
        );

        let (addr, _handle) = serve_tls(tls, router).await;

        let peer_manager = Arc::new(PeerManager::new());
        peer_manager.add_peer(make_peer(target, addr.port(), tls_pubkey));
        let api = HttpApi::with_default_timeouts(peer_manager);

        api.vote(target, &api_request).await.unwrap();
    }

    #[tokio::test]
    async fn rebuilds_client_when_peer_rotates_tls_key() {
        install_default_provider();
        let mut rng = thread_rng();
        let original_tls = EdKeypair::new(&mut rng);
        let new_tls = EdKeypair::new(&mut rng);

        let router = Router::new().route(
            VOTE_PATH,
            post(|_: Bytes| async move { StatusCode::OK }),
        );

        let (addr, _handle) = serve_tls(original_tls, router).await;
        let target = address(7);
        let signer = address(9);

        let peer_manager = Arc::new(PeerManager::new());
        peer_manager.add_peer(make_peer(
            target,
            addr.port(),
            NetworkTlsPubkey::new_unique(),
        ));
        let api = HttpApi::with_default_timeouts(peer_manager.clone());

        // Snapshot the client with a wrong pin — request should fail.
        let request = VoteReq {
            signer,
            candidate: candidate(VoteKind::Snapshot),
            group: GroupIndex(4),
            signature: BlsPrivateKey::from_random().sign(b"x").unwrap(),
        };
        assert!(api.vote(target, &request).await.is_err());

        // Rotate the peer's tls_pubkey to an also-wrong key; cache must
        // reflect the rotation (still mismatched, but the cached entry is
        // fresh).
        peer_manager.add_peer(make_peer(target, addr.port(), pubkey_of(&new_tls)));
        let peer = peer_manager.get(target).unwrap();
        api.client_for(&peer).expect("build fresh client");
        let cached = api.clients.get(&target).unwrap();
        assert_eq!(cached.tls_pubkey, pubkey_of(&new_tls));
    }

    /// Answer every slice push with a receipt and record the client port it came from
    async fn serve_receipts(tls_keypair: EdKeypair) -> (SocketAddr, Arc<std::sync::Mutex<HashSet<u16>>>) {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let addr = std_listener.local_addr().unwrap();
        let server_config =
            build_server_config(&tls_keypair, &[IpAddr::V4(Ipv4Addr::LOCALHOST)]).expect("cfg");
        let rustls = RustlsConfig::from_config(server_config);
        let ports = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let receipt = wincode::serialize(&BlsSignResponse {
            signature: BlsPrivateKey::from_random().sign(b"receipt").unwrap(),
            node: address(7),
            epoch: EpochNumber(1),
        })
        .unwrap();
        let router = Router::new().fallback({
            let ports = ports.clone();
            move |ConnectInfo(peer): ConnectInfo<SocketAddr>, _: Bytes| {
                let ports = ports.clone();
                let receipt = receipt.clone();
                async move {
                    ports.lock().unwrap().insert(peer.port());
                    receipt
                }
            }
        });
        tokio::spawn(async move {
            axum_server::from_tcp_rustls(std_listener, rustls)
                .serve(router.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .unwrap();
        });
        (addr, ports)
    }

    // slice pushes one after another reuse one warm connection
    #[tokio::test]
    async fn warm_connection() {
        install_default_provider();
        let tls = EdKeypair::new(&mut thread_rng());
        let tls_pubkey = pubkey_of(&tls);
        let (addr, ports) = serve_receipts(tls).await;
        let target = address(7);
        let peer_manager = Arc::new(PeerManager::new());
        peer_manager.add_peer(make_peer(target, addr.port(), tls_pubkey));
        let api = HttpApi::with_default_timeouts(peer_manager);

        for spool in 0..6u64 {
            let request = PutSliceReq {
                track: address(9),
                spool: SpoolIndex::from(spool),
                body: Bytes::from(vec![1u8; 64]),
            };
            api.put_slice(target, &request).await.expect("push");
        }

        assert_eq!(ports.lock().unwrap().len(), 1);
    }

    // Plain-TcpListener warning suppression — tokio's import can otherwise go
    // unused in test-only builds where only axum_server is used.
    #[allow(dead_code)]
    fn _tcp_listener_ref(_: TcpListener) {}
}
