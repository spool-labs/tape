//! High-level client for the Tapedrive storage network.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};

use rpc_client::{parse_tape_error, Rpc, RpcClient, RpcError};
use tape_api::compute::CERTIFY_TRACK_CU;
use tape_api::errors::TapeError;
use tape_api::helpers::build_authority_with_tokens_ix;
use tape_api::instruction::{
    build_certify_track_ix, build_delete_track_ix, build_destroy_tape_ix, build_merge_tape_ix,
    build_register_track_ix, build_reserve_tape_ix, build_split_tape_by_epoch_ix,
    build_split_tape_by_size_ix,
};
use tape_api::program::tapedrive::track_pda;
use tape_api::program::MEMBER_COUNT;
use tape_api::state::{Tape, Track};
use tape_core::encoding::EncodingProfile;
use tape_core::erasure::{group_start, spool_for_slice, SPOOL_COUNT, SPOOL_GROUP_SIZE};
use tape_core::spooler::{SpoolAssignment, SpoolGroup, SpoolIndex};
use tape_core::system::Committee;
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::{EpochNumber, NodeId, StorageUnits};
use tape_crypto::Hash;
use peer_http::HttpPeerClient;
use tape_peer::PeerClient;
use tape_slicer::{num_stripes, pick_stripe_size};

use crate::certification::CertificationCollector;
use crate::decoder::BlobDecoder;
use crate::downloader::ParallelDownloader;
use crate::encoder::BlobEncoder;
use crate::error::TapedriveError;
use crate::network::Network;
use crate::routing::SliceRouter;
use crate::tape_key::TapeKey;
use crate::uploader::DistributedUploader;

/// Retries for certification when epoch advances between signature collection and submission.
const CERTIFY_RETRIES: usize = 3;

/// Retries when waiting for RPC to propagate a newly confirmed transaction.
const RPC_PROPAGATION_RETRIES: usize = 5;
const RPC_PROPAGATION_DELAY_MS: u64 = 500;

/// High-level client for the Tapedrive storage network.
///
/// Generic over `R: Rpc` (on-chain) and `P: PeerClient` (storage nodes).
///
/// # Example
/// ```rust,ignore
/// let sdk = Tapedrive::new(rpc, &payer);
///
/// // Write data (creates a tape automatically)
/// let (tape_key, track) = sdk.write(key, b"hello world", 4).await?;
/// tape_key.save("my-tape.json")?;
///
/// // Read it back
/// let data = sdk.read(&tape_key.track_address(&key)).await?;
/// ```
pub struct Tapedrive<R: Rpc, P: PeerClient> {
    pub network: Network<R, P>,
    pub payer: Keypair,
}

/// Default constructor using `HttpPeerClient`.
impl<R: Rpc> Tapedrive<R, HttpPeerClient> {
    /// Create a new Tapedrive client.
    ///
    /// Takes an RPC backend and a payer keypair. Uses the default HTTP
    /// peer client for storage node communication.
    pub fn new(rpc: R, payer: &Keypair) -> Self {
        let rpc_client = Arc::new(RpcClient::from_rpc(rpc));
        let peer_client = Arc::new(HttpPeerClient::default());
        Self {
            network: Network::new(rpc_client, peer_client),
            payer: Keypair::try_from(payer.to_bytes().as_ref()).unwrap(),
        }
    }
}

impl<R: Rpc, P: PeerClient> Tapedrive<R, P> {
    /// Create a Tapedrive client from an existing Network.
    ///
    /// Use this when you need a custom peer client (e.g. for testing)
    /// or a pre-bootstrapped network.
    pub fn from_network(network: Network<R, P>, payer: &Keypair) -> Self {
        Self {
            network,
            payer: Keypair::try_from(payer.to_bytes().as_ref()).unwrap(),
        }
    }

    /// Access the underlying RPC client.
    pub fn rpc(&self) -> &RpcClient<R> {
        self.network.rpc()
    }

    // Data operations

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
    ) -> Result<(TapeKey, Track), TapedriveError> {
        let tape_key = TapeKey::generate();
        let capacity = StorageUnits::from_bytes(data.len() as u64);
        let reserve_capacity = capacity + StorageUnits::mb(1);
        self.reserve(&tape_key, reserve_capacity, epochs).await?;
        let track = self.write_track(&tape_key, key, data).await?;
        Ok((tape_key, track))
    }

    /// Read a track's data by address. No key needed — reads are public.
    pub async fn read(&self, track: &Pubkey) -> Result<Vec<u8>, TapedriveError> {
        let on_chain = self.rpc().get_track_by_address(track).await?;
        let spool_group = on_chain.data.spool_group();
        let k = on_chain.data.profile.k() as usize;

        let state = self.network.state();
        let slice_to_node = self.build_slice_to_node_map(spool_group, &state.spools, &state.committee_as_array());

        let downloader = ParallelDownloader::new(*track, slice_to_node, k);
        let slices = downloader.download_enough_slices(self.network.peer_client().as_ref()).await
            .map_err(crate::error::ClientError::Download)?;

        // Convert global spool indices to local for decoder
        let base = group_start(spool_group);
        let local_slices: Vec<(SpoolIndex, Vec<u8>)> = slices
            .into_iter()
            .map(|(global_idx, data)| ((global_idx - base) as SpoolIndex, data))
            .collect();

        let mut decoder = BlobDecoder::new();
        let data = decoder.decode(local_slices)
            .map_err(|e| TapedriveError::Download(crate::error::ClientError::Decoding(e.to_string())))?;

        Ok(data)
    }

    /// Delete a track and free its capacity on the tape.
    pub async fn delete(
        &self,
        tape_key: &TapeKey,
        track_key: Hash,
    ) -> Result<(), TapedriveError> {
        let ix = build_delete_track_ix(self.payer.pubkey(), tape_key.pubkey(), track_key);
        self.rpc()
            .send_instructions_with_signers(&self.payer, vec![ix], &[tape_key.as_keypair()])
            .await?;
        Ok(())
    }

    /// Verify that `data` matches the on-chain commitment for a track.
    pub async fn verify(&self, track: &Pubkey, data: &[u8]) -> Result<bool, TapedriveError> {
        let on_chain = self.rpc().get_track_by_address(track).await?;
        let mut encoder = BlobEncoder::with_profile(on_chain.data.profile);
        let (_, root) = encoder
            .encode_with_root(data.to_vec())
            .map_err(|e| TapedriveError::Encoding(e.to_string()))?;
        let computed: Hash = root.into();
        Ok(computed == on_chain.data.commitment_hash)
    }

    // Tape management

    /// Reserve a new tape (storage allocation).
    pub async fn reserve(
        &self,
        tape_key: &TapeKey,
        capacity: StorageUnits,
        epochs: u64,
    ) -> Result<Tape, TapedriveError> {
        let epoch = self.rpc().get_epoch().await?;
        let archive = self.rpc().get_archive().await?;

        let activation = epoch.id;
        let expiry = EpochNumber(epoch.id.as_u64() + epochs);

        let cost = Coin::<TAPE>::new(
            archive
                .storage_price
                .as_u64()
                .saturating_mul(capacity.to_mb())
                .saturating_mul(epochs),
        );

        let mut ixs =
            build_authority_with_tokens_ix(self.payer.pubkey(), tape_key.pubkey(), cost);
        ixs.push(build_reserve_tape_ix(
            self.payer.pubkey(),
            tape_key.pubkey(),
            capacity,
            activation,
            expiry,
        ));

        self.rpc()
            .send_instructions_with_signers(&self.payer, ixs, &[tape_key.as_keypair()])
            .await?;

        self.rpc()
            .get_tape(&tape_key.pubkey())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Fetch a tape's on-chain state.
    pub async fn get_tape(&self, tape: &Pubkey) -> Result<Tape, TapedriveError> {
        self.rpc()
            .get_tape_by_address(tape)
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Estimate the token cost of reserving a tape.
    pub async fn estimate_cost(
        &self,
        capacity: StorageUnits,
        epochs: u64,
    ) -> Result<Coin<TAPE>, TapedriveError> {
        let archive = self.rpc().get_archive().await?;
        Ok(Coin::<TAPE>::new(
            archive
                .storage_price
                .as_u64()
                .saturating_mul(capacity.to_mb())
                .saturating_mul(epochs),
        ))
    }

    /// Add time to a tape's expiry.
    pub async fn extend_expiry(
        &self,
        tape_key: &TapeKey,
        extra_epochs: u64,
    ) -> Result<Tape, TapedriveError> {
        let tape = self.rpc().get_tape(&tape_key.pubkey()).await?;
        let archive = self.rpc().get_archive().await?;

        let temp = TapeKey::generate();
        let new_expiry = EpochNumber(tape.expiry_epoch.as_u64() + extra_epochs);

        let cost = Coin::<TAPE>::new(
            archive
                .storage_price
                .as_u64()
                .saturating_mul(tape.capacity.to_mb())
                .saturating_mul(extra_epochs),
        );

        let mut ixs =
            build_authority_with_tokens_ix(self.payer.pubkey(), temp.pubkey(), cost);
        ixs.push(build_reserve_tape_ix(
            self.payer.pubkey(),
            temp.pubkey(),
            tape.capacity,
            tape.expiry_epoch,
            new_expiry,
        ));
        ixs.push(build_merge_tape_ix(
            self.payer.pubkey(),
            temp.pubkey(),
            tape_key.pubkey(),
        ));

        self.rpc()
            .send_instructions_with_signers(
                &self.payer,
                ixs,
                &[temp.as_keypair(), tape_key.as_keypair()],
            )
            .await?;

        self.rpc()
            .get_tape(&tape_key.pubkey())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Add storage capacity to a tape.
    pub async fn extend_capacity(
        &self,
        tape_key: &TapeKey,
        extra: StorageUnits,
    ) -> Result<Tape, TapedriveError> {
        let tape = self.rpc().get_tape(&tape_key.pubkey()).await?;
        let archive = self.rpc().get_archive().await?;

        let temp = TapeKey::generate();
        let duration = tape.expiry_epoch.as_u64().saturating_sub(tape.active_epoch.as_u64());

        let cost = Coin::<TAPE>::new(
            archive
                .storage_price
                .as_u64()
                .saturating_mul(extra.to_mb())
                .saturating_mul(duration),
        );

        let mut ixs =
            build_authority_with_tokens_ix(self.payer.pubkey(), temp.pubkey(), cost);
        ixs.push(build_reserve_tape_ix(
            self.payer.pubkey(),
            temp.pubkey(),
            extra,
            tape.active_epoch,
            tape.expiry_epoch,
        ));
        ixs.push(build_merge_tape_ix(
            self.payer.pubkey(),
            temp.pubkey(),
            tape_key.pubkey(),
        ));

        self.rpc()
            .send_instructions_with_signers(
                &self.payer,
                ixs,
                &[temp.as_keypair(), tape_key.as_keypair()],
            )
            .await?;

        self.rpc()
            .get_tape(&tape_key.pubkey())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Split a tape at an epoch boundary.
    pub async fn split_by_time(
        &self,
        source: &TapeKey,
        destination: &TapeKey,
        at_epoch: EpochNumber,
    ) -> Result<(Tape, Tape), TapedriveError> {
        let ix = build_split_tape_by_epoch_ix(
            self.payer.pubkey(),
            source.pubkey(),
            destination.pubkey(),
            at_epoch,
        );
        self.rpc()
            .send_instructions_with_signers(
                &self.payer,
                vec![ix],
                &[source.as_keypair(), destination.as_keypair()],
            )
            .await?;

        let src = self.rpc().get_tape(&source.pubkey()).await?;
        let dst = self.rpc().get_tape(&destination.pubkey()).await?;
        Ok((src, dst))
    }

    /// Split a tape by capacity.
    pub async fn split_by_capacity(
        &self,
        source: &TapeKey,
        destination: &TapeKey,
        keep: StorageUnits,
    ) -> Result<(Tape, Tape), TapedriveError> {
        let ix = build_split_tape_by_size_ix(
            self.payer.pubkey(),
            source.pubkey(),
            destination.pubkey(),
            keep,
        );
        self.rpc()
            .send_instructions_with_signers(
                &self.payer,
                vec![ix],
                &[source.as_keypair(), destination.as_keypair()],
            )
            .await?;

        let src = self.rpc().get_tape(&source.pubkey()).await?;
        let dst = self.rpc().get_tape(&destination.pubkey()).await?;
        Ok((src, dst))
    }

    /// Merge source tape into destination.
    pub async fn merge(
        &self,
        source: &TapeKey,
        destination: &TapeKey,
    ) -> Result<Tape, TapedriveError> {
        let ix = build_merge_tape_ix(
            self.payer.pubkey(),
            source.pubkey(),
            destination.pubkey(),
        );
        self.rpc()
            .send_instructions_with_signers(
                &self.payer,
                vec![ix],
                &[source.as_keypair(), destination.as_keypair()],
            )
            .await?;

        self.rpc()
            .get_tape(&destination.pubkey())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Destroy an empty, expired tape.
    pub async fn destroy(&self, tape_key: &TapeKey) -> Result<(), TapedriveError> {
        let ix = build_destroy_tape_ix(self.payer.pubkey(), tape_key.pubkey());
        self.rpc()
            .send_instructions_with_signers(&self.payer, vec![ix], &[tape_key.as_keypair()])
            .await?;
        Ok(())
    }

    // Track management

    /// Write a track to an existing tape.
    pub async fn write_track(
        &self,
        tape_key: &TapeKey,
        key: Hash,
        data: &[u8],
    ) -> Result<Track, TapedriveError> {
        // 1. Encode blob
        let mut encoder = BlobEncoder::new();
        let (slices, merkle_root, leaves) = encoder
            .encode_with_leaves(data.to_vec())
            .map_err(|e| TapedriveError::Encoding(e.to_string()))?;

        let commitment_hash: Hash = merkle_root.into();
        let root_hash: Hash = merkle_root.into();
        let storage_units = StorageUnits::from_bytes(data.len() as u64);

        // 2. Resume existing track or register on-chain
        let stripe_size = pick_stripe_size(data.len());
        let stripe_count = num_stripes(data.len(), stripe_size);
        let (track_address, _) = track_pda(tape_key.pubkey(), key);
        let on_chain = match self.rpc().get_track_by_address(&track_address).await {
            Ok(track) => track,
            Err(RpcError::AccountNotFound(_)) => {
                let register_ix = build_register_track_ix(
                    self.payer.pubkey(),
                    tape_key.pubkey(),
                    storage_units,
                    root_hash,
                    commitment_hash,
                    key,
                    EncodingProfile::clay_default(),
                    stripe_size as u64,
                    stripe_count as u64,
                    leaves,
                );

                self.rpc()
                    .send_instructions_with_signers(
                        &self.payer,
                        vec![register_ix],
                        &[tape_key.as_keypair()],
                    )
                    .await?;

                self.retry_fetch_track(&track_address).await?
            }
            Err(error) => {
                return Err(TapedriveError::Rpc(error));
            }
        };

        if on_chain.data.is_certified() {
            return Ok(on_chain);
        }

        let spool_group = on_chain.data.spool_group();

        // 3. Bootstrap network if needed, upload slices
        self.network.bootstrap().await
            .map_err(TapedriveError::Network)?;

        let state = self.network.state();
        let committee = state.committee_as_array();
        let router = SliceRouter::new(state.spools, committee.clone());

        let uploader = DistributedUploader::new(
            track_address,
            spool_group,
            slices,
            router,
        ).map_err(TapedriveError::Upload)?;

        uploader.upload_all(self.network.peer_client().as_ref()).await
            .map_err(TapedriveError::Upload)?;

        // 4. Collect BLS signatures and certify (retry on epoch race)
        for attempt in 0..CERTIFY_RETRIES {
            let system = self.rpc().get_system().await?;

            let collector = CertificationCollector::with_defaults();
            let collected = collector
                .collect_signatures(
                    self.network.peer_client().as_ref(),
                    &track_address,
                    spool_group,
                    &system,
                )
                .await
                .map_err(TapedriveError::Certification)?;

            let compute_ix =
                ComputeBudgetInstruction::set_compute_unit_limit(CERTIFY_TRACK_CU);
            let certify_ix = build_certify_track_ix(
                self.payer.pubkey(),
                tape_key.pubkey(),
                key,
                EpochNumber(collected.epoch),
                collected.bitmap,
                collected.aggregated_signature,
            );

            match self
                .rpc()
                .send_instructions_with_signers(
                    &self.payer,
                    vec![compute_ix, certify_ix],
                    &[tape_key.as_keypair()],
                )
                .await
            {
                Ok(_) => break,
                Err(e) => match parse_tape_error(&e) {
                    Some(TapeError::AlreadyCertified) => break,
                    Some(TapeError::BadSignature) if attempt < CERTIFY_RETRIES - 1 => continue,
                    _ => return Err(TapedriveError::Rpc(e)),
                },
            }
        }

        // 5. Return the certified track
        self.rpc()
            .get_track_by_address(&track_address)
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Fetch a track's on-chain state.
    pub async fn get_track(&self, track: &Pubkey) -> Result<Track, TapedriveError> {
        self.rpc()
            .get_track_by_address(track)
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// List all tracks on a tape.
    pub async fn list_tracks(&self, tape: &Pubkey) -> Result<Vec<(Pubkey, Track)>, TapedriveError> {
        self.rpc()
            .get_tracks_by_tape(tape)
            .await
            .map_err(TapedriveError::Rpc)
    }

    // Private helpers

    /// Build a map of slice_index → NodeId for a spool group from protocol state.
    fn build_slice_to_node_map(
        &self,
        spool_group: SpoolGroup,
        spools: &SpoolAssignment<SPOOL_COUNT>,
        committee: &Committee<MEMBER_COUNT>,
    ) -> HashMap<SpoolIndex, NodeId> {
        let router = SliceRouter::new(*spools, committee.clone());
        let mut map = HashMap::new();
        for local_idx in 0..SPOOL_GROUP_SIZE {
            let global_spool = spool_for_slice(spool_group, local_idx);
            if let Ok(node_id) = router.node_id_for_slice(global_spool) {
                map.insert(global_spool, node_id);
            }
        }
        map
    }

    /// Fetch a track by address with retries for RPC propagation delay.
    async fn retry_fetch_track(&self, address: &Pubkey) -> Result<Track, TapedriveError> {
        let mut last_err = None;
        for attempt in 0..RPC_PROPAGATION_RETRIES {
            match self.rpc().get_track_by_address(address).await {
                Ok(track) => return Ok(track),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < RPC_PROPAGATION_RETRIES - 1 {
                        tokio::time::sleep(Duration::from_millis(RPC_PROPAGATION_DELAY_MS))
                        .await;
                    }
                }
            }
        }
        Err(TapedriveError::Rpc(last_err.unwrap_or(
            RpcError::Internal("track not found after registration".into()),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use bytemuck::Zeroable;
    use peer_memory::MemoryPeerClient;
    use rpc_client::RpcClient;
    use rpc_litesvm::LiteSvmRpc;
    use solana_sdk::signature::Keypair;
    use tape_api::program::tapedrive::{self, archive_pda, tape_pda, track_pda};
    use tape_api::state::{Archive, Tape, Track};
    use tape_core::encoding::EncodingProfile;
    use tape_core::spooler::SpoolGroup;
    use tape_core::tape::TrackData;
    use tape_core::types::coin::{Coin, TAPE};
    use tape_core::types::{EpochNumber, StorageUnits};
    use tape_crypto::hash;

    fn setup() -> (LiteSvmRpc, Tapedrive<LiteSvmRpc, MemoryPeerClient>) {
        let rpc = LiteSvmRpc::new();
        let payer = Keypair::new();
        let rpc_client = Arc::new(RpcClient::from_rpc(rpc.clone()));
        let peer_client = Arc::new(MemoryPeerClient::noop());
        let network = Network::new(rpc_client, peer_client);
        let tapedrive = Tapedrive::from_network(network, &payer);
        (rpc, tapedrive)
    }

    fn pipe(rpc: &LiteSvmRpc, address: Pubkey, packed: &[u8]) {
        rpc.set_account_data(address, tapedrive::ID, packed)
            .unwrap();
    }

    fn make_tape(authority: Pubkey) -> Tape {
        let mut tape: Tape = Zeroable::zeroed();
        tape.authority = authority;
        tape.capacity = StorageUnits::mb(100);
        tape.used = StorageUnits::mb(10);
        tape.active_epoch = EpochNumber(1);
        tape.expiry_epoch = EpochNumber(10);
        tape
    }

    fn make_track(tape_address: Pubkey, key: Hash) -> Track {
        let mut track: Track = Zeroable::zeroed();
        track.tape = tape_address;
        track.key = key;
        track.size = StorageUnits::mb(5);
        track.data = TrackData::new(EpochNumber(1), Hash::default(), SpoolGroup(0));
        track.data.set_profile(EncodingProfile::clay_default());
        track
    }

    fn make_archive(price: u64) -> Archive {
        let mut archive: Archive = Zeroable::zeroed();
        archive.storage_price = Coin::<TAPE>::new(price);
        archive
    }

    #[tokio::test]
    async fn get_tape() {
        let (rpc, tapedrive) = setup();
        let authority = Keypair::new();
        let (tape_address, _) = tape_pda(authority.pubkey());

        let tape = make_tape(authority.pubkey());
        pipe(&rpc, tape_address, &tape.pack());

        let result = tapedrive.get_tape(&tape_address).await.unwrap();
        assert_eq!(result.capacity, StorageUnits::mb(100));
        assert_eq!(result.used, StorageUnits::mb(10));
        assert_eq!(result.active_epoch, EpochNumber(1));
        assert_eq!(result.expiry_epoch, EpochNumber(10));
    }

    #[tokio::test]
    async fn get_track() {
        let (rpc, tapedrive) = setup();
        let authority = Keypair::new();
        let key = Hash::default();
        let (tape_address, _) = tape_pda(authority.pubkey());
        let (track_address, _) = track_pda(authority.pubkey(), key);

        let track = make_track(tape_address, key);
        pipe(&rpc, track_address, &track.pack());

        let result = tapedrive.get_track(&track_address).await.unwrap();
        assert_eq!(result.tape, tape_address);
        assert_eq!(result.key, key);
        assert_eq!(result.size, StorageUnits::mb(5));
    }

    #[tokio::test]
    async fn list_tracks() {
        let (rpc, tapedrive) = setup();

        let authority_a = Keypair::new();
        let authority_b = Keypair::new();
        let (tape_a, _) = tape_pda(authority_a.pubkey());
        let (tape_b, _) = tape_pda(authority_b.pubkey());

        let key1 = hash::hash(b"track1");
        let key2 = hash::hash(b"track2");
        let key3 = hash::hash(b"track3");

        let (addr1, _) = track_pda(authority_a.pubkey(), key1);
        let (addr2, _) = track_pda(authority_a.pubkey(), key2);
        let (addr3, _) = track_pda(authority_b.pubkey(), key3);

        pipe(&rpc, addr1, &make_track(tape_a, key1).pack());
        pipe(&rpc, addr2, &make_track(tape_a, key2).pack());
        pipe(&rpc, addr3, &make_track(tape_b, key3).pack());

        let tracks_a = tapedrive.list_tracks(&tape_a).await.unwrap();
        assert_eq!(tracks_a.len(), 2);
        for (_, track) in &tracks_a {
            assert_eq!(track.tape, tape_a);
        }

        let tracks_b = tapedrive.list_tracks(&tape_b).await.unwrap();
        assert_eq!(tracks_b.len(), 1);
        assert_eq!(tracks_b[0].1.tape, tape_b);
    }

    #[tokio::test]
    async fn estimate_cost() {
        let (rpc, tapedrive) = setup();
        let (archive_address, _) = archive_pda();

        pipe(&rpc, archive_address, &make_archive(100).pack());

        let cost = tapedrive.estimate_cost(StorageUnits::mb(50), 4).await.unwrap();
        assert_eq!(cost.as_u64(), 20_000);
    }

    #[tokio::test]
    async fn verify_match() {
        let (rpc, tapedrive) = setup();
        let authority = Keypair::new();
        let key = hash::hash(b"test-track");
        let (tape_address, _) = tape_pda(authority.pubkey());
        let (track_address, _) = track_pda(authority.pubkey(), key);

        let data = b"hello world, this is test data for verify".to_vec();
        let profile = EncodingProfile::clay_default();

        let mut encoder = BlobEncoder::with_profile(profile);
        let (_, root) = encoder.encode_with_root(data.clone()).unwrap();

        let mut track = make_track(tape_address, key);
        track.data.commitment_hash = root;
        track.data.set_profile(profile);
        pipe(&rpc, track_address, &track.pack());

        assert!(tapedrive.verify(&track_address, &data).await.unwrap());
    }

    #[tokio::test]
    async fn verify_mismatch() {
        let (rpc, tapedrive) = setup();
        let authority = Keypair::new();
        let key = hash::hash(b"test-track");
        let (tape_address, _) = tape_pda(authority.pubkey());
        let (track_address, _) = track_pda(authority.pubkey(), key);

        let data = b"hello world, this is test data for verify".to_vec();
        let profile = EncodingProfile::clay_default();

        let mut encoder = BlobEncoder::with_profile(profile);
        let (_, root) = encoder.encode_with_root(data.clone()).unwrap();

        let mut track = make_track(tape_address, key);
        track.data.commitment_hash = root;
        track.data.set_profile(profile);
        pipe(&rpc, track_address, &track.pack());

        let wrong_data = b"this is different data entirely".to_vec();
        assert!(!tapedrive.verify(&track_address, &wrong_data).await.unwrap());
    }

    #[tokio::test]
    async fn tape_not_found() {
        let (_rpc, tapedrive) = setup();
        let address = Pubkey::new_unique();
        assert!(tapedrive.get_tape(&address).await.is_err());
    }

    #[tokio::test]
    async fn track_not_found() {
        let (_rpc, tapedrive) = setup();
        let address = Pubkey::new_unique();
        assert!(tapedrive.get_track(&address).await.is_err());
    }
}
