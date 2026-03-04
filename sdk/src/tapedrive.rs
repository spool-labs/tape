//! High-level client for the Tapedrive storage network.

use std::collections::HashMap;
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
use tape_api::state::{System, Tape, Track};
use tape_core::encoding::EncodingProfile;
use tape_core::types::coin::{Coin, TAPE};
use tape_core::types::{EpochNumber, NodeId, StorageUnits};
use tape_crypto::Hash;
use tape_slicer::{num_stripes, pick_stripe_size};

use crate::certification::CertificationCollector;
use crate::client::TapeClient;
use crate::discovery::NetworkState;
use crate::encoder::BlobEncoder;
use crate::error::TapedriveError;
use crate::tape_key::TapeKey;


/// Retries for certification when epoch advances between signature collection and submission.
const CERTIFY_RETRIES: usize = 3;

/// Retries when waiting for RPC to propagate a newly confirmed transaction.
const RPC_PROPAGATION_RETRIES: usize = 5;
const RPC_PROPAGATION_DELAY_MS: u64 = 500;

/// High-level client for the Tapedrive storage network.
///
/// Provides a simple interface for storing and retrieving data. Most users
/// only need [`write`](Tapedrive::write), [`read`](Tapedrive::read),
/// [`delete`](Tapedrive::delete), and [`verify`](Tapedrive::verify).
///
/// For fine-grained control over storage allocations (tapes) and individual
/// blobs (tracks), use the tape and track management methods directly.
///
/// # Example
/// ```rust,ignore
/// let sdk = Tapedrive::new(client, &payer);
///
/// // Write data (creates a tape automatically)
/// let (tape_key, track) = sdk.write(key, b"hello world", 4).await?;
/// tape_key.save("my-tape.json")?;
///
/// // Read it back
/// let data = sdk.read(&tape_key.track_address(&key)).await?;
///
/// // Verify integrity
/// let ok = sdk.verify(&tape_key.track_address(&key), &data).await?;
///
/// // Delete when done
/// sdk.delete(&tape_key, key).await?;
/// ```
pub struct Tapedrive<R: Rpc> {
    pub client: RpcClient<R>,
    pub payer: Keypair,
}

impl<R: Rpc> Tapedrive<R> {
    /// Create a new Tapedrive client.
    ///
    /// # Arguments
    /// * `client` - An RPC client for Solana interactions
    /// * `payer` - The keypair that pays for transactions (must hold TAPE and SOL)
    pub fn new(client: RpcClient<R>, payer: &Keypair) -> Self {
        Self {
            client,
            payer: Keypair::try_from(payer.to_bytes().as_ref()).unwrap(),
        }
    }

    // Data operations

    /// Write data to the network in one call.
    ///
    /// Creates a tape sized to fit `data` exactly, registers a track,
    /// uploads erasure-coded slices to storage nodes, and certifies the
    /// track with BLS signatures.
    ///
    /// Returns the tape key (save it!) and the registered track.
    /// For more control, use [`reserve`](Tapedrive::reserve) +
    /// [`write_track`](Tapedrive::write_track) separately.
    pub async fn write(
        &self,
        key: Hash,
        data: &[u8],
        epochs: u64,
    ) -> Result<(TapeKey, Track), TapedriveError> {
        let tape_key = TapeKey::generate();
        let capacity = StorageUnits::from_bytes(data.len() as u64);
        let reserve_capacity = capacity + StorageUnits::mb(1); // 1 MB headroom
        self.reserve(&tape_key, reserve_capacity, epochs).await?;
        let track = self.write_track(&tape_key, key, data).await?;
        Ok((tape_key, track))
    }

    /// Read a track's data by address. No key needed — reads are public.
    ///
    /// Fetches on-chain track metadata, discovers storage nodes, downloads
    /// enough slices, and decodes the original data.
    pub async fn read(&self, track: &Pubkey) -> Result<Vec<u8>, TapedriveError> {
        let on_chain = self.client.get_track_by_address(track).await?;
        let spool_group = on_chain.data.spool_group();
        let k = on_chain.data.profile.k() as usize;

        let network = self.discover_network().await?;
        let tape_client = Self::build_tape_client(&network);
        let node_track = tape_node_client::Pubkey(track.to_bytes());

        tape_client
            .download_blob(node_track, spool_group, k)
            .await
            .map_err(|e| TapedriveError::Download(e))
    }

    /// Delete a track and free its capacity on the tape.
    pub async fn delete(
        &self,
        tape_key: &TapeKey,
        track_key: Hash,
    ) -> Result<(), TapedriveError> {
        let ix = build_delete_track_ix(self.payer.pubkey(), tape_key.pubkey(), track_key);
        self.client
            .send_instructions_with_signers(&self.payer, vec![ix], &[tape_key.as_keypair()])
            .await?;
        Ok(())
    }

    /// Verify that `data` matches the on-chain commitment for a track.
    ///
    /// Re-encodes the data locally and compares the merkle root against
    /// what was registered on-chain. No storage node interaction needed.
    pub async fn verify(&self, track: &Pubkey, data: &[u8]) -> Result<bool, TapedriveError> {
        let on_chain = self.client.get_track_by_address(track).await?;
        let mut encoder = BlobEncoder::with_profile(on_chain.data.profile);
        let (_, root) = encoder
            .encode_with_root(data.to_vec())
            .map_err(|e| TapedriveError::Encoding(e.to_string()))?;
        let computed: Hash = root.into();
        Ok(computed == on_chain.data.commitment_hash)
    }

    // Tape management

    /// Reserve a new tape (storage allocation).
    ///
    /// The payer must hold enough TAPE tokens to cover `capacity * epochs`.
    /// Use [`estimate_cost`](Tapedrive::estimate_cost) to check the price first.
    pub async fn reserve(
        &self,
        tape_key: &TapeKey,
        capacity: StorageUnits,
        epochs: u64,
    ) -> Result<Tape, TapedriveError> {
        let epoch = self.client.get_epoch().await?;
        let archive = self.client.get_archive().await?;

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

        self.client
            .send_instructions_with_signers(&self.payer, ixs, &[tape_key.as_keypair()])
            .await?;

        self.client
            .get_tape(&tape_key.pubkey())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Fetch a tape's on-chain state.
    pub async fn get_tape(&self, tape: &Pubkey) -> Result<Tape, TapedriveError> {
        self.client
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
        let archive = self.client.get_archive().await?;
        Ok(Coin::<TAPE>::new(
            archive
                .storage_price
                .as_u64()
                .saturating_mul(capacity.to_mb())
                .saturating_mul(epochs),
        ))
    }

    /// Add time to a tape's expiry.
    ///
    /// Internally reserves a temporary tape covering the extension period
    /// and merges it into the existing tape.
    pub async fn extend_expiry(
        &self,
        tape_key: &TapeKey,
        extra_epochs: u64,
    ) -> Result<Tape, TapedriveError> {
        let tape = self.client.get_tape(&tape_key.pubkey()).await?;
        let archive = self.client.get_archive().await?;

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

        self.client
            .send_instructions_with_signers(
                &self.payer,
                ixs,
                &[temp.as_keypair(), tape_key.as_keypair()],
            )
            .await?;

        self.client
            .get_tape(&tape_key.pubkey())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Add storage capacity to a tape.
    ///
    /// Internally reserves a temporary tape with the extra capacity
    /// and merges it into the existing tape.
    pub async fn extend_capacity(
        &self,
        tape_key: &TapeKey,
        extra: StorageUnits,
    ) -> Result<Tape, TapedriveError> {
        let tape = self.client.get_tape(&tape_key.pubkey()).await?;
        let archive = self.client.get_archive().await?;

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

        self.client
            .send_instructions_with_signers(
                &self.payer,
                ixs,
                &[temp.as_keypair(), tape_key.as_keypair()],
            )
            .await?;

        self.client
            .get_tape(&tape_key.pubkey())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Split a tape at an epoch boundary.
    ///
    /// Source keeps everything before `at_epoch`.
    /// Destination gets everything from `at_epoch` onward.
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
        self.client
            .send_instructions_with_signers(
                &self.payer,
                vec![ix],
                &[source.as_keypair(), destination.as_keypair()],
            )
            .await?;

        let src = self.client.get_tape(&source.pubkey()).await?;
        let dst = self.client.get_tape(&destination.pubkey()).await?;
        Ok((src, dst))
    }

    /// Split a tape by capacity.
    ///
    /// Source keeps `keep` storage units. Destination gets the remainder.
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
        self.client
            .send_instructions_with_signers(
                &self.payer,
                vec![ix],
                &[source.as_keypair(), destination.as_keypair()],
            )
            .await?;

        let src = self.client.get_tape(&source.pubkey()).await?;
        let dst = self.client.get_tape(&destination.pubkey()).await?;
        Ok((src, dst))
    }

    /// Merge source tape into destination. Source is closed after merge.
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
        self.client
            .send_instructions_with_signers(
                &self.payer,
                vec![ix],
                &[source.as_keypair(), destination.as_keypair()],
            )
            .await?;

        self.client
            .get_tape(&destination.pubkey())
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Destroy an empty, expired tape. Reclaims rent.
    pub async fn destroy(&self, tape_key: &TapeKey) -> Result<(), TapedriveError> {
        let ix = build_destroy_tape_ix(self.payer.pubkey(), tape_key.pubkey());
        self.client
            .send_instructions_with_signers(&self.payer, vec![ix], &[tape_key.as_keypair()])
            .await?;
        Ok(())
    }

    // Track management

    /// Write a track to an existing tape.
    ///
    /// Registers the track on-chain, encodes the data, uploads slices
    /// to storage nodes, collects BLS signatures, and certifies.
    /// The tape must have enough remaining capacity for the data.
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

        // 2. Resume existing track if one already exists, otherwise register it on-chain.
        let stripe_size = pick_stripe_size(data.len());
        let stripe_count = num_stripes(data.len(), stripe_size);
        let (track_address, _) = track_pda(tape_key.pubkey(), key);
        let on_chain = match self.client.get_track_by_address(&track_address).await {
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

                self.client
                    .send_instructions_with_signers(
                        &self.payer,
                        vec![register_ix],
                        &[tape_key.as_keypair()],
                    )
                    .await?;

                // Fetch on-chain track (retry for RPC propagation)
                self.retry_fetch_track(&track_address).await?
            }
            Err(error) => {
                return Err(TapedriveError::Rpc(error));
            }
        };

        if on_chain.data.is_certified() {
            return Ok(on_chain);
        }

        // 3. Fetch on-chain track (retry for RPC propagation)
        let spool_group = on_chain.data.spool_group();

        // 4. Discover network and upload slices
        let network = self.discover_network().await?;
        let tape_client = Self::build_tape_client(&network);
        let node_track = tape_node_client::Pubkey(track_address.to_bytes());

        tape_client
            .upload_slices(node_track, spool_group, slices)
            .await
            .map_err(TapedriveError::Upload)?;

        // 5+6. Collect BLS signatures and certify (retry on epoch race)
        for attempt in 0..CERTIFY_RETRIES {
            let system = self.client.get_system().await?;
            let node_address_map = self.build_node_address_map(&system).await;

            let collector = CertificationCollector::with_defaults();
            let collected = collector
                .collect_signatures(&track_address, spool_group, &system, &node_address_map)
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
                .client
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

        // 7. Return the certified track
        self.client
            .get_track_by_address(&track_address)
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// Fetch a track's on-chain state.
    pub async fn get_track(&self, track: &Pubkey) -> Result<Track, TapedriveError> {
        self.client
            .get_track_by_address(track)
            .await
            .map_err(TapedriveError::Rpc)
    }

    /// List all tracks on a tape.
    pub async fn list_tracks(&self, tape: &Pubkey) -> Result<Vec<(Pubkey, Track)>, TapedriveError> {
        self.client
            .get_tracks_by_tape(tape)
            .await
            .map_err(TapedriveError::Rpc)
    }

    // Private helpers

    /// Discover network state (committee, spool assignment, node addresses)
    /// using the existing RPC client.
    async fn discover_network(&self) -> Result<NetworkState, TapedriveError> {
        let system = self.client.get_system().await?;
        let mut node_addresses = Vec::new();
        let mut warnings = Vec::new();

        for (member_idx, member) in system.committee.iter().enumerate() {
            match self.client.get_node_by_id(member.id).await {
                Ok((_pubkey, node)) => {
                    node_addresses.push((member_idx, node.metadata.network_address));
                }
                Err(e) => {
                    warnings.push(format!(
                        "Failed to fetch node {} (member {}): {}",
                        member.id, member_idx, e
                    ));
                }
            }
        }

        Ok(NetworkState {
            committee: system.committee,
            spool_assignment: system.spools,
            node_addresses,
            warnings,
        })
    }

    /// Build a TapeClient from discovered network state.
    fn build_tape_client(network: &NetworkState) -> TapeClient {
        TapeClient::builder()
            .committee(network.committee.clone())
            .spool_assignment(network.spool_assignment.clone())
            .node_addresses(network.node_addresses.clone())
            .build()
    }

    /// Build a map of NodeId → HTTP address for certification.
    async fn build_node_address_map(
        &self,
        system: &System,
    ) -> HashMap<NodeId, String> {
        let mut map = HashMap::new();
        for member in system.committee.iter() {
            if let Ok((_, node)) = self.client.get_node_by_id(member.id).await {
                if let Ok(addr) = node.metadata.network_address.to_socket_addr() {
                    map.insert(member.id, format!("http://{}", addr));
                }
            }
        }
        map
    }

    /// Fetch a track by address with retries for RPC propagation delay.
    async fn retry_fetch_track(&self, address: &Pubkey) -> Result<Track, TapedriveError> {
        let mut last_err = None;
        for attempt in 0..RPC_PROPAGATION_RETRIES {
            match self.client.get_track_by_address(address).await {
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

    use bytemuck::Zeroable;
    use rpc_client::RpcClient;
    use rpc_litesvm::LiteSvmRpc;
    use solana_sdk::signature::Keypair;
    use tape_api::program::tapedrive::{self, archive_pda, tape_pda, track_pda};
    use tape_api::state::{Archive, Tape, Track};
    use tape_core::encoding::EncodingProfile;
    use tape_core::tape::TrackData;
    use tape_core::types::coin::{Coin, TAPE};
    use tape_core::types::{EpochNumber, StorageUnits};
    use tape_crypto::hash;

    fn setup() -> (LiteSvmRpc, Tapedrive<LiteSvmRpc>) {
        let rpc = LiteSvmRpc::new();
        let payer = Keypair::new();
        let client = RpcClient::from_rpc(rpc.clone());
        let tapedrive = Tapedrive::new(client, &payer);
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
        track.data = TrackData::new(EpochNumber(1), Hash::default(), 0);
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
        // 100 price * 50 MB * 4 epochs = 20000
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
