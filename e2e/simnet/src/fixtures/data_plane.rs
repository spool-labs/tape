use anyhow::{Context, Result};
use peer_http::HttpApi;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::signer::keypair::Keypair;
use tape_api::program::tapedrive::track_pda;
use tape_core::erasure::{GROUP_SIZE, spool_for_slice};
use tape_core::spooler::GroupIndex;
use tape_core::track::types::CompressedTrack;
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
use tape_crypto::Hash;
use tape_sdk::keys::tape_key::TapeKey;
use tape_sdk::tapedrive::Tapedrive;
use tape_store::ops::{SliceOps, SpoolOps};

use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    /// Create an SDK client backed by the simnet chain using an arbitrary keypair.
    pub fn sdk(&self, keypair: &Keypair) -> Tapedrive<LiteSvmRpc, HttpApi> {
        let rpc = self.harness.chain().rpc().clone();
        let payer = CryptoKeypair::from_solana_keypair(keypair)
            .expect("convert simnet payer to crypto keypair");
        Tapedrive::new(rpc, payer)
    }

    /// Upload a blob: reserve tape, register track, upload slices, certify.
    pub async fn upload(
        &self,
        keypair: &Keypair,
        key: Hash,
        data: &[u8],
        epochs: u64,
    ) -> Result<(TapeKey, Address, CompressedTrack)> {
        let sdk = self.sdk(keypair);
        let (tape_key, track) = sdk
            .write(key, data, epochs)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let track_address = track_pda(track.tape, track.track_number).0;
        Ok((tape_key, track_address, track))
    }

    /// Download and reconstruct a blob from its track address.
    pub async fn download(&self, keypair: &Keypair, track: &Address) -> Result<Vec<u8>> {
        let sdk = self.sdk(keypair);
        sdk.read(track)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Count slices stored across all nodes for a track's spool group.
    pub fn count_slices(&self, track: &Address, group: GroupIndex) -> Result<usize> {
        let track_store_key = *track;
        let mut count = 0usize;

        for i in 0..GROUP_SIZE {
            let spool_id = spool_for_slice(group, i);

            for node in self.harness.nodes() {
                if !node.is_running() {
                    continue;
                }
                let ctx = node.context();
                let spools = ctx
                    .store
                    .iter_all_spools()
                    .with_context(|| format!("iter_all_spools node {}", node.id()))?;

                let owns_spool = spools.iter().any(|(id, _)| *id == spool_id);
                if !owns_spool {
                    continue;
                }

                if ctx
                    .store
                    .has_slice(spool_id, track_store_key)
                    .with_context(|| {
                        format!("has_slice node {} spool {spool_id}", node.id())
                    })?
                {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Count slices stored by the canonical current owners of a track's group.
    pub async fn count_current_owner_slices(
        &self,
        track: &Address,
        group: GroupIndex,
    ) -> Result<usize> {
        let system = self.read_system().await?;
        let group_account = self.read_group(system.current_epoch, group).await?;
        let mut count = 0usize;

        for (position, spool) in group_account.spools.iter().enumerate() {
            let owner = spool.node;
            if owner == Address::default() {
                continue;
            }

            let Some(node) = self
                .harness
                .nodes()
                .iter()
                .find(|node| {
                    node.is_running() && Address::from(self.node_address(node.id())) == owner
                })
            else {
                continue;
            };

            let spool_id = group.spool_at(position);
            if node
                .context()
                .store
                .has_slice(spool_id, *track)
                .with_context(|| {
                    format!(
                        "has_slice node {} spool {spool_id} current owner",
                        node.id()
                    )
                })?
            {
                count += 1;
            }
        }

        Ok(count)
    }
}
