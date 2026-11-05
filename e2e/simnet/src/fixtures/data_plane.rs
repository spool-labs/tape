use std::sync::Arc;

use anyhow::{Context, Result};
use tape_peer_http::HttpApi;
use rpc_litesvm::LiteSvmRpc;
use solana_keypair::Keypair;
use tape_api::program::tapedrive::TrackAddress;
use tape_core::spooler::VirtualGroup;
use tape_core::track::types::CompressedTrack;
use tape_crypto::address::Address;
use tape_crypto::ed25519::Keypair as CryptoKeypair;
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

    /// Create an SDK client that presents a reader identity over mTLS
    pub fn reader_sdk(
        &self,
        identity: Arc<CryptoKeypair>,
    ) -> Result<Tapedrive<LiteSvmRpc, HttpApi>> {
        let rpc = self.harness.chain().rpc().clone();

        Tapedrive::new_with_identity(rpc, identity).context("build reader sdk")
    }

    /// Upload a content-addressed blob: reserve tape, register track, upload slices, certify.
    pub async fn upload(
        &self,
        keypair: &Keypair,
        data: &[u8],
        epochs: u64,
    ) -> Result<(TapeKey, Address, CompressedTrack)> {
        let sdk = self.sdk(keypair);
        let tape_key = TapeKey::generate();
        let track = sdk
            .write(&tape_key, data, epochs)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let track_address = track.address();
        Ok((tape_key, track_address, track))
    }

    /// Download and reconstruct a blob from its track address.
    pub async fn download(&self, keypair: &Keypair, track: &Address) -> Result<Vec<u8>> {
        let sdk = self.sdk(keypair);
        sdk.read(track)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Count slices stored across all nodes for a track's group.
    pub fn count_slices(&self, track: &Address, group: VirtualGroup) -> Result<usize> {
        let track_store_key = *track;
        let mut count = 0usize;

        for i in 0..usize::from(self.harness.config().profile.n()) {
            let spool_id = group.spool_at(i);

            for node in self.harness.nodes() {
                if !node.is_running() {
                    continue;
                }
                let ctx = node.context();
                let row = ctx
                    .store
                    .get_spool_state(spool_id)
                    .with_context(|| format!("get_spool_state node {}", node.id()))?;

                if row.is_none() {
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

    /// Count slices stored by the canonical current owners of a track's group:
    /// the nodes of the live group that carries it.
    pub async fn count_current_owner_slices(
        &self,
        track: &Address,
        group: VirtualGroup,
    ) -> Result<usize> {
        let system = self.read_system().await?;
        let live_group = system
            .live_group(group)
            .map_err(|_| anyhow::anyhow!("no spool group is live"))?;
        let group_account = self.read_group(system.current_epoch, live_group).await?;
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
