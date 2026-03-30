use anyhow::{Context, Result};
use peer_http::HttpApi;
use rpc_litesvm::LiteSvmRpc;
use tape_api::program::tapedrive::track_pda;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::keypair::Keypair;
use tape_core::erasure::{SPOOL_GROUP_SIZE, spool_for_slice};
use tape_core::spooler::SpoolGroup;
use tape_core::track::types::CompressedTrack;
use tape_sdk::{TapeKey, Tapedrive};
use tape_store::ops::{SliceOps, SpoolOps};
use tape_store::types::Pubkey as StorePubkey;

use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    /// Create an SDK client backed by the simnet chain using an arbitrary keypair.
    pub fn sdk(&self, keypair: &Keypair) -> Tapedrive<LiteSvmRpc, HttpApi> {
        let rpc = self.harness.chain().rpc().clone();
        Tapedrive::new(rpc, keypair)
    }

    /// Upload a blob: reserve tape, register track, upload slices, certify.
    pub async fn upload(
        &self,
        keypair: &Keypair,
        key: tape_crypto::Hash,
        data: &[u8],
        epochs: u64,
    ) -> Result<(TapeKey, Pubkey, CompressedTrack)> {
        let sdk = self.sdk(keypair);
        let (tape_key, track) = sdk
            .write(key, data, epochs)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let track_address = track_pda(track.tape, track.track_number).0;
        Ok((tape_key, track_address, track))
    }

    /// Download and reconstruct a blob from its track address.
    pub async fn download(&self, keypair: &Keypair, track: &Pubkey) -> Result<Vec<u8>> {
        let sdk = self.sdk(keypair);
        sdk.read(track)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Count slices stored across all nodes for a track's spool group.
    pub fn count_slices(&self, track: &Pubkey, group: SpoolGroup) -> Result<usize> {
        let track_store_key = StorePubkey::new(track.to_bytes());
        let mut count = 0usize;

        for i in 0..SPOOL_GROUP_SIZE {
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
}
