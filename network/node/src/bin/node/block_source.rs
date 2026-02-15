use std::sync::Arc;

use async_trait::async_trait;
use rpc_client::RpcClient;
use rpc_solana::SolanaRpc;
use solana_transaction_status::UiConfirmedBlock;
use tape_core::types::SlotNumber;
use tape_node::ingestor::BlockSource;

pub struct RpcBlockSource {
    rpc: Arc<RpcClient<SolanaRpc>>,
}

impl RpcBlockSource {
    pub fn new(rpc: Arc<RpcClient<SolanaRpc>>) -> Self {
        Self { rpc }
    }
}

#[async_trait]
impl BlockSource for RpcBlockSource {
    async fn get_slot(&self) -> Result<SlotNumber, anyhow::Error> {
        let slot = self.rpc.get_slot().await?;
        Ok(SlotNumber(slot))
    }

    async fn get_block(
        &self,
        slot: SlotNumber,
    ) -> Result<Option<UiConfirmedBlock>, anyhow::Error> {
        match self.rpc.get_block(slot.0).await {
            Ok(block) => Ok(Some(block)),
            Err(e) if e.to_string().contains("skipped") => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
