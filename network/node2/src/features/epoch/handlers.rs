use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::{EpochNumber, NodeId};
use tape_crypto::Pubkey;
use tape_protocol::{Api, fetch::fetch_state};
use tape_retry::retry_if;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::config::EpochManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;

pub struct EpochHandlers<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: EpochManagerConfig,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> EpochHandlers<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: EpochManagerConfig,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            cancel,
        }
    }

    pub async fn handle_advance_epoch(&self, epoch: EpochNumber) -> Result<(), NodeError> {
        let context = self.context.clone();
        let state = retry_if(
            self.config.state_retry.clone(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                async move { fetch_state(&context.rpc).await }
            },
            |error| error.is_retriable() && !error.is_skipped_slot(),
        )
        .await
        .map_err(NodeError::from)?;

        if state.epoch < epoch {
            return Err(NodeError::StateUnavailable {
                expected_epoch: epoch,
            });
        }

        self.context.set_state(state)?;

        if let Err(error) = self.context.refresh_peers().await {
            warn!(error = %error, epoch = epoch.0, "peer refresh failed after epoch advance");
        }

        info!(epoch = epoch.0, "published protocol state");
        Ok(())
    }

    pub async fn handle_sync_epoch(&self, epoch: EpochNumber) -> Result<(), NodeError> {
        debug!(epoch = epoch.0, "received sync epoch");
        Ok(())
    }

    pub async fn handle_advance_pool(&self, node: Pubkey, epoch: EpochNumber) -> Result<(), NodeError> {
        debug!(node = %node, epoch = epoch.0, "received advance pool");
        Ok(())
    }

    pub async fn handle_join_network(&self, node_id: NodeId) -> Result<(), NodeError> {
        debug!(node_id = node_id.0, "received join network");
        Ok(())
    }
}
