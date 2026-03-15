use tokio_util::sync::CancellationToken;
use tracing::info;

pub struct EpochHandlers {
    context: AppContext,
    config: EpochManagerConfig,
    cancel: CancellationToken,
}

impl EpochHandlers {
    pub fn new(context: AppContext, config: EpochManagerConfig, cancel: CancellationToken) -> Self {
        Self {
            context,
            config,
            cancel,
        }
    }

    pub async fn handle_advance_epoch(&self, epoch: EpochId) -> Result<(), NodeError> {
        let context = self.context.clone();
        let state = retry_if(
            self.config.state_retry.clone(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                async move { context.rpc.get_protocol_state(epoch).await }
            },
            NodeError::is_retryable,
        )
        .await?;

        self.context.state.publish(state)?;
        info!(epoch = epoch.0, "published protocol state");
        Ok(())
    }

    pub async fn handle_sync_epoch(&self, epoch: EpochId) -> Result<(), NodeError> {
        info!(epoch = epoch.0, "received sync epoch");
        Ok(())
    }

    pub async fn handle_advance_pool(
        &self,
        spool_id: crate::core::types::SpoolId,
    ) -> Result<(), NodeError> {
        info!(spool_id = spool_id.0, "received advance pool");
        Ok(())
    }

    pub async fn handle_join_network(&self, node_id: NodeId) -> Result<(), NodeError> {
        info!(node_id = node_id.0, "received join network");
        Ok(())
    }
}
