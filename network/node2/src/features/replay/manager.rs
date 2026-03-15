use mpsc::Receiver;

pub struct ReplayManager {
    context: AppContext,
    config: ReplayConfig,
    rx: Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl ReplayManager {
    pub fn new(
        context: AppContext,
        config: ReplayConfig,
        rx: Receiver<Arc<ParsedBlock>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            rx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::ReplayManager })
                        };
                    };

                    self.persist_block(block).await;
                }
            }
        }
    }

    async fn persist_block(&self, block: Arc<ParsedBlock>) {

        debug!(
            height = block.height.0,
            entries = block.extracted.len(),
            "replay state persisted"
        );
    }
}
