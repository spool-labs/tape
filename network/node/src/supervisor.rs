use std::future::Future;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn, Instrument};

use crate::core::error::NodeError;
use crate::core::signals::wait_for_shutdown_signal;
use crate::core::types::ServiceName;

pub struct Supervisor {
    cancel: CancellationToken,
    services: JoinSet<(ServiceName, Result<(), NodeError>)>,
}

impl Supervisor {
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            cancel,
            services: JoinSet::new(),
        }
    }

    pub fn spawn<F>(&mut self, service: ServiceName, future: F)
    where
        F: Future<Output = Result<(), NodeError>> + Send + 'static,
    {
        self.services
            .spawn(async move { (service, future.await) }.in_current_span());
    }

    pub async fn supervise(mut self) -> Result<(), NodeError> {
        let shutdown_signal = wait_for_shutdown_signal();
        tokio::pin!(shutdown_signal);

        let mut first_error = None;
        let mut signal_seen = false;

        while !self.services.is_empty() {
            tokio::select! {
                signal = &mut shutdown_signal, if !signal_seen => {
                    signal_seen = true;
                    match signal {
                        Ok(received) => {
                            info!(signal = ?received, "shutdown signal received");
                            self.cancel.cancel();
                        }
                        Err(error) => {
                            error!(error = %error, "failed to wait for shutdown signal");
                            first_error = first_error.or(Some(error));
                            self.cancel.cancel();
                        }
                    }
                }
                next = self.services.join_next() => {
                    let Some(joined) = next else {
                        continue;
                    };

                    match joined {
                        Ok((service, Ok(()))) => {
                            if self.cancel.is_cancelled() {
                                info!(
                                    service = ?service,
                                    "service stopped: {}",
                                    service.as_str()
                                );
                            } else {
                                warn!(
                                    service = ?service,
                                    "service exited before shutdown: {}",
                                    service.as_str()
                                );
                                self.cancel.cancel();
                                first_error = first_error.or(Some(NodeError::UnexpectedServiceExit { service }));
                            }
                        }
                        Ok((service, Err(error))) => {
                            let shutdown_channel_error = matches!(
                                &error,
                                NodeError::ChannelSend { .. } | NodeError::ChannelClosed { .. }
                            );
                            if self.cancel.is_cancelled() && shutdown_channel_error {
                                info!(
                                    service = ?service,
                                    error = %error,
                                    "service stopped during shutdown: {}",
                                    service.as_str()
                                );
                                continue;
                            }

                            error!(
                                service = ?service,
                                error = %error,
                                "service failed: {}: {}",
                                service.as_str(),
                                error
                            );
                            self.cancel.cancel();
                            first_error = first_error.or(Some(error));
                        }
                        Err(error) => {
                            let service = ServiceName::Unknown;
                            error!(
                                error = %error,
                                "service task join failed: {}: {}",
                                service.as_str(),
                                error
                            );
                            self.cancel.cancel();
                            first_error = first_error.or(Some(NodeError::ServiceJoin { service, source: error }));
                        }
                    }
                }
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;

    use super::Supervisor;
    use crate::core::error::NodeError;
    use crate::core::types::{ChannelName, ServiceName};

    #[tokio::test]
    async fn channel_send_after_cancel_is_graceful() {
        let cancel = CancellationToken::new();
        let mut supervisor = Supervisor::new(cancel.clone());

        cancel.cancel();
        supervisor.spawn(ServiceName::ReplayManager, async {
            Err(NodeError::ChannelSend {
                channel: ChannelName::StoreManager,
            })
        });

        supervisor
            .supervise()
            .await
            .expect("shutdown channel send should be graceful");
    }

    #[tokio::test]
    async fn channel_send_before_cancel_is_fatal() {
        let cancel = CancellationToken::new();
        let mut supervisor = Supervisor::new(cancel);

        supervisor.spawn(ServiceName::ReplayManager, async {
            Err(NodeError::ChannelSend {
                channel: ChannelName::StoreManager,
            })
        });

        let error = supervisor
            .supervise()
            .await
            .expect_err("channel send before shutdown should fail");

        assert!(matches!(
            error,
            NodeError::ChannelSend {
                channel: ChannelName::StoreManager,
            }
        ));
    }

    #[tokio::test]
    async fn non_channel_error_after_cancel_is_fatal() {
        let cancel = CancellationToken::new();
        let mut supervisor = Supervisor::new(cancel.clone());

        cancel.cancel();
        supervisor.spawn(ServiceName::ReplayManager, async {
            Err(NodeError::Store("write failed".into()))
        });

        let error = supervisor
            .supervise()
            .await
            .expect_err("non-channel errors should fail after shutdown");

        assert!(matches!(error, NodeError::Store(message) if message == "write failed"));
    }
}
