use std::future::Future;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

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
        self.services.spawn(async move { (service, future.await) });
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
