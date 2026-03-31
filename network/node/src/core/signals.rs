use crate::core::error::NodeError;
use crate::core::types::ShutdownSignal;

#[cfg(unix)]
pub async fn wait_for_shutdown_signal() -> Result<ShutdownSignal, NodeError> {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(NodeError::SignalRegistration)?;

    tokio::select! {
        ctrl_c = tokio::signal::ctrl_c() => {
            ctrl_c.map_err(NodeError::Io)?;
            Ok(ShutdownSignal::CtrlC)
        }
        _ = sigterm.recv() => Ok(ShutdownSignal::SigTerm),
    }
}

#[cfg(not(unix))]
pub async fn wait_for_shutdown_signal() -> Result<ShutdownSignal, NodeError> {
    tokio::signal::ctrl_c().await.map_err(NodeError::Io)?;
    Ok(ShutdownSignal::CtrlC)
}
