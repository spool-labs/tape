use anyhow::Result;
use tokio::{signal, task::JoinSet};

pub async fn wait_for_shutdown(mut tasks: JoinSet<Result<()>>) -> Result<()> {
    tokio::select! {
        biased;
        _ = signal::ctrl_c() => {
            log::info!("shutting down…");
            tasks.shutdown().await;
        }

        // or first task that ends with Err/panic
        Some(res) = tasks.join_next() => {
            res??;
        }
    }

    // drain any already-finished handles so JoinSet drops cleanly.
    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            log::warn!("task aborted: {e}");
        }
    }
    Ok(())
}
