use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Mutex;
use tracing::warn;

use crate::observer::Observer;
use crate::orchestrator::Orchestrator;
use crate::view::ProdnetView;

pub type SnapshotHandle = Arc<ArcSwap<ProdnetView>>;

pub async fn run(
    observer: Arc<Observer>,
    orchestrator: Arc<Mutex<Orchestrator>>,
    snapshot: SnapshotHandle,
) {
    loop {
        let node_refs = {
            let orch = orchestrator.lock().await;
            orch.node_refs()
        };

        match observer.snapshot(node_refs).await {
            Ok(view) => snapshot.store(Arc::new(view)),
            Err(error) => warn!(error = %error, "prodnet snapshot refresh failed"),
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
