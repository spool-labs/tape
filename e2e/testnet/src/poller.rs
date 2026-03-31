use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Mutex;
use tracing::warn;

use crate::observer::Observer;
use crate::orchestrator::Orchestrator;
use crate::upload::UploadManager;
use crate::view::TestnetView;

pub type SnapshotHandle = Arc<ArcSwap<TestnetView>>;

pub async fn run(
    observer: Arc<Observer>,
    orchestrator: Arc<Mutex<Orchestrator>>,
    upload_manager: Arc<UploadManager>,
    snapshot: SnapshotHandle,
) {
    loop {
        let node_refs = {
            let orch = orchestrator.lock().await;
            orch.node_refs()
        };

        match observer.snapshot(node_refs).await {
            Ok(mut view) => {
                view.uploads = upload_manager.snapshot();
                snapshot.store(Arc::new(view));
            }
            Err(error) => warn!(error = %error, "testnet snapshot refresh failed"),
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
