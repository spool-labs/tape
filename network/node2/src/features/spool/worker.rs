use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::features::spool::types::{TaskKind, WorkerDone};
use crate::features::spool::{recover, repair, scan, sync};

/// Run a single spool worker to completion.
///
/// The manager spawns this into a JoinSet. The returned WorkerDone
/// carries enough context for the manager to apply the FSM transition.
pub async fn run<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: SpoolManagerConfig,
    kind: TaskKind,
    spool: SpoolIndex,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> WorkerDone
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    info!(spool, epoch = epoch.0, task = ?kind, "spool worker started");

    let done = match kind {
        TaskKind::Sync => {
            let result = sync::run(context, &config, spool, &cancel).await;
            WorkerDone::Sync(spool, epoch, result)
        }
        TaskKind::Scan => {
            let result = scan::run(context, &config, spool, &cancel).await;
            WorkerDone::Scan(spool, epoch, result)
        }
        TaskKind::Repair => {
            let result = repair::run(context, &config, spool, &cancel).await;
            WorkerDone::Repair(spool, epoch, result)
        }
        TaskKind::Recover => {
            let result = recover::run(context, &config, spool, &cancel).await;
            WorkerDone::Recover(spool, epoch, result)
        }
    };

    info!(spool, epoch = epoch.0, task = ?kind, ?done, "spool worker completed");

    done
}
