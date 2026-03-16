use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ServiceName;
use crate::features::spool::recover;
use crate::features::spool::scan;
use crate::features::spool::sync;
use crate::features::spool::types::{SpoolAssignment, SpoolTaskKind, SpoolTaskSummary, SpoolWorkItem};

pub async fn run_spool_worker<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: SpoolManagerConfig,
    assignment: SpoolAssignment,
    semaphore: Arc<Semaphore>,
) -> Result<(SpoolWorkItem, Option<SpoolTaskSummary>), NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let _permit = match acquire_slot(semaphore, &assignment.cancel).await? {
        Some(permit) => permit,
        None => return Ok((assignment.work, None)),
    };

    info!(
        spool_id = assignment.work.spool_id,
        epoch = assignment.work.epoch.0,
        task = ?assignment.work.kind,
        "spool worker started"
    );

    let result = run_task(
        &context,
        &config, 
        assignment.work,
        &assignment.cancel
    ).await?;

    info!(
        spool_id = assignment.work.spool_id,
        epoch = assignment.work.epoch.0,
        task = ?assignment.work.kind,
        summary = ?result,
        "spool worker completed"
    );

    Ok((assignment.work, Some(result)))
}

async fn acquire_slot(
    semaphore: Arc<Semaphore>,
    cancel: &CancellationToken,
) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, NodeError> {
    tokio::select! {
        _ = cancel.cancelled() => Ok(None),
        permit = semaphore.acquire_owned() => {
            match permit {
                Ok(permit) => Ok(Some(permit)),
                Err(_) => Err(NodeError::UnexpectedServiceExit {
                    service: ServiceName::SpoolManager,
                }),
            }
        }
    }
}

async fn run_task<Db, Cluster, Blockchain>(
    context: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    work: SpoolWorkItem,
    cancel: &CancellationToken,
) -> Result<SpoolTaskSummary, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    match work.kind {
        SpoolTaskKind::Sync => sync::run(context.clone(), config, work, cancel).await,
        SpoolTaskKind::Scan => scan::run(context.clone(), config, work, cancel).await,
        SpoolTaskKind::Recover => recover::run(context.clone(), config, work, cancel).await,
    }
}
