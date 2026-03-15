use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info};

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ServiceName;
use crate::features::spool::types::{SpoolAssignment, SpoolWorkerExit};

pub async fn run_spool_worker<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: SpoolManagerConfig,
    assignment: SpoolAssignment,
    semaphore: Arc<Semaphore>,
) -> Result<SpoolWorkerExit, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let permit = match acquire_slot(semaphore, &assignment.cancel).await? {
        Some(permit) => permit,
        None => {
            return Ok(SpoolWorkerExit {
                spool_id: assignment.spool_id,
            });
        }
    };

    info!(
        spool_id = assignment.spool_id,
        epoch = assignment.epoch.0,
        "spool worker started"
    );

    let result = worker_loop(context, config, assignment.clone(), permit).await;

    if result.is_ok() {
        info!(
            spool_id = assignment.spool_id,
            epoch = assignment.epoch.0,
            "spool worker stopped"
        );
    }
    result
}

async fn acquire_slot(
    semaphore: Arc<Semaphore>,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Option<OwnedSemaphorePermit>, NodeError> {
    tokio::select! {
        _ = cancel.cancelled() => Ok(None),
        permit = semaphore.acquire_owned() => {
            match permit {
                Ok(permit) => Ok(Some(permit)),
                Err(_) => Err(NodeError::UnexpectedServiceExit { 
                    service: ServiceName::SpoolManager 
                }),
            }
        }
    }
}

async fn worker_loop<Db, Cluster, Blockchain>(
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: SpoolManagerConfig,
    assignment: SpoolAssignment,
    _permit: OwnedSemaphorePermit,
) -> Result<SpoolWorkerExit, NodeError>
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let mut interval = tokio::time::interval(config.worker_heartbeat);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = assignment.cancel.cancelled() => {
                return Ok(SpoolWorkerExit { spool_id: assignment.spool_id });
            }
            _ = interval.tick() => {
                debug!(
                    node_id = context.node_id().0,
                    spool_id = assignment.spool_id,
                    epoch = assignment.epoch.0,
                    "spool heartbeat"
                );
            }
        }
    }
}
