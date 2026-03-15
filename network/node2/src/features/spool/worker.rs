use std::sync::Arc;

use tokio::sync::OwnedSemaphorePermit;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info};


pub async fn run_spool_worker(
    context: AppContext,
    config: SpoolManagerConfig,
    assignment: SpoolAssignment,
    semaphore: Arc<tokio::sync::Semaphore>,
) -> Result<SpoolWorkerExit, NodeError> {
    let permit = match acquire_slot(semaphore, &assignment.cancel).await? {
        Some(permit) => permit,
        None => {
            return Ok(SpoolWorkerExit {
                spool_id: assignment.spool_id,
            });
        }
    };

    info!(
        spool_id = assignment.spool_id.0,
        epoch = assignment.epoch.0,
        "spool worker started"
    );

    let result = worker_loop(context, config, assignment.clone(), permit).await;

    if result.is_ok() {
        info!(
            spool_id = assignment.spool_id.0,
            epoch = assignment.epoch.0,
            "spool worker stopped"
        );
    }
    result
}

async fn acquire_slot(
    semaphore: Arc<tokio::sync::Semaphore>,
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

async fn worker_loop(
    context: AppContext,
    config: SpoolManagerConfig,
    assignment: SpoolAssignment,
    _permit: OwnedSemaphorePermit,
) -> Result<SpoolWorkerExit, NodeError> {
    let mut interval = tokio::time::interval(config.worker_heartbeat);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);


    loop {
        tokio::select! {
            _ = assignment.cancel.cancelled() => {
                return Ok(SpoolWorkerExit { spool_id: assignment.spool_id });
            }
            _ = interval.tick() => {

                // <todo>

                debug!(spool_id = assignment.spool_id.0, "spool heartbeat");
            }
        }
    }
}

