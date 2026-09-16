//! Starting every component and shutting them down together

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tokio::net::TcpListener;
use tokio::signal::ctrl_c;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::api::router::{router, ApiState};
use crate::chain::sdk::SdkChain;
use crate::config::secret::read_secret;
use crate::config::settings::Config;
use crate::funder::upkeep::Funder;
use crate::publisher::flush::Publisher;
use crate::records::store::Records;
use crate::service::error::ServiceError;
use crate::service::producer::{run_producer, ProducerIntervals};
use crate::worker::gateway::GatewayClient;
use crate::worker::moderation::ModerationClient;
use crate::worker::runner::Worker;
use crate::worker::tape_rule::TapeRule;

pub async fn serve(config: Config) -> Result<(), ServiceError> {
    let records = Records::open(&config.database)?;
    let salt = read_secret(&config.api.reporter_salt_file)?;
    let api_key = read_secret(&config.moderation.api_key_file)?;
    let chain = Arc::new(SdkChain::try_new(&config.chain)?);
    if records.list_tapes().await?.newest().is_none() {
        warn!("no named blacklist tape yet; decisions wait until `create-rack` finishes");
    }

    let worker = Worker::new(
        records.clone(),
        GatewayClient::try_new(&config.gateway)?,
        ModerationClient::try_new(&config.moderation, api_key)?,
        config.worker.clone(),
        config.moderation.removal_categories.clone(),
        TapeRule::new(&config.tape_rule),
    );
    let publisher = Publisher::new(records.clone(), Arc::clone(&chain), config.publisher.batch_window);
    let funder = Funder::new(records.clone(), chain, config.funder.clone(), config.tapes.clone());
    let routes = router(ApiState::new(records, &config.api, salt), &config.api)?;
    let listener = TcpListener::bind(config.api.listen)
        .await
        .map_err(|source| ServiceError::Bind {
            address: config.api.listen,
            source,
        })?;
    info!(listen = %config.api.listen, "report api listening");

    let cancel = CancellationToken::new();
    let intervals = ProducerIntervals {
        publish: config.publisher.poll_interval,
        upkeep: config.funder.interval,
    };
    let api = spawn_api(listener, routes, cancel.clone());
    let checks = spawn_worker(worker, cancel.clone());
    let producer = spawn_producer(publisher, funder, intervals, cancel.clone());

    tokio::select! {
        // Safe: signal listeners hold no state worth keeping
        signalled = shutdown_signal() => match signalled {
            Ok(()) => info!("shutting down"),
            Err(error) => warn!(%error, "shutting down without signal handling"),
        },
        // Safe: waiting on cancellation holds no state
        () = cancel.cancelled() => warn!("a component stopped; shutting down"),
    }
    cancel.cancel();

    let (api, checks, producer) = tokio::join!(api, checks, producer);
    for (name, joined) in [("report api", api), ("worker", checks), ("producer", producer)] {
        joined.map_err(|error| ServiceError::Task {
            name,
            reason: error.to_string(),
        })?;
    }
    info!("stopped");
    Ok(())
}

fn spawn_api(listener: TcpListener, routes: Router, cancel: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _stop_others = cancel.clone().drop_guard();
        let service = routes.into_make_service_with_connect_info::<SocketAddr>();
        let served = axum::serve(listener, service)
            .with_graceful_shutdown(cancel.cancelled_owned())
            .await;
        if let Err(error) = served {
            warn!(%error, "report api stopped");
        }
    })
}

fn spawn_worker(worker: Worker, cancel: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _stop_others = cancel.clone().drop_guard();
        worker.run(cancel).await;
    })
}

fn spawn_producer(
    publisher: Publisher<SdkChain>,
    funder: Funder<SdkChain>,
    intervals: ProducerIntervals,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _stop_others = cancel.clone().drop_guard();
        run_producer(publisher, funder, intervals, cancel).await;
    })
}

async fn shutdown_signal() -> Result<(), ServiceError> {
    let mut terminate = signal(SignalKind::terminate()).map_err(ServiceError::Signal)?;
    tokio::select! {
        // Safe: signal listeners hold no state worth keeping
        interrupted = ctrl_c() => interrupted.map_err(ServiceError::Signal),
        // Safe: signal listeners hold no state worth keeping
        _ = terminate.recv() => Ok(()),
    }
}
