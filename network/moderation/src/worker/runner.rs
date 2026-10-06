//! The check worker: runs due jobs through the gateway and the moderation API

use std::collections::HashSet;
use std::sync::Arc;

use reqwest::StatusCode;
use tape_api::program::tapedrive::TrackAddress;
use tape_crypto::address::Address;
use tape_sdk::stream::manifest::ChunkManifest;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::settings::WorkerConfig;
use crate::observe::state::Observer;
use crate::records::rows::{CheckAttempt, CheckSettlement, Job, JobFailure, Removal};
use crate::records::states::{CheckOutcome, TargetState};
use crate::records::store::Records;
use crate::types::category::Category;
use crate::types::ids::AttemptCount;
use crate::types::target::TargetKind;
use crate::types::timestamp::Timestamp;
use crate::worker::content::{
    image_format, prepare, InputType, PreparedInput, IMAGE_MAX_BYTES, TEXT_READ_BYTES,
};
use crate::worker::error::WorkerError;
use crate::worker::gateway::{ContentRead, GatewayClient, GatewayRead};
use crate::worker::moderation::{ModerationClient, ModerationInput};
use crate::worker::retry::retry_delay;
use crate::worker::tape_rule::TapeRule;
use crate::worker::verdict::{decide, SentInput, Verdict};

/// Larger than any stream manifest a single tape can hold
const MANIFEST_READ_BYTES: u64 = 2 * 1_024 * 1_024;

/// Runs check and listing jobs one at a time
pub struct Worker {
    records: Records,
    gateway: GatewayClient,
    moderation: ModerationClient,
    config: WorkerConfig,
    removal_categories: Vec<Category>,
    tape_rule: TapeRule,
}

/// What running one job did
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobOutcome {
    Checked { outcome: CheckOutcome, decisions: usize },
    Listed { queued: usize },
    Retrying,
    Failed,
}

/// A finished check, not yet committed
struct TrackCheck {
    attempt: CheckAttempt,
    state: TargetState,
    tape: Option<Address>,
    removal: Option<Removal>,
}

/// The tape of a removed track and a stream's chunk tracks
struct RemovalScope {
    tape: Address,
    chunks: Vec<Address>,
}

impl Worker {
    pub fn new(
        records: Records,
        gateway: GatewayClient,
        moderation: ModerationClient,
        config: WorkerConfig,
        removal_categories: Vec<Category>,
        tape_rule: TapeRule,
    ) -> Self {
        Self {
            records,
            gateway,
            moderation,
            config,
            removal_categories,
            tape_rule,
        }
    }

    pub async fn run(&self, cancel: CancellationToken, observer: Arc<Observer>) {
        let mut ticker = interval(self.config.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Safe: waiting on cancellation holds no state
                _ = cancel.cancelled() => return,
                // Safe: a tick holds no state; a dropped one waits for the next
                _ = ticker.tick() => {}
            }
            self.drain(&cancel, &observer).await;
        }
    }

    /// Run the job due soonest; `None` when nothing is due or it was cancelled
    pub async fn run_next(
        &self,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<Option<JobOutcome>, WorkerError> {
        let Some(job) = self.records.next_due_job(now).await? else {
            return Ok(None);
        };
        match job.target.kind {
            TargetKind::Track => self.run_track(job, now, cancel).await,
            TargetKind::Tape => self.run_tape(job, now, cancel).await,
        }
    }

    async fn drain(&self, cancel: &CancellationToken, observer: &Observer) {
        while !cancel.is_cancelled() {
            match self.run_next(Timestamp::now(), cancel).await {
                Ok(Some(outcome)) => {
                    observer.worked(Ok(()));
                    count_verdict(observer, outcome);
                    debug!(?outcome, "job ran");
                }
                Ok(None) => {
                    observer.worked(Ok(()));
                    return;
                }
                Err(error) => {
                    warn!(%error, "job did not run");
                    observer.worked(Err(error.to_string()));
                    return;
                }
            }
        }
    }

    async fn run_track(
        &self,
        job: Job,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<Option<JobOutcome>, WorkerError> {
        // Safe to cancel: a check only reads over HTTP and writes nothing before it settles
        let Some(checked) = cancel.run_until_cancelled(self.check_track(job, now)).await else {
            return Ok(None);
        };
        let check = match checked {
            Ok(check) => check,
            Err(error) => return self.fail(job, error, now).await.map(Some),
        };

        let outcome = check.attempt.outcome;
        let settlement = CheckSettlement {
            job,
            attempt: check.attempt,
            state: check.state,
            tape: check.tape,
            removal: check.removal,
        };
        let settled = self.records.settle_check(settlement, self.tape_rule.clone()).await?;
        if settled.is_tape_unlisted {
            info!(track = %job.target.address, "tape rule unlisted the track's tape");
        }
        Ok(Some(JobOutcome::Checked {
            outcome,
            decisions: settled.decisions.len(),
        }))
    }

    async fn run_tape(
        &self,
        job: Job,
        now: Timestamp,
        cancel: &CancellationToken,
    ) -> Result<Option<JobOutcome>, WorkerError> {
        // Safe to cancel: listing only reads over HTTP and writes nothing before it settles
        let Some(listed) = cancel.run_until_cancelled(self.list_tape(job.target.address)).await else {
            return Ok(None);
        };
        let outcome = match listed {
            Ok(tracks) => JobOutcome::Listed {
                queued: self.records.settle_listing(job, tracks, now).await?,
            },
            Err(error) => self.fail(job, error, now).await?,
        };
        Ok(Some(outcome))
    }

    async fn check_track(&self, job: Job, now: Timestamp) -> Result<TrackCheck, WorkerError> {
        let track = job.target.address;
        let read = match self.read_content(track).await? {
            GatewayRead::Gone => return Ok(gone(job, now)),
            GatewayRead::Content(read) => read,
        };

        let mut attempt = CheckAttempt::new(job.id, track, CheckOutcome::Unsupported, now);
        attempt.content_type = read.content_type.clone();
        attempt.read_bytes = Some(read.bytes.len() as u64);
        attempt.total_bytes = read.total_bytes;
        attempt.is_complete = Some(read.is_complete());

        let (sent, input) = match to_request(prepare(read)) {
            Ok(request) => request,
            Err(reason) => return Ok(unsupported(job, attempt, reason)),
        };
        attempt.input_type = Some(sent.input_type);
        attempt.is_complete = Some(sent.is_complete);

        let response = self.moderation.moderate(input).await?;
        attempt.response_id = response.id.clone();
        attempt.model = response.model.clone();
        attempt.categories = response.category_scores();
        let verdict = decide(&response, &sent, &self.removal_categories);
        self.conclude(job, attempt, verdict).await
    }

    async fn conclude(
        &self,
        job: Job,
        mut attempt: CheckAttempt,
        verdict: Verdict,
    ) -> Result<TrackCheck, WorkerError> {
        let (outcome, state) = match &verdict {
            Verdict::Remove { .. } => (CheckOutcome::Remove, TargetState::Removed),
            Verdict::Clean => (CheckOutcome::Clean, TargetState::Clean),
            Verdict::Unresolved { .. } => (CheckOutcome::Unresolved, TargetState::Unresolved),
        };
        attempt.outcome = outcome;

        let (tape, removal) = match verdict {
            Verdict::Remove { categories } => self.removal(job.target.address, categories, &mut attempt).await?,
            Verdict::Clean => (job.tape, None),
            Verdict::Unresolved { reason } => {
                attempt.detail = Some(reason);
                (job.tape, None)
            }
        };
        Ok(TrackCheck {
            attempt,
            state,
            tape,
            removal,
        })
    }

    async fn removal(
        &self,
        track: Address,
        categories: Vec<Category>,
        attempt: &mut CheckAttempt,
    ) -> Result<(Option<Address>, Option<Removal>), WorkerError> {
        let scope = self.removal_scope(track).await?;
        let names: Vec<&str> = categories.iter().map(|category| category.as_str()).collect();
        attempt.detail = Some(format!("flagged: {}", names.join(", ")));
        let removal = Removal {
            chunks: scope.chunks,
            categories,
        };
        Ok((Some(scope.tape), Some(removal)))
    }

    /// Read a text-sized prefix, then a whole image when that is what it starts
    async fn read_content(&self, track: Address) -> Result<GatewayRead, WorkerError> {
        let read = self.gateway.read_object(track, TEXT_READ_BYTES).await?;
        let GatewayRead::Content(content) = &read else {
            return Ok(read);
        };
        let is_whole_image_readable = image_format(&content.bytes).is_some()
            && !content.is_complete()
            && content.total_bytes.is_some_and(|total| total <= IMAGE_MAX_BYTES);
        match is_whole_image_readable {
            true => self.gateway.read_object(track, IMAGE_MAX_BYTES).await,
            false => Ok(read),
        }
    }

    async fn removal_scope(&self, track: Address) -> Result<RemovalScope, WorkerError> {
        let record = self.gateway.track_record(track).await?;
        let mut chunks = Vec::new();
        if record.size.to_bytes() <= MANIFEST_READ_BYTES {
            if let GatewayRead::Content(read) = self.gateway.read_track(track, MANIFEST_READ_BYTES).await? {
                // A manifest can list any address, so a chunk counts only
                // when the gateway has it on the manifest's tape.
                for chunk in manifest_chunks(&read) {
                    match self.gateway.track_record(chunk).await {
                        Ok(found) if found.tape == record.tape => chunks.push(chunk),
                        Ok(_) => {}
                        Err(WorkerError::GatewayStatus { status, .. })
                            if status == StatusCode::NOT_FOUND || status == StatusCode::GONE => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        Ok(RemovalScope {
            tape: record.tape,
            chunks,
        })
    }

    async fn list_tape(&self, tape: Address) -> Result<Vec<Address>, WorkerError> {
        let listed = self
            .gateway
            .list_tracks(tape, None, self.config.tape_track_limit)
            .await?;
        let mut tracks = Vec::new();
        for track in listed {
            if track.is_certified() {
                tracks.push(track.address());
            }
        }
        Ok(tracks)
    }

    async fn fail(&self, job: Job, error: WorkerError, now: Timestamp) -> Result<JobOutcome, WorkerError> {
        let attempts = AttemptCount(job.attempts.0.saturating_add(1));
        let delay = retry_delay(attempts, self.config.initial_retry_delay, self.config.max_retry_delay);
        let retry_at = (attempts < self.config.max_attempts).then(|| now.after(delay));
        warn!(target = %job.target, %error, attempts = attempts.0, "job attempt failed");

        let failure = JobFailure {
            job,
            error: error.to_string(),
            retry_at,
            failed_at: now,
        };
        self.records.record_job_failure(failure).await?;
        Ok(match retry_at {
            Some(_) => JobOutcome::Retrying,
            None => JobOutcome::Failed,
        })
    }
}

fn count_verdict(observer: &Observer, outcome: JobOutcome) {
    match outcome {
        JobOutcome::Checked { outcome, .. } => observer.checked(outcome),
        JobOutcome::Listed { .. } | JobOutcome::Retrying | JobOutcome::Failed => {}
    }
}

fn to_request(prepared: PreparedInput) -> Result<(SentInput, ModerationInput), String> {
    match prepared {
        PreparedInput::Text { segments, is_complete } => {
            let sent = SentInput {
                input_type: InputType::Text,
                input_count: segments.len(),
                is_complete,
            };
            Ok((sent, ModerationInput::Texts(segments)))
        }
        PreparedInput::Image { data_url } => {
            let sent = SentInput {
                input_type: InputType::Image,
                input_count: 1,
                is_complete: true,
            };
            Ok((sent, ModerationInput::image(data_url)))
        }
        PreparedInput::Unsupported { reason } => Err(reason),
    }
}

/// Addresses of a stream manifest's chunks, each once; empty when the bytes are not a manifest
fn manifest_chunks(read: &ContentRead) -> Vec<Address> {
    if !read.is_complete() {
        return Vec::new();
    }
    let Ok(manifest) = ChunkManifest::from_bytes(&read.bytes) else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut chunks = Vec::new();
    for entry in &manifest.chunks {
        if seen.insert(entry.track) {
            chunks.push(entry.track);
        }
    }
    chunks
}

fn gone(job: Job, now: Timestamp) -> TrackCheck {
    let mut attempt = CheckAttempt::new(job.id, job.target.address, CheckOutcome::Gone, now);
    attempt.detail = Some("the gateway already refuses the track".to_string());
    TrackCheck {
        attempt,
        state: TargetState::Gone,
        tape: job.tape,
        removal: None,
    }
}

fn unsupported(job: Job, mut attempt: CheckAttempt, reason: String) -> TrackCheck {
    attempt.outcome = CheckOutcome::Unsupported;
    attempt.detail = Some(reason);
    TrackCheck {
        attempt,
        state: TargetState::Unresolved,
        tape: job.tape,
        removal: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path as FilePath, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::body::Bytes;
    use axum::extract::{Path, State};
    use axum::http::header::{CONTENT_RANGE, CONTENT_TYPE, RANGE};
    use axum::http::{HeaderMap, HeaderValue, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use axum::Router;
    use rusqlite::Connection;
    use serde_json::{json, Value};
    use tape_core::prelude::{VirtualGroup, StorageUnits, TrackNumber};
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_crypto::hash::hash;
    use tape_crypto::Hash;
    use tape_protocol::api::{
        ListTracksByTapeRequest, ListTracksByTapeResponse, TrackResponse, TAPE_TRACK_LIST_PATH,
        TRACK_PATH,
    };
    use tape_sdk::stream::manifest::{ChunkEntry, MANIFEST_VERSION};
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::config::settings::{GatewayConfig, ModerationConfig, TapeRuleConfig};
    use crate::records::rows::NewReport;
    use crate::types::ids::RemovalCount;
    use crate::types::reason::Reason;
    use crate::types::target::Target;

    const IMAGE_CATEGORIES: [Category; 6] = [
        Category::SelfHarm,
        Category::SelfHarmInstructions,
        Category::SelfHarmIntent,
        Category::Sexual,
        Category::Violence,
        Category::ViolenceGraphic,
    ];

    #[derive(Clone)]
    struct StubContent {
        status: StatusCode,
        content_type: Option<&'static str>,
        bytes: Vec<u8>,
    }

    #[derive(Clone, Default)]
    struct StubGateway {
        objects: Arc<Mutex<HashMap<String, StubContent>>>,
        raw_tracks: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        records: Arc<Mutex<HashMap<String, CompressedTrack>>>,
        listings: Arc<Mutex<HashMap<String, Vec<CompressedTrack>>>>,
    }

    #[derive(Clone)]
    struct StubModeration {
        status: Arc<Mutex<StatusCode>>,
        flagged: Arc<Mutex<Vec<Category>>>,
        has_null_flags: Arc<Mutex<bool>>,
        requests: Arc<Mutex<Vec<Value>>>,
    }

    struct StubServer {
        url: String,
        cancel: CancellationToken,
        handle: JoinHandle<()>,
    }

    struct Harness {
        dir: TempDir,
        records: Records,
        worker: Worker,
        gateway: StubGateway,
        moderation: StubModeration,
        servers: Vec<StubServer>,
    }

    impl StubServer {
        async fn spawn(router: Router) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
            let address = listener.local_addr().expect("stub address");
            let cancel = CancellationToken::new();
            let shutdown = cancel.clone();
            let handle = tokio::spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(shutdown.cancelled_owned())
                    .await
                    .expect("serve stub");
            });
            Self {
                url: format!("http://{address}"),
                cancel,
                handle,
            }
        }

        async fn stop(self) {
            self.cancel.cancel();
            self.handle.await.expect("join stub");
        }
    }

    impl Harness {
        async fn new(config: WorkerConfig, rule: TapeRuleConfig) -> Self {
            let gateway = StubGateway::default();
            let moderation = StubModeration {
                status: Arc::new(Mutex::new(StatusCode::OK)),
                flagged: Arc::new(Mutex::new(Vec::new())),
                has_null_flags: Arc::new(Mutex::new(false)),
                requests: Arc::new(Mutex::new(Vec::new())),
            };
            let gateway_server = StubServer::spawn(gateway_router(gateway.clone())).await;
            let moderation_server = StubServer::spawn(moderation_router(moderation.clone())).await;

            let dir = tempfile::tempdir().expect("temp dir");
            let records = Records::open(&dir.path().join("records.sqlite")).expect("open records");
            let gateway_client = GatewayClient::try_new(&GatewayConfig {
                url: gateway_server.url.clone(),
                request_timeout: Duration::from_secs(5),
            })
            .expect("gateway client");
            let moderation_client = ModerationClient::try_new(
                &ModerationConfig {
                    base_url: moderation_server.url.clone(),
                    model: "omni-moderation-latest".to_string(),
                    api_key_file: PathBuf::new(),
                    request_timeout: Duration::from_secs(5),
                    max_response_bytes: 1_024 * 1_024,
                    removal_categories: Category::ALL.to_vec(),
                },
                "test-key".to_string(),
            )
            .expect("moderation client");

            let worker = Worker::new(
                records.clone(),
                gateway_client,
                moderation_client,
                config,
                Category::ALL.to_vec(),
                TapeRule::new(&rule),
            );
            Self {
                dir,
                records,
                worker,
                gateway,
                moderation,
                servers: vec![gateway_server, moderation_server],
            }
        }

        async fn stop(self) {
            for server in self.servers {
                server.stop().await;
            }
        }

        fn object(&self, track: Address, content_type: Option<&'static str>, bytes: &[u8]) {
            let content = StubContent {
                status: StatusCode::OK,
                content_type,
                bytes: bytes.to_vec(),
            };
            self.gateway.objects.lock().expect("lock").insert(track.to_string(), content);
        }

        fn track_record(&self, track: Address, record: CompressedTrack, raw: &[u8]) {
            self.gateway.records.lock().expect("lock").insert(track.to_string(), record);
            self.gateway.raw_tracks.lock().expect("lock").insert(track.to_string(), raw.to_vec());
        }

        fn flag(&self, categories: &[Category]) {
            *self.moderation.flagged.lock().expect("lock") = categories.to_vec();
        }

        async fn report(&self, target: Target, now: Timestamp) {
            let report = NewReport {
                target,
                reason: Reason::Other,
                note: None,
                idempotency_key: format!("key-{}-{}", target.address, now.0),
                reporter_hash: "reporter".to_string(),
            };
            self.records.submit_report(report, now).await.expect("submit report");
        }

        async fn run(&self, now: Timestamp) -> Option<JobOutcome> {
            self.worker
                .run_next(now, &CancellationToken::new())
                .await
                .expect("run job")
        }

        fn database(&self) -> PathBuf {
            self.dir.path().join("records.sqlite")
        }

        fn requests(&self) -> Vec<Value> {
            self.moderation.requests.lock().expect("lock").clone()
        }
    }

    fn config() -> WorkerConfig {
        WorkerConfig {
            poll_interval: Duration::from_secs(1),
            max_attempts: AttemptCount(3),
            initial_retry_delay: Duration::from_secs(10),
            max_retry_delay: Duration::from_secs(15),
            tape_track_limit: 2,
        }
    }

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn record(tape: Address, number: u64, size: u64) -> CompressedTrack {
        CompressedTrack {
            tape,
            track_number: TrackNumber(number),
            key: hash(&number.to_le_bytes()),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits(size),
            group: VirtualGroup(0),
            value_hash: Hash::default(),
        }
    }

    fn decisions(database: &FilePath) -> Vec<(String, String, String)> {
        let connection = Connection::open(database).expect("open database");
        let mut statement = connection
            .prepare("SELECT action, target_kind, target FROM decisions ORDER BY id")
            .expect("prepare");
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .expect("query");
        rows.map(|row| row.expect("row")).collect()
    }

    fn target_state(database: &FilePath, target: Target) -> String {
        let connection = Connection::open(database).expect("open database");
        connection
            .query_row(
                "SELECT state FROM targets WHERE kind = ?1 AND address = ?2",
                [target.kind.as_str().to_string(), target.address.to_string()],
                |row| row.get(0),
            )
            .expect("target state")
    }

    fn gateway_router(gateway: StubGateway) -> Router {
        Router::new()
            .route("/object/{id}", get(stub_object))
            .route("/track/{id}", get(stub_raw_track))
            .route(TRACK_PATH, get(stub_record))
            .route(TAPE_TRACK_LIST_PATH, post(stub_listing))
            .with_state(gateway)
    }

    fn moderation_router(moderation: StubModeration) -> Router {
        Router::new()
            .route("/moderations", post(stub_moderate))
            .with_state(moderation)
    }

    async fn stub_object(
        State(gateway): State<StubGateway>,
        Path(id): Path<String>,
        headers: HeaderMap,
    ) -> Response {
        let content = gateway.objects.lock().expect("lock").get(&id).cloned();
        match content {
            Some(content) if content.status == StatusCode::OK => ranged(content, &headers),
            Some(content) => content.status.into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn stub_raw_track(
        State(gateway): State<StubGateway>,
        Path(id): Path<String>,
        headers: HeaderMap,
    ) -> Response {
        let bytes = gateway.raw_tracks.lock().expect("lock").get(&id).cloned();
        match bytes {
            Some(bytes) => ranged(
                StubContent {
                    status: StatusCode::OK,
                    content_type: None,
                    bytes,
                },
                &headers,
            ),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn stub_record(State(gateway): State<StubGateway>, Path(id): Path<String>) -> Response {
        let track = gateway.records.lock().expect("lock").get(&id).copied();
        match track {
            Some(track) => {
                let response = TrackResponse { track: track.pack() };
                wincode::serialize(&response).expect("encode record").into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn stub_listing(
        State(gateway): State<StubGateway>,
        Path(tape): Path<String>,
        body: Bytes,
    ) -> Response {
        let request: ListTracksByTapeRequest = wincode::deserialize(&body).expect("decode listing");
        let tracks = gateway.listings.lock().expect("lock").get(&tape).cloned().unwrap_or_default();
        let after: Vec<CompressedTrack> = tracks
            .into_iter()
            .filter(|track| request.cursor.is_none_or(|cursor| track.track_number > cursor))
            .collect();
        let page: Vec<CompressedTrack> = after.iter().take(request.limit as usize).copied().collect();
        let next_cursor = (after.len() > page.len())
            .then(|| page.last().map(|track| track.track_number))
            .flatten();
        let response = ListTracksByTapeResponse {
            tracks: page.iter().map(CompressedTrack::pack).collect(),
            next_cursor,
        };
        wincode::serialize(&response).expect("encode listing").into_response()
    }

    async fn stub_moderate(State(moderation): State<StubModeration>, body: Bytes) -> Response {
        let request: Value = serde_json::from_slice(&body).expect("moderation request");
        moderation.requests.lock().expect("lock").push(request.clone());
        let status = *moderation.status.lock().expect("lock");
        if status != StatusCode::OK {
            return status.into_response();
        }

        let inputs = request["input"].as_array().cloned().unwrap_or_default();
        let is_image = inputs.first().is_some_and(Value::is_object);
        let flagged = moderation.flagged.lock().expect("lock").clone();
        let has_null_flags = *moderation.has_null_flags.lock().expect("lock");
        let results: Vec<Value> = inputs
            .iter()
            .map(|_| result_json(&flagged, is_image, has_null_flags))
            .collect();
        let response = json!({"id": "modr-test", "model": "omni-moderation-latest", "results": results});
        (StatusCode::OK, [(CONTENT_TYPE, "application/json")], response.to_string()).into_response()
    }

    fn result_json(flagged: &[Category], is_image: bool, has_null_flags: bool) -> Value {
        let mut categories = serde_json::Map::new();
        let mut scores = serde_json::Map::new();
        let mut applied = serde_json::Map::new();
        for category in Category::ALL {
            let is_flagged = flagged.contains(&category);
            let flag = match has_null_flags {
                true => Value::Null,
                false => json!(is_flagged),
            };
            let types = match IMAGE_CATEGORIES.contains(&category) {
                true => json!(["text", "image"]),
                false => json!(["text"]),
            };
            let applies = !is_image || IMAGE_CATEGORIES.contains(&category);
            categories.insert(category.as_str().to_string(), flag);
            scores.insert(category.as_str().to_string(), json!(if is_flagged { 0.98 } else { 0.01 }));
            applied.insert(category.as_str().to_string(), if applies { types } else { json!(["text"]) });
        }
        json!({
            "flagged": !flagged.is_empty(),
            "categories": categories,
            "category_scores": scores,
            "category_applied_input_types": applied,
        })
    }

    fn ranged(content: StubContent, headers: &HeaderMap) -> Response {
        let total = content.bytes.len();
        let end = headers
            .get(RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("bytes=0-"))
            .and_then(|last| last.parse::<usize>().ok())
            .map(|last| (last + 1).min(total));

        let mut response_headers = HeaderMap::new();
        if let Some(content_type) = content.content_type {
            response_headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        }
        match end {
            None => (StatusCode::OK, response_headers, content.bytes).into_response(),
            Some(_) if total == 0 => {
                response_headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes */0"));
                (StatusCode::RANGE_NOT_SATISFIABLE, response_headers).into_response()
            }
            Some(end) => {
                let range = HeaderValue::from_str(&format!("bytes 0-{}/{total}", end - 1)).expect("range header");
                response_headers.insert(CONTENT_RANGE, range);
                (StatusCode::PARTIAL_CONTENT, response_headers, content.bytes[..end].to_vec()).into_response()
            }
        }
    }

    // flagged text removes the track and records the check
    #[tokio::test]
    async fn removal() {
        let harness = Harness::new(config(), TapeRuleConfig::default()).await;
        let track = address(1);
        harness.object(track, Some("text/plain"), b"hateful words");
        harness.track_record(track, record(address(50), 0, 13), b"hateful words");
        harness.flag(&[Category::Hate]);
        harness.report(Target::track(track), Timestamp(100)).await;

        let outcome = harness.run(Timestamp(100)).await;

        assert_eq!(outcome, Some(JobOutcome::Checked { outcome: CheckOutcome::Remove, decisions: 1 }));
        assert_eq!(
            decisions(&harness.database()),
            vec![("remove".to_string(), "track".to_string(), track.to_string())]
        );
        assert_eq!(target_state(&harness.database(), Target::track(track)), "removed");
        harness.stop().await;
    }

    // removing a stream's manifest removes its chunk tracks on the same tape and no others
    #[tokio::test]
    async fn stream() {
        let harness = Harness::new(config(), TapeRuleConfig::default()).await;
        let tape = address(60);
        let manifest_track = address(2);
        let chunks = vec![record(tape, 3, 10), record(tape, 4, 10)];
        let other_tape = record(address(61), 0, 10);
        let unknown = record(tape, 9, 10);
        let entry = |chunk: &CompressedTrack, offset: u64| ChunkEntry {
            track: chunk.address(),
            offset: StorageUnits(offset),
            size: StorageUnits(10),
        };
        let manifest = ChunkManifest {
            version: MANIFEST_VERSION,
            total_size: StorageUnits(40),
            chunk_count: TrackNumber(4),
            chunk_size: StorageUnits(10),
            key: Hash::default(),
            chunks: vec![
                entry(&chunks[0], 0),
                entry(&chunks[1], 10),
                entry(&other_tape, 20),
                entry(&unknown, 30),
            ],
        };
        let manifest_bytes = manifest.to_bytes().expect("manifest bytes");
        harness.object(manifest_track, Some("text/plain"), b"violent text");
        harness.track_record(manifest_track, record(tape, 5, manifest_bytes.len() as u64), &manifest_bytes);
        for chunk in [chunks[0], chunks[1], other_tape] {
            harness.track_record(chunk.address(), chunk, b"");
        }
        harness.flag(&[Category::Violence]);
        harness.report(Target::track(manifest_track), Timestamp(100)).await;

        let outcome = harness.run(Timestamp(100)).await;

        assert_eq!(outcome, Some(JobOutcome::Checked { outcome: CheckOutcome::Remove, decisions: 3 }));
        let removed: Vec<String> = decisions(&harness.database()).into_iter().map(|row| row.2).collect();
        assert_eq!(
            removed,
            vec![
                manifest_track.to_string(),
                chunks[0].address().to_string(),
                chunks[1].address().to_string(),
            ]
        );
        harness.stop().await;
    }

    // whole text with every category applied is clean and publishes nothing
    #[tokio::test]
    async fn clean() {
        let harness = Harness::new(config(), TapeRuleConfig::default()).await;
        let track = address(3);
        harness.object(track, None, b"a pleasant note");
        harness.report(Target::track(track), Timestamp(100)).await;

        let outcome = harness.run(Timestamp(100)).await;

        assert_eq!(outcome, Some(JobOutcome::Checked { outcome: CheckOutcome::Clean, decisions: 0 }));
        assert!(decisions(&harness.database()).is_empty());
        assert_eq!(target_state(&harness.database(), Target::track(track)), "clean");
        harness.stop().await;
    }

    // a later report on an unresolved target is checked again, since nothing was decided
    #[tokio::test]
    async fn recheck_unresolved() {
        let harness = Harness::new(config(), TapeRuleConfig::default()).await;
        let track = address(9);
        let text = "a".repeat(8 * 8_000 + 10);
        harness.object(track, Some("text/plain"), text.as_bytes());
        harness.report(Target::track(track), Timestamp(100)).await;
        let first = harness.run(Timestamp(100)).await;

        harness.object(track, Some("text/plain"), b"short enough to judge");
        harness.report(Target::track(track), Timestamp(200)).await;
        let second = harness.run(Timestamp(200)).await;

        assert_eq!(first, Some(JobOutcome::Checked { outcome: CheckOutcome::Unresolved, decisions: 0 }));
        assert_eq!(second, Some(JobOutcome::Checked { outcome: CheckOutcome::Clean, decisions: 0 }));
        assert_eq!(target_state(&harness.database(), Target::track(track)), "clean");
        harness.stop().await;
    }

    // text longer than the segments sent stays unresolved even with no flags
    #[tokio::test]
    async fn partial_text() {
        let harness = Harness::new(config(), TapeRuleConfig::default()).await;
        let track = address(4);
        let text = "a".repeat(8 * 8_000 + 10);
        harness.object(track, Some("text/plain"), text.as_bytes());
        harness.report(Target::track(track), Timestamp(100)).await;

        let outcome = harness.run(Timestamp(100)).await;

        assert_eq!(outcome, Some(JobOutcome::Checked { outcome: CheckOutcome::Unresolved, decisions: 0 }));
        assert_eq!(harness.requests()[0]["input"].as_array().map(Vec::len), Some(8));
        assert_eq!(target_state(&harness.database(), Target::track(track)), "unresolved");
        harness.stop().await;
    }

    // an image is sent whole and judged on the categories that apply to images
    #[tokio::test]
    async fn image() {
        let harness = Harness::new(config(), TapeRuleConfig::default()).await;
        let track = address(5);
        harness.object(track, Some("image/png"), b"\x89PNG\r\n\x1a\npixels");
        harness.report(Target::track(track), Timestamp(100)).await;

        let outcome = harness.run(Timestamp(100)).await;

        assert_eq!(outcome, Some(JobOutcome::Checked { outcome: CheckOutcome::Clean, decisions: 0 }));
        let input = &harness.requests()[0]["input"][0];
        assert_eq!(input["type"], "image_url");
        assert!(input["image_url"]["url"].as_str().is_some_and(|url| url.starts_with("data:image/png;base64,")));
        harness.stop().await;
    }

    // null category values are never read as clean
    #[tokio::test]
    async fn null_flags() {
        let harness = Harness::new(config(), TapeRuleConfig::default()).await;
        let track = address(6);
        harness.object(track, Some("text/plain"), b"some text");
        *harness.moderation.has_null_flags.lock().expect("lock") = true;
        harness.report(Target::track(track), Timestamp(100)).await;

        let outcome = harness.run(Timestamp(100)).await;

        assert_eq!(outcome, Some(JobOutcome::Checked { outcome: CheckOutcome::Unresolved, decisions: 0 }));
        harness.stop().await;
    }

    // unsupported content settles unresolved without calling the api
    #[tokio::test]
    async fn unsupported_type() {
        let harness = Harness::new(config(), TapeRuleConfig::default()).await;
        let track = address(7);
        harness.object(track, Some("application/pdf"), b"%PDF-1.7 binary");
        harness.report(Target::track(track), Timestamp(100)).await;

        let outcome = harness.run(Timestamp(100)).await;

        assert_eq!(outcome, Some(JobOutcome::Checked { outcome: CheckOutcome::Unsupported, decisions: 0 }));
        assert!(harness.requests().is_empty());
        assert_eq!(target_state(&harness.database(), Target::track(track)), "unresolved");
        harness.stop().await;
    }

    // a track the gateway already refuses settles as gone without retrying
    #[tokio::test]
    async fn gone_track() {
        let harness = Harness::new(config(), TapeRuleConfig::default()).await;
        let track = address(8);
        let content = StubContent {
            status: StatusCode::GONE,
            content_type: None,
            bytes: Vec::new(),
        };
        harness.gateway.objects.lock().expect("lock").insert(track.to_string(), content);
        harness.report(Target::track(track), Timestamp(100)).await;

        let outcome = harness.run(Timestamp(100)).await;

        assert_eq!(outcome, Some(JobOutcome::Checked { outcome: CheckOutcome::Gone, decisions: 0 }));
        assert!(harness.requests().is_empty());
        assert_eq!(harness.run(Timestamp(10_000)).await, None);
        assert_eq!(target_state(&harness.database(), Target::track(track)), "gone");
        harness.stop().await;
    }

    // api failures retry after doubling delays and fail after the configured attempts
    #[tokio::test]
    async fn retries() {
        let harness = Harness::new(config(), TapeRuleConfig::default()).await;
        let track = address(9);
        harness.object(track, Some("text/plain"), b"some text");
        *harness.moderation.status.lock().expect("lock") = StatusCode::INTERNAL_SERVER_ERROR;
        harness.report(Target::track(track), Timestamp(100)).await;

        let first = harness.run(Timestamp(100)).await;
        let early = harness.run(Timestamp(109)).await;
        let second = harness.run(Timestamp(110)).await;
        let capped_early = harness.run(Timestamp(124)).await;
        let third = harness.run(Timestamp(125)).await;

        assert_eq!(
            [first, early, second, capped_early, third],
            [Some(JobOutcome::Retrying), None, Some(JobOutcome::Retrying), None, Some(JobOutcome::Failed)]
        );
        let connection = Connection::open(harness.database()).expect("open database");
        let errors: u64 = connection
            .query_row("SELECT COUNT(*) FROM checks WHERE outcome = 'error'", [], |row| row.get(0))
            .expect("count errors");
        assert_eq!(errors, 3);
        assert_eq!(target_state(&harness.database(), Target::track(track)), "failed");
        harness.stop().await;
    }

    // a tape report queues checks for up to the configured number of its certified tracks
    #[tokio::test]
    async fn listing() {
        let harness = Harness::new(config(), TapeRuleConfig::default()).await;
        let tape = address(70);
        let listed = vec![record(tape, 0, 10), record(tape, 1, 10), record(tape, 2, 10)];
        harness.gateway.listings.lock().expect("lock").insert(tape.to_string(), listed.clone());
        harness.report(Target::tape(tape), Timestamp(100)).await;

        let outcome = harness.run(Timestamp(100)).await;

        assert_eq!(outcome, Some(JobOutcome::Listed { queued: 2 }));
        let connection = Connection::open(harness.database()).expect("open database");
        let queued: Vec<String> = connection
            .prepare("SELECT target FROM jobs WHERE target_kind = 'track' AND tape = ?1 ORDER BY id")
            .expect("prepare")
            .query_map([tape.to_string()], |row| row.get(0))
            .expect("query")
            .map(|row| row.expect("row"))
            .collect();
        assert_eq!(
            queued,
            vec![listed[0].address().to_string(), listed[1].address().to_string()]
        );
        assert_eq!(target_state(&harness.database(), Target::tape(tape)), "listed");
        harness.stop().await;
    }

    // removals on one tape reaching the configured count unlist the tape
    #[tokio::test]
    async fn tape_unlisted() {
        let rule = TapeRuleConfig {
            triggering_categories: vec![Category::SexualMinors],
            removal_count: RemovalCount(2),
        };
        let harness = Harness::new(config(), rule).await;
        let tape = address(80);
        let listed = vec![record(tape, 0, 10), record(tape, 1, 10)];
        for track in &listed {
            let address = track.address();
            harness.object(address, Some("text/plain"), b"hateful words");
            harness.track_record(address, *track, b"hateful words");
        }
        harness.gateway.listings.lock().expect("lock").insert(tape.to_string(), listed);
        harness.flag(&[Category::Hate]);
        harness.report(Target::tape(tape), Timestamp(100)).await;

        let listing = harness.run(Timestamp(100)).await;
        let first = harness.run(Timestamp(100)).await;
        let second = harness.run(Timestamp(100)).await;

        assert_eq!(listing, Some(JobOutcome::Listed { queued: 2 }));
        assert_eq!(first, Some(JobOutcome::Checked { outcome: CheckOutcome::Remove, decisions: 1 }));
        assert_eq!(second, Some(JobOutcome::Checked { outcome: CheckOutcome::Remove, decisions: 2 }));
        let last = decisions(&harness.database()).pop().expect("unlist decision");
        assert_eq!(last, ("unlist".to_string(), "tape".to_string(), tape.to_string()));
        harness.stop().await;
    }
}
