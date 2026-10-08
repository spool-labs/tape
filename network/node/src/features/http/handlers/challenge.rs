//! Challenge round endpoints: a peer's proofs in, and this node's board out.

use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use serde::Deserialize;

use tape_core::types::SlotNumber;
use tape_protocol::api::{ChallengeRequest, BINARY_CONTENT};
use tape_protocol::Api;
use tape_rpc::Rpc;
use tape_store_api::Store;

use crate::features::challenge::board::BOARD_REPLY_ROUNDS;
use crate::features::challenge::handle::Arrival;
use crate::features::challenge::holders::answers_for;
use crate::features::challenge::wire::{from_request, to_board};
use crate::features::http::auth::ActivePeer;
use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

#[derive(Deserialize)]
pub struct BoardQuery {
    /// The slot of the newest round the caller holds.
    #[serde(default)]
    pub since: u64,
}

/// Take a peer's proofs for a round
///
/// The arrival is stamped first and the answer goes out before any proof is
/// checked. The task checks them afterwards.
pub async fn challenge<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    active_peer: ActivePeer,
    body: Bytes,
) -> Result<StatusCode, RouteError> {
    let at = Instant::now();
    let handle = &state.context.challenge;
    if !handle.is_enabled() {
        return Err(RouteError::Forbidden("challenge rounds disabled on this node".into()));
    }

    let request: ChallengeRequest = wincode::deserialize(&body)
        .map_err(|error| RouteError::BadRequest(format!("challenge request: {error}")))?;
    let batch = from_request(request);

    if !answers_for(&state.context.state(), active_peer.node, &batch) {
        return Err(RouteError::Forbidden("sender does not answer for these spools".into()));
    }

    let arrival = Arrival { sender: active_peer.node, batch, at };
    if !handle.submit(arrival) {
        return Ok(StatusCode::SERVICE_UNAVAILABLE);
    }

    Ok(StatusCode::ACCEPTED)
}

/// Answer with this node's rounds after a slot, oldest first
pub async fn challenge_board<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
    Query(query): Query<BoardQuery>,
) -> Result<impl IntoResponse, RouteError> {
    let handle = &state.context.challenge;
    let board = if handle.is_enabled() {
        to_board(true, handle.rounds_since(SlotNumber(query.since), BOARD_REPLY_ROUNDS))
    } else {
        to_board(false, Vec::new())
    };

    let body = wincode::serialize(&board)
        .map_err(|error| RouteError::Internal(format!("challenge board: {error}")))?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, BINARY_CONTENT)], body))
}

#[cfg(test)]
mod tests {
    use peer_memory::MemoryApi;
    use rpc_litesvm::LiteSvmRpc;
    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_core::types::{EpochNumber, RoundNumber, SpoolIndex};
    use tape_crypto::{Address, Hash};
    use tape_protocol::api::{ChallengeBoard, ChallengeData, ChallengeProof};
    use tape_store_memory::MemoryStore;

    use crate::features::challenge::schedule::Schedule;
    use crate::harness::{NodeHarness, TestContext};

    use super::*;

    const NODES: usize = 20;
    const EPOCH: EpochNumber = EpochNumber(10);
    const START: SlotNumber = SlotNumber(1_000);

    type App = State<AppState<MemoryStore, MemoryApi, LiteSvmRpc>>;

    async fn cluster(is_enabled: bool) -> NodeHarness {
        NodeHarness::builder()
            .nodes(NODES)
            .epoch(EPOCH)
            .current_group_count(1)
            .challenge(is_enabled)
            .build()
            .await
            .expect("build harness")
    }

    fn app(ctx: &TestContext) -> App {
        State(AppState { context: ctx.clone() })
    }

    fn peer(node: Address) -> ActivePeer {
        ActivePeer { node, tls_pubkey: NetworkTlsPubkey::new([0u8; 32]) }
    }

    /// A request carrying one proof for `spool`
    fn body(spool: SpoolIndex) -> Bytes {
        let proof = ChallengeProof {
            spool,
            track: Address::from([7u8; 32]),
            data: ChallengeData::Inline(vec![1, 2, 3]),
        };
        let request = ChallengeRequest { epoch: EPOCH, round: RoundNumber(1), proofs: vec![proof] };

        Bytes::from(wincode::serialize(&request).expect("request encodes"))
    }

    /// Poll a node's board and decode the answer
    async fn board(ctx: &TestContext, since: u64) -> ChallengeBoard {
        let response = challenge_board(app(ctx), Query(BoardQuery { since }))
            .await
            .expect("board answers")
            .into_response();
        assert_eq!(response.headers()[header::CONTENT_TYPE], BINARY_CONTENT);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("board body");
        wincode::deserialize(&body).expect("board decodes")
    }

    // with the flag off the proofs route refuses and the board says disabled
    #[tokio::test]
    async fn disabled() {
        let harness = cluster(false).await;
        let ctx = harness.ctx_for(0);
        let sender = harness.ctx_for(1).node_address();
        let spool = harness.owned_spools(1)[0];

        let refused = challenge(app(&ctx), peer(sender), body(spool)).await;
        let board = board(&ctx, 0).await;

        assert!(matches!(refused, Err(RouteError::Forbidden(_))));
        assert!(!board.is_enabled);
        assert!(board.rounds.is_empty());
    }

    // a body that does not decode is refused
    #[tokio::test]
    async fn malformed() {
        let harness = cluster(true).await;
        let ctx = harness.ctx_for(0);
        let sender = harness.ctx_for(1).node_address();

        let refused = challenge(app(&ctx), peer(sender), Bytes::from_static(b"junk")).await;

        assert!(matches!(refused, Err(RouteError::BadRequest(_))));
    }

    // a sender that holds none of the spools it sends proofs for is refused
    #[tokio::test]
    async fn not_the_holder() {
        let harness = cluster(true).await;
        let ctx = harness.ctx_for(0);
        let sender = harness.ctx_for(2).node_address();
        let spool = harness.owned_spools(1)[0];

        let refused = challenge(app(&ctx), peer(sender), body(spool)).await;

        assert!(matches!(refused, Err(RouteError::Forbidden(_))));
    }

    // a holder's proofs are queued and answered before anything is checked or recorded
    #[tokio::test]
    async fn queued() {
        let harness = cluster(true).await;
        let ctx = harness.ctx_for(0);
        let sender = harness.ctx_for(1).node_address();
        let spool = harness.owned_spools(1)[0];
        let mut inbox = ctx.challenge.attach();
        let before = Instant::now();

        let answer = challenge(app(&ctx), peer(sender), body(spool)).await;

        let arrival = inbox.try_recv().expect("queued batch");
        assert!(matches!(answer, Ok(StatusCode::ACCEPTED)));
        assert_eq!(arrival.sender, sender);
        assert_eq!(arrival.batch.proofs[0].spool, spool);
        assert!(arrival.at >= before && arrival.at <= Instant::now());
        assert!(ctx.challenge.challenger().board().is_empty());
    }

    // with no task the proofs are turned away, and a task that starts after
    // another ended takes the queue over
    #[tokio::test]
    async fn task_restart() {
        let harness = cluster(true).await;
        let ctx = harness.ctx_for(0);
        let sender = harness.ctx_for(1).node_address();
        let spool = harness.owned_spools(1)[0];

        let before_any = challenge(app(&ctx), peer(sender), body(spool)).await;
        drop(ctx.challenge.attach());
        let after_first = challenge(app(&ctx), peer(sender), body(spool)).await;
        let mut second = ctx.challenge.attach();
        let with_second = challenge(app(&ctx), peer(sender), body(spool)).await;

        assert!(matches!(before_any, Ok(StatusCode::SERVICE_UNAVAILABLE)));
        assert!(matches!(after_first, Ok(StatusCode::SERVICE_UNAVAILABLE)));
        assert!(matches!(with_second, Ok(StatusCode::ACCEPTED)));
        assert!(second.try_recv().is_ok());
    }

    // a poll gets the rounds after its slot, oldest first, and never more than the cap
    #[tokio::test]
    async fn board_reply() {
        let harness = cluster(true).await;
        let ctx = harness.ctx_for(0);
        let rounds = BOARD_REPLY_ROUNDS as u64 + 44;
        {
            let mut challenger = ctx.challenge.challenger();
            challenger.begin_epoch(EPOCH, Schedule::new(START, 1), true);
            for round in 1..=rounds {
                let slot = SlotNumber(START.0 + round);
                challenger.on_block(&ctx.store, &ctx.state(), slot, Hash::default(), false, Instant::now());
            }
        }

        let first = board(&ctx, 0).await;
        let newest = first.rounds[first.rounds.len() - 1].slot;
        let rest = board(&ctx, newest.0).await;

        assert!(first.is_enabled);
        assert_eq!(first.rounds.len(), BOARD_REPLY_ROUNDS);
        assert_eq!(first.rounds[0].slot, SlotNumber(START.0 + 1));
        assert!(first.rounds.windows(2).all(|pair| pair[0].slot < pair[1].slot));
        assert!(first.rounds.iter().all(|round| !round.is_observed));
        assert_eq!(rest.rounds.len(), 44);
        assert_eq!(rest.rounds[0].slot, SlotNumber(newest.0 + 1));
    }
}
