use std::fmt::Display;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tape_crypto::Pubkey;
use tape_store::ops::SpoolOps;
use tape_store::types::{Pubkey as StorePubkey, SpoolState};

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

pub(crate) fn parse_track_address(track_id: &str) -> Result<Pubkey, RouteError> {
    track_id
        .parse()
        .map_err(|error| RouteError::BadRequest(format!("invalid track id: {error}")))
}

pub(crate) fn parse_track_key(track_id: &str) -> Result<(Pubkey, StorePubkey), RouteError> {
    let track = parse_track_address(track_id)?;
    Ok((track, track.into()))
}

pub(crate) fn deserialize_body<T>(body: &[u8], label: &str) -> Result<T, RouteError>
where
    T: for<'de> wincode::SchemaRead<'de, Dst = T>,
{
    wincode::deserialize(body)
        .map_err(|error| RouteError::BadRequest(format!("{label}: {error}")))
}

pub(crate) fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

pub(crate) fn ensure_spool_known<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    spool_id: u16,
) -> Result<SpoolState, RouteError> {
    state
        .context
        .store
        .get_spool_state(spool_id)
        .map_err(store_error)?
        .ok_or(RouteError::NotResponsible)
}

pub(crate) fn ensure_spool_writable<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    spool_id: u16,
) -> Result<(), RouteError> {
    let spool_state = ensure_spool_known(state, spool_id)?;

    if spool_state.is_locked() {
        Err(RouteError::NotResponsible)
    } else {
        Ok(())
    }
}
