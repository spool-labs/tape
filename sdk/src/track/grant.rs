use std::future::Future;

use tape_core::grant::ReadGrant;
use tape_protocol::api::{encode_read_grant, with_read_grant};

/// Run a fetch with the grant in scope, so the peer client stamps it on the way out
pub(crate) async fn with_grant<Fetch: Future>(grant: Option<ReadGrant>, fetch: Fetch) -> Fetch::Output {
    match grant {
        Some(grant) => with_read_grant(encode_read_grant(&grant), fetch).await,
        None => fetch.await,
    }
}
