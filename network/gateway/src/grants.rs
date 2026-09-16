//! Read grants the gateway has been issued for restricted tracks.

use std::collections::HashMap;
use std::future::Future;

use tape_core::grant::{ReadGrant, WILDCARD_TRACK_KEY};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_protocol::api::{decode_read_grant, encode_read_grant, with_read_grant};
use tracing::warn;

/// Grants the operator configured, indexed by the track they open.
#[derive(Debug, Default)]
pub struct GrantStore {
    by_track: HashMap<(Address, Hash), ReadGrant>,
}

impl GrantStore {
    /// Empty store, for a gateway that serves no restricted tracks
    pub fn new() -> Self {
        Self {
            by_track: HashMap::new(),
        }
    }

    /// Decode the configured grants, skipping any the gateway cannot parse
    pub fn from_config(entries: &[String]) -> Self {
        let mut by_track = HashMap::new();
        for entry in entries {
            match decode_read_grant(entry) {
                Ok(grant) => {
                    by_track.insert((grant.tape, grant.key), grant);
                }
                Err(error) => {
                    warn!(reason = error.reason(), "skipping unparsable read grant");
                }
            }
        }

        Self { by_track }
    }

    /// The grant that opens one track key on one tape, or the tape's wildcard
    pub fn grant_for(&self, tape: Address, key: Hash) -> Option<ReadGrant> {
        self.by_track
            .get(&(tape, key))
            .or_else(|| self.by_track.get(&(tape, WILDCARD_TRACK_KEY)))
            .copied()
    }

    /// Whether the gateway holds any grant at all
    pub fn is_empty(&self) -> bool {
        self.by_track.is_empty()
    }
}

/// Run a fetch with the grant in scope, so the peer client stamps it on the way out
pub async fn with_grant<Fetch: Future>(grant: Option<ReadGrant>, fetch: Fetch) -> Fetch::Output {
    match grant {
        Some(grant) => with_read_grant(encode_read_grant(&grant), fetch).await,
        None => fetch.await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_crypto::ed25519::{Keypair, SecretKey};

    fn sample_grant(tape: Address, key: Hash) -> ReadGrant {
        let signer = Keypair::from_secret(SecretKey::from_bytes([0x21; 32]));

        ReadGrant::issue(
            &signer,
            tape,
            key,
            NetworkTlsPubkey::new([0x33; 32]),
            0,
            1_900_000_000,
        )
    }

    // a configured grant is found by the track it opens
    #[test]
    fn indexes_by_track() {
        let tape = Address::new_unique();
        let key = Hash([0x44; 32]);
        let grant = sample_grant(tape, key);

        let store = GrantStore::from_config(&[encode_read_grant(&grant)]);

        assert_eq!(store.grant_for(tape, key), Some(grant));
        assert_eq!(store.grant_for(tape, Hash([0x45; 32])), None);
    }

    // a wildcard grant answers for every track on its tape
    #[test]
    fn wildcard_entry() {
        let tape = Address::new_unique();
        let grant = sample_grant(tape, WILDCARD_TRACK_KEY);

        let store = GrantStore::from_config(&[encode_read_grant(&grant)]);

        assert_eq!(store.grant_for(tape, Hash([0x46; 32])), Some(grant));
        assert_eq!(store.grant_for(Address::new_unique(), Hash([0x46; 32])), None);
    }

    // an exact grant is preferred over the tape's wildcard
    #[test]
    fn exact_wins() {
        let tape = Address::new_unique();
        let key = Hash([0x47; 32]);
        let exact = sample_grant(tape, key);
        let wildcard = sample_grant(tape, WILDCARD_TRACK_KEY);

        let store = GrantStore::from_config(&[
            encode_read_grant(&exact),
            encode_read_grant(&wildcard),
        ]);

        assert_eq!(store.grant_for(tape, key), Some(exact));
    }

    // an unparsable entry is skipped rather than taking the gateway down
    #[test]
    fn skips_junk() {
        let store = GrantStore::from_config(&["not a grant".to_string()]);

        assert!(store.is_empty());
    }
}
