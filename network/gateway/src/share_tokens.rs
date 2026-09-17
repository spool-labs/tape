//! Operator-configured share tokens for the gateway's own fetches of hidden tracks

use std::collections::HashMap;

use tape_core::share::{ShareToken, ALL_TRACKS};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_protocol::api::decode_share_token;
use tracing::warn;

/// Configured share tokens, indexed by tape and track key
#[derive(Debug, Default)]
pub struct ConfiguredShareTokens {
    by_track: HashMap<(Address, Hash), ShareToken>,
}

impl ConfiguredShareTokens {
    /// Decode the configured tokens and skip unparsable ones
    pub fn from_config(entries: &[String]) -> Self {
        let mut by_track = HashMap::new();
        for entry in entries {
            match decode_share_token(entry) {
                Ok(token) => {
                    by_track.insert((token.tape, token.track_key), token);
                }
                Err(error) => {
                    warn!(reason = error.reason(), "skipping unparsable share token");
                }
            }
        }

        Self { by_track }
    }

    /// The token for one track key on one tape, or the tape's all-tracks token
    pub fn token_for(&self, tape: Address, key: Hash) -> Option<ShareToken> {
        self.by_track
            .get(&(tape, key))
            .or_else(|| self.by_track.get(&(tape, ALL_TRACKS)))
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::types::tls::NetworkTlsPubkey;
    use tape_crypto::ed25519::{Keypair, SecretKey};
    use tape_protocol::api::encode_share_token;

    fn sample_token(tape: Address, key: Hash) -> ShareToken {
        let signer = Keypair::from_secret(SecretKey::from_bytes([0x21; 32]));

        ShareToken::issue(
            &signer,
            tape,
            key,
            NetworkTlsPubkey::new([0x33; 32]),
            Hash::default(),
            1_900_000_000,
        )
    }

    // a lookup by track key finds its configured token
    #[test]
    fn exact_track() {
        let tape = Address::new_unique();
        let key = Hash([0x44; 32]);
        let token = sample_token(tape, key);

        let tokens = ConfiguredShareTokens::from_config(&[encode_share_token(&token)]);

        assert_eq!(tokens.token_for(tape, key), Some(token));
        assert_eq!(tokens.token_for(tape, Hash([0x45; 32])), None);
    }

    // an all-tracks token answers for every track on its tape
    #[test]
    fn all_tracks() {
        let tape = Address::new_unique();
        let token = sample_token(tape, ALL_TRACKS);

        let tokens = ConfiguredShareTokens::from_config(&[encode_share_token(&token)]);

        assert_eq!(tokens.token_for(tape, Hash([0x46; 32])), Some(token));
        assert_eq!(tokens.token_for(Address::new_unique(), Hash([0x46; 32])), None);
    }

    // an exact token is preferred over the tape's all-tracks token
    #[test]
    fn exact_wins() {
        let tape = Address::new_unique();
        let key = Hash([0x47; 32]);
        let exact = sample_token(tape, key);
        let all = sample_token(tape, ALL_TRACKS);

        let tokens = ConfiguredShareTokens::from_config(&[
            encode_share_token(&exact),
            encode_share_token(&all),
        ]);

        assert_eq!(tokens.token_for(tape, key), Some(exact));
    }

    // an unparsable entry is skipped and the gateway keeps running
    #[test]
    fn skips_junk() {
        let tokens = ConfiguredShareTokens::from_config(&["not a token".to_string()]);

        assert!(tokens.by_track.is_empty());
    }
}
