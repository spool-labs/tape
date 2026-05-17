//! Generic vote coordination columns.

use store::Column;
use tape_core::bls::BlsSignature;

use crate::types::VoteSigKey;

/// Pushed BLS signatures keyed by vote candidate, group, and signer.
pub struct VoteSigCol;

impl Column for VoteSigCol {
    const CF_NAME: &'static str = "vote_sig";
    type Key = VoteSigKey;
    type Value = BlsSignature;
}
