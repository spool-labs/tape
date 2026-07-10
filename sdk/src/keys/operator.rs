//! Tape operator abstraction.

use tape_crypto::address::Address;
use tape_crypto::ed25519::{Keypair, Pubkey};

use crate::keys::tape_key::TapeKey;

/// An identity authorized to operate (write/certify tracks) on a tape.
pub trait TapeOperator: Send + Sync {
    /// Keypair that signs the write and certify instructions.
    fn keypair(&self) -> &Keypair;

    /// Operator pubkey placed in the instruction's signer slot.
    fn pubkey(&self) -> Pubkey;

    /// On-chain address of the tape being operated on.
    fn address(&self) -> Address;
}

impl TapeOperator for TapeKey {
    fn keypair(&self) -> &Keypair {
        TapeKey::keypair(self)
    }

    fn pubkey(&self) -> Pubkey {
        TapeKey::pubkey(self)
    }

    fn address(&self) -> Address {
        TapeKey::address(self)
    }
}

/// A delegate authorized to write to a tape it does not own.
pub struct TapeDelegate {
    keypair: Keypair,
    tape: Address,
}

impl TapeDelegate {
    /// Bind a delegate keypair to the tape it may operate on.
    pub fn new(keypair: Keypair, tape: Address) -> Self {
        Self { keypair, tape }
    }

    /// The tape this delegate operates on.
    pub fn tape(&self) -> Address {
        self.tape
    }
}

impl TapeOperator for TapeDelegate {
    fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    fn pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    fn address(&self) -> Address {
        self.tape
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // operator trait matches TapeKey's inherent accessors
    #[test]
    fn operator_accessors() {
        let key = TapeKey::generate();
        let operator: &dyn TapeOperator = &key;
        assert_eq!(operator.pubkey(), key.pubkey());
        assert_eq!(operator.address(), key.address());
    }

    // delegate signs with its own pubkey but targets the owner's tape
    #[test]
    fn delegate_identity() {
        let owner = TapeKey::generate();
        let delegate_keypair = Keypair::new(&mut rand::thread_rng());
        let delegate_pubkey = delegate_keypair.pubkey();
        let delegate = TapeDelegate::new(delegate_keypair, owner.address());

        assert_eq!(delegate.pubkey(), delegate_pubkey);
        assert_eq!(delegate.address(), owner.address());
        assert_ne!(delegate.pubkey(), owner.pubkey());
    }
}
