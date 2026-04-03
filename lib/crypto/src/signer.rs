#![allow(unexpected_cfgs)]

#[cfg(not(target_os = "solana"))]
use crate::address::Address;
#[cfg(not(target_os = "solana"))]
use crate::ed25519::{Keypair, Pubkey, Signature};

#[cfg(not(target_os = "solana"))]
pub trait Signer: Send + Sync {
    fn pubkey(&self) -> Pubkey;
    fn sign(&self, message: &[u8]) -> Signature;

    fn address(&self) -> Address {
        self.pubkey().into()
    }
}

#[cfg(not(target_os = "solana"))]
impl Signer for Keypair {
    fn pubkey(&self) -> Pubkey {
        Keypair::pubkey(self)
    }

    fn sign(&self, message: &[u8]) -> Signature {
        Keypair::sign(self, message)
    }
}

#[cfg(all(test, not(target_os = "solana")))]
mod tests {
    use crate::ed25519::Keypair;

    use super::Signer;

    #[test]
    fn keypair_implements_signer() {
        let mut rng = rand::thread_rng();
        let keypair = Keypair::new(&mut rng);
        let signer: &dyn Signer = &keypair;
        let message = b"hello signer";
        let signature = signer.sign(message);

        assert_eq!(signer.pubkey(), keypair.pubkey());
        assert_eq!(signer.address(), keypair.address());
        assert!(signer.pubkey().verify(message, &signature).is_ok());
    }
}
