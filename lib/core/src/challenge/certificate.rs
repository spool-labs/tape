//! Evidence that a quorum accepted one answer to a challenge round.
//!
//! A certificate proves timely access. Its absence is inconclusive because an
//! honest response or certificate may be delayed. Every signer signs the same
//! round and candidate block, preventing signatures from different branches
//! from being combined.

use tape_crypto::Address;
use tape_crypto::hash::Hash;

use crate::bls::{BlsPubkey, BlsSignature};
use crate::cert::challenge::ChallengeAttestMessage;
use crate::types::{EpochNumber, GroupIndex, RoundNumber, SpoolIndex};

/// Why a certificate would not stand.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CertificateRejection {
    /// Fewer signatures than the group's agreement threshold.
    BelowQuorum,
    /// Nobody but the challenged owner signed.
    OwnerOnly,
    /// A signer appears more than once.
    DuplicateSigner,
    /// A signer has no registered key.
    UnknownSigner,
    /// The aggregate does not verify against the signers' keys.
    BadAggregate,
}

/// A quorum's agreement that one spool answered one round in time. Signers are
/// stored in ascending address order for deterministic certificate bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SuccessCertificate {
    pub epoch: EpochNumber,
    pub group: GroupIndex,
    pub round: RoundNumber,
    pub spool: SpoolIndex,
    pub block: Hash,
    pub signers: Vec<Address>,
    pub signature: BlsSignature,
}

impl SuccessCertificate {
    /// Combine a round's attestations into a certificate.
    pub fn aggregate(
        epoch: EpochNumber,
        group: GroupIndex,
        round: RoundNumber,
        spool: SpoolIndex,
        block: Hash,
        mut attestations: Vec<(Address, BlsSignature)>,
    ) -> Option<Self> {
        if attestations.is_empty() {
            return None;
        }

        attestations.sort_unstable_by_key(|(signer, _)| *signer);
        let signatures: Vec<BlsSignature> = attestations.iter().map(|(_, sig)| *sig).collect();

        Some(Self {
            epoch,
            group,
            round,
            spool,
            block,
            signers: attestations.into_iter().map(|(signer, _)| signer).collect(),
            signature: BlsSignature::aggregate(&signatures).ok()?,
        })
    }

    /// The message every signer signed.
    pub fn message(&self) -> ChallengeAttestMessage {
        ChallengeAttestMessage::new(self.epoch, self.group, self.round, self.spool, self.block)
    }

    /// Whether this certificate stands, against a quorum key summed once for
    /// the whole signer set.
    #[cfg(not(target_os = "solana"))]
    pub fn verify_summed(
        &self,
        threshold: usize,
        owner: Address,
        quorum: &crate::bls::BlsQuorumKey,
    ) -> Result<(), CertificateRejection> {
        if self.signers.len() < threshold {
            return Err(CertificateRejection::BelowQuorum);
        }
        if self.signers.iter().all(|signer| *signer == owner) {
            return Err(CertificateRejection::OwnerOnly);
        }
        if self.signers.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(CertificateRejection::DuplicateSigner);
        }
        self.signature
            .verify_quorum(self.message().to_bytes(), quorum)
            .map_err(|_| CertificateRejection::BadAggregate)
    }

    /// Whether this certificate stands.
    pub fn verify(
        &self,
        threshold: usize,
        owner: Address,
        key_of: impl Fn(Address) -> Option<BlsPubkey>,
    ) -> Result<(), CertificateRejection> {
        if self.signers.len() < threshold {
            return Err(CertificateRejection::BelowQuorum);
        }

        // The owner may contribute one signature but cannot certify itself. Its
        // own is one of the q, since the threshold counts it among the members
        // and a group at the fault bound has no position to spare, but a roster
        // of nobody else is not a witness.
        if self.signers.iter().all(|signer| *signer == owner) {
            return Err(CertificateRejection::OwnerOnly);
        }

        if self.signers.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(CertificateRejection::DuplicateSigner);
        }

        let keys: Vec<BlsPubkey> = self
            .signers
            .iter()
            .map(|signer| key_of(*signer).ok_or(CertificateRejection::UnknownSigner))
            .collect::<Result<_, _>>()?;

        self.signature
            .verify_aggregate(self.message().to_bytes(), &keys)
            .map_err(|_| CertificateRejection::BadAggregate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bls::BlsPrivateKey;
    use std::collections::HashMap;

    const THRESHOLD: usize = 4;

    struct Group {
        keys: HashMap<Address, BlsPubkey>,
        signed: Vec<(Address, BlsSignature)>,
        owner: Address,
    }

    fn message(block: u8) -> ChallengeAttestMessage {
        ChallengeAttestMessage::new(
            EpochNumber(2),
            GroupIndex(1),
            RoundNumber(5),
            SpoolIndex(21),
            Hash([block; 32]),
        )
    }

    fn group_of(count: usize, block: u8) -> Group {
        let mut keys = HashMap::new();
        let mut signed = Vec::new();

        for _ in 0..count {
            let key = BlsPrivateKey::from_random();
            let signer = Address::new_unique();
            keys.insert(signer, key.public_key().expect("pubkey"));
            signed.push((signer, key.sign(message(block).to_bytes()).expect("sign")));
        }

        let owner = Address::new_unique();
        keys.insert(owner, BlsPrivateKey::from_random().public_key().expect("pubkey"));

        Group { keys, signed, owner }
    }

    fn certificate(group: &Group, block: u8) -> SuccessCertificate {
        SuccessCertificate::aggregate(
            EpochNumber(2),
            GroupIndex(1),
            RoundNumber(5),
            SpoolIndex(21),
            Hash([block; 32]),
            group.signed.clone(),
        )
        .expect("aggregate")
    }

    #[test]
    fn quorum_certifies() {
        let group = group_of(THRESHOLD, 0xAA);
        let certificate = certificate(&group, 0xAA);

        assert_eq!(
            certificate.verify(THRESHOLD, group.owner, |s| group.keys.get(&s).copied()),
            Ok(())
        );
    }

    #[test]
    fn ordered_signers() {
        let group = group_of(THRESHOLD, 0xAA);
        let mut reversed = group.signed.clone();
        reversed.reverse();

        let one = certificate(&group, 0xAA);
        let other = SuccessCertificate::aggregate(
            EpochNumber(2),
            GroupIndex(1),
            RoundNumber(5),
            SpoolIndex(21),
            Hash([0xAA; 32]),
            reversed,
        )
        .expect("aggregate");

        assert_eq!(one, other);
        assert!(one.signers.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn below_quorum() {
        let group = group_of(THRESHOLD - 1, 0xAA);
        let certificate = certificate(&group, 0xAA);

        assert_eq!(
            certificate.verify(THRESHOLD, group.owner, |s| group.keys.get(&s).copied()),
            Err(CertificateRejection::BelowQuorum)
        );
    }

    #[test]
    fn owner_counts() {
        let mut group = group_of(THRESHOLD - 1, 0xAA);
        let key = BlsPrivateKey::from_random();
        group.keys.insert(group.owner, key.public_key().expect("pubkey"));
        group
            .signed
            .push((group.owner, key.sign(message(0xAA).to_bytes()).expect("sign")));

        let certificate = certificate(&group, 0xAA);
        assert_eq!(
            certificate.verify(THRESHOLD, group.owner, |s| group.keys.get(&s).copied()),
            Ok(())
        );
    }

    #[test]
    fn owner_only() {
        let mut group = group_of(0, 0xAA);
        let key = BlsPrivateKey::from_random();
        group.keys.insert(group.owner, key.public_key().expect("pubkey"));
        group
            .signed
            .push((group.owner, key.sign(message(0xAA).to_bytes()).expect("sign")));

        let certificate = certificate(&group, 0xAA);
        assert_eq!(
            certificate.verify(1, group.owner, |s| group.keys.get(&s).copied()),
            Err(CertificateRejection::OwnerOnly)
        );
    }

    #[test]
    fn duplicate_signer() {
        let group = group_of(THRESHOLD, 0xAA);
        let mut doubled = certificate(&group, 0xAA);
        doubled.signers[1] = doubled.signers[0];

        assert_eq!(
            doubled.verify(THRESHOLD, group.owner, |s| group.keys.get(&s).copied()),
            Err(CertificateRejection::DuplicateSigner)
        );
    }

    #[test]
    fn unknown_signer() {
        let group = group_of(THRESHOLD, 0xAA);
        let certificate = certificate(&group, 0xAA);
        let stranger = certificate.signers[0];

        assert_eq!(
            certificate.verify(THRESHOLD, group.owner, |s| {
                if s == stranger {
                    None
                } else {
                    group.keys.get(&s).copied()
                }
            }),
            Err(CertificateRejection::UnknownSigner)
        );
    }

    #[test]
    fn mixed_branches() {
        let mut group = group_of(THRESHOLD - 1, 0xAA);
        let other = group_of(1, 0xBB);
        group.keys.extend(other.keys);
        group.signed.extend(other.signed);

        let certificate = certificate(&group, 0xAA);
        assert_eq!(
            certificate.verify(THRESHOLD, group.owner, |s| group.keys.get(&s).copied()),
            Err(CertificateRejection::BadAggregate)
        );
    }

    #[test]
    fn moved_round() {
        let group = group_of(THRESHOLD, 0xAA);
        let mut moved = certificate(&group, 0xAA);
        moved.round = RoundNumber(6);

        assert_eq!(
            moved.verify(THRESHOLD, group.owner, |s| group.keys.get(&s).copied()),
            Err(CertificateRejection::BadAggregate)
        );
    }
}
