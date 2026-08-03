//! The standing evidence a round produces when a quorum accepted one answer.
//!
//! A certificate is positive evidence of timely access, never proof of failure.
//! Its absence means only that this node did not see a quorum: an honest response
//! or an honest certificate can be delayed, so a missing certificate is a local
//! gap rather than an accusation.
//!
//! Every signer signed identical bytes, which is what lets their signatures
//! combine into one. Because the message names the candidate block, signatures
//! made against different branches cannot combine, and an aggregate can never
//! claim a quorum that never agreed on one history.

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
    /// The challenged owner is among the signers.
    SelfCertified,
    /// A signer appears more than once.
    DuplicateSigner,
    /// A signer has no registered key.
    UnknownSigner,
    /// The aggregate does not verify against the signers' keys.
    BadAggregate,
}

/// A quorum's agreement that one spool answered one round in time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SuccessCertificate {
    /// Epoch the round belongs to.
    pub epoch: EpochNumber,
    /// Group whose members signed it.
    pub group: GroupIndex,
    /// Round within the epoch.
    pub round: RoundNumber,
    /// The spool the certificate is about.
    pub spool: SpoolIndex,
    /// Candidate entropy block the round was drawn from.
    pub block: Hash,
    /// Who signed, in ascending order so the same quorum yields the same bytes.
    pub signers: Vec<Address>,
    /// The signers' attestations combined into one.
    pub signature: BlsSignature,
}

impl SuccessCertificate {
    /// Combine a round's attestations into a certificate.
    ///
    /// Signers are sorted so two nodes aggregating the same quorum produce the
    /// same certificate, which is what lets one be compared with another.
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

    /// Whether this certificate stands.
    ///
    /// `owner` is who holds the challenged spool, and `key_of` resolves a signer
    /// to its registered key. Both come from replayed state, never from the
    /// certificate itself.
    pub fn verify(
        &self,
        threshold: usize,
        owner: Address,
        key_of: impl Fn(Address) -> Option<BlsPubkey>,
    ) -> Result<(), CertificateRejection> {
        if self.signers.len() < threshold {
            return Err(CertificateRejection::BelowQuorum);
        }

        // A spool may contribute one signature but cannot certify itself, so a
        // roster containing the challenged owner is refused outright.
        if self.signers.contains(&owner) {
            return Err(CertificateRejection::SelfCertified);
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

    /// `count` observers signing one round, plus the owner they are about.
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

    // a quorum of other members certifies the round
    #[test]
    fn quorum_certifies() {
        let group = group_of(THRESHOLD, 0xAA);
        let certificate = certificate(&group, 0xAA);

        assert_eq!(
            certificate.verify(THRESHOLD, group.owner, |s| group.keys.get(&s).copied()),
            Ok(())
        );
    }

    // two nodes gathering the same attestations in different orders produce the
    // same certificate, or neither can be compared with the other
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

    // one signature short of the threshold is refused
    #[test]
    fn below_quorum() {
        let group = group_of(THRESHOLD - 1, 0xAA);
        let certificate = certificate(&group, 0xAA);

        assert_eq!(
            certificate.verify(THRESHOLD, group.owner, |s| group.keys.get(&s).copied()),
            Err(CertificateRejection::BelowQuorum)
        );
    }

    // the owner may sign, but a roster it appears in is not a quorum of others
    #[test]
    fn self_certified() {
        let mut group = group_of(THRESHOLD - 1, 0xAA);
        let key = BlsPrivateKey::from_random();
        group.keys.insert(group.owner, key.public_key().expect("pubkey"));
        group
            .signed
            .push((group.owner, key.sign(message(0xAA).to_bytes()).expect("sign")));

        let certificate = certificate(&group, 0xAA);
        assert_eq!(
            certificate.verify(THRESHOLD, group.owner, |s| group.keys.get(&s).copied()),
            Err(CertificateRejection::SelfCertified)
        );
    }

    // a signer counted twice is refused, or one peer reaches the threshold by
    // repeating itself
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

    // a signer with no registered key is refused
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

    // signers who saw different candidate blocks signed different bytes, so
    // their aggregate verifies against neither round
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

    // a certificate moved to another round is refused
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
