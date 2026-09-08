use tape_core::challenge::proof::ProofRejection;

/// Why this node would not accept an answer.
///
/// A refusal costs the answering owner a miss it may not have earned, so the
/// reason reaches the caller and the counters rather than dying in a log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusalReason {
    /// This node derived no question of its own for the round.
    NoLocalQuestion,
    /// The sampled track's registered encoding is not held here.
    EncodingNotHeld,
    /// No node owns the answering spool in this node's view.
    UnknownOwner,
    /// The owner holds no registered key in this node's view.
    UnknownKey,
    /// The answer names a group the spool does not belong to.
    WrongGroup,
    /// The answer is to a different question than the round asked.
    SampleMismatch,
    /// The proof does not reach the registered root.
    BadProof,
    /// The owner's signature does not cover the answer.
    BadSignature,
    /// The answer arrived after the round's deadline.
    Late,
}

impl RefusalReason {
    pub const COUNT: usize = 9;

    pub const ALL: [RefusalReason; Self::COUNT] = [
        RefusalReason::NoLocalQuestion,
        RefusalReason::EncodingNotHeld,
        RefusalReason::UnknownOwner,
        RefusalReason::UnknownKey,
        RefusalReason::WrongGroup,
        RefusalReason::SampleMismatch,
        RefusalReason::BadProof,
        RefusalReason::BadSignature,
        RefusalReason::Late,
    ];

    /// Stable name for logs, counters and the refusal body
    pub fn label(self) -> &'static str {
        match self {
            RefusalReason::NoLocalQuestion => "no_local_question",
            RefusalReason::EncodingNotHeld => "encoding_not_held",
            RefusalReason::UnknownOwner => "unknown_owner",
            RefusalReason::UnknownKey => "unknown_key",
            RefusalReason::WrongGroup => "wrong_group",
            RefusalReason::SampleMismatch => "sample_mismatch",
            RefusalReason::BadProof => "bad_proof",
            RefusalReason::BadSignature => "bad_signature",
            RefusalReason::Late => "late",
        }
    }

    /// Position in the counter array
    pub fn index(self) -> usize {
        match self {
            RefusalReason::NoLocalQuestion => 0,
            RefusalReason::EncodingNotHeld => 1,
            RefusalReason::UnknownOwner => 2,
            RefusalReason::UnknownKey => 3,
            RefusalReason::WrongGroup => 4,
            RefusalReason::SampleMismatch => 5,
            RefusalReason::BadProof => 6,
            RefusalReason::BadSignature => 7,
            RefusalReason::Late => 8,
        }
    }
}

impl From<ProofRejection> for RefusalReason {
    fn from(rejection: ProofRejection) -> Self {
        match rejection {
            ProofRejection::WrongGroup => RefusalReason::WrongGroup,
            ProofRejection::WrongSample => RefusalReason::SampleMismatch,
            ProofRejection::BadProof => RefusalReason::BadProof,
            ProofRejection::BadSignature => RefusalReason::BadSignature,
            ProofRejection::Late => RefusalReason::Late,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // the array and the index have to agree or a counter lands on another reason
    #[test]
    fn index_matches_position() {
        for (position, reason) in RefusalReason::ALL.iter().enumerate() {
            assert_eq!(reason.index(), position);
        }
    }

    // a label names one reason only, since operators filter on it
    #[test]
    fn labels_are_distinct() {
        let mut labels: Vec<&str> = RefusalReason::ALL.iter().map(|r| r.label()).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), RefusalReason::COUNT);
    }

    // every verify rejection maps to its own reason, so none of them collapse
    #[test]
    fn rejection_mapping() {
        assert_eq!(RefusalReason::from(ProofRejection::WrongGroup), RefusalReason::WrongGroup);
        assert_eq!(RefusalReason::from(ProofRejection::WrongSample), RefusalReason::SampleMismatch);
        assert_eq!(RefusalReason::from(ProofRejection::BadProof), RefusalReason::BadProof);
        assert_eq!(RefusalReason::from(ProofRejection::BadSignature), RefusalReason::BadSignature);
        assert_eq!(RefusalReason::from(ProofRejection::Late), RefusalReason::Late);
    }
}
