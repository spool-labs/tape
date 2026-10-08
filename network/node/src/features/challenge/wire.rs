//! Converts batches and board rounds to and from their wire forms

use tape_core::track::blob::SubLeafProof;
use tape_protocol::api::{
    ChallengeBoard, ChallengeData, ChallengeEntry, ChallengeLeaf, ChallengeOutcome,
    ChallengeProof, ChallengeReq, ChallengeRequest, ChallengeRound, ChallengeSeen,
};

use crate::features::challenge::board::{BoardRound, Outcome};
use crate::features::challenge::proof::Proof;
use crate::features::challenge::round::{Batch, SpoolProof};

pub fn to_req(batch: Batch) -> ChallengeReq {
    let mut proofs = Vec::with_capacity(batch.proofs.len());
    for proof in batch.proofs {
        let data = match proof.proof {
            Proof::Coded(leaf) => ChallengeData::Coded(ChallengeLeaf {
                sub_leaf: leaf.sub_leaf,
                sub_proof: leaf.sub_proof,
            }),
            Proof::Inline(payload) => ChallengeData::Inline(payload),
        };
        proofs.push(ChallengeProof { spool: proof.spool, track: proof.track, data });
    }

    ChallengeReq { epoch: batch.epoch, round: batch.round, proofs }
}

pub fn from_request(request: ChallengeRequest) -> Batch {
    let mut proofs = Vec::with_capacity(request.proofs.len());
    for proof in request.proofs {
        let data = match proof.data {
            ChallengeData::Coded(leaf) => Proof::Coded(SubLeafProof {
                sub_leaf: leaf.sub_leaf,
                sub_proof: leaf.sub_proof,
            }),
            ChallengeData::Inline(payload) => Proof::Inline(payload),
        };
        proofs.push(SpoolProof { spool: proof.spool, track: proof.track, proof: data });
    }

    Batch { epoch: request.epoch, round: request.round, proofs }
}

/// Build the board route's answer from the rounds read off the board
pub fn to_board(is_enabled: bool, rounds: Vec<BoardRound>) -> ChallengeBoard {
    let mut wire = Vec::with_capacity(rounds.len());
    for round in rounds {
        let mut entries = Vec::with_capacity(round.entries.len());
        for entry in round.entries {
            let seen = entry.seen.map(|seen| ChallengeSeen {
                outcome: to_outcome(seen.outcome),
                ms: seen.ms,
            });
            entries.push(ChallengeEntry { spool: entry.spool, seen });
        }

        wire.push(ChallengeRound {
            slot: round.slot,
            is_observed: round.opened.is_some(),
            entries,
        });
    }

    ChallengeBoard { is_enabled, rounds: wire }
}

fn to_outcome(outcome: Outcome) -> ChallengeOutcome {
    match outcome {
        Outcome::Ok => ChallengeOutcome::Ok,
        Outcome::Invalid => ChallengeOutcome::Invalid,
        Outcome::WrongTrack => ChallengeOutcome::WrongTrack,
    }
}

#[cfg(test)]
mod tests {
    use tape_core::types::{EpochNumber, RoundNumber, SlotNumber, SpoolIndex};
    use tape_crypto::{Address, Hash};

    use crate::features::challenge::board::{Entry, Seen, BOARD_REPLY_ROUNDS};

    use super::*;

    // a batch comes back from its wire form as it went in
    #[test]
    fn batch_round_trip() {
        let leaf = SubLeafProof {
            sub_leaf: vec![0x5A; 100],
            sub_proof: vec![Hash::from([0x22; 32]); 16],
        };
        let batch = Batch {
            epoch: EpochNumber(12),
            round: RoundNumber(40),
            proofs: vec![
                SpoolProof {
                    spool: SpoolIndex(64),
                    track: Address::from([7u8; 32]),
                    proof: Proof::Coded(leaf),
                },
                SpoolProof {
                    spool: SpoolIndex(84),
                    track: Address::from([8u8; 32]),
                    proof: Proof::Inline(vec![1, 2, 3]),
                },
            ],
        };

        let req = to_req(batch.clone());
        let request = ChallengeRequest { epoch: req.epoch, round: req.round, proofs: req.proofs };
        let bytes = wincode::serialize(&request).expect("request encodes");
        let decoded: ChallengeRequest = wincode::deserialize(&bytes).expect("request decodes");

        assert_eq!(from_request(decoded), batch);
    }

    // the board answer survives the wire, a negative time and an empty entry included
    #[test]
    fn board_round_trip() {
        let entries = vec![
            Entry { spool: SpoolIndex(60), seen: Some(Seen { outcome: Outcome::WrongTrack, ms: -12 }) },
            Entry { spool: SpoolIndex(69), seen: None },
        ];
        let sat_out = BoardRound { slot: SlotNumber(1_200), opened: None, entries };

        let board = to_board(true, vec![sat_out]);
        let bytes = wincode::serialize(&board).expect("board encodes");
        let decoded: ChallengeBoard = wincode::deserialize(&bytes).expect("board decodes");

        assert_eq!(decoded, board);
        assert!(!decoded.rounds[0].is_observed);
        assert_eq!(decoded.rounds[0].entries[0].seen.map(|seen| seen.ms), Some(-12));
        assert_eq!(decoded.rounds[0].entries[1].seen, None);
    }

    // a full reply from a node in four groups stays well under a megabyte
    #[test]
    fn board_size() {
        let peers = 76;
        let mut rounds = Vec::new();
        for round in 0..BOARD_REPLY_ROUNDS as u64 {
            let mut entries = Vec::new();
            for spool in 0..peers {
                let seen = Some(Seen { outcome: Outcome::Ok, ms: 38 });
                entries.push(Entry { spool: SpoolIndex(spool), seen });
            }
            rounds.push(BoardRound { slot: SlotNumber(1_000 + round * 5), opened: None, entries });
        }

        let bytes = wincode::serialize(&to_board(true, rounds)).expect("board encodes");

        assert!(bytes.len() < 512 * 1024, "{} bytes", bytes.len());
    }
}
