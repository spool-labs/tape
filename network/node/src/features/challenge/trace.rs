//! When each round's evidence reached this node.
//!
//! The peer record says whether a round certified. This says when: when the
//! round opened on its entropy block, when each answer and attestation arrived,
//! and when a certificate assembled. Every stamp is an arrival here, never a
//! peer's send, which is the only claim one vantage can make honestly.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use tape_core::types::{EpochNumber, GroupIndex, RoundNumber, SlotNumber, SpoolIndex};
use tape_crypto::Address;
use tape_crypto::hash::Hash;

#[cfg(feature = "metrics")]
use crate::observe::{board, stream};

/// Rounds the ring keeps, counting distinct rounds rather than entries so a
/// node in several groups holds the same span of time as one in a single group.
pub const TRACED_ROUNDS: usize = 16;

/// Traces held at once, bounding a node that holds spools in many groups.
const MAX_TRACES: usize = 128;

/// Marks one trace collects before it stops taking them, so a peer replaying
/// attestations cannot grow the ring without bound.
const MAX_MARKS: usize = 512;

/// How often a round in flight is sent out.
///
/// Every mark carries the whole trace, and a full group's round is hundreds of
/// them, so pushing each one would send the round's own size squared. Coalescing
/// costs nothing a reader can see: a quarter second is under one slot, and an
/// outcome goes out the moment it lands whatever the interval says.
const PUSH_INTERVAL_MS: u64 = 250;

/// What one mark records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkKind {
    /// This node built and broadcast its own answer.
    AnswerOut,
    /// A peer's answer arrived and verified.
    AnswerIn,
    /// An answer was turned away at the door.
    AnswerRefused,
    /// This node signed an attestation.
    AttestOut,
    /// A peer's attestation arrived.
    AttestIn,
    /// A certificate assembled or arrived for the spool.
    Certified,
}

/// One thing that happened to one spool in one round.
#[derive(Clone, Debug)]
pub struct TraceMark {
    pub spool: SpoolIndex,
    /// The answering owner, or the signer for an attestation.
    pub peer: Option<Address>,
    pub kind: MarkKind,
    pub at_ms: u64,
}

/// How one spool's round settled.
///
/// Kept apart from the marks: settlement runs when the next round opens, a
/// whole cadence after this round ended, so it is a verdict rather than a
/// moment on this round's clock.
#[derive(Clone, Debug)]
pub struct SpoolOutcome {
    pub spool: SpoolIndex,
    pub owner: Address,
    pub certified: bool,
}

/// How a round ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TraceClose {
    /// Still taking marks.
    #[default]
    Open,
    /// Settled against every spool in the group.
    Settled,
    /// Charged to nobody: the entropy block never finalized.
    Unfinalized,
    /// Charged to nobody: the group had nothing to be asked about.
    Nothing,
}

/// One group's round, as this node saw it unfold.
#[derive(Clone, Debug)]
pub struct RoundTrace {
    pub epoch: EpochNumber,
    pub round: RoundNumber,
    pub group: GroupIndex,
    pub anchor_slot: SlotNumber,
    pub block: Hash,
    pub opened_ms: u64,
    pub close: TraceClose,
    pub marks: Vec<TraceMark>,
    pub outcomes: Vec<SpoolOutcome>,
    /// When this trace was last sent out, for the push interval.
    pushed_ms: u64,
}

impl RoundTrace {
    fn matches(&self, epoch: EpochNumber, round: RoundNumber, group: GroupIndex) -> bool {
        self.epoch == epoch && self.round == round && self.group == group
    }
}

/// The ring of recent traces, newest last.
#[derive(Default)]
pub struct TraceRing {
    traces: Mutex<VecDeque<RoundTrace>>,
}

impl TraceRing {
    /// Starts a trace for a round this node just opened.
    pub fn open(
        &self,
        epoch: EpochNumber,
        round: RoundNumber,
        group: GroupIndex,
        anchor_slot: SlotNumber,
        block: Hash,
    ) {
        let mut traces = self.traces.lock().expect("trace ring");
        if traces.iter().any(|trace| trace.matches(epoch, round, group)) {
            return;
        }

        traces.push_back(RoundTrace {
            epoch,
            round,
            group,
            anchor_slot,
            block,
            opened_ms: now_ms(),
            close: TraceClose::Open,
            marks: Vec::new(),
            outcomes: Vec::new(),
            pushed_ms: 0,
        });
        retire(&mut traces);
        push(traces.back());
    }

    /// Records one mark against an open round, ignoring one this node never saw
    /// open: a mark with no round to hang on has no place on the timeline.
    pub fn mark(
        &self,
        epoch: EpochNumber,
        round: RoundNumber,
        group: GroupIndex,
        spool: SpoolIndex,
        kind: MarkKind,
        peer: Option<Address>,
    ) {
        let mut traces = self.traces.lock().expect("trace ring");
        let Some(trace) = traces.iter_mut().find(|trace| trace.matches(epoch, round, group))
        else {
            return;
        };
        // At the cap, the oldest vote gives way rather than the newest mark being
        // refused: a round's tail is its certificate, which is the evidence a
        // reader is waiting on, and its middle is one attestation among twenty.
        if trace.marks.len() >= MAX_MARKS {
            let oldest = trace
                .marks
                .iter()
                .position(|mark| mark.kind == MarkKind::AttestIn);
            match oldest {
                Some(index) => {
                    trace.marks.remove(index);
                }
                None => return,
            }
        }

        let at_ms = now_ms();
        trace.marks.push(TraceMark { spool, peer, kind, at_ms });

        // A certificate is worth sending promptly, but a group settling fires
        // twenty of them at once and each carries the whole trace, so they share
        // a shorter interval rather than bypassing it and flooding the backlog.
        let interval = if kind == MarkKind::Certified {
            PUSH_INTERVAL_MS / 4
        } else {
            PUSH_INTERVAL_MS
        };
        if at_ms.saturating_sub(trace.pushed_ms) >= interval {
            trace.pushed_ms = at_ms;
            push(Some(&*trace));
        }
    }

    /// Records how one spool's round settled.
    pub fn settle(
        &self,
        epoch: EpochNumber,
        round: RoundNumber,
        group: GroupIndex,
        spool: SpoolIndex,
        owner: Address,
        certified: bool,
    ) {
        let mut traces = self.traces.lock().expect("trace ring");
        let Some(trace) = traces.iter_mut().find(|trace| trace.matches(epoch, round, group))
        else {
            return;
        };
        if trace.outcomes.iter().any(|outcome| outcome.spool == spool) {
            return;
        }

        trace.outcomes.push(SpoolOutcome { spool, owner, certified });
    }

    /// Closes a round, which is what stops the dashboard drawing it as live.
    pub fn close(
        &self,
        epoch: EpochNumber,
        round: RoundNumber,
        group: GroupIndex,
        close: TraceClose,
    ) {
        let mut traces = self.traces.lock().expect("trace ring");
        if let Some(trace) = traces.iter_mut().find(|trace| trace.matches(epoch, round, group)) {
            trace.close = close;
            trace.pushed_ms = now_ms();
            push(Some(&*trace));
        }
    }

    /// Every trace held, oldest first.
    pub fn snapshot(&self) -> Vec<RoundTrace> {
        let traces = self.traces.lock().expect("trace ring");
        traces.iter().cloned().collect()
    }

}

/// Drops traces behind the newest rounds, then behind the entry cap.
fn retire(traces: &mut VecDeque<RoundTrace>) {
    let mut rounds: Vec<(EpochNumber, RoundNumber)> =
        traces.iter().map(|trace| (trace.epoch, trace.round)).collect();
    rounds.sort_unstable();
    rounds.dedup();
    if rounds.len() > TRACED_ROUNDS {
        let cutoff = rounds[rounds.len() - TRACED_ROUNDS];
        traces.retain(|trace| (trace.epoch, trace.round) >= cutoff);
    }
    while traces.len() > MAX_TRACES {
        traces.pop_front();
    }
}

/// Sends the trace to any dashboard watching, and builds nothing when none is.
#[cfg(feature = "metrics")]
fn push(trace: Option<&RoundTrace>) {
    if !stream::watching() {
        return;
    }
    if let Some(trace) = trace {
        stream::push_round(&board::wire_trace(trace));
    }
}

/// Without the observe surface there is nobody to send a trace to.
#[cfg(not(feature = "metrics"))]
fn push(_trace: Option<&RoundTrace>) {}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opened(ring: &TraceRing, round: u64, group: u64) {
        ring.open(
            EpochNumber(1),
            RoundNumber(round),
            GroupIndex(group),
            SlotNumber(round * 150),
            Hash::default(),
        );
    }

    #[test]
    fn marks_land_on_their_round() {
        let ring = TraceRing::default();
        opened(&ring, 1, 0);
        ring.mark(
            EpochNumber(1),
            RoundNumber(1),
            GroupIndex(0),
            SpoolIndex(3),
            MarkKind::AnswerIn,
            None,
        );

        let traces = ring.snapshot();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].marks.len(), 1);
        assert_eq!(traces[0].marks[0].spool, SpoolIndex(3));
    }

    #[test]
    fn a_mark_without_a_round_is_dropped() {
        let ring = TraceRing::default();
        ring.mark(
            EpochNumber(1),
            RoundNumber(9),
            GroupIndex(0),
            SpoolIndex(3),
            MarkKind::AnswerIn,
            None,
        );

        assert!(ring.snapshot().is_empty());
    }

    #[test]
    fn opening_twice_keeps_the_first_trace() {
        let ring = TraceRing::default();
        opened(&ring, 1, 0);
        ring.mark(
            EpochNumber(1),
            RoundNumber(1),
            GroupIndex(0),
            SpoolIndex(3),
            MarkKind::AnswerOut,
            None,
        );
        opened(&ring, 1, 0);

        let traces = ring.snapshot();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].marks.len(), 1);
    }

    #[test]
    fn groups_of_one_round_retire_together() {
        let ring = TraceRing::default();
        for round in 0..(TRACED_ROUNDS as u64 + 4) {
            opened(&ring, round, 0);
            opened(&ring, round, 1);
        }

        let traces = ring.snapshot();
        let rounds: std::collections::BTreeSet<u64> =
            traces.iter().map(|trace| trace.round.0).collect();
        assert_eq!(rounds.len(), TRACED_ROUNDS);
        assert_eq!(traces.len(), TRACED_ROUNDS * 2);
        assert_eq!(*rounds.iter().next().expect("oldest"), 4);
    }

    #[test]
    fn the_entry_cap_holds() {
        let ring = TraceRing::default();
        for group in 0..(MAX_TRACES as u64 + 8) {
            opened(&ring, 1, group);
        }

        assert_eq!(ring.snapshot().len(), MAX_TRACES);
    }

    #[test]
    fn marks_stop_at_the_cap() {
        let ring = TraceRing::default();
        opened(&ring, 1, 0);
        for _ in 0..(MAX_MARKS + 10) {
            ring.mark(
                EpochNumber(1),
                RoundNumber(1),
                GroupIndex(0),
                SpoolIndex(0),
                MarkKind::AttestIn,
                None,
            );
        }

        assert_eq!(ring.snapshot()[0].marks.len(), MAX_MARKS);
    }

    #[test]
    fn closing_marks_the_round() {
        let ring = TraceRing::default();
        opened(&ring, 1, 0);
        ring.close(EpochNumber(1), RoundNumber(1), GroupIndex(0), TraceClose::Unfinalized);

        assert_eq!(ring.snapshot()[0].close, TraceClose::Unfinalized);
    }
}
