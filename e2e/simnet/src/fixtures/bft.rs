use std::time::Duration;

use anyhow::Result;
use tape_api::program::tapedrive::MIN_COMMITTEE_SIZE;
use tape_core::bft::{max_faulty, min_correct};

use crate::scenario::SimnetScenario;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BftTargets {
    pub total_nodes: usize,
    pub max_faulty: usize,
    pub min_correct: usize,
    pub min_for_advance: usize,
}

impl SimnetScenario<'_> {
    pub fn bft_targets(&self) -> BftTargets {
        let total_nodes = self.harness.nodes().len();
        let max_faulty = max_faulty(total_nodes as u64) as usize;
        let min_correct = min_correct(total_nodes as u64) as usize;
        let min_for_advance = min_correct.max(MIN_COMMITTEE_SIZE);

        BftTargets {
            total_nodes,
            max_faulty,
            min_correct,
            min_for_advance,
        }
    }

    pub fn honest_nodes(&self) -> Vec<usize> {
        let targets = self.bft_targets();
        (0..targets.min_for_advance.min(targets.total_nodes)).collect()
    }

    pub async fn wait_next_bft(&self, timeout: Duration) -> Result<()> {
        let targets = self.bft_targets();
        self.wait_next_quorum(targets.min_for_advance, timeout).await
    }
}
