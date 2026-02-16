use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tape_api::prelude::{Archive, Epoch, System};

use crate::harness::fixture::network::SimNet;
use crate::harness::log::append_log;

impl SimNet {
    pub async fn read_system(&self) -> Result<System> {
        self.client.get_system().await.context("read system")
    }

    pub async fn read_epoch(&self) -> Result<Epoch> {
        self.client.get_epoch().await.context("read epoch")
    }

    pub async fn read_archive(&self) -> Result<Archive> {
        self.client.get_archive().await.context("read archive")
    }

    pub async fn committee_size(&self) -> Result<usize> {
        Ok(self.read_system().await?.committee.size())
    }

    pub async fn committee_next_size(&self) -> Result<usize> {
        Ok(self.read_system().await?.committee_next.size())
    }

    pub async fn is_bootstrap_mode(&self) -> Result<bool> {
        Ok(self.read_system().await?.committee_prev_empty())
    }

    pub async fn would_block_advance(&self) -> Result<bool> {
        Ok(self.read_system().await?.will_be_low_quorum())
    }

    pub async fn is_low_quorum(&self) -> Result<bool> {
        Ok(self.read_system().await?.is_low_quorum())
    }

    pub async fn wait_quorum(&self, min_size: usize, timeout: Duration) -> Result<()> {
        append_log(&format!("wait quorum start min={min_size} timeout_ms={}", timeout.as_millis()));
        let start = Instant::now();
        loop {
            let size = self.committee_size().await?;
            if size >= min_size {
                append_log(&format!("wait quorum done size={size}"));
                return Ok(());
            }
            if start.elapsed() >= timeout {
                append_log(&format!("wait quorum fail size={size}"));
                bail!("timed out waiting for committee size >= {min_size}, got {size}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn wait_next_quorum(&self, min_size: usize, timeout: Duration) -> Result<()> {
        append_log(&format!(
            "wait next quorum start min={min_size} timeout_ms={}",
            timeout.as_millis()
        ));
        let start = Instant::now();
        loop {
            let size = self.committee_next_size().await?;
            if size >= min_size {
                append_log(&format!("wait next quorum done size={size}"));
                return Ok(());
            }
            if start.elapsed() >= timeout {
                append_log(&format!("wait next quorum fail size={size}"));
                bail!("timed out waiting for committee_next size >= {min_size}, got {size}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
