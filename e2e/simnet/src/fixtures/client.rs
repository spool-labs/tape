use anyhow::{Context, Result};
use tape_node_client::{NodeClient, NodeClientBuilder};

use crate::scenario::SimnetScenario;

impl SimnetScenario<'_> {
    fn node_endpoint(&self, index: usize) -> Result<String> {
        let node = self
            .harness
            .node(index)
            .with_context(|| format!("node {index} missing"))?;

        Ok(format!(
            "{}:{}",
            node.context().config.public_host,
            node.context().config.public_port
        ))
    }

    /// Build a plain-HTTP node client for current runtime server mode.
    pub fn build_client_http(&self, target_index: usize) -> Result<NodeClient> {
        let address = self.node_endpoint(target_index)?;
        let url = format!("http://{address}")
            .parse()
            .context("parse node http url")?;

        NodeClientBuilder::new()
            .build_with_url(url)
            .context("build http node client")
    }

    /// Build a node client for the target node.
    /// In simnet we run HTTP-only, so this uses plain HTTP.
    pub fn build_client_https_pinned(&self, target_index: usize) -> Result<NodeClient> {
        self.build_client_http(target_index)
    }

    /// Build a client from source to target node.
    /// In simnet we run HTTP-only, so this uses plain HTTP.
    pub fn build_client_mtls(
        &self,
        _source_index: usize,
        target_index: usize,
    ) -> Result<NodeClient> {
        self.build_client_http(target_index)
    }
}
