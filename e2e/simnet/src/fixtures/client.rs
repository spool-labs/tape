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

    /// Build an HTTPS node client pinned to the target node authority key.
    pub fn build_client_https_pinned(&self, target_index: usize) -> Result<NodeClient> {
        let address = self.node_endpoint(target_index)?;
        let target = self
            .harness
            .node(target_index)
            .with_context(|| format!("node {target_index} missing"))?;

        NodeClientBuilder::new()
            .server_tls_key(target.authority().to_bytes())
            .build(&address)
            .context("build pinned https node client")
    }

    /// Build an mTLS + pinned client from source to target node.
    pub fn build_client_mtls(
        &self,
        source_index: usize,
        target_index: usize,
    ) -> Result<NodeClient> {
        let address = self.node_endpoint(target_index)?;
        let source = self
            .harness
            .node(source_index)
            .with_context(|| format!("node {source_index} missing"))?;
        let target = self
            .harness
            .node(target_index)
            .with_context(|| format!("node {target_index} missing"))?;

        NodeClientBuilder::new()
            .server_tls_key(target.authority().to_bytes())
            .with_client_paths(
                Some(source.tls_cert_path()),
                Some(source.tls_key_path()),
            )
            .build(&address)
            .context("build mtls node client")
    }
}
