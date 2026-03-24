use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer as _;
use tape_core::bls::BlsPrivateKey;
use tape_sdk::{load_bls_keypair, load_solana_keypair};
use tokio::process::{Child, Command};
use tracing::info;

use crate::observer::NodeRef;

#[derive(Debug)]
pub enum RemoveNodeError {
    NotFound,
    AlreadyStopped,
    StopFailed(anyhow::Error),
}

impl std::fmt::Display for RemoveNodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "node not found"),
            Self::AlreadyStopped => write!(f, "node already stopped"),
            Self::StopFailed(e) => write!(f, "stop failed: {e:#}"),
        }
    }
}

pub struct NodeHandle {
    pub id: usize,
    pub port: u16,
    pub authority: Keypair,
    pub bls_keypair: BlsPrivateKey,
    pub config_path: PathBuf,
    pub data_dir: PathBuf,
    child: Option<Child>,
}

pub struct ProcessSupervisor {
    node_binary: PathBuf,
    data_root: PathBuf,
    rpc_url: String,
    base_port: u16,
    nodes: Vec<NodeHandle>,
}

impl ProcessSupervisor {
    pub fn new(
        node_binary: PathBuf,
        data_root: PathBuf,
        rpc_url: String,
        base_port: u16,
    ) -> Self {
        Self {
            node_binary,
            data_root,
            rpc_url,
            base_port,
            nodes: Vec::new(),
        }
    }

    pub fn node(&self, id: usize) -> &NodeHandle {
        &self.nodes[id]
    }

    pub fn load_existing_nodes(&mut self) -> Result<usize> {
        if !self.data_root.exists() {
            return Ok(0);
        }

        let mut node_dirs = std::fs::read_dir(&self.data_root)
            .with_context(|| format!("read prodnet data dir: {}", self.data_root.display()))?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let file_type = entry.file_type().ok()?;
                if !file_type.is_dir() {
                    return None;
                }

                let name = entry.file_name();
                let name = name.to_str()?;
                let id = name.strip_prefix("node-")?.parse::<usize>().ok()?;
                Some((id, entry.path()))
            })
            .collect::<Vec<_>>();

        node_dirs.sort_by_key(|(id, _)| *id);

        let mut nodes = Vec::with_capacity(node_dirs.len());
        for (expected_id, (id, node_dir)) in node_dirs.into_iter().enumerate() {
            if id != expected_id {
                anyhow::bail!(
                    "node directories must be contiguous starting at node-0; expected node-{expected_id}, found node-{id}"
                );
            }

            nodes.push(self.load_node_handle(id, &node_dir)?);
        }

        self.nodes = nodes;
        Ok(self.nodes.len())
    }

    pub fn node_refs(&self) -> Vec<NodeRef> {
        self.nodes
            .iter()
            .map(|h| NodeRef {
                id: h.id,
                port: h.port,
                authority: h.authority.pubkey(),
            })
            .collect()
    }

    pub fn running_node_count(&self) -> usize {
        self.nodes.iter().filter(|handle| handle.child.is_some()).count()
    }

    pub fn first_stopped_node_id(&self) -> Option<usize> {
        self.nodes
            .iter()
            .find(|handle| handle.child.is_none())
            .map(|handle| handle.id)
    }

    pub fn stopped_node_ids(&self) -> Vec<usize> {
        self.nodes
            .iter()
            .filter(|handle| handle.child.is_none())
            .map(|handle| handle.id)
            .collect()
    }

    pub fn last_running_node_id(&self) -> Option<usize> {
        self.nodes
            .iter()
            .rev()
            .find(|handle| handle.child.is_some())
            .map(|handle| handle.id)
    }

    pub fn prepare_node(&mut self) -> Result<usize> {
        let id = self.nodes.len();
        let port = self.base_port + id as u16;
        let node_dir = self.data_root.join(format!("node-{id}"));
        if node_dir.exists() {
            anyhow::bail!(
                "node directory already exists for node {id}: {}",
                node_dir.display()
            );
        }
        std::fs::create_dir_all(&node_dir)
            .with_context(|| format!("create node dir: {}", node_dir.display()))?;

        let authority = Keypair::new();
        let bls_keypair = BlsPrivateKey::from_random();
        let tls_keypair = Keypair::new();

        let keypair_path = node_dir.join("id.json");
        write_solana_keypair(&keypair_path, &authority)?;

        let bls_path = node_dir.join("bls.key");
        write_bls_keypair(&bls_path, &bls_keypair)?;

        let tls_path = node_dir.join("tls.key");
        write_solana_keypair(&tls_path, &tls_keypair)?;

        let store_path = node_dir.join("data");
        let config_path = node_dir.join("config.yaml");
        let yaml = build_node_yaml(
            id,
            port,
            &keypair_path,
            &bls_path,
            &tls_path,
            &store_path,
            &self.rpc_url,
        );
        std::fs::write(&config_path, yaml)
            .with_context(|| format!("write config: {}", config_path.display()))?;

        self.nodes.push(NodeHandle {
            id,
            port,
            authority,
            bls_keypair,
            config_path,
            data_dir: node_dir,
            child: None,
        });

        Ok(id)
    }

    pub fn spawn_node(&mut self, id: usize) -> Result<()> {
        let handle = &mut self.nodes[id];
        if handle.child.is_some() {
            return Ok(());
        }
        let log_path = handle.data_dir.join("node.log");
        let stdout = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("open node log file: {}", log_path.display()))?;
        let stderr = stdout
            .try_clone()
            .with_context(|| format!("clone node log handle: {}", log_path.display()))?;

        let child = Command::new(&self.node_binary)
            .arg("--config")
            .arg(&handle.config_path)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .with_context(|| {
                format!(
                    "spawn tape-node2 for node {id}: {}",
                    self.node_binary.display()
                )
            })?;

        handle.child = Some(child);
        Ok(())
    }

    pub async fn stop_node(&mut self, id: usize) -> Result<()> {
        let handle = &mut self.nodes[id];
        let mut child = match handle.child.take() {
            Some(c) => c,
            None => return Ok(()),
        };

        let pid = child.id().context("child has no pid")?;

        // Send SIGTERM for graceful shutdown
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGTERM,
        )
        .with_context(|| format!("SIGTERM node {id}"))?;

        // Wait up to 10s for clean exit, then force kill
        match tokio::time::timeout(Duration::from_secs(10), child.wait()).await {
            Ok(Ok(status)) => {
                info!(id, ?status, "node exited");
                Ok(())
            }
            Ok(Err(e)) => {
                Err(e).with_context(|| format!("waiting for node {id} exit"))
            }
            Err(_) => {
                info!(id, "node did not exit in 10s, sending SIGKILL");
                child
                    .kill()
                    .await
                    .with_context(|| format!("SIGKILL node {id}"))?;
                child
                    .wait()
                    .await
                    .with_context(|| format!("wait after SIGKILL node {id}"))?;
                Ok(())
            }
        }
    }

    pub async fn health_check(&self, id: usize) -> bool {
        let port = self.nodes[id].port;
        let url = format!("http://127.0.0.1:{port}/v1/health");

        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
        {
            Ok(c) => c,
            Err(_) => return false,
        };

        match client.get(&url).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }

    pub async fn wait_healthy(&self, id: usize, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if self.health_check(id).await {
                info!(id, elapsed_ms = start.elapsed().as_millis(), "node healthy");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        anyhow::bail!(
            "node {id} did not become healthy within {}s",
            timeout.as_secs()
        );
    }

    pub async fn remove_node(&mut self, id: usize) -> Result<(), RemoveNodeError> {
        if id >= self.nodes.len() {
            return Err(RemoveNodeError::NotFound);
        }
        if self.nodes[id].child.is_none() {
            return Err(RemoveNodeError::AlreadyStopped);
        }
        self.stop_node(id).await.map_err(RemoveNodeError::StopFailed)
    }

    pub async fn shutdown_all(&mut self) -> Result<()> {
        let mut first_error: Option<anyhow::Error> = None;
        for id in (0..self.nodes.len()).rev() {
            if let Err(e) = self.stop_node(id).await {
                info!(id, error = %e, "error stopping node");
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        match first_error {
            Some(e) => Err(e).context("shutdown_all had failures"),
            None => Ok(()),
        }
    }

    fn load_node_handle(&self, id: usize, node_dir: &Path) -> Result<NodeHandle> {
        let keypair_path = node_dir.join("id.json");
        let bls_path = node_dir.join("bls.key");
        let config_path = node_dir.join("config.yaml");

        let authority = load_solana_keypair(&keypair_path)?;
        let bls_keypair = load_bls_keypair(&bls_path)?;

        if !config_path.exists() {
            anyhow::bail!("missing config file: {}", config_path.display());
        }

        Ok(NodeHandle {
            id,
            port: self.base_port + id as u16,
            authority,
            bls_keypair,
            config_path,
            data_dir: node_dir.to_path_buf(),
            child: None,
        })
    }
}

pub(crate) fn write_solana_keypair(path: &Path, keypair: &Keypair) -> Result<()> {
    let bytes = keypair.to_bytes().to_vec();
    let json = serde_json::to_vec(&bytes).context("serialize keypair")?;
    std::fs::write(path, json).with_context(|| format!("write keypair: {}", path.display()))
}

fn write_bls_keypair(path: &Path, key: &BlsPrivateKey) -> Result<()> {
    let bytes = key.0 .0.to_vec();
    let json = serde_json::to_vec(&bytes).context("serialize bls keypair")?;
    std::fs::write(path, json).with_context(|| format!("write bls keypair: {}", path.display()))
}

fn build_node_yaml(
    id: usize,
    port: u16,
    keypair_path: &Path,
    bls_path: &Path,
    tls_path: &Path,
    store_path: &Path,
    rpc_url: &str,
) -> String {
    format!(
        r#"node:
  name: "prodnet-node-{id}"
  node_keypair: "{keypair}"
  bls_keypair: "{bls}"
  commission: 0

solana:
  rpc: "{rpc_url}"

network:
  host: "127.0.0.1"
  port: {port}

http:
  listen: "127.0.0.1:{port}"

store:
  path: "{store}"

metrics:
  enabled: true

logging:
  filter: "info"
  format: compact

tls:
  identity_keypair: "{tls}"
  self_signed: true
"#,
        keypair = keypair_path.display(),
        bls = bls_path.display(),
        store = store_path.display(),
        tls = tls_path.display(),
    )
}
