//! DigitalOcean v2 API client, narrow to the subset tape-network uses.
//!
//! Endpoints we hit: create/get/list/delete droplet, list/create SSH key.
//! Auth: bearer token. All instances we manage are tagged with the testbed id.

use std::process::Command as StdCommand;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use tokio::time::{Duration, sleep};
use tracing::{debug, info};

use super::{CloudProvider, Instance};
use crate::settings::Settings;

const API_BASE: &str = "https://api.digitalocean.com/v2";

pub struct DigitalOcean {
    settings: Settings,
    http: Client,
    ssh_pubkey: String,
}

impl DigitalOcean {
    pub fn new(settings: Settings) -> Result<Self> {
        let ssh_pubkey = std::fs::read_to_string(&settings.cloud.ssh_key_file)
            .with_context(|| {
                format!(
                    "reading ssh public key {}",
                    settings.cloud.ssh_key_file.display()
                )
            })?;
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("building http client")?;
        Ok(Self {
            settings,
            http,
            ssh_pubkey,
        })
    }

    fn token(&self) -> &str {
        self.settings.cloud.token.trim()
    }

    fn tag(&self) -> &str {
        &self.settings.testbed_id
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        let url = format!("{API_BASE}{path}");
        let resp = self
            .http
            .get(&url)
            .bearer_auth(self.token())
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("DO GET {url} -> {status}: {text}");
        }
        serde_json::from_str(&text)
            .with_context(|| format!("parsing DO response from {url}: {text}"))
    }

    async fn post<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<T> {
        let url = format!("{API_BASE}{path}");
        let resp = self
            .http
            .post(&url)
            .bearer_auth(self.token())
            .json(&body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("DO POST {url} -> {status}: {text}");
        }
        serde_json::from_str(&text)
            .with_context(|| format!("parsing DO response from {url}: {text}"))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let url = format!("{API_BASE}{path}");
        let resp = self
            .http
            .delete(&url)
            .bearer_auth(self.token())
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            bail!("DO DELETE {url} -> {status}: {text}");
        }
        Ok(())
    }

    /// Shared droplet-create helper used by both `create_instances` and
    /// `create_one`.
    async fn create_droplet(
        &self,
        name: &str,
        tag: &str,
        size_override: Option<&str>,
        fingerprint: &str,
    ) -> Result<Instance> {
        let size = size_override.unwrap_or(&self.settings.cloud.size);
        let body = json!({
            "name": name,
            "region": self.settings.cloud.region,
            "size": size,
            "image": self.settings.cloud.image,
            "ssh_keys": [fingerprint],
            "tags": [tag],
            "ipv6": false,
            "monitoring": false,
            "with_droplet_agent": false,
        });
        info!(name = %name, size = %size, tag = %tag, "creating droplet");
        #[derive(Deserialize)]
        struct CreateResp {
            droplet: Droplet,
        }
        let resp: CreateResp = self.post("/droplets", body).await?;
        Ok(resp.droplet.into_instance())
    }

    /// Compute the MD5 fingerprint DO uses to identify SSH keys. Shells out to
    /// `ssh-keygen -l -E md5 -f <pubkey>`.
    fn ssh_key_fingerprint(&self) -> Result<String> {
        let output = StdCommand::new("ssh-keygen")
            .args(["-l", "-E", "md5", "-f"])
            .arg(&self.settings.cloud.ssh_key_file)
            .output()
            .context("running ssh-keygen")?;
        if !output.status.success() {
            bail!(
                "ssh-keygen failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let fp = stdout
            .split_whitespace()
            .find_map(|tok| tok.strip_prefix("MD5:"))
            .ok_or_else(|| anyhow!("no MD5 fingerprint in ssh-keygen output: {stdout}"))?;
        Ok(fp.to_string())
    }
}

#[async_trait]
impl CloudProvider for DigitalOcean {
    async fn create_instances(&self, count: u32) -> Result<Vec<Instance>> {
        let fingerprint = self.ensure_ssh_key().await?;
        let mut created = Vec::with_capacity(count as usize);
        for i in 0..count {
            let name = format!("{}-node-{i}", self.tag());
            let inst = self
                .create_droplet(&name, self.tag(), None, &fingerprint)
                .await?;
            created.push(inst);
        }
        Ok(created)
    }

    async fn create_one(
        &self,
        name: &str,
        tag: &str,
        size_override: Option<&str>,
    ) -> Result<Instance> {
        let fingerprint = self.ensure_ssh_key().await?;
        self.create_droplet(name, tag, size_override, &fingerprint)
            .await
    }

    async fn get_instance(&self, provider_id: &str) -> Result<Instance> {
        #[derive(Deserialize)]
        struct GetResp {
            droplet: Droplet,
        }
        let resp: GetResp = self.get(&format!("/droplets/{provider_id}")).await?;
        Ok(resp.droplet.into_instance())
    }

    async fn list_instances(&self) -> Result<Vec<Instance>> {
        self.list_instances_by_tag(self.tag()).await
    }

    async fn list_instances_by_tag(&self, tag: &str) -> Result<Vec<Instance>> {
        #[derive(Deserialize)]
        struct ListResp {
            droplets: Vec<Droplet>,
        }
        let resp: ListResp = self
            .get(&format!("/droplets?tag_name={}", tag))
            .await?;
        Ok(resp.droplets.into_iter().map(Droplet::into_instance).collect())
    }

    async fn delete_instance(&self, provider_id: &str) -> Result<()> {
        self.delete(&format!("/droplets/{provider_id}")).await
    }

    async fn ensure_ssh_key(&self) -> Result<String> {
        let fingerprint = self.ssh_key_fingerprint()?;
        debug!(%fingerprint, "checking ssh key registration");

        // Look up by fingerprint; 200 if present, 404 if not.
        let url = format!("{API_BASE}/account/keys/{fingerprint}");
        let resp = self
            .http
            .get(&url)
            .bearer_auth(self.token())
            .send()
            .await?;
        if resp.status().is_success() {
            return Ok(fingerprint);
        }
        if resp.status() != reqwest::StatusCode::NOT_FOUND {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            bail!("DO GET /account/keys/{fingerprint} -> {status}: {text}");
        }

        // Not registered — upload it.
        info!("registering ssh public key with DigitalOcean");
        let body = json!({
            "name": format!("tape-network-{}", self.tag()),
            "public_key": self.ssh_pubkey.trim(),
        });
        #[derive(Deserialize)]
        struct CreateResp {
            ssh_key: SshKey,
        }
        #[derive(Deserialize)]
        struct SshKey {
            fingerprint: String,
        }
        let created: CreateResp = self.post("/account/keys", body).await?;
        Ok(created.ssh_key.fingerprint)
    }
}

#[derive(Deserialize)]
struct Droplet {
    id: u64,
    name: String,
    status: String,
    region: DropletRegion,
    #[serde(default)]
    networks: DropletNetworks,
}

#[derive(Deserialize)]
struct DropletRegion {
    slug: String,
}

#[derive(Default, Deserialize)]
struct DropletNetworks {
    #[serde(default)]
    v4: Vec<V4Network>,
}

#[derive(Deserialize)]
struct V4Network {
    ip_address: String,
    #[serde(rename = "type")]
    kind: String,
}

impl Droplet {
    fn into_instance(self) -> Instance {
        let public_ip = self
            .networks
            .v4
            .into_iter()
            .find(|n| n.kind == "public")
            .map(|n| n.ip_address);
        Instance {
            provider_id: self.id.to_string(),
            name: self.name,
            public_ip,
            region: self.region.slug,
            status: self.status,
        }
    }
}

/// Poll `get_instance` on each provider_id until `status == "active"` and a
/// public IPv4 is assigned. Returns the updated list.
pub async fn wait_until_ready(
    provider: &dyn CloudProvider,
    ids: &[String],
    timeout: Duration,
) -> Result<Vec<Instance>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut out = Vec::with_capacity(ids.len());
        let mut ready = true;
        for id in ids {
            let inst = provider.get_instance(id).await?;
            if inst.status != "active" || inst.public_ip.is_none() {
                ready = false;
            }
            out.push(inst);
        }
        if ready {
            return Ok(out);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "timed out waiting for droplets to become ready; current state: {:?}",
                out
            );
        }
        sleep(Duration::from_secs(5)).await;
    }
}
