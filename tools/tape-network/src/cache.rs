//! `tape-network cache …` — lifecycle of the single RPC cache droplet
//! that sits in front of the committee fleet. Mirrors the shape of the
//! `testnet` command group.
//!
//! Tagging: the cache droplet gets `{testbed_id}-cache`. That's a
//! distinct tag from the committee (`{testbed_id}`) and the builder
//! (`{testbed_id}-builder`), so `testnet destroy` doesn't accidentally
//! nuke it.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use tracing::info;

use crate::cloud::{self, Instance};
use crate::settings::Settings;
use crate::ssh;

const CACHE_TAG_SUFFIX: &str = "-cache";
const WORKING_DIR: &str = "/opt/rpc-cache";
const LOCAL_BINARY_REL: &str = "target/x86_64-unknown-linux-gnu/release/rpc-cache";
const SERVICE_TEMPLATE: &str = include_str!("../assets/rpc-cache.service.tmpl");
const API_KEY_FILE: &str = "cache-api-key";

/// Default cache-droplet size. The proxy is I/O-bound; a small shared-CPU
/// node is plenty for a 20-node committee. Override via settings if you
/// scale past that.
const DEFAULT_CACHE_SIZE: &str = "s-2vcpu-4gb-amd";

/// Listen port the rpc-cache binds on the cache droplet. Committee nodes
/// are configured to hit this port.
pub const CACHE_LISTEN_PORT: u16 = 8899;

#[derive(Serialize)]
pub struct CacheStatus {
    pub present: bool,
    pub provider_id: Option<String>,
    pub public_ip: Option<String>,
    pub url: Option<String>,
}

/// Look up an existing cache droplet. Used by both the `status` subcommand
/// and `bootstrap` (to pick the cache URL when rendering `node.yaml`).
pub async fn discover(settings: &Settings) -> Result<Option<Instance>> {
    let provider = cloud::from_settings(settings)?;
    let tag = cache_tag(settings);
    let existing = provider.list_instances_by_tag(&tag).await?;
    Ok(existing.into_iter().next())
}

/// Render the URL committee nodes use to talk to the cache, with the
/// api_key baked in as a query parameter.
pub fn url_for(instance: &Instance, api_key: &str) -> Option<String> {
    let ip = instance.public_ip.as_deref()?;
    Some(format!("http://{ip}:{CACHE_LISTEN_PORT}?api={api_key}"))
}

/// Provision + install the cache droplet. Idempotent: reuses an existing
/// droplet if present, reinstalls the binary + config every time.
///
/// `size_override` controls the DO droplet size slug used when provisioning
/// a new droplet (`None` falls back to [`DEFAULT_CACHE_SIZE`]). It does not
/// resize an existing droplet — destroy first if you want a different size.
pub async fn deploy(settings: &Settings, size_override: Option<&str>) -> Result<CacheStatus> {
    // Locate the local rpc-cache binary. Build-linux produces it
    // alongside tape-node at target/x86_64-unknown-linux-gnu/release/.
    let source_root = settings_source_root(settings)?;
    let binary = source_root.join(LOCAL_BINARY_REL);
    if !binary.exists() {
        bail!(
            "no rpc-cache binary found at {}; run `tape-network build-linux` first",
            binary.display()
        );
    }

    if settings.solana.endpoint.is_empty() {
        bail!("solana.endpoint must be set before deploying the cache");
    }
    let upstream = settings.solana.upstream_url();
    let upstream_headers = settings.solana.upstream_headers();

    let api_key = load_or_create_api_key(settings)?;

    let instance = find_or_create(settings, size_override).await?;
    let host = instance
        .public_ip
        .as_deref()
        .ok_or_else(|| anyhow!("cache droplet has no public IP"))?;
    let key = &settings.cloud.ssh_private_key_file;

    info!(host = %host, "waiting for ssh");
    ssh::wait_until_reachable(&settings.ssh, key, host, Duration::from_secs(180)).await?;

    info!(host = %host, "preparing working dir");
    let prep = format!(
        "set -e\nmkdir -p {WORKING_DIR}/bin\nsystemctl is-enabled rpc-cache >/dev/null 2>&1 && systemctl stop rpc-cache || true\n"
    );
    ssh::exec(&settings.ssh, key, host, &prep).await?;

    info!(host = %host, "uploading binary");
    ssh::upload(
        &settings.ssh,
        key,
        host,
        &binary,
        &format!("{WORKING_DIR}/bin/rpc-cache"),
    )
    .await?;
    ssh::exec(
        &settings.ssh,
        key,
        host,
        &format!("chmod 0755 {WORKING_DIR}/bin/rpc-cache"),
    )
    .await?;

    info!(host = %host, "writing config");
    let config_yaml = render_config(&upstream, &api_key, &upstream_headers);
    upload_str(
        settings,
        host,
        &config_yaml,
        &format!("{WORKING_DIR}/rpc-cache.yaml"),
    )
    .await?;

    info!(host = %host, "installing systemd unit");
    let unit = SERVICE_TEMPLATE.replace("{{WORKING_DIR}}", WORKING_DIR);
    upload_str(settings, host, &unit, "/etc/systemd/system/rpc-cache.service").await?;
    ssh::exec(
        &settings.ssh,
        key,
        host,
        "systemctl daemon-reload && systemctl enable --now rpc-cache",
    )
    .await?;

    let url = url_for(&instance, &api_key).unwrap_or_default();
    Ok(CacheStatus {
        present: true,
        provider_id: Some(instance.provider_id.clone()),
        public_ip: instance.public_ip.clone(),
        url: Some(url),
    })
}

/// Kill the cache droplet if present.
pub async fn destroy(settings: &Settings) -> Result<()> {
    let provider = cloud::from_settings(settings)?;
    let tag = cache_tag(settings);
    let existing = provider.list_instances_by_tag(&tag).await?;
    if existing.is_empty() {
        println!("no cache droplet for testbed {}", settings.testbed_id);
        return Ok(());
    }
    for inst in &existing {
        info!(name = %inst.name, id = %inst.provider_id, "deleting cache droplet");
        provider.delete_instance(&inst.provider_id).await?;
    }
    println!("deleted {} cache droplet(s)", existing.len());
    Ok(())
}

/// Print current cache-droplet state (or "absent").
pub async fn status(settings: &Settings) -> Result<CacheStatus> {
    match discover(settings).await? {
        Some(inst) => {
            let url = match read_api_key(settings) {
                Ok(Some(key)) => url_for(&inst, &key),
                _ => None,
            };
            Ok(CacheStatus {
                present: true,
                provider_id: Some(inst.provider_id),
                public_ip: inst.public_ip,
                url,
            })
        }
        None => Ok(CacheStatus {
            present: false,
            provider_id: None,
            public_ip: None,
            url: None,
        }),
    }
}

/// Tail the cache's systemd journal.
pub async fn logs(settings: &Settings, tail: usize, follow: bool) -> Result<()> {
    let inst = discover(settings)
        .await?
        .ok_or_else(|| anyhow!("no cache droplet — run `tape-network cache deploy` first"))?;
    let host = inst
        .public_ip
        .as_deref()
        .ok_or_else(|| anyhow!("cache droplet has no public IP"))?;
    let key = &settings.cloud.ssh_private_key_file;

    if follow {
        let args = vec![
            "journalctl".into(),
            "-u".into(),
            "rpc-cache".into(),
            "--no-pager".into(),
            "-f".into(),
            format!("-n{tail}"),
        ];
        ssh::interactive(&settings.ssh, key, host, &args).await
    } else {
        let cmd = format!("journalctl -u rpc-cache --no-pager -n{tail}");
        let out = ssh::exec(&settings.ssh, key, host, &cmd).await?;
        print!("{out}");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn cache_tag(settings: &Settings) -> String {
    format!("{}{}", settings.testbed_id, CACHE_TAG_SUFFIX)
}

fn settings_source_root(settings: &Settings) -> Result<std::path::PathBuf> {
    match &settings.build.source {
        crate::settings::BuildSource::Local { path } => Ok(path.clone()),
        crate::settings::BuildSource::Git { .. } => {
            bail!("cache deploy only supports build.source.kind = local right now")
        }
    }
}

async fn find_or_create(settings: &Settings, size_override: Option<&str>) -> Result<Instance> {
    let provider = cloud::from_settings(settings)?;
    let tag = cache_tag(settings);
    if let Some(inst) = provider
        .list_instances_by_tag(&tag)
        .await?
        .into_iter()
        .next()
    {
        info!(
            id = %inst.provider_id,
            ip = ?inst.public_ip,
            "reusing existing cache droplet"
        );
        if inst.public_ip.is_some() {
            return Ok(inst);
        }
        return cloud::digitalocean::wait_until_ready(
            provider.as_ref(),
            &[inst.provider_id],
            Duration::from_secs(300),
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("cache droplet failed to become ready"));
    }

    let name = format!("{}{}", settings.testbed_id, CACHE_TAG_SUFFIX);
    let size = size_override.unwrap_or(DEFAULT_CACHE_SIZE);
    info!(tag = %tag, size = %size, "provisioning new cache droplet");
    let created = provider.create_one(&name, &tag, Some(size)).await?;
    let ready = cloud::digitalocean::wait_until_ready(
        provider.as_ref(),
        &[created.provider_id.clone()],
        Duration::from_secs(600),
    )
    .await?;
    ready
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("cache droplet never became ready"))
}

fn render_config(
    upstream: &str,
    api_key: &str,
    upstream_headers: &[(String, String)],
) -> String {
    let mut yaml = format!(
        "listen: \"0.0.0.0:{port}\"\nupstream: \"{upstream}\"\napi_key: \"{api_key}\"\nmin_429_delay: \"10s\"\nlog_submits: true\nmax_entries: 10000\n",
        port = CACHE_LISTEN_PORT,
    );
    if !upstream_headers.is_empty() {
        yaml.push_str("upstream_headers:\n");
        for (name, value) in upstream_headers {
            yaml.push_str(&format!("  {name}: \"{value}\"\n"));
        }
    }
    yaml
}

/// Path where we persist the cache api_key for this testbed.
fn api_key_path(settings: &Settings) -> PathBuf {
    PathBuf::from("work").join(&settings.testbed_id).join(API_KEY_FILE)
}

/// Read a previously-generated api_key, or return `Ok(None)` if none has
/// been persisted yet.
pub fn read_api_key(settings: &Settings) -> Result<Option<String>> {
    let path = api_key_path(settings);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading {}", path.display()))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(trimmed.to_string()))
}

/// Return the persisted api_key, generating + writing one on first call.
fn load_or_create_api_key(settings: &Settings) -> Result<String> {
    if let Some(key) = read_api_key(settings)? {
        return Ok(key);
    }
    let key = random_api_key();
    let path = api_key_path(settings);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(&path, format!("{key}\n"))
        .with_context(|| format!("writing {}", path.display()))?;
    info!(path = %path.display(), "generated new cache api_key");
    Ok(key)
}

/// 32 hex chars (128 bits). Plenty for a port-scanner filter.
fn random_api_key() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // Mix in address of a stack var so parallel deploys don't collide on
    // the time source. Not cryptographic — doesn't need to be.
    let mixin = &seed as *const _ as usize as u128;
    let a = seed ^ mixin;
    let b = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15_9E37_79B9_7F4A_7C15);
    format!("{a:016x}{b:016x}")
}

async fn upload_str(
    settings: &Settings,
    host: &str,
    contents: &str,
    remote_path: &str,
) -> Result<()> {
    let tmp = write_temp(contents).context("writing temp config")?;
    ssh::upload(
        &settings.ssh,
        &settings.cloud.ssh_private_key_file,
        host,
        tmp.path(),
        remote_path,
    )
    .await?;
    Ok(())
}

struct TempPath(std::path::PathBuf);
impl TempPath {
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn write_temp(contents: &str) -> std::io::Result<TempPath> {
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut path = std::env::temp_dir();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    path.push(format!("tape-network-cache-{suffix:x}"));
    let mut f = std::fs::File::create(&path)?;
    f.write_all(contents.as_bytes())?;
    Ok(TempPath(path))
}
