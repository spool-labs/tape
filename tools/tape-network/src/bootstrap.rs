//! `tape-network bootstrap` — end-to-end wire-up from bare droplets to running
//! nodes registered on-chain.
//!
//! Steps are serial; each helper is independently invocable so a failure can
//! be resumed by re-running just that step.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::cloud::Instance;
use crate::settings::{BuildSource, Settings};
use crate::ssh;

const SERVICE_TEMPLATE: &str = include_str!("../assets/tape-node.service.tmpl");
const HTTP_PORT: u16 = 8080;
const PEER_PORT: u16 = 9000;

/// Options controlling which bootstrap steps run.
#[derive(Debug, Default, Clone, Copy)]
pub struct RunOptions {
    /// Skip step 6b (fund wallets). Useful when resuming a partial run against
    /// already-funded nodes.
    pub skip_fund: bool,
}

/// Perform the full bootstrap pipeline in order.
pub async fn run(
    settings: &Settings,
    work_dir: Option<PathBuf>,
    options: RunOptions,
) -> Result<()> {
    let work_dir = work_dir.unwrap_or_else(|| default_work_dir(&settings.testbed_id));
    validate(settings)?;

    info!("step 1: local keygen");
    let key_dirs = local_keygen(settings, &work_dir)?;

    info!("step 2: discover droplets");
    let droplets = discover_droplets(settings, key_dirs.len()).await?;

    info!("step 3: locate node binary");
    let binary = find_node_binary(settings)?;
    info!(binary = %binary.display(), "using binary");

    info!("step 4+5: install deps and upload bundle on each droplet");
    for (node_dir, droplet) in key_dirs.iter().zip(&droplets) {
        let ip = droplet_ip(droplet)?;
        info!(host = %ip, "installing + uploading");
        ssh::wait_until_reachable(
            &settings.ssh,
            &settings.cloud.ssh_private_key_file,
            ip,
            Duration::from_secs(120),
        )
        .await?;
        install_remote(settings, ip).await?;
        upload_bundle(settings, ip, node_dir, &binary).await?;
        upload_node_yaml(settings, ip, node_dir, &droplet.name).await?;
    }

    info!("step 6a: initialize TAPE mint (idempotent)");
    init_mint_if_missing(settings).await?;

    info!("step 6a': initialize chain (system + expand + archive/epoch)");
    init_chain_if_missing(settings).await?;

    if options.skip_fund {
        info!("step 6b: skipped (--skip-fund)");
    } else {
        info!("step 6b: fund node wallets from treasury");
        fund_wallets(settings, &key_dirs).await?;
    }

    info!("step 7: register each node on-chain and join network");
    for (node_dir, droplet) in key_dirs.iter().zip(&droplets) {
        let ip = droplet_ip(droplet)?;
        register_on_chain(settings, node_dir, ip).await?;
    }

    info!("step 8: start systemd service on each droplet");
    for droplet in &droplets {
        let ip = droplet_ip(droplet)?;
        start_service(settings, ip).await?;
    }

    info!("step 9: verify /v1/health");
    verify_health(&droplets).await?;

    info!("bootstrap complete");
    Ok(())
}

// ========================================================================
// Step 1: local keygen
// ========================================================================

/// Generate N per-node key bundles on the operator workstation.
///
/// Idempotent: a node directory that already contains an `identity.json` is
/// left untouched. Delete the dir (or just the file) to force regeneration —
/// but be aware that fresh identities mean re-funding, so prefer preserving
/// existing dirs when recovering from a partial bootstrap.
pub fn local_keygen(settings: &Settings, work_dir: &Path) -> Result<Vec<PathBuf>> {
    std::fs::create_dir_all(work_dir)
        .with_context(|| format!("creating {}", work_dir.display()))?;

    let mut dirs = Vec::new();
    for i in 0..settings.network.node_count {
        let dir = work_dir.join(format!("node-{i}"));
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("creating {}", dir.display()))?;

        if dir.join("identity.json").exists() {
            tracing::info!(node = i, "keygen: reusing existing bundle");
            dirs.push(dir);
            continue;
        }

        let name = format!("{}-{i}", settings.testbed_id);
        let mut cmd = StdCommand::new("tape-node");
        cmd.arg("keygen")
            .args(["--name", &name])
            .args(["--out", &dir.display().to_string()]);
        if let Some(seed) = &settings.genesis.deterministic_seed {
            let mixed = derive_node_seed(seed, i)?;
            cmd.args(["--seed", &mixed]);
        }
        let output = cmd.output().context("spawning tape-node keygen")?;
        if !output.status.success() {
            bail!(
                "tape-node keygen for node {i} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        dirs.push(dir);
    }
    Ok(dirs)
}

// ========================================================================
// Step 2: discover provisioned droplets
// ========================================================================

async fn discover_droplets(settings: &Settings, needed: usize) -> Result<Vec<Instance>> {
    // Gap-fill: reuse existing `{testbed_id}-node-{i}` droplets, provision any
    // missing ones. Guarantees the returned vec is in node-index order so the
    // caller can zip(key_dirs) without name-sort-vs-index hazards.
    crate::testnet::ensure_fleet(settings, needed as u32).await
}

fn droplet_ip(droplet: &Instance) -> Result<&str> {
    droplet
        .public_ip
        .as_deref()
        .ok_or_else(|| anyhow!("droplet {} has no public IP", droplet.name))
}

// ========================================================================
// Step 3: find the linux node binary on disk
// ========================================================================

pub fn find_node_binary(settings: &Settings) -> Result<PathBuf> {
    let source_root = match &settings.build.source {
        BuildSource::Local { path } => path.clone(),
        BuildSource::Git { .. } => {
            bail!(
                "build.source.kind = git is not yet supported; switch to \
                 kind: local and build locally, or ship a pre-built binary"
            );
        }
    };
    let candidates = [
        source_root.join("target/x86_64-unknown-linux-gnu/release/tape-node"),
        source_root.join("target/release/tape-node"),
    ];
    for p in &candidates {
        if p.exists() {
            return Ok(p.clone());
        }
    }
    bail!(
        "no tape-node binary found; expected one of:\n  {}\n\n\
         build with: cargo build --release --target x86_64-unknown-linux-gnu",
        candidates
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join("\n  ")
    )
}

// ========================================================================
// Step 4: install runtime dependencies on the droplet
// ========================================================================

async fn install_remote(settings: &Settings, host: &str) -> Result<()> {
    let working_dir = settings.network.working_dir.display();
    let data_dir = settings.network.data_dir.display();
    // On Ubuntu the `tape` group is a reserved system group for tape-drive
    // hardware access. We reuse it for our tape-node user rather than fail on
    // the collision — the two uses are mutually irrelevant.
    // Stop the service (if installed) so we can overwrite the binary. The
    // bool guard on `systemctl stop` keeps this a no-op on first bring-up.
    let script = format!(
        r#"set -e
getent group tape >/dev/null 2>&1 || groupadd -r tape
id -u tape >/dev/null 2>&1 || useradd -r -m -d /var/lib/tape -s /usr/sbin/nologin -g tape tape
mkdir -p {working_dir}/bin {working_dir}/keys
mkdir -p {data_dir}/store
chown -R tape:tape {working_dir} {data_dir}
systemctl is-enabled tape-node >/dev/null 2>&1 && systemctl stop tape-node || true
"#
    );
    ssh::exec(
        &settings.ssh,
        &settings.cloud.ssh_private_key_file,
        host,
        &script,
    )
    .await?;
    Ok(())
}

// ========================================================================
// Step 5: upload binary and per-node bundle
// ========================================================================

async fn upload_bundle(
    settings: &Settings,
    host: &str,
    node_dir: &Path,
    binary: &Path,
) -> Result<()> {
    let working = settings.network.working_dir.display();
    let ssh_key = &settings.cloud.ssh_private_key_file;

    ssh::upload(&settings.ssh, ssh_key, host, binary, &format!("{working}/bin/tape-node"))
        .await?;
    ssh::exec(
        &settings.ssh,
        ssh_key,
        host,
        &format!("chmod 0755 {working}/bin/tape-node && chown tape:tape {working}/bin/tape-node"),
    )
    .await?;

    for keyfile in ["identity.json", "bls.json", "tls.json"] {
        let local = node_dir.join(keyfile);
        let remote = format!("{working}/keys/{keyfile}");
        ssh::upload(&settings.ssh, ssh_key, host, &local, &remote).await?;
    }
    ssh::exec(
        &settings.ssh,
        ssh_key,
        host,
        &format!("chown -R tape:tape {working}/keys && chmod 0700 {working}/keys && chmod 0600 {working}/keys/*.json"),
    )
    .await?;
    Ok(())
}

async fn upload_node_yaml(
    settings: &Settings,
    host: &str,
    _node_dir: &Path,
    node_name: &str,
) -> Result<()> {
    let rendered = render_node_yaml(settings, host, node_name);
    let working = settings.network.working_dir.display();
    let remote = format!("{working}/node.yaml");
    upload_str(settings, host, &rendered, &remote).await?;
    ssh::exec(
        &settings.ssh,
        &settings.cloud.ssh_private_key_file,
        host,
        &format!("chown tape:tape {remote} && chmod 0644 {remote}"),
    )
    .await?;
    Ok(())
}

fn render_node_yaml(settings: &Settings, public_ip: &str, node_name: &str) -> String {
    let working = settings.network.working_dir.display();
    let data = settings.network.data_dir.display();
    let rpc = &settings.solana.rpc_url;
    let commission = settings.genesis.commission_bp;
    format!(
        "node:\n  name: \"{node_name}\"\n  node_keypair: \"{working}/keys/identity.json\"\n  bls_keypair: \"{working}/keys/bls.json\"\n  commission: {commission}\nsolana:\n  rpc: \"{rpc}\"\nnetwork:\n  host: \"{public_ip}\"\n  port: {PEER_PORT}\nhttp:\n  listen: \"0.0.0.0:{HTTP_PORT}\"\nstore:\n  path: \"{data}/store\"\ntls:\n  identity_keypair: \"{working}/keys/tls.json\"\n"
    )
}

async fn upload_str(settings: &Settings, host: &str, contents: &str, remote: &str) -> Result<()> {
    let tmp = tempfile_with_contents(contents)?;
    ssh::upload(
        &settings.ssh,
        &settings.cloud.ssh_private_key_file,
        host,
        tmp.path(),
        remote,
    )
    .await?;
    Ok(())
}

struct TempFile {
    path: PathBuf,
}

impl TempFile {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn tempfile_with_contents(contents: &str) -> Result<TempFile> {
    let mut path = std::env::temp_dir();
    path.push(format!("tape-network-{}", rand_suffix()));
    let mut f = std::fs::File::create(&path)
        .with_context(|| format!("creating temp file {}", path.display()))?;
    f.write_all(contents.as_bytes())
        .with_context(|| format!("writing temp file {}", path.display()))?;
    Ok(TempFile { path })
}

fn rand_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{n:x}")
}

// ========================================================================
// Step 6a: initialize TAPE mint (idempotent)
// ========================================================================

async fn init_mint_if_missing(settings: &Settings) -> Result<()> {
    let ctx = tape_admin::Context::new(
        settings.solana.rpc_url.clone(),
        &settings.solana.treasury_keypair,
    )
    .map_err(|e| anyhow!("tape-admin context: {e}"))?;

    match tape_admin::mint::init(&ctx).await {
        Ok(()) => {
            info!("TAPE mint initialized");
            Ok(())
        }
        Err(e) => {
            // The init fails if the mint PDA already exists (common case on
            // re-run). Downgrade to a warning and continue; if the mint
            // truly isn't initialized the fund step below will surface a
            // clear error.
            info!(reason = %e, "mint init skipped (likely already initialized)");
            Ok(())
        }
    }
}

async fn init_chain_if_missing(settings: &Settings) -> Result<()> {
    let ctx = tape_admin::Context::new(
        settings.solana.rpc_url.clone(),
        &settings.solana.treasury_keypair,
    )
    .map_err(|e| anyhow!("tape-admin context: {e}"))?;
    tape_admin::chain::init_all(&ctx)
        .await
        .map_err(|e| anyhow!("chain init: {e}"))
}

// ========================================================================
// Step 6b: fund wallets
// ========================================================================

async fn fund_wallets(settings: &Settings, node_dirs: &[PathBuf]) -> Result<()> {
    let identity_paths: Vec<PathBuf> =
        node_dirs.iter().map(|d| d.join("identity.json")).collect();
    let pubkeys = tape_admin::treasury::load_pubkeys_from_identities(&identity_paths)
        .map_err(|e| anyhow!("collecting pubkeys: {e}"))?;

    let ctx = tape_admin::Context::new(
        settings.solana.rpc_url.clone(),
        &settings.solana.treasury_keypair,
    )
    .map_err(|e| anyhow!("tape-admin context: {e}"))?;

    let lamports = (settings.genesis.per_node_sol * 1e9) as u64;
    let flux = (settings.genesis.per_node_tape * 1e6) as u64;
    tape_admin::treasury::fund(&ctx, &pubkeys, lamports, flux)
        .await
        .map_err(|e| anyhow!("treasury fund: {e}"))?;
    Ok(())
}

// ========================================================================
// Step 7: register on-chain and join network
// ========================================================================

async fn register_on_chain(
    settings: &Settings,
    node_dir: &Path,
    public_ip: &str,
) -> Result<()> {
    let ctx = tape_admin::Context::new(
        settings.solana.rpc_url.clone(),
        &node_dir.join("identity.json"),
    )
    .map_err(|e| anyhow!("tape-admin context: {e}"))?;

    let name = node_dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("tape-node")
        .to_string();

    let identity_path = node_dir.join("identity.json");
    let address = format!("{public_ip}:{PEER_PORT}");

    tape_admin::node::register(
        &ctx,
        tape_admin::node::RegisterParams {
            name,
            identity_path: identity_path.clone(),
            bls_path: node_dir.join("bls.json"),
            tls_path: node_dir.join("tls.json"),
            address: address.clone(),
            commission_bp: settings.genesis.commission_bp,
        },
    )
    .await
    .map_err(|e| anyhow!("register: {e}"))?;

    // Always reassert the network address. On first bring-up this is a no-op
    // (matches what register_node just wrote); on resurrection after a droplet
    // rebuild it catches the new IP.
    tape_admin::node::set_address(&ctx, &identity_path, &address)
        .await
        .map_err(|e| anyhow!("set_address: {e}"))?;

    let stake_flux = (settings.genesis.stake_amount * 1e6) as u64;
    tape_admin::node::stake(&ctx, &identity_path, stake_flux)
        .await
        .map_err(|e| anyhow!("stake: {e}"))?;

    tape_admin::node::advance_pool(&ctx, &identity_path)
        .await
        .map_err(|e| anyhow!("advance_pool: {e}"))?;

    tape_admin::node::join_network(&ctx, &identity_path)
        .await
        .map_err(|e| anyhow!("join_network: {e}"))?;
    Ok(())
}

// ========================================================================
// Step 8: install and start systemd service
// ========================================================================

async fn start_service(settings: &Settings, host: &str) -> Result<()> {
    let working = settings.network.working_dir.display();
    let rendered = SERVICE_TEMPLATE.replace("{{WORKING_DIR}}", &working.to_string());
    let unit_path = "/etc/systemd/system/tape-node.service";
    upload_str(settings, host, &rendered, unit_path).await?;
    ssh::exec(
        &settings.ssh,
        &settings.cloud.ssh_private_key_file,
        host,
        "systemctl daemon-reload && systemctl enable --now tape-node",
    )
    .await?;
    Ok(())
}

// ========================================================================
// Step 9: verify /v1/health
// ========================================================================

async fn verify_health(droplets: &[Instance]) -> Result<()> {
    // Nodes serve the HTTP API over TLS with per-node self-signed certs (see
    // `docs/tls-pinning.md`). We disable cert verification here since we have
    // no pinned cert at the operator; this check is for liveness, not
    // authentication.
    //
    // Force rustls: feature unification elsewhere in the tree enables
    // reqwest's native-tls feature, which on macOS ends up using LibreSSL and
    // chokes on the node's Ed25519 cert handshake. rustls handles it cleanly.
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(10))
        .build()?;
    for droplet in droplets {
        let ip = droplet_ip(droplet)?;
        let url = format!("https://{ip}:{HTTP_PORT}/v1/health");
        let ok = poll_health(&client, &url, Duration::from_secs(90)).await?;
        if !ok {
            warn!(%ip, "health check failed after timeout");
        } else {
            info!(%ip, "node is healthy");
        }
    }
    Ok(())
}

async fn poll_health(client: &reqwest::Client, url: &str, timeout: Duration) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(resp) = client.get(url).send().await {
            if resp.status().is_success() {
                return Ok(true);
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        sleep(Duration::from_secs(3)).await;
    }
}

// ========================================================================
// Shared helpers
// ========================================================================

fn validate(settings: &Settings) -> Result<()> {
    if !settings.solana.program_ids.all_deployed() {
        bail!(
            "solana.program_ids has null entries; run `tape-network genesis \
             deploy-programs` first and pin the ids in settings.yaml"
        );
    }
    if !settings.solana.treasury_keypair.exists() {
        bail!(
            "treasury keypair not found at {}",
            settings.solana.treasury_keypair.display()
        );
    }
    if !settings.cloud.ssh_key_file.exists() {
        bail!(
            "ssh public key not found at {}",
            settings.cloud.ssh_key_file.display()
        );
    }
    if !settings.cloud.ssh_private_key_file.exists() {
        bail!(
            "ssh private key not found at {}",
            settings.cloud.ssh_private_key_file.display()
        );
    }
    Ok(())
}

fn default_work_dir(testbed_id: &str) -> PathBuf {
    PathBuf::from("work").join(testbed_id)
}

fn derive_node_seed(master: &str, idx: u32) -> Result<String> {
    let bytes = hex::decode(master.trim_start_matches("0x"))
        .with_context(|| format!("parsing master seed {master}"))?;
    if bytes.len() != 32 {
        bail!("master seed must be 32 bytes hex, got {} bytes", bytes.len());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    let idx_bytes = idx.to_le_bytes();
    for (i, b) in idx_bytes.iter().enumerate() {
        out[28 + i] ^= *b;
    }
    Ok(hex::encode(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_node_seed_is_deterministic_and_differs_per_index() {
        let master = "00".repeat(32);
        let a = derive_node_seed(&master, 0).unwrap();
        let b = derive_node_seed(&master, 1).unwrap();
        let a2 = derive_node_seed(&master, 0).unwrap();
        assert_eq!(a, a2);
        assert_ne!(a, b);
    }

    #[test]
    fn derive_node_seed_rejects_bad_length() {
        let err = derive_node_seed("00", 0).unwrap_err();
        assert!(err.to_string().contains("32 bytes"));
    }

    #[test]
    fn renders_node_yaml_parseable() {
        let settings = sample_settings();
        let rendered = render_node_yaml(&settings, "1.2.3.4", "test-node");
        assert!(rendered.contains("name: \"test-node\""));
        assert!(rendered.contains("host: \"1.2.3.4\""));
        assert!(rendered.contains("port: 9000"));
    }

    fn sample_settings() -> Settings {
        let yaml = r#"
testbed_id: test
cloud:
  provider: digitalocean
  token: dummy
  ssh_key_file: /tmp/nope.pub
  ssh_private_key_file: /tmp/nope
  region: nyc3
  size: s-1vcpu-1gb
  image: ubuntu-24-04-x64
network:
  node_count: 1
solana:
  cluster: devnet
  rpc_url: https://api.devnet.solana.com
  treasury_keypair: /tmp/treasury.json
genesis:
  per_node_sol: 1
  per_node_tape: 1
  stake_amount: 1
"#;
        serde_yaml::from_str(yaml).unwrap()
    }
}
