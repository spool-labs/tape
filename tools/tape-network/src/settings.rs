//! Settings file loader with environment-variable substitution and `~`
//! expansion for path-typed fields.
//!
//! Schema mirrors `docs/testnet-deployment.md`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Settings {
    pub testbed_id: String,
    pub cloud: CloudSettings,
    pub network: NetworkSettings,
    pub solana: SolanaSettings,
    pub genesis: GenesisSettings,
    #[serde(default)]
    pub build: BuildSettings,
    #[serde(default)]
    pub monitoring: MonitoringSettings,
    #[serde(default)]
    pub ssh: SshSettings,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CloudSettings {
    pub provider: Provider,
    /// DigitalOcean API token. Typically sourced via env substitution, e.g.
    /// `"${DIGITALOCEAN_ACCESS_TOKEN}"`.
    pub token: String,
    /// Path to an SSH *public* key already registered with DigitalOcean.
    pub ssh_key_file: PathBuf,
    /// Path to the matching SSH private key used for subsequent exec/scp.
    pub ssh_private_key_file: PathBuf,
    pub region: String,
    pub size: String,
    pub image: String,
    #[serde(default)]
    pub volume_gb: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    Digitalocean,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NetworkSettings {
    pub node_count: u32,
    #[serde(default = "default_working_dir")]
    pub working_dir: PathBuf,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_open_ports")]
    pub open_ports: Vec<u16>,
}

fn default_working_dir() -> PathBuf {
    PathBuf::from("/opt/tape")
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("/mnt/tape-data")
}

fn default_open_ports() -> Vec<u16> {
    vec![8080, 9000]
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SolanaSettings {
    pub cluster: String,
    pub rpc_url: String,
    #[serde(default)]
    pub ws_url: Option<String>,
    pub treasury_keypair: PathBuf,
    #[serde(default)]
    pub program_ids: ProgramIds,
    /// Extra HTTP headers attached to every upstream RPC request the cache
    /// makes. Used for providers that authenticate via header (Triton One
    /// uses `x-token`) rather than via URL query string. Empty map for
    /// public/keyless endpoints.
    #[serde(default)]
    pub rpc_headers: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ProgramIds {
    #[serde(default)]
    pub tapedrive: Option<String>,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub staking: Option<String>,
    #[serde(default)]
    pub exchange: Option<String>,
}

impl ProgramIds {
    pub fn all_deployed(&self) -> bool {
        self.tapedrive.is_some()
            && self.token.is_some()
            && self.staking.is_some()
            && self.exchange.is_some()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GenesisSettings {
    #[serde(default)]
    pub deterministic_seed: Option<String>,
    /// SOL to disburse per node. Fractional values OK.
    #[serde(default = "default_per_node_sol")]
    pub per_node_sol: f64,
    /// TAPE to disburse per node. Fractional values OK.
    #[serde(default = "default_per_node_tape")]
    pub per_node_tape: f64,
    #[serde(default = "default_stake_amount")]
    pub stake_amount: f64,
    #[serde(default)]
    pub commission_bp: u16,
}

fn default_per_node_sol() -> f64 {
    2.0
}

fn default_per_node_tape() -> f64 {
    1000.0
}

fn default_stake_amount() -> f64 {
    500.0
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BuildSettings {
    #[serde(default = "default_build_mode")]
    pub mode: String,
    #[serde(default)]
    pub source: BuildSource,
}

fn default_build_mode() -> String {
    "release".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum BuildSource {
    Local { path: PathBuf },
    Git { url: String, commit: String },
}

impl Default for BuildSource {
    fn default() -> Self {
        BuildSource::Local {
            path: PathBuf::from("../.."),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitoringSettings {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub dedicated_droplet: bool,
    #[serde(default = "default_scrape_interval")]
    pub scrape_interval_secs: u32,
}

impl Default for MonitoringSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            dedicated_droplet: false,
            scrape_interval_secs: default_scrape_interval(),
        }
    }
}

fn default_scrape_interval() -> u32 {
    15
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SshSettings {
    #[serde(default = "default_ssh_user")]
    pub user: String,
    #[serde(default = "default_ssh_timeout")]
    pub timeout_secs: u32,
    #[serde(default = "default_ssh_retries")]
    pub retries: u32,
}

impl Default for SshSettings {
    fn default() -> Self {
        Self {
            user: default_ssh_user(),
            timeout_secs: default_ssh_timeout(),
            retries: default_ssh_retries(),
        }
    }
}

fn default_ssh_user() -> String {
    "root".to_string()
}

fn default_ssh_timeout() -> u32 {
    30
}

fn default_ssh_retries() -> u32 {
    3
}

impl Settings {
    /// Load a settings file, applying `${VAR}` substitution and `~` expansion
    /// on path-typed fields after parsing.
    pub fn from_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading settings file {}", path.display()))?;
        let substituted = substitute_env(&raw, &env_map())?;
        let mut settings: Settings = serde_yaml::from_str(&substituted)
            .with_context(|| format!("parsing settings file {}", path.display()))?;
        settings.expand_paths();
        Ok(settings)
    }

    fn expand_paths(&mut self) {
        expand_inplace(&mut self.cloud.ssh_key_file);
        expand_inplace(&mut self.cloud.ssh_private_key_file);
        expand_inplace(&mut self.network.working_dir);
        expand_inplace(&mut self.network.data_dir);
        expand_inplace(&mut self.solana.treasury_keypair);
        if let BuildSource::Local { path } = &mut self.build.source {
            expand_inplace(path);
        }
    }
}

fn expand_inplace(p: &mut PathBuf) {
    if let Some(expanded) = expand_tilde(p) {
        *p = expanded;
    }
}

fn expand_tilde(p: &Path) -> Option<PathBuf> {
    let s = p.to_str()?;
    if let Some(rest) = s.strip_prefix("~/") {
        let home = std::env::var_os("HOME")?;
        let mut pb = PathBuf::from(home);
        pb.push(rest);
        Some(pb)
    } else if s == "~" {
        let home = std::env::var_os("HOME")?;
        Some(PathBuf::from(home))
    } else {
        None
    }
}

fn env_map() -> HashMap<String, String> {
    std::env::vars().collect()
}

fn substitute_env(input: &str, env: &HashMap<String, String>) -> Result<String> {
    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").unwrap();
    let mut out = String::with_capacity(input.len());
    let mut last_end = 0usize;
    for cap in re.captures_iter(input) {
        let whole = cap.get(0).unwrap();
        let name = cap.get(1).unwrap().as_str();
        out.push_str(&input[last_end..whole.start()]);
        let value = env
            .get(name)
            .ok_or_else(|| anyhow!("environment variable `{}` is not set", name))?;
        out.push_str(value);
        last_end = whole.end();
    }
    out.push_str(&input[last_end..]);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_env_vars() {
        let mut env = HashMap::new();
        env.insert("USER".into(), "alice".into());
        env.insert("HOME".into(), "/home/alice".into());
        let out = substitute_env("id=${USER} home=${HOME}", &env).unwrap();
        assert_eq!(out, "id=alice home=/home/alice");
    }

    #[test]
    fn missing_env_var_errors() {
        let env = HashMap::new();
        let err = substitute_env("${NOT_SET}", &env).unwrap_err();
        assert!(err.to_string().contains("NOT_SET"));
    }

    #[test]
    fn expand_tilde_basic() {
        // SAFETY: test is single-threaded and the value is restored by
        // dropping the returned guard.
        let prior = std::env::var_os("HOME");
        unsafe { std::env::set_var("HOME", "/h") };
        let p = expand_tilde(Path::new("~/foo")).unwrap();
        assert_eq!(p, PathBuf::from("/h/foo"));
        match prior {
            Some(v) => unsafe { std::env::set_var("HOME", v) },
            None => unsafe { std::env::remove_var("HOME") },
        }
    }
}
