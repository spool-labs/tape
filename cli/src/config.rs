use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TapeConfig {
    pub solana: SolanaConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SolanaConfig {
    pub rpc_url: String,
    pub ws_url: Option<String>,
    pub commitment: String,
}

impl TapeConfig {
    /// load configuration from ~/tape.toml file
    pub fn load() -> anyhow::Result<Self> {
        let home_dir = dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("Could not determine home directory"))?;
        let config_path = home_dir.join("tape.toml");
        Self::load_from_path(config_path)
    }

    // TODO: load configuration from specified path
    pub fn load_from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let contents = fs::read_to_string(path)?;
        let config: TapeConfig = toml::from_str(&contents)?;
        //config.validate()?;
        Ok(config)
    }

    /// Create default configuration and save to file
    pub fn create_default() -> anyhow::Result<Self> {
        let config = Self::default();
        let toml_string = toml::to_string_pretty(&config)?;
        let home_dir = dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("Could not determine home directory"))?;
        let config_path = home_dir.join("tape.toml");
        fs::write(config_path, toml_string)?;
        Ok(config)
    }
}

impl Default for TapeConfig {
    fn default() -> Self {
        Self {
            solana: SolanaConfig {
                rpc_url: "https://api.devnet.solana.com".to_string(),
                ws_url: Some("wss://api.devnet.solana.com/".to_string()),
                commitment: "confirmed".to_string(),
            },
        }
    }
}
