use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TapeConfig {
    pub transaction: TransactionConfig,
    pub identity: IdentityConfig,
    pub solana: SolanaConfig,
    pub storage: StorageConfig,
    pub network: NetworkConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionConfig {
    pub priority_fee_lamports: u64,
    pub max_tx_retries: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IdentityConfig {
    pub keypair_path: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SolanaConfig {
    pub rpc_url: String,
    pub ws_url: Option<String>,
    pub commitment: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageConfig {
    pub rocksdb_primary_path: String,
    pub rocksdb_secondary_path: Option<String>,
    pub rocksdb_cache_size_mb: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NetworkConfig {
    pub bind_address: String,
    pub metrics_endpoint: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoggingConfig {
    pub log_level: String,
    pub log_path: Option<String>,
    pub metrics_interval: u64,
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
        config.validate()?;
        Ok(config)
    }

    /// validate values in tape.toml
    fn validate(&self) -> anyhow::Result<()> {
        // commitment level
        match self.solana.commitment.as_str() {
            "processed" | "confirmed" | "finalized" => {},
            _ => return Err(anyhow::anyhow!("Invalid commitment level: {}", self.solana.commitment)),
        }

        // log level
        match self.logging.log_level.as_str() {
            "error" | "warn" | "info" | "debug" | "trace" => {},
            _ => return Err(anyhow::anyhow!("Invalid log level: {}", self.logging.log_level)),
        }

        // keypair path 
        if !Path::new(&self.identity.keypair_path).exists() {
            return Err(anyhow::anyhow!("Keypair file not found: {}", self.identity.keypair_path));
        }

        Ok(())
    }

    /// create default configuration and save to file
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
            transaction: TransactionConfig {
                priority_fee_lamports: 1000,
                max_tx_retries: 3,
            },
            identity: IdentityConfig {
                keypair_path: "~/.config/solana/id.json".to_string(),
            },
            solana: SolanaConfig {
                rpc_url: "https://api.devnet.solana.com".to_string(),
                ws_url: Some("wss://api.devnet.solana.com/".to_string()),
                commitment: "confirmed".to_string(),
            },
            storage: StorageConfig {
                rocksdb_primary_path: "./db_tapestore".to_string(),
                rocksdb_secondary_path: Some("./db_tapestore_secondary".to_string()),
                rocksdb_cache_size_mb: 512,
            },
            network: NetworkConfig {
                bind_address: "127.0.0.1:8080".to_string(),
                metrics_endpoint: "127.0.0.1:9090".to_string(),
            },
            logging: LoggingConfig {
                log_level: "info".to_string(),
                log_path: Some("./logs/tape.log".to_string()),
                metrics_interval: 30,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toml_parsing_works_properly() {
        let toml_content = r#"
[transaction]
priority_fee_lamports = 2000
max_tx_retries = 5

[identity]
keypair_path = "~/.config/solana/id.json"

[solana]
rpc_url = "https://api.mainnet-beta.solana.com"
ws_url = "wss://api.mainnet-beta.solana.com/"
commitment = "finalized"

[storage]
rocksdb_primary_path = "./test_db"
rocksdb_secondary_path = "./test_db_secondary"
rocksdb_cache_size_mb = 1024

[network]
bind_address = "0.0.0.0:8081"
metrics_endpoint = "0.0.0.0:9091"

[logging]
log_level = "debug"
log_path = "./test.log"
metrics_interval = 60
"#;

        let config: TapeConfig = toml::from_str(toml_content).unwrap();
        
        assert_eq!(config.transaction.priority_fee_lamports, 2000);
        assert_eq!(config.transaction.max_tx_retries, 5);
        assert_eq!(config.identity.keypair_path, "~/.config/solana/id.json");
        assert_eq!(config.solana.rpc_url, "https://api.mainnet-beta.solana.com");
        assert_eq!(config.solana.ws_url, Some("wss://api.mainnet-beta.solana.com/".to_string()));
        assert_eq!(config.solana.commitment, "finalized");
        assert_eq!(config.storage.rocksdb_primary_path, "./test_db");
        assert_eq!(config.storage.rocksdb_secondary_path, Some("./test_db_secondary".to_string()));
        assert_eq!(config.storage.rocksdb_cache_size_mb, 1024);
        assert_eq!(config.network.bind_address, "0.0.0.0:8081");
        assert_eq!(config.network.metrics_endpoint, "0.0.0.0:9091");
        assert_eq!(config.logging.log_level, "debug");
        assert_eq!(config.logging.log_path, Some("./test.log".to_string()));
        assert_eq!(config.logging.metrics_interval, 60);
    }
}
