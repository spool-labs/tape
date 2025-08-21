use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TapeConfig {
    pub transaction: TransactionConfig,
    pub performance: PerformanceConfig,
    pub identity: IdentityConfig,
    pub solana: SolanaConfig,
    pub storage: StorageConfig,
    pub network: NetworkConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionConfig {
    pub priority_fee: u64,
    pub tx_retries: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerformanceConfig {
    pub num_cores: usize,
    pub max_memory_mb: u64,
    pub rocksdb_conn_pool_size: u32,
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
            transaction: TransactionConfig {
                priority_fee: 1000,
                tx_retries: 3,
            },
            performance: PerformanceConfig {
                num_cores: num_cpus::get(),
                max_memory_mb: 2048,
                rocksdb_conn_pool_size: 10,
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
