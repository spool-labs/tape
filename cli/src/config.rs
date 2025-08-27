use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use solana_sdk::commitment_config::CommitmentConfig;
use crate::log::print_error;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TapeConfig {
    pub mining_config: MiningConfig,
    pub identity: IdentityConfig,
    pub solana: SolanaConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MiningConfig {
    pub num_cores: usize,
    pub max_memory_mb: u64,
    pub max_poa_threads: u64,
    pub max_pow_threads: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IdentityConfig {
    pub keypair_path: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SolanaConfig {
    pub rpc_url: String,
    pub ws_url: Option<String>,
    pub commitment: CommitmentLevel,
    pub priority_fee_lamports: u64,
    pub max_transaction_retries: u32,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub rocksdb: Option<RocksDbConfig>, 
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RocksDbConfig {
    pub primary_path: String,
    pub secondary_path: Option<String>,
    pub cache_size_mb: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum StorageBackend {
    RocksDb,
    Postgres
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoggingConfig {
    pub log_level: LogLevel,
    pub log_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CommitmentLevel {
    Processed,
    Confirmed, 
    Finalized,
}

impl ToString for CommitmentLevel {
    fn to_string(&self) -> String {
        match self {
            CommitmentLevel::Processed => "processed".to_string(),
            CommitmentLevel::Confirmed => "confirmed".to_string(),
            CommitmentLevel::Finalized => "finalized".to_string(),
        }
    }
}

impl CommitmentLevel {
    pub fn to_commitment_config(&self) -> CommitmentConfig {
        match self {
            CommitmentLevel::Processed => CommitmentConfig::processed(),
            CommitmentLevel::Confirmed => CommitmentConfig::confirmed(),
            CommitmentLevel::Finalized => CommitmentConfig::finalized(),
        }
    }
}

impl TapeConfig {
    /// load configuration from ~/tape.toml file
    // TOFIX: catches all error including validation error and treats them as "config not found",
    // so do proper error handling for config validation and tape.toml creation
    pub fn load() -> anyhow::Result<Self> {
        let home_dir = dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("Could not determine home directory"))?;
        let config_path = home_dir.join("tape.toml");

        // CHECK tape.toml exist or not else return file not found error
        Self::load_from_path(config_path)
    }

    // TODO: load configuration from specified path
    pub fn load_from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(anyhow::anyhow!("tape.toml config file not found"));
        }
        let contents = fs::read_to_string(path)?;
        let config: TapeConfig = toml::from_str(&contents)?;
        // ADD validation check
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        // solana rpc and websocket url
        self.validate_url(&self.solana.rpc_url, "Solana RPC URL", &["http://", "https://"])?;
        if let Some(ref ws_url) = self.solana.ws_url {
            self.validate_url(ws_url, "Solana WebSocket URL", &["ws://", "wss://"])?;
        }

        // keypair
        let keypair_path = &*shellexpand::tilde(&self.identity.keypair_path);
        if !Path::new(&keypair_path).exists() {
            print_error("Keypair not found, please check you tape.toml/keypair path");
            std::process::exit(1);
        }

        Ok(())
    }

    fn validate_url(&self, url: &str, field_name: &str, valid_schemes: &[&str]) -> anyhow::Result<()> {
        let has_valid_scheme = valid_schemes.iter().any(|scheme| url.starts_with(scheme));
        
        if !has_valid_scheme {
            print_error(&format!("{} must start with one of {:?}. Found: '{}'", 
                field_name, valid_schemes, url));
            std::process::exit(1);
        }

        if url.contains(' ') {
            print_error(&format!("{} cannot contain spaces. Found: '{}'", field_name, url));
            std::process::exit(1);
        }

        if url.trim().is_empty() {
            print_error(&format!("{} cannot be empty", field_name));
            std::process::exit(1);
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
            mining_config: MiningConfig{
                num_cores: num_cpus::get(),
                max_memory_mb: 16384,
                max_poa_threads: 4,
                max_pow_threads: 4
            },
            identity: IdentityConfig {
                keypair_path: "~/.config/solana/id.json".to_string(),
            },
            solana: SolanaConfig {
                rpc_url: "https://api.devnet.solana.com".to_string(),
                ws_url: Some("wss://api.devnet.solana.com/".to_string()),
                commitment: CommitmentLevel::Confirmed,
                priority_fee_lamports: 1000,
                max_transaction_retries: 3,
            },
            storage: StorageConfig {
                backend: StorageBackend::RocksDb,
                rocksdb: Some(RocksDbConfig{
                primary_path: "./db_tapestore".to_string(),
                secondary_path: Some("./db_tapestore_secondary".to_string()),
                cache_size_mb: 512,
                })
            },
            logging: LoggingConfig {
                log_level: LogLevel::Info,
                log_path: Some("./logs/tape.log".to_string()),
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
[mining_config]
num_cores = 4                    
max_memory_mb = 16384            
max_poa_threads = 4
max_pow_threads = 4

[identity]
keypair_path = "~/.config/solana/id.json"

[solana]
rpc_url = "https://api.mainnet-beta.solana.com"
ws_url = "wss://api.mainnet-beta.solana.com/"
commitment = "finalized"
priority_fee_lamports = 2000
max_transaction_retries = 5

[storage]
backend = "rocksdb"

[storage.rocksdb]
primary_path = "./data/primary"
secondary_path = "./data/secondary"
cache_size_mb = 512

[logging]
log_level = "debug"
log_path = "./test.log"
"#;

        let config: TapeConfig = toml::from_str(toml_content).unwrap();

        
        assert_eq!(config.identity.keypair_path, "~/.config/solana/id.json");
        assert_eq!(config.solana.rpc_url, "https://api.mainnet-beta.solana.com");
        assert_eq!(config.solana.ws_url, Some("wss://api.mainnet-beta.solana.com/".to_string()));
        assert_eq!(config.solana.commitment, CommitmentLevel::Finalized);
        assert_eq!(config.solana.priority_fee_lamports, 2000);
        assert_eq!(config.solana.max_transaction_retries, 5);

        assert_eq!(config.storage.backend, StorageBackend::RocksDb);  
        let rocksdb_config = config.storage.rocksdb.as_ref().unwrap();
        assert_eq!(rocksdb_config.primary_path, "./data/primary");
        assert_eq!(rocksdb_config.secondary_path, Some("./data/secondary".to_string()));
        assert_eq!(rocksdb_config.cache_size_mb, 512);


        assert_eq!(config.logging.log_level, LogLevel::Debug);  
        assert_eq!(config.logging.log_path, Some("./test.log".to_string()));
    }
}
