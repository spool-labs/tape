use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use solana_sdk::commitment_config::CommitmentConfig;
use std::fmt;

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
pub struct PerformanceConfig {
    pub num_cores: usize,
    pub max_memory_mb: u64,
    pub max_poa_threads: u64,
    pub max_pow_threads: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerformanceConfig {
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

   pub fn load_with_path(config_path: &Option<PathBuf>) -> Result<Self, TapeConfigError> {
        match config_path {
            Some(path) => {
                let expanded_path = expand_path(path);
                if !expanded_path.exists() {
                    return Err(TapeConfigError::CustomConfigFileNotFound(
                        expanded_path.display().to_string()
                    ));
                }
                Self::load_from_path(expanded_path)
            },
            None => {
                let default_path = get_default_config_path()?;
                Self::load_from_path(default_path)
            }
        }
    }

    pub fn load_from_path<P: AsRef<Path>>(path: P) -> Result<Self, TapeConfigError> {
        let path = path.as_ref();
        
        if !path.exists() {
            return Err(TapeConfigError::ConfigFileNotFound);
        }
        
        let contents = fs::read_to_string(path)
            .map_err(TapeConfigError::FileReadError)?;
        let config: TapeConfig = toml::from_str(&contents)
            .map_err(TapeConfigError::ParseError)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), TapeConfigError> {
        // solana rpc and websocket url validation
        self.validate_url(&self.solana.rpc_url, "Solana RPC URL", &["http://", "https://"])?;
        if let Some(ref ws_url) = self.solana.ws_url {
            self.validate_url(ws_url, "Solana WebSocket URL", &["ws://", "wss://"])?;
        }

        // keypair validation
        let keypair_path = &*shellexpand::tilde(&self.identity.keypair_path);
        if !Path::new(&keypair_path).exists() {
            return Err(TapeConfigError::KeypairNotFound(keypair_path.to_string()));
        }

        Ok(())
    }

    fn validate_url(&self, url: &str, field_name: &str, valid_schemes: &[&str]) -> Result<(), TapeConfigError> {
        let has_valid_scheme = valid_schemes.iter().any(|scheme| url.starts_with(scheme));
        
        if !has_valid_scheme {
            return Err(TapeConfigError::InvalidUrl(
                format!("{} must start with one of {:?}, found: '{}'", field_name, valid_schemes, url)
            ));
        }

        if url.contains(' ') {
            return Err(TapeConfigError::InvalidUrl(
                format!("{} cannot contain spaces, found: '{}'", field_name, url)
            ));
        }

        if url.trim().is_empty() {
            return Err(TapeConfigError::InvalidUrl(
                format!("{} cannot be empty", field_name)
            ));
        }

        Ok(())
    }

    fn validate_url(&self, url: &str, field_name: &str, valid_schemes: &[&str]) -> Result<(), TapeConfigError> {
        let has_valid_scheme = valid_schemes.iter().any(|scheme| url.starts_with(scheme));
        
        if !has_valid_scheme {
            return Err(TapeConfigError::InvalidUrl(
                format!("{} must start with one of {:?}, found: '{}'", field_name, valid_schemes, url)
            ));
        }

        if url.contains(' ') {
            return Err(TapeConfigError::InvalidUrl(
                format!("{} cannot contain spaces, found: '{}'", field_name, url)
            ));
        }

        if url.trim().is_empty() {
            return Err(TapeConfigError::InvalidUrl(
                format!("{} cannot be empty", field_name)
            ));
        }

        Ok(())
    }

    fn validate_url(&self, url: &str, field_name: &str, valid_schemes: &[&str]) -> Result<(), TapeConfigError> {
        let has_valid_scheme = valid_schemes.iter().any(|scheme| url.starts_with(scheme));
        
        if !has_valid_scheme {
            return Err(TapeConfigError::InvalidUrl(
                format!("{} must start with one of {:?}, found: '{}'", field_name, valid_schemes, url)
            ));
        }

        if url.contains(' ') {
            return Err(TapeConfigError::InvalidUrl(
                format!("{} cannot contain spaces, found: '{}'", field_name, url)
            ));
        }

        if url.trim().is_empty() {
            return Err(TapeConfigError::InvalidUrl(
                format!("{} cannot be empty", field_name)
            ));
        }

        Ok(())
    }

    /// create default configuration and save to file
    pub fn create_default() -> Result<Self, TapeConfigError> {
        let config = Self::default();
        let toml_string = toml::to_string_pretty(&config)
            .map_err(|e| TapeConfigError::DefaultConfigCreationFailed(format!("Serialization failed: {}", e)))?;
            
        let home_dir = dirs::home_dir()
            .ok_or(TapeConfigError::HomeDirectoryNotFound)?;
        let config_path = home_dir.join("tape.devnet.toml");
        
        fs::write(config_path, toml_string)
            .map_err(|e| TapeConfigError::DefaultConfigCreationFailed(format!("Write failed: {}", e)))?;
            
        Ok(config)
    }
}

pub fn get_default_config_path() -> Result<PathBuf, TapeConfigError> {
    let home_dir = dirs::home_dir()
        .ok_or(TapeConfigError::HomeDirectoryNotFound)?;
    Ok(home_dir.join("tape.devnet.toml"))
}

    pub fn expand_path<P: AsRef<Path>>(path: P) -> PathBuf {
        let path_str = path.as_ref().to_string_lossy();
        let expanded = shellexpand::tilde(&path_str);
        PathBuf::from(expanded.as_ref())
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
            performance: PerformanceConfig{
                num_cores: num_cpus::get(),
                max_memory_mb: 16384,
                max_poa_threads: 4,
                max_pow_threads: 4
            },
            performance: PerformanceConfig{
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

[performance]
num_cores = 4                    
max_memory_mb = 16384            
max_poa_threads = 4
max_pow_threads = 4

[performance]
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

#[derive(Debug)]
pub enum TapeConfigError {
    ConfigFileNotFound,
    CustomConfigFileNotFound(String),
    InvalidUrl(String), 
    KeypairNotFound(String), 
    HomeDirectoryNotFound,
    FileReadError(std::io::Error),
    ParseError(toml::de::Error),
    DefaultConfigCreationFailed(String),
}

impl fmt::Display for TapeConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TapeConfigError::ConfigFileNotFound => write!(f, "Configuration file not found"),
            TapeConfigError::CustomConfigFileNotFound(path) => write!(f, "Configuration file not found at path: {}", path),
            TapeConfigError::InvalidUrl(msg) => write!(f, "Invalid URL configuration: {}", msg),
            TapeConfigError::KeypairNotFound(path) => write!(f, "Keypair not found at path: {}", path),
            TapeConfigError::HomeDirectoryNotFound => write!(f, "Home directory not found"),
            TapeConfigError::FileReadError(e) => write!(f, "Failed to read config file: {}", e),
            TapeConfigError::ParseError(e) => write!(f, "Failed to parse config file: {}", e),
            TapeConfigError::DefaultConfigCreationFailed(msg) => write!(f, "Failed to create default config: {}", msg),
        }
    }
}

impl std::error::Error for TapeConfigError {}
