use anyhow::Result;
use clap::{Parser, Subcommand};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use tape_network::store::TapeStore;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use crate::keypair::get_payer;
use crate::config::TapeConfig;

#[derive(Parser)]
#[command(
    name = "tapedrive",
    about = "Your data, permanently recorded — uncensorable, uneditable, and here for good.",
    arg_required_else_help = true,
    version = env!("CARGO_PKG_VERSION")
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[arg(short = 'c', long = "config", help = "Path to config file (overrides default)", global = true)]
    pub config: Option<PathBuf>,

    #[arg(short = 'k', long = "keypair", global = true)]
    pub keypair_path: Option<PathBuf>,

    #[arg(short = 'v', long = "verbose", help = "Print verbose output", global = true)]
    pub verbose: bool,
}

#[derive(Subcommand)]
pub enum Commands {

    // Tape Commands

    Read {
        #[arg(help = "Tape account to read")]
        tape: String,

        #[arg(short = 'o', long = "output", help = "Output file")]
        output: Option<String>,
    },

    Write {
        #[arg(
            help = "File to write, message text, or remote URL",
            required_unless_present_any = ["filename", "message", "remote"],
            conflicts_with_all = ["message", "remote"]
        )]
        filename: Option<String>,

        #[arg(short = 'm', long = "message", conflicts_with_all = ["filename", "remote"])]
        message: Option<String>,

        #[arg(short = 'r', long = "remote", conflicts_with_all = ["filename", "message"])]
        remote: Option<String>,

        #[arg(short = 'n', long = "tape-name", help = "Custom name for the tape (defaults to timestamp)")]
        tape_name: Option<String>,
    },


    // Miner Commands

    #[command(hide = true)]
    Register {
        #[arg(help = "The name of the miner you're registering")]
        name: String,
    },

    Claim {
        #[arg(help = "Miner account public key")]
        miner: String,

        #[arg(help = "Amount of tokens to claim")]
        amount: u64,
    },

    // Node Commands

    Archive {
        #[arg(help = "Trusted peer to connect to", short = 'p', long = "peer")]
        trusted_peer: Option<String>,

        #[arg(help = "Miner account public key", short = 'm', long = "miner")]
        miner_address: Option<String>,
    },
    Mine {
        #[arg(help = "Miner account public key", conflicts_with = "name")]
        pubkey: Option<String>,
    },
    Web {
        #[arg(help = "Port to run the web RPC service on")]
        port: Option<u16>,
    },

    // Admin Commands

    #[command(hide = true)]
    Init {},

    Airdrop {
        #[arg(help = "Amount of tokens to airdrop")]
        amount: u64,
    },

    // Store Management Commands

    #[command(subcommand)]
    Snapshot(SnapshotCommands),

    // Info Commands

    #[command(subcommand)]
    Info(InfoCommands),

}

#[derive(Subcommand)]
pub enum SnapshotCommands {
    Stats {},

    Resync {
        #[arg(help = "Tape account public key to re-sync")]
        tape_address: String,

        #[arg(help = "Miner account public key", short = 'm', long = "miner")]
        miner_address: Option<String>,
    },

    Create {
        #[arg(help = "Output path for the snapshot file (defaults to a timestamped file in current directory)")]
        output: Option<String>,
    },

    Load {
        #[arg(help = "Path to the snapshot file to load")]
        input: String,
    },

    GetTape {
        #[arg(help = "Tape account public key")]
        tape_address: String,

        #[arg(short = 'o', long = "output", help = "Output file")]
        output: Option<String>,

        #[arg(short = 'r', long = "raw", help = "Output raw segments instead of decoded tape")]
        raw: bool,

        #[arg(help = "Miner account public key", short = 'm', long = "miner")]
        miner_address: Option<String>,
    },

    GetSegment {
        #[arg(help = "Tape account public key")]
        tape_address: String,

        #[arg(help = "Segment index (0 to tape size - 1)")]
        index: u32,

        #[arg(help = "Miner account public key", short = 'm', long = "miner")]
        miner_address: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum InfoCommands {
    Tape {
        #[arg(help = "Tape account public key")]
        pubkey: String,
    },
    FindTape {
        #[arg(help = "Tape number to find")]
        number: u64,
    },
    Miner {
        #[arg(help = "Miner account public key", conflicts_with = "name")]
        pubkey: Option<String>,
    },

    Archive {},
    Epoch {},
    Block {},
}


pub struct Context {
    pub config: Arc<TapeConfig>,
    pub rpc: Arc<RpcClient>,
    pub payer: Keypair
}

impl Context{
    pub fn try_build(cli:&Cli) -> Result<Self> {
        
        // loading up configs
        let config = Arc::new(TapeConfig::load(&cli.config)?);

        let rpc_url = config.solana.rpc_url.to_string();
        let commitment_level = config.solana.commitment.to_commitment_config();
        let rpc = Arc::new(
            RpcClient::new_with_commitment(rpc_url.clone(),
            commitment_level)
        );

        let keypair_path = config.solana.keypair_path();
        let payer = get_payer(keypair_path.clone())?;
        
        Ok(Self {
             config,
             rpc,
             payer
        })

    }

    pub fn keypair_path(&self) -> PathBuf {
        self.config.solana.keypair_path()
    }

    pub fn rpc(&self) -> &Arc<RpcClient>{
        &self.rpc
    }

    pub fn open_primary_store_conn(&self) -> Result<TapeStore> {
        let rocksdb_config = self.config.storage.rocksdb.as_ref()
            .ok_or_else(|| anyhow::anyhow!("RocksDB config not found"))?;
        Ok(tape_network::store::primary(&rocksdb_config.primary_path)?)
    }

    pub fn open_secondary_store_conn_mine(&self) -> Result<TapeStore> {
        let rocksdb_config = self.config.storage.rocksdb.as_ref()
            .ok_or_else(|| anyhow::anyhow!("RocksDB config not found"))?;
        Ok(tape_network::store::secondary_mine(&rocksdb_config.primary_path, &rocksdb_config.secondary_path_mine)?)
    }

    pub fn open_secondary_store_conn_web(&self) -> Result<TapeStore> {
        let rocksdb_config = self.config.storage.rocksdb.as_ref()
            .ok_or_else(|| anyhow::anyhow!("RocksDB config not found"))?;
        Ok(tape_network::store::secondary_web(&rocksdb_config.primary_path, &rocksdb_config.secondary_path_web)?)
    }

    pub fn open_read_only_store_conn(&self) -> Result<TapeStore> {
        let rocksdb_config = self.config.storage.rocksdb.as_ref()
            .ok_or_else(|| anyhow::anyhow!("RocksDB config not found"))?;
        Ok(tape_network::store::read_only(&rocksdb_config.primary_path)?)
    }


    pub fn payer(&self) -> &Keypair{
        &self.payer
    }
    
    pub fn max_transaction_retries(&self) -> u32 {
        self.config.solana.max_transaction_retries
    }

    pub fn miner_name_owned(&self) -> String {
        self.config.mining.miner_name.clone()
    }
}
