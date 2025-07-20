use clap::{Parser, Subcommand};
use std::str::FromStr;
use std::path::PathBuf;

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

    #[arg(short = 'k', long = "keypair", global = true)]
    pub keypair_path: Option<PathBuf>,

    #[arg(
        short = 'u', 
        long = "cluster", 
        default_value = "l", 
        global = true,
        help = "Cluster to use: l (localnet), m (mainnet), d (devnet), t (testnet),\n or a custom RPC URL"
    )]
    pub cluster: Cluster,

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
        #[arg(help = "Starting slot to archive from, defaults to the latest slot")]
        starting_slot: Option<u64>,

        #[arg(help = "Trusted peer to connect to")]
        trusted_peer: Option<String>,
    },
    Mine {
        #[arg(help = "Miner account public key", conflicts_with = "name")]
        pubkey: Option<String>,

        #[arg(help = "Name of the miner you're mining with", conflicts_with = "pubkey", short = 'n', long = "name")]
        name: Option<String>,
    },
    Web {
        #[arg(help = "Port to run the web RPC service on")]
        port: Option<u16>,
    },

    // Admin Commands

    #[command(hide = true)]
    Initialize {},

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
        tape: String,
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
        tape: String,

        #[arg(short = 'o', long = "output", help = "Output file")]
        output: Option<String>,

        #[arg(short = 'r', long = "raw", help = "Output raw segments instead of decoded tape")]
        raw: bool,
    },

    GetSegment {
        #[arg(help = "Tape account public key")]
        tape: String,

        #[arg(help = "Segment index (0 to tape size - 1)")]
        index: u32,
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

        #[arg(help = "Name of the miner you're mining with", conflicts_with = "pubkey", short = 'n', long = "name")]
        name: Option<String>,
    },

    Archive {},
    Epoch {},
    Block {},
}

#[derive(Debug, Clone)]
pub enum Cluster {
    Localnet,
    Mainnet,
    Devnet,
    Testnet,
    Custom(String),
}

impl Cluster {
    pub fn rpc_url(&self) -> String {
        match self {
            Cluster::Localnet => "http://127.0.0.1:8899".to_string(),
            Cluster::Mainnet => "https://api.mainnet-beta.solana.com".to_string(),
            Cluster::Devnet => "https://api.devnet.solana.com".to_string(),
            Cluster::Testnet => "https://api.testnet.solana.com".to_string(),
            Cluster::Custom(url) => url.clone(),
        }
    }
}

impl FromStr for Cluster {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "l" => Ok(Cluster::Localnet),
            "m" => Ok(Cluster::Mainnet),
            "d" => Ok(Cluster::Devnet),
            "t" => Ok(Cluster::Testnet),
            s if s.starts_with("http://") || s.starts_with("https://") => Ok(Cluster::Custom(s.to_string())),
            _ => Err(format!(
                "Invalid cluster value: '{}'. Use l, m, d, t, or a valid RPC URL (http:// or https://)",
                s
            )),
        }
    }
}
