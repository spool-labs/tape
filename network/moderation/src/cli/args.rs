//! Command-line arguments

use std::path::PathBuf;
use std::str::FromStr;

use clap::{Parser, Subcommand};
use tape_crypto::address::Address;

/// Tapedrive's blacklist producer
#[derive(Debug, Parser)]
#[command(name = "tape-moderation", version)]
pub struct Cli {
    /// The YAML config file
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the report API, check worker, publisher and funder
    Serve,
    /// Queue removal of a track
    Remove {
        #[command(subcommand)]
        target: TrackTarget,
    },
    /// Queue unlisting of a tape
    Unlist {
        #[command(subcommand)]
        target: TapeTarget,
    },
    /// Undo this producer's decisions on a track or tape
    Restore {
        #[command(subcommand)]
        target: AnyTarget,
    },
    /// Create the list's rack and first blacklist tape, resuming an interrupted run, and print the subscription
    CreateRack,
    /// Print pending jobs, queued decisions, the rack and its blacklist tapes
    Status,
}

#[derive(Debug, Subcommand)]
pub enum TrackTarget {
    Track {
        #[arg(value_parser = parse_address)]
        address: Address,
    },
}

#[derive(Debug, Subcommand)]
pub enum TapeTarget {
    Tape {
        #[arg(value_parser = parse_address)]
        address: Address,
    },
}

#[derive(Debug, Subcommand)]
pub enum AnyTarget {
    Track {
        #[arg(value_parser = parse_address)]
        address: Address,
    },
    Tape {
        #[arg(value_parser = parse_address)]
        address: Address,
    },
}

fn parse_address(value: &str) -> Result<Address, String> {
    Address::from_str(value).map_err(|_| format!("`{value}` is not a base58 address"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRACK: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

    // operator commands parse with the config before or after them
    #[test]
    fn commands() {
        let remove = Cli::try_parse_from(["tape-moderation", "remove", "track", TRACK, "--config", "c.yaml"]).expect("remove");
        let restore = Cli::try_parse_from(["tape-moderation", "--config", "c.yaml", "restore", "tape", TRACK]).expect("restore");
        let create = Cli::try_parse_from(["tape-moderation", "create-rack", "--config", "c.yaml"]).expect("create rack");

        assert!(matches!(remove.command, Command::Remove { target: TrackTarget::Track { .. } }));
        assert!(matches!(restore.command, Command::Restore { target: AnyTarget::Tape { .. } }));
        assert!(matches!(create.command, Command::CreateRack));
        assert_eq!(restore.config, Some(PathBuf::from("c.yaml")));
    }

    // remove takes only tracks, unlist only tapes, and addresses must parse
    #[test]
    fn refused() {
        for arguments in [
            vec!["tape-moderation", "remove", "tape", TRACK],
            vec!["tape-moderation", "unlist", "track", TRACK],
            vec!["tape-moderation", "remove", "track", "not-an-address"],
        ] {
            assert!(Cli::try_parse_from(arguments).is_err());
        }
    }
}
