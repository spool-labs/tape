
use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use crate::cli::{Cli, Commands, Context, InfoCommands};
use crate::log;
use tape_client as tapedrive;
use tape_api::utils::from_name;
use tape_client::TapeHeader;

use super::network::resolve_miner;

pub async fn handle_info_commands(cli: Cli, context: Context) -> Result<()> {
    if let Commands::Info(info) = cli.command {
        match info {
            InfoCommands::Archive {} => {
                let (archive, _address) = tapedrive::get_archive_account(context.rpc()).await?;
                log::print_section_header("Archive Account");
                log::print_message(&format!("Tapes: {}", archive.tapes_stored));
                log::print_message(&format!("Segments: {}", archive.segments_stored));
                log::print_message(&format!("Bytes: {}", archive.bytes_stored));
            }
            InfoCommands::Epoch {} => {
                let (epoch, _address) = tapedrive::get_epoch_account(context.rpc()).await?;
                log::print_section_header("Epoch Account");
                log::print_message(&format!("Current Epoch: {}", epoch.number));
                log::print_message(&format!("Progress: {}", epoch.progress));
                log::print_message(&format!("Target Difficulty: {}", epoch.target_difficulty));
                log::print_message(&format!("Target Participation: {}", epoch.target_participation));
                log::print_message(&format!("Reward Rate: {}", epoch.reward_rate));
                log::print_message(&format!("Duplicates: {}", epoch.duplicates));
                log::print_message(&format!("Last Epoch At: {}", epoch.last_epoch_at));
            }
            InfoCommands::Block {} => {
                let (block, _address) = tapedrive::get_block_account(context.rpc()).await?;
                log::print_section_header("Block Account");
                log::print_message(&format!("Current Block: {}", block.number));
                log::print_message(&format!("Progress: {}", block.progress));
                log::print_message(&format!("Challenge: {:?}", block.challenge));
                log::print_message(&format!("Challenge Set: {}", block.challenge_set));
                log::print_message(&format!("Last Proof At: {}", block.last_proof_at));
                log::print_message(&format!("Last Block At: {}", block.last_block_at));
            }
            InfoCommands::FindTape { number } => {
                let res = tapedrive::find_tape_account(context.rpc(), number).await?;
                match res {
                    Some((tape_address, _tape_account)) => {
                        log::print_section_header("Tape Address");
                        log::print_message(&format!("Tape Number: {number}"));
                        log::print_message(&format!("Address: {tape_address}"));
                        log::print_divider();
                    }
                    None => {
                        log::print_error("Tape not found");
                        return Ok(());
                    }
                }
            }
            InfoCommands::Tape { pubkey } => {
                let tape_address: Pubkey = pubkey.parse()?;
                let (tape, _) = tapedrive::get_tape_account(context.rpc(), &tape_address).await?;

                log::print_section_header("Tape Account");
                log::print_message(&format!("Id: {}", tape.number));
                log::print_message(&format!("Name: {}", from_name(&tape.name)));
                log::print_message(&format!("Address: {tape_address}"));
                log::print_message(&format!("Authority: {}", tape.authority));
                log::print_message(&format!("Merkle Seed: {:?}", tape.merkle_seed));
                log::print_message(&format!("Merkle Root: {:?}", tape.merkle_root));
                log::print_message(&format!("First Slot: {}", tape.first_slot));
                log::print_message(&format!("Tail Slot: {}", tape.tail_slot));
                log::print_message(&format!("Balance: {}", tape.balance));
                log::print_message(&format!("Last Rent Block: {}", tape.last_rent_block));
                log::print_message(&format!("Total Segments: {}", tape.total_segments));
                log::print_message(&format!("Total Size: {} bytes", tape.total_size));
                log::print_message(&format!("State: {}", tape.state));

                if let Ok(header) = TapeHeader::try_from_bytes(&tape.header) {
                    log::print_message(&format!("Header: {header:?}"));
                }

                log::print_divider();
            }

            InfoCommands::Miner { pubkey, name } => {
                let miner_address = resolve_miner(context.rpc(), context.payer(), pubkey, name, false).await?;
                let (miner, _) = tapedrive::get_miner_account(context.rpc(), &miner_address).await?;
                log::print_section_header("Miner Account");
                log::print_message(&format!("Name: {}", from_name(&miner.name)));
                log::print_message(&format!("Address: {miner_address}"));
                log::print_message(&format!("Owner: {}", miner.authority));
                log::print_message(&format!("Unclaimed Rewards: {}", miner.unclaimed_rewards));
                log::print_message(&format!("Challenge: {:?}", miner.challenge));
                log::print_message(&format!("Multiplier: {}", miner.multiplier));
                log::print_message(&format!("Last Proof Block: {}", miner.last_proof_block));
                log::print_message(&format!("Last Proof At: {}", miner.last_proof_at));
                log::print_message(&format!("Total Proofs: {}", miner.total_proofs));
                log::print_message(&format!("Total Rewards: {}", miner.total_rewards));
                log::print_divider();
            }
        }
    }
    Ok(())
}
