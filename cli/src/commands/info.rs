use anyhow::Result;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::Keypair};
use crate::cli::{Cli, Commands, InfoCommands};
use crate::log;
use tape_client as tapedrive;
use tape_api::utils::from_name;
use tape_client::TapeHeader;

use super::network::resolve_miner;

pub async fn handle_info_commands(cli: Cli, client: RpcClient, payer: Keypair) -> Result<()> {
    match cli.command {
        Commands::Info(info) => {
            match info {
                InfoCommands::Archive {} => {
                    let (archive, _address) = tapedrive::get_archive_account(&client).await?;
                    log::print_section_header("Archive Account");
                    log::print_message(&format!("Tapes: {}", archive.tapes_stored));
                    log::print_message(&format!("Bytes Stored: {}", archive.bytes_stored));
                }
                InfoCommands::Epoch {} => {
                    let (epoch, _address) = tapedrive::get_epoch_account(&client).await?;
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
                    let (block, _address) = tapedrive::get_block_account(&client).await?;
                    log::print_section_header("Block Account");
                    log::print_message(&format!("Current Block: {}", block.number));
                    log::print_message(&format!("Progress: {}", block.progress));
                    log::print_message(&format!("Challenge: {:?}", block.challenge));
                    log::print_message(&format!("Challenge Set: {}", block.challenge_set));
                    log::print_message(&format!("Last Proof At: {}", block.last_proof_at));
                    log::print_message(&format!("Last Block At: {}", block.last_block_at));
                }
                InfoCommands::FindTape { number } => {
                    let res = tapedrive::find_tape_account(&client, number).await?;
                    match res {
                        Some((tape_address, _tape_account)) => {
                            log::print_section_header("Tape Address");
                            log::print_message(&format!("Tape Number: {}", number));
                            log::print_message(&format!("Address: {}", tape_address));
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
                    let (tape, _) = tapedrive::get_tape_account(&client, &tape_address).await?;
                    let header = TapeHeader::try_from_bytes(&tape.header)?;

                    log::print_section_header("Tape Account");
                    log::print_message(&format!("Id: {}", tape.number));
                    log::print_message(&format!("Name: {}", from_name(&tape.name)));
                    log::print_message(&format!("Address: {}", tape_address));
                    log::print_message(&format!("Authority: {}", tape.authority));
                    log::print_message(&format!("Merkle Seed: {:?}", tape.merkle_seed));
                    log::print_message(&format!("Merkle Root: {:?}", tape.merkle_root));
                    log::print_message(&format!("First Slot: {}", tape.first_slot));
                    log::print_message(&format!("Tail Slot: {}", tape.tail_slot));
                    log::print_message(&format!("Total Segments: {}", tape.total_segments));
                    log::print_message(&format!("Total Size: {} bytes", tape.total_size));
                    log::print_message(&format!("State: {}", tape.state));
                    log::print_message(&format!("{:?}", header));
                    log::print_divider();
                }

                InfoCommands::Miner { pubkey, name } => {
                    let miner_address = resolve_miner(&client, &payer, pubkey, name, false).await?;
                    let (miner, _) = tapedrive::get_miner_account(&client, &miner_address).await?;
                    log::print_section_header("Miner Account");
                    log::print_message(&format!("Name: {}", from_name(&miner.name)));
                    log::print_message(&format!("Address: {}", miner_address));
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
        _ => {}
    }
    Ok(())
}
