use solana_sdk::signature::Keypair;
use std::path::PathBuf;
use anyhow::{Result, anyhow};
use std::fs;

pub fn create_keypair(path: &PathBuf) -> Result<Keypair> {
    let keypair = Keypair::new();
    let bytes = keypair.to_bytes().to_vec();
    let json = serde_json::to_string(&bytes)
        .map_err(|e| anyhow!("Failed to serialize keypair to JSON: {}", e))?;
    fs::write(path, json)
        .map_err(|e| anyhow!("Failed to write keypair file {}: {}", path.display(), e))?;
    Ok(keypair)
}

pub fn load_keypair(path: &PathBuf) -> Result<Keypair> {
    let data = fs::read_to_string(path)
        .map_err(|e| anyhow!("Failed to read keypair file {}: {}", path.display(), e))?;
    let bytes: Vec<u8> = serde_json::from_str(&data)
        .map_err(|e| anyhow!("Failed to parse keypair JSON: {}", e))?;
    Keypair::from_bytes(&bytes)
        .map_err(|e| anyhow!("Failed to create keypair from bytes: {}", e))
}

pub fn get_payer(keypair_path: PathBuf) -> Result<Keypair> {
    let payer = match load_keypair(&keypair_path) {
        Ok(payer) => payer,
        Err(_) => {
            create_keypair(&keypair_path)?
        }
    };
    Ok(payer)
}
