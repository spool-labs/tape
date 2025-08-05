use chrono::Local;
use serde::{Deserialize, Serialize};
use steel::*;
use tape_api::instruction::{
    tape as tape_ix,
    miner as miner_ix,
    spool as spool_ix,
    program as program_ix,
};
use std::path::PathBuf;
use std::fs::{read_to_string, write};
use std::path::Path;
use sha2::{Sha256, Digest};
use solana_sdk::{signature::Keypair, signer::Signer, transaction::Transaction};
use solana_compute_budget::compute_budget::ComputeBudget;
use litesvm::{types::{TransactionMetadata, TransactionResult}, LiteSVM};
use pretty_hex::*;
use bincode;

use crate::setup_environment;

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ProgramIx {
    ProgramInitialize,
    #[cfg(feature = "airdrop")]
    Airdrop,
    // Tape instructions
    TapeCreate,
    TapeWrite,
    TapeUpdate,
    TapeFinalize,
    TapeSetHeader,
    TapeSubsidize,
    // Miner instructions
    MinerRegister,
    MinerUnregister,
    MinerMine,
    MinerClaim,
    // Spool instructions
    SpoolCreate,
    SpoolDestroy,
    SpoolPack,
    SpoolUnpack,
    SpoolCommit,
}

impl ToString for ProgramIx {
    fn to_string(&self) -> String {
        match self {
            ProgramIx::ProgramInitialize => "ProgramInitialize",
            #[cfg(feature = "airdrop")]
            ProgramIx::Airdrop => "Airdrop",
            
            // Tape instructions
            ProgramIx::TapeCreate => "TapeCreate",
            ProgramIx::TapeWrite => "TapeWrite",
            ProgramIx::TapeUpdate => "TapeUpdate",
            ProgramIx::TapeFinalize => "TapeFinalize",
            ProgramIx::TapeSetHeader => "TapeSetHeader",
            ProgramIx::TapeSubsidize => "TapeSubsidize",
            
            // Miner instructions
            ProgramIx::MinerRegister => "MinerRegister",
            ProgramIx::MinerUnregister => "MinerUnregister",
            ProgramIx::MinerMine => "MinerMine",
            ProgramIx::MinerClaim => "MinerClaim",
            
            // Spool instructions
            ProgramIx::SpoolCreate => "SpoolCreate",
            ProgramIx::SpoolDestroy => "SpoolDestroy",
            ProgramIx::SpoolPack => "SpoolPack",
            ProgramIx::SpoolUnpack => "SpoolUnpack",
            ProgramIx::SpoolCommit => "SpoolCommit",
        }.to_string()
    }
}

pub struct ComputeUnitsTracker(HashMap<ProgramIx, u64>);

#[derive(Serialize, Deserialize)]
struct CuLog {
    timestamp: String,
    entries: HashMap<String, CuEntry>,
    checksum: String,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
struct CuEntry {
    value: u64,
    diff: i64
}

impl Default for CuEntry{
    fn default() -> Self {
        CuEntry { value: 0, diff: 0 }
    }
}


impl ComputeUnitsTracker {
    pub fn new() -> Self {
        ComputeUnitsTracker(HashMap::new())
    }

    pub fn track_cus(&mut self, ix: ProgramIx, cus: u64){
        let ix = self.0.entry(ix).or_default();
        *ix  += cus;
    }

    fn hash_cu_log_entries(logs: &HashMap<ProgramIx, u64>) -> String {
        let mut entries: Vec<_> = logs.iter().collect();
        entries.sort_by_key(|(ix, _)| ix.to_string()); // deterministic order

        let mut hasher = Sha256::new();
        for (ix, cu) in entries {
            hasher.update(format!("{}-{}", ix.to_string(), cu));
        }
        format!("{:x}", hasher.finalize())
    }


    pub fn commit_to_change_log(&self) {
        let path = Path::new("cu_logs.json");
        let logs_hash = Self::hash_cu_log_entries(&self.0);
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

        let mut previous_entries: Option<HashMap<String, CuEntry>> = None;
        let mut all_logs: Vec<CuLog> = Vec::new();

        // Load existing JSON array
        if path.exists() {
            if let Ok(data) = read_to_string(path) {
                if let Ok(logs) = serde_json::from_str::<Vec<CuLog>>(&data) {
                    if let Some(last) = logs.first() {
                        let prev_map = last.entries.iter()
                            .map(|(k, v)| (k.clone(), *v))
                            .collect();
                        previous_entries = Some(prev_map);
                    }
                    all_logs = logs;
                }
            }
        }

        // Skip if checksum already exists
        if all_logs.iter().any(|entry| entry.checksum == logs_hash) {
            return;
        }

        // Calculate diffs
        let mut entries = HashMap::new();
        for (ix, cu) in &self.0 {
            let name = ix.to_string();
            let old = previous_entries
                .as_ref()
                .and_then(|prev| prev.get(&name))
                .cloned()
                .unwrap_or(CuEntry::default());
            let diff = if old.value > 0 {
                *cu as i64 - old.value as i64
            } else {
                0
            };

            entries.insert(name, CuEntry { 
                value: *cu, 
                diff,
            });
        }

        // Prepend new log entry
        let new_log = CuLog {
            timestamp,
            checksum: logs_hash,
            entries,
        };
        all_logs.insert(0, new_log);

        // Truncate to max 5 entries
        if all_logs.len() > 5 {
            all_logs.truncate(5);
        }

        // Write full array back to file
        let output = serde_json::to_string_pretty(&all_logs).expect("serialization failed");
        write(path, output).expect("write failed");
    }

}

pub struct SvmWithCUTracker{
    pub svm: LiteSVM,
    pub cu_tracker: ComputeUnitsTracker,
    pub payer: Keypair
}

impl SvmWithCUTracker{
    pub fn new() -> Self {
        let (svm, payer) = setup_environment();
        Self { svm, cu_tracker: ComputeUnitsTracker::new(), payer }
    }

    pub fn payer(&self) -> &Keypair {
        &self.payer
    }

    pub fn track_cus_consumed(&mut self, ix: ProgramIx, cus: u64){
        self.cu_tracker.track_cus(ix, cus);
    }

    pub fn commit_cus_change_log(&self) {
        self.cu_tracker.commit_to_change_log();
    }

}


pub fn program_bytes() -> Vec<u8> {
   // Fetch the tape program bytes from target
   let mut so_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
   so_path.push("../target/deploy/tape.so");
   std::fs::read(so_path).unwrap()
}

pub fn metadata_bytes() -> Vec<u8> {
    // Fetch the metadata program bytes from elfs/ dir before running the test
    // solana program dump --url mainnet-beta metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s metadata.so
    let mut so_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    so_path.push("tests/elfs/metadata.so");
    std::fs::read(so_path).unwrap()
}

pub fn setup_svm() -> LiteSVM {
    let mut svm = LiteSVM::new().with_compute_budget(ComputeBudget {
        compute_unit_limit: 1_000_000,
        ..Default::default()
    });

    svm.add_program(mpl_token_metadata::ID, &metadata_bytes());
    svm.add_program(tape_api::ID, &program_bytes());

    svm
}

pub fn send_tx(svm: &mut LiteSVM, tx: Transaction) -> TransactionResult {
    let res = svm.send_transaction(tx.clone());

    let meta = match res.as_ref() {
        Ok(v) => v.clone(),
        Err(v) => v.meta.clone()
    };

    print_tx(meta, tx);

    if res.is_err() {
        println!("error:\t{:?}", res.as_ref().err().unwrap().err);
    }

    res.clone()
}

pub fn create_payer(svm: &mut LiteSVM) -> Keypair {
    let payer_kp = Keypair::new();
    let payer_pk = payer_kp.pubkey();
    svm.airdrop(&payer_pk, 1_000_000_000).unwrap();
    payer_kp
}

pub fn create_keypair() -> Keypair {
    Keypair::new()
}

pub fn get_tx_size(tx: &Transaction) -> usize {
    bincode::serialize(tx).unwrap().len()
}

pub fn print_tx(meta: TransactionMetadata, tx: Transaction) {
    let msg = tx.message().serialize();

    println!("\n");
    println!("--------------------------------------------------------------------------------");
    println!("sig:\t{:?}", meta.signature);
    println!("len:\t{:?}", msg.len());

    for i in 0..tx.message.instructions.len() {
        let ix = &tx.message.instructions[i];

        let discriminator = ix.data[0];
        let ix_type = if let Ok(instruction) = program_ix::ProgramInstruction::try_from_primitive(discriminator) {
            format!("ProgramInstruction::{:?}", instruction)
        } else if let Ok(instruction) = tape_ix::TapeInstruction::try_from_primitive(discriminator) {
            format!("TapeInstruction::{:?}", instruction)
        } else if let Ok(instruction) = miner_ix::MinerInstruction::try_from_primitive(discriminator) {
            format!("MinerInstruction::{:?}", instruction)
        } else if let Ok(instruction) = spool_ix::SpoolInstruction::try_from_primitive(discriminator) {
            format!("SpoolInstruction::{:?}", instruction)
        } else {
            format!("Invalid (discriminator: {})", discriminator)
        };


        println!("\nix:\t{:?} ({})", ix_type, ix.data[0]);
        println!("accounts:");

        for key in &ix.accounts {
            println!("\t{}: {:?}", key, tx.message.account_keys[*key as usize]);
        }

        println!("\ndata:\n\t{:?}", ix.data);
        println!("\n\n{}\n", pretty_hex(&ix.data))
    }

    println!();
    println!("size:\t{:?}", get_tx_size(&tx));
    println!("cu:\t{:?}", meta.compute_units_consumed);
    println!("logs:");
    for log in &meta.logs {
        println!("\t{log:?}");
    }
    println!();
}
