//! The gate checks each public request against the tape CLI's RPC surface
//! and each submitted transaction against the programs it may call
//!
//! See `docs/public-rpc.md`.

use serde_json::Value;
use solana_message::VersionedMessage;
use solana_transaction_status::{EncodedTransaction, TransactionBinaryEncoding};

use tape_api::program::tapedrive::{
    ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID, PROGRAM_ID, SPL_TOKEN_PROGRAM_ID,
};
use tape_crypto::address::Address;

/// Agave's own cap on one `getMultipleAccounts`
const MAX_ACCOUNTS: usize = 100;
/// Agave's own cap on one `getSignatureStatuses`
const MAX_SIGNATURES: usize = 256;

const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

/// Why the gate turned a request away
///
/// The CLI retries every error that `RpcError::is_retriable` reads as
/// transient, so no message here may read that way.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Refusal {
    Method,
    Batch,
    Malformed,
    Accounts,
    Signatures,
    Undecodable,
    ForeignProgram,
    NoTapedrive,
}

impl Refusal {
    /// The JSON-RPC error code sent back
    pub fn code(self) -> i64 {
        match self {
            Refusal::Method => METHOD_NOT_FOUND,
            Refusal::Batch | Refusal::Malformed => INVALID_REQUEST,
            Refusal::Accounts
            | Refusal::Signatures
            | Refusal::Undecodable
            | Refusal::ForeignProgram
            | Refusal::NoTapedrive => INVALID_PARAMS,
        }
    }

    /// The JSON-RPC error message sent back
    pub fn message(self) -> String {
        match self {
            Refusal::Method => "Method not found".into(),
            Refusal::Batch => "batch requests are unavailable on this endpoint".into(),
            Refusal::Malformed => "expected one JSON-RPC request object".into(),
            Refusal::Accounts => format!("at most {MAX_ACCOUNTS} accounts per request"),
            Refusal::Signatures => format!("at most {MAX_SIGNATURES} signatures per request"),
            Refusal::Undecodable => "transaction could not be decoded".into(),
            Refusal::ForeignProgram => "transaction calls a program this endpoint does not relay".into(),
            Refusal::NoTapedrive => "transaction has no tapedrive instruction".into(),
        }
    }
}

/// Decides which requests a public instance answers and which transactions it relays
pub struct Gate {
    tapedrive: Address,
    token_programs: [Address; 2],
}

impl Gate {
    /// A gate for the compiled tapedrive program
    pub fn new() -> Self {
        Self {
            tapedrive: Address::from(PROGRAM_ID),
            token_programs: [
                Address::from(ASSOCIATED_TOKEN_ACCOUNT_PROGRAM_ID),
                Address::from(SPL_TOKEN_PROGRAM_ID),
            ],
        }
    }

    /// Check one request against the allowlist
    pub fn check_request(&self, method: &str, params: &Value) -> Result<(), Refusal> {
        match method {
            "getAccountInfo" | "getLatestBlockhash" | "getGenesisHash" | "getTransaction" => Ok(()),
            "getMultipleAccounts" => check_list_len(params, MAX_ACCOUNTS, Refusal::Accounts),
            "getSignatureStatuses" => check_list_len(params, MAX_SIGNATURES, Refusal::Signatures),
            "sendTransaction" | "simulateTransaction" => self.check_submit(params),
            _other => Err(Refusal::Method),
        }
    }

    /// Check a `getTransaction` result before it is served or cached
    ///
    /// A null result passes, since the CLI polls until its transaction lands.
    /// Binary results arrive as `[blob, encoding]`, json and jsonParsed ones
    /// as a message object.
    pub fn check_transaction_result(&self, result: &Value) -> Result<(), Refusal> {
        if result.is_null() {
            return Ok(());
        }
        let transaction = result.get("transaction").ok_or(Refusal::Undecodable)?;
        let has_tapedrive = match transaction {
            Value::Array(encoded) => self.binary_calls_tapedrive(encoded)?,
            Value::Object(_) => self.json_calls_tapedrive(transaction)?,
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
                return Err(Refusal::Undecodable);
            }
        };
        if has_tapedrive {
            Ok(())
        } else {
            Err(Refusal::NoTapedrive)
        }
    }

    /// Whether a base64 or base58 transaction calls tapedrive at the top level
    fn binary_calls_tapedrive(&self, encoded: &[Value]) -> Result<bool, Refusal> {
        let blob = encoded
            .first()
            .and_then(Value::as_str)
            .ok_or(Refusal::Undecodable)?;
        let encoding = match encoded.get(1).and_then(Value::as_str) {
            Some("base64") => TransactionBinaryEncoding::Base64,
            Some("base58") => TransactionBinaryEncoding::Base58,
            Some(_) | None => return Err(Refusal::Undecodable),
        };
        let transaction = EncodedTransaction::Binary(blob.to_string(), encoding)
            .decode()
            .ok_or(Refusal::Undecodable)?;
        Ok(self.classify_calls(&transaction.message)?.has_tapedrive)
    }

    /// Whether a json or jsonParsed transaction calls tapedrive at the top level
    ///
    /// json gives each instruction's program as an index into the account
    /// keys; jsonParsed gives the program id itself.
    fn json_calls_tapedrive(&self, transaction: &Value) -> Result<bool, Refusal> {
        let message = transaction.get("message").ok_or(Refusal::Undecodable)?;
        let keys = message
            .get("accountKeys")
            .and_then(Value::as_array)
            .ok_or(Refusal::Undecodable)?;
        let instructions = message
            .get("instructions")
            .and_then(Value::as_array)
            .ok_or(Refusal::Undecodable)?;

        let tapedrive = self.tapedrive.to_string();
        for instruction in instructions {
            let program = match instruction.get("programId") {
                Some(program) => program.as_str(),
                None => instruction
                    .get("programIdIndex")
                    .and_then(Value::as_u64)
                    .and_then(|index| keys.get(usize::try_from(index).ok()?))
                    .and_then(Value::as_str),
            };
            if program == Some(tapedrive.as_str()) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A submitted transaction must call only relayed programs, tapedrive at least once
    fn check_submit(&self, params: &Value) -> Result<(), Refusal> {
        let blob = params
            .get(0)
            .and_then(Value::as_str)
            .ok_or(Refusal::Undecodable)?;
        let encoding = submit_encoding(params)?;
        let transaction = EncodedTransaction::Binary(blob.to_string(), encoding)
            .decode()
            .ok_or(Refusal::Undecodable)?;

        let calls = self.classify_calls(&transaction.message)?;
        if calls.has_foreign {
            return Err(Refusal::ForeignProgram);
        }
        if calls.has_tapedrive {
            Ok(())
        } else {
            Err(Refusal::NoTapedrive)
        }
    }

    /// Sort each top-level instruction by the program it calls
    ///
    /// Sanitized messages index top-level programs into their static keys,
    /// lookup tables included.
    fn classify_calls(&self, message: &VersionedMessage) -> Result<Calls, Refusal> {
        let keys = message.static_account_keys();
        let mut calls = Calls { has_tapedrive: false, has_foreign: false };
        for instruction in message.instructions() {
            let program = keys
                .get(usize::from(instruction.program_id_index))
                .ok_or(Refusal::Undecodable)?;
            let program = Address::from(program.to_bytes());
            if program == self.tapedrive {
                calls.has_tapedrive = true;
            } else if !self.token_programs.contains(&program) {
                calls.has_foreign = true;
            }
        }
        Ok(calls)
    }
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

/// Records whether top-level instructions call tapedrive or a foreign program
struct Calls {
    has_tapedrive: bool,
    has_foreign: bool,
}

/// A list in the first param holds at most `max` entries
fn check_list_len(params: &Value, max: usize, refusal: Refusal) -> Result<(), Refusal> {
    let len = params
        .get(0)
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    if len > max {
        return Err(refusal);
    }
    Ok(())
}

/// The wire encoding of a submitted transaction; agave reads base58 when none is given
fn submit_encoding(params: &Value) -> Result<TransactionBinaryEncoding, Refusal> {
    let encoding = params
        .get(1)
        .and_then(|config| config.get("encoding"))
        .and_then(Value::as_str);
    match encoding {
        None | Some("base58") => Ok(TransactionBinaryEncoding::Base58),
        Some("base64") => Ok(TransactionBinaryEncoding::Base64),
        Some(_) => Err(Refusal::Undecodable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use solana_message::v1::{Message as V1Message, TransactionConfig};
    use solana_message::{AccountMeta, Address as SolanaAddress, Hash, Instruction, Message};
    use solana_transaction::Signature;
    use solana_transaction::versioned::VersionedTransaction;
    use tape_api::helpers::build_authority_with_tokens_ix;
    use tape_api::instruction::build_reserve_tape_ix;
    use tape_core::tape::TapeFlags;
    use tape_core::types::coin::TAPE;
    use tape_core::types::{EpochNumber, StorageUnits};
    use tape_rpc::RpcError;

    const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

    fn payer() -> Address {
        Address::new_unique()
    }

    /// Sign nothing: the gate reads structure, and sanitize only counts signatures
    fn encode(message: VersionedMessage) -> String {
        let signers = usize::from(message.header().num_required_signatures);
        let transaction = VersionedTransaction {
            signatures: vec![Signature::default(); signers],
            message,
        };
        let raw = wincode::serialize(&transaction).expect("encode");
        base64::encode(raw)
    }

    /// A v1 message, which is what the CLI sends
    fn v1(payer: Address, instructions: &[Instruction]) -> String {
        let message = V1Message::try_compile_with_config(
            &(*payer.as_bytes()).into(),
            instructions,
            Hash::default(),
            TransactionConfig::empty().with_compute_unit_limit(200_000),
        )
        .expect("compile");
        encode(VersionedMessage::V1(message))
    }

    fn legacy(payer: Address, instructions: &[Instruction]) -> String {
        let message = Message::new_with_blockhash(
            instructions,
            Some(&(*payer.as_bytes()).into()),
            &Hash::default(),
        );
        encode(VersionedMessage::Legacy(message))
    }

    fn call(program: [u8; 32], payer: Address) -> Instruction {
        Instruction::new_with_bytes(
            program.into(),
            &[1, 2, 3],
            vec![AccountMeta::new((*payer.as_bytes()).into(), true)],
        )
    }

    fn system_transfer(payer: Address) -> Instruction {
        let program: SolanaAddress = SYSTEM_PROGRAM.parse().expect("system id");
        call(program.to_bytes(), payer)
    }

    fn send(blob: &str) -> Value {
        json!([blob, {"encoding": "base64", "skipPreflight": true}])
    }

    // the CLI's read methods pass
    #[test]
    fn cli_reads() {
        let gate = Gate::new();

        for method in ["getAccountInfo", "getLatestBlockhash", "getGenesisHash", "getTransaction"] {
            assert_eq!(gate.check_request(method, &json!([])), Ok(()), "{method}");
        }
    }

    // methods outside the CLI surface read as unknown, getMembership included
    #[test]
    fn unknown_methods() {
        let gate = Gate::new();

        for method in ["getBalance", "getBlock", "getSlot", "getMembership", "requestAirdrop", "getProgramAccounts"] {
            assert_eq!(gate.check_request(method, &json!([])), Err(Refusal::Method), "{method}");
        }
    }

    // list params are capped at agave's own limits
    #[test]
    fn list_caps() {
        let gate = Gate::new();
        let accounts: Vec<String> = vec!["x".into(); MAX_ACCOUNTS + 1];
        let signatures: Vec<String> = vec!["x".into(); MAX_SIGNATURES];

        let too_many = gate.check_request("getMultipleAccounts", &json!([accounts]));
        let at_cap = gate.check_request("getSignatureStatuses", &json!([signatures]));

        assert_eq!(too_many, Err(Refusal::Accounts));
        assert_eq!(at_cap, Ok(()));
    }

    // the create transaction the CLI builds is relayed
    #[test]
    fn create_tape() {
        let gate = Gate::new();
        let payer = payer();
        let tape_key = Address::new_unique();
        let mut instructions = build_authority_with_tokens_ix(payer, tape_key, TAPE(1))
            .expect("token ixs");
        instructions.push(build_reserve_tape_ix(
            payer,
            tape_key,
            StorageUnits(1 << 20),
            EpochNumber(2),
            TapeFlags::NONE,
        ));

        let verdict = gate.check_request("sendTransaction", &send(&v1(payer, &instructions)));

        assert_eq!(verdict, Ok(()));
    }

    // a tapedrive call alongside a foreign program is refused
    #[test]
    fn piggyback() {
        let gate = Gate::new();
        let payer = payer();
        let instructions = [call(PROGRAM_ID, payer), system_transfer(payer)];

        let verdict = gate.check_request("sendTransaction", &send(&v1(payer, &instructions)));

        assert_eq!(verdict, Err(Refusal::ForeignProgram));
    }

    // token instructions alone are refused
    #[test]
    fn token_only() {
        let gate = Gate::new();
        let payer = payer();
        let instructions = [call(SPL_TOKEN_PROGRAM_ID, payer)];

        let verdict = gate.check_request("simulateTransaction", &send(&legacy(payer, &instructions)));

        assert_eq!(verdict, Err(Refusal::NoTapedrive));
    }

    // agave reads base58 when a submit gives no encoding
    #[test]
    fn submit_default() {
        let unnamed = submit_encoding(&json!(["blob"]));
        let named = submit_encoding(&json!(["blob", {"encoding": "base64"}]));

        assert_eq!(unnamed, Ok(TransactionBinaryEncoding::Base58));
        assert_eq!(named, Ok(TransactionBinaryEncoding::Base64));
    }

    // bytes that are not a transaction are refused
    #[test]
    fn garbage_submit() {
        let gate = Gate::new();

        let verdict = gate.check_request("sendTransaction", &send("AAAA"));

        assert_eq!(verdict, Err(Refusal::Undecodable));
    }

    // getTransaction results must be tapedrive transactions; null passes
    #[test]
    fn transaction_result() {
        let gate = Gate::new();
        let payer = payer();
        let ours = json!({"transaction": [legacy(payer, &[call(PROGRAM_ID, payer)]), "base64"]});
        let theirs = json!({"transaction": [legacy(payer, &[system_transfer(payer)]), "base64"]});

        assert_eq!(gate.check_transaction_result(&ours), Ok(()));
        assert_eq!(gate.check_transaction_result(&theirs), Err(Refusal::NoTapedrive));
        assert_eq!(gate.check_transaction_result(&Value::Null), Ok(()));
    }

    // json results name programs by account index, jsonParsed results by id
    #[test]
    fn json_results() {
        let gate = Gate::new();
        let tapedrive = Address::from(PROGRAM_ID).to_string();
        let payer = Address::new_unique().to_string();
        let json = json!({"transaction": {"message": {
            "accountKeys": [payer, tapedrive],
            "instructions": [{"programIdIndex": 1, "accounts": [0]}],
        }}});
        let parsed = json!({"transaction": {"message": {
            "accountKeys": [{"pubkey": payer}],
            "instructions": [{"programId": tapedrive, "accounts": [payer]}],
        }}});
        let theirs = json!({"transaction": {"message": {
            "accountKeys": [payer, SYSTEM_PROGRAM],
            "instructions": [{"programIdIndex": 1, "accounts": [0]}],
        }}});

        assert_eq!(gate.check_transaction_result(&json), Ok(()));
        assert_eq!(gate.check_transaction_result(&parsed), Ok(()));
        assert_eq!(gate.check_transaction_result(&theirs), Err(Refusal::NoTapedrive));
    }

    // the CLI's own retry rule reads no refusal message as transient
    #[test]
    fn messages_not_retried() {
        let refusals = [
            Refusal::Method,
            Refusal::Batch,
            Refusal::Malformed,
            Refusal::Accounts,
            Refusal::Signatures,
            Refusal::Undecodable,
            Refusal::ForeignProgram,
            Refusal::NoTapedrive,
        ];

        for refusal in refusals {
            let error = RpcError::Request(refusal.message());
            assert!(!error.is_retriable(), "{refusal:?} reads as retriable");
        }
    }
}
