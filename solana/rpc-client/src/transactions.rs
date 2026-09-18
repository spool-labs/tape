use crate::client::RpcClient;
use tape_rpc::{CommitmentLevel, Rpc, RpcError};
use solana_hash::Hash;
use solana_instruction::Instruction;
use solana_message::v1::{Message, TransactionConfig, MAX_TRANSACTION_SIZE};
use solana_message::VersionedMessage;
use solana_pubkey::Pubkey as SolanaPubkey;
use solana_signature::Signature as SolanaSignature;
use solana_signer::{Signer as SolanaSigner, SignerError as SolanaSignerError};
use solana_transaction::versioned::VersionedTransaction;
use tape_api::compute::{
    DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT, MAX_COMPUTE_UNIT_LIMIT,
    MAX_LOADED_ACCOUNTS_DATA_SIZE, MEASURED_CU_HEADROOM_PERCENT,
};
use tape_crypto::signer::Signer as TapeSigner;
use tape_crypto::tx::Txid;

struct SolanaSignerAdapter<'a>(&'a dyn TapeSigner);

impl SolanaSigner for SolanaSignerAdapter<'_> {
    fn try_pubkey(&self) -> Result<SolanaPubkey, SolanaSignerError> {
        Ok(self.0.pubkey().into())
    }

    fn try_sign_message(&self, message: &[u8]) -> Result<SolanaSignature, SolanaSignerError> {
        Ok(self.0.sign(message).to_bytes().into())
    }

    fn is_interactive(&self) -> bool {
        false
    }
}

/// Per-call knobs for one transaction submission.
#[derive(Clone, Copy)]
struct SubmitOptions {
    /// Wait for the transaction to reach `commitment` before returning.
    confirm: bool,
    /// Commitment the confirmation waits for.
    commitment: CommitmentLevel,
    /// Skip the RPC node's own preflight simulation.
    skip_preflight: bool,
    /// Simulate before sending, and drop the send if the simulation fails.
    simulate_first: bool,
    /// Metrics label for this operation.
    operation: &'static str,
}

impl SubmitOptions {
    /// Confirmed send, preflight skipped, no pre-send simulation. Override
    /// single fields with struct update syntax.
    fn new(commitment: CommitmentLevel, operation: &'static str) -> Self {
        Self {
            confirm: true,
            commitment,
            skip_preflight: true,
            simulate_first: false,
            operation,
        }
    }
}

impl<R: Rpc> RpcClient<R> {
    /// Build and send a transaction from instructions
    ///
    /// This is the primary method for submitting transactions to the Tape program.
    /// It handles:
    /// - Fetching the latest blockhash
    /// - Building and signing the transaction
    /// - Sending and confirming the transaction
    ///
    /// # Arguments
    /// * `payer` - The keypair that will pay for and sign the transaction
    /// * `instructions` - The instructions to include in the transaction
    ///
    /// # Returns
    /// The confirmed transaction signature
    ///
    /// # Errors
    /// Returns an error if:
    /// - The blockhash cannot be fetched
    /// - The transaction fails to send
    /// - The transaction fails to confirm
    /// - The transaction simulation fails
    pub async fn send_instructions(
        &self,
        payer: &dyn TapeSigner,
        instructions: Vec<Instruction>,
    ) -> Result<Txid, RpcError> {
        self.submit(
            payer,
            &[],
            default_compute_unit_limit(&instructions),
            &instructions,
            SubmitOptions::new(self.rpc().commitment(), "send_instructions"),
        )
        .await
    }

    /// Send instructions under a fixed compute unit limit, and if the program
    /// exceeds it, measure the real cost by simulation and resend once with
    /// the measured limit plus margin.
    ///
    /// Static limits can fall short at runtime: on-chain address derivation
    /// costs vary with the bump each account happens to need, so an epoch can
    /// draw an address that makes an instruction deterministically exceed a
    /// budget that held for every prior epoch.
    pub async fn send_instructions_with_compute_unit_limit(
        &self,
        payer: &dyn TapeSigner,
        compute_unit_limit: u32,
        instructions: Vec<Instruction>,
    ) -> Result<Txid, RpcError> {
        self.send_capped(
            payer,
            &[],
            compute_unit_limit,
            instructions,
            SubmitOptions::new(self.rpc().commitment(), "send_instructions"),
        )
        .await
    }

    /// Send a transaction with custom signers
    ///
    /// Use this when you need additional signers beyond the payer.
    /// The payer is automatically included as the first signer.
    ///
    /// # Arguments
    /// * `payer` - The keypair that will pay for the transaction
    /// * `instructions` - The instructions to include in the transaction
    /// * `signers` - Additional signers required by the instructions
    ///
    /// # Returns
    /// The confirmed transaction signature
    ///
    /// # Errors
    /// Returns an error if:
    /// - The blockhash cannot be fetched
    /// - The transaction fails to send
    /// - The transaction fails to confirm
    /// - The transaction simulation fails
    pub async fn send_instructions_with_signers(
        &self,
        payer: &dyn TapeSigner,
        instructions: Vec<Instruction>,
        signers: &[&dyn TapeSigner],
    ) -> Result<Txid, RpcError> {
        self.submit(
            payer,
            signers,
            default_compute_unit_limit(&instructions),
            &instructions,
            SubmitOptions::new(self.rpc().commitment(), "send_instructions_with_signers"),
        )
        .await
    }

    /// Compute-unit-limited send. `commitment` and `skip_preflight` are
    /// per-call: hot write paths pass a fast commitment and skip preflight,
    /// other paths keep preflight. Routes through `send_capped`, which on
    /// budget exhaustion measures the real cost by simulation and resends once.
    pub async fn send_instructions_with_signers_and_compute_unit_limit(
        &self,
        payer: &dyn TapeSigner,
        compute_unit_limit: u32,
        instructions: Vec<Instruction>,
        signers: &[&dyn TapeSigner],
        commitment: CommitmentLevel,
        skip_preflight: bool,
    ) -> Result<Txid, RpcError> {
        self.send_capped(
            payer,
            signers,
            compute_unit_limit,
            instructions,
            SubmitOptions {
                skip_preflight,
                ..SubmitOptions::new(commitment, "send_instructions_with_signers")
            },
        )
        .await
    }

    /// Simulate the instructions first and only send when the simulation
    /// succeeds.
    ///
    /// Contended protocol transactions are submitted by several committee
    /// members at once. Whoever loses the race would otherwise land a failing
    /// transaction and pay its fee for nothing. A failed simulation is
    /// returned in the same shape a landed failure produces, so callers
    /// classify it and pace their retries the same way.
    pub async fn simulate_then_send(
        &self,
        payer: &dyn TapeSigner,
        instructions: Vec<Instruction>,
    ) -> Result<Txid, RpcError> {
        self.submit(
            payer,
            &[],
            default_compute_unit_limit(&instructions),
            &instructions,
            SubmitOptions {
                simulate_first: true,
                ..SubmitOptions::new(self.rpc().commitment(), "simulate_then_send")
            },
        )
        .await
    }

    /// `simulate_then_send` under a fixed compute unit limit. Budget
    /// exhaustion is caught by the simulation, so the measured-limit resend in
    /// `send_capped` now costs nothing to trigger.
    pub async fn simulate_then_send_with_compute_unit_limit(
        &self,
        payer: &dyn TapeSigner,
        compute_unit_limit: u32,
        instructions: Vec<Instruction>,
    ) -> Result<Txid, RpcError> {
        self.send_capped(
            payer,
            &[],
            compute_unit_limit,
            instructions,
            SubmitOptions {
                simulate_first: true,
                ..SubmitOptions::new(self.rpc().commitment(), "simulate_then_send")
            },
        )
        .await
    }

    /// Send a transaction without waiting for confirmation
    ///
    /// Use this when you want to send the transaction and continue immediately
    /// without waiting for confirmation. You can check the status later using
    /// `get_signature_status` on the RPC client.
    ///
    /// # Arguments
    /// * `payer` - The keypair that will pay for and sign the transaction
    /// * `instructions` - The instructions to include in the transaction
    ///
    /// # Returns
    /// The transaction signature (not yet confirmed)
    ///
    /// # Errors
    /// Returns an error if:
    /// - The blockhash cannot be fetched
    /// - The transaction fails to send
    pub async fn send_instructions_async(
        &self,
        payer: &dyn TapeSigner,
        instructions: Vec<Instruction>,
    ) -> Result<Txid, RpcError> {
        self.submit(
            payer,
            &[],
            default_compute_unit_limit(&instructions),
            &instructions,
            SubmitOptions {
                confirm: false,
                ..SubmitOptions::new(self.rpc().commitment(), "send_instructions_async")
            },
        )
        .await
    }

    /// Send a transaction with custom signers without waiting for confirmation
    ///
    /// # Arguments
    /// * `payer` - The keypair that will pay for the transaction
    /// * `instructions` - The instructions to include in the transaction
    /// * `signers` - Additional signers required by the instructions
    ///
    /// # Returns
    /// The transaction signature (not yet confirmed)
    ///
    /// # Errors
    /// Returns an error if:
    /// - The blockhash cannot be fetched
    /// - The transaction fails to send
    pub async fn send_instructions_with_signers_async(
        &self,
        payer: &dyn TapeSigner,
        instructions: Vec<Instruction>,
        signers: &[&dyn TapeSigner],
    ) -> Result<Txid, RpcError> {
        self.submit(
            payer,
            signers,
            default_compute_unit_limit(&instructions),
            &instructions,
            SubmitOptions {
                confirm: false,
                ..SubmitOptions::new(
                    self.rpc().commitment(),
                    "send_instructions_with_signers_async",
                )
            },
        )
        .await
    }

    /// Send the batch under a compute unit limit, and on budget exhaustion
    /// measure the real cost by simulation and resend once.
    async fn send_capped(
        &self,
        payer: &dyn TapeSigner,
        signers: &[&dyn TapeSigner],
        compute_unit_limit: u32,
        instructions: Vec<Instruction>,
        options: SubmitOptions,
    ) -> Result<Txid, RpcError> {
        let mut attempted = None;
        let result = self
            .submit_reporting(
                payer,
                signers,
                compute_unit_limit,
                &instructions,
                options,
                &mut attempted,
            )
            .await;

        let Err(err) = &result else {
            return result;
        };
        if !err.is_compute_budget_exceeded() {
            return result;
        }

        // The resend signs a second, independently valid transaction: a
        // different compute unit limit and a fresh blockhash, so both can land.
        // Ask what became of the first signature before signing another.
        //
        // `Ok(None)` still resends: it means the RPC has not seen the
        // signature, which leaves a transaction in flight but not yet visible
        // as the remaining exposure. Refusing there would delete the
        // measured-limit path entirely.
        if let Some(attempted) = attempted {
            match self.rpc().get_signature_status(&attempted).await {
                // Unseen, or landed as a failure, which moved nothing.
                Ok(None) | Ok(Some(Err(_))) => {}
                Ok(Some(Ok(()))) => {
                    tracing::warn!(
                        %attempted,
                        "compute budget exceeded was reported for a transaction that landed; \
                         not resending"
                    );
                    return result;
                }
                Err(error) => {
                    tracing::warn!(
                        %attempted, %error,
                        "cannot confirm the failed transaction did not land; not resending"
                    );
                    return result;
                }
            }
        }

        let Some(measured) = self
            .measured_compute_unit_limit(payer, signers, &instructions)
            .await
        else {
            return result;
        };

        tracing::warn!(
            requested = compute_unit_limit,
            measured,
            "compute budget exceeded, resending with measured limit"
        );

        self.submit(payer, signers, measured, &instructions, options)
            .await
    }

    /// Simulate a signed transaction and report an execution failure as the
    /// rejection the caller would have got by sending it.
    ///
    /// None when the simulation passed, and also when the simulation itself
    /// could not be run: an unreachable simulate endpoint must not stop the
    /// node from submitting, and a real send reports the truth either way.
    ///
    /// The simulation runs at the client's commitment rather than through the
    /// RPC node's preflight, which defaults to the finalized bank and would
    /// reject transactions whose precondition landed seconds ago.
    async fn simulation_rejection(
        &self,
        transaction: &VersionedTransaction,
    ) -> Option<RpcError> {
        let simulation = match self.rpc().simulate_transaction(transaction).await {
            Ok(simulation) => simulation,
            Err(err) => {
                tracing::debug!(%err, "simulation unavailable, sending anyway");
                return None;
            }
        };

        let err = simulation.err?;
        let message = err.to_string();
        tracing::debug!(%message, "simulation failed, transaction not sent");
        Some(RpcError::Transaction {
            err: Some(err),
            message,
            simulated: true,
        })
    }

    /// Simulate the batch at the runtime ceiling and return the consumed units
    /// plus headroom, or None if simulation cannot produce a usable
    /// measurement.
    async fn measured_compute_unit_limit(
        &self,
        payer: &dyn TapeSigner,
        signers: &[&dyn TapeSigner],
        instructions: &[Instruction],
    ) -> Option<u32> {
        // Simulation replaces or ignores the blockhash, so the probe signs
        // against the default one instead of fetching a fresh one.
        let transaction = sign_transaction(
            payer,
            signers,
            MAX_COMPUTE_UNIT_LIMIT,
            instructions,
            Hash::default(),
        )
        .ok()?;

        let simulation = self.rpc().simulate_transaction(&transaction).await.ok()?;
        if simulation.err.is_some() {
            return None;
        }

        let consumed = simulation.units_consumed?;
        let limit = consumed
            .saturating_add(consumed * MEASURED_CU_HEADROOM_PERCENT / 100)
            .min(MAX_COMPUTE_UNIT_LIMIT as u64);
        Some(limit as u32)
    }

    /// Build, sign, and send one transaction, recording metrics under the
    /// given operation label. Confirmed sends wait for the signature status;
    /// async sends return as soon as the transaction is accepted.
    async fn submit(
        &self,
        payer: &dyn TapeSigner,
        signers: &[&dyn TapeSigner],
        compute_unit_limit: u32,
        instructions: &[Instruction],
        options: SubmitOptions,
    ) -> Result<Txid, RpcError> {
        self.submit_reporting(
            payer,
            signers,
            compute_unit_limit,
            instructions,
            options,
            &mut None,
        )
        .await
    }

    /// `submit`, additionally reporting the signature it put on the wire.
    ///
    /// `attempted` is set as soon as the transaction is signed, so a caller
    /// that wants to retry has a signature to ask about even when the send
    /// itself reports a failure.
    async fn submit_reporting(
        &self,
        payer: &dyn TapeSigner,
        signers: &[&dyn TapeSigner],
        compute_unit_limit: u32,
        instructions: &[Instruction],
        options: SubmitOptions,
        attempted: &mut Option<Txid>,
    ) -> Result<Txid, RpcError> {
        #[cfg(not(feature = "metrics"))]
        let _ = options.operation;
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let transaction = self
                .build_signed_transaction(payer, signers, compute_unit_limit, instructions)
                .await?;
            *attempted = transaction
                .signatures
                .first()
                .map(|signature| Txid::new((*signature).into()));

            if options.simulate_first {
                if let Some(rejection) = self.simulation_rejection(&transaction).await {
                    return Err(rejection);
                }
            }

            if options.confirm {
                self.rpc()
                    .send_and_confirm_transaction(
                        &transaction,
                        options.commitment,
                        options.skip_preflight,
                    )
                    .await
            } else {
                self.rpc().send_transaction(&transaction).await
            }
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            match &result {
                Ok(_) => {
                    metrics.record_transaction_success();
                    if let Some(timer) = &timer {
                        if options.confirm {
                            metrics.record_transaction_confirmation("confirmed", timer);
                        }
                        metrics.record_operation(options.operation, "success", timer);
                    }
                }
                Err(_) => {
                    metrics.record_transaction_error();
                    if let Some(timer) = &timer {
                        if options.confirm {
                            metrics.record_transaction_confirmation("error", timer);
                        }
                        metrics.record_operation(options.operation, "error", timer);
                    }
                }
            }
        }

        result
    }

    /// Sign instructions into a transaction against the latest blockhash,
    /// with the payer as the first signer.
    async fn build_signed_transaction(
        &self,
        payer: &dyn TapeSigner,
        signers: &[&dyn TapeSigner],
        compute_unit_limit: u32,
        instructions: &[Instruction],
    ) -> Result<VersionedTransaction, RpcError> {
        let blockhash = self.rpc().get_latest_blockhash().await?;
        sign_transaction(payer, signers, compute_unit_limit, instructions, blockhash)
    }
}

/// Sign instructions into a v1 transaction with the payer as the first signer.
pub fn sign_transaction(
    payer: &dyn TapeSigner,
    signers: &[&dyn TapeSigner],
    compute_unit_limit: u32,
    instructions: &[Instruction],
    blockhash: Hash,
) -> Result<VersionedTransaction, RpcError> {
    let payer_pubkey: SolanaPubkey = payer.pubkey().into();

    let mut all_signers: Vec<SolanaSignerAdapter<'_>> = Vec::with_capacity(signers.len() + 1);
    all_signers.push(SolanaSignerAdapter(payer));
    for signer in signers {
        if all_signers.iter().any(|existing| existing.0.pubkey() == signer.pubkey()) {
            continue;
        }
        all_signers.push(SolanaSignerAdapter(*signer));
    }

    let message = Message::try_compile_with_config(
        &payer_pubkey,
        instructions,
        blockhash,
        transaction_config(compute_unit_limit),
    )
    .map_err(|err| RpcError::Internal(format!("compile transaction: {err}")))?;

    let transaction = VersionedTransaction::try_new(VersionedMessage::V1(message), &all_signers)
        .map_err(|err| RpcError::Internal(format!("sign transaction: {err}")))?;

    // The in-memory test chain accepts any size, so the cap is enforced here.
    let len = transaction_len(&transaction)?;
    if len > MAX_TRANSACTION_SIZE {
        return Err(RpcError::Internal(format!(
            "transaction is {len} bytes, over the {MAX_TRANSACTION_SIZE} byte limit"
        )));
    }

    Ok(transaction)
}

/// Wire size of a signed transaction.
pub fn transaction_len(transaction: &VersionedTransaction) -> Result<usize, RpcError> {
    let len = wincode::serialized_size(transaction)
        .map_err(|err| RpcError::Internal(format!("measure transaction: {err}")))?;
    usize::try_from(len).map_err(|err| RpcError::Internal(format!("measure transaction: {err}")))
}

/// A v1 message carries its budget in the config, and a limit left out of the
/// config is zero, so every transaction sets both.
fn transaction_config(compute_unit_limit: u32) -> TransactionConfig {
    TransactionConfig::empty()
        .with_compute_unit_limit(compute_unit_limit)
        .with_loaded_accounts_data_size_limit(MAX_LOADED_ACCOUNTS_DATA_SIZE)
}

/// Budget for a send that sets no limit of its own.
fn default_compute_unit_limit(instructions: &[Instruction]) -> u32 {
    let count = u32::try_from(instructions.len()).unwrap_or(u32::MAX);
    DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT
        .saturating_mul(count)
        .min(MAX_COMPUTE_UNIT_LIMIT)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::time::Duration;

    use super::*;
    use tape_rpc_solana::RpcConfig;
    use solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient;
    use solana_keypair::Keypair as SolanaKeypair;
    use solana_pubkey::Pubkey;
    use solana_system_interface::instruction as system_instruction;
    use tape_crypto::ed25519::Keypair;

    const LOCAL_VALIDATOR: &str = "http://127.0.0.1:8899";
    const MEMO_PROGRAM: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

    // The memo program spends nearly the whole compute ceiling on a payload
    // this size, so these sends run at MAX_COMPUTE_UNIT_LIMIT.
    fn memo(len: usize) -> Instruction {
        Instruction {
            program_id: Pubkey::from_str(MEMO_PROGRAM).expect("memo program id"),
            accounts: Vec::new(),
            data: vec![b'a'; len],
        }
    }

    // A delegate can also pay the fee, and repeated keys must produce only one signature each.
    #[test]
    fn repeated_signers() {
        let payer = Keypair::from_keypair_bytes(SolanaKeypair::new().to_bytes()).expect("payer");
        let other = Keypair::from_keypair_bytes(SolanaKeypair::new().to_bytes()).expect("other signer");
        for source in [&payer, &other] {
            let instruction = system_instruction::transfer(&source.pubkey().into(), &Pubkey::new_unique(), 1);
            let transaction = sign_transaction(
                &payer,
                &[&payer, source, source],
                MAX_COMPUTE_UNIT_LIMIT,
                &[instruction],
                Hash::default(),
            ).expect("sign with repeated keys");

            let expected = if source.pubkey() == payer.pubkey() { 1 } else { 2 };
            assert_eq!(transaction.signatures.len(), expected);
            transaction.verify_and_hash_message().expect("valid signatures");
        }
    }

    // Sign with no size check, so the validator is the one to refuse.
    fn sign_unchecked(payer: &Keypair, instruction: Instruction, blockhash: Hash) -> VersionedTransaction {
        let message = Message::try_compile_with_config(
            &payer.pubkey().into(),
            &[instruction],
            blockhash,
            transaction_config(MAX_COMPUTE_UNIT_LIMIT),
        )
        .expect("compile");
        VersionedTransaction::try_new(VersionedMessage::V1(message), &[SolanaSignerAdapter(payer)])
            .expect("sign")
    }

    // A v1 transaction of exactly MAX_TRANSACTION_SIZE lands on a real
    // validator and one byte more is refused. Needs Agave 4.2.2 or later.
    #[tokio::test]
    #[ignore] // Requires solana-test-validator on 127.0.0.1:8899
    async fn v1_size_boundary() {
        let client = RpcClient::new(RpcConfig {
            endpoints: vec![LOCAL_VALIDATOR.to_string()],
            ..RpcConfig::default()
        })
        .expect("client");

        let solana_payer = SolanaKeypair::new();
        let payer = Keypair::from_keypair_bytes(solana_payer.to_bytes()).expect("convert payer");
        let payer_pubkey: Pubkey = payer.pubkey().into();

        let faucet = SolanaRpcClient::new(LOCAL_VALIDATOR.to_string());
        faucet
            .request_airdrop(&payer_pubkey, 1_000_000_000)
            .await
            .expect("airdrop");
        while faucet.get_balance(&payer_pubkey).await.expect("balance") == 0 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        let empty = transaction_len(&sign_unchecked(&payer, memo(0), Hash::default()))
            .expect("measure");
        let largest = MAX_TRANSACTION_SIZE - empty;

        let fits = sign_transaction(&payer, &[], MAX_COMPUTE_UNIT_LIMIT, &[memo(largest)], Hash::default())
            .expect("sign largest");
        assert_eq!(transaction_len(&fits).expect("measure"), MAX_TRANSACTION_SIZE);

        client
            .send_instructions_with_compute_unit_limit(
                &payer,
                MAX_COMPUTE_UNIT_LIMIT,
                vec![memo(largest)],
            )
            .await
            .expect("a full-size v1 transaction should land");

        let blockhash = client.rpc().get_latest_blockhash().await.expect("blockhash");
        let oversized = sign_unchecked(&payer, memo(largest + 1), blockhash);
        assert_eq!(transaction_len(&oversized).expect("measure"), MAX_TRANSACTION_SIZE + 1);
        let refused = client
            .rpc()
            .send_transaction(&oversized)
            .await
            .expect_err("validator accepted an oversized transaction");
        assert!(refused.to_string().contains("too large"), "refused for another reason: {refused}");
    }

    #[tokio::test]
    #[ignore] // Requires actual RPC endpoint
    async fn test_send_instructions() {
        let config = RpcConfig::default();
        let client = RpcClient::new(config).unwrap();

        let solana_payer = SolanaKeypair::new();
        let payer = Keypair::from_keypair_bytes(solana_payer.to_bytes()).expect("convert payer");
        let to = Pubkey::new_unique();
        let payer_pubkey = payer.pubkey().into();

        let instruction = system_instruction::transfer(&payer_pubkey, &to, 1000);

        // This would fail without funds, but tests the API
        let result = client.send_instructions(&payer, vec![instruction]).await;
        // Expected to fail due to insufficient funds
        assert!(result.is_err());
    }
}
