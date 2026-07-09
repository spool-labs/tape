use crate::client::RpcClient;
use crate::compute::with_compute_unit_limit;
use rpc::{Rpc, RpcError};
use solana_instruction::Instruction;
use solana_pubkey::Pubkey as SolanaPubkey;
use solana_signature::Signature as SolanaSignature;
use solana_signer::{Signer as SolanaSigner, SignerError as SolanaSignerError};
use solana_transaction::Transaction;
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
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            // Fetch the latest blockhash
            let blockhash = self.rpc().get_latest_blockhash().await?;
            let payer_pubkey: SolanaPubkey = payer.pubkey().into();
            let signers = [SolanaSignerAdapter(payer)];

            // Build and sign the transaction
            let transaction = Transaction::new_signed_with_payer(
                &instructions,
                Some(&payer_pubkey),
                &signers,
                blockhash,
            );

            // Send and confirm the transaction
            self.rpc().send_and_confirm_transaction(&transaction).await
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            match &result {
                Ok(_) => {
                    metrics.record_transaction_success();
                    if let Some(timer) = &timer {
                        metrics.record_transaction_confirmation("confirmed", timer);
                        metrics.record_operation("send_instructions", "success", timer);
                    }
                }
                Err(_) => {
                    metrics.record_transaction_error();
                    if let Some(timer) = &timer {
                        metrics.record_transaction_confirmation("error", timer);
                        metrics.record_operation("send_instructions", "error", timer);
                    }
                }
            }
        }

        result
    }

    pub async fn send_instructions_with_compute_unit_limit(
        &self,
        payer: &dyn TapeSigner,
        compute_unit_limit: u32,
        instructions: Vec<Instruction>,
    ) -> Result<Txid, RpcError> {
        let ix = with_compute_unit_limit(
            compute_unit_limit, 
            instructions
        );

        self.send_instructions(payer, ix)
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
        self.send_instructions_with_signers_inner(payer, instructions, signers, false)
            .await
    }

    /// Build, sign, and send-and-confirm, optionally skipping preflight. Shared
    /// by the preflighted path and the hot-write-path skip variant so both keep
    /// the same metrics.
    async fn send_instructions_with_signers_inner(
        &self,
        payer: &dyn TapeSigner,
        instructions: Vec<Instruction>,
        signers: &[&dyn TapeSigner],
        skip_preflight: bool,
    ) -> Result<Txid, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let blockhash = self.rpc().get_latest_blockhash().await?;
            let payer_pubkey: SolanaPubkey = payer.pubkey().into();

            let mut all_signers: Vec<SolanaSignerAdapter<'_>> =
                Vec::with_capacity(signers.len() + 1);
            all_signers.push(SolanaSignerAdapter(payer));
            all_signers.extend(signers.iter().copied().map(SolanaSignerAdapter));

            let transaction = Transaction::new_signed_with_payer(
                &instructions,
                Some(&payer_pubkey),
                &all_signers,
                blockhash,
            );

            if skip_preflight {
                self.rpc()
                    .send_and_confirm_transaction_skip_preflight(&transaction)
                    .await
            } else {
                self.rpc().send_and_confirm_transaction(&transaction).await
            }
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            match &result {
                Ok(_) => {
                    metrics.record_transaction_success();
                    if let Some(timer) = &timer {
                        metrics.record_transaction_confirmation("confirmed", timer);
                        metrics.record_operation("send_instructions_with_signers", "success", timer);
                    }
                }
                Err(_) => {
                    metrics.record_transaction_error();
                    if let Some(timer) = &timer {
                        metrics.record_transaction_confirmation("error", timer);
                        metrics.record_operation("send_instructions_with_signers", "error", timer);
                    }
                }
            }
        }

        result
    }

    pub async fn send_instructions_with_signers_and_compute_unit_limit(
        &self,
        payer: &dyn TapeSigner,
        compute_unit_limit: u32,
        instructions: Vec<Instruction>,
        signers: &[&dyn TapeSigner],
    ) -> Result<Txid, RpcError> {
        self.send_instructions_with_signers(
            payer,
            with_compute_unit_limit(compute_unit_limit, instructions),
            signers,
        )
        .await
    }

    /// Like the preflighted compute-unit-limit variant, but skips preflight
    /// simulation. For the latency-sensitive hot write path only; a rejection
    /// here lands on chain (paid) rather than failing simulation, so callers must
    /// be paths where rejections are not expected in steady state.
    pub async fn send_instructions_with_signers_and_compute_unit_limit_skip_preflight(
        &self,
        payer: &dyn TapeSigner,
        compute_unit_limit: u32,
        instructions: Vec<Instruction>,
        signers: &[&dyn TapeSigner],
    ) -> Result<Txid, RpcError> {
        self.send_instructions_with_signers_inner(
            payer,
            with_compute_unit_limit(compute_unit_limit, instructions),
            signers,
            true,
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
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            // Fetch the latest blockhash
            let blockhash = self.rpc().get_latest_blockhash().await?;
            let payer_pubkey: SolanaPubkey = payer.pubkey().into();
            let signers = [SolanaSignerAdapter(payer)];

            // Build and sign the transaction
            let transaction = Transaction::new_signed_with_payer(
                &instructions,
                Some(&payer_pubkey),
                &signers,
                blockhash,
            );

            // Send without waiting for confirmation
            self.rpc().send_transaction(&transaction).await
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            match &result {
                Ok(_) => {
                    metrics.record_transaction_success();
                    if let Some(timer) = &timer {
                        metrics.record_operation("send_instructions_async", "success", timer);
                    }
                }
                Err(_) => {
                    metrics.record_transaction_error();
                    if let Some(timer) = &timer {
                        metrics.record_operation("send_instructions_async", "error", timer);
                    }
                }
            }
        }

        result
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
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            // Fetch the latest blockhash
            let blockhash = self.rpc().get_latest_blockhash().await?;
            let payer_pubkey: SolanaPubkey = payer.pubkey().into();

            // Combine payer with additional signers
            let mut all_signers: Vec<SolanaSignerAdapter<'_>> =
                Vec::with_capacity(signers.len() + 1);
            all_signers.push(SolanaSignerAdapter(payer));
            all_signers.extend(signers.iter().copied().map(SolanaSignerAdapter));

            // Build and sign the transaction
            let transaction = Transaction::new_signed_with_payer(
                &instructions,
                Some(&payer_pubkey),
                &all_signers,
                blockhash,
            );

            // Send without waiting for confirmation
            self.rpc().send_transaction(&transaction).await
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            match &result {
                Ok(_) => {
                    metrics.record_transaction_success();
                    if let Some(timer) = &timer {
                        metrics.record_operation("send_instructions_with_signers_async", "success", timer);
                    }
                }
                Err(_) => {
                    metrics.record_transaction_error();
                    if let Some(timer) = &timer {
                        metrics.record_operation("send_instructions_with_signers_async", "error", timer);
                    }
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rpc_solana::RpcConfig;
    use solana_keypair::Keypair as SolanaKeypair;
    use solana_pubkey::Pubkey;
    use solana_system_interface::instruction as system_instruction;
    use tape_crypto::ed25519::Keypair;

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
