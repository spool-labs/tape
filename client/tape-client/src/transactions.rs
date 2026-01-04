use crate::client::TapeClient;
use solana_sdk::instruction::Instruction;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_sdk::transaction::Transaction;
use tape_rpc::{Rpc, RpcError};

impl<R: Rpc> TapeClient<R> {
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
        payer: &Keypair,
        instructions: Vec<Instruction>,
    ) -> Result<Signature, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            // Fetch the latest blockhash
            let blockhash = self.rpc().get_latest_blockhash().await?;

            // Build and sign the transaction
            let transaction = Transaction::new_signed_with_payer(
                &instructions,
                Some(&payer.pubkey()),
                &[payer],
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
        payer: &Keypair,
        instructions: Vec<Instruction>,
        signers: &[&Keypair],
    ) -> Result<Signature, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            // Fetch the latest blockhash
            let blockhash = self.rpc().get_latest_blockhash().await?;

            // Combine payer with additional signers
            let mut all_signers: Vec<&Keypair> = vec![payer];
            all_signers.extend(signers);

            // Build and sign the transaction
            let transaction = Transaction::new_signed_with_payer(
                &instructions,
                Some(&payer.pubkey()),
                &all_signers,
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
        payer: &Keypair,
        instructions: Vec<Instruction>,
    ) -> Result<Signature, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            // Fetch the latest blockhash
            let blockhash = self.rpc().get_latest_blockhash().await?;

            // Build and sign the transaction
            let transaction = Transaction::new_signed_with_payer(
                &instructions,
                Some(&payer.pubkey()),
                &[payer],
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
        payer: &Keypair,
        instructions: Vec<Instruction>,
        signers: &[&Keypair],
    ) -> Result<Signature, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            // Fetch the latest blockhash
            let blockhash = self.rpc().get_latest_blockhash().await?;

            // Combine payer with additional signers
            let mut all_signers: Vec<&Keypair> = vec![payer];
            all_signers.extend(signers);

            // Build and sign the transaction
            let transaction = Transaction::new_signed_with_payer(
                &instructions,
                Some(&payer.pubkey()),
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
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::system_instruction;

    #[tokio::test]
    #[ignore] // Requires actual RPC endpoint
    async fn test_send_instructions() {
        let config = tape_rpc::RpcConfig::default();
        let client = TapeClient::new(config).unwrap();

        let payer = Keypair::new();
        let to = Pubkey::new_unique();

        let instruction = system_instruction::transfer(&payer.pubkey(), &to, 1000);

        // This would fail without funds, but tests the API
        let result = client.send_instructions(&payer, vec![instruction]).await;
        // Expected to fail due to insufficient funds
        assert!(result.is_err());
    }
}
