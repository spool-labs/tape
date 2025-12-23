use crate::client::TapeClient;
use solana_client::rpc_config::RpcProgramAccountsConfig;
use solana_client::rpc_filter::{Memcmp, RpcFilterType};
use solana_sdk::pubkey::Pubkey;
use tape_rpc::RpcError;

// Import tape-api types
use tape_api::prelude::*;
use tape_api::state::{AccountType, Archive, Epoch, History, Node, Stake, System, Tape, Track};
use tape_api::program::tapedrive::{
    self, SYSTEM_ADDRESS, EPOCH_ADDRESS, ARCHIVE_ADDRESS,
    node_pda, stake_pda, tape_pda, track_pda, history_pda,
};

impl TapeClient {
    // ========================================================================
    // Singleton Accounts
    // ========================================================================

    /// Fetch the System singleton account
    pub async fn get_system(&self) -> Result<System, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let account = self.rpc().get_account(&SYSTEM_ADDRESS).await?;
            System::unpack_with_discriminator(&account.data)
                .map(|s| *s)
                .map_err(|e| RpcError::Deserialization(e.to_string()))
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("system", status, timer);
            }
        }

        result
    }

    /// Fetch the Epoch singleton account
    pub async fn get_epoch(&self) -> Result<Epoch, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let account = self.rpc().get_account(&EPOCH_ADDRESS).await?;
            Epoch::unpack_with_discriminator(&account.data)
                .map(|e| *e)
                .map_err(|e| RpcError::Deserialization(e.to_string()))
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("epoch", status, timer);
            }
        }

        result
    }

    /// Fetch the Archive singleton account
    pub async fn get_archive(&self) -> Result<Archive, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let account = self.rpc().get_account(&ARCHIVE_ADDRESS).await?;
            Archive::unpack_with_discriminator(&account.data)
                .map(|a| *a)
                .map_err(|e| RpcError::Deserialization(e.to_string()))
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("archive", status, timer);
            }
        }

        result
    }

    // ========================================================================
    // Parameterized Accounts
    // ========================================================================

    /// Fetch a Node account by authority
    ///
    /// # Arguments
    /// * `authority` - The authority public key of the node
    pub async fn get_node(&self, authority: &Pubkey) -> Result<Node, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let (address, _bump) = node_pda(*authority);
            let account = self.rpc().get_account(&address).await?;
            Node::unpack_with_discriminator(&account.data)
                .map(|n| *n)
                .map_err(|e| RpcError::Deserialization(e.to_string()))
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("node", status, timer);
            }
        }

        result
    }

    /// Fetch a Stake account
    ///
    /// # Arguments
    /// * `authority` - The authority public key of the staker
    /// * `node` - The node public key being staked to
    pub async fn get_stake(
        &self,
        authority: &Pubkey,
        node: &Pubkey,
    ) -> Result<Stake, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let (address, _bump) = stake_pda(*authority, *node);
            let account = self.rpc().get_account(&address).await?;
            Stake::unpack_with_discriminator(&account.data)
                .map(|s| *s)
                .map_err(|e| RpcError::Deserialization(e.to_string()))
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("stake", status, timer);
            }
        }

        result
    }

    /// Fetch a Tape account by authority
    ///
    /// # Arguments
    /// * `authority` - The authority public key of the tape
    pub async fn get_tape(&self, authority: &Pubkey) -> Result<Tape, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let (address, _bump) = tape_pda(*authority);
            let account = self.rpc().get_account(&address).await?;
            Tape::unpack_with_discriminator(&account.data)
                .map(|t| *t)
                .map_err(|e| RpcError::Deserialization(e.to_string()))
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("tape", status, timer);
            }
        }

        result
    }

    /// Fetch a Track account
    ///
    /// # Arguments
    /// * `authority` - The authority public key of the track
    /// * `hash` - The hash of the track
    pub async fn get_track(
        &self,
        authority: &Pubkey,
        hash: &Hash,
    ) -> Result<Track, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let (address, _bump) = track_pda(*authority, *hash);
            let account = self.rpc().get_account(&address).await?;
            Track::unpack_with_discriminator(&account.data)
                .map(|t| *t)
                .map_err(|e| RpcError::Deserialization(e.to_string()))
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("track", status, timer);
            }
        }

        result
    }

    /// Fetch a History account for a node
    ///
    /// # Arguments
    /// * `node` - The node public key
    pub async fn get_history(&self, node: &Pubkey) -> Result<History, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let (address, _bump) = history_pda(*node);
            let account = self.rpc().get_account(&address).await?;
            History::unpack_with_discriminator(&account.data)
                .map(|h| *h)
                .map_err(|e| RpcError::Deserialization(e.to_string()))
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("history", status, timer);
            }
        }

        result
    }

    // ========================================================================
    // Discovery Methods
    // ========================================================================

    /// Find all nodes registered in the system
    ///
    /// WARNING: This is an expensive operation that fetches all node accounts.
    /// Use sparingly, especially on mainnet.
    pub async fn get_all_nodes(&self) -> Result<Vec<(Pubkey, Node)>, RpcError> {
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                0, // Offset 0 is the discriminator
                vec![AccountType::Node as u8],
            ))]),
            account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                commitment: Some(solana_sdk::commitment_config::CommitmentConfig {
                    commitment: self.rpc().commitment(),
                }),
                data_slice: None,
                min_context_slot: None,
            },
            with_context: None,
            sort_results: None,
        };

        let accounts = self.rpc().get_program_accounts(&tapedrive::ID, config).await?;

        accounts
            .into_iter()
            .map(|(pubkey, account)| {
                let node = Node::unpack_with_discriminator(&account.data)
                    .map(|n| *n)
                    .map_err(|e| RpcError::Deserialization(e.to_string()))?;
                Ok((pubkey, node))
            })
            .collect()
    }

    /// Find all tapes registered in the system
    ///
    /// WARNING: This is an expensive operation that fetches all tape accounts.
    /// Use sparingly, especially on mainnet.
    pub async fn get_all_tapes(&self) -> Result<Vec<(Pubkey, Tape)>, RpcError> {
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                0, // Offset 0 is the discriminator
                vec![AccountType::Tape as u8],
            ))]),
            account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                commitment: Some(solana_sdk::commitment_config::CommitmentConfig {
                    commitment: self.rpc().commitment(),
                }),
                data_slice: None,
                min_context_slot: None,
            },
            with_context: None,
            sort_results: None,
        };

        let accounts = self.rpc().get_program_accounts(&tapedrive::ID, config).await?;

        accounts
            .into_iter()
            .map(|(pubkey, account)| {
                let tape = Tape::unpack_with_discriminator(&account.data)
                    .map(|t| *t)
                    .map_err(|e| RpcError::Deserialization(e.to_string()))?;
                Ok((pubkey, tape))
            })
            .collect()
    }
}
