use crate::client::RpcClient;
use solana_client::rpc_config::RpcProgramAccountsConfig;
use solana_client::rpc_filter::{Memcmp, RpcFilterType};
use solana_sdk::pubkey::Pubkey;
use rpc::{Rpc, RpcError};

// Import tape-api types
use tape_api::prelude::*;
use tape_api::state::{AccountType, Archive, Epoch, History, Node, Stake, System, Tape, Track};
use tape_api::program::tapedrive::{
    self, SYSTEM_ADDRESS, EPOCH_ADDRESS, ARCHIVE_ADDRESS,
    node_pda, stake_pda, tape_pda, track_pda, history_pda,
};

// Import tape-core types for ID lookups
use tape_core::types::{NodeId, TapeNumber, TrackNumber};

impl<R: Rpc> RpcClient<R> {
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
    /// * `authority` - The authority public key of the stake
    pub async fn get_stake(
        &self,
        authority: &Pubkey,
    ) -> Result<Stake, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let (address, _bump) = stake_pda(*authority);
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

    // ========================================================================
    // ID-based Lookups
    // ========================================================================
    // These methods find accounts by their unique ID field using getProgramAccounts
    // with memcmp filters. More expensive than direct PDA lookup but necessary
    // when only the ID is known.

    /// Find a Node account by its NodeId.
    ///
    /// Uses getProgramAccounts with a memcmp filter on the id field.
    /// This is necessary when you only have the NodeId (e.g., from CommitteeMember)
    /// and need to look up the Node's network address or other metadata.
    ///
    /// # Arguments
    /// * `node_id` - The unique NodeId assigned when the node registered
    ///
    /// # Returns
    /// The Node account address and data, or an error if not found.
    pub async fn get_node_by_id(&self, node_id: NodeId) -> Result<(Pubkey, Node), RpcError> {
        // Account layout: [discriminator (1 byte)][id (8 bytes)]...
        // Filter on both discriminator and id field
        let id_bytes = node_id.as_u64().to_le_bytes();

        let config = RpcProgramAccountsConfig {
            filters: Some(vec![
                // Filter by discriminator at offset 0
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                    0,
                    vec![AccountType::Node as u8],
                )),
                // Filter by NodeId at offset 1
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                    1,
                    id_bytes.to_vec(),
                )),
            ]),
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
            .next()
            .map(|(pubkey, account)| {
                let node = Node::unpack_with_discriminator(&account.data)
                    .map(|n| *n)
                    .map_err(|e| RpcError::Deserialization(e.to_string()))?;
                Ok((pubkey, node))
            })
            .unwrap_or(Err(RpcError::Internal(format!("Node not found with id {}", node_id))))
    }

    /// Find a Tape account by its TapeNumber.
    ///
    /// Uses getProgramAccounts with a memcmp filter on the id field.
    ///
    /// # Arguments
    /// * `tape_number` - The unique TapeNumber assigned when the tape was created
    ///
    /// # Returns
    /// The Tape account address and data, or an error if not found.
    pub async fn get_tape_by_number(&self, tape_number: TapeNumber) -> Result<(Pubkey, Tape), RpcError> {
        // Account layout: [discriminator (1 byte)][id (8 bytes)]...
        let id_bytes = tape_number.as_u64().to_le_bytes();

        let config = RpcProgramAccountsConfig {
            filters: Some(vec![
                // Filter by discriminator at offset 0
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                    0,
                    vec![AccountType::Tape as u8],
                )),
                // Filter by TapeNumber at offset 1
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                    1,
                    id_bytes.to_vec(),
                )),
            ]),
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
            .next()
            .map(|(pubkey, account)| {
                let tape = Tape::unpack_with_discriminator(&account.data)
                    .map(|t| *t)
                    .map_err(|e| RpcError::Deserialization(e.to_string()))?;
                Ok((pubkey, tape))
            })
            .unwrap_or(Err(RpcError::Internal(format!("Tape not found with number {}", tape_number))))
    }

    /// Find a Track account by its TrackNumber.
    ///
    /// Uses getProgramAccounts with a memcmp filter on the id field.
    ///
    /// # Arguments
    /// * `track_number` - The unique TrackNumber assigned when the track was created
    ///
    /// # Returns
    /// The Track account address and data, or an error if not found.
    pub async fn get_track_by_number(&self, track_number: TrackNumber) -> Result<(Pubkey, Track), RpcError> {
        // Account layout: [discriminator (1 byte)][id (8 bytes)]...
        let id_bytes = track_number.as_u64().to_le_bytes();

        let config = RpcProgramAccountsConfig {
            filters: Some(vec![
                // Filter by discriminator at offset 0
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                    0,
                    vec![AccountType::Track as u8],
                )),
                // Filter by TrackNumber at offset 1
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                    1,
                    id_bytes.to_vec(),
                )),
            ]),
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
            .next()
            .map(|(pubkey, account)| {
                let track = Track::unpack_with_discriminator(&account.data)
                    .map(|t| *t)
                    .map_err(|e| RpcError::Deserialization(e.to_string()))?;
                Ok((pubkey, track))
            })
            .unwrap_or(Err(RpcError::Internal(format!("Track not found with number {}", track_number))))
    }
}
