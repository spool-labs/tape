use crate::client::RpcClient;
use core::mem::size_of;
use solana_client::rpc_config::RpcProgramAccountsConfig;
use solana_client::rpc_filter::{Memcmp, RpcFilterType};
use solana_sdk::commitment_config::CommitmentLevel;
use rpc::{Rpc, RpcError};

use tape_api::dynamic::DynamicState;
use tape_api::state::{
    AccountType, Archive, Committee, Epoch, Group, History, Node, PeerSet, Stake, System, Tape,
};
use tape_api::program::tapedrive::{
    self, SYSTEM_ADDRESS, ARCHIVE_ADDRESS, PEER_SET_ADDRESS,
    committee_pda, epoch_pda, group_pda, history_pda, node_pda, stake_pda, tape_pda,
};

use tape_core::spooler::GroupIndex;
use tape_core::system::{Member, Peer};
use tape_core::types::{EpochNumber, NodeId, TapeNumber};
use tape_crypto::address::Address;

impl<R: Rpc> RpcClient<R> {
    // ========================================================================
    // Singleton Accounts
    // ========================================================================

    /// Fetch the System singleton account
    pub async fn get_system(&self) -> Result<System, RpcError> {
        self.get_system_with_commitment(self.rpc().commitment())
            .await
    }

    /// Fetch the System singleton account at an explicit commitment.
    pub async fn get_system_with_commitment(
        &self,
        commitment: CommitmentLevel,
    ) -> Result<System, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let account = self
                .rpc()
                .get_account_with_commitment(&SYSTEM_ADDRESS, commitment)
                .await?;

            // Check account size before unpacking to avoid panic on partially initialized accounts
            let expected_size = std::mem::size_of::<System>() + 8; // +8 for discriminator
            if account.data.len() < expected_size {
                return Err(RpcError::Deserialization(format!(
                    "System account too small: {} bytes (expected {})",
                    account.data.len(),
                    expected_size
                )));
            }

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

    /// Fetch the Epoch account for the given epoch number.
    pub async fn get_epoch(&self, epoch: EpochNumber) -> Result<Epoch, RpcError> {
        self.get_epoch_with_commitment(epoch, self.rpc().commitment())
            .await
    }

    /// Fetch the Epoch account for the given epoch number at an explicit commitment.
    pub async fn get_epoch_with_commitment(
        &self,
        epoch: EpochNumber,
        commitment: CommitmentLevel,
    ) -> Result<Epoch, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let (address, _) = epoch_pda(epoch);
        let result = async {
            let account = self
                .rpc()
                .get_account_with_commitment(&address, commitment)
                .await?;
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

    /// Fetch the active members for an epoch-scoped Committee account.
    pub async fn get_committee(&self, epoch: EpochNumber) -> Result<Vec<Member>, RpcError> {
        self.get_committee_with_commitment(epoch, self.rpc().commitment())
            .await
    }

    /// Fetch the active members for an epoch-scoped Committee account at an explicit commitment.
    pub async fn get_committee_with_commitment(
        &self,
        epoch: EpochNumber,
        commitment: CommitmentLevel,
    ) -> Result<Vec<Member>, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let (address, _) = committee_pda(epoch);
        let result = async {
            let account = self
                .rpc()
                .get_account_with_commitment(&address, commitment)
                .await?;
            let (committee, members) =
                unpack_dynamic_entries::<Committee>(&account.data, "Committee")?;

            if committee.epoch != epoch {
                return Err(RpcError::Deserialization(format!(
                    "Committee account epoch mismatch: got {}, expected {}",
                    committee.epoch, epoch
                )));
            }

            Ok(members)
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("committee", status, timer);
            }
        }

        result
    }

    /// Fetch active peer entries from the PeerSet singleton account.
    pub async fn get_peer_set(&self) -> Result<Vec<Peer>, RpcError> {
        self.get_peer_set_with_commitment(self.rpc().commitment())
            .await
    }

    /// Fetch active peer entries from the PeerSet singleton account at an explicit commitment.
    pub async fn get_peer_set_with_commitment(
        &self,
        commitment: CommitmentLevel,
    ) -> Result<Vec<Peer>, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let account = self
                .rpc()
                .get_account_with_commitment(&PEER_SET_ADDRESS, commitment)
                .await?;
            let (_, peers) = unpack_dynamic_entries::<PeerSet>(&account.data, "PeerSet")?;
            Ok(peers)
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("peer_set", status, timer);
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

    /// Fetch an epoch-scoped Group account.
    pub async fn get_group(
        &self,
        epoch: EpochNumber,
        group: GroupIndex,
    ) -> Result<Group, RpcError> {
        self.get_group_with_commitment(epoch, group, self.rpc().commitment())
            .await
    }

    /// Fetch an epoch-scoped Group account at an explicit commitment.
    pub async fn get_group_with_commitment(
        &self,
        epoch: EpochNumber,
        group: GroupIndex,
        commitment: CommitmentLevel,
    ) -> Result<Group, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let (address, _) = group_pda(epoch, group);
        let result = async {
            let account = self
                .rpc()
                .get_account_with_commitment(&address, commitment)
                .await?;
            unpack_group(&account.data, epoch, group)
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("group", status, timer);
            }
        }

        result
    }

    /// Fetch all epoch-scoped Group accounts in one deterministic batch.
    pub async fn get_groups(
        &self,
        epoch: EpochNumber,
        total_groups: u64,
    ) -> Result<Vec<Group>, RpcError> {
        self.get_groups_with_commitment(epoch, total_groups, self.rpc().commitment())
            .await
    }

    /// Fetch all epoch-scoped Group accounts in one deterministic batch at an explicit commitment.
    pub async fn get_groups_with_commitment(
        &self,
        epoch: EpochNumber,
        total_groups: u64,
        commitment: CommitmentLevel,
    ) -> Result<Vec<Group>, RpcError> {
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_operation());

        let result = async {
            let group_count = usize::try_from(total_groups).map_err(|_| {
                RpcError::Deserialization(format!("group count too large: {total_groups}"))
            })?;

            if group_count == 0 {
                return Ok(Vec::new());
            }

            let addresses: Vec<Address> = (0..total_groups)
                .map(|idx| group_pda(epoch, GroupIndex(idx)).0)
                .collect();

            let accounts = self
                .rpc()
                .get_multiple_accounts_with_commitment(&addresses, commitment)
                .await?;

            if accounts.len() != group_count {
                return Err(RpcError::Deserialization(format!(
                    "group batch returned {} accounts, expected {}",
                    accounts.len(),
                    group_count
                )));
            }

            accounts
                .into_iter()
                .enumerate()
                .map(|(idx, account)| {
                    let group = GroupIndex(idx as u64);
                    let account = account.ok_or(RpcError::AccountNotFound(addresses[idx]))?;
                    unpack_group(&account.data, epoch, group)
                })
                .collect()
        }
        .await;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            if let Some(timer) = &timer {
                let status = if result.is_ok() { "success" } else { "error" };
                metrics.record_account_fetch("groups", status, timer);
            }
        }

        result
    }

    /// Fetch a Node account by authority
    ///
    /// # Arguments
    /// * `authority` - The authority public key of the node
    pub async fn get_node(&self, authority: &Address) -> Result<Node, RpcError> {
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
        authority: &Address,
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
    pub async fn get_tape(&self, authority: &Address) -> Result<Tape, RpcError> {
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

    /// Fetch a History account for a node
    ///
    /// # Arguments
    /// * `node` - The node public key
    pub async fn get_history(&self, node: &Address) -> Result<History, RpcError> {
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
    pub async fn get_all_nodes(&self) -> Result<Vec<(Address, Node)>, RpcError> {
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

        let program_id: Address = tapedrive::ID.into();
        let accounts = self.rpc().get_program_accounts(&program_id, config).await?;

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
    pub async fn get_all_tapes(&self) -> Result<Vec<(Address, Tape)>, RpcError> {
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

        let program_id: Address = tapedrive::ID.into();
        let accounts = self.rpc().get_program_accounts(&program_id, config).await?;

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
    pub async fn get_node_by_id(&self, node_id: NodeId) -> Result<(Address, Node), RpcError> {
        // Account layout: [discriminator (8 bytes)][id (8 bytes)]...
        // Filter on both discriminator and id field
        let id_bytes = node_id.as_u64().to_le_bytes();

        let config = RpcProgramAccountsConfig {
            filters: Some(vec![
                // Filter by discriminator at offset 0
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                    0,
                    vec![AccountType::Node as u8],
                )),
                // Filter by NodeId at offset 8 (after 8-byte discriminator)
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                    8,
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

        let program_id: Address = tapedrive::ID.into();
        let accounts = self.rpc().get_program_accounts(&program_id, config).await?;

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
    pub async fn get_tape_by_number(&self, tape_number: TapeNumber) -> Result<(Address, Tape), RpcError> {
        // Account layout: [discriminator (8 bytes)][id (8 bytes)]...
        let id_bytes = tape_number.as_u64().to_le_bytes();

        let config = RpcProgramAccountsConfig {
            filters: Some(vec![
                // Filter by discriminator at offset 0
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                    0,
                    vec![AccountType::Tape as u8],
                )),
                // Filter by TapeNumber at offset 8 (after 8-byte discriminator)
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                    8,
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

        let program_id: Address = tapedrive::ID.into();
        let accounts = self.rpc().get_program_accounts(&program_id, config).await?;

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

    /// Fetch a Tape account by its PDA address directly.
    ///
    /// This is useful when you already know the tape address (e.g., from
    /// `tape_pda(authority)`) and need to read the on-chain data.
    ///
    /// # Arguments
    /// * `address` - The tape PDA address
    pub async fn get_tape_by_address(&self, address: &Address) -> Result<Tape, RpcError> {
        let account = self.rpc().get_account(address).await?;
        Tape::unpack_with_discriminator(&account.data)
            .map(|t| *t)
            .map_err(|e| RpcError::Deserialization(e.to_string()))
    }

}

fn unpack_group(data: &[u8], epoch: EpochNumber, group: GroupIndex) -> Result<Group, RpcError> {
    if data.len() < Group::get_size() {
        return Err(RpcError::Deserialization(format!(
            "Group account too small: {} bytes (expected {})",
            data.len(),
            Group::get_size()
        )));
    }

    let decoded = Group::unpack_with_discriminator(data)
        .map(|group| *group)
        .map_err(|error| RpcError::Deserialization(error.to_string()))?;

    if decoded.epoch != epoch || decoded.id != group {
        return Err(RpcError::Deserialization(format!(
            "Group account mismatch: got epoch {} group {}, expected epoch {} group {}",
            decoded.epoch, decoded.id, epoch, group
        )));
    }

    Ok(decoded)
}

fn unpack_dynamic_entries<'a, T>(
    data: &'a [u8],
    label: &str,
) -> Result<(&'a T, Vec<T::Entry>), RpcError>
where
    T: DynamicState,
    T::Entry: Copy,
{
    let header_start: usize = 8;
    let header_len = size_of::<T>();
    let header_end = header_start
        .checked_add(header_len)
        .ok_or_else(|| RpcError::Deserialization(format!("{label} header size overflow")))?;

    if data.len() < header_end {
        return Err(RpcError::Deserialization(format!(
            "{label} account too small: {} bytes (expected at least {})",
            data.len(),
            header_end
        )));
    }

    if data[0] != T::discriminator() {
        return Err(RpcError::Deserialization(format!(
            "{label} discriminator mismatch: got {}, expected {}",
            data[0],
            T::discriminator()
        )));
    }

    let header = bytemuck::try_from_bytes::<T>(&data[header_start..header_end])
        .map_err(|error| RpcError::Deserialization(error.to_string()))?;

    let capacity = usize::try_from(header.tail().capacity).map_err(|_| {
        RpcError::Deserialization(format!(
            "{label} capacity too large: {}",
            header.tail().capacity
        ))
    })?;
    let count = usize::try_from(header.tail().count).map_err(|_| {
        RpcError::Deserialization(format!("{label} count too large: {}", header.tail().count))
    })?;

    if count > capacity {
        return Err(RpcError::Deserialization(format!(
            "{label} count {} exceeds capacity {}",
            count, capacity
        )));
    }

    let body_len = capacity
        .checked_mul(size_of::<T::Entry>())
        .ok_or_else(|| RpcError::Deserialization(format!("{label} body size overflow")))?;
    let body_end = header_end
        .checked_add(body_len)
        .ok_or_else(|| RpcError::Deserialization(format!("{label} body offset overflow")))?;

    if data.len() < body_end {
        return Err(RpcError::Deserialization(format!(
            "{label} account too small for tail: {} bytes (expected at least {})",
            data.len(),
            body_end
        )));
    }

    let entries = bytemuck::try_cast_slice::<u8, T::Entry>(&data[header_end..body_end])
        .map_err(|error| RpcError::Deserialization(error.to_string()))?;

    Ok((header, entries[..count].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use rpc_litesvm::LiteSvmRpc;
    use tape_api::program::tapedrive::{self, peer_set_pda};
    use tape_core::bls::BlsPrivateKey;
    use tape_core::types::{StorageUnits, Tail};
    use tape_core::types::coin::TAPE;

    fn client() -> RpcClient<LiteSvmRpc> {
        RpcClient::from_rpc(LiteSvmRpc::new())
    }

    fn address(byte: u8) -> Address {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Address::new(bytes)
    }

    #[tokio::test]
    async fn committee_returns_active_members_only() {
        let client = client();
        let epoch = EpochNumber(7);
        let (committee_address, _) = committee_pda(epoch);
        let members = vec![
            Member {
                node: address(1),
                stake: TAPE(100),
                blacklist: StorageUnits::zero(),
                spools: 3,
            },
            Member {
                node: address(2),
                stake: TAPE(90),
                blacklist: StorageUnits::zero(),
                spools: 2,
            },
        ];
        let mut body = members.clone();
        body.push(Member {
            node: address(3),
            stake: TAPE(1),
            blacklist: StorageUnits::zero(),
            spools: 0,
        });

        let committee = Committee {
            epoch,
            members: Tail::new(body.len() as u64, members.len() as u64),
        };

        client
            .rpc()
            .set_account_data(committee_address, tapedrive::ID, &committee.pack_with(&body))
            .expect("store committee");

        let decoded = client.get_committee(epoch).await.expect("read committee");
        assert_eq!(decoded, members);
    }

    #[tokio::test]
    async fn peer_set_returns_active_peers_only() {
        let client = client();
        let (peer_set_address, _) = peer_set_pda();
        let peers = vec![
            Peer {
                node: address(1),
                ..Peer::zeroed()
            },
            Peer {
                node: address(2),
                ..Peer::zeroed()
            },
        ];
        let mut body = peers.clone();
        body.push(Peer {
            node: address(3),
            ..Peer::zeroed()
        });

        let peer_set = PeerSet {
            peers: Tail::new(body.len() as u64, peers.len() as u64),
        };

        client
            .rpc()
            .set_account_data(peer_set_address, tapedrive::ID, &peer_set.pack_with(&body))
            .expect("store peer set");

        let decoded = client.get_peer_set().await.expect("read peer set");
        assert_eq!(decoded, peers);
    }

    #[tokio::test]
    async fn groups_batch_reads_deterministic_pdas() {
        let client = client();
        let epoch = EpochNumber(9);
        let mut expected = Vec::new();

        for idx in 0..2 {
            let group_id = GroupIndex(idx);
            let (group_address, _) = group_pda(epoch, group_id);
            let mut group = Group {
                epoch,
                id: group_id,
                size: StorageUnits::gb(2 + idx),
                ..Group::zeroed()
            };

            let sk = BlsPrivateKey::from_random();
            group.spools[0].node = address((idx + 1) as u8);
            group.spools[0].bls_pubkey = sk.public_key().expect("bls pubkey");

            client
                .rpc()
                .set_account_data(group_address, tapedrive::ID, &group.pack())
                .expect("store group");
            expected.push(group);
        }

        let decoded = client
            .get_groups(epoch, expected.len() as u64)
            .await
            .expect("read groups");
        assert_eq!(decoded, expected);
    }
}
