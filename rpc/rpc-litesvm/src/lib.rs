#![allow(clippy::result_large_err)]

mod block;
mod convert;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use block::{RecordedTransaction, SlotData};
use convert::{tx_result_to_status_result, tx_result_to_transaction_status};
use litesvm::types::TransactionResult;
use litesvm::LiteSVM;
use rpc::{Rpc, RpcError};
use solana_client::rpc_config::RpcProgramAccountsConfig;
use solana_client::rpc_filter::RpcFilterType;
use solana_sdk::account::{Account, ReadableAccount};
use solana_sdk::clock::{Clock, Slot};
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use solana_transaction_status::UiConfirmedBlock;
use tokio::task::JoinHandle;

struct Inner {
    svm: LiteSVM,
    slots: HashMap<Slot, SlotData>,
    tx_slot_index: HashMap<Signature, Slot>,
    current_block_height: u64,
    last_recorded_slot: Option<Slot>,
    /// Highest slot visible via `get_slot()` / `get_block()`.
    confirmed_tip: u64,
    /// Next SVM slot where a submitted transaction will be recorded.
    pending_slot: u64,
}

/// LiteSVM-backed Rpc implementation with simulated block production.
///
/// Transactions execute immediately (state changes visible via
/// `get_account`), but resulting blocks only become queryable through
/// `get_slot` / `get_block` once `confirmed_tip` advances past the
/// slot they were recorded in.
///
/// Call [`LiteSvmRpc::start_block_producer`] to spawn a background
/// task that advances `confirmed_tip` at a steady cadence.
#[derive(Clone)]
pub struct LiteSvmRpc {
    inner: Arc<Mutex<Inner>>,
}

impl LiteSvmRpc {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                svm: LiteSVM::new().with_transaction_history(10_000),
                slots: HashMap::new(),
                tx_slot_index: HashMap::new(),
                current_block_height: 0,
                last_recorded_slot: None,
                confirmed_tip: 0,
                pending_slot: 1,
            })),
        }
    }

    pub fn airdrop(&self, pubkey: &Pubkey, lamports: u64) -> Result<(), RpcError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        inner
            .svm
            .airdrop(pubkey, lamports)
            .map(|_| ())
            .map_err(|e| RpcError::Request(format!("{e:?}")))
    }

    /// Advance the SVM slot **and** `confirmed_tip` to `slot`.
    ///
    /// This is the explicit "harness drives time" path — blocks recorded
    /// at slots <= `slot` become immediately visible via `get_block`.
    pub fn warp_to_slot(&self, slot: u64) -> Result<(), RpcError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        inner.svm.warp_to_slot(slot);
        if slot > inner.confirmed_tip {
            inner.confirmed_tip = slot;
        }
        if inner.pending_slot <= slot {
            inner.pending_slot = slot + 1;
        }
        Ok(())
    }

    /// Spawn a background task that advances `confirmed_tip` by one slot
    /// every `interval`.  Returns a handle the caller can `abort()` to
    /// stop the producer.
    pub fn start_block_producer(&self, interval: Duration) -> JoinHandle<()> {
        let rpc = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let mut inner = rpc.inner.lock().expect("mutex poisoned");
                inner.confirmed_tip += 1;
                if inner.pending_slot <= inner.confirmed_tip {
                    inner.pending_slot = inner.confirmed_tip + 1;
                }
            }
        })
    }

    pub fn advance_time(&self, seconds: i64) -> Result<(), RpcError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        let mut clock = inner.svm.get_sysvar::<Clock>();
        clock.unix_timestamp = clock.unix_timestamp.saturating_add(seconds);
        inner.svm.set_sysvar(&clock);
        Ok(())
    }

    pub fn unix_timestamp(&self) -> Result<i64, RpcError> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        Ok(inner.svm.get_sysvar::<Clock>().unix_timestamp)
    }

    pub fn add_program_from_file(
        &self,
        program_id: impl Into<Pubkey>,
        path: impl AsRef<std::path::Path>,
    ) -> Result<(), RpcError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        inner
            .svm
            .add_program_from_file(program_id, path)
            .map_err(|e| RpcError::Internal(format!("add_program_from_file failed: {e:?}")))
    }

    pub fn add_program(
        &self,
        program_id: impl Into<Pubkey>,
        program_bytes: &[u8],
    ) -> Result<(), RpcError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        inner
            .svm
            .add_program(program_id, program_bytes)
            .map_err(|e| RpcError::Internal(format!("add_program failed: {e:?}")))
    }

    /// Stores/overwrites a full account in the in-memory VM.
    pub fn set_account(
        &self,
        pubkey: impl Into<Pubkey>,
        account: Account,
    ) -> Result<(), RpcError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        inner
            .svm
            .set_account(pubkey.into(), account)
            .map_err(|e| RpcError::Request(format!("set_account failed: {e:?}")))
    }

    /// Store an account with the minimum rent-exempt lamport balance.
    pub fn set_account_data(
        &self,
        pubkey: impl Into<Pubkey>,
        owner: impl Into<Pubkey>,
        data: &[u8],
    ) -> Result<(), RpcError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;

        let lamports = inner.svm.minimum_balance_for_rent_exemption(data.len());
        let mut account = Account::new(lamports, data.len(), &owner.into());
        account.data = data.to_vec();

        inner
            .svm
            .set_account(pubkey.into(), account)
            .map_err(|e| RpcError::Request(format!("set_account_data failed: {e:?}")))
    }

    fn current_slot_locked(inner: &Inner) -> Slot {
        inner.svm.get_sysvar::<Clock>().slot
    }

    fn balances_for_transaction(inner: &Inner, tx: &solana_sdk::transaction::VersionedTransaction) -> Vec<u64> {
        tx.message
            .static_account_keys()
            .iter()
            .map(|pk| inner.svm.get_balance(pk).unwrap_or(0))
            .collect()
    }

    fn record_transaction_locked(
        inner: &mut Inner,
        tx: solana_sdk::transaction::VersionedTransaction,
        result: &TransactionResult,
        pre_balances: Vec<u64>,
        post_balances: Vec<u64>,
    ) {
        let slot = Self::current_slot_locked(inner);
        let sig = match result {
            Ok(meta) => meta.signature,
            Err(failed) => failed.meta.signature,
        };

        let previous_blockhash = inner
            .last_recorded_slot
            .and_then(|prev_slot| inner.slots.get(&prev_slot).map(|s| s.blockhash.clone()))
            .unwrap_or_else(|| Hash::default().to_string());
        let parent_slot = inner.last_recorded_slot.unwrap_or(0);

        let slot_data = inner.slots.entry(slot).or_insert_with(|| {
            inner.current_block_height += 1;

            SlotData {
                blockhash: inner.svm.latest_blockhash().to_string(),
                previous_blockhash,
                parent_slot,
                transactions: Vec::new(),
                block_height: inner.current_block_height,
            }
        });

        slot_data.transactions.push(RecordedTransaction {
            tx,
            result: result.clone(),
            pre_balances,
            post_balances,
        });

        inner.tx_slot_index.insert(sig, slot);
        inner.last_recorded_slot = Some(slot);
    }
}

#[async_trait]
impl Rpc for LiteSvmRpc {
    fn commitment(&self) -> CommitmentLevel {
        CommitmentLevel::Confirmed
    }

    async fn get_slot(&self) -> Result<u64, RpcError> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        Ok(inner.confirmed_tip)
    }

    async fn get_latest_blockhash(&self) -> Result<Hash, RpcError> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        Ok(inner.svm.latest_blockhash())
    }

    async fn get_block(&self, slot: u64) -> Result<UiConfirmedBlock, RpcError> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;

        if slot > inner.confirmed_tip {
            return Err(RpcError::Request(format!(
                "SlotSkipped: slot {slot} not yet confirmed (tip: {})",
                inner.confirmed_tip
            )));
        }

        let data = inner.slots.get(&slot).ok_or_else(|| {
            RpcError::Request(format!("SlotSkipped: slot {slot} was skipped or not produced"))
        })?;

        data.to_ui_confirmed_block()
            .map_err(|e| RpcError::Internal(format!("failed to encode block: {e}")))
    }

    async fn get_block_height(&self) -> Result<u64, RpcError> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        Ok(inner.current_block_height)
    }

    async fn get_account(&self, pubkey: &Pubkey) -> Result<Account, RpcError> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        inner
            .svm
            .get_account(pubkey)
            .ok_or(RpcError::AccountNotFound(*pubkey))
    }

    async fn get_multiple_accounts(
        &self,
        pubkeys: &[Pubkey],
    ) -> Result<Vec<Option<Account>>, RpcError> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;
        Ok(pubkeys.iter().map(|pk| inner.svm.get_account(pk)).collect())
    }

    async fn get_program_accounts(
        &self,
        program_id: &Pubkey,
        config: RpcProgramAccountsConfig,
    ) -> Result<Vec<(Pubkey, Account)>, RpcError> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;

        let filters = config.filters.unwrap_or_default();

        let out = inner
            .svm
            .accounts_db()
            .inner
            .iter()
            .filter_map(|(k, acc)| {
                if acc.owner() != program_id {
                    return None;
                }

                let passes = filters.iter().all(|f| match f {
                    RpcFilterType::DataSize(size) => acc.data().len() as u64 == *size,
                    RpcFilterType::Memcmp(memcmp) => memcmp.bytes_match(acc.data()),
                    RpcFilterType::TokenAccountState => true,
                });

                if passes {
                    Some((*k, Account::from(acc.clone())))
                } else {
                    None
                }
            })
            .collect();

        Ok(out)
    }

    async fn send_transaction(&self, transaction: &Transaction) -> Result<Signature, RpcError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;

        // Warp SVM to the pending slot so the on-chain program sees the
        // correct Clock::slot value and the block is recorded there.
        let tx_slot = inner.pending_slot;
        inner.svm.warp_to_slot(tx_slot);

        let vtx: solana_sdk::transaction::VersionedTransaction = transaction.clone().into();
        let pre_balances = Self::balances_for_transaction(&inner, &vtx);

        let result = inner.svm.send_transaction(vtx.clone());

        let post_balances = Self::balances_for_transaction(&inner, &vtx);
        Self::record_transaction_locked(&mut inner, vtx, &result, pre_balances, post_balances);

        // Advance to the next pending slot for the next transaction.
        inner.pending_slot += 1;

        // Fresh blockhash so repeated identical messages don't hit
        // "already processed" in tight test loops.
        inner.svm.expire_blockhash();

        match result {
            Ok(meta) => Ok(meta.signature),
            Err(failed) => Err(RpcError::Transaction(failed.err.to_string())),
        }
    }

    async fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Signature, RpcError> {
        // LiteSVM executes immediately, so send == send_and_confirm.
        self.send_transaction(transaction).await
    }

    async fn get_signature_status(
        &self,
        signature: &Signature,
    ) -> Result<Option<Result<(), solana_sdk::transaction::TransactionError>>, RpcError> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| RpcError::Internal(format!("mutex poisoned: {e}")))?;

        let Some(slot) = inner.tx_slot_index.get(signature) else {
            return Ok(None);
        };

        let status = match inner.svm.get_transaction(signature) {
            Some(result) => {
                let _ = tx_result_to_transaction_status(&result, *slot);
                tx_result_to_status_result(&result)
            }
            None => return Ok(None),
        };

        Ok(Some(status))
    }
}

impl Default for LiteSvmRpc {
    fn default() -> Self {
        Self::new()
    }
}
