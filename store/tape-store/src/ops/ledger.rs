//! Accounting-ledger operations: the atomic reserve to commit/refund cost control
//!
//! Every cost-bearing S3 write reserves a per-op **estimate** against the owner
//! principal's budget before the on-chain write, then reconciles to the
//! actual afterward.

use store::{Column, Direction, Store, WriteBatch};
use tape_crypto::address::Address;

use crate::columns::{LedgerCol, LedgerReservationCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{BudgetLimits, LedgerEntry, LedgerReservation, LedgerReservationKey};
use crate::TapeStore;

/// Seconds in the rolling write-count window (matches `puts_per_hour`)
const HOUR_SECS: i64 = 3_600;
/// Seconds in the rolling bytes/SOL window (matches `*_per_day`)
const DAY_SECS: i64 = 86_400;

/// Upper bound on the `SlowDown` retry-after we report.
const RETRY_AFTER_CAP_SECS: u64 = 3_600;

/// The per-op cost estimate a write reserves up front, before the on-chain write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReserveRequest {
    /// Estimated write-count this op consumes (1 for object writes, 0 otherwise)
    pub writes: u32,
    /// Estimated object bytes this op writes
    pub bytes: u64,
    /// Estimated SOL fee (lamports) this op spends
    pub sol: u64,
    /// Whether the op performs an on-chain operation (bumps `onchain_ops_total`)
    pub is_onchain: bool,
    /// Whether the op consumes tape capacity (bumps `capacity_consumed_total`)
    pub meters_capacity: bool,
}

/// The outcome of a reserve admission check
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReserveOutcome {
    /// Admitted: the returned key identifies the durable reservation to commit or
    /// refund
    Granted(LedgerReservationKey),
    /// Over the soft budget: throttle and retry after the window rolls
    SlowDown {
        /// Seconds until the over-budget window rolls over
        retry_after_secs: u64,
    },
    /// Over the hard ceiling: reject outright with the reason code
    Ceiling(String),
}

/// Roll any elapsed window: reset its committed counters and restart it at `now`.
/// Outstanding reservations (`*_reserved`) are window-independent and untouched.
fn roll_windows(entry: &mut LedgerEntry, now: i64) {
    if now.saturating_sub(entry.writes_window_start) >= HOUR_SECS {
        entry.writes_window_start = now;
        entry.writes_committed = 0;
    }
    if now.saturating_sub(entry.daily_window_start) >= DAY_SECS {
        entry.daily_window_start = now;
        entry.bytes_committed = 0;
        entry.sol_committed = 0;
    }
}

/// The budget in force for a principal: its per-row override, else the
/// caller-supplied default (the operator default, then the YAML default)
fn effective_budget(entry: &LedgerEntry, default: BudgetLimits) -> BudgetLimits {
    entry.budget_override.unwrap_or(default)
}

/// Evaluate one budget dimension.
fn check_dimension(
    request: u64,
    committed: u64,
    reserved: u64,
    limit: u64,
    window_end: i64,
    now: i64,
) -> (bool, Option<u64>) {
    if request == 0 {
        return (false, None);
    }
    // The op's own estimate exceeds the entire window budget.
    if request > limit {
        return (true, None);
    }
    // The op fits a fresh window.
    let projected = committed
        .saturating_add(reserved)
        .saturating_add(request);
    if projected > limit {
        let remaining = window_end.saturating_sub(now).max(1) as u64;
        (false, Some(remaining.min(RETRY_AFTER_CAP_SECS)))
    } else {
        (false, None)
    }
}

/// Operations for the durable per-principal accounting ledger
///
/// `reserve`/`commit`/`refund` are a read-modify-write of one principal's row;
/// the caller must serialize them per ledger (single instance: one lock) so the
/// admission check and the reservation cannot race. Each persists atomically.
pub trait LedgerOps {
    /// Read a principal's ledger row, defaulting to an empty ledger row when
    /// the principal has never written
    fn get_ledger(&self, principal: &Address) -> Result<LedgerEntry>;

    /// Set (or clear, with `None`) a principal's per-row budget override
    fn set_principal_budget(
        &self,
        principal: &Address,
        budget: Option<BudgetLimits>,
    ) -> Result<()>;

    /// Admission-check and, if admitted, durably reserve `request`'s estimate.
    fn reserve(
        &self,
        principal: &Address,
        request: ReserveRequest,
        default_budget: BudgetLimits,
        now: i64,
    ) -> Result<ReserveOutcome>;

    /// Reconcile a reservation to its actual cost and commit it.
    fn commit(&self, key: &LedgerReservationKey, actual_bytes: u64, now: i64) -> Result<bool>;

    /// Release a reservation without committing any cost.
    fn refund(&self, key: &LedgerReservationKey) -> Result<()>;

    /// Reclaim every reservation older than `ttl_secs` by refunding it. Returns
    /// the number reclaimed. The caller holds the ledger lock for the duration.
    fn sweep_reservations(&self, now: i64, ttl_secs: i64) -> Result<usize>;
}

/// Serialize a value to wincode bytes, mapping errors to the store error type
fn serialize_value<Value>(label: &str, value: &Value) -> Result<Vec<u8>>
where
    Value: wincode::SchemaWrite<Src = Value>,
{
    wincode::serialize(value).map_err(|error| TapeStoreError::Serialization(format!("{label}: {error}")))
}

impl<Backend: Store> LedgerOps for TapeStore<Backend> {
    fn get_ledger(&self, principal: &Address) -> Result<LedgerEntry> {
        Ok(self.get::<LedgerCol>(principal)?.unwrap_or_default())
    }

    fn set_principal_budget(
        &self,
        principal: &Address,
        budget: Option<BudgetLimits>,
    ) -> Result<()> {
        let mut entry = self.get_ledger(principal)?;
        entry.budget_override = budget;
        self.put::<LedgerCol>(principal, &entry)?;
        Ok(())
    }

    fn reserve(
        &self,
        principal: &Address,
        request: ReserveRequest,
        default_budget: BudgetLimits,
        now: i64,
    ) -> Result<ReserveOutcome> {
        let mut entry = self.get_ledger(principal)?;
        roll_windows(&mut entry, now);
        let budget = effective_budget(&entry, default_budget);

        let mut is_over_hard_ceiling = false;
        let mut soft_retry: Option<u64> = None;

        let (is_hard, soft_retry_secs) = check_dimension(
            request.writes as u64,
            entry.writes_committed as u64,
            entry.writes_reserved as u64,
            budget.puts_per_hour as u64,
            entry.writes_window_start + HOUR_SECS,
            now,
        );
        is_over_hard_ceiling |= is_hard;
        soft_retry = max_option(soft_retry, soft_retry_secs);

        let (is_hard, soft_retry_secs) = check_dimension(
            request.bytes,
            entry.bytes_committed,
            entry.bytes_reserved,
            budget.bytes_per_day,
            entry.daily_window_start + DAY_SECS,
            now,
        );
        is_over_hard_ceiling |= is_hard;
        soft_retry = max_option(soft_retry, soft_retry_secs);

        let (is_hard, soft_retry_secs) = check_dimension(
            request.sol,
            entry.sol_committed,
            entry.sol_reserved,
            budget.sol_per_day,
            entry.daily_window_start + DAY_SECS,
            now,
        );
        is_over_hard_ceiling |= is_hard;
        soft_retry = max_option(soft_retry, soft_retry_secs);

        if is_over_hard_ceiling {
            return Ok(ReserveOutcome::Ceiling(
                "write exceeds the principal's per-window budget and can never be admitted"
                    .to_string(),
            ));
        }
        if let Some(retry_after_secs) = soft_retry {
            return Ok(ReserveOutcome::SlowDown { retry_after_secs });
        }

        // Admitted — record the reservation and the rolled/updated row atomically.
        let sequence = entry.next_reservation_sequence;
        entry.next_reservation_sequence = sequence.saturating_add(1);
        entry.writes_reserved = entry.writes_reserved.saturating_add(request.writes);
        entry.bytes_reserved = entry.bytes_reserved.saturating_add(request.bytes);
        entry.sol_reserved = entry.sol_reserved.saturating_add(request.sol);

        let key = LedgerReservationKey::new(now, *principal, sequence);
        let reservation = LedgerReservation {
            writes: request.writes,
            bytes: request.bytes,
            sol: request.sol,
            is_onchain: request.is_onchain,
            meters_capacity: request.meters_capacity,
        };

        let mut batch = WriteBatch::new();
        batch.put(
            LedgerCol::CF_NAME,
            &serialize_value("ledger key", principal)?,
            &serialize_value("ledger entry", &entry)?,
        );
        batch.put(
            LedgerReservationCol::CF_NAME,
            &serialize_value("reservation key", &key)?,
            &serialize_value("reservation", &reservation)?,
        );
        self.inner().inner().write_batch(batch)?;

        Ok(ReserveOutcome::Granted(key))
    }

    fn commit(&self, key: &LedgerReservationKey, actual_bytes: u64, now: i64) -> Result<bool> {
        let Some(reservation) = self.get::<LedgerReservationCol>(key)? else {
            // Already reclaimed (by a prior commit/refund or the TTL sweep).
            return Ok(false);
        };

        let mut entry = self.get_ledger(&key.principal)?;
        roll_windows(&mut entry, now);

        // Release the reserved estimate.
        entry.writes_reserved = entry.writes_reserved.saturating_sub(reservation.writes);
        entry.bytes_reserved = entry.bytes_reserved.saturating_sub(reservation.bytes);
        entry.sol_reserved = entry.sol_reserved.saturating_sub(reservation.sol);

        // Commit the actuals into the (possibly just-rolled) windows.
        entry.writes_committed = entry.writes_committed.saturating_add(reservation.writes);
        entry.bytes_committed = entry.bytes_committed.saturating_add(actual_bytes);
        entry.sol_committed = entry.sol_committed.saturating_add(reservation.sol);

        // Lifetime meters.
        entry.writes_total = entry.writes_total.saturating_add(reservation.writes as u64);
        entry.bytes_total = entry.bytes_total.saturating_add(actual_bytes);
        if reservation.is_onchain {
            entry.onchain_ops_total = entry.onchain_ops_total.saturating_add(1);
        }
        entry.sol_spent_total = entry.sol_spent_total.saturating_add(reservation.sol);
        if reservation.meters_capacity {
            entry.capacity_consumed_total =
                entry.capacity_consumed_total.saturating_add(actual_bytes);
        }

        let mut batch = WriteBatch::new();
        batch.put(
            LedgerCol::CF_NAME,
            &serialize_value("ledger key", &key.principal)?,
            &serialize_value("ledger entry", &entry)?,
        );
        batch.delete(LedgerReservationCol::CF_NAME, &serialize_value("reservation key", key)?);
        self.inner().inner().write_batch(batch)?;
        Ok(true)
    }

    fn refund(&self, key: &LedgerReservationKey) -> Result<()> {
        let Some(reservation) = self.get::<LedgerReservationCol>(key)? else {
            return Ok(());
        };

        let mut entry = self.get_ledger(&key.principal)?;
        // Refund touches only the window-independent reserved counters, so no roll
        // is needed.
        entry.writes_reserved = entry.writes_reserved.saturating_sub(reservation.writes);
        entry.bytes_reserved = entry.bytes_reserved.saturating_sub(reservation.bytes);
        entry.sol_reserved = entry.sol_reserved.saturating_sub(reservation.sol);

        let mut batch = WriteBatch::new();
        batch.put(
            LedgerCol::CF_NAME,
            &serialize_value("ledger key", &key.principal)?,
            &serialize_value("ledger entry", &entry)?,
        );
        batch.delete(LedgerReservationCol::CF_NAME, &serialize_value("reservation key", key)?);
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn sweep_reservations(&self, now: i64, ttl_secs: i64) -> Result<usize> {
        let cutoff = now.saturating_sub(ttl_secs);
        let raw = self.inner().inner();

        // Reservations are ordered by created_at, so scan from the oldest and stop
        // once past the cutoff.
        let mut expired: Vec<LedgerReservationKey> = Vec::new();
        for (key_bytes, _value) in raw.iter_from(LedgerReservationCol::CF_NAME, &[], Direction::Asc)?
        {
            let key: LedgerReservationKey = wincode::deserialize(&key_bytes)
                .map_err(|error| TapeStoreError::Serialization(format!("reservation key: {error}")))?;
            if key.created_at > cutoff {
                break;
            }
            expired.push(key);
        }

        let mut reclaimed = 0;
        for key in expired {
            self.refund(&key)?;
            reclaimed += 1;
        }
        Ok(reclaimed)
    }
}

/// `max` of two `Option<u64>`, treating `None` as absent
fn max_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left_value), Some(right_value)) => Some(left_value.max(right_value)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;

    use super::*;

    fn store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn budget() -> BudgetLimits {
        BudgetLimits {
            sol_per_day: 100_000,
            bytes_per_day: 10_000,
            puts_per_hour: 5,
            max_concurrent_multipart: 4,
        }
    }

    fn put_request(bytes: u64) -> ReserveRequest {
        ReserveRequest {
            writes: 1,
            bytes,
            sol: 5_000,
            is_onchain: true,
            meters_capacity: true,
        }
    }

    fn put_count_request() -> ReserveRequest {
        ReserveRequest { writes: 1, bytes: 0, sol: 0, is_onchain: true, meters_capacity: false }
    }

    // commit reports false (and bills nothing) when the TTL sweep reclaimed first
    #[test]
    fn commit_after_sweep() {
        let store = store();
        let principal = Address::new_unique();
        let now = 1_000_000;

        let ReserveOutcome::Granted(key) =
            store.reserve(&principal, put_request(1024), budget(), now).expect("reserve")
        else {
            unreachable!("expected grant");
        };

        // The TTL sweep reclaims the reservation before the slow write commits.
        let swept = store.sweep_reservations(now + 10, 0).expect("sweep");
        assert_eq!(swept, 1);

        // Commit finds nothing left to bill and reports it; the write goes unbilled.
        let is_committed = store.commit(&key, 1000, now + 10).expect("commit");
        assert!(!is_committed);
        let after = store.get_ledger(&principal).expect("get ledger");
        assert_eq!(after.bytes_committed, 0);
        assert_eq!(after.bytes_reserved, 0);
    }

    // reserve then commit releases the estimate and meters the actuals
    #[test]
    fn reserve_commit() {
        let store = store();
        let principal = Address::new_unique();
        let now = 1_000_000;

        let ReserveOutcome::Granted(key) =
            store.reserve(&principal, put_request(1024), budget(), now).expect("reserve")
        else {
            unreachable!("expected grant");
        };

        // While reserved, the estimate is outstanding (not yet committed).
        let mid = store.get_ledger(&principal).expect("get ledger");
        assert_eq!(mid.bytes_reserved, 1024);
        assert_eq!(mid.writes_reserved, 1);
        assert_eq!(mid.bytes_committed, 0);

        assert!(store.commit(&key, 1000, now).expect("commit"), "reservation was billed");
        let after = store.get_ledger(&principal).expect("get ledger");
        assert_eq!(after.bytes_reserved, 0);
        assert_eq!(after.writes_reserved, 0);
        assert_eq!(after.sol_reserved, 0);
        assert_eq!(after.bytes_committed, 1000, "commits the actual, not the estimate");
        assert_eq!(after.writes_committed, 1);
        assert_eq!(after.sol_committed, 5_000);
        assert_eq!(after.bytes_total, 1000);
        assert_eq!(after.writes_total, 1);
        assert_eq!(after.onchain_ops_total, 1);
        assert_eq!(after.sol_spent_total, 5_000);
        assert_eq!(after.capacity_consumed_total, 1000);
        // The reservation record is gone.
        assert!(store
            .get::<LedgerReservationCol>(&key)
            .expect("get reservation")
            .is_none());
    }

    // a refunded reservation releases without billing
    #[test]
    fn refund() {
        let store = store();
        let principal = Address::new_unique();
        let now = 1_000_000;
        let ReserveOutcome::Granted(key) =
            store.reserve(&principal, put_request(2048), budget(), now).expect("reserve")
        else {
            unreachable!("expected grant");
        };
        store.refund(&key).expect("refund");
        let after = store.get_ledger(&principal).expect("get ledger");
        assert_eq!(after.bytes_reserved, 0);
        assert_eq!(after.bytes_committed, 0, "a refunded write never bills");
        assert_eq!(after.bytes_total, 0);
        assert!(store
            .get::<LedgerReservationCol>(&key)
            .expect("get reservation")
            .is_none());
    }

    // a second commit or a later refund is a no-op
    #[test]
    fn commit_idempotent() {
        let store = store();
        let principal = Address::new_unique();
        let now = 1_000_000;
        let ReserveOutcome::Granted(key) =
            store.reserve(&principal, put_request(1024), budget(), now).expect("reserve")
        else {
            unreachable!("expected grant");
        };
        store.commit(&key, 1024, now).expect("commit");
        let snapshot = store.get_ledger(&principal).expect("get ledger");
        // A second commit (or a refund) for the same reservation must not move the
        // counters again.
        store.commit(&key, 1024, now).expect("commit");
        store.refund(&key).expect("refund");
        assert_eq!(store.get_ledger(&principal).expect("get ledger"), snapshot);
    }

    // outstanding reservations cannot race past the budget
    #[test]
    fn reservation_ceiling() {
        // puts_per_hour = 5. Reserve (without committing) up to the budget; the next
        // reserve is throttled because outstanding reservations already fill it.
        let store = store();
        let principal = Address::new_unique();
        let now = 1_000_000;
        let mut granted = 0;
        for _ in 0..6 {
            match store.reserve(&principal, put_count_request(), budget(), now).expect("reserve") {
                ReserveOutcome::Granted(_) => granted += 1,
                ReserveOutcome::SlowDown { .. } => break,
                ReserveOutcome::Ceiling(reason) => unreachable!("unexpected ceiling: {reason}"),
            }
        }
        assert_eq!(granted, 5, "outstanding reservations cannot exceed the budget");
        assert!(matches!(
            store.reserve(&principal, put_count_request(), budget(), now).expect("reserve"),
            ReserveOutcome::SlowDown { .. }
        ));
    }

    // an oversized single op is rejected outright
    #[test]
    fn hard_ceiling() {
        // A single object larger than the entire daily byte budget can never fit in
        // any window, so it is rejected outright (not throttled).
        let store = store();
        let principal = Address::new_unique();
        let now = 1_000_000;
        let huge = ReserveRequest {
            writes: 1,
            bytes: budget().bytes_per_day + 1,
            sol: 5_000,
            is_onchain: true,
            meters_capacity: true,
        };
        assert!(matches!(
            store.reserve(&principal, huge, budget(), now).expect("reserve"),
            ReserveOutcome::Ceiling(_)
        ));
    }

    // a full window throttles with a bounded retry-after
    #[test]
    fn soft_throttle() {
        // 6th write (budget = 5) fits a fresh window but the current window is full
        // -> SlowDown with a bounded retry-after.
        let store = store();
        let principal = Address::new_unique();
        let now = 1_000_000;
        for _ in 0..5 {
            let ReserveOutcome::Granted(key) =
                store.reserve(&principal, put_count_request(), budget(), now).expect("reserve")
            else {
                unreachable!("expected grant");
            };
            store.commit(&key, 0, now).expect("commit");
        }
        match store.reserve(&principal, put_count_request(), budget(), now).expect("reserve") {
            ReserveOutcome::SlowDown { retry_after_secs } => {
                assert!(retry_after_secs >= 1 && retry_after_secs <= HOUR_SECS as u64);
            }
            ReserveOutcome::Granted(_) => unreachable!("expected slowdown, got grant"),
            ReserveOutcome::Ceiling(reason) => unreachable!("expected slowdown, got ceiling: {reason}"),
        }
    }

    // a rolled window restores the budget
    #[test]
    fn window_rollover() {
        let store = store();
        let principal = Address::new_unique();
        let now = 1_000_000;
        for _ in 0..5 {
            if let ReserveOutcome::Granted(key) =
                store.reserve(&principal, put_count_request(), budget(), now).expect("reserve")
            {
                store.commit(&key, 0, now).expect("commit");
            }
        }
        // Saturated now; after the hourly window rolls, writes are admitted again.
        assert!(matches!(
            store.reserve(&principal, put_count_request(), budget(), now).expect("reserve"),
            ReserveOutcome::SlowDown { .. }
        ));
        let later = now + HOUR_SECS + 1;
        assert!(matches!(
            store.reserve(&principal, put_count_request(), budget(), later).expect("reserve"),
            ReserveOutcome::Granted(_)
        ));
    }

    // a delete is not blocked by the put budget
    #[test]
    fn delete_unblocked() {
        // Saturate puts; a delete (writes=0) still reserves because it consumes no
        // write-count budget.
        let store = store();
        let principal = Address::new_unique();
        let now = 1_000_000;
        for _ in 0..5 {
            if let ReserveOutcome::Granted(key) =
                store.reserve(&principal, put_count_request(), budget(), now).expect("reserve")
            {
                store.commit(&key, 0, now).expect("commit");
            }
        }
        let delete = ReserveRequest { writes: 0, bytes: 0, sol: 5_000, is_onchain: true, meters_capacity: false };
        assert!(matches!(
            store.reserve(&principal, delete, budget(), now).expect("reserve"),
            ReserveOutcome::Granted(_)
        ));
    }

    // a per-principal override beats the default
    #[test]
    fn principal_override() {
        let store = store();
        let principal = Address::new_unique();
        let now = 1_000_000;
        // Override down to a single put/hour.
        store.set_principal_budget(
            &principal,
            Some(BudgetLimits { sol_per_day: 100_000, bytes_per_day: 10_000, puts_per_hour: 1, max_concurrent_multipart: 1 }),
        )
        .expect("set budget");
        // budget=1 -> first put granted, second throttled by the override even
        // though the default (5) would admit it.
        assert!(matches!(
            store.reserve(&principal, put_count_request(), budget(), now).expect("reserve"),
            ReserveOutcome::Granted(_)
        ));
        assert!(matches!(
            store.reserve(&principal, put_count_request(), budget(), now).expect("reserve"),
            ReserveOutcome::SlowDown { .. }
        ));
    }

    // the sweep reclaims only stale reservations
    #[test]
    fn sweep_stale() {
        let store = store();
        let principal = Address::new_unique();
        let old = 1_000_000;
        let fresh = old + 100;

        let ReserveOutcome::Granted(stale) =
            store.reserve(&principal, put_request(1024), budget(), old).expect("reserve")
        else {
            unreachable!("expected grant");
        };
        let ReserveOutcome::Granted(recent) =
            store.reserve(&principal, put_request(512), budget(), fresh).expect("reserve")
        else {
            unreachable!("expected grant");
        };

        // TTL of 50s at t=old+60: the t=old reservation is stale, the t=old+100 is not.
        let reclaimed = store.sweep_reservations(old + 60, 50).expect("sweep");
        assert_eq!(reclaimed, 1);
        assert!(store
            .get::<LedgerReservationCol>(&stale)
            .expect("get reservation")
            .is_none());
        assert!(store
            .get::<LedgerReservationCol>(&recent)
            .expect("get reservation")
            .is_some());

        let after = store.get_ledger(&principal).expect("get ledger");
        // Only the recent reservation's bytes remain outstanding.
        assert_eq!(after.bytes_reserved, 512);
    }
}
