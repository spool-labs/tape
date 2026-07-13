//! Append-only write-authorization audit log operations.

use store::{Column, Direction, Store};

use crate::columns::AuditLogCol;
use crate::error::{Result, TapeStoreError};
use crate::types::{AuditEntry, AuditKey};
use crate::TapeStore;

/// Upper bound on scan-result preallocation,
const MAX_SCAN_PREALLOC: usize = 1024;

/// Serialize an AuditKey to raw bytes.
fn serialize_key(key: &AuditKey) -> Result<Vec<u8>> {
    wincode::serialize(key)
        .map_err(|error| TapeStoreError::Serialization(format!("audit key: {}", error)))
}

/// Operations for the append-only authorization audit log
pub trait AuditOps {
    /// Append one decision to the audit log at `sequence`
    ///
    /// `sequence` is a process-monotonic counter the caller owns; it makes every
    /// key unique, so concurrent appends in the same instant never clobber and
    /// the append is a single put with no scan.
    fn append_audit(&self, entry: &AuditEntry, sequence: u64) -> Result<()>;

    /// Scan the audit log in chronological order
    fn scan_audit(&self, start_timestamp: Option<i64>, limit: usize) -> Result<Vec<AuditEntry>>;

    /// The largest sequence recorded so far (0 when the log is empty)
    ///
    /// Read once at startup to seed the in-process sequence counter so a restart
    /// resumes above every existing entry rather than reusing low sequences.
    fn max_audit_sequence(&self) -> Result<u64>;
}

impl<Backend: Store> AuditOps for TapeStore<Backend> {
    fn append_audit(&self, entry: &AuditEntry, sequence: u64) -> Result<()> {
        // Audit timestamps are positive unix time; clamp defensively so a stray
        // negative value still encodes to an ordered (non-wrapping) key.
        let timestamp = entry.timestamp.max(0) as u64;

        let key = serialize_key(&AuditKey::new(timestamp, sequence))?;
        let value = wincode::serialize(entry)
            .map_err(|error| TapeStoreError::Serialization(format!("audit entry: {}", error)))?;

        self.inner().inner().put(AuditLogCol::CF_NAME, &key, &value)?;
        Ok(())
    }

    fn scan_audit(&self, start_timestamp: Option<i64>, limit: usize) -> Result<Vec<AuditEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let raw = self.inner().inner();
        let seek = match start_timestamp {
            Some(timestamp) => AuditKey::timestamp_prefix(timestamp.max(0) as u64).to_vec(),
            None => Vec::new(),
        };
        let iterator = raw.iter_from(AuditLogCol::CF_NAME, &seek, Direction::Asc)?;

        let mut out = Vec::with_capacity(limit.min(MAX_SCAN_PREALLOC));
        for (_key, value_bytes) in iterator {
            let entry: AuditEntry = wincode::deserialize(&value_bytes)
                .map_err(|error| TapeStoreError::Serialization(format!("audit entry: {}", error)))?;
            out.push(entry);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn max_audit_sequence(&self) -> Result<u64> {
        // Keys order by timestamp first, so a clock that stepped backwards could
        // leave the largest sequence under a non-maximal key; scan keys (without
        // their values) to take the true maximum.
        let raw = self.inner().inner();
        let mut max = 0u64;
        for key in raw.iter_keys_prefix(AuditLogCol::CF_NAME, &[])? {
            let decoded: AuditKey = wincode::deserialize(&key)
                .map_err(|error| TapeStoreError::Serialization(format!("audit key: {}", error)))?;
            max = max.max(decoded.sequence);
        }
        Ok(max)
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_crypto::address::Address;

    use super::*;
    use crate::types::{AuditDecision, AuditOp};

    fn store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn entry(timestamp: i64, reason: &str) -> AuditEntry {
        AuditEntry {
            timestamp,
            principal: Address::new_unique(),
            bucket: Address::new_unique(),
            op: AuditOp::Put,
            decision: AuditDecision::Deny,
            reason: reason.to_string(),
        }
    }

    /// Append with monotonically increasing sequences, mirroring the gateway's
    /// process-global counter.
    fn append_all(store: &TapeStore<MemoryStore>, entries: &[AuditEntry]) {
        for (sequence, entry) in entries.iter().enumerate() {
            store.append_audit(entry, sequence as u64).expect("append");
        }
    }

    fn reasons(entries: &[AuditEntry]) -> Vec<&str> {
        let mut out = Vec::new();
        for entry in entries {
            out.push(entry.reason.as_str());
        }
        out
    }

    // appends scan back in chronological order
    #[test]
    fn chronological() {
        let store = store();
        append_all(
            &store,
            &[
                entry(100, "a"),
                entry(100, "b"), // same instant -> sequence disambiguates
                entry(50, "early"),
                entry(200, "late"),
            ],
        );

        let all = store.scan_audit(None, 100).expect("scan");
        assert_eq!(reasons(&all), vec!["early", "a", "b", "late"]);
    }

    // scan respects the limit and start across timestamp boundaries
    #[test]
    fn scan_window() {
        let store = store();
        let entries: Vec<AuditEntry> = [10i64, 20, 30, 40]
            .into_iter()
            .map(|timestamp| entry(timestamp, &timestamp.to_string()))
            .collect();
        append_all(&store, &entries);

        let two = store.scan_audit(None, 2).expect("scan");
        assert_eq!(reasons(&two), vec!["10", "20"]);

        // Start mid-log and read across the remaining timestamp boundaries.
        let from_30 = store.scan_audit(Some(30), 100).expect("scan");
        assert_eq!(reasons(&from_30), vec!["30", "40"]);

        assert!(store.scan_audit(None, 0).expect("scan").is_empty());
    }

    // distinct sequences within one timestamp are not clobbered
    #[test]
    fn same_instant() {
        let store = store();
        let entries: Vec<AuditEntry> = (0..5).map(|index| entry(77, &format!("r{index}"))).collect();
        append_all(&store, &entries);

        let all = store.scan_audit(None, 100).expect("scan");
        assert_eq!(all.len(), 5);
        assert_eq!(reasons(&all), vec!["r0", "r1", "r2", "r3", "r4"]);
    }

    // the max sequence seeds a restart above every existing entry
    #[test]
    fn max_sequence() {
        let store = store();
        assert_eq!(store.max_audit_sequence().expect("max"), 0);

        store.append_audit(&entry(100, "a"), 7).expect("append");
        store.append_audit(&entry(90, "b"), 8).expect("append");

        assert_eq!(store.max_audit_sequence().expect("max"), 8);
    }
}
