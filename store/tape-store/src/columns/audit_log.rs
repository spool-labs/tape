//! Append-only write-authorization audit log column family.

use store::Column;

use crate::types::{AuditEntry, AuditKey};

/// Append-only log of write-authorization decisions, ordered by time.
pub struct AuditLogCol;

impl Column for AuditLogCol {
    const CF_NAME: &'static str = "audit_log";
    type Key = AuditKey;
    type Value = AuditEntry;
}
