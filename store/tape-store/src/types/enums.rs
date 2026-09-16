//! Enum types for tape-store

use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

use tape_core::types::{EpochNumber, SlotNumber};
use tape_crypto::address::Address;

/// System-owned object categories.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum SystemObjectKind {
    Snapshot { epoch: EpochNumber },
    History,
    Blacklist,
}

/// Information about a tracked object
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ObjectInfo {
    /// Object has been blacklisted
    Blacklisted,

    /// Object is invalid
    Invalid {
        epoch: EpochNumber,
        slot: SlotNumber,
    },

    /// Object is valid
    Valid {
        track_address: Address,
        registered_epoch: EpochNumber,
        certified_epoch: Option<EpochNumber>,
        slot: SlotNumber,
    },

    /// System/protocol track. These are live protocol metadata and are not
    /// counted as user objects for assignment sizing.
    System {
        kind: SystemObjectKind,
        track_address: Address,
        registered_epoch: EpochNumber,
        certified_epoch: Option<EpochNumber>,
        slot: SlotNumber,
    },
}

/// The set of buckets a credential is permitted to write to.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub enum CredentialScope {
    /// Any bucket whose on-chain authority is this credential's principal
    AnyOwned,
    /// An explicit allow-list of bucket tape addresses
    Buckets(Vec<Address>),
}

/// Lifecycle status of a credential. Revocation is a durable flip to `Revoked`,
/// effective immediately and fail-closed (a revoked credential never authorizes).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub enum CredentialStatus {
    /// The credential may authorize writes (subject to its other constraints)
    Active,
    /// The credential is permanently disabled
    Revoked,
}

/// The kind of write operation an audit decision concerns.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub enum AuditOp {
    /// `PutObject`
    Put,
    /// `DeleteObject`
    Delete,
    /// `CreateMultipartUpload`
    CreateMultipart,
    /// `UploadPart`
    UploadPart,
    /// `CompleteMultipartUpload`
    CompleteMultipart,
    /// `AbortMultipartUpload`
    Abort,
    /// An admin control-plane mutation.
    Admin,
}

/// The effect a policy rule asserts when it matches a write request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub enum PolicyEffect {
    /// The matched request is permitted (unless another matching rule denies it)
    Allow,
    /// The matched request is rejected; the rule's reason is logged
    Deny,
}

/// The action a policy rule matches.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub enum PolicyAction {
    /// Matches any write action
    Any,
    /// Matches `PutObject`
    Put,
    /// Matches `DeleteObject`
    Delete,
    /// Matches the multipart-upload lifecycle (create / upload-part / complete)
    Multipart,
}

/// The outcome an audit entry records for a write-authorization decision
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub enum AuditDecision {
    /// The write was admitted
    Allow,
    /// The write was rejected (the `reason` field carries the cause)
    Deny,
}

impl ObjectInfo {
    pub fn is_certified(&self) -> bool {
        matches!(
            self,
            ObjectInfo::Valid {
                certified_epoch: Some(_),
                ..
            } | ObjectInfo::System {
                    kind: SystemObjectKind::Snapshot { .. },
                    ..
                }
                | ObjectInfo::System {
                    certified_epoch: Some(_),
                    ..
                }
        )
    }

    pub fn is_live(&self) -> bool {
        matches!(
            self,
            ObjectInfo::Valid { .. } | ObjectInfo::System { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use tape_crypto::address::Address;

    use super::*;

    #[test]
    fn object_info_roundtrip() {
        use tape_core::types::SlotNumber;

        let infos = vec![
            ObjectInfo::Blacklisted,
            ObjectInfo::Invalid {
                epoch: EpochNumber(10),
                slot: SlotNumber(100),
            },
            ObjectInfo::Valid {
                track_address: Address::new([1u8; 32]),
                registered_epoch: EpochNumber(5),
                certified_epoch: Some(EpochNumber(6)),
                slot: SlotNumber(50),
            },
            ObjectInfo::Valid {
                track_address: Address::new([2u8; 32]),
                registered_epoch: EpochNumber(7),
                certified_epoch: None,
                slot: SlotNumber(70),
            },
            ObjectInfo::System {
                kind: SystemObjectKind::Snapshot {
                    epoch: EpochNumber(8),
                },
                track_address: Address::new([3u8; 32]),
                registered_epoch: EpochNumber(8),
                certified_epoch: None,
                slot: SlotNumber(80),
            },
            ObjectInfo::System {
                kind: SystemObjectKind::History,
                track_address: Address::new([4u8; 32]),
                registered_epoch: EpochNumber(8),
                certified_epoch: Some(EpochNumber(8)),
                slot: SlotNumber(80),
            },
        ];

        for info in infos {
            let bytes = wincode::serialize(&info).unwrap();
            let decoded: ObjectInfo = wincode::deserialize(&bytes).unwrap();
            assert_eq!(info, decoded);
        }
    }

    // credential scope round-trips through serialization
    #[test]
    fn scope() {
        let scopes = vec![
            CredentialScope::AnyOwned,
            CredentialScope::Buckets(vec![]),
            CredentialScope::Buckets(vec![Address::new([1u8; 32]), Address::new([2u8; 32])]),
        ];
        for scope in scopes {
            let bytes = wincode::serialize(&scope).expect("serialize");
            let decoded: CredentialScope = wincode::deserialize(&bytes).expect("deserialize");
            assert_eq!(scope, decoded);
        }
    }

    // status, audit, and policy enums round-trip
    #[test]
    fn enum_roundtrip() {
        for status in [CredentialStatus::Active, CredentialStatus::Revoked] {
            let bytes = wincode::serialize(&status).expect("serialize");
            assert_eq!(
                wincode::deserialize::<CredentialStatus>(&bytes).expect("deserialize"),
                status
            );
        }
        for op in [
            AuditOp::Put,
            AuditOp::Delete,
            AuditOp::CreateMultipart,
            AuditOp::UploadPart,
            AuditOp::CompleteMultipart,
            AuditOp::Abort,
            AuditOp::Admin,
        ] {
            let bytes = wincode::serialize(&op).expect("serialize");
            assert_eq!(wincode::deserialize::<AuditOp>(&bytes).expect("deserialize"), op);
        }
        for decision in [AuditDecision::Allow, AuditDecision::Deny] {
            let bytes = wincode::serialize(&decision).expect("serialize");
            assert_eq!(
                wincode::deserialize::<AuditDecision>(&bytes).expect("deserialize"),
                decision
            );
        }
        for effect in [PolicyEffect::Allow, PolicyEffect::Deny] {
            let bytes = wincode::serialize(&effect).expect("serialize");
            assert_eq!(
                wincode::deserialize::<PolicyEffect>(&bytes).expect("deserialize"),
                effect
            );
        }
        for action in [
            PolicyAction::Any,
            PolicyAction::Put,
            PolicyAction::Delete,
            PolicyAction::Multipart,
        ] {
            let bytes = wincode::serialize(&action).expect("serialize");
            assert_eq!(
                wincode::deserialize::<PolicyAction>(&bytes).expect("deserialize"),
                action
            );
        }
    }
}
