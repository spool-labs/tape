//! Value types for tape-store columns

use serde::{Deserialize, Serialize};
use tape_core::bls::BlsSignature;
use tape_core::track::blob::BlobEncoding;
use tape_core::types::{
    ContentType, EpochNumber, SlotNumber, SpoolIndex, StorageUnits, TapeNumber, TrackNumber,
};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use wincode::containers::{Pod, Vec as WincodeVec};
use wincode::len::BincodeLen;
use wincode_derive::{SchemaRead, SchemaWrite};

use super::enums::{
    AuditDecision, AuditOp, CredentialScope, CredentialStatus, PolicyAction, PolicyEffect,
};

const SLICE_BYTES_LIMIT: usize = 10 * 1024 * 1024;

/// A wrapper around a byte vector with a widened decode limit for track slice data
type SliceBytes = WincodeVec<Pod<u8>, BincodeLen<SLICE_BYTES_LIMIT>>;

/// Stored slice bytes with a widened decode limit
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct SliceValue(#[wincode(with = "SliceBytes")] pub Vec<u8>);

/// Snapshot build artifact retained until the corresponding `WriteSnapshot`
/// event lands locally and the staged slice is flushed into `SliceCol`.
///
/// `spool_index` is the exact key the slice belongs at, captured at build time
/// so the event handler doesn't re-derive it from protocol state. The bytes in
/// `slice` are the Clay slice at position (`spool_index - group.base_spool()`); they
/// verify against `blob.leaves[position]`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct SnapshotArtifact {
    pub blob: BlobEncoding,
    pub spool_index: SpoolIndex,
    #[wincode(with = "SliceBytes")]
    pub slice: Vec<u8>,
}

/// Metadata about a tape (storage allocation)
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct TapeInfo {
    /// Unique tape identifier
    pub id: TapeNumber,

    /// Tape behavior flags
    pub flags: u64,

    /// Epoch when the tape expires
    pub end_epoch: EpochNumber,

    /// Next monotonic track number expected for this tape
    pub next_track_number: TrackNumber,
}

impl TapeInfo {
    /// Create a tape info from its identifier, flags, end epoch, and next track number
    pub fn new(id: TapeNumber, flags: u64, end_epoch: EpochNumber, next_track_number: TrackNumber) -> Self {
        Self {
            id,
            flags,
            end_epoch,
            next_track_number,
        }
    }
}

/// Proof data needed to submit an on-chain track invalidation
#[derive(Clone, Debug, Deserialize, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct InvalidationProof {
    pub bitmap: u128,
    pub signature: BlsSignature,
    pub computed_root: [u8; 32],
}

/// Listing-plane metadata for one object, keyed in `object_list` by
/// `[bucket][name]`. Carries exactly what an S3 listing page returns per object
/// (size, etag, last-modified) plus a pointer to the object track, so a listing
/// is a single range scan with no per-object lookups.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct ObjectListEntry {
    /// Object size in bytes
    pub size: StorageUnits,
    /// ETag: the object track's commitment / content root
    pub etag: Hash,
    /// Wall-clock last-modified time (unix seconds), when the block had one
    pub block_time: Option<i64>,
    /// Slot the write was applied at — the precise monotonic order/tiebreak
    pub slot: SlotNumber,
    /// Data tape holding the object-representing track
    pub data_tape: Address,
    /// Track number of the object-representing track on `data_tape`
    pub track_number: TrackNumber,
    /// Storage kind discriminator (`TrackKind::Inline` / `TrackKind::Coded`)
    pub kind: u64,
    /// Hot content type; precise custom strings are deferred to the data plane
    pub content_type: ContentType,
}

/// Name metadata keyed by object track address
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct ObjectMetadata {
    /// Plaintext object name as provided on the write path
    pub name: Vec<u8>,
    /// Hot content type
    pub content_type: ContentType,
}

/// The write operations a credential is permitted to perform. Fail-closed: a cap
/// that is `false` denies that operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct CredentialCaps {
    /// May perform `PutObject`
    pub can_put: bool,
    /// May perform `DeleteObject`
    pub can_delete: bool,
    /// May perform the multipart-upload lifecycle
    pub can_multipart: bool,
}

impl CredentialCaps {
    /// Caps granting every write operation
    pub fn all() -> Self {
        Self {
            can_put: true,
            can_delete: true,
            can_multipart: true,
        }
    }

    /// Caps granting no write operation (the fail-closed default)
    pub fn none() -> Self {
        Self {
            can_put: false,
            can_delete: false,
            can_multipart: false,
        }
    }
}

/// A durable S3 write credential, keyed in `credential` by its access key id.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct Credential {
    /// `HMAC-SHA256(secret_access_key, server_pepper)` — the only form of the
    /// secret ever persisted.
    pub secret_hmac: [u8; 32],
    /// Owner authority pubkey this credential acts on behalf of
    pub principal: Address,
    /// Which buckets this credential may write to
    pub scope: CredentialScope,
    /// Which write operations this credential may perform
    pub caps: CredentialCaps,
    /// Active or revoked. A revoked credential never authorizes
    pub status: CredentialStatus,
    /// Optional expiry as a unix timestamp (seconds). `None` never expires
    pub not_after: Option<i64>,
    /// Metering grade this key reads under. `None` uses the operator default
    pub grade: Option<String>,
}

impl Credential {
    /// Constant-time compare the stored secret HMAC against a freshly computed
    /// `HMAC-SHA256(secret, pepper)`. 
    pub fn verify_secret_hmac(&self, computed: &[u8; 32]) -> bool {
        let mut difference = 0u8;
        for (stored_byte, candidate_byte) in self.secret_hmac.iter().zip(computed.iter()) {
            difference |= stored_byte ^ candidate_byte;
        }
        difference == 0
    }

    /// Whether the credential is usable at `now` (unix seconds).
    pub fn is_usable(&self, now: i64) -> bool {
        matches!(self.status, CredentialStatus::Active)
            && self.not_after.map_or(true, |not_after| now < not_after)
    }

    /// Whether this credential's scope admits writes to `bucket`.
    pub fn allows_bucket(&self, bucket: &Address) -> bool {
        match &self.scope {
            CredentialScope::AnyOwned => true,
            CredentialScope::Buckets(buckets) => buckets.contains(bucket),
        }
    }
}

/// One append-only entry in the write-authorization audit log, stored in
/// `audit_log` ordered by time. 
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct AuditEntry {
    /// When the decision was made (unix seconds). Also drives the log ordering
    pub timestamp: i64,
    /// The acting principal (owner authority). `Address::default()` when the
    /// request was anonymous or the credential could not be resolved.
    pub principal: Address,
    /// The bucket tape the write targeted
    pub bucket: Address,
    /// The write operation that was attempted
    pub op: AuditOp,
    /// Allow or deny
    pub decision: AuditDecision,
    /// Operator-facing reason code/message for the decision
    pub reason: String,
}

/// One ordered rule in the write-authorization policy engine.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct PolicyRule {
    /// Principal (owner authority) this rule matches; `None` matches any principal
    pub principal: Option<Address>,
    /// Bucket (tape) this rule matches; `None` matches any bucket
    pub bucket: Option<Address>,
    /// Action this rule matches
    pub action: PolicyAction,
    /// Allow or deny when matched
    pub effect: PolicyEffect,
    /// Operator-facing reason code recorded in the audit log on every decision
    /// this rule drives.
    pub reason: String,
}

impl PolicyRule {
    /// Whether this rule matches a concrete `(principal, bucket, action)` request.
    /// A `None` subject is a wildcard; an Any rule matches every action.
    pub fn matches(&self, principal: &Address, bucket: &Address, action: PolicyAction) -> bool {
        self.principal.map_or(true, |rule_principal| rule_principal == *principal)
            && self.bucket.map_or(true, |rule_bucket| rule_bucket == *bucket)
            && (matches!(self.action, PolicyAction::Any) || self.action == action)
    }
}

/// Per-principal write budgets.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct BudgetLimits {
    /// Lamports of SOL fees a principal may spend per rolling day
    pub sol_per_day: u64,
    /// Bytes a principal may write per rolling day
    pub bytes_per_day: u64,
    /// `PutObject` operations a principal may perform per rolling hour
    pub puts_per_hour: u32,
    /// Concurrent in-flight multipart uploads a principal may hold open
    pub max_concurrent_multipart: u32,
}

/// Durable write-authorization control state — the singleton row in `auth_state`.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct AuthState {
    /// When true, every S3 write is denied (a global, instant pause)
    pub is_kill_switch_engaged: bool,
    /// Monotonic policy version; bumped on each policy mutation
    pub policy_version: u64,
    /// Operator-set default-budget override; `None` falls back to the YAML
    /// defaults applied by the accounting ledger.
    pub default_budget: Option<BudgetLimits>,
}

/// Per-principal accounting ledger row — the durable cost meter the write
/// chokepoint reserves against, keyed in `ledger` by the owner authority.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct LedgerEntry {
    /// Unix-seconds start of the current hourly window (write count)
    pub writes_window_start: i64,
    /// Committed writes (`PutObject` / `CompleteMultipartUpload`) in the current
    /// hourly window. Gated by `puts_per_hour`.
    pub writes_committed: u32,

    /// Unix-seconds start of the current daily window (bytes + SOL)
    pub daily_window_start: i64,
    /// Committed object bytes written in the current daily window. Gated by
    /// `bytes_per_day`.
    pub bytes_committed: u64,
    /// Committed SOL fees (lamports) spent in the current daily window. Gated by
    /// `sol_per_day`.
    pub sol_committed: u64,

    /// Outstanding reserved writes (uncommitted), window-independent
    pub writes_reserved: u32,
    /// Outstanding reserved bytes (uncommitted), window-independent
    pub bytes_reserved: u64,
    /// Outstanding reserved lamports (uncommitted), window-independent
    pub sol_reserved: u64,

    /// Lifetime committed writes
    pub writes_total: u64,
    /// Lifetime committed object bytes
    pub bytes_total: u64,
    /// Lifetime committed on-chain operations (writes + deletes)
    pub onchain_ops_total: u64,
    /// Lifetime committed SOL fees (lamports)
    pub sol_spent_total: u64,
    /// Lifetime tape capacity consumed (object bytes that occupy tape space)
    pub capacity_consumed_total: u64,

    /// Monotonic per-principal reservation sequence, bumped on each reserve so
    /// concurrent reservations in the same second get distinct keys.
    pub next_reservation_sequence: u64,

    /// Operator-set per-principal budget override. `None` falls back to the
    /// operator default-budget override (in `auth_state`) and then the YAML
    /// defaults.
    pub budget_override: Option<BudgetLimits>,
}

/// One outstanding budget reservation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct LedgerReservation {
    /// Reserved write count released on commit/refund (0 or 1)
    pub writes: u32,
    /// Reserved bytes released on commit/refund
    pub bytes: u64,
    /// Reserved lamports released on commit/refund
    pub sol: u64,
    /// Whether committing increments the cumulative on-chain-op meter
    pub is_onchain: bool,
    /// Whether committing meters tape capacity consumption
    pub meters_capacity: bool,
}

/// Decode limit for a buffered multipart part: S3's 5 GiB maximum part size.
const MULTIPART_PART_BYTES_LIMIT: usize = 5 * 1024 * 1024 * 1024;

/// Buffered multipart part bytes with a widened decode limit
type MultipartPartBytes = WincodeVec<Pod<u8>, BincodeLen<MULTIPART_PART_BYTES_LIMIT>>;

/// An in-progress S3 multipart upload's target, keyed in `s3_multipart_upload`
/// by its opaque upload id.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct MultipartUpload {
    /// Bucket (tape address) the assembled object will land on
    pub bucket: Address,
    /// Object key (the on-chain track name) the assembled object will take
    pub key: String,
    /// Content type captured at CreateMultipartUpload, applied to the object
    pub content_type: ContentType,
    /// Initiation time (unix seconds)
    pub initiated: i64,
    /// Owner authority that opened the upload; bounds the per-principal
    /// concurrent-upload budget (the live record count is the open-upload count)
    pub principal: Address,
}

/// One uploaded multipart part's metadata, kept apart from its payload so
/// listing an upload's parts never reads the buffered bytes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct MultipartPart {
    /// Part number (1..=10000)
    pub part_number: u32,
    /// Per-part content-hash ETag returned by UploadPart and echoed at completion
    pub etag: Hash,
    /// Upload time (unix seconds)
    pub last_modified: i64,
    /// Size of the buffered payload in bytes
    pub size: u64,
}

/// The buffered bytes of one multipart part, stored in its own column so part
/// metadata (ListParts, completion validation) loads without the payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, SchemaRead, SchemaWrite, Serialize)]
pub struct MultipartPartData {
    /// Raw part bytes
    #[wincode(with = "MultipartPartBytes")]
    pub data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{GROUP_SIZE, SLICE_TREE_HEIGHT};
    use tape_core::track::blob::BlobEncoding;
    use tape_core::track::types::{CompressedTrack, PackedTrack};
    use tape_core::types::{StorageUnits, StripeCount};
    use tape_crypto::merkle::root_from_leaf_hashes;
    use tape_crypto::Hash;

    use super::*;
    use super::{
        AuditDecision, AuditOp, CredentialScope, CredentialStatus, PolicyAction, PolicyEffect,
    };

    // an object listing entry round-trips through serialization
    #[test]
    fn list_entry() {
        let entry = ObjectListEntry {
            size: StorageUnits(4096),
            etag: Hash::from([7u8; 32]),
            block_time: Some(1_700_000_123),
            slot: SlotNumber(42),
            data_tape: Address::new([9u8; 32]),
            track_number: TrackNumber(3),
            kind: 1,
            content_type: ContentType::ImageJpeg,
        };
        let bytes = wincode::serialize(&entry).unwrap();
        let decoded: ObjectListEntry = wincode::deserialize(&bytes).unwrap();
        assert_eq!(entry, decoded);
    }

    // object metadata round-trips through serialization
    #[test]
    fn object_metadata() {
        let metadata = ObjectMetadata {
            name: b"photos/cat.jpg".to_vec(),
            content_type: ContentType::ImageJpeg,
        };

        let bytes = wincode::serialize(&metadata).unwrap();
        let decoded: ObjectMetadata = wincode::deserialize(&bytes).unwrap();
        assert_eq!(metadata, decoded);
    }

    // a credential round-trips through serialization
    #[test]
    fn credential() {
        let credentials = vec![
            Credential {
                secret_hmac: [0xAB; 32],
                principal: Address::new([1u8; 32]),
                scope: CredentialScope::AnyOwned,
                caps: CredentialCaps::all(),
                status: CredentialStatus::Active,
                not_after: Some(1_700_000_000),
                grade: Some("firehose".to_string()),
            },
            Credential {
                secret_hmac: [0u8; 32],
                principal: Address::new([2u8; 32]),
                scope: CredentialScope::Buckets(vec![Address::new([9u8; 32])]),
                caps: CredentialCaps::none(),
                status: CredentialStatus::Revoked,
                not_after: None,
                grade: None,
            },
        ];
        for credential in credentials {
            let bytes = wincode::serialize(&credential).expect("serialize");
            let decoded: Credential = wincode::deserialize(&bytes).expect("deserialize");
            assert_eq!(credential, decoded);
        }
    }

    // an exact HMAC match is accepted, a wrong byte is rejected
    #[test]
    fn hmac_exact() {
        let credential = Credential {
            secret_hmac: [0x5A; 32],
            principal: Address::new([1u8; 32]),
            scope: CredentialScope::AnyOwned,
            caps: CredentialCaps::all(),
            status: CredentialStatus::Active,
            not_after: None,
            grade: None,
        };
        assert!(credential.verify_secret_hmac(&[0x5A; 32]));
        let mut wrong = [0x5A; 32];
        wrong[31] ^= 0x01;
        assert!(!credential.verify_secret_hmac(&wrong));
    }

    // the HMAC compare rejects a mismatch at any byte position
    #[test]
    fn constant_time() {
        // The compare must accumulate the difference across all 32 bytes and never
        // short-circuit, so a mismatch at any position — first, middle, or last —
        // is rejected identically (the constant-time invariant, observable as
        // position-independent rejection).
        let credential = Credential {
            secret_hmac: [0xA5; 32],
            principal: Address::new([2u8; 32]),
            scope: CredentialScope::AnyOwned,
            caps: CredentialCaps::all(),
            status: CredentialStatus::Active,
            not_after: None,
            grade: None,
        };
        assert!(credential.verify_secret_hmac(&[0xA5; 32]), "exact match accepts");
        for differing_byte in [0usize, 1, 15, 16, 30, 31] {
            let mut wrong = [0xA5; 32];
            wrong[differing_byte] ^= 0xFF;
            assert!(
                !credential.verify_secret_hmac(&wrong),
                "a mismatch at byte {differing_byte} must reject",
            );
        }
    }

    // usability is fail-closed on expiry and revocation
    #[test]
    fn usability() {
        let base = Credential {
            secret_hmac: [0u8; 32],
            principal: Address::new([1u8; 32]),
            scope: CredentialScope::AnyOwned,
            caps: CredentialCaps::all(),
            status: CredentialStatus::Active,
            not_after: Some(100),
            grade: None,
        };
        assert!(base.is_usable(99));
        assert!(!base.is_usable(100), "expired exactly at not_after");
        assert!(!base.is_usable(101));

        let revoked = Credential {
            status: CredentialStatus::Revoked,
            not_after: None,
            grade: None,
            ..base.clone()
        };
        assert!(!revoked.is_usable(0), "revoked is never usable");

        let no_expiry = Credential {
            not_after: None,
            grade: None,
            ..base
        };
        assert!(no_expiry.is_usable(i64::MAX));
    }

    // scope admits exactly the permitted buckets
    #[test]
    fn scope_buckets() {
        let bucket = Address::new([7u8; 32]);
        let other = Address::new([8u8; 32]);

        let any = Credential {
            secret_hmac: [0u8; 32],
            principal: Address::new([1u8; 32]),
            scope: CredentialScope::AnyOwned,
            caps: CredentialCaps::all(),
            status: CredentialStatus::Active,
            not_after: None,
            grade: None,
        };
        assert!(any.allows_bucket(&bucket));

        let scoped = Credential {
            scope: CredentialScope::Buckets(vec![bucket]),
            ..any
        };
        assert!(scoped.allows_bucket(&bucket));
        assert!(!scoped.allows_bucket(&other));
    }

    // a policy rule round-trips and matches the right requests
    #[test]
    fn policy_rule() {
        let principal = Address::new([1u8; 32]);
        let bucket = Address::new([2u8; 32]);
        let rules = vec![
            PolicyRule {
                principal: Some(principal),
                bucket: Some(bucket),
                action: PolicyAction::Put,
                effect: PolicyEffect::Allow,
                reason: "owner may put".to_string(),
            },
            PolicyRule {
                principal: None,
                bucket: None,
                action: PolicyAction::Any,
                effect: PolicyEffect::Deny,
                reason: "default deny".to_string(),
            },
        ];
        for rule in &rules {
            let bytes = wincode::serialize(rule).expect("serialize");
            let decoded: PolicyRule = wincode::deserialize(&bytes).expect("deserialize");
            assert_eq!(*rule, decoded);
        }

        // Exact match.
        assert!(rules[0].matches(&principal, &bucket, PolicyAction::Put));
        // Wrong action does not match a specific-action rule.
        assert!(!rules[0].matches(&principal, &bucket, PolicyAction::Delete));
        // Wrong bucket does not match.
        assert!(!rules[0].matches(&principal, &Address::new([9u8; 32]), PolicyAction::Put));
        // The wildcard rule matches anything.
        assert!(rules[1].matches(&Address::new([7u8; 32]), &Address::new([8u8; 32]), PolicyAction::Delete));
    }

    // auth state round-trips and has the expected default
    #[test]
    fn auth_state() {
        assert_eq!(
            AuthState::default(),
            AuthState {
                is_kill_switch_engaged: false,
                policy_version: 0,
                default_budget: None,
            },
        );

        let states = vec![
            AuthState::default(),
            AuthState {
                is_kill_switch_engaged: true,
                policy_version: 7,
                default_budget: Some(BudgetLimits {
                    sol_per_day: 1_000,
                    bytes_per_day: 2_000,
                    puts_per_hour: 30,
                    max_concurrent_multipart: 4,
                }),
            },
        ];
        for state in states {
            let bytes = wincode::serialize(&state).expect("serialize");
            let decoded: AuthState = wincode::deserialize(&bytes).expect("deserialize");
            assert_eq!(state, decoded);
        }
    }

    // a ledger entry round-trips through serialization
    #[test]
    fn ledger_entry() {
        assert_eq!(LedgerEntry::default(), LedgerEntry::default());
        let entry = LedgerEntry {
            writes_window_start: 1_700_000_000,
            writes_committed: 7,
            daily_window_start: 1_700_000_001,
            bytes_committed: 4096,
            sol_committed: 5_000,
            writes_reserved: 1,
            bytes_reserved: 1024,
            sol_reserved: 5_000,
            writes_total: 100,
            bytes_total: 1_048_576,
            onchain_ops_total: 120,
            sol_spent_total: 600_000,
            capacity_consumed_total: 1_048_576,
            next_reservation_sequence: 42,
            budget_override: Some(BudgetLimits {
                sol_per_day: 1,
                bytes_per_day: 2,
                puts_per_hour: 3,
                max_concurrent_multipart: 4,
            }),
        };
        let bytes = wincode::serialize(&entry).expect("serialize");
        let decoded: LedgerEntry = wincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(entry, decoded);
    }

    // a ledger reservation round-trips through serialization
    #[test]
    fn ledger_reservation() {
        let reservation = LedgerReservation {
            writes: 1,
            bytes: 65_536,
            sol: 5_000,
            is_onchain: true,
            meters_capacity: true,
        };
        let bytes = wincode::serialize(&reservation).expect("serialize");
        let decoded: LedgerReservation = wincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(reservation, decoded);
    }

    // an audit entry round-trips through serialization
    #[test]
    fn audit_entry() {
        let entry = AuditEntry {
            timestamp: 1_700_000_123,
            principal: Address::new([3u8; 32]),
            bucket: Address::new([4u8; 32]),
            op: AuditOp::Put,
            decision: AuditDecision::Deny,
            reason: "credential not active".to_string(),
        };
        let bytes = wincode::serialize(&entry).expect("serialize");
        let decoded: AuditEntry = wincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(entry, decoded);
    }

    // a tape info round-trips through serialization
    #[test]
    fn tape_info() {
        let info = TapeInfo {
            id: TapeNumber(1),
            flags: 0,
            end_epoch: EpochNumber(200),
            next_track_number: TrackNumber(0),
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: TapeInfo = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    // a packed track round-trips through serialization
    #[test]
    fn packed_track() {
        let info: PackedTrack = [1u8; core::mem::size_of::<CompressedTrack>()];

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: PackedTrack = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    // a blob encoding round-trips through serialization
    #[test]
    fn blob_encoding() {
        let info = BlobEncoding {
            size: StorageUnits(512),
            commitment: Hash::from([3u8; 32]),
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(2),
            leaves: [Hash::default(); GROUP_SIZE],
        };

        let bytes = wincode::serialize(&info).unwrap();
        let decoded: BlobEncoding = wincode::deserialize(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    // the blob commitment root matches its commitment
    #[test]
    fn commitment_root() {
        let leaves = [Hash::default(); GROUP_SIZE];
        let info = BlobEncoding {
            size: StorageUnits(1024),
            commitment: root_from_leaf_hashes::<{ SLICE_TREE_HEIGHT }>(&leaves),
            profile: EncodingProfile::clay_default(),
            stripe_size: StorageUnits::from_bytes(128),
            stripe_count: StripeCount(1),
            leaves,
        };

        assert_eq!(info.commitment_root(), info.commitment);
    }
}
