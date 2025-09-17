use steel::*;
use crate::{state, types::{NetworkAddress, TAPE, Coin, Balance}, hash::Hash};
use crate::define_u64_type;

define_u64_type!(BasisPoints, "bps");
define_u64_type!(VersionNumber, "version");
define_u64_type!(EpochNumber, "epoch");
define_u64_type!(ArchiveNumber, "archive");
define_u64_type!(SpoolNumber, "spool");

const EPOCH_DURATION: u64 = 7 * 24 * 60 * 60 * 1000;

/// The System struct represents the overall archive system, it defines core parameters and versioning.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct System {
    /// The minimum version required to be part of the committee.
    pub version: VersionNumber,

    /// The current committee of nodes responsible for this epoch.
    pub committee: [Pubkey; 256],

    /// The current active set of nodes.
    pub active_set: [Pubkey; 256],

    /// The current voted on price to write data (in TAPE tokens per byte).
    pub write_price: Coin<TAPE>,

    /// The current voted on price to store data (in TAPE tokens per byte per epoch).
    pub storage_price: Coin<TAPE>,

    /// Future epoch accounting 
    pub future_accounting: FutureAccountingRingBuffer<256>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Epoch {
    /// The current epoch number.
    pub number: EpochNumber,

    /// The timestamp of the last epoch transition.
    pub last_epoch_at: i64,
}

pub fn current_epoch(epoch: &Epoch) -> EpochNumber {
    epoch.number
}

pub fn next_epoch(epoch: &Epoch) -> EpochNumber {
    EpochNumber::new(epoch.number.as_u64() + 1)
}

/// A ring buffer to hold FutureAccounting entries.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FutureAccountingRingBuffer<const N: usize> {
    pub index: u64,
    pub length: u64,
    pub entries: [FutureAccounting; N],
}

unsafe impl<const N: usize> Zeroable for FutureAccountingRingBuffer<N> {}
unsafe impl<const N: usize> Pod for FutureAccountingRingBuffer<N> {}

impl<const N: usize> FutureAccountingRingBuffer<N> {
    /// Returns true if the buffer has no entries.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns true if the buffer is full.
    pub fn is_full(&self) -> bool {
        self.length as usize == N
    }

    /// Returns the current number of entries.
    pub fn len(&self) -> usize {
        self.length as usize
    }

    /// Returns the maximum capacity.
    pub fn capacity(&self) -> usize {
        N
    }

    /// Push a new accounting entry into the ring buffer.
    /// If full, overwrites the oldest entry.
    pub fn push(&mut self, entry: FutureAccounting) {
        let idx = (self.index + self.length) % N as u64;
        self.entries[idx as usize] = entry;

        if self.is_full() {
            // overwrite: advance the start
            self.index = (self.index + 1) % N as u64;
        } else {
            self.length += 1;
        }
    }

    /// Returns a reference to the most recent entry, if any.
    pub fn back(&self) -> Option<&FutureAccounting> {
        if self.is_empty() {
            None
        } else {
            let idx = (self.index + self.length - 1) % N as u64;
            Some(&self.entries[idx as usize])
        }
    }

    /// Returns a reference to the oldest entry, if any.
    pub fn front(&self) -> Option<&FutureAccounting> {
        if self.is_empty() {
            None
        } else {
            Some(&self.entries[self.index as usize])
        }
    }

    /// Get an entry by relative index (0 = oldest).
    pub fn get(&self, i: usize) -> Option<&FutureAccounting> {
        if i >= self.len() {
            None
        } else {
            let idx = (self.index + i as u64) % N as u64;
            Some(&self.entries[idx as usize])
        }
    }

    /// Iterate over entries in order from oldest to newest.
    pub fn iter(&self) -> impl Iterator<Item = &FutureAccounting> {
        (0..self.len()).map(move |i| {
            let idx = (self.index + i as u64) % N as u64;
            &self.entries[idx as usize]
        })
    }
}

/// FutureAccounting holds accounting information for future epochs.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct FutureAccounting {
    /// The epoch number this accounting entry is for.
    pub epoch: EpochNumber,

    /// The total storage used in the system at this epoch.
    pub total_storage_used: u64,

    /// The rewards to be distributed at this epoch.
    pub rewards: Balance<TAPE>,
}

#[derive(Debug, PartialEq)]
pub enum PoolError {
    InvalidCommissionRate,
}

/// A Pool represents a staking pool that members can join.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Pool {
    /// The authority that owns this pool.
    pub authority: Pubkey,

    /// The current state of the pool.
    pub state: u64,

    /// The total stake balance in the pool.
    pub total_stake: Balance<TAPE>,

    /// The total number of shares issued by the pool.
    pub total_shares: u64,

    /// The commission rate taken by the pool (in basis points).
    pub commission_rate: BasisPoints,

    /// The epoch when this pool will be actived.
    pub activation_epoch: EpochNumber,

    /// The epoch when this pool was last updated.
    pub last_updated_at: EpochNumber,
}

pub fn register_pool(
    epoch: &Epoch,
    authority: Pubkey,
    commission_rate: BasisPoints,
) -> Result<Pool, PoolError> {
    if commission_rate.as_u64() > 10_000 {
        return Err(PoolError::InvalidCommissionRate);
    }

    let activation_epoch = next_epoch(epoch);
    let last_updated_at = current_epoch(epoch);
    let total_stake = TAPE::new(0);

    Ok(Pool {
        authority,
        state: 0,
        total_stake,
        total_shares: 0,
        commission_rate,
        activation_epoch,
        last_updated_at,
    })
}

#[derive(Debug, PartialEq)]
pub enum NodeError {
    InvalidStorageCapacity,
}

/// A Node represents a storage node in the archive system.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Node {
    /// The authority that owns this node.
    pub authority: Pubkey,

    /// The name of this node
    pub name: [u8; 32],

    /// The SocketAddr of the node
    pub network_address: NetworkAddress,

    /// The public key used for TLS connections to this node.
    pub network_tls: Pubkey,

    /// The staking pool this node is associated with.
    pub pool: Pubkey,

    /// The storage capacity of the node in bytes.
    pub storage_capacity: u64,

    /// The storage used by the node in bytes.
    pub storage_used: u64,

    /// The version of software the node is running.
    pub version: VersionNumber,

    /// The epoch when this node was registered.
    pub registered_epoch: EpochNumber,
}

pub fn register_node(
    epoch: &Epoch,
    pool: &Pool,
    authority: Pubkey,
    pool_address: Pubkey,
    name: [u8; 32],
    network_address: NetworkAddress,
    network_tls: Pubkey,
    storage_capacity: u64,
    version: VersionNumber,
) -> Result<Node, NodeError> {
    if storage_capacity == 0 {
        return Err(NodeError::InvalidStorageCapacity);
    }

    let registered_epoch = current_epoch(epoch);

    Ok(Node {
        authority,
        name,
        network_address,
        network_tls,
        pool: pool_address,
        storage_capacity,
        storage_used: 0,
        version,
        registered_epoch,
    })
}

/// A Share represents a staking contribution to a Pool.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Share {
    /// The authority that owns this share.
    pub authority: Pubkey,

    /// The stake balance the authority has deposited and may unstake.
    pub principal: Balance<TAPE>,

    /// The epoch when this share was registered.
    pub registered_epoch: EpochNumber,

    /// The epoch when this share is activated.
    pub activation_epoch: EpochNumber,

    /// The staking pool this share is associated with.
    pub pool: Pubkey,
}

pub fn stake_with_pool(
    epoch: &Epoch,
    pool: &Pool,
    pool_address: Pubkey,
    authority: Pubkey,
    amount: Balance<TAPE>,
) -> Share {

    let activation_epoch = next_epoch(epoch);
    let registered_epoch = current_epoch(epoch);

    Share {
        authority,
        principal: amount,
        registered_epoch,
        activation_epoch,
        pool: pool_address,
    }
}


/// An Archive is a logical collection of Spools.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Archive {
    pub id: ArchiveNumber,

    /// The encoding scheme used by this archive (e.g., erasure coding = 0, replication = 1).
    pub encoding: u64,

    /// The number of data shards (spools) in the encoding scheme.
    pub spool_count: u64,

    /// The registered epoch of the archive.
    pub registered_epoch: EpochNumber,

    /// The certified epoch of the archive.
    pub certified_epoch: EpochNumber,

    /// The total storage capacity of the archive.
    pub storage_capacity: u64,

    /// The total storage used by the archive.
    pub storage_used: u64,
}

/// A Spool is a collection of Tapes.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Spool {
    /// The index of this spool within the archive.
    pub id: SpoolNumber,

    /// The archive this spool belongs to.
    pub archive: Pubkey,

    /// The total storage capacity of the spool.
    pub storage_capacity: u64,

    /// The total storage used by the spool.
    pub storage_used: u64,
}

#[derive(Debug, PartialEq)]
pub enum TapeError {
    InvalidEpoch,
    IncompatibleEpochs,
    IncompatibleAmounts,
    InvalidEpochRange,
}

/// A Tape is a storage resource used to store Blobs.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Tape {
    /// The authority that owns this tape.
    pub authority: Pubkey,

    /// The spool this tape belongs to.
    pub spool: Pubkey,

    /// The total storage capacity of the tape.
    pub storage_capacity: u64,

    /// The total storage used by the tape.
    pub storage_used: u64,

    /// The epoch when this tape is valid from.
    /// (no data can be written to the tape before this point).
    pub start_epoch: EpochNumber,

    /// The epoch when this tape is valid until 
    /// (any data written to the tape is deleted at this point).
    pub end_epoch: EpochNumber,
}

pub fn create_tape(
    authority: Pubkey,
    spool: Pubkey,
    storage_capacity: u64,
    start_epoch: EpochNumber,
    end_epoch: EpochNumber
) -> Result<Tape, TapeError> {
    if start_epoch >= end_epoch {
        return Err(TapeError::InvalidEpochRange);
    }

    Ok(Tape {
        authority,
        spool,
        storage_capacity,
        storage_used: 0,
        start_epoch,
        end_epoch,
    })
}

/// Extend the tape's end epoch
pub fn extend_tape(tape: &Tape, additional_epochs: u64) -> Result<Tape, TapeError> {
    if additional_epochs == 0 {
        return Err(TapeError::InvalidEpoch);
    }
    let mut new_tape = *tape;
    new_tape.end_epoch = EpochNumber::new(tape.end_epoch.as_u64() + additional_epochs);
    Ok(new_tape)
}

/// Split tape by epoch
pub fn split_by_epoch(tape: &Tape, split_epoch: EpochNumber) -> Result<(Tape, Tape), TapeError> {
    if split_epoch <= tape.start_epoch || split_epoch >= tape.end_epoch {
        return Err(TapeError::InvalidEpoch);
    }

    let first_tape = Tape {
        authority: tape.authority,
        spool: tape.spool,
        storage_capacity: tape.storage_capacity,
        storage_used: tape.storage_used,
        start_epoch: tape.start_epoch,
        end_epoch: split_epoch,
    };

    let second_tape = Tape {
        authority: tape.authority,
        spool: tape.spool,
        storage_capacity: tape.storage_capacity,
        storage_used: tape.storage_used,
        start_epoch: split_epoch,
        end_epoch: tape.end_epoch,
    };

    Ok((first_tape, second_tape))
}

/// Split tape by size
pub fn split_by_size(tape: &Tape, split_size: u64) -> Result<(Tape, Tape), TapeError> {
    if split_size == 0 || split_size >= tape.storage_capacity {
        return Err(TapeError::IncompatibleAmounts);
    }
    if tape.storage_used > split_size {
        return Err(TapeError::IncompatibleAmounts);
    }

    let first_tape = Tape {
        authority: tape.authority,
        spool: tape.spool,
        storage_capacity: split_size,
        storage_used: tape.storage_used,
        start_epoch: tape.start_epoch,
        end_epoch: tape.end_epoch,
    };

    let remaining_capacity = tape.storage_capacity - split_size;

    let second_tape = Tape {
        authority: tape.authority,
        spool: tape.spool,
        storage_capacity: remaining_capacity,
        storage_used: 0,
        start_epoch: tape.start_epoch,
        end_epoch: tape.end_epoch,
    };

    Ok((first_tape, second_tape))
}

/// Merge two tapes by period (adjacent time ranges)
pub fn merge_by_period(tape1: &Tape, tape2: &Tape) -> Result<Tape, TapeError> {
    if tape1.storage_capacity != tape2.storage_capacity {
        return Err(TapeError::IncompatibleAmounts);
    }
    if tape1.end_epoch != tape2.start_epoch {
        return Err(TapeError::IncompatibleEpochs);
    }
    if tape1.authority != tape2.authority || tape1.spool != tape2.spool {
        return Err(TapeError::IncompatibleAmounts);
    }

    let total_storage_used = tape1.storage_used + tape2.storage_used;

    Ok(Tape {
        authority: tape1.authority,
        spool: tape1.spool,
        storage_capacity: tape1.storage_capacity,
        storage_used: total_storage_used,
        start_epoch: tape1.start_epoch,
        end_epoch: tape2.end_epoch,
    })
}

/// Merge two tapes by amount (same time range)
pub fn merge_by_amount(tape1: &Tape, tape2: &Tape) -> Result<Tape, TapeError> {
    if tape1.start_epoch != tape2.start_epoch || tape1.end_epoch != tape2.end_epoch {
        return Err(TapeError::IncompatibleEpochs);
    }
    if tape1.authority != tape2.authority || tape1.spool != tape2.spool {
        return Err(TapeError::IncompatibleAmounts);
    }

    let total_storage_capacity = tape1.storage_capacity + tape2.storage_capacity;
    let total_storage_used = tape1.storage_used + tape2.storage_used;

    Ok(Tape {
        authority: tape1.authority,
        spool: tape1.spool,
        storage_capacity: total_storage_capacity,
        storage_used: total_storage_used,
        start_epoch: tape1.start_epoch,
        end_epoch: tape1.end_epoch,
    })
}

/// General merge function
pub fn merge(tape1: &Tape, tape2: &Tape) -> Result<Tape, TapeError> {
    if tape1.start_epoch == tape2.start_epoch && tape1.end_epoch == tape2.end_epoch {
        merge_by_amount(tape1, tape2)
    } else if tape1.end_epoch == tape2.start_epoch && tape1.storage_capacity == tape2.storage_capacity {
        merge_by_period(tape1, tape2)
    } else {
        Err(TapeError::IncompatibleEpochs)
    }
}


/// A Blob is a unit of data stored in the archive system.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
pub struct Blob {
    /// The authority that owns this blob.
    pub authority: Pubkey,

    /// The size of the blob data in bytes.
    pub size: u64,

    /// The epoch when this blob was registered.
    pub registered_epoch: EpochNumber,

    /// The epoch when this blob was certified.
    pub certified_epoch: EpochNumber,

    /// The tape this blob is stored on.
    pub tape: Pubkey,

    /// The hash of the blob data.
    pub hash: Hash,
}

// Extend struct with account functions

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum AccountType {
    Unknown = 0,
    System,
    Epoch,
    Node,
    Pool,
    Share,
    Archive,
    Spool,
    Tape,
    Blob,
}

state!(AccountType, System);
state!(AccountType, Epoch);
state!(AccountType, Node);
state!(AccountType, Pool);
state!(AccountType, Share);
state!(AccountType, Archive);
state!(AccountType, Spool);
state!(AccountType, Tape);
state!(AccountType, Blob);


pub const PK_SYSTEM:   &[u8] = b"system";
pub const PK_EPOCH:    &[u8] = b"epoch";
pub const PK_NODE:     &[u8] = b"node";
pub const PK_POOL:     &[u8] = b"pool";
pub const PK_SHARE:    &[u8] = b"share";
pub const PK_ARCHIVE:  &[u8] = b"archive";
pub const PK_SPOOL:    &[u8] = b"spool";
pub const PK_TAPE:     &[u8] = b"tape";
pub const PK_BLOB:     &[u8] = b"blob";

pub fn system_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[PK_SYSTEM], &crate::id())
}

pub fn epoch_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[PK_EPOCH], &crate::id())
}

pub fn node_pda(authority: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[PK_NODE, authority.as_ref()], &crate::id())
}

pub fn pool_pda(authority: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[PK_POOL, authority.as_ref()], &crate::id())
}

pub fn share_pda(authority: Pubkey, pool: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[PK_SHARE, authority.as_ref(), pool.as_ref()], &crate::id())
}

pub fn blob_pda(authority: Pubkey, hash: Hash) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[PK_BLOB, authority.as_ref(), hash.as_ref()], &crate::id())
}

pub fn tape_pda(authority: Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[PK_TAPE, authority.as_ref()], &crate::id())
}

pub fn spool_pda(archive: ArchiveNumber, spool: SpoolNumber) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[PK_SPOOL, &archive.as_u64().to_le_bytes(), &spool.as_u64().to_le_bytes()], &crate::id())
}

pub fn archive_pda(archive: ArchiveNumber) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[PK_ARCHIVE, &archive.as_u64().to_le_bytes()], &crate::id())
}


#[cfg(test)]
mod tests {
    use super::*;

    /// A simple in-memory account database for testing purposes.
    struct AccountDB {
        db: std::collections::HashMap<Pubkey, Vec<u8>>
    }

    impl AccountDB {
        fn new() -> Self {
            AccountDB {
                db: std::collections::HashMap::new()
            }
        }

        fn insert(&mut self, key: Pubkey, data: Vec<u8>) {
            self.db.insert(key, data);
        }

        fn get(&self, key: &Pubkey) -> Option<&Vec<u8>> {
            self.db.get(key)
        }

        fn remove(&mut self, key: &Pubkey) {
            self.db.remove(key);
        }

        fn clear(&mut self) {
            self.db.clear();
        }
    }

    impl AccountDB {
        fn get_system_account(&self, address: &Pubkey) -> Option<&System> {
            self.get(address).and_then(|data| System::unpack(data).ok())
        }

        fn set_system_account(&mut self, address: Pubkey, sys: &System) {
            self.insert(address, sys.to_bytes().to_vec());
        }

        fn get_epoch_account(&self, address: &Pubkey) -> Option<&Epoch> {
            self.get(address).and_then(|data| Epoch::unpack(data).ok())
        }

        fn set_epoch_account(&mut self, address: Pubkey, epoch: &Epoch) {
            self.insert(address, epoch.to_bytes().to_vec());
        }

        fn get_node_account(&self, address: &Pubkey) -> Option<&Node> {
            self.get(address).and_then(|data| Node::unpack(data).ok())
        }

        fn set_node_account(&mut self, address: Pubkey, node: &Node) {
            self.insert(address, node.to_bytes().to_vec());
        }

        fn get_pool_account(&self, address: &Pubkey) -> Option<&Pool> {
            self.get(address).and_then(|data| Pool::unpack(data).ok())
        }

        fn set_pool_account(&mut self, address: Pubkey, pool: &Pool) {
            self.insert(address, pool.to_bytes().to_vec());
        }

        fn get_share_account(&self, address: &Pubkey) -> Option<&Share> {
            self.get(address).and_then(|data| Share::unpack(data).ok())
        }

        fn set_share_account(&mut self, address: Pubkey, share: &Share) {
            self.insert(address, share.to_bytes().to_vec());
        }

        fn get_blob_account(&self, address: &Pubkey) -> Option<&Blob> {
            self.get(address).and_then(|data| Blob::unpack(data).ok())
        }

        fn set_blob_account(&mut self, address: Pubkey, blob: &Blob) {
            self.insert(address, blob.to_bytes().to_vec());
        }

        fn get_tape_account(&self, address: &Pubkey) -> Option<&Tape> {
            self.get(address).and_then(|data| Tape::unpack(data).ok())
        }

        fn set_tape_account(&mut self, address: Pubkey, tape: &Tape) {
            self.insert(address, tape.to_bytes().to_vec());
        }

        fn get_spool_account(&self, address: &Pubkey) -> Option<&Spool> {
            self.get(address).and_then(|data| Spool::unpack(data).ok())
        }

        fn set_spool_account(&mut self, address: Pubkey, spool: &Spool) {
            self.insert(address, spool.to_bytes().to_vec());
        }

        fn get_archive_account(&self, address: &Pubkey) -> Option<&Archive> {
            self.get(address).and_then(|data| Archive::unpack(data).ok())
        }

        fn set_archive_account(&mut self, address: Pubkey, archive: &Archive) {
            self.insert(address, archive.to_bytes().to_vec());
        }
    }

    fn setup() -> AccountDB {
        let mut db = AccountDB::new();

        let (sys_key, _sys_bump) = system_pda();
        let (epoch_key, _epoch_bump) = epoch_pda();

        let sys = System {
            version: VersionNumber::new(1),
            committee: [Pubkey::default(); 256],
            active_set: [Pubkey::default(); 256],
            write_price: TAPE::new(0),
            storage_price: TAPE::new(0),
            future_accounting: FutureAccountingRingBuffer {
                index: 0,
                length: 0,
                entries: [FutureAccounting {
                    epoch: EpochNumber::new(0),
                    total_storage_used: 0,
                    rewards: TAPE::new(0),
                }; 256],
            },
        };

        let epoch = Epoch {
            number: EpochNumber::new(0),
            last_epoch_at: 0,
        };

        db.set_system_account(sys_key, &sys);
        db.set_epoch_account(epoch_key, &epoch);

        db
    }

    fn mint_tape(amount: u64) -> Balance<TAPE> {
        TAPE::new(amount)
    }

    #[test]
    fn test_db() {
        let mut db = AccountDB::new();
        let key = Pubkey::new_unique();
        let value = vec![1, 2, 3, 4, 5];

        db.insert(key, value.clone());
        assert_eq!(db.get(&key), Some(&value));

        db.remove(&key);
        assert_eq!(db.get(&key), None);

        db.insert(key, value.clone());
        db.clear();
        assert_eq!(db.get(&key), None);
    }

    #[test]
    fn test_staking_active_set() {
        let db = setup();

        let (epoch_key, _epoch_bump) = epoch_pda();
        let epoch = db.get_epoch_account(&epoch_key).unwrap();

        let (sys_key, _sys_bump) = system_pda();
        let sys = db.get_system_account(&sys_key).unwrap();

        let owner = [
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];

        let (pa_address, _) = pool_pda(owner[0]);
        let (pb_address, _) = pool_pda(owner[1]);
        let (pc_address, _) = pool_pda(owner[2]);

        let pool_a = register_pool(epoch, owner[0], BasisPoints::new(500)).unwrap();
        let pool_b = register_pool(epoch, owner[1], BasisPoints::new(300)).unwrap();
        let pool_c = register_pool(epoch, owner[2], BasisPoints::new(1000)).unwrap();

        let (na_address, _) = node_pda(owner[0]);
        let (nb_address, _) = node_pda(owner[1]);
        let (nc_address, _) = node_pda(owner[2]);

        let name = crate::utils::to_name("node_a");
        let network_address = NetworkAddress::from("127.0.0.1:8080").unwrap();
        let tsl_pubkey = Pubkey::new_unique();
        let storage_capacity = 1_000_000_000; // 1 GB
        let version = VersionNumber::new(1);

        let node_a = register_node(
            epoch,
            &pool_a, 
            owner[0], 
            pa_address, 
            name, 
            network_address,
            tsl_pubkey, 
            storage_capacity,
            version
        );


        /*
        let mut staking = staking_inner::new(0, EPOCH_DURATION, 300, &clock, ctx);

        // register pools in the `StakingInnerV1`.
        let pool_one = test::pool().name(b"pool_1".to_string()).register(&mut staking, ctx);
        let pool_two = test::pool().name(b"pool_2".to_string()).register(&mut staking, ctx);
        let pool_three = test::pool().name(b"pool_3".to_string()).register(&mut staking, ctx);

        // now Alice, Bob, and Carl stake in the pools
        let mut wal_alice = staking.stake_with_pool(test::mint_wal(100000, ctx), pool_one, ctx);
        let wal_alice_2 = staking.stake_with_pool(test::mint_wal(100000, ctx), pool_one, ctx);

        wal_alice.join(wal_alice_2);

        let wal_bob = staking.stake_with_pool(test::mint_wal(200000, ctx), pool_two, ctx);
        let wal_carl = staking.stake_with_pool(test::mint_wal(600000, ctx), pool_three, ctx);

        // expect the active set to be modified
        assert!(staking.active_set().total_stake() == 1000000 * frost_per_wal());
        assert!(staking.active_set().active_ids().length() == 3);
        assert!(staking.active_set().cur_min_stake() == 0);

        // trigger `advance_epoch` to update the committee
        staking.select_committee_and_calculate_votes();
        staking.advance_epoch(vec_map::empty()); // no rewards for E0

        // we expect:
        // - all 3 pools have been advanced
        // - all 3 pools have been added to the committee
        // - shards have been assigned to the pools evenly
        */
    }

}

