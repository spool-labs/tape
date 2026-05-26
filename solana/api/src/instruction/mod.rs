use tape_solana::*;

mod system;
mod blacklist;
mod committee;
mod epoch;
mod exchange;
mod node;
mod peer;
mod pool;
mod stake;
mod tape;
mod token;
mod track;
mod vote;

pub use system::*;
pub use blacklist::*;
pub use committee::*;
pub use epoch::*;
pub use exchange::*;
pub use node::*;
pub use peer::*;
pub use pool::*;
pub use stake::*;
pub use tape::*;
pub use token::*;
pub use track::*;
pub use vote::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum TokenInstruction {
    Unknown = 0x0,

    InitializeMint,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum ExchangeInstruction {
    Unknown = 0x20,

    RegisterExchange,
    SetExchangeRate,
    DepositTape,
    DepositSol,
    WithdrawTape,
    WithdrawSol,
    SwapForTape,
    SwapForSol,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum StakingInstruction {
    Unknown = 0x30,

    StakeTokens,
    UnstakeTokens,
    SplitStake,
    MergeStake,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum TapeInstruction {
    Unknown = 0x40,

    // System
    CreateSystem,
    CreateArchive,
    CreateCommittee,
    CreateEpoch,
    CreatePeerSet,
    ResizeCommittee,
    ResizePeerSet,
    StageGenesisNode,
    StartNetwork,

    // Epoch
    SyncSpool = 0x50,
    CommitEpoch,
    AdvanceEpoch,

    // Operator
    RegisterNode = 0x60,
    JoinCommittee,
    SetAuthority,
    SetName,
    SetBlsPubkey,
    SetNetworkAddress,
    SetNetworkTls,
    SetCommissionRate,
    SetStoragePrice,
    SetBurnFeeBps,
    SetSubsidyDecayBps,
    SetStorageCapacity,
    SetCommitteeSize,
    SetSpoolGroups,
    SetMinVersion,
    ClaimCommission,

    // Blacklist
    AddToBlacklist,
    RemoveFromBlacklist,

    // Pool
    AdvancePool = 0x90,
    StakeWithPool,
    RequestStakeUnlock,
    UnstakeFromPool,
    SplitPoolStake,
    MergePoolStake,

    // Tape
    ReserveTape = 0xA0,
    DestroyTape,
    SplitTapeByEpoch,
    SplitTapeBySize,
    MergeTape,

    // Track
    TrackWrite = 0xB0,
    DeleteTrack,
    CertifyTrack,
    InvalidateTrack,

    // Vote
    ProposeSnapshot = 0xC0,
    VoteSnapshot,
    FinalizeSnapshot,
    ProposeAssignment,
    VoteAssignment,
    FinalizeGroup,
}


tape_solana::instruction!(TokenInstruction, InitializeMint);

tape_solana::instruction!(ExchangeInstruction, RegisterExchange);
tape_solana::instruction!(ExchangeInstruction, SetExchangeRate);
tape_solana::instruction!(ExchangeInstruction, DepositTape);
tape_solana::instruction!(ExchangeInstruction, DepositSol);
tape_solana::instruction!(ExchangeInstruction, WithdrawTape);
tape_solana::instruction!(ExchangeInstruction, WithdrawSol);
tape_solana::instruction!(ExchangeInstruction, SwapForTape);
tape_solana::instruction!(ExchangeInstruction, SwapForSol);

tape_solana::instruction!(StakingInstruction, StakeTokens);
tape_solana::instruction!(StakingInstruction, UnstakeTokens);
tape_solana::instruction!(StakingInstruction, SplitStake);
tape_solana::instruction!(StakingInstruction, MergeStake);

tape_solana::instruction!(TapeInstruction, CreateSystem);
tape_solana::instruction!(TapeInstruction, CreateArchive);
tape_solana::instruction!(TapeInstruction, CreateCommittee);
tape_solana::instruction!(TapeInstruction, CreateEpoch);
tape_solana::instruction!(TapeInstruction, CreatePeerSet);
tape_solana::instruction!(TapeInstruction, ResizeCommittee);
tape_solana::instruction!(TapeInstruction, ResizePeerSet);
tape_solana::instruction!(TapeInstruction, StageGenesisNode);
tape_solana::instruction!(TapeInstruction, StartNetwork);

tape_solana::instruction!(TapeInstruction, SyncSpool);
tape_solana::instruction!(TapeInstruction, CommitEpoch);
tape_solana::instruction!(TapeInstruction, AdvanceEpoch);

tape_solana::instruction!(TapeInstruction, AdvancePool);
tape_solana::instruction!(TapeInstruction, StakeWithPool);
tape_solana::instruction!(TapeInstruction, RequestStakeUnlock);
tape_solana::instruction!(TapeInstruction, UnstakeFromPool);
tape_solana::instruction!(TapeInstruction, SplitPoolStake);
tape_solana::instruction!(TapeInstruction, MergePoolStake);

tape_solana::instruction!(TapeInstruction, RegisterNode);
tape_solana::instruction!(TapeInstruction, JoinCommittee);
tape_solana::instruction!(TapeInstruction, ClaimCommission);
tape_solana::instruction!(TapeInstruction, SetAuthority);
tape_solana::instruction!(TapeInstruction, SetName);
tape_solana::instruction!(TapeInstruction, SetBlsPubkey);
tape_solana::instruction!(TapeInstruction, SetNetworkAddress);
tape_solana::instruction!(TapeInstruction, SetNetworkTls);
tape_solana::instruction!(TapeInstruction, SetCommissionRate);
tape_solana::instruction!(TapeInstruction, SetStoragePrice);
tape_solana::instruction!(TapeInstruction, SetBurnFeeBps);
tape_solana::instruction!(TapeInstruction, SetSubsidyDecayBps);
tape_solana::instruction!(TapeInstruction, SetStorageCapacity);
tape_solana::instruction!(TapeInstruction, SetCommitteeSize);
tape_solana::instruction!(TapeInstruction, SetSpoolGroups);
tape_solana::instruction!(TapeInstruction, SetMinVersion);

tape_solana::instruction!(TapeInstruction, AddToBlacklist);
tape_solana::instruction!(TapeInstruction, RemoveFromBlacklist);

tape_solana::instruction!(TapeInstruction, ReserveTape);
tape_solana::instruction!(TapeInstruction, DestroyTape);
tape_solana::instruction!(TapeInstruction, SplitTapeByEpoch);
tape_solana::instruction!(TapeInstruction, SplitTapeBySize);
tape_solana::instruction!(TapeInstruction, MergeTape);

tape_solana::instruction!(TapeInstruction, TrackWrite);
tape_solana::instruction!(TapeInstruction, DeleteTrack);
tape_solana::instruction!(TapeInstruction, CertifyTrack);
tape_solana::instruction!(TapeInstruction, InvalidateTrack);

tape_solana::instruction!(TapeInstruction, ProposeSnapshot);
tape_solana::instruction!(TapeInstruction, VoteSnapshot);
tape_solana::instruction!(TapeInstruction, FinalizeSnapshot);
tape_solana::instruction!(TapeInstruction, ProposeAssignment);
tape_solana::instruction!(TapeInstruction, VoteAssignment);
tape_solana::instruction!(TapeInstruction, FinalizeGroup);
