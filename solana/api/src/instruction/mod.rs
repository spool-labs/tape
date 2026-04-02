use tape_solana::*;

mod archive;
mod blacklist;
mod epoch;
mod exchange;
mod node;
mod pool;
mod snapshot;
mod util;
pub use snapshot::*;
mod stake;
mod stream;
mod tape;
mod token;
mod track;

pub use archive::*;
pub use blacklist::*;
pub use epoch::*;
pub use exchange::*;
pub use node::*;
pub use pool::*;
pub use stake::*;
pub use util::read_instruction_pod;
pub use stream::*;
pub use tape::*;
pub use token::*;
pub use track::*;

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
    ExpandSystem,
    Initialize,
    AdvanceEpoch,
    //RegisterFeature,
    //CertifyFeature,

    // Pool
    AdvancePool = 0x50,
    StakeWithPool,
    RequestStakeUnlock,
    UnstakeFromPool,
    SplitPoolStake,
    MergePoolStake,

    // Operator
    RegisterNode = 0x60,
    JoinNetwork,
    SyncEpoch,
    SetAuthority,
    SetName,
    SetBlsPubkey,
    SetNetworkAddress,
    SetNetworkTls,
    SetCommissionRate,
    SetStoragePrice,
    SetStorageCapacity,
    ClaimCommission,
    AddToBlacklist,
    RemoveFromBlacklist,
    //VoteOnFeature,
    //VoteOnSlash,

    // Certificate
    CreateBlsCert = 0x80,
    SignBlsCert,
    GroupSignBlsCert,
    DestroyBlsCert,
    //CreateEdwardCert,
    //SignEdwardCert,
    //DestroyEdwardCert,

    // Tape
    ReserveTape = 0x90,
    DestroyTape,
    SplitTapeByEpoch,
    SplitTapeBySize,
    MergeTape,

    // Track
    TrackWrite = 0xA0,
    DeleteTrack,
    CertifyTrack,
    InvalidateTrack,

    // Snapshot
    InitSnapshotEpoch = 0xC0,
    CertifySnapshotGroup,
    FinalizeSnapshotEpoch,

    // Stream
    //CreateStream = 0xB0,
    //RegisterStream,
    //DeleteStream,
    //AppendToStream,
    //UpdateStream,
    //FinalizeStream,
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
tape_solana::instruction!(TapeInstruction, ExpandSystem);
tape_solana::instruction!(TapeInstruction, Initialize);
tape_solana::instruction!(TapeInstruction, AdvanceEpoch);
//instruction!(TapeInstruction, RegisterFeature);
//instruction!(TapeInstruction, CertifyFeature);

tape_solana::instruction!(TapeInstruction, AdvancePool);
tape_solana::instruction!(TapeInstruction, StakeWithPool);
tape_solana::instruction!(TapeInstruction, RequestStakeUnlock);
tape_solana::instruction!(TapeInstruction, UnstakeFromPool);
tape_solana::instruction!(TapeInstruction, SplitPoolStake);
tape_solana::instruction!(TapeInstruction, MergePoolStake);

tape_solana::instruction!(TapeInstruction, RegisterNode);
tape_solana::instruction!(TapeInstruction, JoinNetwork);
tape_solana::instruction!(TapeInstruction, SyncEpoch);
tape_solana::instruction!(TapeInstruction, ClaimCommission);
tape_solana::instruction!(TapeInstruction, SetAuthority);
tape_solana::instruction!(TapeInstruction, SetName);
tape_solana::instruction!(TapeInstruction, SetBlsPubkey);
tape_solana::instruction!(TapeInstruction, SetNetworkAddress);
tape_solana::instruction!(TapeInstruction, SetNetworkTls);
tape_solana::instruction!(TapeInstruction, SetCommissionRate);
tape_solana::instruction!(TapeInstruction, SetStoragePrice);
tape_solana::instruction!(TapeInstruction, SetStorageCapacity);
tape_solana::instruction!(TapeInstruction, AddToBlacklist);
tape_solana::instruction!(TapeInstruction, RemoveFromBlacklist);
//instruction!(TapeInstruction, VoteOnFeature);

tape_solana::instruction!(TapeInstruction, ReserveTape);
tape_solana::instruction!(TapeInstruction, DestroyTape);
tape_solana::instruction!(TapeInstruction, SplitTapeByEpoch);
tape_solana::instruction!(TapeInstruction, SplitTapeBySize);
tape_solana::instruction!(TapeInstruction, MergeTape);

tape_solana::instruction!(TapeInstruction, TrackWrite);
tape_solana::instruction!(TapeInstruction, DeleteTrack);
tape_solana::instruction!(TapeInstruction, CertifyTrack);
tape_solana::instruction!(TapeInstruction, InvalidateTrack);

tape_solana::instruction!(TapeInstruction, InitSnapshotEpoch);
tape_solana::instruction!(TapeInstruction, CertifySnapshotGroup);
tape_solana::instruction!(TapeInstruction, FinalizeSnapshotEpoch);

//instruction!(TapeInstruction, CreateStream);
//instruction!(TapeInstruction, RegisterStream);
//instruction!(TapeInstruction, DeleteStream);
//instruction!(TapeInstruction, AppendToStream);
//instruction!(TapeInstruction, UpdateStream);
//instruction!(TapeInstruction, FinalizeStream);
