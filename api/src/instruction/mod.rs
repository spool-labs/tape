use steel::*;

mod archive;
mod epoch;
mod exchange;
mod node;
mod pool;
mod stake;
mod stream;
mod system;
mod tape;
mod token;
mod track;

pub use archive::*;
pub use epoch::*;
pub use exchange::*;
pub use node::*;
pub use pool::*;
pub use stake::*;
pub use stream::*;
pub use system::*;
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
    Initialize,

    CreateSystem,
    ExpandSystem,
    CreateArchive,

    CreateEpoch,
    AdvanceEpoch,

    RegisterFeature,
    CertifyFeature,

    // Pool
    AdvancePool,
    StakeWithPool,
    RequestStakeUnlock,
    UnstakeFromPool,
    SplitPoolStake,
    MergePoolStake,

    // Operator
    RegisterNode,
    JoinNetwork,
    SyncEpoch,
    SetAuthority,
    SetName,
    SetBlsPubkey,
    SetNetworkAddress,
    SetNetworkTls,
    SetCommissionRate,
    ClaimCommission,
    AddToBlacklist,
    RemoveFromBlacklist,
    VoteOnStoragePrice,
    VoteOnShardSize,
    VoteOnFeature,

    // Tape
    ReserveTape,
    DestroyTape,
    SplitTapeByEpoch,
    SplitTapeBySize,
    MergeTape,

    // Track
    RegisterTrack,
    DeleteTrack,
    CertifyTrack,
    InvalidateTrack,

    // Stream
    CreateStream,
    RegisterStream,
    DeleteStream,
    AppendToStream,
    UpdateStream,
    FinalizeStream,
}


instruction!(TokenInstruction, InitializeMint);

instruction!(ExchangeInstruction, RegisterExchange);
instruction!(ExchangeInstruction, SetExchangeRate);
instruction!(ExchangeInstruction, DepositTape);
instruction!(ExchangeInstruction, DepositSol);
instruction!(ExchangeInstruction, WithdrawTape);
instruction!(ExchangeInstruction, WithdrawSol);
instruction!(ExchangeInstruction, SwapForTape);
instruction!(ExchangeInstruction, SwapForSol);

instruction!(StakingInstruction, StakeTokens);
instruction!(StakingInstruction, UnstakeTokens);
instruction!(StakingInstruction, SplitStake);
instruction!(StakingInstruction, MergeStake);

instruction!(TapeInstruction, Initialize);
instruction!(TapeInstruction, CreateSystem);
instruction!(TapeInstruction, ExpandSystem);
instruction!(TapeInstruction, CreateArchive);
instruction!(TapeInstruction, CreateEpoch);
instruction!(TapeInstruction, AdvanceEpoch);
//instruction!(TapeInstruction, RegisterFeature);
//instruction!(TapeInstruction, CertifyFeature);

instruction!(TapeInstruction, AdvancePool);
instruction!(TapeInstruction, StakeWithPool);
instruction!(TapeInstruction, RequestStakeUnlock);
instruction!(TapeInstruction, UnstakeFromPool);
instruction!(TapeInstruction, SplitPoolStake);
instruction!(TapeInstruction, MergePoolStake);

instruction!(TapeInstruction, RegisterNode);
instruction!(TapeInstruction, JoinNetwork);
instruction!(TapeInstruction, SyncEpoch);
instruction!(TapeInstruction, SetAuthority);
instruction!(TapeInstruction, SetName);
instruction!(TapeInstruction, SetBlsPubkey);
instruction!(TapeInstruction, SetNetworkAddress);
instruction!(TapeInstruction, SetNetworkTls);
instruction!(TapeInstruction, SetCommissionRate);
//instruction!(TapeInstruction, ClaimCommission);
//instruction!(TapeInstruction, AddToBlacklist);
//instruction!(TapeInstruction, RemoveFromBlacklist);
//instruction!(TapeInstruction, VoteOnStoragePrice);
//instruction!(TapeInstruction, VoteOnShardSize);
//instruction!(TapeInstruction, VoteOnFeature);

instruction!(TapeInstruction, ReserveTape);
instruction!(TapeInstruction, DestroyTape);
instruction!(TapeInstruction, SplitTapeByEpoch);
instruction!(TapeInstruction, SplitTapeBySize);
instruction!(TapeInstruction, MergeTape);

instruction!(TapeInstruction, RegisterTrack);
instruction!(TapeInstruction, DeleteTrack);
instruction!(TapeInstruction, CertifyTrack);
instruction!(TapeInstruction, InvalidateTrack);

//instruction!(TapeInstruction, CreateStream);
//instruction!(TapeInstruction, RegisterStream);
//instruction!(TapeInstruction, DeleteStream);
//instruction!(TapeInstruction, AppendToStream);
//instruction!(TapeInstruction, UpdateStream);
//instruction!(TapeInstruction, FinalizeStream);
