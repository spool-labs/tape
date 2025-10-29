use steel::*;

//mod blob;
//mod committee;
//mod feature;
//mod tape;
mod archive;
mod epoch;
mod exchange;
mod node;
mod pool;
mod stake;
mod system;
mod token;

//pub use blob::*;
//pub use committee::*;
//pub use feature::*;
//pub use tape::*;
pub use archive::*;
pub use epoch::*;
pub use exchange::*;
pub use node::*;
pub use pool::*;
pub use stake::*;
pub use system::*;
pub use token::*;

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
    SyncEpoch,

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
    SetAuthority,
    SetNetworkAddress,
    SetNetworkTls,
    SetName,
    SetCommissionRate,
    ClaimCommission,
    AddToBlacklist,
    RemoveFromBlacklist,
    VoteOnStoragePrice,
    VoteOnShardSize,
    VoteOnFeature,

    // Storage
    ReserveTape,
    BurnTape,
    SplitTapeByDuration,
    SplitTapeBySize,
    MergeTape,

    // Blob
    RegisterBlob,
    DeleteBlob,
    CertifyBlob,
    InvalidateBlob,
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
instruction!(TapeInstruction, SyncEpoch);
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
//instruction!(TapeInstruction, SetAuthority);
//instruction!(TapeInstruction, SetNetworkAddress);
//instruction!(TapeInstruction, SetNetworkTls);
//instruction!(TapeInstruction, SetName);
//instruction!(TapeInstruction, SetCommissionRate);
//instruction!(TapeInstruction, ClaimCommission);
//instruction!(TapeInstruction, AddToBlacklist);
//instruction!(TapeInstruction, RemoveFromBlacklist);
//instruction!(TapeInstruction, VoteOnStoragePrice);
//instruction!(TapeInstruction, VoteOnShardSize);
//instruction!(TapeInstruction, VoteOnFeature);
//
//instruction!(TapeInstruction, ReserveTape);
//instruction!(TapeInstruction, BurnTape);
//instruction!(TapeInstruction, SplitTapeByDuration);
//instruction!(TapeInstruction, SplitTapeBySize);
//instruction!(TapeInstruction, MergeTape);
//
//instruction!(TapeInstruction, RegisterBlob);
//instruction!(TapeInstruction, DeleteBlob);
//instruction!(TapeInstruction, CertifyBlob);
//instruction!(TapeInstruction, InvalidateBlob);
