use steel::*;

mod archive;
//mod blob;
//mod committee;
mod epoch;
//mod exchange;
//mod feature;
mod node;
//mod stake;
mod system;
//mod tape;
mod token;

pub use archive::*;
//pub use blob::*;
//pub use committee::*;
pub use epoch::*;
//pub use exchange::*;
//pub use feature::*;
pub use node::*;
//pub use stake::*;
pub use system::*;
//pub use tape::*;
pub use token::*;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum TokenInstruction {
    Unknown = 0,

    InitializeMint,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum ExchangeInstruction {
    Unknown = 10,

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
    Unknown = 30,

    StakeWithNode,
    UnstakeFromNode,
    ClaimStake,
    SplitStake,
    MergeStake,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum TapeInstruction {
    Unknown = 20,

    // System
    Initialize,

    CreateSystem,
    ExpandSystem,

    CreateEpoch,
    AdvanceEpoch,
    CreateArchive,

    RegisterFeature,
    CertifyFeature,

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

instruction!(TapeInstruction, CreateSystem);
instruction!(TapeInstruction, ExpandSystem);
instruction!(TapeInstruction, Initialize);

instruction!(TapeInstruction, CreateEpoch);
instruction!(TapeInstruction, AdvanceEpoch);

instruction!(TapeInstruction, CreateArchive);

//instruction!(ExchangeInstruction, RegisterExchange);
//instruction!(ExchangeInstruction, SetExchangeRate);
//instruction!(ExchangeInstruction, DepositTape);
//instruction!(ExchangeInstruction, DepositSol);
//instruction!(ExchangeInstruction, WithdrawTape);
//instruction!(ExchangeInstruction, WithdrawSol);
//instruction!(ExchangeInstruction, SwapForTape);
//instruction!(ExchangeInstruction, SwapForSol);
//
//instruction!(StakeInstruction, StakeWithNode);
//instruction!(StakeInstruction, UnstakeFromNode);
//instruction!(StakeInstruction, ClaimStake);
//instruction!(StakeInstruction, SplitStake);
//instruction!(StakeInstruction, MergeStake);
//
//instruction!(TapeInstruction, Initialize);
//instruction!(TapeInstruction, CreateSystem);
//instruction!(TapeInstruction, ExpandSystem);
//instruction!(TapeInstruction, CreateEpoch);
//instruction!(TapeInstruction, AdvanceEpoch);
//instruction!(TapeInstruction, RegisterFeature);
//instruction!(TapeInstruction, CertifyFeature);
//
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
//
