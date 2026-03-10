pub mod exchange;
pub mod staking;
pub mod token;
pub mod tapedrive;

// Note: Each program exports `id()`, `ID`, and `PROGRAM_ID` via declare_id!
// Use specific paths like `tapedrive::id()` or `staking::id()` to disambiguate.
#[allow(ambiguous_glob_reexports)]
pub use exchange::*;
#[allow(ambiguous_glob_reexports)]
pub use staking::*;
#[allow(ambiguous_glob_reexports)]
pub use token::*;
#[allow(ambiguous_glob_reexports)]
pub use tapedrive::*;
