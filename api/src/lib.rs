pub mod consts;
pub mod cpi;
pub mod error;
pub mod event;
pub mod instruction;
pub mod loaders;
pub mod pda;
pub mod state;
pub mod utils;
mod macros;

pub mod prelude {
    pub use tape_core::prelude::*;

    pub use crate::consts::*;
    pub use crate::cpi::*;
    pub use crate::error::*;
    pub use crate::event::*;
    pub use crate::instruction::*;
    pub use crate::loaders::*;
    pub use crate::pda::*;
    pub use crate::state::*;
    pub use crate::utils::*;
}

use steel::*;

declare_id!("tape9hFAE7jstfKB2QT1ovFNUZKKtDUyGZiGQpnBFdL"); 
