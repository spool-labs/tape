pub mod consts;
pub mod cpi;
pub mod error;
pub mod event;
pub mod program;
//pub mod instruction;
pub mod loaders;
//pub mod state;
pub mod utils;
mod macros;

pub mod prelude {
    pub use tape_core::prelude::*;

    pub use crate::program::*;
    pub use crate::consts::*;
    pub use crate::cpi::*;
    pub use crate::error::*;
//    pub use crate::event::*;
//    pub use crate::instruction::*;
    pub use crate::loaders::*;
//    pub use crate::state::*;
    pub use crate::utils::*;
}
