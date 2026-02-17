use anyhow::Error;
use tape_api::errors::{ProgramError, TapeError};

fn program_error(error: &Error) -> Option<ProgramError> {
    for cause in error.chain() {
        if let Some(code) = ProgramError::from_error_string(&cause.to_string()) {
            return Some(code);
        }
    }
    None
}

/// True when join operation is already completed/idempotent.
pub fn join_done(error: &Error) -> bool {
    matches!(
        program_error(error),
        Some(ProgramError::Tape(TapeError::UnexpectedState))
    )
}

/// True when pool advancement was already processed for current epoch.
pub fn adv_done(error: &Error) -> bool {
    matches!(
        program_error(error),
        Some(ProgramError::Tape(TapeError::AlreadyAdvanced))
    )
}

/// True when sync operation is already done/idempotent.
pub fn sync_done(error: &Error) -> bool {
    matches!(
        program_error(error),
        Some(ProgramError::Tape(TapeError::AlreadySynced))
    )
}
