use anyhow::Error;
use tape_api::errors::{ProgramError, TapeError};

pub fn prog_err(err: &Error) -> Option<ProgramError> {
    for cause in err.chain() {
        if let Some(code) = ProgramError::from_error_string(&cause.to_string()) {
            return Some(code);
        }
    }
    None
}

pub fn adv_done(err: &Error) -> bool {
    matches!(prog_err(err), Some(ProgramError::Tape(TapeError::AlreadyAdvanced)))
}

pub fn join_done(err: &Error) -> bool {
    matches!(prog_err(err), Some(ProgramError::Tape(TapeError::UnexpectedState)))
}

pub fn sync_done(err: &Error) -> bool {
    matches!(prog_err(err), Some(ProgramError::Tape(TapeError::AlreadySynced)))
}
