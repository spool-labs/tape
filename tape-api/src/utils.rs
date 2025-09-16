use steel::*;

/// Helper: check a condition is true and return an error if not
#[inline(always)]
pub fn check_condition<E>(condition: bool, err: E) -> ProgramResult
where
    E: Into<ProgramError>,
{
    if !condition {
        return Err(err.into());
    }
    Ok(())
}

