use tape_api::program::prelude::*;

pub fn resolve_rate(
    node: &Node,
    history_tape: &Tape,
    history_address: Address,
    node_address: Address,
    target_epoch: EpochNumber,
    pool_rate: PoolRate,
) -> Result<ExchangeRate, ProgramError> {
    if !history_tape.is_history_tape(node.id) {
        return Err(ProgramError::InvalidAccountData);
    }

    let span = pool_rate.span;
    let track = pool_rate.track;
    if span.node != node_address {
        return Err(ProgramError::InvalidInstructionData);
    }
    span.check_contains(target_epoch)
        .map_err(|_| TapeError::RateMissing)?;

    if track.state.tape != history_address
        || track.state.key != span.key()
        || track.state.size != StorageUnits::from_bytes(core::mem::size_of::<RateSpan>() as u64)
        || track.state.value_hash != span.value_hash()
        || !track.state.is_raw()
        || !track.state.is_certified()
    {
        return Err(ProgramError::InvalidInstructionData);
    }

    history_tape
        .tracks
        .verify(&track)
        .map_err(|_| TapeError::BadProof)?;

    Ok(span.rate)
}
