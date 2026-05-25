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

    if pool_rate.is_current() {
        // The current rate is valid only for the node's open span. If an
        // AdvancePool raced the transaction and closed that span, the target
        // epoch is now historical and must be supplied with a closed span.
        if target_epoch < node.rate_span_start {
            return Err(TapeError::RateMissing.into());
        }
        return Ok(node.pool.get_current_rate());
    }

    if !pool_rate.is_closed_span() {
        return Err(ProgramError::InvalidInstructionData);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_rate_rejects_closed_epoch() {
        let node = Node {
            rate_span_start: EpochNumber(41),
            ..Node::zeroed()
        };
        let tape = Tape::history(node.id, EpochNumber(0));

        let err = resolve_rate(
            &node,
            &tape,
            Address::default(),
            Address::default(),
            EpochNumber(40),
            PoolRate::current(),
        )
        .unwrap_err();

        assert_eq!(err, TapeError::RateMissing.into());
    }
}
