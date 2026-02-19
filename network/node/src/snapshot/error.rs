use tape_api::errors::is_account_state_pending_error;
use rpc_client::RpcError;

use crate::tasks::parse_tape_error;

pub enum SubmitClass {
    Done,
    Pending,
    Retryable,
}

pub fn classify_submit_error(err: &RpcError) -> SubmitClass {
    if parse_tape_error(err)
        .map(|error| error.is_already_done())
        .unwrap_or(false)
    {
        return SubmitClass::Done;
    }

    if is_account_state_pending_error(&err.to_string()) {
        return SubmitClass::Pending;
    }

    SubmitClass::Retryable
}
