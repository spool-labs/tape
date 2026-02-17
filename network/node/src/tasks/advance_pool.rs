//! AdvancePool — submit advance_pool instruction on-chain.

use std::sync::Arc;

use rpc::Rpc;
use solana_sdk::instruction::Instruction;
use solana_sdk::signer::Signer;
use store::Store;
use tape_api::instruction::build_advance_pool_ix;
use tape_api::program::tapedrive::node_pda;
use tape_api::errors::TapeError;
use tokio_util::sync::CancellationToken;

use crate::core::NodeContext;
use crate::supervisor::TaskOutcome;
use crate::tasks::parse_tape_error;

fn build_ix(pubkey: solana_sdk::pubkey::Pubkey) -> Instruction {
    let (node_address, _) = node_pda(pubkey);
    build_advance_pool_ix(pubkey, pubkey, node_address)
}

pub async fn run<S: Store, R: Rpc>(
    context: Arc<NodeContext<S, R>>,
    cancel: CancellationToken,
) -> TaskOutcome {
    let pubkey = context.keypair.pubkey();
    let ix = build_ix(pubkey);

    let result = tokio::select! {
        r = context.rpc.send_instructions(&context.keypair, vec![ix]) => r,
        _ = cancel.cancelled() => return TaskOutcome::Success,
    };
    match result {
        Ok(sig) => {
            tracing::info!(%sig, "advance_pool submitted");
            TaskOutcome::Success
        }
        Err(ref e) => match parse_tape_error(e) {
            Some(TapeError::AlreadyAdvanced) => {
                tracing::info!("advance_pool already completed");
                TaskOutcome::Success
            }
            _ => TaskOutcome::Retryable(format!("advance_pool: {e}")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::build_ix;
    use tape_api::program::tapedrive::node_pda;
    use solana_sdk::pubkey::Pubkey;

    #[test]
    fn uses_node() {
        let authority = Pubkey::new_unique();
        let (node_address, _) = node_pda(authority);
        let ix = build_ix(authority);

        // Accounts: fee_payer, authority, system, archive, epoch, pool(node), history
        assert_eq!(ix.accounts[5].pubkey, node_address);
    }
}
