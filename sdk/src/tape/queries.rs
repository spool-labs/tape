use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use tape_api::state::Tape;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Fetch a tape's on-chain state.
    pub async fn get_tape(&self, tape: &Pubkey) -> Result<Tape, TapedriveError> {
        self.rpc()
            .get_tape_by_address(tape)
            .await
            .map_err(TapedriveError::Rpc)
    }
}
