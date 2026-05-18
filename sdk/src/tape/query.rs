use rpc::Rpc;
use tape_api::state::Tape;
use tape_crypto::address::Address;
use tape_protocol::Api;

use crate::error::TapedriveError;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Fetch a tape's on-chain state.
    pub async fn get_tape(&self, tape: &Address) -> Result<Tape, TapedriveError> {
        self.rpc()
            .get_tape_by_address(tape)
            .await
            .map_err(TapedriveError::Rpc)
    }
}
