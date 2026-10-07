use solana_system_interface::instruction as system_instruction;
use tape_api::instruction::build_close_tape_ix;
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_rpc::Rpc;

use crate::balance::sol_balance_of;
use crate::error::TapedriveError;
use crate::keys::tape_key::TapeKey;
use crate::tapedrive::Tapedrive;

impl<Blockchain: Rpc, Cluster: Api> Tapedrive<Blockchain, Cluster> {
    /// Close a tape. Every track on it is gone and its tape number is never
    /// given out again. An immutable tape can be closed only after it expires.
    ///
    /// The tape account's rent comes back to the payer.
    pub async fn close_tape(&self, tape_key: &TapeKey) -> Result<(), TapedriveError> {
        let payer = self.payer()?;
        let tape_signer = tape_key.keypair();
        let tape_address = tape_key.address();
        let wallet: Address = payer.pubkey().into();
        let authority: Address = tape_key.pubkey().into();

        // The program returns the rent to the tape key, and the payer paid it
        // at reserve, so the tape key sends it on.
        let rent = sol_balance_of(self.rpc(), &tape_address).await?;

        let ixs = vec![
            build_close_tape_ix(wallet, authority, tape_address),
            system_instruction::transfer(&authority.into(), &wallet.into(), rent.lamports()),
        ];

        self.rpc()
            .send_instructions_with_signers(payer, ixs, &[tape_signer])
            .await?;

        Ok(())
    }
}
