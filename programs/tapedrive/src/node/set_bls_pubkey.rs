use tape_api::prelude::*;
use steel::*;

pub fn process_set_bls_pubkey(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetBlsPubkey::try_from_bytes(data)?;
    let [
        signer_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != *signer_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let bls_pubkey = args.bls_pubkey;
    let bls_signature = args.bls_pop;
    if !bls_pubkey.is_valid(bls_signature) {
        return Err(ProgramError::Custom(1));
        //return Err(TapeError::InvalidBlsProofOfPossession);
    }

    node.metadata.next_bls_pubkey = args.bls_pubkey;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_set_network_tls() {
        let signer = Pubkey::new_unique();
        let old_bls_pubkey = BlsPubkey::new_unique();

        let new_secret = BlsPrivateKey::from_random();
        let new_bls_pubkey = new_secret.public_key().expect("pubkey");
        let new_bls_pop = new_secret.proof_of_possession().expect("pop");

        let (node_address, _) = node_pda(signer);

        let instruction = build_set_bls_pubkey_ix(signer, node_address, new_bls_pubkey, new_bls_pop);

        let node = Node {
            authority: signer,
            metadata: NodeMetadata {
                bls_pubkey: old_bls_pubkey,
                next_bls_pubkey: old_bls_pubkey,
                ..NodeMetadata::zeroed()
            },
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(signer, 1_000_000_000),
            pda(node_address, node.pack(), tapedrive::ID),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&node_address)
                    .data(Node {
                        metadata: NodeMetadata {
                            next_bls_pubkey: new_bls_pubkey,
                            ..node.metadata
                        },
                        ..node
                    }.pack().as_ref())
                    .build(),
            ],
        );
    }
}
