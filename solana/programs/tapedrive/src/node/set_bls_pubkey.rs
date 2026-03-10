use tape_solana::*;
use tape_api::prelude::*;
use crate::error::*;

pub fn process_set_bls_pubkey(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetBlsPubkey::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        node_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    fee_payer_info
        .is_signer()?
        .is_writable()?;
    authority_info
        .is_signer()?;

    let node = node_info
        .is_writable()?
        .as_account_mut::<Node>(&tapedrive::ID)?;

    if node.authority != *authority_info.key {
        return Err(ProgramError::InvalidAccountData);
    }

    let bls_pubkey = args.bls_pubkey;
    let bls_signature = args.bls_pop;
    if !bls_pubkey.is_valid(bls_signature) {
        return Err(TapeError::BadBlsProof.into());
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
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let old_bls_pubkey = BlsPubkey::new_unique();

        let new_secret = BlsPrivateKey::from_random();
        let new_bls_pubkey = new_secret.public_key().expect("pubkey");
        let new_bls_pop = new_secret.proof_of_possession().expect("pop");

        let (node_address, _) = node_pda(authority);

        let instruction = build_set_bls_pubkey_ix(fee_payer, authority, node_address, new_bls_pubkey, new_bls_pop);

        let node = Node {
            authority,
            metadata: NodeMetadata {
                bls_pubkey: old_bls_pubkey,
                next_bls_pubkey: old_bls_pubkey,
                ..NodeMetadata::zeroed()
            },
            ..Node::zeroed()
        };

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
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
