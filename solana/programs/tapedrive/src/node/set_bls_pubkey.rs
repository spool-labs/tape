use tape_solana::*;
use tape_api::program::prelude::*;

pub fn process_set_bls_pubkey(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = SetBlsPubkey::try_from_bytes(data)?;
    let [
        fee_payer_info,
        authority_info,
        node_info,
        peer_set_info,
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

    if node.authority != (*authority_info.key).into() {
        return Err(ProgramError::InvalidAccountData);
    }

    let bls_pubkey = args.bls_pubkey;
    if !bls_pubkey.is_valid(args.bls_pop) {
        return Err(TapeError::BadBlsProof.into());
    }

    node.metadata.bls_pubkey = bls_pubkey;

    peer_set_info
        .is_writable()?
        .is_peer_set()?;

    let (peer_set, peers) = PeerSet::read_mut(peer_set_info, &tapedrive::ID)?;
    let node_address: Address = (*node_info.key).into();
    let count = peer_set.peers.count as usize;
    if let Some(idx) = peers[..count].iter().position(|p| p.node == node_address) {
        peers[idx].bls_pubkey = bls_pubkey;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn set_bls() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let old_bls_pubkey = BlsPubkey::new_unique();

        let new_secret = BlsPrivateKey::from_random();
        let new_bls_pubkey = new_secret.public_key().expect("pubkey");
        let new_bls_pop = new_secret.proof_of_possession().expect("pop");

        let (node_address, _) = node_pda(authority.into());
        let (peer_set_address, _) = peer_set_pda();

        let instruction = build_set_bls_pubkey_ix(
            fee_payer.into(),
            authority.into(),
            node_address,
            new_bls_pubkey,
            new_bls_pop,
        );

        let node = Node {
            authority: authority.into(),
            metadata: NodeMetadata {
                bls_pubkey: old_bls_pubkey,
                ..NodeMetadata::zeroed()
            },
            ..Node::zeroed()
        };

        // Peer entry for this node — should get its bls_pubkey updated.
        let peer = Peer {
            node: node_address,
            bls_pubkey: old_bls_pubkey,
            ..Peer::zeroed()
        };
        let peer_set_data = PeerSet { peers: Tail::new(8, 1) }.pack_with(&[peer]);

        let accounts = vec![
            sol(fee_payer, 1_000_000_000),
            sol(authority, 0),
            pda(node_address, node.pack(), tapedrive::ID),
            pda(peer_set_address, peer_set_data, tapedrive::ID),
        ];

        let expected_peer = Peer { bls_pubkey: new_bls_pubkey, ..peer };
        let expected_peer_set =
            PeerSet { peers: Tail::new(8, 1) }.pack_with(&[expected_peer]);

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&Pubkey::from(node_address)).data(
                    Node {
                        metadata: NodeMetadata {
                            bls_pubkey: new_bls_pubkey,
                            ..node.metadata
                        },
                        ..node
                    }.pack().as_ref()
                ).build(),
                Check::account(&Pubkey::from(peer_set_address))
                    .data(expected_peer_set.as_ref())
                    .build(),
            ],
        );
    }
}
