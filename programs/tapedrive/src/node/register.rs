use crate::error::*;
use tape_api::prelude::*;
use steel::*;

pub fn process_register_node(accounts: &[AccountInfo<'_>], data: &[u8]) -> ProgramResult {
    let args = RegisterNode::try_from_bytes(data)?;
    let [
        signer_info,

        system_info,
        archive_info,
        epoch_info,
        node_info,
        history_info,

        system_program_info, 
        rent_sysvar_info,
    ] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    signer_info
        .is_signer()?;

    let (node_address, _) = node_pda(*signer_info.key);
    let (history_address, _) = history_pda(node_address);

    node_info
        .is_empty()?
        .is_writable()?
        .has_address(&node_address)?;

    history_info
        .is_empty()?
        .is_writable()?
        .has_address(&history_address)?;

    let system = system_info
        .is_writable()?
        .is_system()?
        .as_account_mut::<System>(&tapedrive::ID)?;

    let archive = archive_info
        .is_archive()?
        .as_account::<Archive>(&tapedrive::ID)?;

    let epoch = epoch_info
        .is_epoch()?
        .as_account::<Epoch>(&tapedrive::ID)?;

    system_program_info
        .is_program(&system_program::ID)?;
    rent_sysvar_info
        .is_sysvar(&sysvar::rent::ID)?;

    let bls_pubkey = args.bls_pubkey;
    let bls_signature = args.bls_pop;
    if !bls_pubkey.is_valid(bls_signature) {
        return Err(ProgramError::Custom(1));
        //return Err(TapeError::InvalidBlsProofOfPossession);
    }

    create_program_account::<Node>(
        node_info,
        system_program_info,
        signer_info,
        &tapedrive::ID,
        &[NODE, signer_info.key.as_ref()],
    )?;

    let node_number = system.total_nodes;
    system.total_nodes = system.total_nodes
        .checked_add(1)
        .ok_or(TapeError::Overflow)?;

    let commission_rate = BasisPoints::unpack(args.commission_rate);

    let node = node_info.as_account_mut::<Node>(&tapedrive::ID)?;

    node.id                   = node_number.into();
    node.authority            = *signer_info.key;
    node.registered_epoch     = current_epoch(epoch);
    node.latest_epoch         = current_epoch(epoch);

    node.blacklist = Blacklist::new();
    node.pool = StakingPool::new(commission_rate);

    node.metadata = NodeMetadata {
        name: args.name,
        network_address: args.network_address,
        network_tls: args.network_tls,
        bls_pubkey: args.bls_pubkey,
        next_bls_pubkey: args.bls_pubkey,
    };

    node.preferences = NodePreferences {
        storage_price: archive.storage_price,
        storage_capacity: archive.storage_capacity,
    };

    create_program_account::<History>(
        history_info,
        system_program_info,
        signer_info,
        &tapedrive::ID,
        &[HISTORY, node_address.as_ref()],
    )?;

    let history = history_info.as_account_mut::<History>(&tapedrive::ID)?;

    history.node              = node_address;
    history.registered_epoch  = node.registered_epoch;
    history.latest_epoch      = node.latest_epoch;
    history.inner             = PoolHistory::new();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_test::*;

    #[test]
    fn test_register_node() {
        let signer = Pubkey::new_unique();

        let name = to_name("hello, world");
        let commission_rate = BasisPoints(100); // 1%
        let network_address = NetworkAddress::default();
        let network_tls = Pubkey::new_unique();

        let secret = BlsPrivateKey::from_random();
        let bls_pubkey = secret.public_key().expect("pubkey");
        let bls_pop = secret.proof_of_possession().expect("pop");

        let instruction = build_register_node_ix(
            signer,
            name,
            commission_rate,
            network_address,
            network_tls,
            bls_pubkey,
            bls_pop,
        );

        let (system_address, _) = system_pda();
        let (archive_address, _) = archive_pda();
        let (epoch_address, _) = epoch_pda();
        let (node_address, _) = node_pda(signer);
        let (history_address, _) = history_pda(node_address);

        // Setup existing accounts
        let system = System::zeroed();
        let archive = Archive {
            storage_price: TAPE(100),
            storage_capacity: StorageUnits(1_000_000),
            ..Archive::zeroed()
        };
        let epoch = Epoch {
            id: EpochNumber(42),
            state: EpochState::new(),
            last_epoch: 0,
        };

        let accounts = vec![
            sol(signer, 1_000_000_000),

            pda(system_address, system.pack(), tapedrive::ID),
            pda(archive_address, archive.pack(), tapedrive::ID),
            pda(epoch_address, epoch.pack(), tapedrive::ID),
            empty(node_address),
            empty(history_address),

            system_program(),
            rent_sysvar(),
        ];

        let env = test_env();
        env.process_instruction(
            &instruction,
            &accounts,
            &[
                Check::success(),
                Check::account(&system_address).data(
                    System {
                        total_nodes: 1,
                        ..system
                    }.pack().as_ref()
                ).build(),
                Check::account(&node_address).data(
                    Node {
                        id: NodeId::new(0),
                        authority: signer,
                        pool: StakingPool::new(commission_rate),
                        blacklist: Blacklist::new(),
                        metadata: NodeMetadata {
                            name,
                            network_address,
                            network_tls,
                            bls_pubkey,
                            next_bls_pubkey: bls_pubkey,
                        },
                        preferences: NodePreferences {
                            storage_price: TAPE(100),
                            storage_capacity: StorageUnits(1_000_000),
                        },
                        registered_epoch: epoch.id,
                        latest_epoch: epoch.id,
                        ..Node::zeroed()
                    }.pack().as_ref()
                ).build(),
                Check::account(&history_address).data(
                    History {
                        node: node_address,
                        registered_epoch: epoch.id,
                        latest_epoch: epoch.id,
                        inner: PoolHistory::new(),
                    }.pack().as_ref()
                ).build(),
            ]
        );
    }
}
