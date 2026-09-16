use tape_solana::*;
use tape_crypto::address::Address;
use tape_core::prelude::*;
use crate::utils::ata;
use crate::program::tapedrive;
use crate::program::tapedrive::*;
use crate::program::token::mint_pda;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ReserveTape {
    pub storage_units: StorageUnits,
    pub activation_epoch: EpochNumber,
    pub expiry_epoch: EpochNumber,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct DestroyTape {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ExtendTapeCapacity {
    pub units: StorageUnits,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct ExtendTapeExpiry {
    pub new_expiry_epoch: EpochNumber,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct SetTapeDelegate {
    pub delegate: Address,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct RevokeTapeDelegate {}

pub fn build_reserve_tape_ix(
    fee_payer: Address,
    authority: Address,
    storage_units: StorageUnits,
    activation_epoch: EpochNumber,
    expiry_epoch: EpochNumber,
) -> Instruction {

    let authority_ata = ata(&authority);
    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (archive_ata, _) = archive_ata();
    let (mint_address, _) = mint_pda();

    let (tape_address, _) = tape_pda(authority);

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(authority_ata.into(), false),

            AccountMeta::new(tape_address.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),
            AccountMeta::new(archive_address.into(), false),
            AccountMeta::new(archive_ata.into(), false),
            AccountMeta::new(mint_address.into(), false),

            AccountMeta::new_readonly(spl_token::ID, false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(sysvar::rent::ID, false),
        ],
        data: ReserveTape {
            storage_units,
            activation_epoch,
            expiry_epoch,
        }.to_bytes(),
    }
}

pub fn build_extend_tape_capacity_ix(
    fee_payer: Address,
    payer: Address,
    tape: Address,
    units: StorageUnits,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: extend_tape_accounts(fee_payer, payer, tape),
        data: ExtendTapeCapacity { units }.to_bytes(),
    }
}

pub fn build_extend_tape_expiry_ix(
    fee_payer: Address,
    payer: Address,
    tape: Address,
    new_expiry_epoch: EpochNumber,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: extend_tape_accounts(fee_payer, payer, tape),
        data: ExtendTapeExpiry { new_expiry_epoch }.to_bytes(),
    }
}

fn extend_tape_accounts(
    fee_payer: Address,
    payer: Address,
    tape: Address,
) -> Vec<AccountMeta> {
    let payer_ata = ata(&payer);
    let (system_address, _) = system_pda();
    let (archive_address, _) = archive_pda();
    let (archive_ata, _) = archive_ata();
    let (mint_address, _) = mint_pda();

    vec![
        AccountMeta::new(fee_payer.into(), true),
        AccountMeta::new_readonly(payer.into(), true),
        AccountMeta::new(payer_ata.into(), false),

        AccountMeta::new(tape.into(), false),
        AccountMeta::new_readonly(system_address.into(), false),
        AccountMeta::new(archive_address.into(), false),
        AccountMeta::new(archive_ata.into(), false),
        AccountMeta::new(mint_address.into(), false),

        AccountMeta::new_readonly(spl_token::ID, false),
    ]
}

pub fn build_destroy_tape_ix(
    fee_payer: Address,
    authority: Address,
) -> Instruction {
    let (tape_address, _) = tape_pda(authority);
    let (system_address, _) = system_pda();

    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),

            AccountMeta::new(tape_address.into(), false),
            AccountMeta::new_readonly(system_address.into(), false),

            AccountMeta::new_readonly(system_program::ID, false),
        ],
        data: DestroyTape {}.to_bytes(),
    }
}

pub fn build_set_tape_delegate_ix(
    fee_payer: Address,
    authority: Address,
    tape: Address,
    delegate: Address,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(tape.into(), false),
        ],
        data: SetTapeDelegate { delegate }.to_bytes(),
    }
}

pub fn build_revoke_tape_delegate_ix(
    fee_payer: Address,
    authority: Address,
    tape: Address,
) -> Instruction {
    Instruction {
        program_id: tapedrive::ID,
        accounts: vec![
            AccountMeta::new(fee_payer.into(), true),
            AccountMeta::new_readonly(authority.into(), true),
            AccountMeta::new(tape.into(), false),
        ],
        data: RevokeTapeDelegate {}.to_bytes(),
    }
}
