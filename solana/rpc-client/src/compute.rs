use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_instruction::Instruction;

pub fn with_compute_unit_limit(
    compute_unit_limit: u32,
    mut instructions: Vec<Instruction>,
) -> Vec<Instruction> {
    let mut instruction_batch = Vec::with_capacity(instructions.len() + 1);
    instruction_batch.push(
        ComputeBudgetInstruction::set_compute_unit_limit(compute_unit_limit),
    );
    instruction_batch.append(&mut instructions);
    instruction_batch
}

#[cfg(test)]
mod tests {
    use solana_instruction::Instruction;
    use solana_pubkey::Pubkey;

    use super::with_compute_unit_limit;

    #[test]
    fn prepends_compute_budget_instruction() {
        let program_instruction = Instruction {
            program_id: Pubkey::new_unique(),
            accounts: Vec::new(),
            data: vec![9, 8, 7],
        };

        let instructions = with_compute_unit_limit(400_000, vec![program_instruction.clone()]);

        assert_eq!(instructions.len(), 2);
        assert_eq!(instructions[1], program_instruction);
    }
}
