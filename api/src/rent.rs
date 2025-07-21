use crate::consts::*;
use crate::state::{Tape, Archive};

/// Rent for one block (~ 1 min).
/// Rounded up so the protocol never under‑charges.
#[inline]
pub const fn rent_per_block(bytes: u64) -> u64 {
    let num  = bytes as u128 * ONE_TAPE as u128;
    let rent = num
        .saturating_add(RENT_DENOMINATOR - 1)
        .saturating_div(RENT_DENOMINATOR);
    rent as u64
}

/// Total rent owed by a tape since it last paid
#[inline]
pub const fn rent_owed(
    bytes:         u64,
    last_block:    u64,
    current_block: u64,
) -> u64 {
    let blocks = current_block.saturating_sub(last_block) as u128;
    (rent_per_block(bytes) as u128 * blocks) as u64
}

impl Tape {
    /// Rent this tape owes **per block** (smallest unit)
    #[inline]
    pub fn rent_per_block(&self) -> u64 {
        rent_per_block(self.total_size)
    }
    /// Rent owed since `last_rent_block`
    #[inline]
    pub fn rent_owed(&self, current_block: u64) -> u64 {
        rent_owed(self.total_size, self.last_rent_block, current_block)
    }
}

impl Archive {
    /// Global reward to miners for the current block
    #[inline]
    pub fn block_reward(&self) -> u64 {
        rent_per_block(self.bytes_stored)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rent_zero_bytes() {
        assert_eq!(rent_per_block(0), 0);
        assert_eq!(rent_owed(0, 0, 12345), 0);
    }

    #[test]
    fn rent_one_mib_one_year() {
        // For 1 MiB, over one year, should be ~1 TAPE
        let per_block = rent_per_block(BYTES_PER_MIB);
        assert!(per_block > 0);

        let owed = rent_owed(BYTES_PER_MIB, 0, MINUTES_PER_YEAR);
        assert!(owed >= ONE_TAPE);
        assert!(owed <= ONE_TAPE + MINUTES_PER_YEAR);
    }

    #[test]
    fn reverse_block_saturates() {
        // If current_block < last_block we expect zero owed.
        assert_eq!(rent_owed(BYTES_PER_MIB, 10, 5), 0);
    }

    #[test]
    fn print_storage_rate_chart() {
        let mut sizes = Vec::new();

        sizes.push(0u64);
        let mut x: u128 = BYTES_PER_MIB as u128;
        while x <= u64::MAX as u128 {
            sizes.push(x as u64);
            x = x.saturating_mul(2);
        }
        sizes.push(x as u64);

        let rents: Vec<u64> = sizes.iter().map(|&b| rent_per_block(b)).collect();

        println!("{:>20} │ {:>20}", "storage", "rent");
        println!("{}", "─".repeat(20 + 3 + 20));
        for (&bytes, &rent) in sizes.iter().zip(rents.iter()) {
            println!("{:>20} │ {:>20}", bytes, rent);
        }
    }
}
