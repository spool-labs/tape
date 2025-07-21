use crate::consts::*;

/// Pre-computed base rate based on current epoch number. After which, the archive
/// storage fees would take over, with no further inflation.
///
/// The hard-coded values avoid CU overhead.
#[inline(always)]
pub fn get_base_rate(current_epoch: u64) -> u64 {
    match current_epoch {
        n if n < 1 * EPOCHS_PER_YEAR   => 10000000000, // Year ~1,  about 1.00 TAPE/min
        n if n < 2 * EPOCHS_PER_YEAR   => 7500000000,  // Year ~2,  about 0.75 TAPE/min
        n if n < 3 * EPOCHS_PER_YEAR   => 5625000000,  // Year ~3,  about 0.56 TAPE/min
        n if n < 4 * EPOCHS_PER_YEAR   => 4218750000,  // Year ~4,  about 0.42 TAPE/min
        n if n < 5 * EPOCHS_PER_YEAR   => 3164062500,  // Year ~5,  about 0.32 TAPE/min
        n if n < 6 * EPOCHS_PER_YEAR   => 2373046875,  // Year ~6,  about 0.24 TAPE/min
        n if n < 7 * EPOCHS_PER_YEAR   => 1779785156,  // Year ~7,  about 0.18 TAPE/min
        n if n < 8 * EPOCHS_PER_YEAR   => 1334838867,  // Year ~8,  about 0.13 TAPE/min
        n if n < 9 * EPOCHS_PER_YEAR   => 1001129150,  // Year ~9,  about 0.10 TAPE/min
        n if n < 10 * EPOCHS_PER_YEAR  => 750846862,   // Year ~10, about 0.08 TAPE/min
        n if n < 11 * EPOCHS_PER_YEAR  => 563135147,   // Year ~11, about 0.06 TAPE/min
        n if n < 12 * EPOCHS_PER_YEAR  => 422351360,   // Year ~12, about 0.04 TAPE/min
        n if n < 13 * EPOCHS_PER_YEAR  => 316763520,   // Year ~13, about 0.03 TAPE/min
        n if n < 14 * EPOCHS_PER_YEAR  => 237572640,   // Year ~14, about 0.02 TAPE/min
        n if n < 15 * EPOCHS_PER_YEAR  => 178179480,   // Year ~15, about 0.02 TAPE/min
        n if n < 16 * EPOCHS_PER_YEAR  => 133634610,   // Year ~16, about 0.01 TAPE/min
        n if n < 17 * EPOCHS_PER_YEAR  => 100225957,   // Year ~17, about 0.01 TAPE/min
        n if n < 18 * EPOCHS_PER_YEAR  => 75169468,    // Year ~18, about 0.01 TAPE/min
        n if n < 19 * EPOCHS_PER_YEAR  => 56377101,    // Year ~19, about 0.01 TAPE/min
        n if n < 20 * EPOCHS_PER_YEAR  => 42282825,    // Year ~20, about 0.00 TAPE/min
        n if n < 21 * EPOCHS_PER_YEAR  => 31712119,    // Year ~21, about 0.00 TAPE/min
        n if n < 22 * EPOCHS_PER_YEAR  => 23784089,    // Year ~22, about 0.00 TAPE/min
        n if n < 23 * EPOCHS_PER_YEAR  => 17838067,    // Year ~23, about 0.00 TAPE/min
        n if n < 24 * EPOCHS_PER_YEAR  => 13378550,    // Year ~24, about 0.00 TAPE/min
        n if n < 25 * EPOCHS_PER_YEAR  => 10033913,    // Year ~25, about 0.00 TAPE/min
        _ => 0,
    }
}
