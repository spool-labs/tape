use crate::consts::*;

/// Pre-computed archive reward rate based on current bytes stored. This is calculated such that
/// each block is worth 1 minute of a 100 year time horizon, with the write cost being
/// 1 tape per megabyte stored. 
///
/// Reward per minute = (total_bytes_stored) / (total_minutes_in_100_years × bytes_per_tape)
/// Equation: reward_per_minute = bytes / (100 * 365 * 24 * 60 * (1 MiB / TAPE))
///
/// The hard-coded values avoid CU overhead.
#[inline(always)]
pub fn get_storage_rate(archive_byte_size: u64) -> u64 {
    match archive_byte_size {
        n if n < 1000              => 0,            // ~ roughly no storage, no reward
        n if n < 1048576           => 190,          // 1.0 MiB      ~ 0.00000002  TAPE/min
        n if n < 2486565           => 451,          // 2.4 MiB      ~ 0.00000005  TAPE/min
        n if n < 5896576           => 1070,         // 5.6 MiB      ~ 0.00000011  TAPE/min
        n if n < 13982985          => 2537,         // 13.3 MiB     ~ 0.00000025  TAPE/min
        n if n < 33158884          => 6017,         // 31.6 MiB     ~ 0.00000060  TAPE/min
        n if n < 78632107          => 14267,        // 75.0 MiB     ~ 0.00000143  TAPE/min
        n if n < 186466111         => 33833,        // 177.8 MiB    ~ 0.00000338  TAPE/min
        n if n < 442180832         => 80231,        // 421.7 MiB    ~ 0.00000802  TAPE/min
        n if n < 1048575999        => 190259,       // 1000.0 MiB   ~ 0.00001903  TAPE/min
        n if n < 2486565554        => 451175,       // 2.3 GiB      ~ 0.00004512  TAPE/min
        n if n < 5896576174        => 1069904,      // 5.5 GiB      ~ 0.00010699  TAPE/min
        n if n < 13982985692       => 2537141,      // 13.0 GiB     ~ 0.00025371  TAPE/min
        n if n < 33158884597       => 6016510,      // 30.9 GiB     ~ 0.00060165  TAPE/min
        n if n < 78632107044       => 14267394,     // 73.2 GiB     ~ 0.00142674  TAPE/min
        n if n < 186466111066      => 33833322,     // 173.7 GiB    ~ 0.00338333  TAPE/min
        n if n < 442180832779      => 80231450,     // 411.8 GiB    ~ 0.00802315  TAPE/min
        n if n < 1048575999999     => 190258752,    // 976.6 GiB    ~ 0.01902588  TAPE/min
        n if n < 2486565554787     => 451174602,    // 2.3 TiB      ~ 0.04511746  TAPE/min
        n if n < 5896576174027     => 1069903587,   // 5.4 TiB      ~ 0.10699036  TAPE/min
        n if n < 13982985692520    => 2537141233,   // 12.7 TiB     ~ 0.25371412  TAPE/min
        n if n < 33158884597887    => 6016510008,   // 30.2 TiB     ~ 0.60165100  TAPE/min
        n if n < 78632107044498    => 14267393633,  // 71.5 TiB     ~ 1.42673936  TAPE/min
        n if n < 186466111066097   => 33833322109,  // 169.6 TiB    ~ 3.38333221  TAPE/min
        n if n < 442180832779129   => 80231450424,  // 402.2 TiB    ~ 8.02314504  TAPE/min
        n if n < 1048576000000000  => 190258751903, // 953.7 TiB    ~ 19.02587519 TAPE/min
        _ => 20,                                    // +1.0 PiB     ~ 20.00000000 TAPE/min
    }
}

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
